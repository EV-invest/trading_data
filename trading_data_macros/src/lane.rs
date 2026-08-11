//! One column list, read once. A stored lane spelled by hand states each column's identity five
//! times over — the builder field, the schema `Field`, the `append`, the position in `finish`, and
//! the name `decode` looks up — and the fourth of those is *positional* against the second. A
//! column added to one and not the other round-trips green, because both sides shift together.
//!
//! Here the order is the field order, so `finish` cannot disagree with `schema` about it.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, LitInt, LitStr, Path};

use crate::diag::{Diag, Result};

struct Col {
	field: Ident,
	name: String,
	/// The stored name in ident position — the builder holds one per column, and naming it after the
	/// column rather than the field is what lets a hand-written columnar path address it by the name
	/// the disk uses.
	col: Ident,
	/// `DataType::#arrow`, `arrow::array::#arrow`+`Builder`, `#arrow`+`Array` — one arrow name
	/// answers the schema, the builder and the reader.
	arrow: Ident,
	nullable: bool,
	/// Rust reading → stored reading, and back. `None` where the field already is the column.
	enc: Option<Path>,
	dec: Option<Path>,
}

struct Lane {
	ty: Ident,
	builders: Ident,
	per_row_min: LitInt,
	/// Whether the lane's columns are raw — then its `Meta` is the precision that reads them, carried
	/// in the file metadata rather than in a column, and the signature every file of a range shares.
	prec: bool,
	cols: Vec<Col>,
}

fn arrow_of(p: &Path) -> Option<(&'static str, Option<(&'static str, &'static str)>)> {
	let n = p.get_ident()?.to_string();
	let ts = Some(("Ts::as_nanos", "Ts::from_nanos"));
	Some(match n.as_str() {
		"ts" => ("Int64", ts),
		"i64" => ("Int64", None),
		"u64" => ("UInt64", None),
		"i32" => ("Int32", None),
		"u32" => ("UInt32", None),
		"u8" => ("UInt8", None),
		"f64" => ("Float64", None),
		_ => return None,
	})
}

fn col(field: Ident, a: &syn::Attribute) -> Result<Col> {
	let mut arrow = None;
	let mut nullable = false;
	let mut name = None;
	let (mut enc, mut dec) = (None, None);
	a.parse_nested_meta(|m| {
		if m.path.is_ident("null") {
			nullable = true;
		} else if m.path.is_ident("name") {
			name = Some(m.value()?.parse::<LitStr>()?.value());
		} else if m.path.is_ident("enc") {
			enc = Some(m.value()?.parse::<Path>()?);
		} else if m.path.is_ident("dec") {
			dec = Some(m.value()?.parse::<Path>()?);
		} else {
			match arrow_of(&m.path) {
				Some((t, codec)) => {
					arrow = Some((Ident::new(t, Span::call_site()), codec));
				}
				None => return Err(m.error("not a stored column type: one of `ts`, `i64`, `u64`, `i32`, `u32`, `u8`, `f64`")),
			}
		}
		Ok(())
	})?;
	let Some((arrow, codec)) = arrow else {
		return Err(Diag::spanned(a, "this column says nothing about how it is stored").help("name the arrow type first: `#[col(ts)]`, `#[col(i32, name = \"price_raw\")]`"));
	};
	// `ts` is a codec and not just a width, so a field that overrides one half of it says which.
	if let Some((e, d)) = codec {
		let lit = |s: &str| syn::parse_str::<Path>(s).expect("a path spelled out in this file");
		enc = enc.or_else(|| Some(lit(e)));
		dec = dec.or_else(|| Some(lit(d)));
	}
	let name = name.unwrap_or_else(|| field.to_string());
	let col = syn::parse_str::<Ident>(&name).map_err(|_| Diag::spanned(a, format!("`{name}` is not a name a builder field can carry")))?;
	Ok(Col {
		name,
		col,
		field,
		arrow,
		nullable,
		enc,
		dec,
	})
}

fn parse(d: &DeriveInput) -> Result<Lane> {
	let Data::Struct(s) = &d.data else {
		return Err(Diag::spanned(&d.ident, "a lane row is a struct"));
	};
	let Fields::Named(fields) = &s.fields else {
		return Err(Diag::spanned(&d.ident, "a lane row's fields have to be named").help("`#[col(..)]` marks fields, and a tuple struct has none to mark"));
	};

	let mut per_row_min = None;
	let mut prec = false;
	for a in d.attrs.iter().filter(|a| a.path().is_ident("lane")) {
		a.parse_nested_meta(|m| {
			if m.path.is_ident("per_row_min") {
				per_row_min = Some(m.value()?.parse::<LitInt>()?);
			} else if m.path.is_ident("prec") {
				prec = true;
			} else {
				return Err(m.error("a lane states `per_row_min = n`, and `prec` where its columns are raw"));
			}
			Ok(())
		})?;
	}
	let Some(per_row_min) = per_row_min else {
		return Err(Diag::spanned(&d.ident, "this lane does not say what a row of it costs")
			.help("`#[lane(per_row_min = 48)]`")
			.note("the writer sizes every builder from it once, rather than letting seven `Vec`s double their way up"));
	};

	let mut cols = Vec::new();
	for f in &fields.named {
		let field = f.ident.clone().expect("named fields");
		match f.attrs.iter().find(|a| a.path().is_ident("col")) {
			Some(a) => cols.push(col(field, a)?),
			None =>
				return Err(Diag::spanned(f, "this field is not stored")
					.help("give it a `#[col(..)]`, or drop it from the row")
					.note("a lane row is exactly what the disk holds, so a field no column carries is one a read cannot bring back")),
		}
	}
	if cols.is_empty() {
		return Err(Diag::spanned(&d.ident, "this lane stores no column"));
	}
	Ok(Lane {
		builders: format_ident!("{}Builders", d.ident),
		ty: d.ident.clone(),
		per_row_min,
		prec,
		cols,
	})
}

pub fn lane(input: TokenStream) -> Result<TokenStream> {
	let d: DeriveInput = syn::parse2(input)?;
	let Lane {
		ty,
		builders,
		per_row_min,
		prec,
		cols,
	} = parse(&d)?;

	let fields = cols.iter().map(|c| {
		let (f, b) = (&c.col, format_ident!("{}Builder", c.arrow));
		quote!(#f: arrow::array::#b)
	});
	let inits = cols.iter().map(|c| {
		let (f, b) = (&c.col, format_ident!("{}Builder", c.arrow));
		quote!(#f: arrow::array::#b::with_capacity(rows))
	});
	let schema_fields = cols.iter().map(|c| {
		let (n, a, null) = (&c.name, &c.arrow, c.nullable);
		quote!(Field::new(#n, DataType::#a, #null))
	});
	let appends = cols.iter().map(|c| {
		let (f, b) = (&c.field, &c.col);
		match (c.nullable, &c.enc) {
			(true, Some(e)) => quote!(b.#b.append_option(self.#f.map(#e))),
			(true, None) => quote!(b.#b.append_option(self.#f)),
			(false, Some(e)) => quote!(b.#b.append_value(#e(self.#f))),
			(false, None) => quote!(b.#b.append_value(self.#f)),
		}
	});
	let finishes = cols.iter().map(|c| {
		let f = &c.col;
		quote!(Arc::new(b.#f.finish()) as ArrayRef)
	});
	let reads = cols.iter().map(|c| {
		let (f, n, a) = (&c.field, &c.name, format_ident!("{}Array", c.arrow));
		quote!(let #f = col::<#a>(batch, #n);)
	});
	let takes = cols.iter().map(|c| {
		let f = &c.field;
		let one = match &c.dec {
			Some(d) => quote!(#d(#f.value(i))),
			None => quote!(#f.value(i)),
		};
		match c.nullable {
			true => quote!(#f: (!#f.is_null(i)).then(|| #one)),
			false => quote!(#f: #one),
		}
	});

	// The precision is the file's, not the row's: written once into the metadata, so every row of a
	// lane owes the same one, and read back from there rather than from a column.
	let (meta_ty, schema_meta, pairs) = match prec {
		true => (quote!(PrecisionPriceQty), quote!(meta), quote!(&prec_pairs(meta))),
		false => (quote!(()), quote!(_meta), quote!(&[])),
	};
	let (sig_arg, sig) = match prec {
		true => (quote!(schema), quote!(prec_sig(schema))),
		false => (quote!(_schema), quote!(None)),
	};

	Ok(quote! {
		pub struct #builders {
			#(#fields,)*
		}

		impl sealed::Sealed for #ty {
			type Builders = #builders;
			type Meta = #meta_ty;

			const PER_ROW_MIN: usize = #per_row_min;

			fn builders(rows: usize) -> #builders {
				#builders { #(#inits,)* }
			}

			fn schema(#schema_meta: #meta_ty) -> SchemaRef {
				schema_with(vec![#(#schema_fields,)*], #pairs)
			}

			fn append(&self, b: &mut #builders, _meta: #meta_ty) {
				#(#appends;)*
			}

			fn finish(b: &mut #builders) -> Vec<ArrayRef> {
				vec![#(#finishes,)*]
			}

			fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
				#(#reads)*
				(0..batch.num_rows()).map(|i| #ty { #(#takes,)* }).collect()
			}

			fn file_sig(#sig_arg: &Schema) -> Option<FileSig> {
				#sig
			}

			fn approx_bytes(&self) -> usize {
				#per_row_min
			}
		}
	})
}
