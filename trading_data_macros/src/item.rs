//! One field list, read once. An item is an event time plus a fixed tuple of `f64` slots, and the
//! out plane asks four traits the same question about it — the shape, the slot order, the
//! perturbation and the stamp. Hand-written they are four restatements nothing cross-checks:
//! `DIMS` is unrelated to what `flat` writes, and `unflat` reads the slots back in an order only a
//! reader compares.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, Type};

use crate::{
	diag::{Diag, Result},
	graph::dag_path,
};

struct Slot {
	name: Ident,
	/// Whether this slot names something that can move at all. A `Bump` returning `h` for one that
	/// cannot is a Jacobian column the witness fabricates.
	discrete: bool,
	/// Whether "no reading" is a state this slot can be in — what `Flat::ABSENTABLE` publishes for
	/// the whole out, and what the kernels check a body's `Expr::MAYBE` against
	/// (`r[outs.absence.typed]`).
	absent: bool,
}

struct Item {
	ty: Ident,
	stamp: Ident,
	stamp_ty: Type,
	slots: Vec<Slot>,
	/// Fields that are neither, which is what `unflat` would have nothing to rebuild from.
	carried: bool,
}

fn parse(d: &DeriveInput) -> Result<Item> {
	let Data::Struct(s) = &d.data else {
		return Err(Diag::spanned(&d.ident, "an item is a struct").note("the out plane reads an item as a stamp and a tuple of `f64` slots, and only a struct has named fields to be those"));
	};
	let Fields::Named(fields) = &s.fields else {
		return Err(Diag::spanned(&d.ident, "an item's fields have to be named").help("`#[stamp]` and `#[slot]` mark fields, and a tuple struct has none to mark"));
	};
	if let Some(p) = d.generics.params.first() {
		return Err(Diag::spanned(p, "a generic item has no one flattening").note("`DIMS` is a `const` of the type, so a parameter that could change the slot count would make it two"));
	}

	let mut stamp: Option<(Ident, Type)> = None;
	let mut slots = Vec::new();
	let mut carried = false;
	for f in &fields.named {
		let name = f.ident.clone().expect("named fields");
		let is_stamp = f.attrs.iter().any(|a| a.path().is_ident("stamp"));
		let slot = f.attrs.iter().find(|a| a.path().is_ident("slot"));
		match (is_stamp, slot) {
			(true, Some(a)) =>
				return Err(Diag::spanned(a, "a stamp is not a slot")
					.note("an event time is the kernel's to carry rather than a differentiation variable (`r[kernels.selection.index-is-not-a-variable]`)")),
			(true, None) => match stamp.replace((name, f.ty.clone())) {
				None => {}
				Some((prev, _)) =>
					return Err(Diag::spanned(f, format!("`{prev}` already stamps this item")).note("`Stamped` answers one time, so two readings of it would be a choice nothing states")),
			},
			(false, Some(a)) => {
				let (mut discrete, mut absent) = (false, false);
				if !matches!(a.meta, syn::Meta::Path(_)) {
					a.parse_nested_meta(|m| {
						if m.path.is_ident("discrete") {
							discrete = true;
						} else if m.path.is_ident("absent") {
							absent = true;
						} else {
							return Err(m.error("a slot is plain, `discrete`, `absent`, or both"));
						}
						Ok(())
					})?;
				}
				slots.push(Slot { name, discrete, absent });
			}
			(false, None) => carried = true,
		}
	}

	let Some((stamp, stamp_ty)) = stamp else {
		return Err(Diag::spanned(&d.ident, "no field stamps this item")
			.help("mark the event-time field `#[stamp]`")
			.note("a retained item you cannot index by time is one you can only read at an assumed cadence"));
	};
	if slots.is_empty() {
		return Err(Diag::spanned(&d.ident, "no field is a slot of this item")
			.help("mark each flattened field `#[slot]`")
			.note("a zero-slot out would fire and leave the tape byte-identical to an unfired one (`r[outs.flat.nonempty]`)"));
	}
	Ok(Item {
		ty: d.ident.clone(),
		stamp,
		stamp_ty,
		slots,
		carried,
	})
}

pub fn item(input: TokenStream) -> Result<TokenStream> {
	let d: DeriveInput = syn::parse2(input)?;
	let Item {
		ty,
		stamp,
		stamp_ty,
		slots,
		carried,
	} = parse(&d)?;
	let dag = dag_path()?;

	let n = slots.len();
	// one `Field` per slot, so a partial reading names a field where it would otherwise number one.
	let fields = slots.iter().enumerate().map(|(i, s)| {
		let name = Ident::new(&s.name.to_string().to_uppercase(), s.name.span());
		quote!(pub const #name: #dag::Field<Self> = #dag::Field::at(#i);)
	});
	// One absent slot is an absence channel on the whole out: `ABSENTABLE` is what a declining body
	// publishes through, and it is per-out because that is what the kernels can check.
	let absentable = slots.iter().any(|s| s.absent);
	let reads = slots.iter().map(|s| &s.name);
	let bumps = slots.iter().enumerate().map(|(i, s)| {
		let name = &s.name;
		match s.discrete {
			true => quote!(#i => (self, 0.0)),
			false => quote!(#i => (Self { #name: self.#name + h, ..self }, h)),
		}
	});

	// `unflat` names every field or it names none: a field the slots do not carry is one the item
	// would come back from a kernel having lost.
	let unflat = (!carried).then(|| {
		let fills = slots.iter().enumerate().map(|(i, s)| {
			let name = &s.name;
			quote!(#name: slots[#i])
		});
		quote! {
			impl #dag::Unflat for #ty {
				fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
					Self { #stamp: <#stamp_ty>::from_nanos(ts_ns), #(#fills),* }
				}
			}
		}
	});

	Ok(quote! {
		impl #ty {
			#(#fields)*
		}

		impl #dag::Flat for #ty {
			const ABSENTABLE: bool = #absentable;
			const DIMS: &'static [usize] = &[#n];

			fn flat(&self, out: &mut [f64]) -> bool {
				out.copy_from_slice(&[#(self.#reads),*]);
				true
			}
		}

		impl #dag::Bump for #ty {
			fn bump(self, slot: usize, h: f64) -> (Self, f64) {
				match slot {
					#(#bumps,)*
					s => panic!("`{}` has {} slots, bumped {}", stringify!(#ty), #n, s),
				}
			}
		}

		impl #dag::Stamped for #ty {
			fn ts_ns(&self) -> i64 {
				self.#stamp.as_nanos()
			}
		}

		#unflat
	})
}
