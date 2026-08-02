//! `#[node]` and `node_alias!` — the two ways a cell puts its `Deps` where a macro can read them.
//!
//! Types are not resolved at expansion time, so `graph!` cannot ask what `Rsi::Deps` is. The one
//! cross-item name the compiler *will* resolve for a macro is another macro's, so each annotated impl
//! leaves a `macro_rules!` row at its crate root carrying its dep tokens, which the driver calls back.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{
	ImplItem, ItemImpl, Token, Type, Visibility,
	parse::{Parse, ParseStream},
	punctuated::Punctuated,
};

use crate::ty::{self, Wrap};

/// `#[node]`, `#[node(latch)]`, `#[node(diff)]` — flags the impl cannot state on its own, because the
/// trait that would state them (`Latch`, `Diff`) is a second impl and a node has only one shim.
struct Flags {
	latch: bool,
	diff: bool,
}
impl Parse for Flags {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let mut f = Flags { latch: false, diff: false };
		for i in Punctuated::<Ident, Token![,]>::parse_terminated(input)? {
			match i.to_string().as_str() {
				"latch" => f.latch = true,
				"diff" => f.diff = true,
				_ => return Err(syn::Error::new(i.span(), "expected `latch` or `diff`")),
			}
		}
		Ok(f)
	}
}

fn assoc<'i>(item: &'i ItemImpl, name: &str) -> Option<&'i Type> {
	item.items.iter().find_map(|i| match i {
		ImplItem::Type(t) if t.ident == name => Some(&t.ty),
		_ => None,
	})
}

/// The shim's own name, and the metavariables its caller fills: one `{shim} {type}` pair per generic
/// parameter, so a dep that *is* a parameter can still say where to ask about it.
struct Sig {
	name: Ident,
	params: Vec<Ident>,
}

fn signature(item: &ItemImpl, prefix: &str) -> syn::Result<Sig> {
	let Type::Path(p) = &*item.self_ty else {
		return Err(syn::Error::new_spanned(&item.self_ty, "`#[node]` wants a named cell as the self type"));
	};
	let last = &p.path.segments.last().expect("a path has a segment").ident;
	if let Some(g) = item.generics.const_params().next() {
		return Err(syn::Error::new_spanned(
			g,
			"`#[node]` cannot shim a const-generic node: the driver knows the dag's own generics structurally",
		));
	}
	Ok(Sig {
		name: Ident::new(&format!("{prefix}{last}"), last.span()),
		params: item.generics.type_params().map(|p| p.ident.clone()).collect(),
	})
}

fn matcher(sig: &Sig) -> TokenStream {
	let args = sig.params.iter().map(|p| {
		let shim = Ident::new(&format!("__shim_{p}"), p.span());
		quote!({ $#shim:path } { $#p:ty })
	});
	quote!(($__driver:path, [ #(#args),* ], @state $($__state:tt)*))
}

pub fn node(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
	let flags: Flags = syn::parse2(attr)?;
	let item: ItemImpl = syn::parse2(item)?;

	let Some((path, _)) = &item.trait_ else {
		return Err(syn::Error::new_spanned(&item, "`#[node]` goes on a trait impl: `impl Node/Emit/Symbolic/Episodic for X`"));
	};
	let trait_name = path.segments.last().expect("a path has a segment").ident.to_string();

	let sig = signature(&item, "__td_node_")?;
	let sty = &item.self_ty;
	// `Self` in a dep resolves against the impl's own self type, which the shim pastes elsewhere.
	let self_ty = ty::shimify(quote!(#sty), &sig.params, &TokenStream::new());
	let self_subst = self_ty.clone();

	if trait_name == "Episodic" {
		let trigger = assoc(&item, "Trigger").ok_or_else(|| syn::Error::new_spanned(&item, "`impl Episodic` without `type Trigger`"))?;
		let shim = ty::shim_path(trigger, "__td_node_")?;
		let tty = ty::shimify(quote!(#trigger), &sig.params, &self_subst);
		let name = signature(&item, "__td_trigger_")?.name;
		let m = matcher(&sig);
		return Ok(quote! {
			#item

			#[macro_export]
			#[doc(hidden)]
			macro_rules! #name {
				#m => { $__driver! { @trigger { #shim } { #tty } @state $($__state)* } };
			}
		});
	}

	let kind = match trait_name.as_str() {
		"Node" | "Symbolic" => quote!(node),
		"Emit" => quote!(emit),
		_ => return Err(syn::Error::new_spanned(path, "`#[node]` goes on `impl Node`, `Emit`, `Symbolic` or `Episodic`")),
	};
	// `Symbolic` earns `Diff` through the same blanket that earns it `Node`.
	let diff = flags.diff || trait_name == "Symbolic";
	let (diff, latch) = (Ident::new(&diff.to_string(), Span::call_site()), Ident::new(&flags.latch.to_string(), Span::call_site()));

	let deps_ty = assoc(&item, "Deps").ok_or_else(|| syn::Error::new_spanned(&item, "`#[node]` needs `type Deps` in the impl"))?;
	let deps: Vec<&Type> = match deps_ty {
		Type::Tuple(t) => t.elems.iter().collect(),
		other => vec![other],
	};

	let deps: Vec<TokenStream> = deps
		.into_iter()
		.map(|d| -> syn::Result<TokenStream> {
			let (cell, _) = ty::unwrap_dep(d);
			// a dep that *is* a parameter has no shim until the caller says: it passes one alongside.
			let shim = match &cell {
				Type::Path(p) if p.qself.is_none() && p.path.get_ident().is_some_and(|i| sig.params.contains(i)) => {
					let m = Ident::new(&format!("__shim_{}", p.path.get_ident().expect("just matched")), Span::call_site());
					quote!($#m)
				}
				_ => ty::shim_path(&cell, "__td_node_")?,
			};
			let dty = ty::shimify(quote!(#d), &sig.params, &self_subst);
			Ok(quote!({ #shim } { #dty }))
		})
		.collect::<syn::Result<_>>()?;

	let (name, m) = (&sig.name, matcher(&sig));
	Ok(quote! {
		#item

		#[macro_export]
		#[doc(hidden)]
		macro_rules! #name {
			#m => {
				$__driver! {
					@kind #kind @diff #diff @latch #latch @self { #self_ty }
					@deps [ #(#deps),* ]
					@state $($__state)*
				}
			};
		}
	})
}

struct Alias {
	attrs: Vec<syn::Attribute>,
	vis: Visibility,
	name: Ident,
	ty: Type,
}
impl Parse for Alias {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let attrs = input.call(syn::Attribute::parse_outer)?;
		let vis = input.parse()?;
		let name = input.parse()?;
		input.parse::<Token![=]>()?;
		let ty = input.parse()?;
		input.parse::<Token![;]>()?;
		Ok(Alias { attrs, vis, name, ty })
	}
}

/// A `type` alias is invisible to macros, so a graph reached through one would find no shim. This
/// declares the alias *and* the forwarding row — which reports the aliased cell's own spelling, so
/// naming a node both ways still lands on one field.
pub fn node_alias(input: TokenStream) -> syn::Result<TokenStream> {
	let Alias { attrs, vis, name, ty } = syn::parse2(input)?;
	if !matches!(ty::unwrap_dep(&ty).1, Wrap::Bare) {
		return Err(syn::Error::new_spanned(&ty, "a `node_alias!` names a cell, not a dep-position wrapper"));
	}
	let target = ty::shim_path(&ty, "__td_node_")?;
	let shim = Ident::new(&format!("__td_node_{name}"), name.span());
	Ok(quote! {
		#(#attrs)*
		#vis type #name = #ty;

		#[macro_export]
		#[doc(hidden)]
		macro_rules! #shim {
			($__driver:path, [], @state $($__state:tt)*) => {
				#target! { $__driver, [], @state $($__state)* }
			};
		}
	})
}
