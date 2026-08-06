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

use crate::{
	graph::dag_path,
	ty::{self, Wrap},
};

/// `#[node]`, `#[node(latch)]` — the one flag the impl cannot state on its own, because the trait
/// that would state it (`Latch`) is a second impl and a node has only one shim.
struct Flags {
	latch: bool,
}
impl Parse for Flags {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let mut f = Flags { latch: false };
		for i in Punctuated::<Ident, Token![,]>::parse_terminated(input)? {
			match i.to_string().as_str() {
				"latch" => f.latch = true,
				_ => return Err(syn::Error::new(i.span(), "expected `latch`")),
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

/// The shim's own name, and the metavariables its caller fills: one `{shim} {arg}` pair per generic
/// parameter, in declaration order, so a dep that *is* a parameter can still say where to ask about
/// it.
struct Sig {
	name: Ident,
	params: Vec<(Ident, bool)>,
}

impl Sig {
	/// The substitution list `ty::shimify` rewrites against — const parameters included, so a
	/// `Folding<Trades, Over<TF>>` dep becomes `Folding<Trades, Over<$TF>>`.
	fn idents(&self) -> Vec<Ident> {
		self.params.iter().map(|(i, _)| i.clone()).collect()
	}
}

fn signature(item: &ItemImpl, prefix: &str) -> syn::Result<Sig> {
	let Type::Path(p) = &*item.self_ty else {
		return Err(syn::Error::new_spanned(&item.self_ty, "`#[node]` wants a named cell as the self type"));
	};
	let last = &p.path.segments.last().expect("a path has a segment").ident;
	Ok(Sig {
		name: Ident::new(&format!("{prefix}{last}"), last.span()),
		params: item
			.generics
			.params
			.iter()
			.filter_map(|p| match p {
				syn::GenericParam::Type(t) => Some((t.ident.clone(), false)),
				syn::GenericParam::Const(c) => Some((c.ident.clone(), true)),
				syn::GenericParam::Lifetime(_) => None,
			})
			.collect(),
	})
}

/// Where a *shim body* spells the shim answering for one of its deps, and the one place the two
/// readings of an unqualified cell part ways. rust#52234 leaves a macro-expanded `macro_export`
/// macro unreachable by absolute path *within its own crate*, so a bare cell is asked for
/// textually — which is why a graph reaching it must live in the crate that declares it. A cell
/// written `crate::C` says the opposite: it is asked for through `$crate::`, which resolves only
/// once the shim is pasted into a graph elsewhere.
fn dep_shim(ty: &Type, prefix: &str) -> syn::Result<TokenStream> {
	let mac = ty::shim_path(ty, prefix)?;
	match ty {
		Type::Path(p) if p.path.segments.first().is_some_and(|s| s.ident == "crate") => Ok(ty::shimify(quote!(crate::#mac), &[], &TokenStream::new())),
		_ => Ok(mac),
	}
}

fn matcher(sig: &Sig) -> TokenStream {
	let args = sig.params.iter().map(|(p, is_const)| {
		let shim = Ident::new(&format!("__shim_{p}"), p.span());
		// a const argument is exactly one token tree, and a `:ty` fragment is not known to re-parse
		// there. The shim slot stays for positional alignment; a const parameter's body never uses it.
		let arg = if *is_const { quote!($#p:tt) } else { quote!($#p:ty) };
		quote!({ $#shim:path } { #arg })
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

	let dag = dag_path()?;
	let sig = signature(&item, "__td_node_")?;
	let sty = &item.self_ty;
	// `Self` in a dep resolves against the impl's own self type, which the shim pastes elsewhere.
	let self_ty = ty::shimify(quote!(#sty), &sig.idents(), &TokenStream::new());
	let self_subst = self_ty.clone();

	if trait_name == "Episodic" {
		let trigger = assoc(&item, "Trigger").ok_or_else(|| syn::Error::new_spanned(&item, "`impl Episodic` without `type Trigger`"))?;
		let shim = dep_shim(trigger, "__td_node_")?;
		let tty = ty::shimify(quote!(#trigger), &sig.idents(), &self_subst);
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

	// which kernel computes this node, and therefore what the engine can read off it. The body trait
	// *is* the choice: there is no attribute spelling it, so a node cannot name a kernel it has no body
	// for.
	let (kernel, emit) = match trait_name.as_str() {
		"Symbolic" => (quote!(#dag::Pure), false),
		"Blind" => (quote!(#dag::Opaque), false),
		"Runs" => (quote!(#dag::Raw), true),
		"Node" => {
			return Err(syn::Error::new_spanned(
				path,
				"`impl Node` is written by `#[node]`, not by hand: a node declares which kernel computes it. Write `impl Symbolic` for an `Expr` body, or `impl Blind` — the stated hatch, which needs a `const WHY`",
			));
		}
		"Emit" => {
			return Err(syn::Error::new_spanned(
				path,
				"`impl Emit` is written by `#[node]`, not by hand: a run-shaped node declares which kernel fills it. Write `impl Runs` — the stated hatch, which needs a `const WHY`",
			));
		}
		_ => return Err(syn::Error::new_spanned(path, "`#[node]` goes on `impl Blind`, `Runs`, `Symbolic` or `Episodic`")),
	};
	let kind = match emit {
		false => quote!(node),
		true => quote!(emit),
	};
	let latch = Ident::new(&flags.latch.to_string(), Span::call_site());

	// the `Node`/`Emit` impl nobody writes: `Deps` and `PLOTS` are forwarded off the body trait, so a
	// node site states each exactly once.
	// `#sty` is the impl's self type as written, arguments and all, so `ty_generics` would repeat them.
	let (imp, _, wher) = item.generics.split_for_impl();
	let node_impl = {
		let body: syn::Path = syn::parse2(quote!(#path)).expect("the impl'd trait is a path");
		let head = match emit {
			false => quote!(#dag::Node),
			true => quote!(#dag::Emit),
		};
		quote! {
			impl #imp #head for #sty #wher {
				type Deps = <Self as #body>::Deps;
				type Kernel = #kernel;
				const PLOTS: &'static [#dag::Plot] = <Self as #body>::PLOTS;
			}
		}
	};

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
				Type::Path(p) if p.qself.is_none() && p.path.get_ident().is_some_and(|i| sig.idents().contains(i)) => {
					let m = Ident::new(&format!("__shim_{}", p.path.get_ident().expect("just matched")), Span::call_site());
					quote!($#m)
				}
				_ => dep_shim(&cell, "__td_node_")?,
			};
			let dty = ty::shimify(quote!(#d), &sig.idents(), &self_subst);
			Ok(quote!({ #shim } { #dty }))
		})
		.collect::<syn::Result<_>>()?;

	let (name, m) = (&sig.name, matcher(&sig));
	Ok(quote! {
		#item
		#node_impl

		#[macro_export]
		#[doc(hidden)]
		macro_rules! #name {
			#m => {
				$__driver! {
					@kind #kind @latch #latch @self { #self_ty }
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
	let target = dep_shim(&ty, "__td_node_")?;
	// the alias is concrete, so the aliased cell's own parameters are filled in here rather than by
	// whoever names the alias.
	let args = ty::type_args(&ty)
		.into_iter()
		.map(|a| match &a {
			syn::GenericArgument::Type(t) => {
				let s = dep_shim(t, "__td_node_")?;
				Ok(quote!({ #s } { #a }))
			}
			// nothing answers for a const, and its body never asks: the slot is positional only
			_ => Ok(quote!({ __td_not_a_cell } { #a })),
		})
		.collect::<syn::Result<Vec<_>>>()?;
	let shim = Ident::new(&format!("__td_node_{name}"), name.span());
	Ok(quote! {
		#(#attrs)*
		#vis type #name = #ty;

		#[macro_export]
		#[doc(hidden)]
		macro_rules! #shim {
			($__driver:path, [], @state $($__state:tt)*) => {
				#target! { $__driver, [ #(#args),* ], @state $($__state)* }
			};
		}
	})
}
