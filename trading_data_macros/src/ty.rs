//! Type tokens as the trampoline passes them around: how a dep unwraps, what a node is keyed on,
//! and where the shim answering for it lives.

use proc_macro2::{Delimiter, Group, Ident, Punct, Spacing, Span, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::{GenericArgument, PathArguments, Type, TypePath};

/// The dep-position wrappers the driver knows structurally — everything else is the cell itself.
pub enum Wrap {
	Bare,
	Fold,
	/// The [`Reach`] type the *engine* is asked to retain, which is what a `Buffer` field is sized on.
	Buf {
		reach: TokenStream,
	},
	/// The point-level read. No reach to carry: a `Latest` field holds one item, whatever was asked.
	Sample,
	Gate,
}

pub fn parse_type(ts: &TokenStream) -> syn::Result<Type> {
	syn::parse2(ts.clone())
}

/// `Folding<C, R>` / `Buffering<C, R>` / `Sampling<C>` / `Gating<C>` → `C` plus which of them it
/// was. Matched on the last path segment, so a wrapper named through any path spelling still reads
/// as one.
///
/// `Buffer<C, H>` and `Latest<C>` are the frame cells those two resolve *against*, and are rejected
/// here rather than aliased onto the field they name — the same seal `#[node]` puts on `impl Node`.
pub fn unwrap_dep(ty: &Type) -> syn::Result<(Type, Wrap)> {
	let bare = || Ok((ty.clone(), Wrap::Bare));
	let Type::Path(p) = ty else { return bare() };
	let Some(seg) = p.path.segments.last() else { return bare() };
	match seg.ident.to_string().as_str() {
		"Buffer" =>
			return Err(syn::Error::new_spanned(
				ty,
				"`Buffer<C, H>` is the frame cell `graph!` grows for you — its reach is the join of every read of `C` in the graph, so it is not yours to state. Write `Buffering<C, R>`",
			)),
		"Latest" => return Err(syn::Error::new_spanned(ty, "`Latest<C>` is the frame cell `graph!` grows for you. Write `Sampling<C>`")),
		_ => (),
	}
	let PathArguments::AngleBracketed(args) = &seg.arguments else { return bare() };
	let mut it = args.args.iter().filter(|a| !matches!(a, GenericArgument::Lifetime(_)));
	let (Some(GenericArgument::Type(cell)), reach) = (it.next(), it.next()) else {
		return bare();
	};
	Ok(match seg.ident.to_string().as_str() {
		"Folding" => (cell.clone(), Wrap::Fold),
		"Gating" => (cell.clone(), Wrap::Gate),
		"Buffering" => {
			let reach = reach.ok_or_else(|| syn::Error::new_spanned(ty, "a `Buffering` dep names the reach the engine retains for it: write `Buffering<C, R>`"))?;
			(cell.clone(), Wrap::Buf { reach: reach.to_token_stream() })
		}
		"Sampling" => (cell.clone(), Wrap::Sample),
		_ => (ty.clone(), Wrap::Bare),
	})
}

/// What a node is deduped on: the last path segment and its arguments, recursively. Path-insensitive
/// because one cell is reached under whatever spelling each consumer happened to import — and two
/// spellings of one type must land on one field, or the frame carries it twice.
pub fn norm(ty: &Type) -> String {
	match ty {
		Type::Path(p) => match p.path.segments.last() {
			Some(seg) => {
				let mut s = seg.ident.to_string();
				if let PathArguments::AngleBracketed(a) = &seg.arguments {
					s.push('<');
					let args: Vec<String> = a
						.args
						.iter()
						.map(|g| match g {
							GenericArgument::Type(t) => norm(t),
							other => other.to_token_stream().to_string().replace(' ', ""),
						})
						.collect();
					s.push_str(&args.join(","));
					s.push('>');
				}
				s
			}
			None => ty.to_token_stream().to_string(),
		},
		other => other.to_token_stream().to_string().replace(' ', ""),
	}
}

/// The arguments a generic node's shim is invoked with — one per type *or const* parameter it
/// declared, in order, since that is what its matcher binds. A const written as an expression
/// (`Bars<{ TF_1MIN }>`) arrives as `Const` where one written as a bare
/// path is indistinguishable from a type; both fill the same slot.
pub fn type_args(ty: &Type) -> Vec<GenericArgument> {
	let Type::Path(p) = ty else { return Vec::new() };
	let Some(seg) = p.path.segments.last() else { return Vec::new() };
	let PathArguments::AngleBracketed(a) = &seg.arguments else { return Vec::new() };
	a.args.iter().filter(|g| !matches!(g, GenericArgument::Lifetime(_))).cloned().collect()
}

/// The `$crate` token, which only a `macro_rules!` body may carry — spelled by hand because
/// `Ident::new` refuses it.
fn dollar_crate() -> TokenStream {
	TokenStream::from_iter([TokenTree::Punct(Punct::new('$', Spacing::Alone)), TokenTree::Ident(Ident::new("crate", Span::call_site()))])
}

/// Where the `macro_rules!` answering for `ty` lives. A shim is written by a macro, and rust#52234
/// leaves those unreachable by absolute path within their own crate — so a cell named without a
/// crate is asked for unqualified, textually, which is why a node's impl must precede the `graph!`
/// reaching it and why a module of cells is declared `#[macro_use]`. A cell named *through* its
/// crate keeps that crate: cross-crate the path resolution works.
pub fn shim_path(ty: &Type, prefix: &str) -> syn::Result<TokenStream> {
	let Type::Path(TypePath { qself, path, .. }) = ty else {
		return Err(syn::Error::new_spanned(ty, "a dep is a named cell: this is not a path type"));
	};
	if qself.is_some() {
		return Err(syn::Error::new_spanned(ty, "cannot resolve an associated type in `Deps`; name a concrete cell"));
	}
	let last = path.segments.last().expect("a path has a segment");
	let mac = Ident::new(&format!("{prefix}{}", last.ident), last.ident.span());
	let first = &path.segments.first().expect("a path has a segment").ident;
	if path.segments.len() == 1 || matches!(first.to_string().as_str(), "crate" | "$crate" | "self" | "super") {
		return Ok(quote!(#mac));
	}
	let lead = path.leading_colon;
	Ok(quote!(#lead #first::#mac))
}

/// Whether `ty`'s outermost name is `ident` — the driver's structural reading of the dag's own zoo
/// (`Armed<N>`), which has no shim of its own.
pub fn head_is(ty: &Type, ident: &str) -> bool {
	matches!(ty, Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == ident))
}

/// The single type argument of a one-parameter generic (`Armed<N>` → `N`).
pub fn sole_arg(ty: &Type) -> Option<Type> {
	let Type::Path(p) = ty else { return None };
	let PathArguments::AngleBracketed(a) = &p.path.segments.last()?.arguments else { return None };
	a.args.iter().find_map(|g| match g {
		GenericArgument::Type(t) => Some(t.clone()),
		_ => None,
	})
}

/// Rewrites a declaration-site type so it still means the same thing wherever a shim pastes it:
/// generic parameters become the shim's metavariables, `Self` becomes the concrete self type, and a
/// `crate::`-rooted path becomes `$crate::`-rooted.
pub fn shimify(ts: TokenStream, params: &[Ident], self_ty: &TokenStream) -> TokenStream {
	ts.into_iter()
		.flat_map(|t| match t {
			TokenTree::Group(g) => vec![TokenTree::Group(Group::new(g.delimiter(), shimify(g.stream(), params, self_ty)))],
			TokenTree::Ident(ref i) if i == "crate" => dollar_crate().into_iter().collect(),
			TokenTree::Ident(ref i) if i == "Self" => self_ty.clone().into_iter().collect(),
			TokenTree::Ident(ref i) if params.contains(i) => TokenStream::from_iter([TokenTree::Punct(Punct::new('$', Spacing::Alone)), t.clone()]).into_iter().collect(),
			other => vec![other],
		})
		.collect()
}

/// The delimiter-stripped reading of a token stream, for keys and messages: a `macro_rules!` `:ty`
/// fragment arrives wrapped in an invisible group, which `to_string` would otherwise spell.
pub fn flatten(ts: TokenStream) -> TokenStream {
	ts.into_iter()
		.flat_map(|t| match t {
			TokenTree::Group(g) if g.delimiter() == Delimiter::None => flatten(g.stream()).into_iter().collect::<Vec<_>>(),
			TokenTree::Group(g) => vec![TokenTree::Group(Group::new(g.delimiter(), flatten(g.stream())))],
			other => vec![other],
		})
		.collect()
}
