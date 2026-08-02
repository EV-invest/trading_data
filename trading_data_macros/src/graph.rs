//! `graph!` — the declaration. It states the roots, the outputs and what else is worth reading; the
//! node set, its order, its buffers and their sizes are the driver's job.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
	Ident, Token, Type, Visibility, braced, bracketed,
	parse::{Parse, ParseStream},
	punctuated::Punctuated,
	token,
};

use crate::{
	state::{Awaiting, Cfg, Dep, Named, Root, State},
	ty,
};

mod kw {
	syn::custom_keyword!(batches);
	syn::custom_keyword!(roots);
	syn::custom_keyword!(out);
	syn::custom_keyword!(outputs);
	syn::custom_keyword!(observe);
}

/// `field: Ty [Event]` — a root cell and the event type its slice carries.
struct RootDef {
	field: Ident,
	ty: Type,
	event: Type,
}
impl Parse for RootDef {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let field = input.parse()?;
		input.parse::<Token![:]>()?;
		let ty = input.parse()?;
		let content;
		bracketed!(content in input);
		let event = content.parse()?;
		Ok(RootDef { field, ty, event })
	}
}

/// `field: Ty` — a cell, under the name the out-struct reads it by.
struct NamedDef {
	field: Ident,
	ty: Type,
}
impl Parse for NamedDef {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let field = input.parse()?;
		input.parse::<Token![:]>()?;
		Ok(NamedDef { field, ty: input.parse()? })
	}
}

struct GraphDef {
	vis: Visibility,
	graph: Ident,
	batches: Ident,
	roots: Vec<RootDef>,
	out: Ident,
	named: Vec<NamedDef>,
}
impl Parse for GraphDef {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let vis: Visibility = input.parse()?;
		input.parse::<Token![struct]>()?;
		let graph: Ident = input.parse()?;
		input.parse::<Token![;]>()?;

		input.parse::<kw::batches>()?;
		let batches: Ident = input.parse()?;
		input.parse::<Token![;]>()?;

		input.parse::<kw::roots>()?;
		let content;
		braced!(content in input);
		let roots: Punctuated<RootDef, Token![,]> = Punctuated::parse_terminated(&content)?;
		input.parse::<Token![;]>()?;
		if roots.is_empty() {
			return Err(syn::Error::new(input.span(), "graph! needs at least one root"));
		}

		input.parse::<kw::out>()?;
		let out: Ident = input.parse()?;
		input.parse::<Token![;]>()?;

		input.parse::<kw::outputs>()?;
		let content;
		braced!(content in input);
		let mut named: Vec<NamedDef> = Punctuated::<NamedDef, Token![,]>::parse_terminated(&content)?.into_iter().collect();
		if named.is_empty() {
			return Err(syn::Error::new(
				input.span(),
				"graph! needs at least one output: a graph that produces nothing is built out of nothing",
			));
		}

		if input.peek(kw::observe) && input.peek2(token::Brace) {
			input.parse::<kw::observe>()?;
			let content;
			braced!(content in input);
			named.extend(Punctuated::<NamedDef, Token![,]>::parse_terminated(&content)?);
		}

		Ok(GraphDef {
			vis,
			graph,
			batches,
			roots: roots.into_iter().collect(),
			out,
			named,
		})
	}
}

pub fn graph(input: TokenStream) -> syn::Result<TokenStream> {
	let def: GraphDef = syn::parse2(input)?;

	let queue: Vec<Dep> = def
		.named
		.iter()
		.map(|n| {
			Ok(Dep {
				shim: ty::shim_path(&n.ty, "__td_node_")?,
				ty: n.ty.to_token_stream(),
			})
		})
		.collect::<syn::Result<_>>()?;

	let st = State {
		cfg: Cfg {
			vis: def.vis.to_token_stream(),
			graph: def.graph,
			batches: def.batches,
			out: def.out,
			roots: def
				.roots
				.into_iter()
				.map(|r| Root {
					field: r.field,
					ty: r.ty.to_token_stream(),
					event: r.event.to_token_stream(),
				})
				.collect(),
			named: def
				.named
				.into_iter()
				.map(|n| Named {
					field: n.field,
					ty: n.ty.to_token_stream(),
				})
				.collect(),
		},
		awaiting: Awaiting::Nothing,
		known: Vec::new(),
		stack: Vec::new(),
		order: Vec::new(),
		bufs: Vec::new(),
		queue,
	};

	Ok(quote! { ::trading_data_dag::__graph_resolve! { @state #st } })
}
