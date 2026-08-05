//! `graph!` — the declaration. It states the roots and the outputs; the node set, its order, its
//! buffers and their sizes are the driver's job.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::{
	Ident, Token, Type, Visibility, braced, bracketed,
	parse::{Parse, ParseStream},
	punctuated::Punctuated,
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
		let named: Vec<NamedDef> = Punctuated::<NamedDef, Token![,]>::parse_terminated(&content)?.into_iter().collect();
		if named.is_empty() {
			return Err(syn::Error::new(
				input.span(),
				"graph! needs at least one output: a graph that produces nothing is built out of nothing",
			));
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

/// Where the invoking crate reaches the dag: either it depends on `trading_data_dag` outright, or on
/// the facade, which re-exports it. Resolved here because `graph!` is the one entry point — from here
/// it rides in the driver state, which a `macro_rules!` shim can pass back but not re-derive.
/// `#[node]` asks separately, since the `impl Node` it writes names the dag at the declaration site.
pub fn dag_path() -> syn::Result<TokenStream> {
	let reach = |name: &str, item: TokenStream| match crate_name(name) {
		Ok(FoundCrate::Itself) => Some(quote!(crate #item)),
		Ok(FoundCrate::Name(n)) => {
			let n = Ident::new(&n, Span::call_site());
			Some(quote!(::#n #item))
		}
		Err(_) => None,
	};
	reach("trading_data_dag", quote!()).or_else(|| reach("trading_data", quote!(::__dag))).ok_or_else(|| {
		syn::Error::new(
			Span::call_site(),
			"`graph!` expands to dag paths: this crate depends on neither `trading_data` nor `trading_data_dag`",
		)
	})
}

pub fn graph(input: TokenStream) -> syn::Result<TokenStream> {
	let def: GraphDef = syn::parse2(input)?;
	let dag = dag_path()?;

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
			dag: dag.clone(),
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
		aliases: Vec::new(),
		queue,
	};

	Ok(quote! { #dag::__graph_resolve! { @state #st } })
}
