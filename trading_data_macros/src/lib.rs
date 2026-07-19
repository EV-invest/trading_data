//! `graph!` — declare a frame's node set and emit the hand-written `step` chain in topological
//! order. Pure ergonomics: `trading_data_dag`'s `Pull` bound already rejects bad orders and cycles
//! at compile time, so this only removes wiring noise. The generated code targets
//! `::trading_data_dag`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
	Ident, Token, Type, Visibility, braced, bracketed,
	parse::{Parse, ParseStream},
	parse_macro_input,
	punctuated::Punctuated,
	token,
};

mod kw {
	syn::custom_keyword!(batches);
	syn::custom_keyword!(roots);
	syn::custom_keyword!(out);
	syn::custom_keyword!(latch);
}

/// `field: Ty [Event]` — a root cell and the event type its slice carries.
struct Root {
	field: Ident,
	ty: Type,
	event: Type,
}
impl Parse for Root {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let field = input.parse()?;
		input.parse::<Token![:]>()?;
		let ty = input.parse()?;
		let content;
		bracketed!(content in input);
		let event = content.parse()?;
		Ok(Root { field, ty, event })
	}
}

/// `field: Ty` — a node field or a latch field.
struct FieldNode {
	field: Ident,
	ty: Type,
}
impl Parse for FieldNode {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let field = input.parse()?;
		input.parse::<Token![:]>()?;
		let ty = input.parse()?;
		Ok(FieldNode { field, ty })
	}
}

struct GraphDef {
	vis: Visibility,
	graph: Ident,
	batches: Ident,
	roots: Vec<Root>,
	out: Ident,
	latches: Vec<FieldNode>,
	nodes: Vec<FieldNode>,
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
		let roots: Punctuated<Root, Token![,]> = Punctuated::parse_terminated(&content)?;
		input.parse::<Token![;]>()?;
		if roots.is_empty() {
			return Err(syn::Error::new(input.span(), "graph! needs at least one root"));
		}

		input.parse::<kw::out>()?;
		let out: Ident = input.parse()?;
		input.parse::<Token![;]>()?;

		let latches = if input.peek(kw::latch) && input.peek2(token::Brace) {
			input.parse::<kw::latch>()?;
			let content;
			braced!(content in input);
			Punctuated::<FieldNode, Token![,]>::parse_terminated(&content)?.into_iter().collect()
		} else {
			Vec::new()
		};

		let nodes: Punctuated<FieldNode, Token![,]> = Punctuated::parse_terminated(input)?;
		if nodes.is_empty() {
			return Err(syn::Error::new(input.span(), "graph! needs at least one node"));
		}

		Ok(GraphDef {
			vis,
			graph,
			batches,
			roots: roots.into_iter().collect(),
			out,
			latches,
			nodes: nodes.into_iter().collect(),
		})
	}
}

/// Wires a declared node list into a graph struct + typed out-struct + batch-native `tick`. Fields
/// in topo order — a wrong order fails the existing `Pull`/`Has` bounds at compile time.
///
/// ```ignore
/// graph! {
///     pub struct Graph;
///     batches Batches;                       // name of the generated root-slices struct
///     roots { trades: Trades[Trade], oi: OiRoot[Oi] };
///     out TickOut;
///     latch { live: Live }                   // optional
///     bar: Bar1m, cvd: Cvd, ...
/// }
/// ```
///
/// Each root cell must have `Out<'t> = &'t [Event]`. `Batches<'t>` gets one `&'t [Event]` field per
/// root (`Default` = all empty). `tick<'t>(&'t mut self, b: Batches<'t>) -> TickOut<'t>` seeds the
/// frame with every root slice and sweeps. `required_events()` returns the `TypeId`s of the events
/// whose root is consumed by some node — the dep tree, computed in isolation.
///
/// An optional `latch { field: Type, .. }` group names `Latch` fields (also in the node list). A
/// latch whose `Cut` out reads `Episode::terminal` is commutated and its gated fields reset to
/// `Default` at the *next* tick's start (deferred: the frame still borrows batch fields).
#[proc_macro]
pub fn graph(input: TokenStream) -> TokenStream {
	let GraphDef {
		vis,
		graph,
		batches,
		roots,
		out,
		latches,
		nodes,
	} = parse_macro_input!(input as GraphDef);

	let dag = quote!(::trading_data_dag);

	let rfields: Vec<&Ident> = roots.iter().map(|r| &r.field).collect();
	let root_tys: Vec<&Type> = roots.iter().map(|r| &r.ty).collect();
	let event_tys: Vec<&Type> = roots.iter().map(|r| &r.event).collect();

	let lfields: Vec<&Ident> = latches.iter().map(|l| &l.field).collect();
	let latch_tys: Vec<&Type> = latches.iter().map(|l| &l.ty).collect();

	let fields: Vec<&Ident> = nodes.iter().map(|n| &n.field).collect();
	let node_tys: Vec<&Type> = nodes.iter().map(|n| &n.ty).collect();

	// deferred commutation, inlined per latch: reset every field gated on the commutated latch.
	// (The cross-product latch × field is a plain nested loop here — no tt-muncher needed.)
	let apply_pending = latches.iter().map(|l| {
		let lfield = &l.field;
		let latch_ty = &l.ty;
		let fields = &fields;
		let node_tys = &node_tys;
		quote! {
			if self.__pending.#lfield {
				self.__pending.#lfield = false;
				<#latch_ty as #dag::Latch>::commutate(&mut self.#lfield);
				#(
					if const {
						#dag::contains(<<#node_tys as #dag::Node>::When as #dag::GateSet>::NAMES, #dag::node_name::<#latch_ty>())
					} {
						self.#fields = ::core::default::Default::default();
					}
				)*
			}
		}
	});

	quote! {
		#[derive(Default)]
		#vis struct #batches<'t> {
			#(pub #rfields: <#root_tys as #dag::Cell>::Out<'t>,)*
		}

		#[derive(Default)]
		#[doc(hidden)]
		struct __Pending {
			#(#lfields: bool,)*
		}

		#[derive(Default)]
		#vis struct #graph {
			#(#fields: #node_tys,)*
			__pending: __Pending,
		}

		const _: () = {
			const METAS: &[#dag::NodeMeta] = &[#(
				#dag::NodeMeta {
					name: #dag::node_name::<#node_tys>(),
					deps: <<#node_tys as #dag::Node>::Deps as #dag::DepSet>::NAMES,
					historic: <#node_tys as #dag::Node>::HISTORIC,
					gates: <<#node_tys as #dag::Node>::When as #dag::GateSet>::NAMES,
				},
			)*];
			#(assert!(
				!#dag::shadowed(#dag::node_name::<#node_tys>(), METAS),
				concat!(stringify!(#node_tys), " is only consumed under a gate: gate it too, or mark it historic")
			);)*
		};

		#[derive(Clone, Copy, Debug)]
		#vis struct #out<'t> {
			#(pub #fields: <#node_tys as #dag::Cell>::Out<'t>,)*
			#[doc(hidden)]
			pub __lt: ::core::marker::PhantomData<&'t ()>,
		}

		impl #dag::Roots for #graph {
			/// `TypeId`s of the events whose root is consumed by some node (the dep tree).
			fn required_events() -> #dag::MacroVec<::core::any::TypeId> {
				const NAMES: &[&[&str]] = &[#(<<#node_tys as #dag::Node>::Deps as #dag::DepSet>::NAMES),*];
				let mut out = #dag::MacroVec::new();
				#(
					if NAMES.iter().any(|ns| #dag::contains(ns, #dag::node_name::<#root_tys>())) {
						out.push(#dag::event_id::<#event_tys>());
					}
				)*
				out
			}
		}

		impl #graph {
			#vis fn tick<'t>(&'t mut self, b: #batches<'t>) -> #out<'t> {
				self.tick_obs(b, &mut ())
			}

			#vis fn tick_obs<'t>(&'t mut self, b: #batches<'t>, obs: &mut impl #dag::Observer) -> #out<'t> {
				// deferred commutation: apply last tick's terminals before anything borrows self.
				#(#apply_pending)*

				let #batches { #(#rfields,)* } = b;
				let Self { #(#fields,)* __pending } = self;

				#(#dag::observe_root::<#root_tys, _>(#rfields, obs);)*
				let f = #dag::Nil;
				#(let f = #dag::Cons::<#root_tys, _> { out: #rfields, tail: f };)*
				#(let f = #dag::step_obs(f, #fields, obs);)*

				#(
					if #dag::Episode::terminal(&#dag::Has::<<#latch_tys as #dag::Latch>::Cut, _>::get(&f)) {
						__pending.#lfields = true;
					}
				)*

				#out {
					#(#fields: #dag::Has::<#node_tys, _>::get(&f),)*
					__lt: ::core::marker::PhantomData,
				}
			}
		}
	}
	.into()
}
