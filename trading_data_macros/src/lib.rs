//! The macros that turn `type Deps` into a frame. `#[node]` leaves each cell's dep tokens where a
//! macro can read them, `graph!` states the roots and the outputs, and `__graph_resolve!` walks the
//! one from the other. Generated code reaches the dag through whichever path the invoking crate has
//! it under — resolved once, in `graph!`, and carried in the driver state from there.

use proc_macro::TokenStream;

mod demand;
mod graph;
mod node;
mod resolve;
mod state;
mod ty;

fn out(r: syn::Result<proc_macro2::TokenStream>) -> TokenStream {
	r.unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Builds a frame from its outputs: the node set, its topological order, the `Emitter` wrapping, the
/// latches and the `Buffer`s (each sized to the join of every read taken of it) are all derived from
/// the annotated `type Deps` of the cells the outputs reach.
///
/// ```ignore
/// trading_data::graph! {
///     pub struct Graph;
///     batches Batches;                       // name of the generated root-slices struct
///     roots { trades: Trades[Trade], oi: OiRoot[Oi] };
///     out TickOut;
///     outputs { cvd: Cvd }                   // what the graph is for
/// }
/// ```
///
/// Every cell named here, and every cell they reach, must carry [`macro@node`] on its trait impl —
/// otherwise expansion fails with `cannot find macro __td_node_Foo`, naming the cell that is missing
/// it. A node no output reaches is not instantiated at all.
///
/// `Batches<'t>` gets one field per root, of that root cell's `Out<'t>`. It is deliberately not
/// `Default`: every field is filled explicitly from a woven step, and a silently-empty root is a
/// footgun. `tick<'t>(&'t mut self, b: Batches<'t>) -> TickOut<'t>` seeds the frame with every root
/// out and sweeps. `required_events()` returns the `TypeId`s of the events whose root is consumed by
/// some node, so a declared root nothing reaches is simply never loaded. `Graph::NODES` is the
/// derived closure in sweep order.
///
/// A `Latch` field (`#[node(latch)]`, or any `Episodic`'s `Armed<Self>`) whose `Cut` out reads
/// `Episode::terminal` is commutated and its gated fields reset to `Default` at the *next* tick's
/// start (deferred: the frame still borrows batch fields).
#[proc_macro]
pub fn graph(input: TokenStream) -> TokenStream {
	out(graph::graph(input.into()))
}

/// Publishes an impl's `type Deps` to [`macro@graph`], which cannot ask the type system for it.
///
/// Goes on `impl Node`/`Emit`/`Symbolic` for the cell, and on `impl Episodic` (which publishes the
/// arm instead — `Armed<Self>`'s dep is an associated-type projection). `#[node(latch)]` marks a cell
/// with a hand-written `impl Latch`, and `#[node(diff)]` one with a hand-written `impl Diff`; both
/// are separate impls, and a cell has one shim.
#[proc_macro_attribute]
pub fn node(attr: TokenStream, item: TokenStream) -> TokenStream {
	out(node::node(attr.into(), item.into()))
}

/// A `type` alias to a cell, declared so [`macro@graph`] can follow it.
///
/// ```ignore
/// trading_data::node_alias! { pub Screener = StdScreener; }
/// ```
///
/// Swapping the right-hand side reroutes every graph that names the alias. Both spellings resolve to
/// one field: the alias reports the cell's own name.
#[proc_macro]
pub fn node_alias(input: TokenStream) -> TokenStream {
	out(node::node_alias(input.into()))
}

/// The [`macro@graph`] driver — one dep-tree step per expansion, ping-ponging with the `#[node]`
/// shims. Never written by hand.
#[doc(hidden)]
#[proc_macro]
pub fn __graph_resolve(input: TokenStream) -> TokenStream {
	out(resolve::resolve(input.into()))
}
