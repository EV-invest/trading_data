//! The macros that turn `type Deps` into a frame. `#[node]` leaves each cell's dep tokens where a
//! macro can read them, `graph!` states the roots and the outputs, and `__graph_resolve!` walks the
//! one from the other. Generated code reaches the dag through whichever path the invoking crate has
//! it under — resolved once, in `graph!`, and carried in the driver state from there.

#![feature(proc_macro_diagnostic, proc_macro_span)]

use proc_macro::TokenStream;

mod demand;
mod diag;
mod graph;
mod item;
mod lane;
mod node;
mod resolve;
mod shape;
mod state;
mod ty;

fn out(r: diag::Result<proc_macro2::TokenStream>) -> TokenStream {
	match r {
		Ok(ts) => ts.into(),
		Err(d) => d.emit(),
	}
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
/// Goes on `impl Blind`/`Emit`/`Symbolic` for the cell, and on `impl Episodic` (which publishes the
/// arm instead — `Armed<Self>`'s dep is an associated-type projection). It also writes the `Node`
/// impl, mapping the body trait to the kernel that computes it. `#[node(latch)]` marks a cell with a
/// hand-written `impl Latch`, which is a separate impl, and a cell has one shim.
#[proc_macro_attribute]
pub fn node(attr: TokenStream, item: TokenStream) -> TokenStream {
	out(node::node(attr.into(), item.into()))
}

/// The out plane's four mechanical readings of one field list: `Flat`, `Bump`, `Stamped`, and
/// `Unflat` where the slots carry the whole item.
///
/// ```ignore
/// #[derive(Clone, Copy, Debug, trading_data::Item)]
/// pub struct Bar {
///     #[stamp] pub ts_close: Ts<Venue>,
///     #[slot] pub open: f64,
///     #[slot] pub close: f64,
/// }
/// ```
///
/// `#[slot]` fields are the flattening, in declaration order — which is also `DIMS`, the `Bump`
/// index and the `unflat` read, so the four cannot drift apart. `#[stamp]` names the event time;
/// its type owes `as_nanos`/`from_nanos`.
///
/// Two things a slot's `f64` cannot say about itself, so the attribute does. `#[slot(discrete)]`
/// marks one that cannot be perturbed: its `Bump` returns `0.0` and its Jacobian column stays NaN
/// rather than a fabricated zero. `#[slot(absent)]` marks one whose NaN is a *decline* rather than
/// arithmetic, and one of them raises `Flat::ABSENTABLE` for the whole out
/// (`r[outs.absence.typed]`) — the const the kernels check a body's `Expr::MAYBE` against.
///
/// A field that is neither is *carried*: it withholds `Unflat` alone, since a kernel writing slots
/// has nothing to rebuild it from. `Glance` is not here — it is a human-facing line, and no two of
/// the items in this workspace write the same one.
#[proc_macro_derive(Item, attributes(stamp, slot))]
pub fn item(input: TokenStream) -> TokenStream {
	out(item::item(input.into()))
}

/// A stored lane's whole encoding from its column list: the builders struct, `schema`, `append`,
/// `finish` and `decode`.
///
/// ```ignore
/// #[derive(Clone, Copy, Debug, PartialEq, trading_data::Lane)]
/// #[lane(per_row_min = 48, prec)]
/// pub struct Trade {
///     #[col(ts)] pub ts_venue_exec: Ts<Venue>,
///     #[col(ts, null)] pub ts_venue_send: Option<Ts<Venue>>,
///     #[col(u8, enc = side_u8, dec = side_from)] pub side: Side,
///     #[col(i32, name = "price_raw")] pub price: i32,
/// }
/// ```
///
/// The column order is the field order, which is what `finish` and `schema` used to state
/// separately — one positional and one not, so a column added to either alone round-tripped green.
/// `ts` is `Int64` plus the `Ts` codec; the rest are stored as they stand unless `enc`/`dec` name
/// the pair that reads them. `prec` makes the lane's `Meta` its `PrecisionPriceQty`, which is what
/// scales its raw columns — carried in the file metadata, never in a column of its own.
///
/// Expands where the row is declared and reaches for that module's `col`, `schema_with`,
/// `prec_pairs`, `prec_sig` and `sealed::Sealed` — the encoding is the storage tier's alone, and a
/// lane declared anywhere else has no disk to be a contract with.
#[proc_macro_derive(Lane, attributes(lane, col))]
pub fn lane(input: TokenStream) -> TokenStream {
	out(lane::lane(input.into()))
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
