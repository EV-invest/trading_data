//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

use std::any::TypeId;

#[cfg(feature = "bench")]
pub mod bench;

// r[impl boundaries.examples.facade-only]
pub use trading_data_core::{Asset, ExchangeName, Instrument, Pair, Symbol};
#[doc(hidden)]
pub use trading_data_dag as __dag;
pub use trading_data_dag::{
	__graph_resolve, Armed, At, Batch, Blind, Buffer, Buffering, Bump, Cell, Cons, DepOuts, Elems, Emit, EmitOuts, Episode, Episodic, Fidelity, Fire, Flat, Folding, Gate, Gating, Glance,
	Guide, Hist, Horizon, Ink, Jac, Latch, Latest, Level, Nil, Node, Nudge, Observer, Opaque, Over, Plot, Present, ProbabilisticDistribution, Pure, Reach, Roots, Rows, Sampling, Series,
	Stamped, Sweep, Symbolic, Tag, TriggerOut, Unbounded, Want, always_present, graph, node, node_alias, observe_root, slice_nudge, step, step_obs, value_nudge,
};
pub use trading_data_derivatives::{
	// the dep shims: a `#[node]` writes one at its own crate's root, and a graph naming the cell
	// through this facade asks for it under the same path.
	__td_node_AvgGain,
	__td_node_AvgLoss,
	__td_node_Bars,
	__td_node_Ohlcs,
	__td_node_Rsi,
	__td_node_RsiDelta,
	__td_node_Volumes,
	AvgGain,
	AvgLoss,
	Bar,
	Bars,
	Ohlc,
	Ohlcs,
	Rsi,
	RsiDelta,
	RsiSpec,
	RsiValues,
	Volume,
	Volumes,
	Wilder,
	WilderAtr,
	WilderAvgGainLoss,
	closed_by,
	rsi,
};
pub use trading_data_expr::{Abs, Add, Ast, Const, Div, Ex, Expr, Mul, Neg, Square, Sub, Sum, Trace, Var, Vars, abs, constant, square, sum};
pub use trading_data_persistence::{
	__td_node_Book, Aggregate, Arrival, BatchTrades, Book, BookAnchors, BookChunk, BookDelta, BookDeltas, BookShape, BookUpdate, Catalog, CatalogError, Clock, Direction, Exact, Feather,
	Feed, FrameKind, InnerTrade, LaneKind, LaneReader, Lanes, LatencyConfig, Live, LiveClock, Local, Mc, McRoot, Oi, OiRoot, Precision, PrecisionPriceQty, ReadClock, Replay, RotationPolicy,
	Row, ShadowBook, Side, Sink, Span, Trade, TradeBuf, TradeCols, Trades, Ts, UnixNanos, Usd, Venue, read_mc, read_oi, read_trades,
};
/// The period a `Bars<TF>` is over: naming one is now part of wiring a graph, and a facade-only
/// consumer has no other route to the type.
pub use v_utils::{Timeframe, TimeframeDesignator};

/// The source lanes a graph requires: maps its [`Roots::required_events`] `TypeId`s to
/// [`LaneKind`]s. Lives here — the dag stays storage-free, and persistence can't see graph types —
/// so it's the one point that knows both an event's `TypeId` and its lane. An unknown event panics.
///
/// Deduped: a graph naming both book roots would otherwise have [`Replay`] load and accumulate the
/// delta lane twice.
pub fn required_lanes<G: Roots>() -> Vec<LaneKind> {
	let mut out = Vec::new();
	for id in G::required_events() {
		// `TypeId` is not a structural-match type, so the arms are guards; it still reads as the
		// one-to-one table it is.
		let lane = match id {
			i if i == TypeId::of::<TradeCols<'static>>() => LaneKind::Trades,
			i if i == TypeId::of::<BookDelta>() => LaneKind::BookDeltas,
			i if i == TypeId::of::<BookShape>() => LaneKind::BookAnchors,
			i if i == TypeId::of::<Oi>() => LaneKind::Oi,
			i if i == TypeId::of::<Mc>() => LaneKind::Mc,
			_ => panic!("required_lanes: root event {id:?} has no known source lane"),
		};
		if !out.contains(&lane) {
			out.push(lane);
		}
	}
	out
}
