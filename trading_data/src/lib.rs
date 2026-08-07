//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

use std::any::TypeId;

#[cfg(feature = "bench")]
pub mod bench;

// r[impl boundaries.examples.facade-only]
pub use trading_data_core::{Asset, ExchangeName, Instrument, Pair, RelayCols, Symbol};
#[doc(hidden)]
pub use trading_data_dag as __dag;
pub use trading_data_dag::{
	Armed, Batch, Blind, Buffering, Bump, Carried, Cell, Close, CloseOuts, Closes, Decides, DepOuts, Elems, EmitOuts, Episode, Episodic, Fidelity, Fire, Flat, Fold, FoldOuts, Folding,
	Folds, Gate, Gating, Glance, Guide, Hist, Horizon, Ink, Latch, Level, Nudge, Observer, Over, Pending, Plot, Predicate, Present, ProbabilisticDistribution, Reach, Roots, Rows, Run,
	RunOuts, Runs, Sampling, Scan, ScanOuts, Scans, Series, Stamped, Symbolic, Tag, TriggerOut, Unbounded, Unflat, Want, always_present, graph, node, node_alias, slice_nudge, value_nudge,
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
	closed_by,
	rsi,
	wilder,
};
pub use trading_data_expr::{Ex, Expr, Slots, Vars, abs, constant, exp, gt, lt, max, min, powi_of, select, sqrt, square, sum};
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
