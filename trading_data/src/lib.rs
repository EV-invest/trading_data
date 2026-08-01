//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

use std::any::TypeId;

pub use trading_data_dag::{
	Abs, Add, Ast, Buffer, Buffering, Bump, Cell, Cons, Const, DepOuts, Diff, Div, Episode, Ex, Expr, Fire, Flat, Gate, Glance, Guide, Hist, Horizon, Ink, Latch, Mul, Neg, Nil, Node, Nudge,
	Observer, Plot, Roots, Series, Square, Stamped, Sub, Sum, Symbolic, Trace, Var, Vars, abs, constant, graph, observe_root, slice_nudge, square, step, step_exact, step_obs, sum,
	value_nudge,
};
pub use trading_data_derivatives::{Wilder, WilderAtr, WilderAvgGainLoss, rsi};
pub use trading_data_persistence::{
	Aggregate, Arrival, BatchTrades, BatchWindow, Book, BookAnchors, BookDeltas, BookShape, BookUpdate, Catalog, CatalogError, Clock, DeltaBuf, DeltaCols, DeltaFrame, Exact, Feather, Feed,
	FrameKind, InnerTrade, LaneKind, LaneReader, Lanes, LatencyConfig, Live, LiveClock, Local, Mc, McRoot, Oi, OiRoot, Precision, PrecisionPriceQty, Replay, RotationPolicy, Row, ShadowBook,
	Side, Sink, Span, Trade, TradeBuf, TradeCols, Trades, Ts, UnixNanos, Venue, read_mc, read_oi, read_trades,
};

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
			i if i == TypeId::of::<DeltaFrame<'static>>() => LaneKind::BookDeltas,
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
