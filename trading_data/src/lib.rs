//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

use std::any::TypeId;

pub use trading_data_dag::{Cell, Cons, DepOuts, Episode, Fire, Flat, Gate, Glance, Guide, Ink, Latch, Nil, Node, Nudge, Observer, Roots, Sketch, graph, observe_root, step, step_obs};
pub use trading_data_derivatives::{WilderAtr, WilderRsi};
pub use trading_data_persistence::{
	Batch, BookDelta, BookShape, BookUpdate, Catalog, CatalogError, Clock, Feather, Feed, LaneKind, LaneReader, LatencyConfig, Live, LiveClock, Mc, Oi, Replay, RotationPolicy, Row, Sink,
	Trade, UnixNanos, read_mc, read_oi, read_trades,
};

/// The source lanes a graph requires: maps its [`Roots::required_events`] `TypeId`s to
/// [`LaneKind`]s. Lives here — the dag stays storage-free, and persistence can't see graph types —
/// so it's the one point that knows both an event's `TypeId` and its lane. An unknown event panics.
pub fn required_lanes<G: Roots>() -> Vec<LaneKind> {
	G::required_events()
		.into_iter()
		.map(|id| {
			if id == TypeId::of::<Trade>() {
				LaneKind::Trades
			} else if id == TypeId::of::<BookDelta>() {
				LaneKind::Book
			} else if id == TypeId::of::<Oi>() {
				LaneKind::Oi
			} else if id == TypeId::of::<Mc>() {
				LaneKind::Mc
			} else {
				panic!("required_lanes: root event {id:?} has no known source lane")
			}
		})
		.collect()
}
