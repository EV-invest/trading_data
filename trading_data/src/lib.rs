//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

pub use trading_data_dag::{Cell, Cons, DepOuts, Episode, Fire, Flat, Gate, Glance, Guide, Ink, Latch, Nil, Node, Nudge, Observer, Sketch, graph, observe_root, step, step_obs};
pub use trading_data_derivatives::{WilderAtr, WilderRsi};
pub use trading_data_persistence::{
	Batch, BookDelta, BookShape, BookUpdate, Catalog, CatalogError, Clock, Feather, Feed, LaneKind, LaneReader, LatencyConfig, Live, LiveClock, Mc, Oi, Replay, RotationPolicy, Row, Sink,
	Trade, UnixNanos, read_mc, read_oi, read_trades,
};
