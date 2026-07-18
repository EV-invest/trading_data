//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

pub use trading_data_dag::{Cell, Cons, Dag, DepOuts, Fire, Flat, Glance, Nil, Node, Observer, Stamped, graph, observe_root, step, step_obs};
pub use trading_data_derivatives::{WilderAtr, WilderRsi};
pub use trading_data_persistence::{
	BookDelta, BookShape, BookUpdate, Catalog, CatalogError, Clock, Feather, LaneReader, LiveBook, LiveClock, Mc, Oi, RotationPolicy, Row, Trade, UnixNanos, read_book, read_mc,
	read_oi, read_trades,
};
