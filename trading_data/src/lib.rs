//! Facade/prelude: the derivation engine, indicator state machines, and the storage tier.
//! Depend on this crate only — sub-crates are wiring detail.

pub use dep_dag::{Cell, Cons, DepOuts, Nil, Node, Observer, step, step_obs};
pub use trading_data_derivatives::{WilderAtr, WilderRsi};
pub use trading_data_persistence::{
	BookDelta, BookShape, BookSnapshot, BookUpdate, Catalog, CatalogError, Clock, Close, Custom, Data, Feather, FileEntry, FileMetadata, Lane, LaneKey, LaneReader, LiveBook, LiveClock,
	Replay, ReplayConfig, RotationPolicy, Row, Trade, UnixNanos, book, catalog, clock, feather, live, read, read_closes, read_deltas, read_snapshots, read_trades, replay, schema,
};
