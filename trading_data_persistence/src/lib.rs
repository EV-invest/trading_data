#![feature(default_field_values)]
mod catalog;
mod clock;
mod feather;
mod read;
mod row;
mod sync;

pub use catalog::{Catalog, CatalogError};
pub use clock::{Clock, LiveClock};
pub use feather::{Feather, RotationPolicy};
pub use read::{LaneReader, read_mc, read_oi, read_trades};
pub use row::{Mc, McRoot, Oi, OiRoot, Row, Trade, UnixNanos};
pub use sync::{Feed, LaneKind, Lanes, Live, Past, Replay, Sink, Step};
pub use trading_data_core::{
	// `Book`'s dep shim: a `#[node]` writes it at its own crate's root, and a graph naming the cell
	// through this facade asks for it under the same path.
	__td_node_Book,
	Aggregate,
	Arrival,
	BatchTrades,
	Book,
	BookAnchors,
	BookChunk,
	BookDelta,
	BookDeltas,
	BookShape,
	BookUpdate,
	Direction,
	Exact,
	FrameKind,
	InnerTrade,
	Local,
	Precision,
	PrecisionPriceQty,
	ReadClock,
	ShadowBook,
	Side,
	Span,
	TradeBuf,
	TradeCols,
	Trades,
	Ts,
	Usd,
	Venue,
};
pub use v_utils::distributions::LatencyConfig;
