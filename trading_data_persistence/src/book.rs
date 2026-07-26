use std::collections::BTreeMap;

use jiff::Timestamp;
use trading_data_core::{PrecisionPriceQty, Timestamped};

/// (price, qty) levels for both sides of an orderbook, keyed by raw price.
/// Both BTreeMaps are ascending; consumers reverse `bids` for best-bid.
#[derive(Clone, Debug, Default)]
pub struct BookShape {
	/// Exchange-provided event time.
	pub ts_event: Timestamp,
	/// When we first received the data backing this shape.
	pub ts_init: Timestamp,
	/// When we last wrote into this shape. Equals `ts_init` for shapes built from a single message.
	pub ts_last: Timestamp,
	pub prec: PrecisionPriceQty,
	pub asks: BTreeMap<i32, u32>,
	pub bids: BTreeMap<i32, u32>,
}

impl Timestamped for BookShape {
	fn ts_event(&self) -> Timestamp {
		self.ts_event
	}

	fn ts_init(&self) -> Timestamp {
		self.ts_init
	}

	fn ts_last(&self) -> Timestamp {
		self.ts_last
	}
}

/// Distinguishes full snapshots from incremental deltas.
/// For deltas: qty=0 means remove that price level.
#[derive(Clone, Debug)]
pub enum BookUpdate {
	Snapshot(BookShape),
	/// `gapped` is `true` when the originating WS event broke the per-pair sequence chain.
	BatchDelta {
		shape: BookShape,
		gapped: bool,
	},
}

impl BookUpdate {
	pub fn shape(&self) -> &BookShape {
		match self {
			Self::Snapshot(s) | Self::BatchDelta { shape: s, .. } => s,
		}
	}
}

impl Timestamped for BookUpdate {
	fn ts_event(&self) -> Timestamp {
		self.shape().ts_event
	}

	fn ts_init(&self) -> Timestamp {
		self.shape().ts_init
	}

	fn ts_last(&self) -> Timestamp {
		self.shape().ts_last
	}
}
