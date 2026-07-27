use std::collections::BTreeMap;

use crate::{Accumulator, PrecisionPriceQty, Timestamped, Timestamps, Ts, Venue};

/// (price, qty) levels for both sides of an orderbook, keyed by raw price.
/// Both BTreeMaps are ascending; consumers reverse `bids` for best-bid.
#[derive(Clone, Debug, Default)]
pub struct BookShape {
	/// `ts.venue.first`/`ts.local.first` are the start of the **current accumulation epoch**: they
	/// reset on snapshot resync, so time-since-resync — how much folded drift this book carries —
	/// is readable off the shape.
	pub ts: Accumulator,
	/// When the venue put the latest contributing update on the wire, if it reports that separately
	/// from the update's own time.
	pub venue_send: Option<Ts<Venue>>,
	pub prec: PrecisionPriceQty,
	pub asks: BTreeMap<i32, u32>,
	pub bids: BTreeMap<i32, u32>,
}

impl Timestamped for BookShape {
	fn timestamps(&self) -> Timestamps {
		Timestamps::Accumulator(self.ts)
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
