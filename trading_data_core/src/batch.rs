use jiff::Timestamp;

use crate::{PrecisionPriceQty, Side, Timestamped};

/// One trade in raw (scaled-int) form; `price`/`qty` are meaningless without the batch's `prec`.
#[derive(Clone, Copy, Debug, Default)]
pub struct InnerTrade {
	pub time: Timestamp,
	pub price: i32,
	/// Base qty.
	pub qty: u32,
	/// Aggressor (taker) side.
	pub side: Side,
}

/// Batched trade stream event. All trades share `prec`.
#[derive(Clone, Debug, Default)]
pub struct BatchTrades {
	prec: PrecisionPriceQty,
	trades: Vec<InnerTrade>,
	/// Exchange-provided event time of the latest trade in the batch.
	ts_event: Timestamp,
	/// When we first received the data backing this batch.
	ts_init: Timestamp,
	/// When we last appended into this batch. Equals `ts_init` for batches built from one message.
	ts_last: Timestamp,
}

impl BatchTrades {
	pub fn new(prec: PrecisionPriceQty, trades: Vec<InnerTrade>, ts_init: Timestamp, ts_last: Timestamp) -> Self {
		assert!(!trades.is_empty(), "BatchTrades is never empty by construction");
		let ts_event = trades.last().expect("never empty").time;
		Self {
			prec,
			trades,
			ts_event,
			ts_init,
			ts_last,
		}
	}

	pub fn len(&self) -> usize {
		self.trades.len()
	}

	pub fn is_empty(&self) -> bool {
		self.trades.is_empty()
	}

	/// The precision shared by every trade in the batch — needed to decode `iter`'s raw ints.
	pub fn prec(&self) -> PrecisionPriceQty {
		self.prec
	}

	/// Iterate `(time, price_raw, qty_raw, side)` tuples. Precision is shared via [`Self::prec`].
	pub fn iter(&self) -> impl Iterator<Item = (Timestamp, i32, u32, Side)> + '_ {
		self.trades.iter().map(|t| (t.time, t.price, t.qty, t.side))
	}
}

impl Timestamped for BatchTrades {
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
