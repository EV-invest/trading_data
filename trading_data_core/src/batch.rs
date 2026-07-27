use crate::{Aggregate, Local, PrecisionPriceQty, Side, Span, Timestamped, Timestamps, Ts, Venue};

/// One trade in raw (scaled-int) form; `price`/`qty` are meaningless without the batch's `prec`.
#[derive(Clone, Copy, Debug, Default)]
pub struct InnerTrade {
	/// When the venue matched it.
	pub time: Ts<Venue>,
	/// When the venue put it on the wire, if it reports that separately from `time`. The gap is the
	/// venue's own internal latency — ours to observe, not to average away.
	pub sent: Option<Ts<Venue>>,
	pub price: i32,
	/// Base qty.
	pub qty: u32,
	/// Aggressor (taker) side.
	pub side: Side,
}

/// Batched trade stream event. All trades share `prec`.
///
/// A container, not an accumulator: it is stamped **once** on reception, so there is no local
/// span to speak of. The venue span is derived from the elements, which keep their own times.
#[derive(Clone, Debug, Default)]
pub struct BatchTrades {
	prec: PrecisionPriceQty,
	trades: Vec<InnerTrade>,
	local_recv: Ts<Local>,
}

impl BatchTrades {
	pub fn new(prec: PrecisionPriceQty, trades: Vec<InnerTrade>, local_recv: Ts<Local>) -> Self {
		assert!(!trades.is_empty(), "BatchTrades is never empty by construction");
		// Drained across multiple WS frames, so venue-time monotonicity is an assumption, not a
		// guarantee — and `span` reads only the ends.
		assert!(trades.is_sorted_by_key(|t| t.time), "BatchTrades must be sorted by venue event time");
		Self { prec, trades, local_recv }
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

	pub fn span(&self) -> Span<Venue> {
		Span::new(self.trades.first().expect("never empty").time, self.trades.last().expect("never empty").time)
	}

	/// Raw ints; decode them with [`Self::prec`].
	pub fn iter(&self) -> impl Iterator<Item = InnerTrade> + '_ {
		self.trades.iter().copied()
	}
}

impl Timestamped for BatchTrades {
	fn timestamps(&self) -> Timestamps {
		Timestamps::Aggregate(Aggregate {
			venue_exec: self.span(),
			local_recv: Span::at(self.local_recv),
		})
	}
}
