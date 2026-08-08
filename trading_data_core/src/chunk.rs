//! The delta lane's [`Batch`]: what the engine retains for [`Book`](crate::Book) when the book
//! itself is dark.
//!
//! A book's levels are idempotent in `(side, price)` — only the last qty of a period survives — so
//! retaining the rows is retaining ~249× what the period actually says. This folds on arrival and
//! pays depth instead: ~141 levels where the rows are ~35k, over the recorded window.

use core::ops::Range;

use trading_data_dag::{Batch, Flat, Glance, Horizon, Latent};

use crate::{BookDelta, Side, Span, Ts, Venue};

/// The net effect of one period's deltas.
///
/// **Tumbling, not sliding.** [`Rows`](trading_data_dag::Rows) trims by timestamp every tick; a
/// folded level cannot be un-folded, so there is nothing here to trim. It tumbles on the absolute
/// `Horizon::Over` boundary instead — floored from the epoch, the same grid
/// [`ReadClock::cell_end`](crate::ReadClock::cell_end) cuts on.
///
/// Two periods are kept, not one. A boundary is exactly where a sleeping book's reach ends, and the
/// net that would carry it across is the one the tumble throws away — so the tumble sets it aside
/// instead. The reach becomes `[15m, 30m)` rather than `[0, 15m)` for one more level pair and no
/// per-row work; it is a wider bound, not a guarantee, and the guarantee is a replay.
#[derive(Clone, Debug)]
pub struct BookChunk {
	/// Absolute period start the net is measured from; `i64::MIN` until the first row lands.
	base: i64,
	bids: Vec<(i32, u32)>,
	asks: Vec<(i32, u32)>,
	/// The seqs the net folded, first to last. `None` before the period's first row.
	seq: Option<Range<u64>>,
	/// Whether those seqs ran unbroken. A break is a hole in our own recording, and a net missing
	/// rows cannot stand in for them — see [`Book::step`](crate::Book::step).
	contiguous: bool,
	span: Span<Venue>,
	fresh: Vec<BookDelta>,
	/// The period before the one `base` names, kept whole. The tumble is precisely where a sleeping
	/// book loses its reach, so what it needs on waking is the net thrown away there; setting it
	/// aside costs one period's levels and nothing per row.
	prev_bids: Vec<(i32, u32)>,
	prev_asks: Vec<(i32, u32)>,
	prev_seq: Option<Range<u64>>,
	prev_contiguous: bool,
}

impl Default for BookChunk {
	fn default() -> Self {
		Self {
			base: i64::MIN,
			bids: Vec::new(),
			asks: Vec::new(),
			seq: None,
			contiguous: true,
			span: Span::default(),
			fresh: Vec::new(),
			prev_bids: Vec::new(),
			prev_asks: Vec::new(),
			prev_seq: None,
			prev_contiguous: true,
		}
	}
}

impl BookChunk {
	/// This tick's rows — the incremental path, for a book that never went dark.
	pub fn fresh(&self) -> &[BookDelta] {
		&self.fresh
	}

	/// The period's net levels, within one period in no meaningful order: they are absolute sets on
	/// distinct keys, so applying them in any order lands in the same book. `joined` prepends the
	/// period before, and *that* order is load-bearing — a key touched in both must end at the later
	/// value.
	pub fn net(&self, joined: bool) -> impl Iterator<Item = (Side, i32, u32)> + '_ {
		let side = |s: Side| move |&(p, q): &(i32, u32)| (s, p, q);
		let prev = joined
			.then(|| self.prev_bids.iter().map(side(Side::Buy)).chain(self.prev_asks.iter().map(side(Side::Sell))))
			.into_iter()
			.flatten();
		prev.chain(self.bids.iter().map(side(Side::Buy))).chain(self.asks.iter().map(side(Side::Sell)))
	}

	/// The seqs a two-period net speaks for, `None` unless the previous period meets this one and
	/// both ran unbroken: a net missing rows cannot stand in for them, and neither can two nets that
	/// do not join.
	pub fn joined(&self) -> Option<Range<u64>> {
		let (prev, cur) = (self.prev_seq.clone()?, self.seq.clone()?);
		(self.prev_contiguous && self.contiguous && prev.end + 1 == cur.start).then_some(prev.start..cur.end)
	}

	/// What the retention actually costs: levels, not rows.
	pub fn levels(&self) -> usize {
		self.bids.len() + self.asks.len() + self.prev_bids.len() + self.prev_asks.len()
	}

	pub fn seq(&self) -> Option<Range<u64>> {
		self.seq.clone()
	}

	pub fn contiguous(&self) -> bool {
		self.contiguous
	}

	pub fn span(&self) -> Span<Venue> {
		self.span
	}

	fn reset(&mut self, base: i64) {
		self.base = base;
		// swapped rather than taken, so the two periods ping-pong one pair of allocations between them
		core::mem::swap(&mut self.bids, &mut self.prev_bids);
		core::mem::swap(&mut self.asks, &mut self.prev_asks);
		self.prev_seq = self.seq.take();
		self.prev_contiguous = self.contiguous;
		self.bids.clear();
		self.asks.clear();
		self.contiguous = true;
		self.span = Span::default();
	}

	fn fold(&mut self, d: &BookDelta) {
		let levels = match d.side {
			Side::Buy => &mut self.bids,
			Side::Sell => &mut self.asks,
		};
		match levels.binary_search_by_key(&d.price, |&(p, _)| p) {
			Ok(j) => levels[j].1 = d.qty,
			Err(j) => levels.insert(j, (d.price, d.qty)),
		}
		self.seq = match self.seq.take() {
			Some(r) => {
				self.contiguous &= d.monotonic_seq == r.end + 1;
				self.span = Span::new(self.span.first, d.ts_venue_exec);
				Some(r.start..d.monotonic_seq)
			}
			None => {
				self.span = Span::at(d.ts_venue_exec);
				Some(d.monotonic_seq..d.monotonic_seq)
			}
		};
	}
}

/// Reads `fresh` only, exactly as a [`Hist`](trading_data_dag::Hist) does: retention adds no
/// signal, so a buffer's [`Fire`](trading_data_dag::Fire) is the series it retains.
impl Flat for &BookChunk {
	const DIMS: &'static [usize] = <BookDelta as Flat>::DIMS;

	fn flat(&self, out: &mut [f64]) -> bool {
		self.fresh().flat(out)
	}

	fn fires(&self) -> usize {
		self.fresh.len()
	}
}

/// The reading of a retention that was not stepped: `graph!` publishes this where every consumer of
/// the buffer is anchored and every one of them is dark, so nobody is there to read it. What makes
/// the rows it did not fold recoverable is the replay, not this — this only has to be honest about
/// having folded nothing.
impl Latent for &BookChunk {
	fn latent() -> Self {
		static UNSTEPPED: BookChunk = BookChunk {
			base: i64::MIN,
			bids: Vec::new(),
			asks: Vec::new(),
			seq: None,
			contiguous: true,
			span: Span {
				first: Ts::from_nanos(0),
				last: Ts::from_nanos(0),
			},
			fresh: Vec::new(),
			prev_bids: Vec::new(),
			prev_asks: Vec::new(),
			prev_seq: None,
			prev_contiguous: true,
		};
		&UNSTEPPED
	}
}

/// The one thing only the chunk can say: what the retention actually costs.
impl Glance for &BookChunk {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{} lvls net", self.levels())
	}
}

impl Batch<BookDelta> for BookChunk {
	type View<'t> = &'t BookChunk;

	fn advance(&mut self, fresh: &[BookDelta], h: Horizon) {
		let period = match h {
			Horizon::Over(tf) => Horizon::ns(tf),
			h => panic!("a book chunk tumbles on a period boundary, which only Horizon::Over names: {h:?}"),
		};
		self.fresh.clear();
		self.fresh.extend_from_slice(fresh);
		for d in fresh {
			let base = d.ts_venue_exec.as_nanos().div_euclid(period) * period;
			if base != self.base {
				self.reset(base);
			}
			self.fold(d);
		}
	}

	fn view(&self, _: Horizon) -> &BookChunk {
		self
	}

	/// A chunk has one meaning, so a shallower read of it is the same read — where a [`Hist`] cuts
	/// to the consumer's reach, the net of a period *is* the period.
	fn narrow<'t>(view: &'t BookChunk, _: Horizon) -> &'t BookChunk
	where
		Self: 't, {
		view
	}

	fn stage(&mut self, view: &BookChunk, _: Option<usize>, _: f64) -> f64 {
		self.clone_from(view);
		0.0 // perturbing a level makes it a different book, not a nearby one
	}
}
