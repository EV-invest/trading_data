use std::collections::BTreeMap;

use trading_data_dag::{Flat, Glance};

use crate::{Aggregate, DeltaBuf, DeltaFrame, Exact, FrameKind, Local, PrecisionPriceQty, Price, Qty, Side, Span, Timestamped, Timestamps, Ts, Venue};

/// (price, qty) levels for both sides of an orderbook, keyed by raw price.
/// The wire/persist shape. Both BTreeMaps are ascending; [`Book`] holds the same levels best-first.
#[derive(Clone, Debug, Default)]
pub struct BookShape {
	/// Both `first`s are the start of the **current accumulation epoch**: they reset on snapshot
	/// resync, so time-since-resync — how much folded drift this book carries — is readable off
	/// the shape.
	pub ts: Aggregate,
	pub prec: PrecisionPriceQty,
	pub asks: BTreeMap<i32, u32>,
	pub bids: BTreeMap<i32, u32>,
}

impl Timestamped for BookShape {
	fn timestamps(&self) -> Timestamps {
		Timestamps::Accumulator {
			venue: self.ts.venue_exec,
			local: Some(self.ts.local_recv),
		}
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

/// The book fold. Domain-only — it knows nothing of `Cell`/`Node`; the graph node wraps *this*
/// through [`Book::step`], and [`ShadowBook`] wraps the same instance, so the persisted stream and
/// the replayed one cannot drift.
///
/// Both sides are sorted **best-first** — bids descending, asks ascending — in contiguous storage:
/// at the depth a lane carries, the memmove of an insert costs less than a B-tree's descent.
#[derive(Clone, Debug, Default)]
pub struct Book {
	prec: PrecisionPriceQty,
	bids: Vec<(i32, u32)>,
	asks: Vec<(i32, u32)>,
	/// Bumped by every resync: a consumer can tell "same book, more levels" from "a different book".
	epoch: u64,
	synced: bool,
	seq: Option<u64>,
	span: Span<Venue>,
}

impl Book {
	pub fn best_bid(&self) -> Option<(Price, Qty)> {
		self.bids.first().map(|&(p, q)| self.level(p, q))
	}

	pub fn best_ask(&self) -> Option<(Price, Qty)> {
		self.asks.first().map(|&(p, q)| self.level(p, q))
	}

	fn level(&self, price: i32, qty: u32) -> (Price, Qty) {
		(Price::new(price, self.prec.price), Qty::new(qty, self.prec.qty))
	}

	pub fn bids(&self) -> &[(i32, u32)] {
		&self.bids
	}

	pub fn asks(&self) -> &[(i32, u32)] {
		&self.asks
	}

	pub fn prec(&self) -> PrecisionPriceQty {
		self.prec
	}

	pub fn epoch(&self) -> u64 {
		self.epoch
	}

	pub fn len(&self) -> usize {
		self.bids.len() + self.asks.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// One fold step, the whole of it: a `monotonic_seq` discontinuity desyncs, a checkpoint reseeds
	/// a book that has no place in the stream (new epoch), and frames fold only while synced.
	/// Returns whether the book is readable afterwards.
	///
	/// One entry point rather than four public verbs — a caller that could `apply` without first
	/// checking `missed` is a caller that can fold onto stale state.
	pub fn step(&mut self, anchor: Option<&BookShape>, frame: DeltaFrame<'_>) -> bool {
		if self.missed(frame.cols().monotonic_seq) {
			self.synced = false;
		}
		// A checkpoint is a seed, not an event. Our own chain is gapless, so while we are synced the
		// checkpoint is the state we already hold — taking it would clone both maps and bump `epoch`
		// for nothing. We take one exactly when we have no place in the stream: unseeded, or desynced
		// by frames that went by while a gate was closed.
		if !self.synced
			&& let Some(s) = anchor
		{
			self.resync(s);
		}
		if self.synced {
			self.apply(frame);
		}
		self.synced
	}

	fn resync(&mut self, s: &BookShape) {
		self.prec = s.prec;
		self.bids.clear();
		self.bids.extend(s.bids.iter().rev().map(|(&p, &q)| (p, q)));
		self.asks.clear();
		self.asks.extend(s.asks.iter().map(|(&p, &q)| (p, q)));
		self.span = s.ts.venue_exec;
		self.epoch += 1;
		self.synced = true;
		// A checkpoint carries no element sequence; the next frame's first seq re-seeds the chain.
		self.seq = None;
	}

	/// A `monotonic_seq` discontinuity: frames were emitted that we did not fold, so whatever we
	/// hold is stale. Same path covers a gated-off episode and an unseeded start.
	fn missed(&self, seq: &[u64]) -> bool {
		let Some(&first) = seq.first() else { return false };
		self.seq.is_some_and(|last| first != last + 1)
	}

	/// Both kinds fold identically — a correction's levels are levels. What the kind decides is
	/// whether a *derivation* downstream may read them as market activity.
	fn apply(&mut self, frame: DeltaFrame<'_>) {
		let cols = frame.cols();
		if cols.is_empty() {
			return;
		}
		assert_eq!(self.prec, cols.prec, "book folded a frame at a different precision");
		for i in 0..cols.len() {
			let side = cols.side[i];
			let levels = match side {
				Side::Buy => &mut self.bids,
				Side::Sell => &mut self.asks,
			};
			// ponytail: sorted Vec wins to ~1k levels; past that, go back to a map.
			debug_assert!(levels.len() <= 1024, "a full-depth feed would make the memmove the wrong trade");
			match (seek(levels, side, cols.price[i]), cols.qty[i]) {
				(Ok(j), 0) => {
					levels.remove(j);
				}
				(Ok(j), q) => levels[j].1 = q,
				// a delete of a level below our window
				(Err(_), 0) => (),
				(Err(j), q) => levels.insert(j, (cols.price[i], q)),
			}
		}
		let exec = cols.exec();
		self.span = Span::new(self.span.first.min(exec[0]), *exec.last().expect("non-empty"));
		self.seq = cols.monotonic_seq.last().copied();
	}

	/// The levels that would carry `self` onto `other`, as raw (price, qty) pairs per side.
	fn diff(&self, other: &BookShape) -> Vec<(Side, i32, u32)> {
		let mut out = Vec::new();
		let mut one = |side: Side, ours: &[(i32, u32)], theirs: &BTreeMap<i32, u32>| {
			for (&p, &q) in theirs {
				if seek(ours, side, p).map(|j| ours[j].1) != Ok(q) {
					out.push((side, p, q));
				}
			}
			for &(p, _) in ours {
				if !theirs.contains_key(&p) {
					out.push((side, p, 0));
				}
			}
		};
		one(Side::Buy, &self.bids, &other.bids);
		one(Side::Sell, &self.asks, &other.asks);
		out
	}

	fn shape(&self, recv: Ts<Local>, epoch_start: Ts<Local>) -> BookShape {
		BookShape {
			ts: Aggregate {
				venue_exec: self.span,
				local_recv: Span::new(epoch_start, recv),
			},
			prec: self.prec,
			bids: self.bids.iter().copied().collect(),
			asks: self.asks.iter().copied().collect(),
		}
	}
}

/// Where `price` sits in a side held best-first: bids descending, asks ascending.
fn seek(levels: &[(i32, u32)], side: Side, price: i32) -> Result<usize, usize> {
	match side {
		Side::Buy => levels.binary_search_by(|&(p, _)| price.cmp(&p)),
		Side::Sell => levels.binary_search_by(|&(p, _)| p.cmp(&price)),
	}
}

/// We persist our own recollection, not the venue's story. A gap or a resync is a fact about our
/// connection; stored raw, every replay would re-derive the reconciliation from whatever venue
/// snapshots happened to land. Reconciled once here, replay is exact and free.
///
/// Venue snapshots are consumed, never emitted — checkpoints are ours, on our cadence.
pub struct ShadowBook {
	book: Book,
	out: DeltaBuf,
	seq: u64,
	cadence: Exact,
	epoch_start: Option<Ts<Local>>,
	last_checkpoint: Option<Ts<Local>>,
}

impl ShadowBook {
	pub fn new(prec: PrecisionPriceQty, cadence: Exact) -> Self {
		Self {
			book: Book { prec, ..Default::default() },
			out: DeltaBuf::new(prec),
			seq: 0,
			cadence,
			epoch_start: None,
			last_checkpoint: None,
		}
	}

	pub fn book(&self) -> &Book {
		&self.book
	}

	/// Our checkpoint, due once per `cadence` on a synced book. Emitted *before* the frames it
	/// precedes are folded downstream, so a replay seeded from it reads a gapless chain.
	pub fn checkpoint(&mut self, recv: Ts<Local>) -> Option<BookShape> {
		if !self.book.synced {
			return None;
		}
		if self.last_checkpoint.is_some_and(|t| recv - t < self.cadence) {
			return None;
		}
		self.last_checkpoint = Some(recv);
		Some(self.book.shape(recv, self.epoch_start.expect("set whenever the book is synced")))
	}

	/// Consume one venue update, emit ours. `None` when the venue told us nothing we did not
	/// already hold — an agreeing snapshot is not an event.
	pub fn ingest(&mut self, u: &BookUpdate, recv: Ts<Local>) -> Option<DeltaFrame<'_>> {
		self.out.clear();
		let (kind, levels) = match u {
			// An unseeded book has no chain to reconcile against, so the venue's own resync is ours.
			BookUpdate::Snapshot(s) if !self.book.synced => {
				self.seed(s, recv);
				return None;
			}
			BookUpdate::Snapshot(s) => (FrameKind::Correction, self.book.diff(s)),
			// A delta before any snapshot *is* the start of the epoch — not a fallback.
			BookUpdate::BatchDelta { shape, .. } if !self.book.synced => {
				self.seed(shape, recv);
				return None;
			}
			BookUpdate::BatchDelta { shape, gapped } => {
				let kind = if *gapped { FrameKind::Correction } else { FrameKind::Update };
				let bids = shape.bids.iter().map(|(&p, &q)| (Side::Buy, p, q));
				(kind, bids.chain(shape.asks.iter().map(|(&p, &q)| (Side::Sell, p, q))).collect())
			}
		};
		if levels.is_empty() {
			return None;
		}
		let exec = u.shape().ts.venue_exec.last;
		for (side, price, qty) in levels {
			self.seq += 1;
			self.out.push(exec, Some(recv), self.seq, kind, side, price, qty);
		}
		let frame = self.out.frame(0..self.out.len());
		self.book.apply(frame);
		Some(frame)
	}

	fn seed(&mut self, s: &BookShape, recv: Ts<Local>) {
		self.book.resync(s);
		self.epoch_start = Some(recv);
		self.last_checkpoint = None;
	}
}

/// A checkpoint reads like the book it seeds.
impl Flat for &BookShape {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		let s = self.prec.price.scale();
		out.copy_from_slice(&[
			self.bids.keys().next_back().map_or(f64::NAN, |&p| p as f64 / s),
			self.asks.keys().next().map_or(f64::NAN, |&p| p as f64 / s),
		]);
		true
	}
}

impl Glance for &BookShape {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "checkpoint {}b/{}a", self.bids.len(), self.asks.len())
	}
}

/// The two prices a book is read for. Unsynced is `None` upstream, which flattens to NaN + unfired.
impl Flat for &Book {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		let px = |l: Option<(Price, Qty)>| l.map_or(f64::NAN, |(p, _)| p.as_f64());
		out.copy_from_slice(&[px(self.best_bid()), px(self.best_ask())]);
		true
	}
}

impl Glance for &Book {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match (self.best_bid(), self.best_ask()) {
			(Some((b, _)), Some((a, _))) => write!(f, "{b}/{a} ({} lvls)", self.len()),
			_ => write!(f, "empty ({} lvls)", self.len()),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Precision;

	const PREC: PrecisionPriceQty = PrecisionPriceQty {
		price: Precision(2),
		qty: Precision(3),
	};
	const CADENCE: Exact = Exact::from_nanos(60_000_000_000);

	fn shape(bids: &[(i32, u32)], asks: &[(i32, u32)], ns: i64) -> BookShape {
		BookShape {
			ts: Aggregate {
				venue_exec: Span::at(Ts::<Venue>::from_nanos(ns)),
				local_recv: Span::at(Ts::<Local>::from_nanos(ns)),
			},
			prec: PREC,
			bids: bids.iter().copied().collect(),
			asks: asks.iter().copied().collect(),
		}
	}

	fn delta(bids: &[(i32, u32)], asks: &[(i32, u32)], ns: i64, gapped: bool) -> BookUpdate {
		BookUpdate::BatchDelta {
			shape: shape(bids, asks, ns),
			gapped,
		}
	}

	fn local(ns: i64) -> Ts<Local> {
		Ts::from_nanos(ns)
	}

	/// One frame as the persisted lane holds it — the kind plus its raw levels and their sequence.
	type Frame = (FrameKind, Vec<(u64, Side, i32, u32)>);

	fn record(sb: &mut ShadowBook, u: &BookUpdate, recv: Ts<Local>, tape: &mut Vec<Frame>) {
		if let Some(f) = sb.ingest(u, recv) {
			let c = f.cols();
			tape.push((f.kind(), (0..c.len()).map(|i| (c.monotonic_seq[i], c.side[i], c.price[i], c.qty[i])).collect()));
		}
	}

	/// The whole point of the layer: the emitted stream is our reconciliation, and folding it from
	/// one of our own checkpoints reproduces the venue's state exactly.
	#[test]
	fn corrections_are_emitted_and_the_emitted_stream_replays_exactly() {
		let mut sb = ShadowBook::new(PREC, CADENCE);
		let mut tape = Vec::new();

		// seed, then ordinary deltas
		record(&mut sb, &BookUpdate::Snapshot(shape(&[(100, 5)], &[(101, 5)], 1)), local(1), &mut tape);
		record(&mut sb, &delta(&[(99, 3)], &[], 2, false), local(2), &mut tape);
		record(&mut sb, &delta(&[], &[(102, 7)], 3, false), local(3), &mut tape);

		let checkpoint = sb.checkpoint(local(3)).expect("first checkpoint is due immediately");
		let from = tape.len();

		// a gapped delta, then a snapshot that disagrees, then more deltas
		record(&mut sb, &delta(&[(98, 1)], &[], 4, true), local(4), &mut tape);
		record(&mut sb, &BookUpdate::Snapshot(shape(&[(100, 5), (99, 9)], &[(101, 5), (102, 7)], 5)), local(5), &mut tape);
		record(&mut sb, &delta(&[(97, 2)], &[], 6, false), local(6), &mut tape);

		assert_eq!(
			tape.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
			[FrameKind::Update, FrameKind::Update, FrameKind::Correction, FrameKind::Correction, FrameKind::Update],
			"a gap and a snapshot disagreement must each surface as a Correction"
		);
		let level = |b: &Book, p: i32| b.bids().iter().find(|l| l.0 == p).map(|l| l.1);
		assert_eq!(level(&sb.book, 98), None, "the correction must have dropped the gapped level the snapshot denies");
		assert_eq!(level(&sb.book, 99), Some(9), "the correction must have carried the snapshot's own value");

		let agreeing = BookUpdate::Snapshot(sb.book.shape(local(7), local(1)));
		assert!(sb.ingest(&agreeing, local(7)).is_none(), "an agreeing snapshot emits nothing");

		// replay: fold the emitted stream from our checkpoint and land on the same book
		let mut replayed = Book::default();
		let mut buf = DeltaBuf::new(PREC);
		for (i, (kind, levels)) in tape[from..].iter().enumerate() {
			buf.clear();
			for &(seq, side, p, q) in levels {
				buf.push(Ts::from_nanos(0), None, seq, *kind, side, p, q);
			}
			let anchor = (i == 0).then_some(&checkpoint);
			assert!(replayed.step(anchor, buf.frame(0..buf.len())), "replay must stay synced");
		}
		assert_eq!(replayed.bids(), sb.book.bids());
		assert_eq!(replayed.asks(), sb.book.asks());
	}

	#[test]
	fn a_missed_frame_desyncs_until_the_next_checkpoint() {
		let mut b = Book::default();
		let anchor = shape(&[(100, 5)], &[(101, 5)], 1);
		let mut buf = DeltaBuf::new(PREC);

		buf.push(Ts::from_nanos(2), None, 1, FrameKind::Update, Side::Buy, 99, 3);
		assert!(b.step(Some(&anchor), buf.frame(0..1)));

		// seq 2 never reached us
		buf.clear();
		buf.push(Ts::from_nanos(3), None, 3, FrameKind::Update, Side::Buy, 98, 1);
		assert!(!b.step(None, buf.frame(0..1)), "a seq discontinuity must desync");
		assert!(!b.bids().iter().any(|l| l.0 == 98), "a desynced book must not fold");

		// the next checkpoint re-arms it, from the checkpoint's state — not the stale one
		let epoch = b.epoch();
		assert!(b.step(Some(&anchor), buf.frame(0..1)));
		assert_eq!(b.epoch(), epoch + 1);
		assert!(!b.bids().iter().any(|l| l.0 == 99), "re-sync must not carry stale levels");
	}
}
