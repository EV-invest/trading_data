use std::collections::BTreeMap;

use trading_data_dag::{Flat, Glance};

use crate::{Aggregate, BookChunk, BookDelta, Exact, FrameKind, Local, PrecisionPriceQty, Price, Qty, Side, Span, Timestamped, Timestamps, Ts, Venue};

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
	fn ts(&self) -> Timestamps {
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

	/// The venue time this fold is *as of* — the newest row it has absorbed, whatever tick it is read
	/// on. A reading taken off the book is stamped with this rather than with the tick.
	pub fn ts(&self) -> Ts<Venue> {
		self.span.last
	}

	/// Where a fold may resume us from: exact, gate-aware and hole-aware, because it is written by the
	/// folds themselves. `None` before the first row, after a resync until the next one, and while
	/// desynced — three states, one answer, because to anyone asking where to resume they are the same
	/// state: we hold no place in the chain.
	pub fn seq(&self) -> Option<u64> {
		self.synced.then_some(self.seq).flatten()
	}

	/// Fold ourselves forward over rows we were not stepped on. `anchor` is a checkpoint to rebuild
	/// from first — the resume that costs depth however long the sleep was; `None` resumes from where
	/// we already stand, which is cheaper only while the sleep was short.
	///
	/// `rows` must be every row the resume point does not already reflect. Re-folding rows it *does*
	/// hold is free — a level is absolute in `(side, price)`, and the later row wins either way — but
	/// skipping one is not.
	///
	/// Not a [`resync`](Self::resync), though it takes the same checkpoint: this reconstructs the
	/// state we would have held anyway, and `epoch` is a claim about *discontinuity*. A book that
	/// arrives where it was always going to arrive has not become a different book.
	pub fn rewind(&mut self, anchor: Option<&BookShape>, rows: &[BookDelta]) {
		if let Some(a) = anchor {
			let (epoch, first, synced) = (self.epoch, self.span.first, self.synced);
			self.resync(a);
			if synced {
				self.epoch = epoch;
				self.span = Span::new(first, self.span.last);
			}
		}
		self.apply(rows);
	}

	pub fn len(&self) -> usize {
		self.bids.len() + self.asks.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// One fold step, the whole of it. Returns whether the book is readable afterwards.
	///
	/// One entry point rather than four public verbs — a caller that could `apply` without first
	/// asking [`Reach`] is a caller that can fold onto stale state.
	pub fn step(&mut self, anchor: Option<&BookShape>, chunk: &BookChunk) -> bool {
		match self.synced.then(|| self.reach(chunk)) {
			// the common case, and as cheap as it ever was: awake last tick, nothing skipped.
			Some(Reach::Fresh) => {
				self.apply(chunk.fresh());
				return true;
			}
			// woke, or the chunk tumbled under us, but the net still reaches back over everything we
			// have not folded — so a wake costs depth, not the length of the sleep.
			Some(Reach::Net { prev }) => {
				self.absorb(chunk, prev);
				return true;
			}
			Some(Reach::Gone) => self.synced = false,
			None => (),
		}
		// A checkpoint is a seed, not an event. Our own chain is gapless, so while we are synced the
		// checkpoint is the state we already hold — taking it would clone both maps and bump `epoch`
		// for nothing. We take one exactly when we have no place in the stream: unseeded, or asleep
		// past what the net covers.
		if let Some(s) = anchor {
			self.resync(s);
			match chunk.contiguous() {
				// The checkpoint stands at or after the period base, and the net's every level is that
				// level's *final* value for the period — so folding it over the checkpoint is idempotent
				// where they overlap and carries it forward where they do not. The period before it is
				// older than the checkpoint, so it says nothing the checkpoint does not say better.
				true => self.absorb(chunk, false),
				// A hole in our own recording: the net cannot speak for rows it never saw, and the
				// checkpoint can. Take only what this tick carries, and let the chain re-seed from it.
				false => self.apply(chunk.fresh()),
			}
		}
		self.synced
	}

	/// How far this chunk reaches back towards what we hold.
	fn reach(&self, chunk: &BookChunk) -> Reach {
		let Some(seq) = chunk.seq() else { return Reach::Fresh }; // nothing has been folded, ever
		let Some(last) = self.seq else {
			// No place in the chain — a resync leaves us there. The only run we may accept is the one
			// that opens the chunk's own period; anything later means rows went by while we were dark.
			return match chunk.fresh().first() {
				Some(f) if f.monotonic_seq == seq.start => Reach::Fresh,
				None => Reach::Fresh,
				Some(_) => Reach::Gone,
			};
		};
		if chunk.fresh().first().is_none_or(|f| f.monotonic_seq == last + 1) {
			return Reach::Fresh;
		}
		if last > seq.end {
			return Reach::Gone; // ahead of the net, which can only mean the chain forked
		}
		// A hole in our own recording is a hole in the net too: it cannot stand in for rows it never
		// saw, however well its seqs bracket ours.
		if chunk.contiguous() && last + 1 >= seq.start {
			return Reach::Net { prev: false };
		}
		match chunk.joined().is_some_and(|j| last + 1 >= j.start) {
			true => Reach::Net { prev: true },
			false => Reach::Gone,
		}
	}

	/// Take the period's net, wholesale. O(depth), and independent of how long we were away.
	fn absorb(&mut self, chunk: &BookChunk, prev: bool) {
		let Some(seq) = chunk.seq() else { return };
		for (side, price, qty) in chunk.net(prev) {
			self.set(side, price, qty);
		}
		self.span = self.span.including(chunk.span());
		self.seq = Some(seq.end);
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

	/// Both kinds fold identically — a correction's levels are levels. What the kind decides is
	/// whether a *derivation* downstream may read them as market activity.
	fn apply(&mut self, rows: &[BookDelta]) {
		assert!(self.synced, "a desynced book must not fold");
		let (Some(first), Some(last)) = (rows.first(), rows.last()) else { return };
		for d in rows {
			assert_eq!(self.prec, d.prec, "book folded a level at a different precision");
			self.set(d.side, d.price, d.qty);
		}
		self.span = self.span.including(Span::new(first.ts_venue_exec, last.ts_venue_exec));
		self.seq = Some(last.monotonic_seq);
	}

	/// One level, absolutely: `qty == 0` deletes. Idempotent in the price key, which is what lets a
	/// period's *net* stand in for the rows that produced it.
	fn set(&mut self, side: Side, price: i32, qty: u32) {
		let levels = match side {
			Side::Buy => &mut self.bids,
			Side::Sell => &mut self.asks,
		};
		// ponytail: sorted Vec wins to ~1k levels; past that, go back to a map.
		debug_assert!(levels.len() <= 1024, "a full-depth feed would make the memmove the wrong trade");
		match (seek(levels, side, price), qty) {
			(Ok(j), 0) => {
				levels.remove(j);
			}
			(Ok(j), q) => levels[j].1 = q,
			// a delete of a level below our window
			(Err(_), 0) => (),
			(Err(j), q) => levels.insert(j, (price, q)),
		}
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

/// What a [`BookChunk`] can do for the state a [`Book`] holds.
enum Reach {
	/// Fold this tick's rows on top, one by one.
	Fresh,
	/// Fold the whole period's net on top, and `prev` the one before it — which is what a sleep that
	/// crossed a boundary needs, and all it needs.
	Net { prev: bool },
	/// Neither: rows we never folded are behind what the net covers, so we are stale.
	///
	/// A feed that can be sought never gets here — the rewind stands the book on the row before this
	/// tick's first, which is [`Fresh`](Reach::Fresh) by construction. This is the branch of a feed
	/// with no past: live, where the recovery is the next checkpoint and there is nothing else it
	/// could be.
	Gone,
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
	out: Vec<BookDelta>,
	seq: u64,
	cadence: Exact,
	epoch_start: Option<Ts<Local>>,
	/// The `cadence` period a checkpoint has already been written in, floored from the epoch.
	checkpointed: Option<i64>,
}

impl ShadowBook {
	pub fn new(prec: PrecisionPriceQty, cadence: Exact) -> Self {
		Self {
			book: Book { prec, ..Default::default() },
			out: Vec::new(),
			seq: 0,
			cadence,
			epoch_start: None,
			checkpointed: None,
		}
	}

	pub fn book(&self) -> &Book {
		&self.book
	}

	/// Our checkpoint, due once per `cadence` *period* on a synced book — the boundary floored from
	/// the epoch, not the time elapsed since the last one. Absolute because the reader's own
	/// retention tumbles on that same grid: a book waking mid-period needs a checkpoint taken inside
	/// it, and a relative cadence only promises one within a cadence of *some* earlier write.
	///
	/// Still emitted on arrival and never batched — this fires from `Live`'s ingest of a single
	/// message, so the first arrival past a boundary carries it.
	pub fn checkpoint(&mut self, recv: Ts<Local>) -> Option<BookShape> {
		if !self.book.synced {
			return None;
		}
		let period = self.cadence.as_nanos();
		assert!(period > 0, "a checkpoint cadence is a period, got {period}ns");
		let base = recv.as_nanos().div_euclid(period);
		if self.checkpointed == Some(base) {
			return None;
		}
		self.checkpointed = Some(base);
		Some(self.book.shape(recv, self.epoch_start.expect("set whenever the book is synced")))
	}

	/// Consume one venue update, emit ours. `None` when the venue told us nothing we did not
	/// already hold — an agreeing snapshot is not an event.
	pub fn ingest(&mut self, u: &BookUpdate, recv: Ts<Local>) -> Option<&[BookDelta]> {
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
			self.out.push(BookDelta {
				prec: self.book.prec,
				ts_venue_exec: exec,
				ts_local_recv: recv,
				monotonic_seq: self.seq,
				kind,
				side,
				price,
				qty,
			});
		}
		self.book.apply(&self.out);
		Some(&self.out)
	}

	fn seed(&mut self, s: &BookShape, recv: Ts<Local>) {
		self.book.resync(s);
		self.epoch_start = Some(recv);
		// a fresh epoch is a fresh book, and the period it opens in has no checkpoint of *this* book
		self.checkpointed = None;
	}
}

/// A checkpoint reads like the book it seeds.
impl Flat for &BookShape {
	/// A side can be empty while the book itself stands, and each price is its own slot.
	const ABSENTABLE: bool = true;
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
	/// One-sided is not unsynced: the book stands, and the side that is empty has no top.
	const ABSENTABLE: bool = true;
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
	use trading_data_dag::{Batch as _, Horizon};
	use v_utils::TF_15MIN;

	use super::*;
	use crate::Precision;

	/// The engine's side of a step: the frame's buffer folds the run into its chunk, then the node
	/// reads it. Driving [`Book::step`] any other way would test a path the graph does not take.
	fn step(b: &mut Book, chunk: &mut BookChunk, anchor: Option<&BookShape>, rows: &[BookDelta]) -> bool {
		chunk.advance(rows, Horizon::Over(TF_15MIN));
		b.step(anchor, chunk)
	}

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

	fn row(seq: u64, kind: FrameKind, side: Side, price: i32, qty: u32) -> BookDelta {
		BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(0),
			ts_local_recv: Ts::from_nanos(0),
			monotonic_seq: seq,
			kind,
			side,
			price,
			qty,
		}
	}

	fn record(sb: &mut ShadowBook, u: &BookUpdate, recv: Ts<Local>, tape: &mut Vec<Vec<BookDelta>>) {
		if let Some(f) = sb.ingest(u, recv) {
			tape.push(f.to_vec());
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
			tape.iter().map(|f| f[0].kind).collect::<Vec<_>>(),
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
		let mut chunk = BookChunk::default();
		for (i, levels) in tape[from..].iter().enumerate() {
			let anchor = (i == 0).then_some(&checkpoint);
			assert!(step(&mut replayed, &mut chunk, anchor, levels), "replay must stay synced");
		}
		assert_eq!(replayed.bids(), sb.book.bids());
		assert_eq!(replayed.asks(), sb.book.asks());
	}

	/// The whole of what an anchored node's sleep is worth: whichever resume it takes, it arrives at
	/// the book it would have folded to, and does not announce itself as a different one.
	#[test]
	fn a_rewind_lands_where_the_fold_would_have_and_calls_it_the_same_book() {
		let seed = shape(&[(100, 5)], &[(101, 5)], 1);
		let rows = [
			row(1, FrameKind::Update, Side::Buy, 99, 3),
			row(2, FrameKind::Update, Side::Sell, 102, 4),
			row(3, FrameKind::Update, Side::Buy, 100, 0),
			row(4, FrameKind::Update, Side::Sell, 101, 9),
			row(5, FrameKind::Correction, Side::Buy, 99, 7),
			row(6, FrameKind::Update, Side::Sell, 102, 0),
		];

		let mut awake = Book::default();
		assert!(step(&mut awake, &mut BookChunk::default(), Some(&seed), &rows));

		// a checkpoint of the book at seq 4 — the anchor resume overshoots it on purpose, so the test
		// says that re-folding rows the checkpoint already holds is free rather than merely untried.
		let mut at4 = Book::default();
		assert!(step(&mut at4, &mut BookChunk::default(), Some(&seed), &rows[..4]));
		let checkpoint = at4.shape(local(0), local(0));

		for anchor in [None, Some(&checkpoint)] {
			let mut slept = Book::default();
			assert!(step(&mut slept, &mut BookChunk::default(), Some(&seed), &rows[..2]));
			let epoch = slept.epoch();

			slept.rewind(anchor, &rows[2..]);
			assert_eq!((slept.bids(), slept.asks()), (awake.bids(), awake.asks()));
			assert_eq!(slept.seq(), awake.seq(), "a rewound book stands where the awake one stands");
			assert_eq!(slept.epoch(), epoch, "a rewind reconstructs a book, it does not replace one");
		}
	}

	#[test]
	fn a_missed_frame_desyncs_until_the_next_checkpoint() {
		let mut b = Book::default();
		let anchor = shape(&[(100, 5)], &[(101, 5)], 1);
		let mut chunk = BookChunk::default();

		assert!(step(&mut b, &mut chunk, Some(&anchor), &[row(1, FrameKind::Update, Side::Buy, 99, 3)]));

		// seq 2 never reached us
		let gapped = [row(3, FrameKind::Update, Side::Buy, 98, 1)];
		assert!(!step(&mut b, &mut chunk, None, &gapped), "a seq discontinuity must desync");
		assert!(!b.bids().iter().any(|l| l.0 == 98), "a desynced book must not fold");

		// the next checkpoint re-arms it, from the checkpoint's state — not the stale one
		let epoch = b.epoch();
		assert!(step(&mut b, &mut chunk, Some(&anchor), &[]));
		assert_eq!(b.epoch(), epoch + 1);
		assert!(!b.bids().iter().any(|l| l.0 == 99), "re-sync must not carry stale levels");
	}
}
