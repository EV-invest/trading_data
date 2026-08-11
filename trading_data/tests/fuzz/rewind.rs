//! **Phase 3a, the anchored half.** *A demanded node answers on the tick it is demanded* — not on
//! the next checkpoint, not eventually. An anchored node that slept past everything the retained
//! nets can reach is replayed forward out of the feed's recorded past before its consumer reads it,
//! and what the consumer reads is the book an ungated twin folded row by row.
//!
//! Two metamorphic claims over one generated tape, which is `book_rewind.rs`'s pair of hand-written
//! cases with the tape generated instead of written out:
//!
//! - **however long it slept**, a woken book equals the twin that never slept — same levels, same
//!   epoch. The gate schedule is the fuzzed axis, so the sleep is any length rather than the three
//!   tumbles the fixture happens to spell.
//! - **whichever resume paid for it**, the two land on the same book. A resume is a cost decision
//!   and nothing more: the cursor path and the checkpoint path are run over the *same* tape rather
//!   than over whichever tape happened to make each cheaper.

use trading_data::{
	Aggregate, Blind, Book, BookAnchors, BookDelta, BookDeltas, BookShape, Buffering, Cell, DepOuts, FrameKind, Gate, Gating, Local, Nudge, Over, Precision, PrecisionPriceQty, Rewound,
	Side, Span, TradeBuf, TradeCols, Trades, Ts, Venue, graph, node,
};
use v_utils::TF_15MIN;

use crate::frng::Frng;

pub const VERSION: u32 = crate::corpus::fnv(&[include_str!("frng.rs"), include_str!("rewind.rs")]);

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
/// One tumble of the retained net, in nanoseconds — the grid the checkpoint is written on.
const PERIOD: i64 = 900_000_000_000;
/// Prices stay inside this window either side of the mid, so a run keeps touching levels the book
/// already holds rather than only ever adding new ones.
const DEPTH: i32 = 6;
const MID: i32 = 10_000;

/// The shipped `Book` node is ungated, so gating it is this wrapper over the same public fold — and
/// both resolve against the *same* `Buffer<BookDeltas, 15m>`, which is what makes the twin a fair
/// comparison rather than a second recording.
#[derive(Clone, Default)]
struct GatedBook(Book);
impl Cell for GatedBook {
	type Out<'t> = Option<&'t Book>;
}
impl Nudge for GatedBook {
	type Scratch = Option<Book>;

	fn stage(out: Option<&Book>, s: &mut Self::Scratch, _: Option<usize>, _: f64) -> f64 {
		*s = out.cloned();
		0.0
	}

	fn view(s: &Self::Scratch) -> Option<&Book> {
		s.as_ref()
	}
}
#[node(anchored)]
impl Blind for GatedBook {
	type Deps = (Gating<Wanted>, BookAnchors, Buffering<BookDeltas, Over<{ TF_15MIN }>>);

	const WHY: &'static str = "a rewind fixture";

	fn advance<'t>(&'t mut self, (_, a, chunk): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.0.step(a, chunk).then_some(&self.0)
	}
}

/// Open exactly when this step carries a trade — a gate the fuzzer drives without a second root.
#[derive(Clone, Default)]
struct Wanted;
impl Cell for Wanted {
	type Out<'t> = bool;
}
#[node]
impl Blind for Wanted {
	type Deps = (Trades,);

	const WHY: &'static str = "a hot-path fixture";

	fn advance<'t>(&'t mut self, (t,): DepOuts<'t, Self>) -> bool {
		!t.is_empty()
	}
}
impl Gate for Wanted {}

graph! {
	struct R;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[BookDelta], anchors: BookAnchors[BookShape] };
	out ROut;
	outputs { book: GatedBook, twin: trading_data::Book }
}

/// The test's stand-in for `trading_data::Past`: what the driver has already handed the graph, and
/// nothing else — grown only *after* a tick, which is the same truncation the real one gets from
/// the weaver's pre-tick cursors. `seek` is a knob rather than a cost decision, so both resumes run
/// over the same tape instead of over whichever the tape happened to make cheaper.
#[derive(Default)]
struct Tape {
	deltas: Vec<BookDelta>,
	anchor: Option<BookShape>,
	seek: bool,
	wakes: usize,
}

impl Tape {
	fn resume(&mut self, b: &mut Book) {
		let Some(need) = self.deltas.last() else { return };
		if b.seq() == Some(need.monotonic_seq) {
			return; // awake last tick
		}
		// No place in the chain and no checkpoint delivered yet: there is no past to be replayed to,
		// which is the same thing an unseeded book already says for itself. `book_rewind.rs`'s tape
		// omits this because its trace always seeds first; the production `Past` has it, and a
		// generated trace reaches deltas-before-any-checkpoint on its own.
		if b.seq().is_none() && self.anchor.is_none() {
			return;
		}
		self.wakes += 1;
		// re-folding rows the checkpoint already holds is free — a level is absolute in `(side, price)`
		// and the later row wins — so the seek resume simply takes the whole tape.
		match (self.seek.then(|| self.anchor.as_ref()).flatten(), b.seq()) {
			(Some(a), _) => b.rewind(Some(a), &self.deltas),
			(None, Some(s)) => {
				let k = self.deltas.partition_point(|d| d.monotonic_seq <= s);
				b.rewind(None, &self.deltas[k..]);
			}
			(None, None) => b.rewind(self.anchor.as_ref(), &self.deltas),
		}
	}
}

impl Rewound<GatedBook> for Tape {
	fn rewind(&mut self, n: &mut GatedBook) {
		self.resume(&mut n.0);
	}
}

impl Rewound<Book> for Tape {
	fn rewind(&mut self, n: &mut Book) {
		self.resume(n);
	}
}

/// Both sides as raw levels plus the epoch — the whole of what "the same book" means here.
type Read = Option<(u64, Vec<(i32, u32)>, Vec<(i32, u32)>)>;

fn read(b: Option<&Book>) -> Read {
	b.map(|b| (b.epoch(), b.bids().to_vec(), b.asks().to_vec()))
}

/// One generated tick: whether a trade opens the gate, this tick's delta run, and whether the
/// recorder's checkpoint lands on it.
struct Beat {
	hot: bool,
	deltas: Vec<BookDelta>,
	checkpoint: bool,
}

/// The tape, and the checkpoint it opens with. A real recording always opens with one — the shadow
/// book's first checkpoint on seeding, or the pre-start anchor a replay carries into the range — and
/// without it the book never syncs, so nothing downstream is exercised at all.
fn trace(f: &mut Frng) -> (BookShape, Vec<Beat>) {
	let side = |f: &mut Frng, base: i32| -> Vec<(i32, u32)> { (0..1 + f.below(4)).map(|_| (base + f.below(DEPTH as u32) as i32, 1 + f.below(500))).collect() };
	let seed = shape(&side(f, MID - DEPTH), &side(f, MID + 1), 0);

	let (mut out, mut seq, mut ts) = (Vec::new(), 0u64, 100i64);
	while f.remaining() > 3 {
		// Gaps aimed at and past the tumble: a sleep inside one period is covered by the net, one
		// across a boundary by the two nets joined, and anything longer is what anchoring is for.
		ts += match f.below(4) {
			0 => 1_000,
			1 => PERIOD,
			2 => 2 * PERIOD,
			_ => 1 + f.span(0.0, 3.0 * PERIOD as f64) as i64,
		};
		let n = 1 + f.below(3) as usize;
		let deltas = (0..n)
			.map(|_| {
				seq += 1;
				let (side, base) = if f.byte() % 2 == 0 { (Side::Buy, MID - DEPTH) } else { (Side::Sell, MID + 1) };
				BookDelta {
					prec: PREC,
					ts_venue_exec: Ts::from_nanos(ts),
					ts_local_recv: Ts::from_nanos(ts),
					monotonic_seq: seq,
					kind: FrameKind::Update,
					side,
					price: base + f.below(DEPTH as u32) as i32,
					// a quarter delete, so a wake has to reproduce removals and not only additions
					qty: if f.below(4) == 0 { 0 } else { 1 + f.below(500) },
				}
			})
			.collect();
		out.push(Beat {
			hot: f.below(3) != 0,
			deltas,
			checkpoint: f.below(4) == 0,
		});
	}
	(seed, out)
}

/// A checkpoint the recorder writes: the twin's book as it stands, stamped at the last row it holds.
/// A real anchor is a snapshot of a real state — one built out of arbitrary levels would be a
/// resync onto a book that never existed, and any disagreement it caused would be the fuzzer's.
fn shape(bids: &[(i32, u32)], asks: &[(i32, u32)], at: i64) -> BookShape {
	BookShape {
		ts: Aggregate {
			venue_exec: Span::at(Ts::<Venue>::from_nanos(at)),
			local_recv: Span::at(Ts::<Local>::from_nanos(at)),
		},
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
	}
}

/// Drive the whole tape under one resume. Returns what the gated book and the twin read per tick,
/// and how many wakes were paid for.
fn drive(seed: &BookShape, beats: &[Beat], seek: bool) -> (Vec<(Read, Read)>, usize) {
	let (mut g, mut trades) = (R::default(), TradeBuf::new(PREC));
	let mut tape = Tape { seek, ..Default::default() };
	let (mut out, mut pending): (Vec<(Read, Read)>, Option<BookShape>) = (Vec::new(), Some(seed.clone()));

	for b in beats {
		trades.clear();
		trades.prec = PREC;
		if b.hot {
			trades.push(Ts::from_nanos(1), None, None, 0, Side::Buy, 1, 1);
		}
		let seen = {
			let o = g.tick_rewind(
				0,
				Batches {
					trades: trades.cols(0..trades.len()),
					deltas: &b.deltas,
					anchors: pending.as_ref(),
				},
				&mut tape,
			);
			(read(*o.book), read(*o.twin))
		};
		tape.deltas.extend_from_slice(&b.deltas);
		if let Some(a) = pending.take() {
			tape.anchor = Some(a);
		}
		// the recorder writes its checkpoint off the book as it now stands, and it reaches the graph on
		// the next tick — a checkpoint is an event that arrives, not a value read out of band.
		if b.checkpoint
			&& let (_, Some(twin)) = &seen
		{
			pending = Some(shape(&twin.1, &twin.2, b.deltas.last().map_or(0, |d| d.ts_local_recv.as_nanos())));
		}
		out.push(seen);
	}
	(out, tape.wakes)
}

pub fn run(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let (seed, beats) = trace(f);
	if beats.is_empty() {
		return Ok(());
	}
	let (cursor, cursor_wakes) = drive(&seed, &beats, false);
	let (seeked, seek_wakes) = drive(&seed, &beats, true);

	if verbose {
		eprintln!("{} ticks, {cursor_wakes} cursor wakes, {seek_wakes} seek wakes", beats.len());
		for (i, (b, ((gb, tw), (sb, _)))) in beats.iter().zip(cursor.iter().zip(&seeked)).enumerate() {
			eprintln!(
				"  [{i:>3}] hot={} d={} ck={}\n        gated {:?}\n        twin  {:?}\n        seek  {:?}",
				b.hot,
				b.deltas.len(),
				b.checkpoint,
				gb,
				tw,
				sb
			);
		}
	}

	let mut demanded = 0;
	for (i, (b, ((gated, twin), (seek_gated, seek_twin)))) in beats.iter().zip(cursor.iter().zip(&seeked)).enumerate() {
		// the twin is an output and nothing ever suppresses it, so it is the row-by-row reference on
		// every tick and under either resume.
		check!(
			twin == seek_twin,
			"tick {i}: the ungated twin depends on which resume the gated node took\n  cursor {twin:?}\n  seek   {seek_twin:?}"
		);
		check!(
			gated == seek_gated,
			"tick {i}: the two resumes land on different books — a resume is a cost decision and nothing more\n  cursor {gated:?}\n  seek   {seek_gated:?}"
		);
		if !b.hot {
			check!(gated.is_none(), "tick {i}: a shut gate left a reading behind: {gated:?}");
			continue;
		}
		demanded += 1;
		// **the guarantee anchoring exists for.** Not on the next checkpoint, not eventually.
		check!(
			gated == twin,
			"tick {i}: the book a consumer demanded is not the book it would have folded itself\n  gated {gated:?}\n  twin  {twin:?}"
		);
	}

	check!(demanded > 0 || beats.len() < 4, "the trace never demanded the book, so nothing was replayed forward");
	// How many wakes a run pays for is a property of the schedule, not of which resume answered them.
	check!(
		cursor_wakes == seek_wakes,
		"the two resumes woke a different number of times ({cursor_wakes} vs {seek_wakes}), so they were not driven over the same tape"
	);
	// Whether a run woke *at all* is coverage rather than an invariant: a trace whose gate never
	// shuts legitimately never sleeps, and failing that would be failing the generator for drawing a
	// legal schedule. The evidence that the resume paths are reached is the deliberate-break check —
	// perturb either arm of `Tape::resume` and this target minimizes to a ~30-byte repro.
	Ok(())
}
