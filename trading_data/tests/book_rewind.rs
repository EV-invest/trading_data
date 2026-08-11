//! The guarantee anchoring exists for: **a demanded book answers on the tick it is demanded.** Not
//! on the next checkpoint, not eventually.
//!
//! `book_gating.rs` is the same fixture without a past — a live feed's reading, where the retained
//! nets are a *bound* on how long a sleep may be and a longer one leaves the node dark until a
//! checkpoint happens to land. That window is what this file says is gone: given a past to seek, a
//! node that slept is replayed forward before its consumer reads it, and what the consumer reads is
//! the book an ungated twin folded row by row — same levels, same epoch, no matter how long the
//! sleep or which resume paid for it.
//!
//! The last test here is the contrast: the *same* graph driven through `tick`, where the past is
//! `Awake` and the old behaviour is what survives.

use book_fixture::{PERIOD, PREC, Read, anchor, read, run};
use trading_data::{Blind, Book, BookAnchors, BookDelta, BookDeltas, BookShape, Buffering, Cell, DepOuts, Gate, Gating, Nudge, Over, Rewound, Side, TradeBuf, TradeCols, Trades, Ts, node};
use v_utils::TF_15MIN;

mod book_fixture;

/// `book_gating.rs`'s fixture, anchored: the shipped `Book` node is ungated, so gating it is this
/// wrapper over the same public fold, and both resolve against the *same* `Buffer<BookDeltas, 15m>`
/// as the ungated twin below.
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
	type Deps = (Gating<Hot>, BookAnchors, Buffering<BookDeltas, Over<{ TF_15MIN }>>);

	const WHY: &'static str = "a book-rewind fixture";

	fn advance<'t>(&'t mut self, (_, a, chunk): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.0.step(a, chunk).then_some(&self.0)
	}
}

/// Open exactly when this step carries a trade — a gate the test body drives without a second root.
#[derive(Clone, Default)]
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
#[node]
impl Blind for Hot {
	type Deps = (Trades,);

	const WHY: &'static str = "a hot-path fixture";

	fn advance<'t>(&'t mut self, (t,): DepOuts<'t, Self>) -> bool {
		!t.is_empty()
	}
}
impl Gate for Hot {}

/// The book's only consumer, behind the same gate.
#[derive(Clone, Copy, Default)]
struct Mid;
impl Cell for Mid {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Mid {
	type Deps = (Gating<Hot>, GatedBook);

	const WHY: &'static str = "a borrowed-root fixture";

	fn advance<'t>(&'t mut self, (_, book): DepOuts<'t, Self>) -> Option<f64> {
		let b = book?;
		let (bid, ask) = (b.best_bid()?, b.best_ask()?);
		Some((bid.0.as_f64() + ask.0.as_f64()) / 2.0)
	}
}

trading_data::graph! {
	pub struct G;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[BookDelta], anchors: BookAnchors[BookShape] };
	out GOut;
	outputs { mid: Mid, book: GatedBook, twin: trading_data::Book }
}

/// The test's stand-in for `trading_data::Past`: what the driver has already handed the graph, and
/// nothing else — grown only *after* a tick, which is the same truncation the real one gets from the
/// weaver's pre-tick cursors.
///
/// Which resume it takes is a cost question the real one decides by counting rows; here it is a knob,
/// so both paths are exercised on the same tape rather than whichever the tape happened to make
/// cheaper.
#[derive(Default)]
struct Tape {
	deltas: Vec<BookDelta>,
	anchor: Option<BookShape>,
	/// take the checkpoint resume even where the node's own cursor is nearer
	seek: bool,
	wakes: usize,
}

impl Tape {
	fn resume(&mut self, b: &mut Book) {
		let Some(need) = self.deltas.last() else { return };
		if b.seq() == Some(need.monotonic_seq) {
			return; // awake last tick
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

/// The ungated twin is an output, so nothing ever suppresses it and this only ever early-outs. The
/// bound is the graph's to state either way — and `wakes` counting it is what says the twin really
/// did fold every row itself.
impl Rewound<Book> for Tape {
	fn rewind(&mut self, n: &mut Book) {
		self.resume(n);
	}
}

fn batch<'t>(trades: &'t mut TradeBuf, hot: bool, a: Option<&'t BookShape>, d: &'t [BookDelta]) -> Batches<'t> {
	trades.clear();
	trades.prec = PREC;
	if hot {
		trades.push(Ts::from_nanos(1), None, None, 0, Side::Buy, 1, 1);
	}
	Batches {
		trades: trades.cols(0..trades.len()),
		deltas: d,
		anchors: a,
	}
}

/// One tick with a past. The tape grows *after* the graph has stepped: what a rewind may read is
/// what earlier ticks delivered, and this tick's own batch is the sweep's to fold.
fn tick(g: &mut G, tape: &mut Tape, trades: &mut TradeBuf, hot: bool, a: Option<&BookShape>, d: &[BookDelta]) -> (Read, Read, Option<f64>) {
	let out = g.tick_rewind(0, batch(trades, hot, a, d), tape);
	let seen = (read(*out.book), read(*out.twin), *out.mid);
	tape.deltas.extend_from_slice(d);
	if let Some(a) = a {
		tape.anchor = Some(a.clone());
	}
	seen
}

/// Seeded, then dark across three tumbles — twice past what the two retained nets can join between
/// them, and long past the checkpoint that arrived with the seed.
fn slept_a_long_time(g: &mut G, tape: &mut Tape, trades: &mut TradeBuf) -> (Read, Read, Option<f64>) {
	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	tick(g, tape, trades, true, Some(&seed), &run(100, &[(1, Side::Buy, 9_995, 7)]));

	tick(g, tape, trades, false, None, &run(200, &[(2, Side::Sell, 10_005, 2)]));
	tick(g, tape, trades, false, None, &run(PERIOD + 100, &[(3, Side::Buy, 9_998, 9)]));
	tick(g, tape, trades, false, None, &run(2 * PERIOD + 100, &[(4, Side::Sell, 10_004, 1)]));
	tick(g, tape, trades, false, None, &run(3 * PERIOD + 100, &[(5, Side::Buy, 9_999, 4)]));

	tick(g, tape, trades, true, None, &run(3 * PERIOD + 200, &[(6, Side::Sell, 10_003, 8)]))
}

#[test]
fn a_demanded_book_answers_on_that_tick_however_long_it_slept() {
	let (mut g, mut tape, mut trades) = (G::default(), Tape::default(), TradeBuf::new(PREC));

	let (gated, twin, mid) = slept_a_long_time(&mut g, &mut tape, &mut trades);
	assert_eq!(gated, twin, "the book a consumer demanded is the book, whatever went by while nobody wanted it");
	assert_eq!(gated.map(|(e, _)| e), Some(1), "a replay reconstructs a book, it does not replace one");
	assert_eq!(mid, Some((99.99 + 100.03) / 2.0), "and the consumer read it on this tick, not on some later one");
	assert_eq!(tape.wakes, 1, "one wake, so the equality above is not a book that never slept");
}

/// The same tape down the other resume. A long sleep is what makes the checkpoint the cheap one, and
/// it must land on the same book — a resume is a cost decision and nothing more.
#[test]
fn the_checkpoint_resume_lands_on_the_same_book_as_the_cursor_one() {
	let (mut g, mut trades) = (G::default(), TradeBuf::new(PREC));
	let mut tape = Tape { seek: true, ..Default::default() };

	let (gated, twin, mid) = slept_a_long_time(&mut g, &mut tape, &mut trades);
	assert_eq!(gated, twin);
	assert_eq!(gated.map(|(e, _)| e), Some(1), "rebuilt off a checkpoint is still not a new book");
	assert_eq!(mid, Some((99.99 + 100.03) / 2.0));
	assert_eq!(tape.wakes, 1);
}

/// The contrast, and what a live feed still gets: `tick` drives the sweep with `Awake`, which never
/// rewinds — so the node is pinned awake and folds every row, and the reading is the one
/// `book_gating.rs` pins.
#[test]
fn under_awake_the_node_is_pinned_and_folds_every_row() {
	let (mut g, mut trades) = (G::default(), TradeBuf::new(PREC));

	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	g.tick(0, batch(&mut trades, true, Some(&seed), &run(100, &[(1, Side::Buy, 9_995, 7)])));
	for d in [
		run(200, &[(2, Side::Sell, 10_005, 2)]),
		run(PERIOD + 100, &[(3, Side::Buy, 9_998, 9)]),
		run(2 * PERIOD + 100, &[(4, Side::Sell, 10_004, 1)]),
		run(3 * PERIOD + 100, &[(5, Side::Buy, 9_999, 4)]),
	] {
		g.tick(0, batch(&mut trades, false, None, &d));
	}

	// its own `Gating<Hot>` still darkened it, and no past means no seek: the sleep outran the two
	// nets, so the book declines rather than fold onto stale state.
	let woke = run(3 * PERIOD + 200, &[(6, Side::Sell, 10_003, 8)]);
	let out = g.tick(0, batch(&mut trades, true, None, &woke));
	assert_eq!((read(*out.book), *out.mid), (None, None), "without a past, a sleep past the nets' reach is still a dark book");
}
