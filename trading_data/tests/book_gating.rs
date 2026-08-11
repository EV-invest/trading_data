//! The book as a gateable stateful node. Four claims, in order:
//!
//! 1. an unseeded book publishes `None` — the fold's own honesty, not a wrapper's;
//! 2. a closed gate leaves the venue frames *untouched* — the node goes dark without pulling its
//!    plain deps, so no checkpoint and no level is read;
//! 3. dark **within one period**, it wakes onto the same book an ungated twin folded row by row,
//!    without a checkpoint and without a new epoch: the retained net covers the whole period, so
//!    there is nothing a sleeping book missed that the net cannot hand it;
//! 4. dark **across one period boundary**, the net it woke into still reaches: the tumble sets the
//!    closing period's net aside rather than dropping it, so the two join over everything missed —
//!    same book as the twin, same epoch, and no checkpoint needed;
//! 5. dark past **two** boundaries, the join no longer reaches, so it declines to fold onto stale
//!    state and resyncs off the next checkpoint instead — one new epoch, and onto the twin's book.
//!    That is the bound, not a guarantee: it is a wider reach, and the guarantee is a replay.
//!
//! (3)–(5) are why gating a book is sound where gating a recurrence is not: the engine retains the
//! deltas as a *net*, so a wake costs depth rather than the length of the sleep.

use book_fixture::{PERIOD, PREC, Read, anchor, read, run};
use trading_data::{Blind, Book, BookAnchors, BookDelta, BookDeltas, BookShape, Buffering, Cell, DepOuts, Gate, Gating, Nudge, Over, Side, TradeBuf, TradeCols, Trades, Ts, node};
use v_utils::TF_15MIN;

mod book_fixture;

/// `Node::Deps` is fixed on the impl, so the shipped `Book` node is the ungated one; gating it is
/// this wrapper over the same public `Book::step` fold. The two cannot drift — and both resolve
/// against the *same* `Buffer<BookDeltas, 15m>` in the frame, which is what makes the twin below a
/// fair comparison rather than a second recording.
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
#[node]
impl Blind for GatedBook {
	/// The claim under test, and until the deltas became a retained [`Buffering`] it did not compile:
	/// a `Folding` dep on a gated node is the one thing the const-assert refuses.
	type Deps = (Gating<Hot>, BookAnchors, Buffering<BookDeltas, Over<{ TF_15MIN }>>);

	const WHY: &'static str = "a book-gating fixture";

	fn advance<'t>(&'t mut self, (_, a, chunk): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.0.step(a, chunk).then_some(&self.0)
	}
}

/// Open exactly when this step carries a trade — a gate we can drive from the test body without a
/// second root.
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

/// One tick, the gate driven by whether a trade is present. Returns what the gated book and its
/// ungated twin each read, and the consumer's own out.
fn tick(g: &mut G, trades: &mut TradeBuf, hot: bool, a: Option<&BookShape>, d: &[BookDelta]) -> (Read, Read, Option<f64>) {
	trades.clear();
	trades.prec = PREC;
	if hot {
		trades.push(Ts::from_nanos(1), None, None, 0, Side::Buy, 1, 1);
	}
	let out = g.tick(
		0,
		Batches {
			trades: trades.cols(0..trades.len()),
			deltas: d,
			anchors: a,
		},
	);
	(read(*out.book), read(*out.twin), *out.mid)
}

#[test]
fn a_gate_closed_within_one_period_costs_no_resync() {
	let mut g = G::default();
	let mut trades = TradeBuf::new(PREC);

	// 1. gate open, but no checkpoint yet: the fold has nothing to be, and says so.
	let (gated, _, mid) = tick(&mut g, &mut trades, true, None, &run(100, &[(1, Side::Buy, 9_900, 5)]));
	assert_eq!((gated, mid), (None, None));

	// 2. the first checkpoint arms it; the same step's levels fold on top.
	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	let (gated, twin, mid) = tick(&mut g, &mut trades, true, Some(&seed), &run(200, &[(2, Side::Buy, 9_995, 7)]));
	assert_eq!(gated, twin, "gated and ungated read the same buffer, so they read the same book");
	assert_eq!(gated.map(|(e, _)| e), Some(1));
	assert_eq!(mid, Some(100.025), "best bid must be the folded level, not the checkpoint's");

	// 3. gate closed: latent, and the levels go by unread — a closed gate pulls no plain dep.
	assert_eq!(tick(&mut g, &mut trades, false, None, &run(300, &[(3, Side::Sell, 10_005, 2)])).0, None);
	assert_eq!(tick(&mut g, &mut trades, false, None, &run(400, &[(4, Side::Sell, 10_004, 2)])).0, None);
	assert_eq!(tick(&mut g, &mut trades, false, None, &run(500, &[(5, Side::Buy, 9_996, 1)])).0, None);

	// 4. reopened inside the same period, with no checkpoint at all: the retained net still reaches
	//    back over every level that went by, so the book lands where the twin did — same epoch.
	let (gated, twin, mid) = tick(&mut g, &mut trades, true, None, &run(600, &[(6, Side::Buy, 9_997, 2)]));
	assert_eq!(gated, twin, "a sleep inside one period is a sleep the net covers");
	assert_eq!(gated.map(|(e, _)| e), Some(1), "nothing was rebuilt, so nothing is a new epoch");
	assert_eq!(mid, Some(100.005), "best bid and ask are the levels that went by while it slept");
}

#[test]
fn a_gate_closed_across_one_boundary_wakes_off_the_joined_net() {
	let mut g = G::default();
	let mut trades = TradeBuf::new(PREC);

	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	tick(&mut g, &mut trades, true, Some(&seed), &run(100, &[(1, Side::Buy, 9_995, 7)]));

	// dark for the rest of the period, and on past its end — the net tumbles under the sleeping book.
	tick(&mut g, &mut trades, false, None, &run(200, &[(2, Side::Sell, 10_005, 2)]));
	tick(&mut g, &mut trades, false, None, &run(300, &[(3, Side::Buy, 9_998, 9)]));
	tick(&mut g, &mut trades, false, None, &run(PERIOD + 100, &[(4, Side::Sell, 10_004, 1)]));

	// woken with no checkpoint at all: the closing period's net was set aside at the tumble, and it
	// joins this one over exactly the levels that went by.
	let (gated, twin, mid) = tick(&mut g, &mut trades, true, None, &run(PERIOD + 200, &[(5, Side::Buy, 9_999, 4)]));
	assert_eq!(gated, twin, "one boundary is a boundary the two nets span between them");
	assert_eq!(gated.map(|(e, _)| e), Some(1), "nothing was rebuilt, so nothing is a new epoch");
	assert_eq!(mid, Some(100.015));
}

#[test]
fn two_boundaries_are_past_the_reach_and_wait_for_a_checkpoint() {
	let mut g = G::default();
	let mut trades = TradeBuf::new(PREC);

	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	tick(&mut g, &mut trades, true, Some(&seed), &run(100, &[(1, Side::Buy, 9_995, 7)]));

	tick(&mut g, &mut trades, false, None, &run(200, &[(2, Side::Sell, 10_005, 2)]));
	tick(&mut g, &mut trades, false, None, &run(300, &[(3, Side::Buy, 9_998, 9)]));
	let (_, twin, _) = tick(&mut g, &mut trades, false, None, &run(PERIOD + 100, &[(4, Side::Sell, 10_004, 1)]));
	// the checkpoint the recorder writes inside the period that is about to close under the sleeper
	let (bids, asks) = twin.expect("the twin never sleeps").1;
	let checkpoint = anchor(&bids, &asks);

	tick(&mut g, &mut trades, false, None, &run(2 * PERIOD + 100, &[(5, Side::Buy, 9_999, 4)]));

	// 1. woken with no checkpoint: the two retained nets reach back to seq 4, and seqs 2..3 are behind
	//    them. Folding on would fold onto stale state, so it declines instead.
	let (gated, _, mid) = tick(&mut g, &mut trades, true, None, &run(2 * PERIOD + 200, &[(6, Side::Sell, 10_003, 8)]));
	assert_eq!((gated, mid), (None, None), "past the reach and without a seed, a book is not readable");

	// 2. the checkpoint re-arms it: one resync, then this period's net on top — and that is the twin's
	//    book, not an approximation of it.
	let (gated, twin, mid) = tick(&mut g, &mut trades, true, Some(&checkpoint), &run(2 * PERIOD + 300, &[(7, Side::Buy, 9_997, 6)]));
	assert_eq!(
		gated.as_ref().map(|(_, l)| l),
		twin.as_ref().map(|(_, l)| l),
		"checkpoint + net is the whole of what the twin folded"
	);
	assert_eq!(gated.map(|(e, _)| e), Some(2), "exactly one resync, so exactly one new epoch");
	assert!(mid.is_some());
}
