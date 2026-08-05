//! The book as a gateable stateful node. Four claims, in order:
//!
//! 1. an unseeded book publishes `None` — the fold's own honesty, not a wrapper's;
//! 2. a closed gate leaves the venue frames *untouched* — the node goes dark without pulling its
//!    plain deps, so no checkpoint and no level is read;
//! 3. dark **within one period**, it wakes onto the same book an ungated twin folded row by row,
//!    without a checkpoint and without a new epoch: the retained net covers the whole period, so
//!    there is nothing a sleeping book missed that the net cannot hand it;
//! 4. dark **across a period boundary**, the net it woke into no longer reaches back over what it
//!    missed, so it resyncs — from the checkpoint written on that same boundary, one new epoch, and
//!    onto the twin's book again. With no such checkpoint it stays dark rather than fold onto stale
//!    state.
//!
//! (3) and (4) are why gating a book is sound where gating a recurrence is not: the engine retains
//! the deltas as a *net*, so a wake costs depth rather than the length of the sleep.

use trading_data::{
	Book, BookAnchors, BookDelta, BookDeltas, BookShape, Buffering, Cell, DepOuts, FrameKind, Gate, Gating, Horizon, Node, Nudge, Precision, PrecisionPriceQty, Side, TradeBuf, TradeCols,
	Trades, Ts, node,
};
use v_utils::TF_15MIN;

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
const REACH: Horizon = Horizon::Span(TF_15MIN);
/// One tumble of the retained net, in nanoseconds — the same grid the checkpoint is written on.
const PERIOD: i64 = 900_000_000_000;

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
impl Node for GatedBook {
	/// The claim under test, and until the deltas became a retained [`Buffering`] it did not compile:
	/// a `Folding` dep on a gated node is the one thing the const-assert refuses.
	type Deps = (Gating<Hot>, BookAnchors, Buffering<BookDeltas, REACH>);

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
impl Node for Hot {
	type Deps = (Trades,);

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
impl Node for Mid {
	type Deps = (Gating<Hot>, GatedBook);

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

/// Both sides as raw levels, which is the whole of what "the same book" means here.
type Shape = (Vec<(i32, u32)>, Vec<(i32, u32)>);

fn anchor(bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	BookShape {
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
		..Default::default()
	}
}

/// One run of levels at the given sequence numbers — the gapless chain the shadow book writes.
fn run(ts_ns: i64, levels: &[(u64, Side, i32, u32)]) -> Vec<BookDelta> {
	levels
		.iter()
		.map(|&(seq, side, price, qty)| BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(ts_ns),
			ts_local_recv: Ts::from_nanos(ts_ns),
			monotonic_seq: seq,
			kind: FrameKind::Update,
			side,
			price,
			qty,
		})
		.collect()
}

/// One tick, the gate driven by whether a trade is present. Returns what the gated book and its
/// ungated twin each read, and the consumer's own out.
type Read = Option<(u64, Shape)>;

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
	let read = |b: Option<&Book>| b.map(|b| (b.epoch(), (b.bids().to_vec(), b.asks().to_vec())));
	(read(out.book), read(out.twin), out.mid)
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
fn a_gate_closed_across_a_boundary_resyncs_from_that_boundary_s_checkpoint() {
	let mut g = G::default();
	let mut trades = TradeBuf::new(PREC);

	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	tick(&mut g, &mut trades, true, Some(&seed), &run(100, &[(1, Side::Buy, 9_995, 7)]));

	// dark for the rest of the period, and on past its end — the net tumbles under the sleeping book.
	tick(&mut g, &mut trades, false, None, &run(200, &[(2, Side::Sell, 10_005, 2)]));
	let (_, twin, _) = tick(&mut g, &mut trades, false, None, &run(300, &[(3, Side::Buy, 9_998, 9)]));
	// the checkpoint the recorder writes on the boundary: the book as it stood when the period closed
	let (bids, asks) = twin.expect("the twin never sleeps").1;
	let boundary = anchor(&bids, &asks);

	tick(&mut g, &mut trades, false, None, &run(PERIOD + 100, &[(4, Side::Sell, 10_004, 1)]));

	// 1. woken with no checkpoint: the net covers only this period, and levels 2..3 are behind it.
	//    Folding on would fold onto stale state, so it declines instead.
	let (gated, _, mid) = tick(&mut g, &mut trades, true, None, &run(PERIOD + 200, &[(5, Side::Buy, 9_999, 4)]));
	assert_eq!((gated, mid), (None, None), "past the net's reach and without a seed, a book is not readable");

	// 2. the boundary checkpoint re-arms it: one resync, then the period's net on top — and that is
	//    the twin's book, not an approximation of it.
	let (gated, twin, mid) = tick(&mut g, &mut trades, true, Some(&boundary), &run(PERIOD + 300, &[(6, Side::Sell, 10_003, 8)]));
	assert_eq!(
		gated.as_ref().map(|(_, l)| l),
		twin.as_ref().map(|(_, l)| l),
		"checkpoint + net is the whole of what the twin folded"
	);
	assert_eq!(gated.map(|(e, _)| e), Some(2), "exactly one resync, so exactly one new epoch");
	assert!(mid.is_some());
}
