//! The book as a gateable stateful node. Three claims, in order:
//!
//! 1. an unseeded book publishes `None` — the fold's own honesty, not a wrapper's;
//! 2. a closed gate leaves the venue frames *untouched* — the node goes dark without pulling its
//!    plain deps, so no checkpoint and no level is read;
//! 3. reopening does not fold onto stale state — the `monotonic_seq` discontinuity the skipped
//!    frames left behind desyncs it, and only the next checkpoint re-arms it.
//!
//! (3) is why gating a book is sound where gating a recurrence is not: it re-warms from a
//! checkpoint instead of from a warmup it can never recover.

use trading_data::{
	Book, BookAnchors, BookDelta, BookDeltas, BookShape, Cell, DepOuts, FrameKind, Gate, Gating, Node, Nudge, Precision, PrecisionPriceQty, Side, TradeBuf, TradeCols, Trades, Ts, node,
};

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};

/// `Node::Deps` is fixed on the impl, so the shipped `Book` node is the ungated one; gating it is
/// this eight-line wrapper over the same public `Book::step` fold. The two cannot drift.
///
/// Its deps are bare where the shipped node declares its deltas `Folding` — and that is the claim
/// under test rather than an oversight: this fold re-warms from a checkpoint, out of band, so the
/// graph owes it no history. A `Folding` dep here would (correctly, for anything else) not compile.
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
	type Deps = (Gating<Hot>, BookAnchors, BookDeltas);

	fn advance<'t>(&'t mut self, (_, a, d): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.0.step(a, d).then_some(&self.0)
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
	outputs { mid: Mid, book: GatedBook }
}

fn anchor(bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	BookShape {
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
		..Default::default()
	}
}

/// One run of levels at the given sequence numbers — the gapless chain the shadow book writes.
fn frame(levels: &[(u64, Side, i32, u32)]) -> Vec<BookDelta> {
	levels
		.iter()
		.map(|&(seq, side, price, qty)| BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(1),
			ts_local_recv: Ts::from_nanos(1),
			monotonic_seq: seq,
			kind: FrameKind::Update,
			side,
			price,
			qty,
		})
		.collect()
}

#[test]
fn gated_book_is_latent_closed_and_resyncs_from_a_checkpoint() {
	let mut g = G::default();
	let mut trades = TradeBuf::new(PREC);

	let tick = |g: &mut G, hot: bool, a: Option<&BookShape>, d: &[BookDelta], trades: &mut TradeBuf| -> (Option<u64>, Option<f64>) {
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
		(out.book.map(Book::epoch), out.mid)
	};

	// 1. gate open, but no checkpoint yet: the fold has nothing to be, and says so.
	assert_eq!(tick(&mut g, true, None, &frame(&[(1, Side::Buy, 9_900, 5)]), &mut trades), (None, None));

	// 2. the first checkpoint arms it; the same step's frame folds on top.
	let seed = anchor(&[(9_990, 3)], &[(10_010, 4)]);
	let (epoch, mid) = tick(&mut g, true, Some(&seed), &frame(&[(2, Side::Buy, 9_995, 7)]), &mut trades);
	assert_eq!(epoch, Some(1));
	assert_eq!(mid, Some(100.025), "best bid must be the folded level, not the checkpoint's");

	// 3. gate closed: latent, and the frames go by unread — a closed gate pulls no plain dep.
	assert_eq!(tick(&mut g, false, None, &frame(&[(3, Side::Sell, 10_005, 2)]), &mut trades), (None, None));
	assert_eq!(tick(&mut g, false, None, &frame(&[(4, Side::Sell, 10_004, 2)]), &mut trades), (None, None));

	// 4. reopened, no checkpoint: seqs 3..4 are missing from our chain, so folding seq 5 onto what
	//    we still hold would be folding onto stale state. It desyncs instead.
	assert_eq!(tick(&mut g, true, None, &frame(&[(5, Side::Buy, 9_996, 1)]), &mut trades), (None, None));

	// 5. the next checkpoint re-arms it — from the checkpoint, not from the stale book.
	let resync = anchor(&[(9_980, 3)], &[(10_020, 4)]);
	let (epoch, mid) = tick(&mut g, true, Some(&resync), &[], &mut trades);
	assert_eq!(epoch, Some(2), "a resync is a new epoch");
	assert_eq!(mid, Some(100.0), "the re-armed book must read the checkpoint, not the pre-gate levels");
}
