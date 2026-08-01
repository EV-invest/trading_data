use core::fmt;

use trading_data::{Book, BookDeltas, Cell, DepOuts, Glance, Horizon, Node, Plot, slice_nudge};

use crate::DEPTH;

#[derive(Clone, Copy, Debug)]
pub struct BookTopSnap {
	pub ts_ns: i64,
	pub best_bid: f64,
	pub best_ask: f64,
	pub top20_bid_depth_usd: f64,
	pub top20_ask_depth_usd: f64,
	pub imbalance: f64,
	pub spread_pct: f64,
}
impl BookTopSnap {
	pub fn mid(&self) -> f64 {
		(self.best_bid + self.best_ask) / 2.0
	}
}

flat_fields!(BookTopSnap[best_bid, best_ask, top20_bid_depth_usd, top20_ask_depth_usd, imbalance, spread_pct]);

impl Glance for BookTopSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.4}/{:.4} imb {:+.3}", self.best_bid, self.best_ask, self.imbalance)
	}
}

/// Best bid/ask, top-20 depth, imbalance and spread off the folded book — derived facts, peer to
/// [`super::Rsi`] or [`super::Atr`], and the delta lane's own cadence is the rate. A book still
/// filling from its first deltas has one side empty; that is warmup, not corruption, so the tick
/// declines and the deprecator simply doesn't enter yet.
#[derive(Clone, Default)]
pub struct BookTop {
	buf: Vec<Option<BookTopSnap>>,
}
impl Cell for BookTop {
	type Out<'t> = &'t [Option<BookTopSnap>];
}
impl Node for BookTop {
	type Deps = (Book, BookDeltas);

	/// The `buf` it clears as `advance`'s first act is the whole of its state — the depth it reads is
	/// `Book`'s to hold, not this node's.
	const HORIZON: Horizon = Horizon::Unit;
	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["bid", "ask", "bid_depth$", "ask_depth$", "imbalance", "spread%"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (book, frame): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let Some(&ts) = frame.cols().exec().last() else { return &self.buf };
		self.buf.push(book.and_then(|b| {
			let (ps, qs) = (b.prec().price.scale(), b.prec().qty.scale());
			let (bid, ask) = (b.best_bid()?, b.best_ask()?);
			let usd = |&(p, q): &(i32, u32)| (p as f64 / ps) * (q as f64 / qs);
			let top20_bid_depth_usd: f64 = b.bids().iter().take(DEPTH).map(usd).sum();
			let top20_ask_depth_usd: f64 = b.asks().iter().take(DEPTH).map(usd).sum();
			let total = top20_bid_depth_usd + top20_ask_depth_usd;
			let (best_bid, best_ask) = (bid.0.as_f64(), ask.0.as_f64());
			Some(BookTopSnap {
				ts_ns: ts.as_nanos(),
				best_bid,
				best_ask,
				top20_bid_depth_usd,
				top20_ask_depth_usd,
				imbalance: if total > 0.0 { (top20_bid_depth_usd - top20_ask_depth_usd) / total } else { 0.0 },
				spread_pct: (best_ask - best_bid) / best_bid * 100.0,
			})
		}));
		&self.buf
	}
}
slice_nudge!(BookTop, Option<BookTopSnap>);
