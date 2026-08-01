use core::fmt;

use trading_data::{Book, BookDeltas, Cell, DepOuts, Glance, Node, Plot, slice_nudge};

use crate::DEPTH;

/// One atomic read of the top of the folded book. Everything here is *observed* — what can be
/// computed from these four numbers is a node of its own, not another field.
#[derive(Clone, Copy, Debug)]
pub struct BookTopSnap {
	pub ts_ns: i64,
	pub best_bid: f64,
	pub best_ask: f64,
	pub top20_bid_depth_usd: f64,
	pub top20_ask_depth_usd: f64,
}
impl BookTopSnap {
	pub fn mid(&self) -> f64 {
		(self.best_bid + self.best_ask) / 2.0
	}
}

flat_fields!(BookTopSnap[best_bid, best_ask, top20_bid_depth_usd, top20_ask_depth_usd]);

impl Glance for BookTopSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.4}/{:.4}", self.best_bid, self.best_ask)
	}
}

/// Best bid/ask and top-20 depth off the folded book — a derived fact, peer to [`super::Rsi`] or
/// [`super::Atr`], and the delta lane's own cadence is the rate. A book still filling from its first
/// deltas has one side empty; that is warmup, not corruption, so the tick declines and the
/// deprecator simply doesn't enter yet.
///
/// A batch is collapsed to the one read at its end, so the out is never longer than a single tick.
#[derive(Clone, Default)]
pub struct BookTop {
	buf: Vec<Option<BookTopSnap>>,
}
impl Cell for BookTop {
	type Out<'t> = &'t [Option<BookTopSnap>];
}
impl Node for BookTop {
	type Deps = (Book, BookDeltas);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["bid", "ask", "bid_depth$", "ask_depth$"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (book, frame): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let Some(&ts) = frame.cols().exec().last() else { return &self.buf };
		self.buf.push(book.and_then(|b| {
			let (ps, qs) = (b.prec().price.scale(), b.prec().qty.scale());
			let (bid, ask) = (b.best_bid()?, b.best_ask()?);
			let usd = |&(p, q): &(i32, u32)| (p as f64 / ps) * (q as f64 / qs);
			Some(BookTopSnap {
				ts_ns: ts.as_nanos(),
				best_bid: bid.0.as_f64(),
				best_ask: ask.0.as_f64(),
				top20_bid_depth_usd: b.bids().iter().take(DEPTH).map(usd).sum(),
				top20_ask_depth_usd: b.asks().iter().take(DEPTH).map(usd).sum(),
			})
		}));
		&self.buf
	}
}
slice_nudge!(BookTop, Option<BookTopSnap>);
