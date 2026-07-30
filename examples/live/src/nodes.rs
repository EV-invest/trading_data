//! Minimal live graph: the trade and book roots the facade ships, feeding Cvd (running signed
//! notional per trade), BookFlow (running signed level qty, market activity only) and Spread off
//! the folded `Book`. 1m bars won't close in 15s, so the proof rides on these per-event outputs.

use trading_data::{Book, BookAnchors, BookDeltas, BookShape, Cell, DeltaFrame, DepOuts, Lanes, Node, TradeCols, Trades, slice_nudge};
use trading_data_core::Side;

/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
	buf: Vec<f64>,
}
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
impl Node for Cvd {
	type Deps = (Trades,);

	fn advance<'t>(&'t mut self, (t,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let (ps, qs) = (t.prec.price_scale(), t.prec.qty_scale());
		for i in 0..t.len() {
			let notional = (t.price[i] as f64 / ps) * (t.qty[i] as f64 / qs);
			self.sum += match t.side[i] {
				Side::Buy => notional,
				Side::Sell => -notional,
			};
			self.buf.push(self.sum);
		}
		&self.buf
	}
}
slice_nudge!(Cvd, f64);

/// Running Σ signed level qty (bid +, ask −), one element per level. A `qty == 0` level is a
/// delete and contributes nothing.
#[derive(Clone, Default)]
pub struct BookFlow {
	sum: f64,
	buf: Vec<f64>,
}
impl Cell for BookFlow {
	type Out<'t> = &'t [f64];
}
impl Node for BookFlow {
	type Deps = (BookDeltas,);

	fn advance<'t>(&'t mut self, (frame,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// A correction is a dropped websocket packet, not market activity: folding one into flow
		// fabricates signal. The enum is what makes ignoring that impossible to do by accident.
		let DeltaFrame::Update(d) = frame else { return &self.buf };
		let qs = d.prec.qty_scale();
		for i in 0..d.len() {
			let q = d.qty[i] as f64 / qs;
			self.sum += match d.side[i] {
				Side::Buy => q,
				Side::Sell => -q,
			};
			self.buf.push(self.sum);
		}
		&self.buf
	}
}
slice_nudge!(BookFlow, f64);

/// The book read as a level: `None` until it is synced, which is also what makes it gateable.
#[derive(Clone, Default)]
pub struct Spread {
	buf: Vec<Option<f64>>,
}
impl Cell for Spread {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Spread {
	type Deps = (Book,);

	fn advance<'t>(&'t mut self, (book,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		self.buf.push(book.and_then(|b| {
			let s = b.prec().price_scale();
			let (bid, ask) = (b.best_bid()?, b.best_ask()?);
			Some((ask.0 - bid.0) as f64 / s)
		}));
		&self.buf
	}
}
slice_nudge!(Spread, Option<f64>);

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape] };
	out TickOut;
	book: Book,
	cvd: Cvd,
	book_flow: BookFlow,
	spread: Spread,
}

impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self {
			trades: l.trades,
			deltas: l.deltas,
			anchors: l.anchor,
		}
	}
}
