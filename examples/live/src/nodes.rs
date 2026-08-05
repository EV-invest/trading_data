//! Minimal live graph: the trade and book roots the facade ships, feeding Cvd (running signed
//! notional per trade), BookFlow (running signed level qty, market activity only), the folded
//! `Book`, and 1m bars off the same trades — the bars are what the chart draws price from.

use trading_data::{BookAnchors, BookDeltas, BookShape, Cell, DeltaFrame, Emit, EmitOuts, Folding, Horizon, Lanes, Side, TradeCols, Trades, node, slice_nudge};
use v_utils::*;

/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
}
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
#[node]
impl Emit for Cvd {
	/// A running sum reaches to the start of the run.
	type Deps = (Folding<Trades, { Horizon::Unbounded }>,);

	fn emit(&mut self, (t,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		let (ps, qs) = (t.prec.price.scale(), t.prec.qty.scale());
		for i in 0..t.len() {
			let notional = (t.price[i] as f64 / ps) * (t.qty[i] as f64 / qs);
			self.sum += match t.side[i] {
				Side::Buy => notional,
				Side::Sell => -notional,
			};
			out.push(self.sum);
		}
	}
}
slice_nudge!(Cvd, f64);

/// Running Σ signed level qty (bid +, ask −), one element per level. A `qty == 0` level is a
/// delete and contributes nothing.
#[derive(Clone, Default)]
pub struct BookFlow {
	sum: f64,
}
impl Cell for BookFlow {
	type Out<'t> = &'t [f64];
}
#[node]
impl Emit for BookFlow {
	/// A running sum reaches to the start of the run.
	type Deps = (Folding<BookDeltas, { Horizon::Unbounded }>,);

	fn emit(&mut self, (frame,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		// A correction is a dropped websocket packet, not market activity: folding one into flow
		// fabricates signal. The enum is what makes ignoring that impossible to do by accident.
		let DeltaFrame::Update(d) = frame else { return };
		let qs = d.prec.qty.scale();
		for i in 0..d.len() {
			let q = d.qty[i] as f64 / qs;
			self.sum += match d.side[i] {
				Side::Buy => q,
				Side::Sell => -q,
			};
			out.push(self.sum);
		}
	}
}
slice_nudge!(BookFlow, f64);

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape] };
	out TickOut;
	outputs { cvd: Cvd, book_flow: BookFlow, book: trading_data::Book, bar_1m: trading_data::Bars<{ TF_1MIN }> }
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
