//! Minimal live graph: the trade and book roots the facade ships, feeding Cvd (running signed
//! notional per trade), BookFlow (running signed level qty, market activity only), the folded
//! `Book`, and 1m bars off the same trades — the bars are what the chart draws price from.

use trading_data::{
	BookAnchors, BookDelta, BookDeltas, BookShape, Carried, Cell, FoldOuts, Folding, Folds, FrameKind, Lanes, Over, RunOuts, Runs, Side, Slots, TradeCols, Trades, Unbounded, Vars, node,
	slice_nudge,
};
use v_utils::*;

/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd(Carried);
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
#[node]
impl Folds for Cvd {
	/// A running sum reaches to the start of the run.
	type Deps = (Folding<Trades, Unbounded>,);

	/// The side, which `Flat` leaves out because a side has no slope — it picks which way the
	/// notional points, and picking is not a variable
	/// (`r[kernels.selection.index-is-not-a-variable]`).
	const EXTRA: usize = 1;
	const STATE: usize = 1;

	fn read((t,): &FoldOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		env[0] = t.price[i] as f64 / t.prec.price.scale();
		env[1] = t.qty[i] as f64 / t.prec.qty.scale();
		env[2] = match t.side[i] {
			Side::Buy => 1.0,
			Side::Sell => -1.0,
		};
		Some(t.exec()[i].as_nanos())
	}

	fn step(&self, v: Vars) -> impl Slots {
		let (price, qty, side, sum) = (v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>());
		sum + side * (price * qty)
	}

	fn value(&self, v: Vars) -> impl Slots {
		v.get::<3>()
	}

	fn carried(&self) -> &Carried {
		&self.0
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.0
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
impl Runs for BookFlow {
	/// A running sum reaches to the start of the run.
	type Deps = (Folding<BookDeltas, Unbounded>,);

	const WHY: &'static str = "a book fold read for flow, which is not a scalar function of its deltas";

	fn emit(&mut self, (levels,): RunOuts<'_, Self>, out: &mut Vec<f64>) {
		// A correction is a dropped websocket packet, not market activity: folding one into flow
		// fabricates signal out of it.
		for l in levels.iter().filter(|l| l.kind == FrameKind::Update) {
			let q = l.qty as f64 / l.prec.qty.scale();
			self.sum += match l.side {
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
	roots { trades: Trades[TradeCols], deltas: BookDeltas[BookDelta], anchors: BookAnchors[BookShape] };
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
