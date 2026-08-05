use trading_data::{Cell, Emit, EmitOuts, node, slice_nudge};

use super::book_top::BookTop;

/// Bid-ask spread as a percentage of the bid.
#[derive(Clone, Default)]
pub struct Spread;
impl Cell for Spread {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Spread {
	type Deps = (BookTop,);

	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (top,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for d in top {
			out.push(d.map(|d| (d.best_ask - d.best_bid) / d.best_bid * 100.0));
		}
	}
}
slice_nudge!(Spread, Option<f64>);
