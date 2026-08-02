use trading_data::{Cell, Emit, EmitOuts, Gating, node, slice_nudge};

use super::{Screener, book_top::BookTop};

/// Bid-ask spread as a percentage of the bid.
#[derive(Clone, Default)]
pub struct Spread;
impl Cell for Spread {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Spread {
	type Deps = (Gating<Screener>, BookTop);

	fn emit(&mut self, (hit, top): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		assert!(hit, "a gating dep reads true inside `emit`");
		for d in top {
			out.push(d.map(|d| (d.best_ask - d.best_bid) / d.best_bid * 100.0));
		}
	}
}
slice_nudge!(Spread, Option<f64>);
