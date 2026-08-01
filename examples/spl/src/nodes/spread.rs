use trading_data::{Cell, DepOuts, Horizon, Node, slice_nudge};

use super::book_top::BookTop;

/// Bid-ask spread as a percentage of the bid.
#[derive(Clone, Default)]
pub struct Spread {
	buf: Vec<Option<f64>>,
}
impl Cell for Spread {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Spread {
	type Deps = (BookTop,);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (top,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for d in top {
			self.buf.push(d.map(|d| (d.best_ask - d.best_bid) / d.best_bid * 100.0));
		}
		&self.buf
	}
}
slice_nudge!(Spread, Option<f64>);
