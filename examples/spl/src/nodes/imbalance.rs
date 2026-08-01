use trading_data::{Cell, DepOuts, Horizon, Node, Plot, slice_nudge};

use super::book_top::BookTop;

/// Which way the top-20 depth leans, in `[-1, 1]`. Zero when both sides are empty — no lean is the
/// honest reading of a book that shows nothing, and the depths are already published for anyone who
/// needs to tell that from a balanced one.
#[derive(Clone, Default)]
pub struct Imbalance {
	buf: Vec<Option<f64>>,
}
impl Cell for Imbalance {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Imbalance {
	type Deps = (BookTop,);

	const HORIZON: Horizon = Horizon::Unit;
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((-1.0, 1.0)),
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (top,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for d in top {
			self.buf.push(d.map(|d| {
				let total = d.top20_bid_depth_usd + d.top20_ask_depth_usd;
				if total > 0.0 { (d.top20_bid_depth_usd - d.top20_ask_depth_usd) / total } else { 0.0 }
			}));
		}
		&self.buf
	}
}
slice_nudge!(Imbalance, Option<f64>);
