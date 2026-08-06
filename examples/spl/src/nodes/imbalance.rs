use trading_data::{Cell, Plot, RunOuts, Runs, node, slice_nudge};

use super::book_top::BookTop;

/// Which way the top-20 depth leans, in `[-1, 1]`. Zero when both sides are empty — no lean is the
/// honest reading of a book that shows nothing, and the depths are already published for anyone who
/// needs to tell that from a balanced one.
#[derive(Clone, Default)]
pub struct Imbalance;
impl Cell for Imbalance {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Runs for Imbalance {
	type Deps = (BookTop,);

	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((-1.0, 1.0)),
		..Plot::DEFAULT
	}];
	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (top,): RunOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for d in top {
			out.push(d.map(|d| {
				let total = d.top20_bid_depth_usd + d.top20_ask_depth_usd;
				if total > 0.0 { (d.top20_bid_depth_usd - d.top20_ask_depth_usd) / total } else { 0.0 }
			}));
		}
	}
}
slice_nudge!(Imbalance, Option<f64>);
