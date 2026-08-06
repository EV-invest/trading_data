use trading_data::{Cell, Flat, Plot, ScanOuts, Scans, Slots, Vars, constant, gt, node, select, slice_nudge};

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
impl Scans for Imbalance {
	type Deps = (BookTop,);

	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((-1.0, 1.0)),
		..Plot::DEFAULT
	}];

	fn read((top,): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		top[i].flat(env);
		top[i].map(|d| d.ts_ns)
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (bids, asks) = (v.get::<2>(), v.get::<3>());
		let total = bids + asks;
		select(gt(total, constant(0.0)), (bids - asks) / total, constant(0.0))
	}
}
slice_nudge!(Imbalance, Option<f64>);
