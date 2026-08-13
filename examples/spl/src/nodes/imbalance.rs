use trading_data::{Cell, DepOuts, Env, Lagged, Plot, Reading, Scans, Slots, Vars, Witness, constant, gt, node, select, slice_nudge};

use super::book_top::BookTop;

/// Which way the top-20 depth leans, in `[-1, 1]`. Zero when both sides are empty — no lean is the
/// honest reading of a book that shows nothing, and the depths are already published for anyone who
/// needs to tell that from a balanced one.
#[derive(Clone, Default)]
pub struct Imbalance;
impl Cell for Imbalance {
	type Out<'t> = &'t [Reading];
}
#[node]
impl Scans for Imbalance {
	type Deps = (BookTop,);

	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((-1.0, 1.0)),
		..Plot::DEFAULT
	}];

	fn read<W: Witness>((top,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (t, lag) = top.at(i)?;
		// a book still filling has one side empty and no top to read: declined before the put, so no
		// absence reaches the body as an operand.
		let d = t.as_ref()?;
		env.dep(0).lag(lag).put(d);
		Some(d.ts_ns)
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (bids, asks) = (v.get::<2>(), v.get::<3>());
		let total = bids + asks;
		select(gt(total, constant(0.0)), (bids - asks) / total, constant(0.0))
	}
}
slice_nudge!(Imbalance, Reading);
