use trading_data::{Cell, Flat, ScanOuts, Scans, Slots, Vars, constant, node, slice_nudge};

use super::book_top::BookTop;

/// Bid-ask spread as a percentage of the bid.
#[derive(Clone, Default)]
pub struct Spread;
impl Cell for Spread {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Scans for Spread {
	type Deps = (BookTop,);

	fn read((top,): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		top[i].flat(env);
		top[i].map(|d| d.ts_ns)
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (bid, ask) = (v.get::<0>(), v.get::<1>());
		(ask - bid) / bid * constant(100.0)
	}
}
slice_nudge!(Spread, Option<f64>);
