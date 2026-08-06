use trading_data::{Buffering, Cell, Flat, Over, ScanOuts, Scans, Slots, Stamped, Vars, constant, gt, node, select, slice_nudge};
use v_utils::*;

/// Percent change over the trailing three minutes, off the closed 1m bars inside it. SPL's backtest
/// mode: reading it off a live Trades window instead is a live-only fidelity choice.
#[derive(Clone, Default)]
pub struct Change3m;
impl Cell for Change3m {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Scans for Change3m {
	type Deps = (Buffering<trading_data::Bars<{ TF_1MIN }>, Over<TF_3MIN>>,);

	const BEYOND: Option<&'static str> = Some("the open of the bar three minutes back, which no one-step column stands for");
	const EXTRA: usize = 1;

	fn read((m1,): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		let b = m1.lagged_at(i, 0).expect("element i of this tick's own fresh run");
		b.flat(&mut env[..5]);
		// an incomplete window declines, and NaN is how it says so.
		env[5] = m1.trailing_at(i).map_or(f64::NAN, |w| w[0].open);
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, base_open) = (v.get::<3>(), v.get::<5>());
		select(gt(base_open, constant(0.0)), (close - base_open) / base_open * constant(100.0), constant(f64::NAN))
	}
}
slice_nudge!(Change3m, Option<f64>);
