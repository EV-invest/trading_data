use trading_data::{Buffering, Cell, Env, Over, ScanOuts, Scans, Slots, Stamped, Vars, Witness, constant, gt, node, select, slice_nudge};
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

	fn read<W: Witness>((m1,): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (b, lag) = m1.lagged_at(i, 0).expect("element i of this tick's own fresh run");
		env.dep(0).lag(lag).put(b);
		match m1.trailing_at(i) {
			// `open` is slot 0 of a bar, which is the default this leaves unsaid.
			Some((w, lag)) => env.dep(0).lag(lag).put(&w[0].open),
			// an incomplete window declines, and NaN is how it says so.
			None => env.opaque().put(&f64::NAN),
		}
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, base_open) = (v.get::<3>(), v.get::<5>());
		select(gt(base_open, constant(0.0)), (close - base_open) / base_open * constant(100.0), constant(f64::NAN))
	}
}
slice_nudge!(Change3m, Option<f64>);
