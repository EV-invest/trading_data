use trading_data::{Cell, RunOuts, Runs, node, slice_nudge};
use v_utils::*;

/// The closed 1m bar's notional, `volume * close` — the close standing in for vwap, as SPL's own
/// volume indie does. Nothing to warm, so there is no declining.
#[derive(Clone, Default)]
pub struct Volume1m;
impl Cell for Volume1m {
	type Out<'t> = &'t [f64];
}
#[node]
impl Runs for Volume1m {
	type Deps = (trading_data::Bars<{ TF_1MIN }>,);

	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (m1,): RunOuts<'_, Self>, out: &mut Vec<f64>) {
		for b in m1 {
			out.push(b.vol_base * b.close);
		}
	}
}
slice_nudge!(Volume1m, f64);
