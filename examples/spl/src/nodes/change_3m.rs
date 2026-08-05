use trading_data::{Buffering, Cell, Emit, EmitOuts, Over, node, slice_nudge};
use v_utils::*;

/// Percent change over the trailing three minutes, off the closed 1m bars inside it. SPL's backtest
/// mode: reading it off a live Trades window instead is a live-only fidelity choice.
#[derive(Clone, Default)]
pub struct Change3m;
impl Cell for Change3m {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Change3m {
	type Deps = (Buffering<trading_data::Bars<{ TF_1MIN }>, Over<TF_3MIN>>,);

	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (m1,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for (b, w3) in m1.fresh().iter().zip(m1.trailing()) {
			out.push(w3.and_then(|w3| {
				let base_open = w3[0].open;
				(base_open > 0.0).then(|| (b.close - base_open) / base_open * 100.0)
			}));
		}
	}
}
slice_nudge!(Change3m, Option<f64>);
