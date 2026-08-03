use trading_data::{Buffering, Cell, Emit, EmitOuts, Horizon, closed_by, node, slice_nudge};
use v_utils::*;

/// Notional of the newest 4h bar to have closed by each 1m close.
#[derive(Clone, Default)]
pub struct Volume4h;
impl Cell for Volume4h {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Volume4h {
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Buffering<trading_data::Bars<{ TF_4H }>, { Horizon::Elems(1) }>);

	fn emit(&mut self, (m1, h4): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in m1 {
			out.push(closed_by(h4.all(), b.ts_close).last().map(|h| h.vol_base * h.close));
		}
	}
}
slice_nudge!(Volume4h, Option<f64>);
