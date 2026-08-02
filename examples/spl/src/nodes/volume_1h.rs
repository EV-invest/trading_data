use trading_data::{Buffering, Cell, Emit, EmitOuts, Gating, Horizon, closed_by, node, slice_nudge};

use super::{Bar1h, Bar1m, Screener};

/// Notional of the newest 1h bar to have closed by each 1m close — the level standing at the
/// screening clock, retained across the minutes the hour publishes nothing.
#[derive(Clone, Default)]
pub struct Volume1h;
impl Cell for Volume1h {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Volume1h {
	type Deps = (Gating<Screener>, Bar1m, Buffering<Bar1h, { Horizon::Elems(1) }>);

	fn emit(&mut self, (hit, m1, h1): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		assert!(hit, "a gating dep reads true inside `emit`");
		for b in m1 {
			out.push(closed_by(h1.all(), b.ts_close).last().map(|h| h.vol_base * h.close));
		}
	}
}
slice_nudge!(Volume1h, Option<f64>);
