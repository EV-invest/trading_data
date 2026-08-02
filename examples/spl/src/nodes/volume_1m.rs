use trading_data::{Cell, Emit, EmitOuts, Gating, node, slice_nudge};

use super::{Bar1m, Screener};

/// The closed 1m bar's notional, `volume * close` — the close standing in for vwap, as SPL's own
/// volume indie does. Nothing to warm, so there is no declining.
#[derive(Clone, Default)]
pub struct Volume1m;
impl Cell for Volume1m {
	type Out<'t> = &'t [f64];
}
#[node]
impl Emit for Volume1m {
	type Deps = (Gating<Screener>, Bar1m);

	fn emit(&mut self, (hit, m1): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		assert!(hit, "a gating dep reads true inside `emit`");
		for b in m1 {
			out.push(b.vol_base * b.close);
		}
	}
}
slice_nudge!(Volume1m, f64);
