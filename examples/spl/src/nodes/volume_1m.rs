use trading_data::{Cell, Flat, ScanOuts, Scans, Slots, Stamped, Vars, node, slice_nudge};
use v_utils::*;

/// The closed 1m bar's notional, `volume * close` — the close standing in for vwap, as SPL's own
/// volume indie does. Nothing to warm, so there is no declining.
#[derive(Clone, Default)]
pub struct Volume1m;
impl Cell for Volume1m {
	type Out<'t> = &'t [f64];
}
#[node]
impl Scans for Volume1m {
	type Deps = (trading_data::Bars<{ TF_1MIN }>,);

	fn read((m1,): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		m1[i].flat(env);
		Some(m1[i].ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, vol_base) = (v.get::<3>(), v.get::<4>());
		vol_base * close
	}
}
slice_nudge!(Volume1m, f64);
