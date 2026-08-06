use trading_data::{Buffering, Cell, Elems, Flat, ScanOuts, Scans, Slots, Stamped, Vars, closed_by, node, slice_nudge};
use v_utils::*;

/// Notional of the newest 1h bar to have closed by each 1m close — the level standing at the
/// screening clock, retained across the minutes the hour publishes nothing.
#[derive(Clone, Default)]
pub struct Volume1h;
impl Cell for Volume1h {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Scans for Volume1h {
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Buffering<trading_data::Bars<{ TF_1H }>, Elems<1>>);

	const BEYOND: Option<&'static str> = Some("the 1h bar standing at this minute, which the hourly column describes in place of the series' own newest");

	fn read((m1, h1): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		let b = m1[i];
		b.flat(&mut env[..5]);
		// `Option`'s own flattening is the NaN fill, so a minute no hour has closed by declines
		// through the same channel the body would.
		closed_by(h1.all(), b.ts_close).last().copied().flat(&mut env[5..]);
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, vol_base) = (v.get::<8>(), v.get::<9>());
		vol_base * close
	}
}
slice_nudge!(Volume1h, Option<f64>);
