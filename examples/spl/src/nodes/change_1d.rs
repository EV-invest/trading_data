use trading_data::{Buffering, Cell, Exact, Flat, Over, ScanOuts, Scans, Slots, Stamped, Vars, abs, closed_by, constant, gt, node, select, slice_nudge};
use v_utils::*;

/// Percent change against the 1h close standing a day back, asked once per closed 1m bar.
#[derive(Clone, Default)]
pub struct Change1d;
impl Cell for Change1d {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Scans for Change1d {
	/// A day of wall clock, not "24 bars": an hour nothing traded emits no bar. The retained run is
	/// that day plus one period of the buffered series itself — the 1m bar asking the question stands
	/// up to a whole period past the newest close of it.
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Buffering<trading_data::Bars<{ TF_1H }>, Over<{ Timeframe(TF_1D.0 + TF_1H.0) }>>);

	const BEYOND: Option<&'static str> = Some("the 1h bar standing a day back, which the hourly column describes in place of the series' own newest");

	fn read((m1, h1): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		let b = m1[i];
		b.flat(&mut env[..5]);
		let closed_1h = closed_by(h1.all(), b.ts_close);
		let day_ago = b.ts_close - Exact::from(TF_1D.duration());
		// The close standing a day back is the first one after `day_ago`; index 0 means the retained
		// run does not reach behind it, so there is nothing a day old to compare against yet.
		let oldest = closed_1h.iter().position(|h| h.ts_close > day_ago).filter(|&i| i > 0).map(|i| closed_1h[i]);
		oldest.flat(&mut env[5..]);
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, oldest) = (v.get::<3>(), v.get::<8>());
		// `|oldest| > 0` rather than a bare comparison: a zero close divides, and an absent one is NaN,
		// and both are the same decline.
		select(gt(abs(oldest), constant(0.0)), (close - oldest) / oldest * constant(100.0), constant(f64::NAN))
	}
}
slice_nudge!(Change1d, Option<f64>);
