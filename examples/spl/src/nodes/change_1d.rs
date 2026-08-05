use trading_data::{Buffering, Cell, Emit, EmitOuts, Exact, Over, closed_by, node, slice_nudge};
use v_utils::*;

/// Percent change against the 1h close standing a day back, asked once per closed 1m bar.
#[derive(Clone, Default)]
pub struct Change1d;
impl Cell for Change1d {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Change1d {
	/// A day of wall clock, not "24 bars": an hour nothing traded emits no bar. The retained run is
	/// that day plus one period of the buffered series itself — the 1m bar asking the question stands
	/// up to a whole period past the newest close of it.
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Buffering<trading_data::Bars<{ TF_1H }>, Over<{ Timeframe(TF_1D.0 + TF_1H.0) }>>);

	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (m1, h1): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in m1 {
			let closed_1h = closed_by(h1.all(), b.ts_close);
			let day_ago = b.ts_close - Exact::from(TF_1D.duration());
			// The close standing a day back is the first one after `day_ago`; index 0 means the retained
			// run does not reach behind it, so there is nothing a day old to compare against yet.
			let oldest = closed_1h.iter().position(|h| h.ts_close > day_ago).filter(|&i| i > 0).map(|i| closed_1h[i].close);
			out.push(oldest.filter(|&o| o != 0.0).map(|o| (b.close - o) / o * 100.0));
		}
	}
}
slice_nudge!(Change1d, Option<f64>);
