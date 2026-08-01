use trading_data::{Buffering, Cell, DepOuts, Horizon, Node, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

use super::bar::{Bar1h, Bar1m, closed_by};

/// A day of wall clock, not "24 bars": an hour nothing traded emits no bar, and SPL's own name for
/// the window is the day.
const SPAN_1D: Timeframe = Timeframe::from_naive(1, TimeframeDesignator::Days);
/// What the 1h series must retain to answer it: the day, plus one period of cross-rate slack — the
/// 1m bar whose close asks the question stands up to a whole 1h period past the newest 1h bar.
pub(super) const REACH_1D: Horizon = Horizon::Span(Timeframe(SPAN_1D.0 + Bar1h::TF.0));

/// Percent change against the 1h close standing a day back, asked once per closed 1m bar.
#[derive(Clone, Default)]
pub struct Change1d {
	buf: Vec<Option<f64>>,
}
impl Cell for Change1d {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Change1d {
	type Deps = (Bar1m, Buffering<Bar1h, REACH_1D>);

	fn advance<'t>(&'t mut self, (m1, h1): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			let deadline = b.ts_close;
			let closed_1h = closed_by(h1.all(), deadline);
			let day_ago = deadline - SPAN_1D.duration().as_nanos() as i64;
			// The close standing a day back is the first one after `day_ago`; index 0 means the retained
			// run does not reach behind it, so there is nothing a day old to compare against yet.
			let oldest = closed_1h.iter().position(|h| h.ts_close > day_ago).filter(|&i| i > 0).map(|i| closed_1h[i].close);
			self.buf.push(oldest.filter(|&o| o != 0.0).map(|o| (b.close - o) / o * 100.0));
		}
		&self.buf
	}
}
slice_nudge!(Change1d, Option<f64>);
