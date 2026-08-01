use core::fmt;

use trading_data::{Buffering, Cell, DepOuts, Glance, Horizon, Node, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

use super::bar::{Bar1h, Bar1m, closed_by};

/// Reach behind `change_1d_pct` — a day of wall clock, not "24 bars": an hour nothing traded emits
/// no bar, and SPL's own name for the window is the day.
const SPAN_1D: Timeframe = Timeframe::from_naive(1, TimeframeDesignator::Days);
/// What the 1h series must retain to answer it: the day, plus one period of cross-rate slack — the
/// 1m bar whose close asks the question stands up to a whole 1h period past the newest 1h bar.
pub(super) const REACH_1D: Horizon = Horizon::Span(Timeframe(SPAN_1D.0 + Bar1h::TF.0));
/// Reach behind `change_3m_pct` — three minutes, spanned by the closes of the 1m bars inside it.
pub(super) const SPAN_3M: Timeframe = Timeframe::from_naive(3, TimeframeDesignator::Minutes);

#[derive(Clone, Copy, Debug)]
pub struct PriceSnap {
	pub current: f64,
	pub change_3m_pct: f64,
	pub change_1d_pct: f64,
}

flat_fields!(PriceSnap[current, change_3m_pct, change_1d_pct]);

impl Glance for PriceSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.4} 1d {:+.2}%", self.current, self.change_1d_pct)
	}
}

/// SPL backtest mode: the 3m delta comes off the last three closed 1m bars (the live Trades window
/// is a live-only fidelity choice), the 1d delta off 24 closed 1h closes.
#[derive(Clone, Default)]
pub struct Price {
	buf: Vec<Option<PriceSnap>>,
}
impl Cell for Price {
	type Out<'t> = &'t [Option<PriceSnap>];
}
impl Node for Price {
	type Deps = (Buffering<Bar1m, { Horizon::Span(SPAN_3M) }>, Buffering<Bar1h, REACH_1D>);

	fn advance<'t>(&'t mut self, (m1, h1): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for (b, w3) in m1.fresh().iter().zip(m1.trailing()) {
			let closed_1h = closed_by(h1.all(), b.ts_close);
			let day_ago = b.ts_close - SPAN_1D.duration().as_nanos() as i64;
			// The close standing a day back is the first one after `day_ago`; index 0 means the retained
			// run does not reach behind it, so there is nothing a day old to compare against yet.
			let oldest_1h = closed_1h.iter().position(|h| h.ts_close > day_ago).filter(|&i| i > 0).map(|i| closed_1h[i].close);
			self.buf.push(match (w3, oldest_1h) {
				(Some(w3), Some(oldest_1h)) => {
					let base_open = w3[0].open;
					(base_open > 0.0 && oldest_1h != 0.0).then(|| PriceSnap {
						current: b.close,
						change_3m_pct: (b.close - base_open) / base_open * 100.0,
						change_1d_pct: (b.close - oldest_1h) / oldest_1h * 100.0,
					})
				}
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(Price, Option<PriceSnap>);
