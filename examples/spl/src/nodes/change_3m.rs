use trading_data::{Buffering, Cell, DepOuts, Horizon, Node, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

use super::bar::Bar1m;

/// Reach behind the change — three minutes, spanned by the opens of the 1m bars inside it.
pub(super) const SPAN_3M: Timeframe = Timeframe::from_naive(3, TimeframeDesignator::Minutes);

/// Percent change over the trailing three minutes, off the closed 1m bars inside it. SPL's backtest
/// mode: reading it off a live Trades window instead is a live-only fidelity choice.
#[derive(Clone, Default)]
pub struct Change3m {
	buf: Vec<Option<f64>>,
}
impl Cell for Change3m {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Change3m {
	type Deps = (Buffering<Bar1m, { Horizon::Span(SPAN_3M) }>,);

	/// The window is the dep's to retain.
	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (m1,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for (b, w3) in m1.fresh().iter().zip(m1.trailing()) {
			self.buf.push(w3.and_then(|w3| {
				let base_open = w3[0].open;
				(base_open > 0.0).then(|| (b.close - base_open) / base_open * 100.0)
			}));
		}
		&self.buf
	}
}
slice_nudge!(Change3m, Option<f64>);
