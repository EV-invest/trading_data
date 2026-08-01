use trading_data::{Cell, DepOuts, Folding, Horizon, Node, slice_nudge};

use super::{
	bar::Bar1m,
	change_1d::Change1d,
	latest,
	rsi::{Rsi, RsiValues},
	screened::Screened,
};
use crate::config::{Screen, strategy};

/// Top gainer: overbought on 4h while up on the day. [`Bar1m`] is the screening clock, so a
/// verdict is reached once a minute exactly as in SPL — the rate comes from this node's own inputs.
#[derive(Clone, Default)]
pub struct RsiScreener {
	rsi: Option<RsiValues>,
	buf: Vec<Option<Screened>>,
}
impl Cell for RsiScreener {
	type Out<'t> = &'t [Option<Screened>];
}
impl Node for RsiScreener {
	/// The cached RSI level stands until the next publish, however many minutes that takes.
	type Deps = (Bar1m, Change1d, Folding<Rsi, { Horizon::Unbounded }>);

	fn advance<'t>(&'t mut self, (bars, change_1d, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), change_1d.len(), "Bar1m/Change1d rate mismatch");
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` reads the two screeners as one
		// signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Rsi(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.rsi, rsi, bars.len());
		for (b, change) in bars.iter().zip(change_1d) {
			self.buf.push(match (change, self.rsi) {
				(Some(change), Some(rsi)) => (rsi.actual > c.rsi_threshold && *change > *c.price_percent).then_some(Screened { ts_ns: b.close_ns(Bar1m::TF) }),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(RsiScreener, Option<Screened>);
