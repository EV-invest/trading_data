use trading_data::{Cell, DepOuts, Node, slice_nudge};

use super::{
	bar::Bar1m,
	latest,
	momentum::{MomSnap, Momentum},
	screened::Screened,
};
use crate::config::{Screen, strategy};

/// Pine's overvalued zone at both of momentum's legs. The slow leg is vacuously satisfied when the
/// config names no slow timeframe.
#[derive(Clone, Default)]
pub struct StdScreener {
	momentum: Option<MomSnap>,
	buf: Vec<Option<Screened>>,
}
impl Cell for StdScreener {
	type Out<'t> = &'t [Option<Screened>];
}
impl Node for StdScreener {
	type Deps = (Bar1m, Momentum);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` reads the two screeners as one
		// signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Std(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.momentum, momentum);
		for b in bars {
			self.buf.push(self.momentum.and_then(|m| {
				// The vacuous slow leg must come from `indies.momentum.slow` and nothing else: `Momentum`
				// declines to publish at all when a configured slow leg is degenerate, so an absent Sharpe
				// here would otherwise let a wiring bug read as an unconditional hit.
				assert_eq!(m.slow.is_some(), strategy().indies.momentum.slow.is_some(), "a slow Sharpe disagrees with indies.momentum.slow");
				let slow = m.slow.is_none_or(|x| x > c.slow_overvalued);
				(slow && m.fast > c.fast_overvalued).then_some(Screened { ts_ns: b.close_ns(Bar1m::TF) })
			}));
		}
		&self.buf
	}
}
slice_nudge!(StdScreener, Option<Screened>);
