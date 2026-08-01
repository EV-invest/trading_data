use trading_data::{Cell, DepOuts, Node, slice_nudge};

use super::{
	bar::Bar1m,
	latest,
	price::Price,
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
	type Deps = (Bar1m, Price, Rsi);

	fn advance<'t>(&'t mut self, (bars, price, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), price.len(), "Bar1m/Price rate mismatch");
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` reads the two screeners as one
		// signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Rsi(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.rsi, rsi);
		for (b, p) in bars.iter().zip(price) {
			self.buf.push(match (p, self.rsi) {
				(Some(p), Some(rsi)) => (rsi.actual > c.rsi_threshold && p.change_1d_pct > *c.price_percent).then_some(Screened { ts_ns: b.close_ns(Bar1m::TF) }),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(RsiScreener, Option<Screened>);
