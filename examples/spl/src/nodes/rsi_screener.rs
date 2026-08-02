use trading_data::{Cell, DepOuts, Folding, Gate, Horizon, Node, node, value_nudge};

use super::{Bar1m, Rsi, RsiValues, change_1d::Change1d, latest};
use crate::config::{Screen, strategy};

/// Top gainer: overbought on 4h while up on the day. [`Bar1m`] is the screening clock, so a
/// verdict is reached once a minute exactly as in SPL — the rate comes from this node's own inputs,
/// and a tick that closed no bar has screened nothing.
#[derive(Clone, Default)]
pub struct RsiScreener {
	rsi: Option<RsiValues>,
}
impl Cell for RsiScreener {
	type Out<'t> = bool;
}
#[node]
impl Node for RsiScreener {
	/// The cached RSI level stands until the next publish, however many minutes that takes.
	type Deps = (Bar1m, Change1d, Folding<Rsi, { Horizon::Unbounded }>);

	fn advance<'t>(&'t mut self, (bars, change_1d, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), change_1d.len(), "Bar1m/Change1d rate mismatch");
		let Screen::Rsi(c) = strategy().screen else {
			panic!("the graph is wired for RsiScreener; config.nix names {:?}", strategy().screen)
		};
		latest(&mut self.rsi, rsi, bars.len());
		let Some(rsi) = self.rsi else { return false };
		rsi.actual > c.rsi_threshold && change_1d.iter().flatten().any(|change| *change > *c.price_percent)
	}
}
impl Gate for RsiScreener {}
value_nudge!(RsiScreener);
