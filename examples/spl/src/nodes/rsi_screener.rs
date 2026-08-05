use trading_data::{Cell, DepOuts, Gate, Node, Sampling, node, value_nudge};
use v_utils::*;

use super::{Rsi, change_1d::Change1d};
use crate::config::{Screen, strategy};

/// Top gainer: overbought on 4h while up on the day. The 1m series is the screening clock, so a
/// verdict is reached once a minute exactly as in SPL — the rate comes from this node's own inputs,
/// and a tick that closed no bar has screened nothing.
#[derive(Clone, Default)]
pub struct RsiScreener;
impl Cell for RsiScreener {
	type Out<'t> = bool;
}
#[node]
impl Node for RsiScreener {
	/// The sampled RSI level stands until the next publish, however many minutes that takes.
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Change1d, Sampling<Rsi>);

	fn advance<'t>(&'t mut self, (bars, change_1d, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), change_1d.len(), "Bar:1m/Change1d rate mismatch");
		let Screen::Rsi(c) = strategy().screen else {
			panic!("the graph is wired for RsiScreener; config.nix names {:?}", strategy().screen)
		};
		let Some(rsi) = rsi else { return false };
		rsi.actual > c.rsi_threshold && change_1d.iter().flatten().any(|change| *change > *c.price_percent)
	}
}
impl Gate for RsiScreener {}
value_nudge!(RsiScreener);
