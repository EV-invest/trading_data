use trading_data::{Blind, Cell, DepOuts, Gate, Sampling, node, value_nudge};
use v_utils::*;

use super::{ChangeBack, Knobs};
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
impl Blind for RsiScreener {
	/// The sampled RSI level stands until the next publish, however many minutes that takes.
	type Deps = (
		trading_data::Bars<{ TF_1MIN }>,
		ChangeBack<{ TF_1MIN }, { TF_1H }, { TF_1D }, { Timeframe(TF_1D.0 + TF_1H.0) }>,
		Sampling<trading_data::Rsi<trading_data::Bars<{ TF_5MIN }>, Knobs>>,
	);

	const WHY: &'static str = "`Decides` reads a non-leading dep at its last element, and this asks whether *any* day-change in the whole run cleared the threshold — a quantifier no \
	                          `Expr` body states. `StdScreener` fits `Predicate` because its test is one comparison";

	fn advance<'t>(&'t mut self, (bars, change_1d, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), change_1d.len(), "Bar:1m/ChangeBack:1m/1d rate mismatch");
		let Screen::Rsi(c) = strategy().screen else {
			panic!("the graph is wired for RsiScreener; config.nix names {:?}", strategy().screen)
		};
		let Some(rsi) = rsi else { return false };
		rsi.actual.get().expect("`Present` says both legs are warm") > c.rsi_threshold && change_1d.iter().filter_map(|r| r.get()).any(|change| change > *c.price_percent)
	}
}
impl Gate for RsiScreener {}
value_nudge!(RsiScreener);
