use trading_data::{Cell, DepOuts, Gate, Node, Sampling, node, value_nudge};
use v_utils::*;

use super::momentum::Momentum;
use crate::config::{Screen, strategy};

/// Pine's overvalued zone at momentum's leg. The verdict is per *closed 1m bar*, so a tick that
/// closed none has screened nothing — which is the empty-batch guard below, and the reason this is
/// not a bare read of the sampled level.
#[derive(Clone, Default)]
pub struct StdScreener;
impl Cell for StdScreener {
	type Out<'t> = bool;
}
#[node]
impl Node for StdScreener {
	/// The sampled momentum level stands until the next publish, however many minutes that takes.
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Sampling<Momentum>);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		let Screen::Std(c) = strategy().screen else {
			panic!("the graph is wired for StdScreener; config.nix names {:?}", strategy().screen)
		};
		!bars.is_empty() && momentum.is_some_and(|m| m > c.fast_overvalued)
	}
}
impl Gate for StdScreener {}
value_nudge!(StdScreener);
