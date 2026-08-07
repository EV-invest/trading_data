use trading_data::{Cell, Decides, Expr, Gate, Sampling, Vars, constant, gt, node, value_nudge};
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
impl Decides for StdScreener {
	/// `Bars<1m>` drives — the verdict is per closed 1m bar, so a tick that closed none has screened
	/// nothing, and that is the kernel's reading of an empty run rather than a test inside the body.
	/// Dropping it would make the gate a level at the read clock instead of the minute pulse
	/// `Classify` takes its own rate from. The sampled momentum level stands until the next publish,
	/// however many minutes that takes.
	type Deps = (trading_data::Bars<{ TF_1MIN }>, Sampling<Momentum<{ TF_5MIN }, 181>>);

	fn body(&self, v: Vars) -> impl Expr {
		let Screen::Std(c) = strategy().screen else {
			panic!("the graph is wired for StdScreener; config.nix names {:?}", strategy().screen)
		};
		// an unpublished momentum is NaN, which compares false either way — the same decline
		// `is_some_and` was.
		gt(v.get::<5>(), constant(c.fast_overvalued))
	}
}
impl Gate for StdScreener {}
value_nudge!(StdScreener);
