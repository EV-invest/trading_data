use trading_data::{Cell, Folding, RunOuts, Runs, Unbounded, WilderAtr, node, slice_nudge};
use v_utils::*;

use crate::config::strategy;

/// Wilder ATR(14) on 1m bars. An indie in its own right rather than an execution-owned indicator:
/// that is what removed SPL's per-situation bar subscribe/unsubscribe flicker.
#[derive(Clone)]
pub struct Atr {
	atr: WilderAtr,
}
impl Default for Atr {
	fn default() -> Self {
		Self {
			atr: WilderAtr::new(strategy().indies.atr.period),
		}
	}
}
impl Cell for Atr {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Runs for Atr {
	/// A Wilder recurrence reaches to the start of the run.
	type Deps = (Folding<trading_data::Bars<{ TF_1MIN }>, Unbounded>,);

	const WHY: &'static str = "a recurrence carried across elements, which the `Fold` kernel is not built for yet";

	fn emit(&mut self, (bars,): RunOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in bars {
			out.push(self.atr.update(b.high, b.low, b.close));
		}
	}
}
slice_nudge!(Atr, Option<f64>);
