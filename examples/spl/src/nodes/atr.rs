use trading_data::{Cell, Emit, EmitOuts, Folding, Horizon, WilderAtr, slice_nudge};

use super::Bar1m;
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
impl Emit for Atr {
	/// A Wilder recurrence reaches to the start of the run.
	type Deps = (Folding<Bar1m, { Horizon::Unbounded }>,);

	fn emit(&mut self, (bars,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in bars {
			out.push(self.atr.update(b.high, b.low, b.close));
		}
	}
}
slice_nudge!(Atr, Option<f64>);
