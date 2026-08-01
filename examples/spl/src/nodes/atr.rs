use trading_data::{Cell, DepOuts, Node, WilderAtr, slice_nudge};

use super::bar::Bar1m;
use crate::config::strategy;

/// Wilder ATR(14) on 1m bars. An indie in its own right rather than an execution-owned indicator:
/// that is what removed SPL's per-situation bar subscribe/unsubscribe flicker.
#[derive(Clone)]
pub struct Atr {
	atr: WilderAtr,
	buf: Vec<Option<f64>>,
}
impl Default for Atr {
	fn default() -> Self {
		Self {
			atr: WilderAtr::new(strategy().indies.atr.period),
			buf: Vec::new(),
		}
	}
}
impl Cell for Atr {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Atr {
	type Deps = (Bar1m,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			self.buf.push(self.atr.update(b.high, b.low, b.close));
		}
		&self.buf
	}
}
slice_nudge!(Atr, Option<f64>);
