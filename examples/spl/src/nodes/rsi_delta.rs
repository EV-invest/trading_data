use trading_data::{Cell, DepOuts, Folding, Horizon, Node, slice_nudge};

use super::bar::{Bar1h, Bar4h, Bar5m, Bar15m};
use crate::config::strategy;

/// Only the previous close is held — of whichever series the config names, which is why every
/// candidate carries it.
const PREV: Horizon = Horizon::Elems(1);

/// Close-to-close change on the timeframe `indies.rsi.timeframe` names — the one series both Wilder
/// averages are taken of, so the config is read here and nowhere downstream. Every wired bar series
/// is a candidate input, which is why they are all deps.
///
/// Rate-changing on the very first bar: a change needs two closes.
#[derive(Clone, Default)]
pub struct RsiDelta {
	prev_close: Option<f64>,
	buf: Vec<f64>,
}
impl Cell for RsiDelta {
	type Out<'t> = &'t [f64];
}
impl Node for RsiDelta {
	type Deps = (Folding<Bar5m, PREV>, Folding<Bar15m, PREV>, Folding<Bar1h, PREV>, Folding<Bar4h, PREV>);

	fn advance<'t>(&'t mut self, (m5, m15, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let cfg = strategy().indies.rsi;
		let bars = match cfg.timeframe {
			Bar5m::TF => m5,
			Bar15m::TF => m15,
			Bar1h::TF => h1,
			Bar4h::TF => h4,
			_ => panic!("`{cfg}`: the graph wires {}/{}/{}/{} bars and no others", Bar5m::TF, Bar15m::TF, Bar1h::TF, Bar4h::TF),
		};
		for b in bars {
			if let Some(prev) = self.prev_close.replace(b.close) {
				self.buf.push(b.close - prev);
			}
		}
		&self.buf
	}
}
slice_nudge!(RsiDelta, f64);
