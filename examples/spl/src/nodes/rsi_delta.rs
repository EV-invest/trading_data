use trading_data::{Cell, DepOuts, Horizon, Node, slice_nudge};

use super::bar::{Bar1h, Bar4h, Bar5m, Bar15m};
use crate::config::strategy;

/// Close-to-close change on the timeframe `indies.rsi.timeframe` names — the one series both Wilder
/// averages are taken of, so the config is read here and nowhere downstream. Every wired bar series
/// is a candidate input, which is why they are all deps.
///
/// Rate-changing on the very first bar: a change needs two closes.
#[derive(Clone)]
pub struct RsiDelta {
	prev_close: Option<f64>,
	buf: Vec<f64>,
}
/// `graph!` builds through `Default` and `main` builds the graph right after `Config::load`, so this
/// is the first instant a config naming a series the graph does not wire can be rejected — minutes
/// before the first bar of that series would have closed and reached `advance`.
impl Default for RsiDelta {
	fn default() -> Self {
		let tf = strategy().indies.rsi.timeframe;
		assert!(
			[Bar5m::TF, Bar15m::TF, Bar1h::TF, Bar4h::TF].contains(&tf),
			"indies.rsi.timeframe = {tf}: this graph wires {}/{}/{}/{} bars and no others. Which series an indie runs on is wiring, not a knob.",
			Bar5m::TF,
			Bar15m::TF,
			Bar1h::TF,
			Bar4h::TF
		);
		Self { prev_close: None, buf: Vec::new() }
	}
}
impl Cell for RsiDelta {
	type Out<'t> = &'t [f64];
}
impl Node for RsiDelta {
	type Deps = (Bar5m, Bar15m, Bar1h, Bar4h);

	/// Only the previous close is held.
	const HORIZON: Horizon = Horizon::Elems(1);

	fn advance<'t>(&'t mut self, (m5, m15, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let bars = match strategy().indies.rsi.timeframe {
			Bar5m::TF => m5,
			Bar15m::TF => m15,
			Bar1h::TF => h1,
			Bar4h::TF => h4,
			_ => unreachable!("`Default` asserted the timeframe against the series this node wires"),
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
