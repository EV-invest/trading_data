use trading_data::{Cell, Emit, EmitOuts, Folding, Horizon, slice_nudge};

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
#[derive(Clone)]
pub struct RsiDelta {
	prev_close: Option<f64>,
}
/// `graph!` builds through `Default` and `main` builds the graph right after `Config::load`, so this
/// is the first instant a config naming a series the graph does not wire can be rejected — minutes
/// before the first bar of that series would have closed and reached `emit`.
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
		Self { prev_close: None }
	}
}
impl Cell for RsiDelta {
	type Out<'t> = &'t [f64];
}
impl Emit for RsiDelta {
	type Deps = (Folding<Bar5m, PREV>, Folding<Bar15m, PREV>, Folding<Bar1h, PREV>, Folding<Bar4h, PREV>);

	fn emit(&mut self, (m5, m15, h1, h4): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		let bars = match strategy().indies.rsi.timeframe {
			Bar5m::TF => m5,
			Bar15m::TF => m15,
			Bar1h::TF => h1,
			Bar4h::TF => h4,
			_ => unreachable!("`Default` asserted the timeframe against the series this node wires"),
		};
		for b in bars {
			if let Some(prev) = self.prev_close.replace(b.close) {
				out.push(b.close - prev);
			}
		}
	}
}
slice_nudge!(RsiDelta, f64);
