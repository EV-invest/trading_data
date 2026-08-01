use trading_data::{Cell, Emit, EmitOuts, Folding, Horizon, Wilder, slice_nudge};

use super::rsi_delta::RsiDelta;
use crate::config::strategy;

/// RSI's denominator: the Wilder average of the down moves as a positive magnitude, warm after
/// `indies.rsi.base_len` deltas.
#[derive(Clone)]
pub struct AvgLoss {
	avg: Wilder,
}
impl Default for AvgLoss {
	fn default() -> Self {
		Self {
			avg: Wilder::new(strategy().indies.rsi.base_len),
		}
	}
}
impl Cell for AvgLoss {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for AvgLoss {
	/// A Wilder recurrence reaches to the start of the run.
	type Deps = (Folding<RsiDelta, { Horizon::Unbounded }>,);

	fn emit(&mut self, (deltas,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for d in deltas {
			out.push(self.avg.update((-d).max(0.0)));
		}
	}
}
slice_nudge!(AvgLoss, Option<f64>);
