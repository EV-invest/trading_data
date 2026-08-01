use trading_data::{Cell, DepOuts, Folding, Horizon, Node, Wilder, slice_nudge};

use super::rsi_delta::RsiDelta;
use crate::config::strategy;

/// RSI's numerator: the Wilder average of the up moves, warm after `indies.rsi.base_len` deltas.
#[derive(Clone)]
pub struct AvgGain {
	avg: Wilder,
	buf: Vec<Option<f64>>,
}
impl Default for AvgGain {
	fn default() -> Self {
		Self {
			avg: Wilder::new(strategy().indies.rsi.base_len),
			buf: Vec::new(),
		}
	}
}
impl Cell for AvgGain {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for AvgGain {
	/// A Wilder recurrence reaches to the start of the run.
	type Deps = (Folding<RsiDelta, { Horizon::Unbounded }>,);

	fn advance<'t>(&'t mut self, (deltas,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for d in deltas {
			self.buf.push(self.avg.update(d.max(0.0)));
		}
		&self.buf
	}
}
slice_nudge!(AvgGain, Option<f64>);
