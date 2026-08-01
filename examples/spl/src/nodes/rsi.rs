use core::fmt;

use trading_data::{Cell, DepOuts, Glance, Node, Plot, rsi, slice_nudge};

use super::{avg_gain::AvgGain, avg_loss::AvgLoss};
use crate::config::strategy;

#[derive(Clone, Copy, Debug)]
pub struct RsiValues {
	pub actual: f64,
	pub smooth: f64,
}

flat_fields!(RsiValues[actual, smooth]);

impl Glance for RsiValues {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{} {:.1}", strategy().indies.rsi.timeframe, self.actual)
	}
}

/// Nautilus's `ExponentialMovingAverage`: seeded on the first sample, warm after `period` of them.
#[derive(Clone)]
struct Ema {
	period: usize,
	value: f64,
	seen: usize,
}
impl Ema {
	fn new(period: usize) -> Self {
		assert!(period > 0);
		Self { period, value: 0.0, seen: 0 }
	}

	fn update(&mut self, x: f64) -> Option<f64> {
		let alpha = 2.0 / (self.period as f64 + 1.0);
		self.value = if self.seen == 0 { x } else { alpha * x + (1.0 - alpha) * self.value };
		self.seen += 1;
		(self.seen >= self.period).then_some(self.value)
	}
}

/// Wilder RSI, EMA-smoothed. Warmth is `base_len + smooth_len` closed bars, which is exactly when
/// both stages are warm: the averages need `base_len` deltas, and only then does the EMA start
/// seeing values.
#[derive(Clone)]
pub struct Rsi {
	smooth: Ema,
	buf: Vec<Option<RsiValues>>,
}
impl Default for Rsi {
	fn default() -> Self {
		Self {
			smooth: Ema::new(strategy().indies.rsi.smooth_len),
			buf: Vec::new(),
		}
	}
}
impl Cell for Rsi {
	type Out<'t> = &'t [Option<RsiValues>];
}
impl Node for Rsi {
	type Deps = (AvgGain, AvgLoss);

	// No threshold guide: the trigger is a `config.nix` value and `Plot` is a const, so drawing
	// one here would pin a number the config is free to move.
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 100.0)),
		labels: &["actual", "smooth"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (gain, loss): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(gain.len(), loss.len(), "AvgGain/AvgLoss rate mismatch");
		self.buf.clear();
		for (g, l) in gain.iter().zip(loss) {
			self.buf.push(g.zip(*l).and_then(|(g, l)| {
				let actual = rsi(g, l);
				self.smooth.update(actual).map(|smooth| RsiValues { actual, smooth })
			}));
		}
		&self.buf
	}
}
slice_nudge!(Rsi, Option<RsiValues>);
