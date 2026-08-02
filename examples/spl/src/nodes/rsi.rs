use trading_data::RsiSpec;

use super::Bar5m;
use crate::config::strategy;

/// Which series the RSI chain runs on. `indies.rsi.timeframe` still *names* it — the timeframe is
/// half the indie's signature, and everything reporting the indie reads it from there — but which
/// series an indie runs on is wiring, not a knob, so the config is checked against this in
/// [`crate::config::Config::load`] rather than dispatched on.
pub type RsiSeries = Bar5m;

pub type RsiDelta = trading_data::RsiDelta<RsiSeries>;
pub type AvgGain = trading_data::AvgGain<RsiSeries, Knobs>;
pub type AvgLoss = trading_data::AvgLoss<RsiSeries, Knobs>;
pub type Rsi = trading_data::Rsi<RsiSeries, Knobs>;
/// The two Wilder lengths, out of `config.nix`.
pub struct Knobs;
impl RsiSpec for Knobs {
	fn base_len() -> usize {
		strategy().indies.rsi.base_len
	}

	fn smooth_len() -> usize {
		strategy().indies.rsi.smooth_len
	}
}
