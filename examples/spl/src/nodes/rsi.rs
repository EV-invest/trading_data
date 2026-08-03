use trading_data::RsiSpec;
use v_utils::Timeframe;

use super::{Bar5m, M5};
use crate::config::strategy;

/// The period [`RsiSeries`] is wired on. `indies.rsi.timeframe` is checked against it rather than
/// dispatched on — see the alias below.
pub const RSI_TF: Timeframe = M5;

trading_data::node_alias! {
	/// Which series the RSI chain runs on. `indies.rsi.timeframe` still *names* it — the timeframe is
	/// half the indie's signature, and everything reporting the indie reads it from there — but which
	/// series an indie runs on is wiring, not a knob, so the config is checked against this in
	/// [`crate::config::Config::load`] rather than dispatched on.
	pub RsiSeries = Bar5m;
}

trading_data::node_alias! { pub RsiDelta = trading_data::RsiDelta<RsiSeries>; }
trading_data::node_alias! { pub AvgGain = trading_data::AvgGain<RsiSeries, Knobs>; }
trading_data::node_alias! { pub AvgLoss = trading_data::AvgLoss<RsiSeries, Knobs>; }
trading_data::node_alias! { pub Rsi = trading_data::Rsi<RsiSeries, Knobs>; }
/// The two Wilder lengths, out of `config.nix`.
pub struct Knobs;
impl RsiSpec for Knobs {
	const NAME: &'static str = "Knobs";

	fn base_len() -> usize {
		strategy().indies.rsi.base_len
	}

	fn smooth_len() -> usize {
		strategy().indies.rsi.smooth_len
	}
}
