use trading_data::RsiSpec;

use crate::config::strategy;

/// The two Wilder lengths, out of `config.nix`. Which series the chain runs on is not here: it is
/// the `Bars<..>` every naming site writes out, and `indies.rsi.timeframe` is checked against that
/// in [`crate::config::Config::load`] rather than dispatched on.
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
