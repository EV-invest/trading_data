//! `config.nix`, evaluated the way scam_pump_liqs evaluates its own: `nix eval --json` into serde.
//! Nix over TOML because the knobs are expressions (`"2%"`, `"1m"`, commented-out screener arms) and
//! because it is the format the strategy this ports already speaks.
//!
//! The strategy half is read by graph nodes, which `graph!` builds through `Default` and so cannot
//! take constructor arguments — hence the process-wide [`strategy`] handle, set once before the
//! first tick.

use std::{path::Path, process::Command, sync::OnceLock};

use serde::Deserialize;
use trading_data::LatencyConfig;
use trading_data_core::PrecisionPriceQty;
use v_utils::{Timeframe, macros::CompactFormatNamed, percent::PercentU};

/// Panics if read before [`Config::load`] — a node running without its configuration is a wiring
/// bug, not a case for defaults.
pub fn config() -> &'static Config {
	CONFIG.get().expect("Config::load must run before the graph ticks")
}
/// The strategy knobs, for the nodes.
pub fn strategy() -> &'static Strategy {
	&config().strategy
}
#[derive(Debug, Deserialize)]
pub struct Config {
	pub situation: Situation,
	pub strategy: Strategy,
	pub backtest: Backtest,
}
impl Config {
	/// Evaluates `config.nix` and publishes it to [`config`].
	pub fn load(path: &Path) -> &'static Self {
		assert!(path.extension().is_some_and(|e| e == "nix"), "config '{}' must be a .nix file", path.display());
		let out = Command::new("nix")
			.args(["eval", "--json", "--impure", "--expr"])
			.arg(format!("import {}", path.display()))
			.output()
			.unwrap_or_else(|e| panic!("failed to run `nix eval` (is nix installed?): {e}"));
		assert!(out.status.success(), "nix evaluation of '{}' failed:\n{}", path.display(), String::from_utf8_lossy(&out.stderr));
		let parsed: Self = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("config '{}' does not match the expected shape: {e}", path.display()));
		// A buffer's reach is a const capacity, the lookback a runtime knob. Failing here names the
		// key; failing at the window would only look like one that silently never warms.
		let lookback = parsed.strategy.indies.momentum.lookback;
		assert!(
			lookback as u64 + 1 <= crate::nodes::MOM_PERIODS,
			"indies.momentum.lookback = {lookback} wants a {}-bar window, over the {} periods the graph retains",
			lookback + 1,
			crate::nodes::MOM_PERIODS
		);
		assert!(CONFIG.set(parsed).is_ok(), "Config::load runs once");
		config()
	}
}

/// One backtest target: which instrument, and the window replayed. Indies warm inside it — a cold
/// node declines, so the strategy simply does not fire until it has the history it reads.
#[derive(Debug, Deserialize)]
pub struct Situation {
	pub pair: String,
	pub bybit_symbol: String,
	pub coingecko_id: String,
	pub precision: PrecisionPriceQty,
	#[serde(with = "utc_date")]
	pub start: jiff::civil::Date,
	/// Exclusive.
	#[serde(with = "utc_date")]
	pub end: jiff::civil::Date,
}
#[derive(Debug, Deserialize)]
pub struct Strategy {
	pub indies: Indies,
	pub screen: Screen,
	pub classification: Classification,
}
#[derive(Debug, Deserialize)]
pub struct Indies {
	pub rsi: Rsi,
	pub momentum: Momentum,
	pub atr: Atr,
}
/// `Display` is the indie's signature — `rsi:t5m:b14:s14` — so anything naming this indie names the
/// timeframe with it. There is no default: the timeframe RSI runs on is the strategy.
#[derive(Clone, CompactFormatNamed, Copy, Debug, Deserialize)]
pub struct Rsi {
	pub timeframe: Timeframe,
	pub base_len: usize,
	pub smooth_len: usize,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Momentum {
	pub fast: Timeframe,
	/// The second, slower leg. `null` drops it entirely — never tracked, never warmed, and the
	/// screener's slow leg is then vacuous. There is no default: a leg exists only if named.
	pub slow: Option<Timeframe>,
	pub lookback: usize,
	pub risk_free_rate: f64,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Atr {
	pub period: usize,
}
/// Exactly one screener is configured; the other node stays inert. Both still declare their own
/// `type Deps`, so which one runs is all this decides — not which indie is readable.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Screen {
	Rsi(RsiScreen),
	Std(StdScreen),
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct RsiScreen {
	pub rsi_threshold: f64,
	pub price_percent: PercentU,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct StdScreen {
	pub fast_overvalued: f64,
	pub slow_overvalued: f64,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Classification {
	pub drain_grace: Timeframe,
	pub liquidations: Liquidations,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Liquidations {
	pub atr_sl_x: f64,
	pub atr_tp_x: f64,
	pub trail_pct: PercentU,
	pub trail_severity: PercentU,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Backtest {
	/// Equity the position sizes off. SPL reads live portfolio equity; the simulated venue's seed is
	/// the honest stand-in.
	pub starting_balance: f64,
	pub arrival_latency: ArrivalLatency,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct ArrivalLatency {
	pub p68: Timeframe,
	pub p95: Timeframe,
	pub p997: Timeframe,
}
static CONFIG: OnceLock<Config> = OnceLock::new();

impl From<ArrivalLatency> for LatencyConfig {
	fn from(l: ArrivalLatency) -> Self {
		Self {
			p68: l.p68.duration(),
			p95: l.p95.duration(),
			p997: l.p997.duration(),
			seed: 0,
		}
	}
}

/// `"YYYY-MM-DD"`, the same form SPL's situations file accepts.
mod utc_date {
	use std::str::FromStr as _;

	use serde::{Deserialize as _, Deserializer};

	pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<jiff::civil::Date, D::Error> {
		let s = String::deserialize(d)?;
		jiff::civil::Date::from_str(&s).map_err(serde::de::Error::custom)
	}
}
