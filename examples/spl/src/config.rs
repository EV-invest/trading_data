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
use v_utils::{Timeframe, percent::PercentU};

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
		assert!(CONFIG.set(parsed).is_ok(), "Config::load runs once");
		config()
	}
}

/// One backtest target: which instrument, and the trading window. Data before `start` is warmup —
/// the graph consumes it silently and nothing may fire on it, SPL's `Screener::trade_start`.
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
impl Strategy {
	/// Closed bars of each timeframe the required indies need, as `(minutes, count)` — SPL's
	/// `merge_warmup_requirements` over `screen.shallow_topics()`. The universal set is always
	/// present; the configured screener's own indie joins it.
	fn warmup_bars(&self) -> Vec<(u64, usize)> {
		// price: 24 closed 1h for the 1d change, 3 closed 1m for the 3m change.
		// volume: latest closed bar per timeframe. atr: one Wilder period of 1m.
		let mut out = vec![(60, 24), (1, 3), (1, 1), (60, 1), (240, 1), (1, self.indies.atr.period)];
		let required = self.indies.rsi.base_len + self.indies.rsi.smooth_len;
		let closes = self.indies.momentum.lookback + 1;
		match self.screen {
			Screen::Rsi(_) => out.extend([(5, required), (15, required), (60, required), (240, required)]),
			Screen::Std(_) => {
				out.push((5, closes));
				if self.indies.momentum.use_4h {
					out.push((240, closes));
				}
			}
		}
		out
	}

	/// Whole UTC days of history the required indies need before `start`. SPL's
	/// `warmup_buffer_days`: the widest single requirement, rounded up to a day.
	pub fn warmup_days(&self) -> i64 {
		let widest = self.warmup_bars().iter().map(|(m, n)| m * 60 * *n as u64).max().expect("the universal set is never empty");
		(widest / 86_400) as i64 + 1
	}
}

#[derive(Debug, Deserialize)]
pub struct Indies {
	pub rsi: Rsi,
	pub momentum: Momentum,
	pub atr: Atr,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Rsi {
	pub base_len: usize,
	pub smooth_len: usize,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Momentum {
	pub lookback: usize,
	pub risk_free_rate: f64,
	/// `false` drops the 4h slot entirely — never tracked, never warmed, and the screener's 4h leg
	/// is then vacuous.
	pub use_4h: bool,
}
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct Atr {
	pub period: usize,
}
/// Exactly one screener is configured; the other node stays inert. Which one also decides whose
/// indie joins the universal set in `ShallowSnap`'s required fields.
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
	pub overvalued_threshold_4h: f64,
	pub overvalued_threshold_5m: f64,
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
/// Days the run covers, warmup first: `[start - warmup_days, end)`.
pub fn days(situation: &Situation, warmup_days: i64) -> Vec<jiff::civil::Date> {
	let mut d = situation.start.checked_sub(jiff::Span::new().days(warmup_days)).expect("date in range");
	let mut out = Vec::new();
	while d < situation.end {
		out.push(d);
		d = d.tomorrow().expect("date in range");
	}
	out
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
