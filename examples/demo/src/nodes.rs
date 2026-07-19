//! SPL replica graph: Trades → Bar1m → (Cvd, Rsi14, Atr14, Momentum, VolUsd1h, Lambda1m) → Screener → Classify.
//! All outs are value types; the `graph!` field order is the topo order.

use core::fmt;

use trading_data::{Cell, DepOuts, Flat, Gate, Glance, Guide, Ink, Node, Sketch, Trade, WilderAtr, WilderRsi};
use v_utils::trades::Side;

pub const MOM_WINDOW: usize = 60;
/// Same value as [`MOM_WINDOW`], different identity: λ's window is its own tunable.
pub const LAMBDA_WINDOW: usize = 60;
// Tuned to TAO-USDT 2025-01-03: the goal is the mechanism firing, not signal quality.
const MOM_TH: f64 = 1.0;
const RSI_HI: f64 = 65.0;
const RSI_LO: f64 = 35.0;
const VOL_TH: f64 = 100_000.0;
const STREAK_N: u32 = 2;
const MOM_HIGH_BAND: f64 = 3.0;
const MOM_MID_BAND: f64 = 2.0;

fn signed_notional(t: &Trade) -> f64 {
	let notional = t.price * t.qty;
	match t.side {
		Side::Buy => notional,
		Side::Sell => -notional,
	}
}

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_open: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub vol_quote: f64,
	/// Σ signed `price*qty` (Buy = +, Sell = −) over the bar.
	pub flow_quote: f64,
}

impl Flat for Bar {
	const DIMS: &'static [usize] = &[6];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.open, self.high, self.low, self.close, self.vol_quote, self.flow_quote]);
		true
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		let mut r = *self;
		*match slot {
			0 => &mut r.open,
			1 => &mut r.high,
			2 => &mut r.low,
			3 => &mut r.close,
			4 => &mut r.vol_quote,
			5 => &mut r.flow_quote,
			_ => unreachable!("LEN = 6"),
		} += h;
		r
	}
}

impl Glance for Bar {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "close {}", self.close)
	}
}

/// Interpretation of a [`Classify`] dist — the wire is the `[f64; 4]` itself, ordered
/// `[None, Liquidations, MmClosing, Manipulation]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
	None,
	Liquidations,
	MmClosing,
	Manipulation,
}

impl Category {
	pub fn argmax(dist: [f64; 4]) -> Self {
		let (i, _) = dist.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)).expect("4 elements");
		[Category::None, Category::Liquidations, Category::MmClosing, Category::Manipulation][i]
	}
}

/// The 4-way category distribution — the wire is its `[f64; 4]`, ordered as [`Category`] above.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Dist(pub [f64; 4]);

impl Flat for Dist {
	const DIMS: &'static [usize] = &[4];

	fn flat(&self, out: &mut [f64]) -> bool {
		self.0.flat(out)
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		Dist(self.0.nudge(slot, h))
	}
}

impl Glance for Dist {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let p = self.0.iter().copied().fold(f64::MIN, f64::max);
		write!(f, "{:?}: {:.0}%", Category::argmax(self.0), p * 100.0)
	}
}

pub struct Trades;
impl Cell for Trades {
	type Out<'t> = Option<Trade>;
}

#[derive(Clone, Default)]
pub struct Bar1m {
	acc: Option<Bar>,
}
impl Cell for Bar1m {
	type Out<'t> = Option<Bar>;
}
impl Node for Bar1m {
	type Deps = (Trades,);

	fn advance<'t>(&mut self, (t,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let t = t?;
		let ts_open = t.ts_event - t.ts_event.rem_euclid(60_000_000_000);
		match &mut self.acc {
			Some(b) if b.ts_open == ts_open => {
				b.high = b.high.max(t.price);
				b.low = b.low.min(t.price);
				b.close = t.price;
				b.vol_quote += t.price * t.qty;
				b.flow_quote += signed_notional(&t);
				None
			}
			acc => {
				let done = *acc;
				*acc = Some(Bar {
					ts_open,
					open: t.price,
					high: t.price,
					low: t.price,
					close: t.price,
					vol_quote: t.price * t.qty,
					flow_quote: signed_notional(&t),
				});
				done
			}
		}
	}
}

/// Cumulative volume delta: running Σ signed notional over every trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
}
impl Cell for Cvd {
	type Out<'t> = Option<f64>;
}
impl Node for Cvd {
	type Deps = (Trades,);

	fn advance<'t>(&mut self, (t,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.sum += signed_notional(&t?);
		Some(self.sum)
	}
}

// The 14-period constants are part of these nodes' identity, so Default is honest.
#[derive(Clone)]
pub struct Rsi14(pub WilderRsi);
impl Default for Rsi14 {
	fn default() -> Self {
		Rsi14(WilderRsi::new(14))
	}
}
impl Cell for Rsi14 {
	type Out<'t> = Option<f64>;
}
impl Node for Rsi14 {
	type Deps = (Bar1m,);

	const SKETCH: Sketch = Sketch {
		range: Some((0.0, 100.0)),
		guides: &[
			Guide {
				label: "30",
				value: 30.0,
				ink: Ink::FAINT,
			},
			Guide {
				label: "70",
				value: 70.0,
				ink: Ink::FAINT,
			},
		],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0.update(bar?.close)
	}
}

#[derive(Clone)]
pub struct Atr14(pub WilderAtr);
impl Default for Atr14 {
	fn default() -> Self {
		Atr14(WilderAtr::new(14))
	}
}
impl Cell for Atr14 {
	type Out<'t> = Option<f64>;
}
impl Node for Atr14 {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let b = bar?;
		self.0.update(b.high, b.low, b.close)
	}
}

#[derive(Clone, Default)]
pub struct Momentum {
	prev_close: Option<f64> = None,
	returns: [f64; MOM_WINDOW] = [0.0; MOM_WINDOW],
	idx: usize = 0,
	filled: usize = 0,
}
impl Cell for Momentum {
	type Out<'t> = Option<f64>;
}
impl Node for Momentum {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let close = bar?.close;
		let prev = self.prev_close.replace(close)?;
		self.returns[self.idx] = close / prev - 1.0;
		self.idx = (self.idx + 1) % MOM_WINDOW;
		self.filled = (self.filled + 1).min(MOM_WINDOW);
		if self.filled < MOM_WINDOW {
			return None;
		}
		let n = MOM_WINDOW as f64;
		let mean = self.returns.iter().sum::<f64>() / n;
		let var = self.returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
		if var == 0.0 {
			return Some(0.0);
		}
		Some(mean / var.sqrt() * n.sqrt())
	}
}

#[derive(Clone)]
pub struct VolUsd1h {
	ring: [f64; 60],
	idx: usize,
}
impl Default for VolUsd1h {
	fn default() -> Self {
		Self { ring: [0.0; 60], idx: 0 }
	}
}
impl Cell for VolUsd1h {
	type Out<'t> = f64;
}
impl Node for VolUsd1h {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if let Some(b) = bar {
			self.ring[self.idx] = b.vol_quote;
			self.idx = (self.idx + 1) % self.ring.len();
		}
		self.ring.iter().sum()
	}
}

/// Kyle's λ: through-origin OLS of per-bar Δclose on signed flow, `λ = Σ(Δp·f) / Σ(f²)`.
#[derive(Clone, Default)]
pub struct Lambda1m {
	prev_close: Option<f64> = None,
	d_close: [f64; LAMBDA_WINDOW] = [0.0; LAMBDA_WINDOW],
	flow: [f64; LAMBDA_WINDOW] = [0.0; LAMBDA_WINDOW],
	idx: usize = 0,
	filled: usize = 0,
}
impl Cell for Lambda1m {
	type Out<'t> = Option<f64>;
}
impl Node for Lambda1m {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let b = bar?;
		let prev = self.prev_close.replace(b.close)?;
		self.d_close[self.idx] = b.close - prev;
		self.flow[self.idx] = b.flow_quote;
		self.idx = (self.idx + 1) % LAMBDA_WINDOW;
		self.filled = (self.filled + 1).min(LAMBDA_WINDOW);
		if self.filled < LAMBDA_WINDOW {
			return None;
		}
		let denom = self.flow.iter().map(|f| f * f).sum::<f64>();
		if denom == 0.0 {
			return Some(0.0);
		}
		Some(self.d_close.iter().zip(&self.flow).map(|(dp, f)| dp * f).sum::<f64>() / denom)
	}
}

/// Stateful hysteresis streak — deliberately non-vectorizable, like the SPL RsiScreener.
#[derive(Clone, Default)]
pub struct Screener {
	streak: u32,
}
impl Cell for Screener {
	type Out<'t> = bool;
}
impl Node for Screener {
	type Deps = (Momentum, Rsi14, VolUsd1h);

	fn advance<'t>(&mut self, (mom, rsi, vol): DepOuts<'t, Self>) -> Self::Out<'t> {
		// not-warm = closed
		let Some((mom, rsi)) = mom.zip(rsi) else { return false };
		let hit = mom.abs() > MOM_TH && !(RSI_LO..=RSI_HI).contains(&rsi) && vol > VOL_TH;
		self.streak = if hit { self.streak + 1 } else { 0 };
		self.streak >= STREAK_N
	}
}
impl Gate for Screener {}

#[derive(Clone, Default)]
pub struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Dist>;
}
impl Node for Classify {
	type Deps = (Momentum,);
	type When = (Screener,);

	const HISTORIC: bool = false;

	// Element order is the [`Dist`] wire order.
	const SKETCH: Sketch = Sketch {
		labels: &["None", "Liquidations", "MmClosing", "Manipulation"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&mut self, (mom,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let mom = mom.expect("Screener only opens once Momentum is warm");
		let category = if mom.abs() > MOM_HIGH_BAND {
			Category::Manipulation
		} else if mom.abs() > MOM_MID_BAND {
			Category::Liquidations
		} else {
			Category::MmClosing
		};
		let rest = (1.0 - 0.6) / 3.0;
		Some(Dist(
			[Category::None, Category::Liquidations, Category::MmClosing, Category::Manipulation].map(|c| if c == category { 0.6 } else { rest }),
		))
	}
}

trading_data::graph! {
	pub struct Graph;
	root Trades, event Trade;
	out TickOut;
	bar: Bar1m,
	cvd: Cvd,
	rsi: Rsi14,
	atr: Atr14,
	momentum: Momentum,
	vol_usd_1h: VolUsd1h,
	lambda: Lambda1m,
	screener: Screener,
	classified: Classify,
}
