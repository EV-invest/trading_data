//! SPL replica graph, batch-native: two roots (Trades, OiRoot) weave into
//! Bar1m → (Cvd, Rsi14, Atr14, Momentum, VolUsd1h, Lambda1m) → Screener → Classify, plus an
//! Oi-consuming OiChange node. Every node buffers its per-tick emissions and lends `&self.buf`;
//! rate is slice length, firing is element `Option`-ness. The `graph!` field order is topo order.

use core::fmt;

use trading_data::{
	Bump, Cell, DepOuts, Exact, Expr, Flat, Glance, Guide, Ink, Lanes, Node, Oi, OiRoot, Sketch, Symbolic, TradeCols, Trades, Vars, WilderAtr, WilderRsi, constant, slice_nudge,
};
use trading_data_core::Side;

const MINUTE: Exact = Exact::from_nanos(60_000_000_000);

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
const ATR_STOP_K: f64 = 3.0;

fn signed(side: Side, notional: f64) -> f64 {
	match side {
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
}

impl Bump for Bar {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		*match slot {
			0 => &mut self.open,
			1 => &mut self.high,
			2 => &mut self.low,
			3 => &mut self.close,
			4 => &mut self.vol_quote,
			5 => &mut self.flow_quote,
			_ => unreachable!("LEN = 6"),
		} += h;
		(self, h)
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
}

impl Bump for Dist {
	fn bump(self, slot: usize, h: f64) -> (Self, f64) {
		let (a, dh) = self.0.bump(slot, h);
		(Dist(a), dh)
	}
}

impl Glance for Dist {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let p = self.0.iter().copied().fold(f64::MIN, f64::max);
		write!(f, "{:?}: {:.0}%", Category::argmax(self.0), p * 100.0)
	}
}

/// Trades → 1m OHLC bars. Rate-changing: emits one non-optional bar per minute boundary crossed,
/// so a trades batch spanning two minutes emits two bars; a partial minute emits none (its bar
/// stays in `acc` until the next minute opens).
#[derive(Clone, Default)]
pub struct Bar1m {
	acc: Option<Bar>,
	buf: Vec<Bar>,
}
impl Cell for Bar1m {
	type Out<'t> = &'t [Bar];
}
impl Node for Bar1m {
	type Deps = (Trades,);

	fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// precision is the run's, so the two scales are hoisted once instead of read per trade.
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		for (i, exec) in trades.exec().iter().enumerate() {
			let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
			let ts_open = exec.floor(MINUTE).as_nanos();
			match &mut self.acc {
				Some(b) if b.ts_open == ts_open => {
					b.high = b.high.max(price);
					b.low = b.low.min(price);
					b.close = price;
					b.vol_quote += price * qty;
					b.flow_quote += signed(trades.side[i], price * qty);
				}
				acc => {
					if let Some(done) = acc.take() {
						self.buf.push(done);
					}
					*acc = Some(Bar {
						ts_open,
						open: price,
						high: price,
						low: price,
						close: price,
						vol_quote: price * qty,
						flow_quote: signed(trades.side[i], price * qty),
					});
				}
			}
		}
		&self.buf
	}
}
slice_nudge!(Bar1m, Bar);

/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
	buf: Vec<f64>,
}
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
impl Node for Cvd {
	type Deps = (Trades,);

	fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		for i in 0..trades.len() {
			self.sum += signed(trades.side[i], (trades.price[i] as f64 / ps) * (trades.qty[i] as f64 / qs));
			self.buf.push(self.sum);
		}
		&self.buf
	}
}
slice_nudge!(Cvd, f64);

// The 14-period constants are part of these nodes' identity, so Default is honest.
#[derive(Clone)]
pub struct Rsi14 {
	rsi: WilderRsi,
	buf: Vec<Option<f64>>,
}
impl Default for Rsi14 {
	fn default() -> Self {
		Self {
			rsi: WilderRsi::new(14),
			buf: Vec::new(),
		}
	}
}
impl Cell for Rsi14 {
	type Out<'t> = &'t [Option<f64>];
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

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			self.buf.push(self.rsi.update(b.close));
		}
		&self.buf
	}
}
slice_nudge!(Rsi14, Option<f64>);

#[derive(Clone)]
pub struct Atr14 {
	atr: WilderAtr,
	buf: Vec<Option<f64>>,
}
impl Default for Atr14 {
	fn default() -> Self {
		Self {
			atr: WilderAtr::new(14),
			buf: Vec::new(),
		}
	}
}
impl Cell for Atr14 {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Atr14 {
	type Deps = (Bar1m,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			self.buf.push(self.atr.update(b.high, b.low, b.close));
		}
		&self.buf
	}
}
slice_nudge!(Atr14, Option<f64>);

#[derive(Clone, Default)]
pub struct Momentum {
	prev_close: Option<f64> = None,
	returns: [f64; MOM_WINDOW] = [0.0; MOM_WINDOW],
	idx: usize = 0,
	filled: usize = 0,
	buf: Vec<Option<f64>> = Vec::new(),
}
impl Cell for Momentum {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Momentum {
	type Deps = (Bar1m,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			let m = self.step(b.close);
			self.buf.push(m);
		}
		&self.buf
	}
}
impl Momentum {
	fn step(&mut self, close: f64) -> Option<f64> {
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
slice_nudge!(Momentum, Option<f64>);

#[derive(Clone)]
pub struct VolUsd1h {
	ring: [f64; 60],
	idx: usize,
	buf: Vec<f64>,
}
impl Default for VolUsd1h {
	fn default() -> Self {
		Self {
			ring: [0.0; 60],
			idx: 0,
			buf: Vec::new(),
		}
	}
}
impl Cell for VolUsd1h {
	type Out<'t> = &'t [f64];
}
impl Node for VolUsd1h {
	type Deps = (Bar1m,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			self.ring[self.idx] = b.vol_quote;
			self.idx = (self.idx + 1) % self.ring.len();
			self.buf.push(self.ring.iter().sum());
		}
		&self.buf
	}
}
slice_nudge!(VolUsd1h, f64);

/// Kyle's λ: through-origin OLS of per-bar Δclose on signed flow, `λ = Σ(Δp·f) / Σ(f²)`.
#[derive(Clone, Default)]
pub struct Lambda1m {
	prev_close: Option<f64> = None,
	d_close: [f64; LAMBDA_WINDOW] = [0.0; LAMBDA_WINDOW],
	flow: [f64; LAMBDA_WINDOW] = [0.0; LAMBDA_WINDOW],
	idx: usize = 0,
	filled: usize = 0,
	buf: Vec<Option<f64>> = Vec::new(),
}
impl Cell for Lambda1m {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Lambda1m {
	type Deps = (Bar1m,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			let l = self.step(b.close, b.flow_quote);
			self.buf.push(l);
		}
		&self.buf
	}
}
impl Lambda1m {
	fn step(&mut self, close: f64, flow: f64) -> Option<f64> {
		let prev = self.prev_close.replace(close)?;
		self.d_close[self.idx] = close - prev;
		self.flow[self.idx] = flow;
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
slice_nudge!(Lambda1m, Option<f64>);

/// Rolling OI %-change vs the previous OI observation — proof of multi-lane replay. One element
/// per OI event.
#[derive(Clone, Default)]
pub struct OiChange {
	prev: Option<f64> = None,
	buf: Vec<Option<f64>> = Vec::new(),
}
impl Cell for OiChange {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for OiChange {
	type Deps = (OiRoot,);

	fn advance<'t>(&'t mut self, (ois,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for o in ois {
			self.buf.push(self.prev.replace(o.oi).map(|p| o.oi / p - 1.0));
		}
		&self.buf
	}
}
slice_nudge!(OiChange, Option<f64>);

/// Stateful hysteresis streak — deliberately non-vectorizable, like the SPL RsiScreener. Zips its
/// three same-rate bar deps by index (len assert is the tripwire), one bool per closed bar.
#[derive(Clone, Default)]
pub struct Screener {
	streak: u32,
	buf: Vec<bool>,
}
impl Cell for Screener {
	type Out<'t> = &'t [bool];
}
impl Node for Screener {
	type Deps = (Momentum, Rsi14, VolUsd1h);

	fn advance<'t>(&'t mut self, (mom, rsi, vol): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(mom.len(), rsi.len(), "Momentum/Rsi14 rate mismatch");
		assert_eq!(mom.len(), vol.len(), "Momentum/VolUsd1h rate mismatch");
		self.buf.clear();
		for i in 0..mom.len() {
			// not-warm = closed; streak preserved across not-warm bars.
			let Some((m, r)) = mom[i].zip(rsi[i]) else {
				self.buf.push(false);
				continue;
			};
			let hit = m.abs() > MOM_TH && !(RSI_LO..=RSI_HI).contains(&r) && vol[i] > VOL_TH;
			self.streak = if hit { self.streak + 1 } else { 0 };
			self.buf.push(self.streak >= STREAK_N);
		}
		&self.buf
	}
}
slice_nudge!(Screener, bool);

/// Per-bar 4-way distribution, gated (as a plain zip, not `When`) on the same-rate Screener: a
/// `Some` where the screener fired, `None` otherwise.
#[derive(Clone, Default)]
pub struct Classify {
	buf: Vec<Option<Dist>>,
}
impl Cell for Classify {
	type Out<'t> = &'t [Option<Dist>];
}
impl Node for Classify {
	type Deps = (Screener, Momentum);

	// Element order is the [`Dist`] wire order.
	const SKETCH: Sketch = Sketch {
		labels: &["None", "Liquidations", "MmClosing", "Manipulation"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (screener, mom): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(screener.len(), mom.len(), "Screener/Momentum rate mismatch");
		self.buf.clear();
		for i in 0..screener.len() {
			self.buf.push(screener[i].then(|| {
				let m = mom[i].expect("Screener only fires once Momentum is warm");
				let category = if m.abs() > MOM_HIGH_BAND {
					Category::Manipulation
				} else if m.abs() > MOM_MID_BAND {
					Category::Liquidations
				} else {
					Category::MmClosing
				};
				let rest = (1.0 - 0.6) / 3.0;
				Dist([Category::None, Category::Liquidations, Category::MmClosing, Category::Manipulation].map(|c| if c == category { 0.6 } else { rest }))
			}));
		}
		&self.buf
	}
}
slice_nudge!(Classify, Option<Dist>);

/// Price-denominated trailing stop `close - K·ATR`, rendered on the candle pane (overlay).
#[derive(Clone, Default)]
pub struct AtrStop {
	buf: Vec<Option<f64>>,
}
impl Cell for AtrStop {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for AtrStop {
	type Deps = (Bar1m, Atr14);

	const SKETCH: Sketch = Sketch { overlay: true, ..Sketch::DEFAULT };

	fn advance<'t>(&'t mut self, (bars, atr): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), atr.len(), "Bar1m/Atr14 rate mismatch");
		self.buf.clear();
		for i in 0..bars.len() {
			self.buf.push(atr[i].map(|a| bars[i].close - ATR_STOP_K * a));
		}
		&self.buf
	}
}
slice_nudge!(AtrStop, Option<f64>);

/// A pure blend of the current signal levels — the one genuinely differentiable node here (every
/// other kernel is stateful/batch). Its value *is* an [`Expr`] of the scalar (`.last()`) views of
/// Momentum/Lambda1m/Atr14, so it differentiates and documents itself exactly; the FD Jacobian and
/// the exact one agree tick-for-tick (the demo asserts it).
#[derive(Clone, Copy, Default)]
pub struct Signal;
impl Cell for Signal {
	type Out<'t> = f64;
}
impl Symbolic for Signal {
	type Deps = (Momentum, Lambda1m, Atr14);

	fn body(&self, v: Vars) -> impl Expr {
		let (mom, lambda, atr) = (v.get::<0>(), v.get::<1>(), v.get::<2>());
		constant(0.5) * mom + constant(0.3) * lambda - atr
	}
}

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], oi: OiRoot[Oi] };
	out TickOut;
	diff { signal: Signal }
	bar: Bar1m,
	cvd: Cvd,
	rsi: Rsi14,
	atr: Atr14,
	atr_stop: AtrStop,
	momentum: Momentum,
	vol_usd_1h: VolUsd1h,
	lambda: Lambda1m,
	oi_change: OiChange,
	signal: Signal,
	screener: Screener,
	classified: Classify,
}

/// The whole of the routing an app needs: every lane is present, and the graph names the ones it
/// takes. No discriminant to re-dispatch, no `Default` fill.
impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self { trades: l.trades, oi: l.oi }
	}
}
