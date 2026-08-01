//! SPL replica graph, batch-native: two roots (Trades, OiRoot) weave into
//! Bar1m → (Cvd, Rsi14, Atr14, Momentum, VolUsd1h, Lambda1m) → Screener → Classify, plus an
//! Oi-consuming OiChange node. Rate is slice length, firing is element `Option`-ness. The `graph!`
//! field order is topo order.

use core::fmt;

use trading_data::{
	Buffer, Buffering, Bump, Cell, Emit, EmitOuts, Exact, Expr, Flat, Glance, Guide, Horizon, Ink, Lanes, Oi, OiRoot, Plot, Stamped, Symbolic, TradeCols, Trades, Vars, WilderAtr,
	WilderAvgGainLoss, constant, rsi, slice_nudge,
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

impl Stamped for Bar {
	fn ts_ns(&self) -> i64 {
		self.ts_open
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
}
impl Cell for Bar1m {
	type Out<'t> = &'t [Bar];
}
impl Emit for Bar1m {
	type Deps = (Trades,);

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Bar>) {
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
						out.push(done);
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
	}
}
slice_nudge!(Bar1m, Bar);

/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
}
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
impl Emit for Cvd {
	type Deps = (Trades,);

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		for i in 0..trades.len() {
			self.sum += signed(trades.side[i], (trades.price[i] as f64 / ps) * (trades.qty[i] as f64 / qs));
			out.push(self.sum);
		}
	}
}
slice_nudge!(Cvd, f64);

// The 14-period constants are part of these nodes' identity, so Default is honest.
#[derive(Clone)]
pub struct Rsi14 {
	rsi: WilderAvgGainLoss,
}
impl Default for Rsi14 {
	fn default() -> Self {
		Self { rsi: WilderAvgGainLoss::new(14) }
	}
}
impl Cell for Rsi14 {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for Rsi14 {
	type Deps = (Bar1m,);

	const PLOTS: &'static [Plot] = &[Plot {
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
		..Plot::DEFAULT
	}];

	fn emit(&mut self, (bars,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in bars {
			out.push(self.rsi.update(b.close).map(|(g, l)| rsi(g, l)));
		}
	}
}
slice_nudge!(Rsi14, Option<f64>);

#[derive(Clone)]
pub struct Atr14 {
	atr: WilderAtr,
}
impl Default for Atr14 {
	fn default() -> Self {
		Self { atr: WilderAtr::new(14) }
	}
}
impl Cell for Atr14 {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for Atr14 {
	type Deps = (Bar1m,);

	fn emit(&mut self, (bars,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for b in bars {
			out.push(self.atr.update(b.high, b.low, b.close));
		}
	}
}
slice_nudge!(Atr14, Option<f64>);

/// Sharpe-like `mean/stdev * √n` of the `MOM_WINDOW` returns spanned by a `MOM_WINDOW + 1` close
/// window. A degenerate (zero-variance) window is flat, not corrupt.
#[derive(Clone, Default)]
pub struct Momentum;
impl Cell for Momentum {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for Momentum {
	type Deps = (Buffering<Bar1m, { Horizon::Elems(MOM_WINDOW + 1) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.trailing().map(|w| w.map(sharpe)));
	}
}
fn sharpe(closes: &[Bar]) -> f64 {
	let n = MOM_WINDOW as f64;
	let ret = |w: &[Bar]| w[1].close / w[0].close - 1.0;
	let mean = closes.windows(2).map(ret).sum::<f64>() / n;
	let var = closes.windows(2).map(|w| (ret(w) - mean).powi(2)).sum::<f64>() / n;
	if var == 0.0 { 0.0 } else { mean / var.sqrt() * n.sqrt() }
}
slice_nudge!(Momentum, Option<f64>);

/// Rolling 60-bar quote volume. `None` until the hour is whole — a partial sum compared against a
/// threshold is a lie, not a warmup.
#[derive(Clone, Default)]
pub struct VolUsd1h;
impl Cell for VolUsd1h {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for VolUsd1h {
	type Deps = (Buffering<Bar1m, { Horizon::Elems(60) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.trailing().map(|w| w.map(|w| w.iter().map(|b| b.vol_quote).sum())));
	}
}
slice_nudge!(VolUsd1h, Option<f64>);

/// Kyle's λ: through-origin OLS of per-bar Δclose on signed flow, `λ = Σ(Δp·f) / Σ(f²)`, over the
/// `LAMBDA_WINDOW` deltas spanned by a `LAMBDA_WINDOW + 1` bar window.
#[derive(Clone, Default)]
pub struct Lambda1m;
impl Cell for Lambda1m {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for Lambda1m {
	type Deps = (Buffering<Bar1m, { Horizon::Elems(LAMBDA_WINDOW + 1) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.trailing().map(|w| w.map(kyle_lambda)));
	}
}
fn kyle_lambda(bars: &[Bar]) -> f64 {
	let denom: f64 = bars[1..].iter().map(|b| b.flow_quote * b.flow_quote).sum();
	if denom == 0.0 {
		return 0.0;
	}
	bars.windows(2).map(|w| (w[1].close - w[0].close) * w[1].flow_quote).sum::<f64>() / denom
}
slice_nudge!(Lambda1m, Option<f64>);

/// Rolling OI %-change vs the previous OI observation — proof of multi-lane replay. One element
/// per OI event.
#[derive(Clone, Default)]
pub struct OiChange {
	prev: Option<f64> = None,
}
impl Cell for OiChange {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for OiChange {
	type Deps = (OiRoot,);

	fn emit(&mut self, (ois,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for o in ois {
			out.push(self.prev.replace(o.oi).map(|p| o.oi / p - 1.0));
		}
	}
}
slice_nudge!(OiChange, Option<f64>);

/// Stateful hysteresis streak — deliberately non-vectorizable, like the SPL RsiScreener. Zips its
/// three same-rate bar deps by index (len assert is the tripwire), one bool per closed bar.
#[derive(Clone, Default)]
pub struct Screener {
	streak: u32,
}
impl Cell for Screener {
	type Out<'t> = &'t [bool];
}
impl Emit for Screener {
	type Deps = (Momentum, Rsi14, VolUsd1h);

	fn emit(&mut self, (mom, rsi, vol): EmitOuts<'_, Self>, out: &mut Vec<bool>) {
		assert_eq!(mom.len(), rsi.len(), "Momentum/Rsi14 rate mismatch");
		assert_eq!(mom.len(), vol.len(), "Momentum/VolUsd1h rate mismatch");
		for i in 0..mom.len() {
			// not-warm = closed; streak preserved across not-warm bars.
			let Some(((m, r), v)) = mom[i].zip(rsi[i]).zip(vol[i]) else {
				out.push(false);
				continue;
			};
			let hit = m.abs() > MOM_TH && !(RSI_LO..=RSI_HI).contains(&r) && v > VOL_TH;
			self.streak = if hit { self.streak + 1 } else { 0 };
			out.push(self.streak >= STREAK_N);
		}
	}
}
slice_nudge!(Screener, bool);

/// Per-bar 4-way distribution, gated (as a plain zip, not `When`) on the same-rate Screener: a
/// `Some` where the screener fired, `None` otherwise.
#[derive(Clone, Default)]
pub struct Classify;
impl Cell for Classify {
	type Out<'t> = &'t [Option<Dist>];
}
impl Emit for Classify {
	type Deps = (Screener, Momentum);

	// Element order is the [`Dist`] wire order.
	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["None", "Liquidations", "MmClosing", "Manipulation"],
		..Plot::DEFAULT
	}];

	fn emit(&mut self, (screener, mom): EmitOuts<'_, Self>, out: &mut Vec<Option<Dist>>) {
		assert_eq!(screener.len(), mom.len(), "Screener/Momentum rate mismatch");
		for i in 0..screener.len() {
			out.push(screener[i].then(|| {
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
	}
}
slice_nudge!(Classify, Option<Dist>);

/// Price-denominated trailing stop `close - K·ATR`, rendered on the candle pane (overlay).
#[derive(Clone, Default)]
pub struct AtrStop;
impl Cell for AtrStop {
	type Out<'t> = &'t [Option<f64>];
}
impl Emit for AtrStop {
	type Deps = (Bar1m, Atr14);

	const PLOTS: &'static [Plot] = &[Plot { overlay: true, ..Plot::DEFAULT }];

	fn emit(&mut self, (bars, atr): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		assert_eq!(bars.len(), atr.len(), "Bar1m/Atr14 rate mismatch");
		for i in 0..bars.len() {
			out.push(atr[i].map(|a| bars[i].close - ATR_STOP_K * a));
		}
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
	outputs { classified }
	// `main.rs`'s day-end levels, the exact/FD witness and the viz overlay — nothing downstream reads
	// any of them.
	observe { cvd, oi_change, signal, atr_stop }
	diff { signal: Signal }
	emit bar: Bar1m,
	// 61 = the deepest request (Momentum/Lambda1m's `window + 1`); VolUsd1h's 60 rides along.
	bar_hist: Buffer<Bar1m, { Horizon::Elems(61) }>,
	emit cvd: Cvd,
	emit rsi: Rsi14,
	emit atr: Atr14,
	emit atr_stop: AtrStop,
	emit momentum: Momentum,
	emit vol_usd_1h: VolUsd1h,
	emit lambda: Lambda1m,
	emit oi_change: OiChange,
	signal: Signal,
	emit screener: Screener,
	emit classified: Classify,
}

/// The whole of the routing an app needs: every lane is present, and the graph names the ones it
/// takes. No discriminant to re-dispatch, no `Default` fill.
impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self { trades: l.trades, oi: l.oi }
	}
}
