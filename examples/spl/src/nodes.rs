//! The scam_pump_liqs strategy as a compile-time step graph: five roots weave into per-timeframe
//! bars, the indies derive off those, the configured screener fires off the ones it reads,
//! `Classify` turns a hit into a distribution, and `Deprecator` runs the per-book-tick degrader that
//! produces `target_q`.
//!
//! Everything up to `target_q` is pure and ported verbatim — windows, thresholds and the two
//! misnomers SPL keeps from its Pine source. Execution (`trailing_limit`, `OrderAction`,
//! `rebalance_threshold`, cache reads, the market flatten) is out of scope.
//!
//! SPL sorts its indies into shallow and deep tiers, and that split is not strategy: deep is the
//! per-instrument *switchable* subscription it opens on a situation and closes on exit, and
//! `shallow_topics()` is a hand-rolled runtime list of which indies the configured screener consumes
//! — needed only because a msgbus has no static knowledge of who reads what. Here `type Deps` *is*
//! that list, and the compiler checks it. So there are no tiers: every node names exactly the inputs
//! it reads, and warmth is per-input rather than the union of a tier's. What `config.nix` still
//! decides is *which screener runs*, not which indie is registered to be readable.

use core::fmt;
use std::collections::VecDeque;

use trading_data::{
	Book, BookAnchors, BookDeltas, BookShape, Bump, Cell, DeltaFrame, DepOuts, Exact, Flat, Glance, Lanes, Mc, McRoot, Node, Oi, OiRoot, Sketch, TradeCols, Trades, WilderAtr, WilderRsi,
	slice_nudge,
};
use trading_data_core::Side;

use crate::config::{Screen, strategy};

const MIN_NS: i64 = 60_000_000_000;

// ─── ported constants ───────────────────────────────────────────────────────────────────────────
//
// Everything SPL exposes in `config.nix` is read from [`crate::config::strategy`] instead; what
// stays here is what SPL also hardcodes.

/// Pine's `* 365`, kept verbatim regardless of bar timeframe — which is why the 4h and 5m Sharpe
/// scales differ and each gets its own threshold.
const PINE_PERIODS_PER_YEAR: f64 = 365.0;
/// Closed 1h bars behind `change_1d_pct`.
const BARS_1D: usize = 24;
/// Closed 1m bars behind `change_3m_pct` — offsets 0,1,2 span exactly three minutes.
const BARS_3M: usize = 3;
/// SPL's `execution::RISK_FRACTION`: fraction of equity committed per entry.
const RISK_FRACTION: f64 = 0.03;
/// SPL's `OrderBookActor::DEPTH`.
const DEPTH: usize = 20;

// ─── flattening ─────────────────────────────────────────────────────────────────────────────────

/// `Flat` + `Bump` for a record whose observed slots are plain `f64` fields, in the order given.
macro_rules! flat_fields {
	($T:ty [$($f:ident),+ $(,)?]) => {
		impl Flat for $T {
			const DIMS: &'static [usize] = &[[$(stringify!($f)),+].len()];

			fn flat(&self, out: &mut [f64]) -> bool {
				out.copy_from_slice(&[$(self.$f),+]);
				true
			}
		}
		impl Bump for $T {
			fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
				let mut i = 0;
				$(
					if i == slot {
						self.$f += h;
						return (self, h);
					}
					i += 1;
				)+
				unreachable!("slot {slot} of {i}")
			}
		}
	};
}

/// A record the finite-difference witness cannot perturb: its slots are nested, `Option`-valued or
/// enum-like, so no single scalar bump corresponds to perturbing an input the consumer reads. `0.0`
/// is the contract's "this slot has no derivative", which leaves the Jacobian column NaN rather than
/// a fabricated zero.
macro_rules! structural_bump {
	($T:ty) => {
		impl Bump for $T {
			fn bump(self, _: usize, _: f64) -> (Self, f64) {
				(self, 0.0)
			}
		}
	};
}

// ─── bars ───────────────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_open: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	/// Base-denominated. SPL's volume indie reads `volume * close` — the close standing in for vwap.
	pub vol_base: f64,
}
impl Bar {
	fn close_ns(&self, minutes: u64) -> i64 {
		self.ts_open + minutes as i64 * MIN_NS
	}
}

flat_fields!(Bar[open, high, low, close, vol_base]);

impl Glance for Bar {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "close {}", self.close)
	}
}

/// Trades → `M`-minute OHLCV bars. Rate-changing: one non-optional bar per boundary crossed, so a
/// batch spanning two periods emits two; a partial period emits none (its bar stays in `acc`).
#[derive(Clone, Default)]
pub struct Bars<const M: u64> {
	acc: Option<Bar>,
	buf: Vec<Bar>,
}
impl<const M: u64> Cell for Bars<M> {
	type Out<'t> = &'t [Bar];
}
impl<const M: u64> Node for Bars<M> {
	type Deps = (Trades,);

	fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// precision is the run's, so the two scales are hoisted once instead of read per trade.
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		let step = Exact::from_nanos(M as i64 * MIN_NS);
		for (i, exec) in trades.exec().iter().enumerate() {
			let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
			let ts_open = exec.floor(step).as_nanos();
			match &mut self.acc {
				Some(b) if b.ts_open == ts_open => {
					b.high = b.high.max(price);
					b.low = b.low.min(price);
					b.close = price;
					b.vol_base += qty;
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
						vol_base: qty,
					});
				}
			}
		}
		&self.buf
	}
}
slice_nudge!(Bars<1>, Bar);
slice_nudge!(Bars<5>, Bar);
slice_nudge!(Bars<15>, Bar);
slice_nudge!(Bars<60>, Bar);
slice_nudge!(Bars<240>, Bar);

#[derive(Clone, Copy, Debug)]
pub struct PriceSnap {
	pub current: f64,
	pub change_3m_pct: f64,
	pub change_1d_pct: f64,
}
/// SPL backtest mode: the 3m delta comes off the last three closed 1m bars (the live Trades window
/// is a live-only fidelity choice), the 1d delta off 24 closed 1h closes.
#[derive(Clone, Default)]
pub struct Price {
	bars_3m: VecDeque<(f64, f64)>,
	closes_1h: VecDeque<f64>,
	buf: Vec<Option<PriceSnap>>,
}
impl Price {
	fn snap(&self, current: f64) -> Option<PriceSnap> {
		if self.bars_3m.len() < BARS_3M || self.closes_1h.len() < BARS_1D {
			return None;
		}
		let (base_open, _) = *self.bars_3m.front().expect("checked full");
		let oldest_1h = *self.closes_1h.front().expect("checked full");
		if base_open <= 0.0 || oldest_1h == 0.0 {
			return None;
		}
		Some(PriceSnap {
			current,
			change_3m_pct: (current - base_open) / base_open * 100.0,
			change_1d_pct: (current - oldest_1h) / oldest_1h * 100.0,
		})
	}
}

#[derive(Clone, Copy, Debug)]
pub struct VolSnap {
	pub volume_1m_usd: f64,
	pub volume_1h_usd: f64,
	pub volume_4h_usd: f64,
}
/// Latest closed bar per timeframe, notional as `volume * close`.
#[derive(Clone, Default)]
pub struct Volume {
	last_1h: Option<f64>,
	last_4h: Option<f64>,
	buf: Vec<Option<VolSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct RsiValues {
	pub actual: f64,
	pub smooth: f64,
}
#[derive(Clone, Copy, Debug)]
pub struct RsiSnap {
	pub rsi_5m: RsiValues,
	pub rsi_15m: RsiValues,
	pub rsi_1h: RsiValues,
	pub rsi_4h: RsiValues,
}
/// Wilder RSI(14) per timeframe, each EMA(14)-smoothed. Emits per 5m bar — every 15m/1h/4h boundary
/// is also a 5m one, so nothing is missed by anchoring there.
#[derive(Clone)]
pub struct Rsi {
	m5: RsiSlot,
	m15: RsiSlot,
	h1: RsiSlot,
	h4: RsiSlot,
	buf: Vec<Option<RsiSnap>>,
}
/// Wilder ATR(14) on 1m bars. An indie in its own right rather than an execution-owned indicator:
/// that is what removed SPL's per-situation bar subscribe/unsubscribe flicker.
#[derive(Clone)]
pub struct Atr {
	atr: WilderAtr,
	buf: Vec<Option<f64>>,
}
#[derive(Clone, Copy, Debug)]
pub struct MomSnap {
	/// `None` when 4h is disabled: the slot is never created, so there is no window to compute from.
	pub sharpe_4h: Option<f64>,
	pub sharpe_5m: f64,
}
#[derive(Clone, Default)]
pub struct Momentum {
	closes_5m: VecDeque<f64>,
	closes_4h: VecDeque<f64>,
	buf: Vec<Option<MomSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct OiSnap {
	pub oi_delta_5m_pct: f64,
	pub oi_delta_15m_pct: f64,
}
/// Bybit 5min open interest read 1-back and 3-back — 5 and 15 minutes.
#[derive(Clone, Default)]
pub struct OiDelta {
	window: VecDeque<f64>,
	buf: Vec<Option<OiSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct McSnap {
	pub market_cap: f64,
	pub rank: Option<u32>,
}
#[derive(Clone, Default)]
pub struct MarketCap {
	buf: Vec<Option<McSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct BookTopSnap {
	pub ts_ns: i64,
	pub best_bid: f64,
	pub best_ask: f64,
	pub top20_bid_depth_usd: f64,
	pub top20_ask_depth_usd: f64,
	pub imbalance: f64,
	pub spread_pct: f64,
}
impl BookTopSnap {
	pub fn mid(&self) -> f64 {
		(self.best_bid + self.best_ask) / 2.0
	}
}

/// Best bid/ask, top-20 depth, imbalance and spread off the folded book — derived facts, peer to
/// [`Rsi`] or [`Atr`], and the delta lane's own cadence is the rate. A book still filling from its
/// first deltas has one side empty; that is warmup, not corruption, so the tick declines and the
/// deprecator simply doesn't enter yet.
#[derive(Clone, Default)]
pub struct BookTop {
	buf: Vec<Option<BookTopSnap>>,
}
/// A screener hit. Binary: certainty is signed and saturated on a hit, and a miss emits nothing at
/// all (SPL's `Screened` contract to the classifier). A graded certainty is a later refinement.
#[derive(Clone, Copy, Debug)]
pub struct Screened {
	pub ts_ns: i64,
	pub certainty: f64,
}
/// Top gainer: overbought on 4h while up on the day. [`Bars<1>`] is the screening clock, so a
/// verdict is reached once a minute exactly as in SPL — the rate comes from this node's own inputs.
#[derive(Clone, Default)]
pub struct RsiScreener {
	rsi: Option<RsiSnap>,
	buf: Vec<Option<Screened>>,
}
/// Pine's overvalued zone at both timeframes. The 4h leg is vacuously satisfied when 4h is disabled.
#[derive(Clone, Default)]
pub struct StdScreener {
	momentum: Option<MomSnap>,
	buf: Vec<Option<Screened>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
	None,
	Liquidations,
	MmClosing,
	Manipulation,
}
/// Size scales exactly exponentially.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quality {
	A,
	B,
	C,
	D,
	E,
}
/// SPL's `ClassificationActor::classify` is still a stub returning one outcome at 100%; ported as
/// it stands rather than invented over.
#[derive(Clone, Copy, Debug)]
pub struct Classified {
	pub ts_ns: i64,
	pub probability: f64,
	pub category: Category,
	pub quality: Quality,
}
/// SPL runs exactly one screener per config; both are wired here, so a hit on either classifies.
#[derive(Clone, Default)]
pub struct Classify {
	buf: Vec<Option<Classified>>,
}
/// Per-book-tick trailing term: ratchets the favourable extreme and degrades linearly with the retrace
/// from it — certainty 0.0 at the extreme, 1.0 at `distance`. Certainty itself ratchets, so
/// proximity recovering re-adds no size, and its impact caps at `severity`.
///
/// The extreme seeds from the first update price, not the entry price: at entry the only cached
/// price can lag the book by whole percents on thin instruments, and a phantom extreme above the
/// market fires the stop on its first tick.
#[derive(Clone)]
pub struct TrailingStop {
	side: Side,
	distance: f64,
	severity: f64,
	extreme: Option<f64>,
	certainty: f64,
}
impl TrailingStop {
	pub fn new(side: Side, distance: f64, severity: f64) -> Self {
		assert!(distance > 0.0, "non-positive trail distance would be full certainty from the first tick");
		Self {
			side,
			distance,
			severity,
			extreme: None,
			certainty: 0.0,
		}
	}

	/// Ratchet on `price` and read the tick off in one act: the `1 - severity * certainty` multiplier
	/// the degrader applies, the surviving fraction `1 - certainty`, and the price level where the
	/// trail fires. The stop is `None` once fully retraced — at full certainty the term has deprecated
	/// all the size it controls, so it stops drawing.
	pub fn step(&mut self, price: f64) -> (f64, f64, Option<f64>) {
		let extreme = self.extreme.get_or_insert(price);
		let retrace = match self.side {
			Side::Buy => {
				*extreme = extreme.max(price);
				*extreme - price
			}
			Side::Sell => {
				*extreme = extreme.min(price);
				price - *extreme
			}
		};
		self.certainty = self.certainty.max((retrace / self.distance).clamp(0.0, 1.0));
		let stop = match (self.certainty >= 1.0, self.side) {
			(true, _) => None,
			(false, Side::Buy) => Some(*extreme - self.distance),
			(false, Side::Sell) => Some(*extreme + self.distance),
		};
		(1.0 - self.severity * self.certainty, 1.0 - self.certainty, stop)
	}
}

/// One book tick of an open episode — the persisted intent stream, minus the execution fields.
#[derive(Clone, Copy, Debug)]
pub struct Intent {
	pub ts_ns: i64,
	/// Which Idle→Active episode this tick belongs to — SPL's per-`Degrader` `Uuid`, as a counter.
	/// Two episodes can be adjacent in the intent stream (a classification can land between two book
	/// ticks), so this is the only thing that separates them.
	pub episode: u64,
	pub side: Side,
	pub base_q: f64,
	pub target_q: f64,
	pub eval: f64,
	pub lambda_atr: f64,
	pub trail_fraction: f64,
	pub sl: f64,
	pub tp: f64,
	/// The level where the trail fires; `None` once fully retraced, when it stops drawing.
	pub trail_stop: Option<f64>,
	pub draining: bool,
}
/// SPL's `ExecutorState` + `Degrader`, one for one. Entry mid-prices off the book (not the last
/// trade — on thin instruments that lags by whole percents and centres the envelope on a phantom);
/// every book tick then reduces one weighted ATR-envelope lambda against the trailing term.
#[derive(Clone, Default)]
pub struct Deprecator {
	state: State,
	episodes: u64,
	last_atr: Option<f64>,
	last_top: Option<BookTopSnap>,
	buf: Vec<Option<Intent>>,
}
/// The deprecator's three price-denominated levels, on the candle pane. Purely an observation
/// shape, so an absent trail collapses to [`Flat::flat`]'s `None` ⇒ NaN + unfired here and nowhere else.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
	pub sl: f64,
	pub tp: f64,
	pub trail_stop: f64,
}
#[derive(Clone, Default)]
pub struct Envelope {
	buf: Vec<Option<Bounds>>,
}
/// Advances a slower bar cursor to every bar that has closed by `deadline`, feeding each to `sink`.
/// Bars of different timeframes reach SPL as independent bus messages; here they arrive in one
/// batch, so the merge is explicit rather than accidental.
fn drain_upto(bars: &[Bar], minutes: u64, cursor: &mut usize, deadline: i64, mut sink: impl FnMut(&Bar)) {
	while *cursor < bars.len() && bars[*cursor].close_ns(minutes) <= deadline {
		sink(&bars[*cursor]);
		*cursor += 1;
	}
}

/// Caches a slower dep's latest publish as a level, for a node clocked by a faster one. A dep that
/// declined this tick (`None`) is not a publish, so the cached level stands.
fn latest<T: Copy>(slot: &mut Option<T>, dep: &[Option<T>]) {
	if let Some(Some(v)) = dep.last() {
		*slot = Some(*v);
	}
}

// ─── indies ─────────────────────────────────────────────────────────────────────────────────────

flat_fields!(PriceSnap[current, change_3m_pct, change_1d_pct]);

impl Glance for PriceSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.4} 1d {:+.2}%", self.current, self.change_1d_pct)
	}
}

impl Cell for Price {
	type Out<'t> = &'t [Option<PriceSnap>];
}
impl Node for Price {
	type Deps = (Bars<1>, Bars<60>);

	fn advance<'t>(&'t mut self, (m1, h1): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let mut cursor = 0;
		for b in m1 {
			let closes_1h = &mut self.closes_1h;
			drain_upto(h1, 60, &mut cursor, b.close_ns(1), |h| {
				closes_1h.push_back(h.close);
				while closes_1h.len() > BARS_1D {
					closes_1h.pop_front();
				}
			});
			self.bars_3m.push_back((b.open, b.close));
			while self.bars_3m.len() > BARS_3M {
				self.bars_3m.pop_front();
			}
			self.buf.push(self.snap(b.close));
		}
		let closes_1h = &mut self.closes_1h;
		drain_upto(h1, 60, &mut cursor, i64::MAX, |h| {
			closes_1h.push_back(h.close);
			while closes_1h.len() > BARS_1D {
				closes_1h.pop_front();
			}
		});
		&self.buf
	}
}
slice_nudge!(Price, Option<PriceSnap>);

flat_fields!(VolSnap[volume_1m_usd, volume_1h_usd, volume_4h_usd]);

impl Glance for VolSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "1h ${:.3e}", self.volume_1h_usd)
	}
}

impl Cell for Volume {
	type Out<'t> = &'t [Option<VolSnap>];
}
impl Node for Volume {
	type Deps = (Bars<1>, Bars<60>, Bars<240>);

	fn advance<'t>(&'t mut self, (m1, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let (mut c1, mut c4) = (0, 0);
		for b in m1 {
			let (last_1h, last_4h) = (&mut self.last_1h, &mut self.last_4h);
			drain_upto(h1, 60, &mut c1, b.close_ns(1), |h| *last_1h = Some(h.vol_base * h.close));
			drain_upto(h4, 240, &mut c4, b.close_ns(1), |h| *last_4h = Some(h.vol_base * h.close));
			self.buf.push(last_1h.zip(*last_4h).map(|(volume_1h_usd, volume_4h_usd)| VolSnap {
				volume_1m_usd: b.vol_base * b.close,
				volume_1h_usd,
				volume_4h_usd,
			}));
		}
		&self.buf
	}
}
slice_nudge!(Volume, Option<VolSnap>);

impl Flat for RsiSnap {
	const DIMS: &'static [usize] = &[8];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[
			self.rsi_5m.actual, self.rsi_5m.smooth, self.rsi_15m.actual, self.rsi_15m.smooth, self.rsi_1h.actual, self.rsi_1h.smooth, self.rsi_4h.actual, self.rsi_4h.smooth,
		]);
		true
	}
}
structural_bump!(RsiSnap);

impl Glance for RsiSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "4h {:.1}", self.rsi_4h.actual)
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

#[derive(Clone)]
struct RsiSlot {
	base: WilderRsi,
	smooth: Ema,
	last: Option<RsiValues>,
}
impl RsiSlot {
	fn new() -> Self {
		Self {
			base: WilderRsi::new(strategy().indies.rsi.base_len),
			smooth: Ema::new(strategy().indies.rsi.smooth_len),
			last: None,
		}
	}

	/// Warmth is `base_len + smooth_len` closed bars, which is exactly when both stages are warm:
	/// the Wilder base needs `base_len` deltas, and only then does the EMA start seeing values.
	fn update(&mut self, close: f64) {
		if let Some(actual) = self.base.update(close)
			&& let Some(smooth) = self.smooth.update(actual)
		{
			self.last = Some(RsiValues { actual, smooth });
		}
	}
}

impl Default for Rsi {
	fn default() -> Self {
		Self {
			m5: RsiSlot::new(),
			m15: RsiSlot::new(),
			h1: RsiSlot::new(),
			h4: RsiSlot::new(),
			buf: Vec::new(),
		}
	}
}
impl Cell for Rsi {
	type Out<'t> = &'t [Option<RsiSnap>];
}
impl Node for Rsi {
	type Deps = (Bars<5>, Bars<15>, Bars<60>, Bars<240>);

	// No threshold guide: the trigger is a `config.nix` value and `Sketch` is a const, so drawing
	// one here would pin a number the config is free to move.
	const SKETCH: Sketch = Sketch {
		range: Some((0.0, 100.0)),
		labels: &["5m", "5m~", "15m", "15m~", "1h", "1h~", "4h", "4h~"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (m5, m15, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let (mut c15, mut c1h, mut c4h) = (0, 0, 0);
		for b in m5 {
			let deadline = b.close_ns(5);
			drain_upto(m15, 15, &mut c15, deadline, |x| self.m15.update(x.close));
			drain_upto(h1, 60, &mut c1h, deadline, |x| self.h1.update(x.close));
			drain_upto(h4, 240, &mut c4h, deadline, |x| self.h4.update(x.close));
			self.m5.update(b.close);
			self.buf.push(match (self.m5.last, self.m15.last, self.h1.last, self.h4.last) {
				(Some(rsi_5m), Some(rsi_15m), Some(rsi_1h), Some(rsi_4h)) => Some(RsiSnap { rsi_5m, rsi_15m, rsi_1h, rsi_4h }),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(Rsi, Option<RsiSnap>);

impl Default for Atr {
	fn default() -> Self {
		Self {
			atr: WilderAtr::new(strategy().indies.atr.period),
			buf: Vec::new(),
		}
	}
}
impl Cell for Atr {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Atr {
	type Deps = (Bars<1>,);

	fn advance<'t>(&'t mut self, (bars,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in bars {
			self.buf.push(self.atr.update(b.high, b.low, b.close));
		}
		&self.buf
	}
}
slice_nudge!(Atr, Option<f64>);

impl Flat for MomSnap {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.sharpe_4h.unwrap_or(f64::NAN), self.sharpe_5m]);
		true
	}
}
structural_bump!(MomSnap);

impl Glance for MomSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "4h {:?} 5m {:.2}", self.sharpe_4h.map(|s| (s * 100.0).round() / 100.0), self.sharpe_5m)
	}
}

/// Sharpe over a window of `lookback + 1` closes, per `bullmart_sri.pine`. `None` until the window
/// is full or when stdev is zero — all returns identical is degenerate, not corrupt.
fn sharpe(closes: &VecDeque<f64>) -> Option<f64> {
	let n = strategy().indies.momentum.lookback;
	if closes.len() < n + 1 {
		return None;
	}
	let mut prev = closes[0];
	assert!(prev > 0.0, "non-positive close at window head");
	let mut returns = Vec::with_capacity(n);
	for &c in closes.iter().skip(1) {
		assert!(prev > 0.0, "non-positive close inside window");
		returns.push((c - prev) / prev);
		prev = c;
	}
	let mean = returns.iter().sum::<f64>() / n as f64;
	let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n as f64;
	// Pine: `stdev * sqrt(lookback)` — non-standard, kept verbatim.
	let stdev_ann = var.sqrt() * (n as f64).sqrt();
	if stdev_ann == 0.0 {
		return None;
	}
	Some((mean * PINE_PERIODS_PER_YEAR - strategy().indies.momentum.risk_free_rate) / stdev_ann)
}

fn push_close(closes: &mut VecDeque<f64>, close: f64) {
	closes.push_back(close);
	while closes.len() > strategy().indies.momentum.lookback + 1 {
		closes.pop_front();
	}
}

impl Cell for Momentum {
	type Out<'t> = &'t [Option<MomSnap>];
}
impl Node for Momentum {
	type Deps = (Bars<5>, Bars<240>);

	const SKETCH: Sketch = Sketch {
		labels: &["4h", "5m"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (m5, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let mut cursor = 0;
		for b in m5 {
			let closes_4h = &mut self.closes_4h;
			drain_upto(h4, 240, &mut cursor, b.close_ns(5), |x| push_close(closes_4h, x.close));
			push_close(&mut self.closes_5m, b.close);
			// A degenerate (zero-stdev) window skips the publish rather than fabricating a Sharpe.
			self.buf.push(match (sharpe(&self.closes_5m), use_4h().then(|| sharpe(&self.closes_4h))) {
				(Some(sharpe_5m), Some(Some(sharpe_4h))) => Some(MomSnap {
					sharpe_4h: Some(sharpe_4h),
					sharpe_5m,
				}),
				(Some(sharpe_5m), None) => Some(MomSnap { sharpe_4h: None, sharpe_5m }),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(Momentum, Option<MomSnap>);

flat_fields!(OiSnap[oi_delta_5m_pct, oi_delta_15m_pct]);

impl Glance for OiSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "5m {:+.2}% 15m {:+.2}%", self.oi_delta_5m_pct, self.oi_delta_15m_pct)
	}
}

impl Cell for OiDelta {
	type Out<'t> = &'t [Option<OiSnap>];
}
impl Node for OiDelta {
	type Deps = (OiRoot,);

	fn advance<'t>(&'t mut self, (ois,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for o in ois {
			self.window.push_back(o.oi);
			while self.window.len() > 4 {
				self.window.pop_front();
			}
			self.buf.push((self.window.len() == 4).then(|| {
				let (current, ago_5m, ago_15m) = (self.window[3], self.window[2], self.window[0]);
				// SPL's own zero guard: an OI of exactly zero is a dead contract, reported as no change.
				let pct = |ago: f64| if ago != 0.0 { (current - ago) / ago * 100.0 } else { 0.0 };
				OiSnap {
					oi_delta_5m_pct: pct(ago_5m),
					oi_delta_15m_pct: pct(ago_15m),
				}
			}));
		}
		&self.buf
	}
}
slice_nudge!(OiDelta, Option<OiSnap>);

impl Flat for McSnap {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.market_cap, self.rank.map_or(f64::NAN, f64::from)]);
		true
	}
}
structural_bump!(McSnap);

impl Glance for McSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.3e} rank {:?}", self.market_cap, self.rank)
	}
}

impl Cell for MarketCap {
	type Out<'t> = &'t [Option<McSnap>];
}
impl Node for MarketCap {
	type Deps = (McRoot,);

	fn advance<'t>(&'t mut self, (mcs,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for m in mcs {
			self.buf.push(Some(McSnap {
				market_cap: m.market_cap,
				rank: m.rank,
			}));
		}
		&self.buf
	}
}
slice_nudge!(MarketCap, Option<McSnap>);

// ─── book top ───────────────────────────────────────────────────────────────────────────────────

flat_fields!(BookTopSnap[best_bid, best_ask, top20_bid_depth_usd, top20_ask_depth_usd, imbalance, spread_pct]);

impl Glance for BookTopSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.4}/{:.4} imb {:+.3}", self.best_bid, self.best_ask, self.imbalance)
	}
}

impl Cell for BookTop {
	type Out<'t> = &'t [Option<BookTopSnap>];
}
impl Node for BookTop {
	type Deps = (Book, BookDeltas);

	const SKETCH: Sketch = Sketch {
		labels: &["bid", "ask", "bid_depth$", "ask_depth$", "imbalance", "spread%"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (book, frame): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let Some(&ts) = frame.cols().exec().last() else { return &self.buf };
		self.buf.push(book.and_then(|b| {
			let (ps, qs) = (b.prec().price.scale(), b.prec().qty.scale());
			let (bid, ask) = (b.best_bid()?, b.best_ask()?);
			let usd = |(p, q): (&i32, &u32)| (*p as f64 / ps) * (*q as f64 / qs);
			let top20_bid_depth_usd: f64 = b.bids().iter().rev().take(DEPTH).map(usd).sum();
			let top20_ask_depth_usd: f64 = b.asks().iter().take(DEPTH).map(usd).sum();
			let total = top20_bid_depth_usd + top20_ask_depth_usd;
			let (best_bid, best_ask) = (bid.0.as_f64(), ask.0.as_f64());
			Some(BookTopSnap {
				ts_ns: ts.as_nanos(),
				best_bid,
				best_ask,
				top20_bid_depth_usd,
				top20_ask_depth_usd,
				imbalance: if total > 0.0 { (top20_bid_depth_usd - top20_ask_depth_usd) / total } else { 0.0 },
				spread_pct: (best_ask - best_bid) / best_bid * 100.0,
			})
		}));
		&self.buf
	}
}
slice_nudge!(BookTop, Option<BookTopSnap>);

// ─── screeners ──────────────────────────────────────────────────────────────────────────────────

flat_fields!(Screened[certainty]);

impl Glance for Screened {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:+.1}", self.certainty)
	}
}

/// The sign is the direction the screener votes for, its magnitude the confidence; both screeners
/// currently saturate, so a hit is the full vote.
fn screened(ts_ns: i64, hit: bool) -> Option<Screened> {
	hit.then_some(Screened { ts_ns, certainty: -1.0 })
}

impl Cell for RsiScreener {
	type Out<'t> = &'t [Option<Screened>];
}
impl Node for RsiScreener {
	type Deps = (Bars<1>, Price, Rsi);

	fn advance<'t>(&'t mut self, (bars, price, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), price.len(), "Bars<1>/Price rate mismatch");
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` zips the two screeners by
		// index, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Rsi(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.rsi, rsi);
		for (b, p) in bars.iter().zip(price) {
			self.buf.push(match (p, self.rsi) {
				(Some(p), Some(rsi)) => screened(b.close_ns(1), rsi.rsi_4h.actual > c.rsi_threshold && p.change_1d_pct > *c.price_percent),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(RsiScreener, Option<Screened>);

impl Cell for StdScreener {
	type Out<'t> = &'t [Option<Screened>];
}
impl Node for StdScreener {
	type Deps = (Bars<1>, Momentum);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` zips the two screeners by
		// index, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Std(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.momentum, momentum);
		for b in bars {
			self.buf.push(self.momentum.and_then(|m| {
				// The vacuous 4h leg must come from `use_4h` and nothing else: `Momentum` declines to
				// publish at all when 4h is enabled but degenerate, so an absent Sharpe here would
				// otherwise let a wiring bug read as an unconditional hit.
				assert_eq!(m.sharpe_4h.is_some(), use_4h(), "sharpe_4h presence disagrees with indies.momentum.use_4h");
				let trigger_4h = m.sharpe_4h.is_none_or(|x| x > c.overvalued_threshold_4h);
				screened(b.close_ns(1), trigger_4h && m.sharpe_5m > c.overvalued_threshold_5m)
			}));
		}
		&self.buf
	}
}
slice_nudge!(StdScreener, Option<Screened>);

// ─── classification ─────────────────────────────────────────────────────────────────────────────

flat_fields!(Classified[probability]);

impl Glance for Classified {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?}/{:?} {:.0}%", self.category, self.quality, self.probability * 100.0)
	}
}

impl Cell for Classify {
	type Out<'t> = &'t [Option<Classified>];
}
impl Node for Classify {
	type Deps = (RsiScreener, StdScreener);

	fn advance<'t>(&'t mut self, (rsi, std): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(rsi.len(), std.len(), "RsiScreener/StdScreener rate mismatch");
		self.buf.clear();
		for i in 0..rsi.len() {
			self.buf.push(rsi[i].or(std[i]).map(|s| Classified {
				ts_ns: s.ts_ns,
				probability: 1.0,
				category: Category::None,
				quality: Quality::A,
			}));
		}
		&self.buf
	}
}
slice_nudge!(Classify, Option<Classified>);

// ─── deprecator ─────────────────────────────────────────────────────────────────────────────────

impl Flat for Intent {
	const DIMS: &'static [usize] = &[5];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.target_q, self.base_q, self.eval, self.lambda_atr, self.trail_fraction]);
		true
	}
}
structural_bump!(Intent);

impl Glance for Intent {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?} q {:.4}/{:.4}{}", self.side, self.target_q, self.base_q, if self.draining { " draining" } else { "" })
	}
}

#[derive(Clone)]
struct Active {
	episode: u64,
	side: Side,
	base_q: f64,
	entry_price: f64,
	trail: TrailingStop,
	/// Latched: degrader recovery does not cancel the exit.
	drain_deadline_ns: Option<i64>,
}

#[derive(Clone, Default)]
enum State {
	#[default]
	Idle,
	Active(Active),
}

impl Cell for Deprecator {
	type Out<'t> = &'t [Option<Intent>];
}
impl Node for Deprecator {
	type Deps = (Classify, Atr, BookTop);

	const SKETCH: Sketch = Sketch {
		labels: &["target_q", "base_q", "eval", "lambda_atr", "trail_fraction"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (classify, atr, top): DepOuts<'t, Self>) -> Self::Out<'t> {
		let liq = &strategy().classification.liquidations;
		self.buf.clear();
		latest(&mut self.last_atr, atr);
		// Classification is honored only while Idle; once Active we drive solely off book ticks. No
		// book yet ⇒ don't enter — the screener keeps firing, so the next classification retries.
		if matches!(self.state, State::Idle)
			&& classify.iter().any(Option::is_some)
			&& let Some(book) = self.last_top
		{
			let entry_price = book.mid();
			//TODO: real selection over the full distribution; derive the side from the classification
			// context (e.g. cascade direction) rather than pinning it here.
			let side = Side::Buy;
			self.episodes += 1;
			self.state = State::Active(Active {
				episode: self.episodes,
				side,
				//TODO: scale RISK_FRACTION by certainty × quality via a historic-returns lookup.
				base_q: RISK_FRACTION * equity_usdt() / entry_price,
				entry_price,
				trail: TrailingStop::new(side, entry_price * *liq.trail_pct, *liq.trail_severity),
				drain_deadline_ns: None,
			});
		}

		// Rate-preserving over `top`, so the two zip by index — that is how a consumer recovers the
		// tick's mid price for an intent without it being copied into one.
		for d in top {
			let Some(d) = d else {
				self.buf.push(None);
				continue;
			};
			self.last_top = Some(*d);
			// Management needs `target_q` off the ATR envelope; skip until the first ATR lands.
			let (State::Active(a), Some(atr)) = (&mut self.state, self.last_atr) else {
				self.buf.push(None);
				continue;
			};
			let mid = d.mid();
			let (trail_mult, trail_fraction, trail_stop) = a.trail.step(mid);
			let (sl, tp) = match a.side {
				Side::Buy => (a.entry_price - atr * liq.atr_sl_x, a.entry_price + atr * liq.atr_tp_x),
				Side::Sell => (a.entry_price + atr * liq.atr_sl_x, a.entry_price - atr * liq.atr_tp_x),
			};
			let inside = match a.side {
				Side::Buy => mid > sl && mid < tp,
				Side::Sell => mid < sl && mid > tp,
			};
			// One weight-1.0 component, so the normalised weighted sum is the lambda itself.
			let lambda_atr = if inside { 1.0 } else { 0.0 };
			let eval = lambda_atr * trail_mult;

			// 100% deprecation starts the drain clock; SPL keeps the reduce-side limit working the
			// book until it expires, which is the part that lives past `target_q`.
			if eval == 0.0 && a.drain_deadline_ns.is_none() {
				a.drain_deadline_ns = Some(d.ts_ns + drain_grace_ns());
			}
			let draining = a.drain_deadline_ns.is_some();
			self.buf.push(Some(Intent {
				ts_ns: d.ts_ns,
				episode: a.episode,
				side: a.side,
				base_q: a.base_q,
				target_q: if draining { 0.0 } else { a.base_q * eval },
				eval,
				lambda_atr,
				trail_fraction,
				sl,
				tp,
				trail_stop,
				draining,
			}));
			if a.drain_deadline_ns.is_some_and(|dl| d.ts_ns >= dl) {
				self.state = State::Idle;
			}
		}
		&self.buf
	}
}
slice_nudge!(Deprecator, Option<Intent>);

flat_fields!(Bounds[sl, tp, trail_stop]);

impl Glance for Bounds {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "sl {:.4} tp {:.4}", self.sl, self.tp)
	}
}

impl Cell for Envelope {
	type Out<'t> = &'t [Option<Bounds>];
}
impl Node for Envelope {
	type Deps = (Deprecator,);

	const SKETCH: Sketch = Sketch {
		overlay: true,
		labels: &["sl", "tp", "trail_stop"],
		..Sketch::DEFAULT
	};

	fn advance<'t>(&'t mut self, (intents,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for i in intents {
			self.buf.push(i.map(|i| Bounds {
				sl: i.sl,
				tp: i.tp,
				trail_stop: i.trail_stop.unwrap_or(f64::NAN),
			}));
		}
		&self.buf
	}
}
slice_nudge!(Envelope, Option<Bounds>);

// ─── graph ──────────────────────────────────────────────────────────────────────────────────────

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out TickOut;
	bar_1m: Bars<1>,
	bar_5m: Bars<5>,
	bar_15m: Bars<15>,
	bar_1h: Bars<60>,
	bar_4h: Bars<240>,
	price: Price,
	volume: Volume,
	rsi: Rsi,
	atr: Atr,
	momentum: Momentum,
	oi_delta: OiDelta,
	market_cap: MarketCap,
	book: Book,
	book_top: BookTop,
	rsi_screener: RsiScreener,
	std_screener: StdScreener,
	classify: Classify,
	deprecator: Deprecator,
	envelope: Envelope,
}

/// The whole of the routing an app needs: every lane is present, and the graph names the ones it
/// takes. No discriminant to re-dispatch, no `Default` fill.
impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self {
			trades: l.trades,
			deltas: l.deltas,
			anchors: l.anchor,
			oi: l.oi,
			mc: l.mc,
		}
	}
}

fn use_4h() -> bool {
	strategy().indies.momentum.use_4h
}

/// SPL sizes off live portfolio equity; the simulated venue's seed is the honest stand-in.
fn equity_usdt() -> f64 {
	crate::config::config().backtest.starting_balance
}

fn drain_grace_ns() -> i64 {
	strategy().classification.drain_grace.duration().as_nanos() as i64
}
