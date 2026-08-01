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

use trading_data::{
	Book, BookAnchors, BookDeltas, BookShape, Buffer, Buffering, Bump, Cell, DeltaFrame, DepOuts, Exact, Flat, Gate, Glance, Horizon, Lanes, Mc, McRoot, Node, Oi, OiRoot, Plot, Stamped,
	TradeCols, Trades, WilderAtr, WilderAvgGainLoss, rsi, slice_nudge, value_nudge,
};
use trading_data_core::Side;
use v_utils::{Timeframe, TimeframeDesignator};

use crate::config::{Screen, strategy};

// ─── ported constants ───────────────────────────────────────────────────────────────────────────
//
// Everything SPL exposes in `config.nix` is read from [`crate::config::strategy`] instead; what
// stays here is what SPL also hardcodes.

/// Pine's `* 365`, kept verbatim regardless of bar timeframe — which is why the 4h and 5m Sharpe
/// scales differ and each gets its own threshold.
const PINE_PERIODS_PER_YEAR: f64 = 365.0;
/// Reach behind `change_1d_pct` — a day of wall clock, not "24 bars": an hour nothing traded emits
/// no bar, and SPL's own name for the window is the day.
const SPAN_1D: Timeframe = Timeframe::from_naive(1, TimeframeDesignator::Days);
/// What the 1h series must retain to answer it: the day, plus one period of cross-rate slack — the
/// 1m bar whose close asks the question stands up to a whole 1h period past the newest 1h bar.
const REACH_1D: Horizon = Horizon::Span(Timeframe(SPAN_1D.0 + Bar1h::TF.0));
/// Reach behind `change_3m_pct` — three minutes, spanned by the opens of the 1m bars inside it.
const SPAN_3M: Timeframe = Timeframe::from_naive(3, TimeframeDesignator::Minutes);
/// Bybit's open-interest publish cadence: the deltas read the publish standing a whole number of
/// these back, so the retained reach is one past the longer leg.
const OI_STEP: Timeframe = Timeframe::from_naive(5, TimeframeDesignator::Minutes);
const OI_REACH: Horizon = Horizon::Span(Timeframe(4 * OI_STEP.0));
/// SPL's `execution::RISK_FRACTION`: fraction of equity committed per entry.
const RISK_FRACTION: f64 = 0.03;
/// SPL's `OrderBookActor::DEPTH`.
const DEPTH: usize = 20;
/// Periods retained behind `indies.momentum.lookback`, which is a runtime knob where a buffer's
/// reach is a const — so this is a *capacity*, checked against the configured lookback in
/// [`crate::config::Config::load`]. Raising it costs `2 * (MOM_PERIODS - lookback)` retained bars.
pub const MOM_PERIODS: u64 = 256;
/// That capacity as the reach it is: `MOM_PERIODS` of the series' own periods.
pub const fn mom_cap(tf: Timeframe) -> Horizon {
	Horizon::Span(Timeframe(MOM_PERIODS * tf.0))
}

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
	fn close_ns(&self, tf: Timeframe) -> i64 {
		self.ts_open + tf.duration().as_nanos() as i64
	}
}

flat_fields!(Bar[open, high, low, close, vol_base]);

impl Glance for Bar {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "close {}", self.close)
	}
}

/// The open: a bar is retained and windowed by the period it *covers*, and its close is that plus
/// the series' own timeframe, which a bare `Bar` does not know.
impl Stamped for Bar {
	fn ts_ns(&self) -> i64 {
		self.ts_open
	}
}

/// [`Timeframe`]'s parse rules in const position. Parsing is the cheap direction — const-formatting
/// a number back out is not — so the literal that *names* a series is the same one that *defines*
/// it, and `Bar:1m` cannot drift into being a one-*second* series.
const fn tf(s: &str) -> Timeframe {
	let b = s.as_bytes();
	let (mut n, mut i) = (0u64, 0);
	while i < b.len() && b[i].is_ascii_digit() {
		n = n * 10 + (b[i] - b'0') as u64;
		i += 1;
	}
	assert!(n > 0, "a timeframe leads with its count");
	let (_, designator) = b.split_at(i);
	Timeframe::from_naive(
		n,
		match designator {
			b"s" => TimeframeDesignator::Seconds,
			b"m" => TimeframeDesignator::Minutes,
			b"h" => TimeframeDesignator::Hours,
			b"d" => TimeframeDesignator::Days,
			_ => panic!("timeframe designator is one of s/m/h/d"),
		},
	)
}

/// Trades → OHLCV bars at one period. Rate-changing: one non-optional bar per boundary crossed, so
/// a batch spanning two periods emits two; a partial period emits none (its bar stays in `acc`).
/// Shared by every series: only the period and the name are per-type.
#[derive(Clone, Default)]
pub struct BarAcc {
	acc: Option<Bar>,
	buf: Vec<Bar>,
}
impl BarAcc {
	fn advance<'t>(&'t mut self, trades: <Trades as Cell>::Out<'_>, tf: Timeframe) -> &'t [Bar] {
		self.buf.clear();
		// precision is the run's, so the two scales are hoisted once instead of read per trade.
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		let step = Exact::from_nanos(tf.duration().as_nanos() as i64);
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

/// One named series per timeframe. A distinct type per period is what the graph already demands
/// (node identity *is* its type); naming it after the timeframe is what makes the period legible
/// everywhere the name surfaces — DAG cards, dep edges, `step_until`.
macro_rules! bars {
	($($ty:ident = $tf:literal),+ $(,)?) => { $(
		#[derive(Clone, Default)]
		pub struct $ty(BarAcc);
		impl $ty {
			pub const TF: Timeframe = tf($tf);
		}
		impl Cell for $ty {
			type Out<'t> = &'t [Bar];

			const NAME: &'static str = concat!("Bar:", $tf);
		}
		impl Node for $ty {
			type Deps = (Trades,);

			/// Only the partial bar is held, so the state reaches back exactly one period.
			const HORIZON: Horizon = Horizon::Span(Self::TF);

			fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
				self.0.advance(trades, Self::TF)
			}
		}
		slice_nudge!($ty, Bar);
	)+ };
}
bars!(Bar1m = "1m", Bar5m = "5m", Bar15m = "15m", Bar1h = "1h", Bar4h = "4h");

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
	buf: Vec<Option<PriceSnap>>,
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
	buf: Vec<Option<VolSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct AvgGainLoss {
	pub gain: f64,
	pub loss: f64,
}
/// The two Wilder averages RSI is a ratio of, clocked by `indies.rsi.timeframe`. Every wired bar
/// series is a candidate input, which is why they are all deps; the config picks which one is read.
#[derive(Clone)]
pub struct RsiAverages {
	avgs: WilderAvgGainLoss,
	buf: Vec<Option<AvgGainLoss>>,
}
#[derive(Clone, Copy, Debug)]
pub struct RsiValues {
	pub actual: f64,
	pub smooth: f64,
}
/// Wilder RSI, EMA-smoothed. Warmth is `base_len + smooth_len` closed bars, which is exactly when
/// both stages are warm: the averages need `base_len` deltas, and only then does the EMA start
/// seeing values.
#[derive(Clone)]
pub struct Rsi {
	smooth: Ema,
	buf: Vec<Option<RsiValues>>,
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
	pub fast: f64,
	/// `None` when `indies.momentum.slow` names no second leg: the slot is never created, so there is
	/// no window to compute from.
	pub slow: Option<f64>,
}
/// Sharpe on the timeframe `indies.momentum.fast` names, plus an optional slower leg. Both wired
/// series are candidate inputs, which is why both are deps; the config picks which is which.
#[derive(Clone, Default)]
pub struct Momentum {
	buf: Vec<Option<MomSnap>>,
}
#[derive(Clone, Copy, Debug)]
pub struct OiSnap {
	pub oi_delta_5m_pct: f64,
	pub oi_delta_15m_pct: f64,
}
/// Bybit open interest against the publish standing 5 and 15 minutes back.
#[derive(Clone, Default)]
pub struct OiDelta {
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
/// A screener hit. A miss emits nothing at all — SPL's `Screened` contract to the classifier.
#[derive(Clone, Copy, Debug)]
pub struct Screened {
	pub ts_ns: i64,
}
/// Top gainer: overbought on 4h while up on the day. [`Bar1m`] is the screening clock, so a
/// verdict is reached once a minute exactly as in SPL — the rate comes from this node's own inputs.
#[derive(Clone, Default)]
pub struct RsiScreener {
	rsi: Option<RsiValues>,
	buf: Vec<Option<Screened>>,
}
/// Pine's overvalued zone at both of momentum's legs. The slow leg is vacuously satisfied when the
/// config names no slow timeframe.
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
/// Open on a tick either screener fired. SPL runs exactly one per config and this reads both, so
/// `Classify` is gated identically either way — which arm runs stays a config fact, not a wiring one.
#[derive(Clone, Copy, Default)]
pub struct ScreenHit;
/// SPL runs exactly one screener per config; both are wired here, so a hit on either classifies.
/// Classification is a per-hit act rather than a series — everything it reads is in the hit — so it
/// is a gated current node, latent on the ticks nothing fired.
#[derive(Clone, Copy, Default)]
pub struct Classify;
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
	/// The episode's last intent: the drain deadline passed on this book tick, so this one is
	/// published and `Active` closes. A reader has no other way to tell a spent episode from a book
	/// tick that merely declined to publish.
	pub terminal: bool,
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
/// The prefix of a slower series that has *closed* by `deadline` — the cross-rate read a node
/// clocked by a faster series makes against a [`Buffering`] dep.
fn closed_by(bars: &[Bar], tf: Timeframe, deadline: i64) -> &[Bar] {
	&bars[..bars.partition_point(|b| b.close_ns(tf) <= deadline)]
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
	type Deps = (Buffering<Bar1m, { Horizon::Span(SPAN_3M) }>, Buffering<Bar1h, REACH_1D>);

	fn advance<'t>(&'t mut self, (m1, h1): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for (b, w3) in m1.fresh().iter().zip(m1.trailing()) {
			let deadline = b.close_ns(Bar1m::TF);
			let closed_1h = closed_by(h1.all(), Bar1h::TF, deadline);
			let day_ago = deadline - SPAN_1D.duration().as_nanos() as i64;
			// The close standing a day back is the first one after `day_ago`; index 0 means the retained
			// run does not reach behind it, so there is nothing a day old to compare against yet.
			let oldest_1h = closed_1h.iter().position(|h| h.close_ns(Bar1h::TF) > day_ago).filter(|&i| i > 0).map(|i| closed_1h[i].close);
			self.buf.push(match (w3, oldest_1h) {
				(Some(w3), Some(oldest_1h)) => {
					let base_open = w3[0].open;
					(base_open > 0.0 && oldest_1h != 0.0).then(|| PriceSnap {
						current: b.close,
						change_3m_pct: (b.close - base_open) / base_open * 100.0,
						change_1d_pct: (b.close - oldest_1h) / oldest_1h * 100.0,
					})
				}
				_ => None,
			});
		}
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
	// One element: the level standing at each 1m bar's close, retained across the ticks where the
	// slower series emits nothing.
	type Deps = (Bar1m, Buffering<Bar1h, { Horizon::Elems(1) }>, Buffering<Bar4h, { Horizon::Elems(1) }>);

	fn advance<'t>(&'t mut self, (m1, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			let usd = |bars: &[Bar], tf: Timeframe| closed_by(bars, tf, b.close_ns(Bar1m::TF)).last().map(|h| h.vol_base * h.close);
			self.buf
				.push(usd(h1.all(), Bar1h::TF).zip(usd(h4.all(), Bar4h::TF)).map(|(volume_1h_usd, volume_4h_usd)| VolSnap {
					volume_1m_usd: b.vol_base * b.close,
					volume_1h_usd,
					volume_4h_usd,
				}));
		}
		&self.buf
	}
}
slice_nudge!(Volume, Option<VolSnap>);

flat_fields!(AvgGainLoss[gain, loss]);

impl Glance for AvgGainLoss {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "+{:.4} -{:.4}", self.gain, self.loss)
	}
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

impl Default for RsiAverages {
	fn default() -> Self {
		Self {
			avgs: WilderAvgGainLoss::new(strategy().indies.rsi.base_len),
			buf: Vec::new(),
		}
	}
}
impl Cell for RsiAverages {
	type Out<'t> = &'t [Option<AvgGainLoss>];
}
impl Node for RsiAverages {
	type Deps = (Bar5m, Bar15m, Bar1h, Bar4h);

	// Price units, so no range.
	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["avg gain", "avg loss"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (m5, m15, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let cfg = strategy().indies.rsi;
		let bars = match cfg.timeframe {
			Bar5m::TF => m5,
			Bar15m::TF => m15,
			Bar1h::TF => h1,
			Bar4h::TF => h4,
			_ => panic!("`{cfg}`: the graph wires {}/{}/{}/{} bars and no others", Bar5m::TF, Bar15m::TF, Bar1h::TF, Bar4h::TF),
		};
		for b in bars {
			self.buf.push(self.avgs.update(b.close).map(|(gain, loss)| AvgGainLoss { gain, loss }));
		}
		&self.buf
	}
}
slice_nudge!(RsiAverages, Option<AvgGainLoss>);

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
	type Deps = (RsiAverages,);

	// No threshold guide: the trigger is a `config.nix` value and `Plot` is a const, so drawing
	// one here would pin a number the config is free to move.
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 100.0)),
		labels: &["actual", "smooth"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (avgs,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for a in avgs {
			self.buf.push(a.and_then(|a| {
				let actual = rsi(a.gain, a.loss);
				self.smooth.update(actual).map(|smooth| RsiValues { actual, smooth })
			}));
		}
		&self.buf
	}
}
slice_nudge!(Rsi, Option<RsiValues>);

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
	type Deps = (Bar1m,);

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
		// An unconfigured slow leg has no value, which is the empty slot the flattening already spells.
		out.copy_from_slice(&[self.fast, self.slow.unwrap_or(f64::NAN)]);
		true
	}
}
structural_bump!(MomSnap);

impl Glance for MomSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let cfg = strategy().indies.momentum;
		write!(f, "{} {:.2}", cfg.fast, self.fast)?;
		match (cfg.slow, self.slow) {
			(Some(tf), Some(x)) => write!(f, " {tf} {x:.2}"),
			_ => Ok(()),
		}
	}
}

/// Sharpe over a window of `lookback + 1` closes, per `bullmart_sri.pine`. `None` when stdev is
/// zero — all returns identical is degenerate, not corrupt.
fn sharpe(window: &[Bar]) -> Option<f64> {
	let n = window.len() - 1;
	let ret = |w: &[Bar]| {
		assert!(w[0].close > 0.0, "non-positive close inside window");
		(w[1].close - w[0].close) / w[0].close
	};
	let mean = window.windows(2).map(ret).sum::<f64>() / n as f64;
	let var = window.windows(2).map(|w| (ret(w) - mean).powi(2)).sum::<f64>() / n as f64;
	// Pine: `stdev * sqrt(lookback)` — non-standard, kept verbatim.
	let stdev_ann = var.sqrt() * (n as f64).sqrt();
	if stdev_ann == 0.0 {
		return None;
	}
	Some((mean * PINE_PERIODS_PER_YEAR - strategy().indies.momentum.risk_free_rate) / stdev_ann)
}

impl Cell for Momentum {
	type Out<'t> = &'t [Option<MomSnap>];
}
impl Node for Momentum {
	type Deps = (Buffering<Bar5m, { mom_cap(Bar5m::TF) }>, Buffering<Bar4h, { mom_cap(Bar4h::TF) }>);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["fast", "slow"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (m5, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let cfg = strategy().indies.momentum;
		let n = cfg.lookback + 1;
		// Only these two series are retained a momentum window deep, so naming any other timeframe is a
		// config bug rather than a leg that silently never fills.
		let series = |tf| match tf {
			Bar5m::TF => m5,
			Bar4h::TF => h4,
			_ => panic!(
				"indies.momentum names {tf}, over which no {n}-bar window is retained — the graph buffers {} and {}",
				Bar5m::TF,
				Bar4h::TF
			),
		};
		// the lookback is a runtime knob, so the window is narrowed off the retained reach here.
		let fast = series(cfg.fast).narrowed(Horizon::Elems(n));
		let slow = cfg.slow.map(|tf| (tf, series(tf)));
		for (b, w) in fast.fresh().iter().zip(fast.trailing()) {
			let slow = slow.map(|(tf, s)| {
				let closed = closed_by(s.all(), tf, b.close_ns(cfg.fast));
				(closed.len() >= n).then(|| &closed[closed.len() - n..]).and_then(sharpe)
			});
			// A degenerate (zero-stdev) window skips the publish rather than fabricating a Sharpe.
			self.buf.push(match (w.and_then(sharpe), slow) {
				(Some(fast), None) => Some(MomSnap { fast, slow: None }),
				(Some(fast), Some(Some(slow))) => Some(MomSnap { fast, slow: Some(slow) }),
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
	type Deps = (Buffering<OiRoot, OI_REACH>,);

	/// Every input is read at a declared reach and nothing is accumulated, so this can be gated.
	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let step_ns = OI_STEP.duration().as_nanos() as i64;
		for (i, cur) in hist.fresh().iter().enumerate() {
			self.buf.push(hist.trailing_at(i).and_then(|w| {
				// The publish standing `back` before this one. A gap that leaves none within a publish
				// interval of that instant declines, rather than passing a shorter delta off as this one.
				let ago = |back: i64| {
					let target = cur.ts_ns() - back;
					let o = w.iter().rev().find(|o| o.ts_ns() <= target)?;
					// SPL's own zero guard: an OI of exactly zero is a dead contract, reported as no change.
					(target - o.ts_ns() < step_ns).then(|| if o.oi != 0.0 { (cur.oi - o.oi) / o.oi * 100.0 } else { 0.0 })
				};
				Some(OiSnap {
					oi_delta_5m_pct: ago(step_ns)?,
					oi_delta_15m_pct: ago(3 * step_ns)?,
				})
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

	/// The `buf` it clears as `advance`'s first act is the whole of its state — the depth it reads is
	/// `Book`'s to hold, not this node's.
	const HORIZON: Horizon = Horizon::Unit;
	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["bid", "ask", "bid_depth$", "ask_depth$", "imbalance", "spread%"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (book, frame): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let Some(&ts) = frame.cols().exec().last() else { return &self.buf };
		self.buf.push(book.and_then(|b| {
			let (ps, qs) = (b.prec().price.scale(), b.prec().qty.scale());
			let (bid, ask) = (b.best_bid()?, b.best_ask()?);
			let usd = |&(p, q): &(i32, u32)| (p as f64 / ps) * (q as f64 / qs);
			let top20_bid_depth_usd: f64 = b.bids().iter().take(DEPTH).map(usd).sum();
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

impl Flat for Screened {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}
}
structural_bump!(Screened);

impl Glance for Screened {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("hit")
	}
}

impl Cell for RsiScreener {
	type Out<'t> = &'t [Option<Screened>];
}
impl Node for RsiScreener {
	type Deps = (Bar1m, Price, Rsi);

	fn advance<'t>(&'t mut self, (bars, price, rsi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(bars.len(), price.len(), "Bar1m/Price rate mismatch");
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `ScreenHit` reads the two screeners as
		// one signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Rsi(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.rsi, rsi);
		for (b, p) in bars.iter().zip(price) {
			self.buf.push(match (p, self.rsi) {
				(Some(p), Some(rsi)) => (rsi.actual > c.rsi_threshold && p.change_1d_pct > *c.price_percent).then_some(Screened { ts_ns: b.close_ns(Bar1m::TF) }),
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
	type Deps = (Bar1m, Momentum);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `ScreenHit` reads the two screeners as
		// one signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Std(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.momentum, momentum);
		for b in bars {
			self.buf.push(self.momentum.and_then(|m| {
				// The vacuous slow leg must come from `indies.momentum.slow` and nothing else: `Momentum`
				// declines to publish at all when a configured slow leg is degenerate, so an absent Sharpe
				// here would otherwise let a wiring bug read as an unconditional hit.
				assert_eq!(m.slow.is_some(), strategy().indies.momentum.slow.is_some(), "a slow Sharpe disagrees with indies.momentum.slow");
				let slow = m.slow.is_none_or(|x| x > c.slow_overvalued);
				(slow && m.fast > c.fast_overvalued).then_some(Screened { ts_ns: b.close_ns(Bar1m::TF) })
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

impl Cell for ScreenHit {
	type Out<'t> = bool;
}
impl Node for ScreenHit {
	type Deps = (RsiScreener, StdScreener);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (rsi, std): DepOuts<'t, Self>) -> bool {
		assert_eq!(rsi.len(), std.len(), "RsiScreener/StdScreener rate mismatch");
		rsi.iter().chain(std).any(Option::is_some)
	}
}
impl Gate for ScreenHit {}
value_nudge!(ScreenHit);

impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = (RsiScreener, StdScreener);
	type When = (ScreenHit,);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (rsi, std): DepOuts<'t, Self>) -> Self::Out<'t> {
		// A batch spanning several bar closes can carry more than one hit; the latest is the one an
		// entry would act on, and the older ones are already stale by the time this tick publishes.
		let hit = rsi.iter().zip(std).rev().find_map(|(r, s)| r.or(*s)).expect("gate open ⇒ some screener fired");
		Some(Classified {
			ts_ns: hit.ts_ns,
			probability: 1.0,
			category: Category::None,
			quality: Quality::A,
		})
	}
}
value_nudge!(Classify);

// ─── deprecator ─────────────────────────────────────────────────────────────────────────────────

impl Flat for Intent {
	const DIMS: &'static [usize] = &[8];

	fn flat(&self, out: &mut [f64]) -> bool {
		// A fully-retraced trail has no level, which is the empty slot the flattening already spells.
		out.copy_from_slice(&[
			self.target_q,
			self.base_q,
			self.eval,
			self.lambda_atr,
			self.trail_fraction,
			self.sl,
			self.tp,
			self.trail_stop.unwrap_or(f64::NAN),
		]);
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

	const PLOTS: &'static [Plot] = &[
		Plot {
			slots: &[0, 1, 2, 3, 4],
			labels: &["target_q", "base_q", "eval", "lambda_atr", "trail_fraction"],
			..Plot::DEFAULT
		},
		Plot {
			slots: &[5, 6, 7],
			labels: &["sl", "tp", "trail_stop"],
			overlay: true,
			..Plot::DEFAULT
		},
	];

	fn advance<'t>(&'t mut self, (classify, atr, top): DepOuts<'t, Self>) -> Self::Out<'t> {
		let liq = &strategy().classification.liquidations;
		self.buf.clear();
		latest(&mut self.last_atr, atr);
		// Classification is honored only while Idle; once Active we drive solely off book ticks. No
		// book yet ⇒ don't enter — the screener keeps firing, so the next classification retries.
		if matches!(self.state, State::Idle)
			&& classify.is_some()
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
			let terminal = a.drain_deadline_ns.is_some_and(|dl| d.ts_ns >= dl);
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
				terminal,
			}));
			if terminal {
				self.state = State::Idle;
			}
		}
		&self.buf
	}
}
slice_nudge!(Deprecator, Option<Intent>);

// ─── graph ──────────────────────────────────────────────────────────────────────────────────────

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out TickOut;
	bar_1m: Bar1m,
	bar_5m: Bar5m,
	bar_15m: Bar15m,
	bar_1h: Bar1h,
	bar_4h: Bar4h,
	bar_1m_hist: Buffer<Bar1m, { Horizon::Span(SPAN_3M) }>,
	bar_5m_hist: Buffer<Bar5m, { mom_cap(Bar5m::TF) }>,
	bar_1h_hist: Buffer<Bar1h, REACH_1D>,
	bar_4h_hist: Buffer<Bar4h, { mom_cap(Bar4h::TF) }>,
	oi_hist: Buffer<OiRoot, OI_REACH>,
	price: Price,
	volume: Volume,
	rsi_averages: RsiAverages,
	rsi: Rsi,
	atr: Atr,
	momentum: Momentum,
	oi_delta: OiDelta,
	market_cap: MarketCap,
	book: Book,
	book_top: BookTop,
	rsi_screener: RsiScreener,
	std_screener: StdScreener,
	screen_hit: ScreenHit,
	classify: Classify,
	deprecator: Deprecator,
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

/// SPL sizes off live portfolio equity; the simulated venue's seed is the honest stand-in.
fn equity_usdt() -> f64 {
	crate::config::config().backtest.starting_balance
}

fn drain_grace_ns() -> i64 {
	strategy().classification.drain_grace.duration().as_nanos() as i64
}
