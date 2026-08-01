use core::fmt;

use trading_data::{Armed, Cell, DepOuts, Episode, Episodic, Flat, Glance, Horizon, Node, Plot, TriggerOut, slice_nudge};
use trading_data_core::Side;

use super::{atr::Atr, bar::Bar1m, book_top::BookTop, classify::Classify, latest};
use crate::config::strategy;

/// SPL's `execution::RISK_FRACTION`: fraction of equity committed per entry.
const RISK_FRACTION: f64 = 0.03;

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

impl Episode for Intent {
	fn terminal(&self) -> bool {
		self.terminal
	}
}

impl Glance for Intent {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?} q {:.4}/{:.4}{}", self.side, self.target_q, self.base_q, if self.draining { " draining" } else { "" })
	}
}

/// SPL's `ExecutorState` + `Degrader`, one for one. Entry mid-prices off the book (not the last
/// trade — on thin instruments that lags by whole percents and centres the envelope on a phantom);
/// every book tick then reduces one weighted ATR-envelope lambda against the trailing term.
#[derive(Clone, Default)]
pub struct Deprecator {
	state: State,
	last_atr: Option<f64>,
	buf: Vec<Option<Intent>>,
}
#[derive(Clone)]
struct Active {
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
	type When = (Armed<Deprecator>,);

	/// After the latch's reset the only cross-tick state is `last_atr`, which the next 1m publish
	/// refills — so an episode's head declines until then.
	const HORIZON: Horizon = Horizon::Span(Bar1m::TF);
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
		latest(&mut self.last_atr, atr, top.len());

		// Rate-preserving over `top`, so the two zip by index — that is how a consumer recovers the
		// tick's mid price for an intent without it being copied into one.
		for d in top {
			let Some(d) = d else {
				self.buf.push(None);
				continue;
			};
			// Entry prices off *this* tick's book rather than a cached one: the latch resets the node on
			// commutation, so a cache would be cold on exactly the tick the arming classification lands.
			// Classification is honored only while Idle; once Active we drive solely off book ticks.
			if matches!(self.state, State::Idle) && classify.is_some() {
				let entry_price = d.mid();
				//TODO: real selection over the full distribution; derive the side from the classification
				// context (e.g. cascade direction) rather than pinning it here.
				let side = Side::Buy;
				self.state = State::Active(Active {
					side,
					//TODO: scale RISK_FRACTION by certainty × quality via a historic-returns lookup.
					// SPL sizes off live portfolio equity; the simulated venue's seed is the honest stand-in.
					base_q: RISK_FRACTION * crate::config::config().backtest.starting_balance / entry_price,
					entry_price,
					trail: TrailingStop::new(side, entry_price * *liq.trail_pct, *liq.trail_severity),
					drain_deadline_ns: None,
				});
			}
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
				a.drain_deadline_ns = Some(d.ts_ns + strategy().classification.drain_grace.duration().as_nanos() as i64);
			}
			let draining = a.drain_deadline_ns.is_some();
			let terminal = a.drain_deadline_ns.is_some_and(|dl| d.ts_ns >= dl);
			self.buf.push(Some(Intent {
				ts_ns: d.ts_ns,
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

impl Episodic for Deprecator {
	type Trigger = Classify;

	fn arms<'t>(c: TriggerOut<'t, Self>) -> bool {
		c.is_some()
	}
}
