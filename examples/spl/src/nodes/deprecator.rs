use core::fmt;

use trading_data::{Armed, Cell, DepOuts, Direction, Episode, Episodic, Flat, Gating, Glance, Plot, Reading, Runs, Sampling, Side, TriggerOut, node, slice_nudge};
use v_utils::*;

use super::{
	atr::Atr,
	book_top::BookTop,
	decision::{Decided, Decision},
};
use crate::config::strategy;

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
	/// trail fires. The stop is absent once fully retraced — at full certainty the term has deprecated
	/// all the size it controls, so it stops drawing.
	pub fn step(&mut self, price: f64) -> (f64, f64, Reading) {
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
			(true, _) => Reading::ABSENT,
			(false, Side::Buy) => Reading::from(*extreme - self.distance),
			(false, Side::Sell) => Reading::from(*extreme + self.distance),
		};
		(1.0 - self.severity * self.certainty, 1.0 - self.certainty, stop)
	}
}

/// A change in the standing intent of an open episode — the persisted intent stream, minus the
/// execution fields. A book tick that would republish the last value publishes nothing: the out is
/// read as what stands (`Sampling`-shaped), so an element is a *move*, and absence of elements
/// means the last one still holds.
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
	/// The level where the trail fires; absent once fully retraced, when it stops drawing.
	pub trail_stop: Reading,
	pub draining: bool,
	/// The episode's last intent: the drain deadline passed on this book tick, so this one is
	/// published and `Active` closes. A reader has no other way to tell a spent episode from a book
	/// tick that merely declined to publish.
	pub terminal: bool,
}

/// Bit-identity, so an intent stream can be compared across implementations that share nothing else.
/// Deliberately not `PartialEq`: an intent carries `f64`s, and equality on those is the comparison
/// nobody wants — this is the digest's alphabet, not an ordering key.
impl core::hash::Hash for Intent {
	fn hash<H: core::hash::Hasher>(&self, h: &mut H) {
		h.write_i64(self.ts_ns);
		h.write_u8((self.side == Side::Buy) as u8);
		for b in [self.base_q, self.target_q, self.eval, self.lambda_atr, self.trail_fraction, self.sl, self.tp] {
			h.write_u64(b.to_bits());
		}
		h.write_u64(self.trail_stop.get().map_or(0, f64::to_bits));
		h.write_u8(self.draining as u8 | (self.terminal as u8) << 1);
	}
}

impl Intent {
	/// Value-plane equality: the `flat` slots plus the booleans beside them. `ts_ns` is when, not
	/// what, which is the whole difference between this and the digest's bit-identity above.
	pub fn same_value(&self, other: &Self) -> bool {
		let (mut a, mut b) = ([0.0; 8], [0.0; 8]);
		self.flat(&mut a);
		other.flat(&mut b);
		// bitwise, so an absent trail_stop reads equal to an absent trail_stop
		a.iter().zip(&b).all(|(x, y)| x.to_bits() == y.to_bits()) && self.side == other.side && self.draining == other.draining && self.terminal == other.terminal
	}
}

impl Flat for Intent {
	/// The trail's level alone: an intent stands whether or not the trail still draws one.
	const ABSENTABLE: bool = true;
	const DIMS: &'static [usize] = &[8];

	fn flat(&self, out: &mut [f64]) -> bool {
		let (fields, trail) = out.split_at_mut(7);
		fields.copy_from_slice(&[self.target_q, self.base_q, self.eval, self.lambda_atr, self.trail_fraction, self.sl, self.tp]);
		self.trail_stop.flat(trail);
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
	last_decision: Option<Decided>,
	/// What the stream last said, so a book tick that moves nothing publishes nothing. Dropped by
	/// the commutation reset with everything else, so an episode's first intent always publishes.
	last_published: Option<Intent>,
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
	type Out<'t> = &'t [Intent];
}
#[node]
impl Runs for Deprecator {
	/// The ATR is sampled rather than cached here: it measures the market, not the episode, so the
	/// commutation reset below has no business dropping it — a new episode enters on the envelope
	/// standing now, not on a re-warm.
	type Deps = (Gating<Armed<Deprecator>>, Decision, Sampling<Atr<{ TF_1MIN }>>, BookTop);

	const PLOTS: &'static [Plot] = &[
		Plot {
			slots: &[0, 1, 2, 3, 4],
			labels: &[&["target_q", "base_q", "eval", "lambda_atr", "trail_fraction"]],
			..Plot::DEFAULT
		},
		Plot {
			slots: &[5, 6, 7],
			labels: &[&["sl", "tp", "trail_stop"]],
			overlay: true,
			..Plot::DEFAULT
		},
	];
	const WHY: &'static str = "an episode walk driven by control flow rather than arithmetic";

	fn emit(&mut self, (armed, decision, atr, top): DepOuts<'_, Self>, out: &mut Vec<Intent>) {
		assert!(armed, "a gating dep reads true inside `emit`");
		let liq = &strategy().classification.liquidations;
		// The arming tick and the ticks that act on it are different lanes: `Decision` is trade-clocked,
		// and every book tick that could enter on it is a later one. The latch carries the *fact* of the
		// hit across them, this carries its content — and the commutation reset drops it, so no episode
		// enters on the decision of the one before it.
		if decision.is_some_and(|d| d.direction != Direction::Flat) {
			self.last_decision = decision;
		}

		for d in top {
			// nothing to read is nothing to move: the last published intent still stands.
			let Some(d) = d else { continue };
			if let (State::Idle, Some(dec)) = (&self.state, self.last_decision) {
				let entry_price = d.mid();
				let side = Side::try_from(dec.direction).expect("the latch arms on a non-flat direction");
				self.state = State::Active(Active {
					side,
					base_q: *dec.size / entry_price,
					entry_price,
					trail: TrailingStop::new(side, entry_price * *liq.trail_pct, *liq.trail_severity),
					drain_deadline_ns: None,
				});
			}
			// Management needs `target_q` off the ATR envelope; skip until the first ATR lands.
			let (State::Active(a), Some(atr)) = (&mut self.state, atr) else { continue };
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
			let intent = Intent {
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
			};
			if !self.last_published.is_some_and(|l| l.same_value(&intent)) {
				self.last_published = Some(intent);
				out.push(intent);
			}
			if terminal {
				self.state = State::Idle;
			}
		}
	}
}
slice_nudge!(Deprecator, Intent);

#[node]
impl Episodic for Deprecator {
	type Trigger = Decision;

	fn arms<'t>(d: TriggerOut<'t, Self>) -> bool {
		d.is_some_and(|d| d.direction != Direction::Flat)
	}
}
