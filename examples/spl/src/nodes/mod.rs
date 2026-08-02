//! The scam_pump_liqs strategy as a compile-time step graph: five roots feed per-timeframe bars, the
//! indies derive off those, the configured screener fires off the ones it reads, `Classify` turns a
//! hit into a distribution, and `Deprecator` runs the per-book-tick degrader that produces
//! `target_q`.
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
//! decides is the screener's *thresholds*; which arm runs is [`Screener`], because a gate is named in
//! `Node::Deps` like any other input and there is no runtime selection of one.

/// `Flat` + `Bump` for a record whose observed slots are plain `f64` fields, in the order given.
macro_rules! flat_fields {
	($T:ty [$($f:ident),+ $(,)?]) => {
		impl trading_data::Flat for $T {
			const DIMS: &'static [usize] = &[[$(stringify!($f)),+].len()];

			fn flat(&self, out: &mut [f64]) -> bool {
				out.copy_from_slice(&[$(self.$f),+]);
				true
			}
		}
		impl trading_data::Bump for $T {
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
		impl trading_data::Bump for $T {
			fn bump(self, _: usize, _: f64) -> (Self, f64) {
				(self, 0.0)
			}
		}
	};
}

// each declares a cell, and a cell's dep shim is textually scoped — `graph!` below reaches them
// under the same unqualified names their deps are written in.
#[macro_use]
mod atr;
#[macro_use]
mod book_top;
#[macro_use]
mod change_1d;
#[macro_use]
mod change_3m;
#[macro_use]
mod classify;
#[macro_use]
mod deprecator;
#[macro_use]
mod imbalance;
#[macro_use]
mod momentum;
#[macro_use]
mod oi_delta;
#[macro_use]
mod rsi;
#[macro_use]
mod rsi_screener;
#[macro_use]
mod spread;
#[macro_use]
mod std_screener;
#[macro_use]
mod volume_1h;
#[macro_use]
mod volume_1m;
#[macro_use]
mod volume_4h;

pub use atr::Atr;
pub use book_top::{BookTop, BookTopSnap};
pub use change_1d::Change1d;
pub use change_3m::Change3m;
pub use classify::{Category, Classified, Classify, Quality};
pub use deprecator::{Deprecator, Intent, TrailingStop};
pub use imbalance::Imbalance;
pub use momentum::{MOM_PERIODS, Momentum, mom_cap};
pub use oi_delta::{OiDelta5m, OiDelta15m};
pub use rsi::{AvgGain, AvgLoss, Knobs, RSI_TF, Rsi, RsiDelta, RsiSeries};
pub use rsi_screener::RsiScreener;
pub use spread::Spread;
pub use std_screener::StdScreener;
use trading_data::{Armed, BookAnchors, BookDeltas, BookShape, DeltaFrame, Horizon, Lanes, Mc, McRoot, Oi, OiRoot, TradeCols, Trades};
pub use trading_data::{Bar, RsiValues};
use v_utils::Timeframe;

// the graph reaches every cell through a shim keyed on its name, and a bare `use` leaves no shim
// behind — so the series this strategy runs on are aliased rather than imported.
/// The clocks this strategy runs on, and the whole of what makes its series the periods they are.
pub const M1: Timeframe = Timeframe::from_str_const("1m");
pub const M5: Timeframe = Timeframe::from_str_const("5m");
pub const M15: Timeframe = Timeframe::from_str_const("15m");
pub const H1: Timeframe = Timeframe::from_str_const("1h");
pub const H4: Timeframe = Timeframe::from_str_const("4h");

trading_data::node_alias! { pub Bar1m = trading_data::Bars<M1>; }
trading_data::node_alias! { pub Bar5m = trading_data::Bars<M5>; }
trading_data::node_alias! { pub Bar15m = trading_data::Bars<M15>; }
trading_data::node_alias! { pub Bar1h = trading_data::Bars<H1>; }
trading_data::node_alias! { pub Bar4h = trading_data::Bars<H4>; }

trading_data::node_alias! {
	/// The compiled screener — the gate `Classify` and everything under it hangs dormant off. The
	/// other arm stays written and unwired: naming it here is the whole of switching, and nothing
	/// pulls the indies it reads on its behalf meanwhile.
	pub Screener = StdScreener;
}
pub use volume_1h::Volume1h;
pub use volume_1m::Volume1m;
pub use volume_4h::Volume4h;

// a buffer's reach is written where the dep is, and lands here — so what those reaches are spelled
// in has to be in scope here too.
use crate::nodes::{change_1d::REACH_1D, change_3m::SPAN_3M, oi_delta::OI_REACH};

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out TickOut;
	outputs { deprecator: Deprecator, rsi: Rsi }
}

/// Caches a slower dep's latest publish as a level, for a node clocked by a faster one. A dep that
/// declined this tick is not a publish, so the cached level stands.
fn latest<T: Copy>(slot: &mut Option<T>, dep: &[Option<T>], ticks: usize) {
	let Some(v) = dep.iter().flatten().last() else { return };
	assert!(ticks <= 1, "a level published inside a {ticks}-tick batch cannot be placed against those ticks");
	*slot = Some(*v);
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
