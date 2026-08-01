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

mod atr;
mod avg_gain;
mod avg_loss;
mod bar;
mod book_top;
mod change_1d;
mod change_3m;
mod classify;
mod deprecator;
mod imbalance;
mod market_cap;
mod momentum;
mod oi_delta;
mod rsi;
mod rsi_delta;
mod rsi_screener;
mod screened;
mod spread;
mod std_screener;
mod volume_1h;
mod volume_1m;
mod volume_4h;

pub use atr::Atr;
pub use avg_gain::AvgGain;
pub use avg_loss::AvgLoss;
pub use bar::{Bar, Bar1h, Bar1m, Bar4h, Bar5m, Bar15m};
pub use book_top::{BookTop, BookTopSnap};
pub use change_1d::Change1d;
pub use change_3m::Change3m;
pub use classify::{Category, Classified, Classify, Quality};
pub use deprecator::{Deprecator, Intent, TrailingStop};
pub use imbalance::Imbalance;
pub use market_cap::{MarketCap, McSnap};
pub use momentum::{MOM_PERIODS, Momentum, mom_cap};
pub use oi_delta::{OiDelta5m, OiDelta15m};
pub use rsi::{Rsi, RsiValues};
pub use rsi_delta::RsiDelta;
pub use rsi_screener::RsiScreener;
pub use screened::Screened;
pub use spread::Spread;
pub use std_screener::StdScreener;
use trading_data::{Armed, Book, BookAnchors, BookDeltas, BookShape, Buffer, DeltaFrame, Horizon, Lanes, Mc, McRoot, Oi, OiRoot, TradeCols, Trades};
pub use volume_1h::Volume1h;
pub use volume_1m::Volume1m;
pub use volume_4h::Volume4h;

use crate::nodes::{change_1d::REACH_1D, change_3m::SPAN_3M, oi_delta::OI_REACH};

/// Caches a slower dep's latest publish as a level, for a node clocked by a faster one. A dep that
/// declined this tick is not a publish, so the cached level stands.
///
/// `ticks` is how many of its own the caller is about to apply the level to. A publish landing in a
/// batch that carries more than one of those is unplaceable — neither slice says which ticks precede
/// it — so the early ones would read a level out of their own future. Reaching that needs a batch
/// window at least as wide as the consumer's period, which is not a case to guess through.
fn latest<T: Copy>(slot: &mut Option<T>, dep: &[Option<T>], ticks: usize) {
	let Some(v) = dep.iter().flatten().last() else { return };
	assert!(ticks <= 1, "a level published inside a {ticks}-tick batch cannot be placed against those ticks");
	*slot = Some(*v);
}

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out TickOut;
	latch { armed: Armed<Deprecator> }
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
	change_3m: Change3m,
	change_1d: Change1d,
	volume_1m: Volume1m,
	volume_1h: Volume1h,
	volume_4h: Volume4h,
	rsi_delta: RsiDelta,
	avg_gain: AvgGain,
	avg_loss: AvgLoss,
	rsi: Rsi,
	atr: Atr,
	momentum: Momentum,
	oi_delta_5m: OiDelta5m,
	oi_delta_15m: OiDelta15m,
	market_cap: MarketCap,
	rsi_screener: RsiScreener,
	std_screener: StdScreener,
	classify: Classify,
	armed: Armed<Deprecator>,
	book: Book,
	book_top: BookTop,
	imbalance: Imbalance,
	spread: Spread,
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
