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
mod decision;
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
pub use decision::{Decided, Decision};
pub use deprecator::{Deprecator, Intent, TrailingStop};
pub use imbalance::Imbalance;
pub use momentum::{LEGS, Momentum};
pub use oi_delta::{OiDelta5m, OiDelta15m, OiReach};
pub use rsi::{AvgGain, AvgLoss, Knobs, Rsi, RsiDelta, RsiSeries};
pub use rsi_screener::RsiScreener;
pub use spread::Spread;
pub use std_screener::StdScreener;
use trading_data::{Armed, BookAnchors, BookDelta, BookDeltas, BookShape, Elems, Fidelity, Lanes, Mc, McRoot, Oi, OiRoot, Over, TradeCols, Trades};
pub use trading_data::{Bar, RsiValues};
// a `type Deps` const expression is re-expanded here, so the `TF_*` a dep names has to resolve here too.
use v_utils::*;

trading_data::node_alias! {
	/// The compiled screener — the gate `Classify` and everything under it hangs dormant off. The
	/// other arm stays written and unwired: naming it here is the whole of switching, and nothing
	/// pulls the indies it reads on its behalf meanwhile.
	pub Screener = StdScreener;
}
pub use volume_1h::Volume1h;
pub use volume_1m::Volume1m;
pub use volume_4h::Volume4h;

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols], deltas: BookDeltas[BookDelta], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out TickOut;
	outputs { bar_1m: trading_data::Bars<{ TF_1MIN }>, deprecator: Deprecator, rsi: Rsi }
}

const _: () = assert!(
	tally(true) == 0,
	"the partial count moved: close the omission, or say in the commit what this graph's new node leaves out of its derivative"
);
const _: () = assert!(
	tally(false) == 31,
	"the opaque count moved: write algebra, or say in the commit why this graph needs another hand-written node"
);
/// The hatch, pinned (`r[kernels.fidelity.stated]`). Two numbers, because a node with no algebra and
/// a node whose algebra is narrower than what its body read are different admissions. Either may fall
/// silently; either may only rise in a diff that argues for it.
const fn tally(partial: bool) -> usize {
	let (mut n, mut i) = (0, 0);
	while i < Graph::FIDELITY.len() {
		n += match (Graph::FIDELITY[i].1, partial) {
			(Fidelity::Partial(_), true) | (Fidelity::Opaque(_), false) => 1,
			_ => 0,
		};
		i += 1;
	}
	n
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
