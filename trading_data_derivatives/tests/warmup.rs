//! Where a Wilder chain starts having a number, read off the nodes rather than off the expressions.
//!
//! `trading_data_derivatives`' own unit test drives [`wilder`](trading_data_derivatives::wilder)
//! directly and checks the goldens; nothing there says which *elements* of a run carry a value. That
//! boundary is the thing a change of absence representation could move without any golden noticing,
//! so it is pinned here: absent for exactly the cold elements, a number from the first warm one, and
//! the two stages compose — a half-warm [`Rsi`] is wholly absent.

use trading_data_core::Ts;
use trading_data_dag::{Buffering, Cell, Elems, Folding, Rows, Unbounded, graph, slice_nudge};
#[allow(
	unused_imports,
	reason = "rust#52234: `graph!` reaches a node's shim textually, so the import is the only thing that puts it in scope"
)]
use trading_data_derivatives::{__td_node_AvgGain, __td_node_AvgLoss, __td_node_Rsi, __td_node_RsiDelta};
use trading_data_derivatives::{AvgGain, AvgLoss, Bar, Rsi, RsiDelta, RsiSpec};

/// The bars arrive from the test rather than from a `Closes` over trades: what is under test is the
/// chain above them, and a root series is the shortest thing that feeds it.
struct Closes;
impl Cell for Closes {
	type Out<'t> = &'t [Bar];
}
slice_nudge!(Closes, Bar);

/// Short on purpose. The boundary is `base_len` deltas then `smooth_len` values, and a 14/9 spec
/// would need 24 bars to say the same thing.
struct Tiny;
impl RsiSpec for Tiny {
	const NAME: &'static str = "3/2";

	fn base_len() -> usize {
		3
	}

	fn smooth_len() -> usize {
		2
	}
}

graph! {
	struct G;
	batches Batches;
	roots { bars: Closes[Bar] };
	out GOut;
	outputs { delta: RsiDelta<Closes>, gain: AvgGain<Closes, Tiny>, loss: AvgLoss<Closes, Tiny>, rsi: Rsi<Closes, Tiny> }
}

fn bar(i: i64, close: f64) -> Bar {
	Bar {
		ts_close: Ts::from_nanos((i + 1) * 60_000_000_000),
		open: close,
		high: close,
		low: close,
		close,
		vol_base: 1.0,
	}
}

/// A rising-then-falling walk, so `AvgGain` and `AvgLoss` both see samples and neither ratio is
/// degenerate — the numbers are not what is asserted, but a leg that only ever saw zeros would warm
/// on values that could not tell a bug from a boundary.
fn walk(n: usize) -> Vec<Bar> {
	(0..n).map(|i| bar(i as i64, 100.0 + (i as f64 * 0.7).sin() * 5.0)).collect()
}

/// One bar per tick, and every out concatenated back into the run the chain published.
#[derive(Default)]
struct Runs {
	delta: Vec<Option<f64>>,
	gain: Vec<Option<f64>>,
	loss: Vec<Option<f64>>,
	rsi: Vec<Option<f64>>,
}

fn drive(bars: &[Bar]) -> Runs {
	let mut g = G::default();
	let mut r = Runs::default();
	for b in bars {
		let o = g.tick(0, Batches { bars: core::slice::from_ref(b) });
		r.delta.extend(o.delta.iter().copied());
		r.gain.extend(o.gain.iter().copied());
		r.loss.extend(o.loss.iter().copied());
		r.rsi.extend(o.rsi.iter().map(|v| v.map(|x| x.actual)));
	}
	r
}

/// The index of the first element carrying a number, and the claim that everything before it carries
/// none — a single `position` would pass on a run that came back absent afterwards.
fn boundary(run: &[Option<f64>]) -> usize {
	let first = run.iter().position(Option::is_some).unwrap_or_else(|| panic!("never warmed over {} elements", run.len()));
	assert!(run[..first].iter().all(Option::is_none), "absence is a prefix, not a scatter: {run:?}");
	assert!(run[first..].iter().all(Option::is_some), "warm is forever: {run:?}");
	assert!(run[first..].iter().flatten().all(|v| v.is_finite()), "a warm element is a number: {run:?}");
	first
}

/// Rate-preserving throughout: every stage publishes one element per bar, cold ones included. That is
/// what makes "absent for exactly the cold elements" a statement about *which* elements rather than
/// about how many there are.
#[test]
fn every_stage_publishes_one_element_per_bar() {
	let bars = walk(12);
	let r = drive(&bars);
	assert_eq!((r.delta.len(), r.gain.len(), r.loss.len(), r.rsi.len()), (12, 12, 12, 12));
}

/// A change needs two closes, so the first bar of the run carries nothing and every bar after it
/// carries a number.
#[test]
fn a_delta_is_absent_only_for_the_bar_that_has_nothing_behind_it() {
	assert_eq!(boundary(&drive(&walk(12)).delta), 1);
}

/// `base_len` deltas warm a Wilder average, and the first bar contributed none — so the first three
/// deltas land on bars 1..=3 and bar 3 is the one that has a value.
#[test]
fn a_wilder_average_is_absent_for_exactly_its_cold_bars() {
	let r = drive(&walk(12));
	assert_eq!(boundary(&r.gain), Tiny::base_len());
	assert_eq!(boundary(&r.loss), Tiny::base_len());
}

/// The EMA sees its first value where the averages warm, and needs `smooth_len` of them. Both slots
/// of the item are one reading: while the smoothing leg is cold the whole element is absent, even
/// though `actual` is already a number the body computed.
#[test]
fn a_half_warm_rsi_is_wholly_absent() {
	let r = drive(&walk(12));
	assert_eq!(boundary(&r.rsi), Tiny::base_len() + Tiny::smooth_len() - 1);
	assert!(r.rsi[..Tiny::base_len() + Tiny::smooth_len() - 1].iter().all(Option::is_none));
}
