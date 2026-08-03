//! `Cell::CLOCK` through `graph!`: a node that states its own publish rate, and the engine that owns
//! the boundary. The question the whole thing answers is which of two readings a consumer holds —
//! the one taken when the period closed, or the one the last tick happened to revise.

use trading_data_dag::{Bump, Cell, Emit, EmitOuts, Flat, Folding, Glance, Horizon, Sampling, Stamped, always_present, graph, node, slice_nudge};
use v_utils::Timeframe;

const MIN: Timeframe = Timeframe(60_000);

/// One unit of `v` is one second of `ts`, so a fixture's numbers double as its timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Tick {
	ts: i64,
	v: f64,
}
fn t(v: f64) -> Tick {
	Tick { ts: (v * 1e9) as i64, v }
}
impl Stamped for Tick {
	fn ts_ns(&self) -> i64 {
		self.ts
	}
}
impl Flat for Tick {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.v;
		true
	}
}
impl Bump for Tick {
	fn bump(mut self, _: usize, h: f64) -> (Self, f64) {
		self.v += h;
		(self, h)
	}
}
impl Glance for Tick {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.v)
	}
}
always_present!(Tick);

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// Publishes the level it stands on once a minute, and states so. Nothing in `emit` knows what a
/// period is — that is the point: the rate is declared, not re-derived from whatever arrived.
#[derive(Clone, Default)]
struct Minutely;
impl Cell for Minutely {
	type Out<'t> = &'t [f64];

	const CLOCK: Option<Timeframe> = Some(MIN);
}
#[node]
impl Emit for Minutely {
	type Deps = (Sampling<Src>,);

	fn emit(&mut self, (level,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		out.extend(level.map(|x| x.v));
	}
}
slice_nudge!(Minutely, f64);

/// The same reading with no clock: it republishes on every tick, so what it holds is a measurement
/// of the feed's batching. The negative control below is this node's output, not the graph's.
#[derive(Clone, Default)]
struct Continuous;
impl Cell for Continuous {
	type Out<'t> = &'t [f64];
}
#[node]
impl Emit for Continuous {
	type Deps = (Sampling<Src>,);

	fn emit(&mut self, (level,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		out.extend(level.map(|x| x.v));
	}
}
slice_nudge!(Continuous, f64);

/// The accumulator shape: the same rate, held by the node instead of the engine. A period closes on
/// the first item of the next one, so this publishes on whatever tick carries that item — which is
/// rarely the first tick of the period, and never one the engine could have guessed.
#[derive(Clone, Default)]
struct Closes(Option<(i64, f64)>);
impl Cell for Closes {
	type Out<'t> = &'t [f64];

	const CLOCK: Option<Timeframe> = Some(MIN);
}
#[node]
impl Emit for Closes {
	type Deps = (Folding<Src, { Horizon::Span(MIN) }>,);

	fn emit(&mut self, (items,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		for x in items {
			let period = x.ts / (MIN.0 * 1_000_000) as i64;
			match &mut self.0 {
				Some((p, v)) if *p == period => *v = x.v,
				slot => {
					if let Some((_, done)) = slot.replace((period, x.v)) {
						out.push(done);
					}
				}
			}
		}
	}
}
slice_nudge!(Closes, f64);

/// Downstream of an accumulator and clocked to the same period — the join `Bars` is over its two.
/// Its input is this tick's batch and no other, so the rate it declares is one it inherits.
#[derive(Clone, Default)]
struct Joined;
impl Cell for Joined {
	type Out<'t> = &'t [f64];

	const CLOCK: Option<Timeframe> = Some(MIN);
}
#[node]
impl Emit for Joined {
	type Deps = (Closes,);

	fn emit(&mut self, (closes,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		out.extend(closes);
	}
}
slice_nudge!(Joined, f64);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	outputs { minutely: Minutely, continuous: Continuous, closes: Closes, joined: Joined }
}

/// `Elems(n)` is a count until the producer's clock says what it is worth in wall clock. Const, so
/// the conversion a replay would derive its warmup depth through is checked where it is declared.
const _: () = {
	let clock = match <Minutely as Cell>::CLOCK {
		Some(tf) => tf,
		None => panic!("the clocked node under test declares one"),
	};
	assert!(Horizon::Elems(14).span(clock).0 == 14 * 60_000);
	assert!(Horizon::Span(Timeframe(300_000)).span(clock).0 == 300_000);
	assert!(Horizon::Unit.span(clock).0 == 60_000);
};

/// The API under test, in one function: a tick timeline and how the message stream is cut across it
/// in, the two series out. Every case below differs in the cut and in nothing else.
fn run(ticks: &[(f64, Vec<Tick>)]) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
	let mut g = G::default();
	let (mut clocked, mut every, mut closed, mut joined) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
	for (at, batch) in ticks {
		let o = g.tick((at * 1e9) as i64, Batches { src: batch });
		clocked.extend(o.minutely);
		every.extend(o.continuous);
		closed.extend(o.closes);
		joined.extend(o.joined);
	}
	(clocked, every, closed, joined)
}

/// Ticks every ten seconds over three minutes — the timeline every cut below shares.
fn timeline() -> Vec<f64> {
	(0..=18).map(|i| (i * 10) as f64).collect()
}

/// The message stream: one item mid-decade, so no item ever falls on a tick or on a boundary.
fn items() -> Vec<Tick> {
	(0..18).map(|i| t((i * 10 + 5) as f64)).collect()
}

/// Each item in the first tick at or after its own timestamp.
fn eager() -> Vec<(f64, Vec<Tick>)> {
	timeline()
		.into_iter()
		.map(|at| (at, items().into_iter().filter(|x| (x.v > at - 10.0) && x.v <= at).collect()))
		.collect()
}

/// A minute's items all arrive in the tick that closes it — the same messages, the same timeline,
/// one batch instead of six.
fn per_minute() -> Vec<(f64, Vec<Tick>)> {
	timeline()
		.into_iter()
		.map(|at| (at, items().into_iter().filter(|x| (x.v > at - 60.0) && x.v <= at && at % 60.0 == 0.0).collect()))
		.collect()
}

/// A batch cut by the boundary tick: the last two items of each minute arrive together on it, where
/// `eager` split them across the two ticks before.
fn straddling() -> Vec<(f64, Vec<Tick>)> {
	timeline()
		.into_iter()
		.map(|at| {
			let straddle = at % 60.0 == 0.0;
			let reach = if straddle { 20.0 } else { 10.0 };
			(
				at,
				items().into_iter().filter(|x| x.v > at - reach && x.v <= at && (straddle || (at + 10.0) % 60.0 != 0.0)).collect(),
			)
		})
		.collect()
}

/// Four periods are ticked, and the first of them has nothing to publish yet — so the clock fires
/// four times and speaks three, where the same node unclocked speaks on every tick it is warm for.
#[test]
fn a_clock_publishes_once_per_period() {
	let (clocked, every, ..) = run(&eager());
	assert_eq!(clocked, [55.0, 115.0, 175.0], "one reading per closed minute, taken at the boundary");
	assert_eq!(every.len(), 18, "the unclocked twin stands on the same level and republishes it every tick it has one");
}

/// The invariant: how the messages of a period were cut across its ticks is not something the node
/// clocked to it can see. Not claimed for a cut that delivers a message *after* the boundary that
/// would have published it — a clock pegs publication to the tick timeline, and late data is late.
#[test]
fn a_cut_within_the_period_does_not_move_the_reading() {
	let eager = run(&eager()).0;
	assert_eq!(eager, run(&per_minute()).0, "six batches or one, the level at the boundary is the same level");
	assert_eq!(eager, run(&straddling()).0, "a batch the boundary tick cuts is still wholly behind it");
}

/// The control: the same three cuts through the *unclocked* twin disagree, so the comparison above
/// is one that can fail.
#[test]
#[should_panic(expected = "a cut is visible to a node with no clock")]
fn without_a_clock_the_cut_is_the_reading() {
	let eager = run(&eager()).1;
	assert_eq!(eager, run(&per_minute()).1, "a cut is visible to a node with no clock");
}

/// A declared rate is not licence to withhold a tick. `Joined`'s input is this tick's batch, so a
/// tick it does not run on is a batch nobody sees again — and its producer publishes when a *period*
/// closed, on whatever tick carried the item that closed it, which is not the tick a period-of-`ts`
/// counter would have picked.
#[test]
fn a_clocked_node_reading_a_batch_is_not_withheld_a_tick() {
	for (name, cut) in [("eager", eager()), ("per minute", per_minute()), ("straddling", straddling())] {
		let (.., closed, joined) = run(&cut);
		assert_eq!(closed, joined, "{name}: what an accumulator published, the node clocked to the same period read");
		assert!(!closed.is_empty(), "{name}: a comparison of two empty vectors proves nothing");
	}
}
