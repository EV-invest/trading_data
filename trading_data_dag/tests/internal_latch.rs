//! The *internal* latch: same dynamics as `latch.rs`, but the arm is a graph node rather than a
//! root. `Episodic` + `Armed<N>` seal the loop — the gate a node arms is the gate its own terminal
//! out cuts — and the episode is an `Emit`: the engine owns its run, so dark is the empty one and
//! commutation hands back a buffer rather than a fresh allocation.
//!
//! The load-bearing pins: an arm leg going dark mid-episode does not cut the latch, and a terminal
//! batch whose terminal element is *not* last still commutates (`Episode for &[T]` is `any`, not
//! `last`).

use trading_data_dag::{Armed, Bump, Cell, DepOuts, Emit, EmitOuts, Episode, Episodic, Flat, Gate, Gating, Glance, Node, TriggerOut, graph, node, slice_nudge, value_nudge};

#[derive(Clone, Copy, Debug, PartialEq)]
struct Pulse;
impl Flat for Pulse {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}
}
impl Bump for Pulse {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}
impl Glance for Pulse {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.write_str("pulse")
	}
}

/// Arms the episode.
struct Trig;
impl Cell for Trig {
	type Out<'t> = &'t [Pulse];
}
slice_nudge!(Trig, Pulse);

/// Drives it: one element per tick, independent of the trigger.
struct Feed;
impl Cell for Feed {
	type Out<'t> = &'t [Pulse];
}
slice_nudge!(Feed, Pulse);

/// Drives the gate on the arm leg. Closing it mid-episode is the original ask: the latch must hold.
struct Sw;
impl Cell for Sw {
	type Out<'t> = &'t [bool];
}
slice_nudge!(Sw, bool);

#[derive(Clone, Default)]
struct Open(bool);
impl Cell for Open {
	type Out<'t> = bool;
}
#[node]
impl Node for Open {
	type Deps = (Sw,);

	fn advance<'t>(&'t mut self, (sw,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if let Some(&b) = sw.last() {
			self.0 = b;
		}
		self.0
	}
}
impl Gate for Open {}

/// Witnesses whether the arm leg ran: gated exactly as `Classify` is, and `None` is what dark looks
/// like — a count that stops climbing is the only thing telling a skipped node from an empty one.
#[derive(Clone, Default)]
struct Beat(f64);
impl Cell for Beat {
	type Out<'t> = Option<f64>;
}
#[node]
impl Node for Beat {
	type Deps = (Gating<Open>,);

	fn advance<'t>(&'t mut self, (open,): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(open, "a gating dep reads true inside `advance`");
		self.0 += 1.0;
		Some(self.0)
	}
}
value_nudge!(Beat);

/// The arm leg: gated on `Open`, so it can go dark while the episode runs.
#[derive(Clone, Default)]
struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Pulse>;
}
#[node]
impl Node for Classify {
	type Deps = (Gating<Open>, Trig);

	fn advance<'t>(&'t mut self, (open, trig): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(open, "a gating dep reads true inside `advance`");
		trig.first().copied()
	}
}
value_nudge!(Classify);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
	Degrading(u32),
	Done,
}
impl Episode for Phase {
	fn terminal(&self) -> bool {
		matches!(self, Phase::Done)
	}
}
impl Flat for Phase {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = match self {
			Phase::Degrading(t) => *t as f64,
			Phase::Done => -1.0,
		};
		true
	}
}
impl Bump for Phase {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}
impl Glance for Phase {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{self:?}")
	}
}

/// Batch-out episode, rate-preserving over `Feed`: one element per feed element. On the terminal
/// element it goes idle and keeps pushing `None` for the rest of the batch — so the terminal element
/// is not the last, which is exactly what `Episode for &[T]`'s `any` reads and `.last()` would miss.
#[derive(Clone, Default)]
struct Deprec {
	t: u32,
	idle: bool,
}
impl Cell for Deprec {
	type Out<'t> = &'t [Option<Phase>];
}
#[node]
impl Emit for Deprec {
	type Deps = (Gating<Armed<Deprec>>, Classify, Feed);

	fn emit(&mut self, (_, _, feed): EmitOuts<'_, Self>, out: &mut Vec<Option<Phase>>) {
		for _ in feed {
			if self.idle {
				out.push(None);
				continue;
			}
			self.t += 1;
			let phase = if self.t >= 3 { Phase::Done } else { Phase::Degrading(self.t) };
			self.idle = phase.terminal();
			out.push(Some(phase));
		}
	}
}
slice_nudge!(Deprec, Option<Phase>);

#[node]
impl Episodic for Deprec {
	type Trigger = Classify;

	fn arms<'t>(c: TriggerOut<'t, Self>) -> bool {
		c.is_some()
	}
}

/// A second leg gated on the same latch, and not the one that cuts it: commutation resets every
/// gated field, not just the `Cut`.
#[derive(Clone, Default)]
struct Leg;
impl Cell for Leg {
	type Out<'t> = &'t [Option<Pulse>];
}
#[node]
impl Emit for Leg {
	type Deps = (Gating<Armed<Deprec>>, Feed);

	fn emit(&mut self, (_, feed): EmitOuts<'_, Self>, out: &mut Vec<Option<Pulse>>) {
		out.extend(feed.iter().copied().map(Some));
	}
}
slice_nudge!(Leg, Option<Pulse>);

/// Ungated bystander: commutation must not reset it.
#[derive(Clone, Default)]
struct Ticks {
	n: u32,
}
impl Cell for Ticks {
	type Out<'t> = f64;
}
#[node]
impl Node for Ticks {
	type Deps = (Feed,);

	fn advance<'t>(&'t mut self, _: DepOuts<'t, Self>) -> Self::Out<'t> {
		self.n += 1;
		self.n as f64
	}
}

graph! {
	struct G;
	batches Batches;
	roots { trig: Trig[Pulse], feed: Feed[Pulse], sw: Sw[bool] };
	out GOut;
	outputs { deprec: Deprec, armed: Armed<Deprec>, leg: Leg, ticks: Ticks, beat: Beat }
}

/// One tick's out, owned: `tick` lends the graph for the out's whole lifetime.
#[derive(Debug)]
struct Snap {
	armed: bool,
	deprec: Vec<Option<Phase>>,
	leg: Vec<Option<Pulse>>,
	ticks: f64,
	beat: Option<f64>,
}

fn tick(g: &mut G, trig: &[Pulse], feed: &[Pulse], sw: &[bool]) -> Snap {
	let o = g.tick(0, Batches { trig, feed, sw });
	Snap {
		armed: o.armed,
		deprec: o.deprec.to_vec(),
		leg: o.leg.to_vec(),
		ticks: o.ticks,
		beat: o.beat,
	}
}

const PULSE: &[Pulse] = &[Pulse];
const IDLE: &[Pulse] = &[];
/// Three feed elements in one tick, so a single batch carries the whole episode tail:
/// `[Done, None, None]`.
const WIDE: &[Pulse] = &[Pulse, Pulse, Pulse];
const OPEN: &[bool] = &[true];
const SHUT: &[bool] = &[false];
/// No switch this tick: the gate keeps whatever it was set to.
const HOLD: &[bool] = &[];

#[test]
fn arm_from_a_node_hold_through_a_dark_arm_cut_from_within() {
	let mut g = G::default();

	// idle: latch down, both gated legs dark — and dark is `&[]`, not a fabricated zero. The arm leg
	// is not gated on the latch, so it advanced.
	let s = tick(&mut g, IDLE, PULSE, OPEN);
	assert!(!s.armed);
	assert_eq!((s.deprec.as_slice(), s.leg.as_slice()), (&[][..], &[][..]));
	assert_eq!(s.beat, Some(1.0));

	// the arm fires: both gated legs come live, from `Default`.
	let s = tick(&mut g, PULSE, PULSE, HOLD);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(1))]);
	assert_eq!(s.leg, [Some(Pulse)]);
	assert_eq!(s.beat, Some(2.0));

	// the arm's own gate closes mid-episode — the arm goes dark, the latch must hold.
	let s = tick(&mut g, IDLE, PULSE, SHUT);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(2))]);
	assert_eq!(s.beat, None, "the arm leg is dark: its gate is closed");

	// a trigger during the episode is absorbed, not a restart.
	let s = tick(&mut g, PULSE, WIDE, OPEN);
	assert!(s.armed);
	// the terminal element is *not* last: `Episode for &[T]` must read `any`, not `last`.
	assert_eq!(s.deprec, [Some(Phase::Done), None, None]);
	assert_eq!(s.leg.len(), 3);

	// commutated: latch down, both legs dark, and the during-episode trigger did not re-arm.
	let s = tick(&mut g, IDLE, PULSE, HOLD);
	assert!(!s.armed, "the terminal element cut the latch — under `.last()` semantics this never fires");
	assert_eq!((s.deprec.as_slice(), s.leg.as_slice()), (&[][..], &[][..]));

	// re-arm: fresh episode from `Default` — the reset is observable as the episode restarting at 1,
	// while the ungated `beat` kept climbing across the one tick it was dark for.
	let s = tick(&mut g, PULSE, PULSE, HOLD);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(1))]);
	assert_eq!(s.beat, Some(5.0), "six ticks, one of them with the arm leg dark");

	// ungated bystander: never reset, counted every tick.
	assert_eq!(s.ticks, 6.0);
}
