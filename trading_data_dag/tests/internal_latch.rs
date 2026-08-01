//! The *internal* latch: same dynamics as `latch.rs`, but the arm is a graph node rather than a
//! root. `Episodic` + `Armed<N>` seal the loop — the gate a node arms is the gate its own terminal
//! out cuts — and the episode is an `Emit`: the engine owns its run, so dark is the empty one and
//! commutation hands back a buffer rather than a fresh allocation.
//!
//! The load-bearing pins: an arm leg going dark mid-episode does not cut the latch, and a terminal
//! batch whose terminal element is *not* last still commutates (`Episode for &[T]` is `any`, not
//! `last`).

use trading_data_dag::{
	Armed, Bump, Cell, DepOuts, Emit, EmitOuts, Episode, Episodic, Flat, Gate, Gating, Glance, Horizon, Node, NodeMeta, TriggerOut, graph, shadowed, slice_nudge, value_nudge,
};

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

/// A plain gate on the arm leg. Closing it mid-episode is the original ask: the latch must hold.
#[derive(Clone, Default)]
struct Open(bool);
impl Cell for Open {
	type Out<'t> = bool;
}
impl Node for Open {
	type Deps = ();

	fn advance<'t>(&'t mut self, (): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0
	}
}
impl Gate for Open {}

/// The arm leg: gated on `Open`, so it can go dark while the episode runs.
#[derive(Clone, Default)]
struct Classify {
	calls: u32,
}
impl Cell for Classify {
	type Out<'t> = Option<Pulse>;
}
impl Node for Classify {
	type Deps = (Gating<Open>, Trig);

	fn advance<'t>(&'t mut self, (open, trig): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(open, "a gating dep reads true inside `advance`");
		self.calls += 1;
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
	calls: u32,
}
impl Cell for Deprec {
	type Out<'t> = &'t [Option<Phase>];
}
impl Emit for Deprec {
	type Deps = (Gating<Armed<Deprec>>, Classify, Feed);

	fn emit(&mut self, (_, _, feed): EmitOuts<'_, Self>, out: &mut Vec<Option<Phase>>) {
		self.calls += 1;
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

impl Episodic for Deprec {
	type Trigger = Classify;

	fn arms<'t>(c: TriggerOut<'t, Self>) -> bool {
		c.is_some()
	}
}

/// A second leg gated on the same latch, and not the one that cuts it: commutation resets every
/// gated field, not just the `Cut`.
#[derive(Clone, Default)]
struct Leg {
	calls: u32,
}
impl Cell for Leg {
	type Out<'t> = &'t [Option<Pulse>];
}
impl Emit for Leg {
	type Deps = (Gating<Armed<Deprec>>, Feed);

	fn emit(&mut self, (_, feed): EmitOuts<'_, Self>, out: &mut Vec<Option<Pulse>>) {
		self.calls += 1;
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
	roots { trig: Trig[Pulse], feed: Feed[Pulse] };
	out GOut;
	outputs { deprec }
	observe { leg, ticks }
	latch { armed: Armed<Deprec> }
	open: Open,
	classify: Classify,
	armed: Armed<Deprec>,
	emit deprec: Deprec,
	emit leg: Leg,
	ticks: Ticks,
}

/// One tick's out, owned: `tick` lends the graph for the out's whole lifetime, so the call counters
/// — the only witness that a dark node was skipped rather than merely emptied — can only be read
/// once it is done.
#[derive(Debug)]
struct Snap {
	armed: bool,
	deprec: Vec<Option<Phase>>,
	leg: Vec<Option<Pulse>>,
	ticks: f64,
	/// `(classify, deprec, leg)`.
	calls: (u32, u32, u32),
}

fn tick(g: &mut G, trig: &[Pulse], feed: &[Pulse]) -> Snap {
	let o = g.tick(Batches { trig, feed });
	let (armed, deprec, leg, ticks) = (o.armed, o.deprec.to_vec(), o.leg.to_vec(), o.ticks);
	Snap {
		armed,
		deprec,
		leg,
		ticks,
		calls: (g.classify.calls, g.deprec.calls, g.leg.calls),
	}
}

const PULSE: &[Pulse] = &[Pulse];
const IDLE: &[Pulse] = &[];
/// Three feed elements in one tick, so a single batch carries the whole episode tail:
/// `[Done, None, None]`.
const WIDE: &[Pulse] = &[Pulse, Pulse, Pulse];

#[test]
fn arm_from_a_node_hold_through_a_dark_arm_cut_from_within() {
	let mut g = G { open: Open(true), ..G::default() };

	// idle: latch down, both gated legs dark — and dark is `&[]`, not a fabricated zero. The arm leg
	// is not gated on the latch, so it advanced.
	let s = tick(&mut g, IDLE, PULSE);
	assert!(!s.armed);
	assert_eq!((s.deprec.as_slice(), s.leg.as_slice()), (&[][..], &[][..]));
	assert_eq!(s.calls, (1, 0, 0));

	// the arm fires: both gated legs come live, from `Default`.
	let s = tick(&mut g, PULSE, PULSE);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(1))]);
	assert_eq!(s.leg, [Some(Pulse)]);
	assert_eq!(s.calls, (2, 1, 1));

	// the arm's own gate closes mid-episode — the arm goes dark, the latch must hold.
	g.open = Open(false);
	let s = tick(&mut g, IDLE, PULSE);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(2))]);
	assert_eq!(s.calls, (2, 2, 2), "the arm leg is dark: its gate is closed");

	// a trigger during the episode is absorbed, not a restart.
	g.open = Open(true);
	let s = tick(&mut g, PULSE, WIDE);
	assert!(s.armed);
	// the terminal element is *not* last: `Episode for &[T]` must read `any`, not `last`.
	assert_eq!(s.deprec, [Some(Phase::Done), None, None]);
	assert_eq!(s.leg.len(), 3);

	// commutated: latch down, both legs dark, and the during-episode trigger did not re-arm.
	let s = tick(&mut g, IDLE, PULSE);
	assert!(!s.armed, "the terminal element cut the latch — under `.last()` semantics this never fires");
	assert_eq!((s.deprec.as_slice(), s.leg.as_slice()), (&[][..], &[][..]));

	// re-arm: fresh episode from `Default` — the reset is observable as the counters restarting at 1.
	let s = tick(&mut g, PULSE, PULSE);
	assert!(s.armed);
	assert_eq!(s.deprec, [Some(Phase::Degrading(1))]);
	assert_eq!(s.calls, (5, 1, 1), "six ticks, one of them with the arm leg dark");

	// ungated bystander: never reset, counted every tick.
	assert_eq!(s.ticks, 6.0);
}

// the discount must hold in const position — that's where graph! calls it.
const LATCH_DISCOUNT: &[NodeMeta] = &[
	NodeMeta {
		name: "latch",
		deps: &["arm"],
		reach: &[Horizon::Unbounded],
		folds: &[true],
		gates: &[false],
		retains: false,
		latch: true,
	},
	// bounded, ungated, and its only consumer sits behind the latch — a plain gate would shadow it.
	NodeMeta {
		name: "warm",
		deps: &["root"],
		reach: &[Horizon::Unit],
		folds: &[false],
		gates: &[false],
		retains: false,
		latch: false,
	},
	NodeMeta {
		name: "episode",
		deps: &["latch", "warm"],
		reach: &[Horizon::Unit, Horizon::Unit],
		folds: &[false, false],
		gates: &[true, false],
		retains: false,
		latch: false,
	},
];
const _: () = assert!(!shadowed("warm", LATCH_DISCOUNT));
