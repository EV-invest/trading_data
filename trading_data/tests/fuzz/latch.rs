//! **Phase 3a, the latch half.** A latch is momentary and its commutation is *deferred*: the
//! terminal tick's out is published, and only at the **next tick's start** does the latch drop and
//! its `Cut` reset to `Default`. A trigger arriving during a live episode — including on its
//! terminal tick — is absorbed and lost.
//!
//! Four sentences, and the oracle is a nine-line model of them. This is the one target here that is
//! a second model rather than a metamorphic comparison, and deliberately so: the claim is about
//! *when* a reset lands relative to a publication, which no reordering of the same inputs exposes —
//! there is nothing to permute. What the fuzzer buys over
//! `trading_data_dag/tests/latch.rs`'s six hand-written ticks is every trigger schedule instead of
//! one, and in particular the schedules that hand a pulse to the tick the commutation lands on.
//!
//! The retention beside it is the metamorphic half after all: a `Buffering` behind the latch is
//! pinned, so a node that woke onto it reads the window every element went into — which is a
//! function of the element stream, and is checked against it directly.

use trading_data::{Blind, Buffering, Bump, Cell, DepOuts, Elems, Episode, Flat, Gate, Gating, Glance, Latch, graph, node, slice_nudge, value_nudge};
use v_utils::fuzz::Frng;

use crate::fixture::{self, Tick};

pub const VERSION: u32 = v_utils::fuzz::fnv(&[v_utils::fuzz::FRNG_SRC, include_str!("latch.rs")]);

/// Ticks an episode runs before it is terminal. The model below is the only other place that knows.
const EPISODE: u32 = 3;
/// What the retention behind the latch reaches over.
const REACH: usize = 3;

#[derive(Clone, Copy, Debug)]
pub struct Pulse;
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

struct Trig;
impl Cell for Trig {
	type Out<'t> = &'t [Pulse];
}
slice_nudge!(Trig, Pulse);

/// The data series, separate from the trigger, so the retention a woken node reads is not itself a
/// function of the schedule that woke it.
struct Data;
impl Cell for Data {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Data, Tick);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Phase {
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

#[derive(Clone, Default)]
struct Standing {
	armed: bool,
}
impl Cell for Standing {
	type Out<'t> = bool;
}
#[node(latch)]
impl Blind for Standing {
	type Deps = (Trig,);

	const WHY: &'static str = "a latch fixture";

	fn advance<'t>(&'t mut self, (pulses,): DepOuts<'t, Self>) -> bool {
		self.armed |= !pulses.is_empty();
		self.armed
	}
}
impl Gate for Standing {}
impl Latch for Standing {
	type Cut = Run;

	fn commutate(&mut self) {
		self.armed = false;
	}

	fn standing(&self) -> bool {
		self.armed
	}
}

/// The episode the latch cuts: node-held state, reset to `Default` at the commutation.
#[derive(Clone, Default)]
struct Run {
	t: u32,
}
impl Cell for Run {
	type Out<'t> = Option<Phase>;
}
#[node]
impl Blind for Run {
	type Deps = (Gating<Standing>, Trig);

	const WHY: &'static str = "a latch fixture";

	fn advance<'t>(&'t mut self, (armed, _): DepOuts<'t, Self>) -> Option<Phase> {
		assert!(armed, "a gating dep reads true inside `advance`");
		self.t += 1;
		Some(if self.t >= EPISODE { Phase::Done } else { Phase::Degrading(self.t) })
	}
}
value_nudge!(Run);

/// Behind the same latch but not its `Cut`: it is darkened, never reset, and everything it reads is
/// retention the frame keeps whole. So a wake costs it nothing the sleep took away.
#[derive(Clone, Default)]
struct Recall;
impl Cell for Recall {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Recall {
	type Deps = (Gating<Standing>, Buffering<Data, Elems<{ REACH }>>);

	const WHY: &'static str = "a re-warm fixture";

	fn advance<'t>(&'t mut self, (_, hist): DepOuts<'t, Self>) -> Option<f64> {
		let w = hist.all();
		(!w.is_empty()).then(|| w.iter().map(|x| x.v).sum())
	}
}
value_nudge!(Recall);

/// The ungated bystander: a commutation resets the `Cut` and nothing else.
#[derive(Clone, Default)]
struct Ticks {
	n: u32,
}
impl Cell for Ticks {
	type Out<'t> = f64;
}
#[node]
impl Blind for Ticks {
	type Deps = (Trig,);

	const WHY: &'static str = "a latch fixture";

	fn advance<'t>(&'t mut self, _: DepOuts<'t, Self>) -> f64 {
		self.n += 1;
		self.n as f64
	}
}
value_nudge!(Ticks);

graph! {
	struct L;
	batches Batches;
	roots { trig: Trig[Pulse], data: Data[Tick] };
	out LOut;
	outputs { run: Run, armed: Standing, ticks: Ticks, recall: Recall }
}

/// The four sentences of the module doc, as state. `pending` is the whole of "deferred": the reset
/// is decided on the terminal tick and lands at the *start* of the next one, which is what lets the
/// terminal out be published at all.
#[derive(Default)]
struct Model {
	armed: bool,
	t: u32,
	pending: bool,
}

impl Model {
	fn tick(&mut self, pulse: bool) -> (bool, Option<Phase>) {
		if self.pending {
			self.armed = false;
			self.t = 0;
			self.pending = false;
		}
		// a trigger during a live episode is absorbed: `|=` on an already-standing latch is a no-op,
		// and the commutation above has already happened by the time this tick's pulse is read.
		self.armed |= pulse;
		if !self.armed {
			return (false, None);
		}
		self.t += 1;
		let phase = if self.t >= EPISODE { Phase::Done } else { Phase::Degrading(self.t) };
		self.pending = phase.terminal();
		(true, Some(phase))
	}
}

struct Beat {
	pulse: bool,
	data: Vec<Tick>,
}

fn trace(f: &mut Frng) -> Vec<Beat> {
	let mut out = Vec::new();
	while f.remaining() > 2 {
		// A third pulses, so an episode gets to run to terminal and commutate rather than being
		// re-armed on every tick — the schedules worth reaching are the ones around the commutation.
		let pulse = f.below(3) == 0;
		let n = f.below(3) as usize;
		out.push(Beat {
			pulse,
			data: (0..n).map(|i| fixture::t(out.len() as i64 * 1_000_000_000 + i as i64, f.span(-50.0, 50.0))).collect(),
		});
	}
	out
}

pub fn run(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let beats = trace(f);
	if beats.is_empty() {
		return Ok(());
	}
	let (mut g, mut model) = (L::default(), Model::default());
	// what the frame's retention holds, as a function of the element stream and nothing else
	let mut window: Vec<Tick> = Vec::new();
	let (mut episodes, mut commutations) = (0, 0);
	let mut was_armed = false;

	for (i, b) in beats.iter().enumerate() {
		let pulses: &[Pulse] = if b.pulse { &[Pulse] } else { &[] };
		let out = g.tick(0, Batches { trig: pulses, data: &b.data });
		let (armed, phase) = model.tick(b.pulse);
		window.extend_from_slice(&b.data);
		// a buffer trims to its declared reach *before* the append, so what `all()` answers is that
		// many elements behind the batch plus the whole batch.
		let reach = REACH + b.data.len();

		if verbose {
			eprintln!(
				"  [{i:>3}] pulse={} data={} → armed={} run={:?} recall={:?} ticks={}",
				b.pulse,
				b.data.len(),
				*out.armed,
				out.run,
				out.recall,
				*out.ticks
			);
		}

		check!(*out.armed == armed, "tick {i}: the latch stands {} where the model says {armed}", *out.armed);
		check!(out.run == phase, "tick {i}: the episode reads {:?} where the model says {phase:?}", out.run);
		// the latch is what decides demand, so the episode is readable exactly while it stands.
		check!(out.run.is_some() == *out.armed, "tick {i}: the episode read {:?} with the latch {}", out.run, *out.armed);
		// nothing but the `Cut` is reset: the bystander counts every tick there has been.
		check!(out.ticks == (i + 1) as f64, "tick {i}: a commutation reset an ungated bystander to {}", *out.ticks);

		// retention behind the latch is pinned, so a woken reader sees the window every element went
		// into — including the ones that arrived while nobody was reading.
		if *out.armed && !window.is_empty() {
			let want: f64 = window[window.len().saturating_sub(reach)..].iter().map(|x| x.v).sum();
			let got = out
				.recall
				.ok_or_else(|| format!("tick {i}: the latch stands, elements have arrived, and the retention reads nothing"))?;
			check!((got - want).abs() < 1e-9, "tick {i}: a woken reader sees {got}, the last {reach} elements add to {want}");
		} else if *out.armed {
			check!(out.recall.is_none(), "tick {i}: the retention read {:?} before any element arrived", out.recall);
		} else {
			check!(out.recall.is_none(), "tick {i}: a darkened reader left a reading behind: {:?}", out.recall);
		}

		if armed && !was_armed {
			episodes += 1;
		}
		if was_armed && !armed {
			commutations += 1;
		}
		was_armed = armed;
	}

	if verbose {
		eprintln!("{} ticks, {episodes} episodes, {commutations} commutations", beats.len());
	}
	// A trace that never commutated says nothing about a deferred commutation.
	check!(commutations > 0 || beats.len() < 3 * EPISODE as usize, "the trace ran {episodes} episodes and never commutated");
	Ok(())
}
