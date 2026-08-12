//! One graph, wired through the facade, that the schedule/gate/out targets all drive.
//!
//! It is shaped by what the oracles need rather than by what a strategy looks like:
//!
//! - two folds ([`Total`], [`Bucket`]) whose deps are `Folding`, so `rates.folds.exactly-once`
//!   makes their element sequence invariant under every grouping;
//! - one deliberate counter-example ([`Peak`]) that reads the *batch's last element*, which is a
//!   running extremum over evaluated states and is legitimately allowed to differ — the spec says
//!   so twice, and a fixture with nothing outside the invariant makes the scoping unfalsifiable;
//! - two readers of engine-held history behind gates ([`Win`] on a `Buffering`, [`Lvl`] on a
//!   `Sampling`), which is the history a skip re-warms;
//! - a node no output reaches ([`Mid`]) read by two gated consumers, so its demand is a real
//!   disjunction rather than `⊤` — the DNF the macro propagates, and the half of the shape/sweep
//!   correspondence that is not just "its own gate".
//!
//! No gate is *set*: a gate is derived from its deps, and a setter would let a fuzzer reach states
//! the type system says are unreachable. Both gates read a root the fuzzer feeds, exactly as
//! `trading_data_dag/tests/gate.rs` does, so every state reached here is production-reachable.

use trading_data::{
	Blind, Buffering, Bump, Carried, Cell, DepOuts, Elems, Env, Flat, Folding, Folds, Gate, Gating, Glance, Lagged, Over, Reading, Runs, Sampling, Slots, Stamped, Unbounded, Vars, Witness,
	absent, always_present, constant, graph, lt, min, node, select, slice_nudge, value_nudge,
};
use v_utils::TF_1MIN;

/// One unit of `v` is one second of `ts`, so a generated trace's numbers double as its timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Tick {
	pub ts: i64,
	pub v: f64,
}

pub fn t(ts: i64, v: f64) -> Tick {
	Tick { ts, v }
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

/// The element series every fold below reaches over.
pub struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// The two gate-driving roots. Separate, so the 2² assignments are independently reachable — one
/// root with two thresholds would correlate them and half the assignments would never happen.
pub struct CtlA;
impl Cell for CtlA {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(CtlA, Tick);

pub struct CtlB;
impl Cell for CtlB {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(CtlB, Tick);

#[derive(Clone, Default)]
pub struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
#[node]
impl Blind for Hot {
	type Deps = (CtlA,);

	const WHY: &'static str = "a gate fixture";

	fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> bool {
		c.last().is_some_and(|x| x.v > 0.0)
	}
}
impl Gate for Hot {}

#[derive(Clone, Default)]
pub struct Warm;
impl Cell for Warm {
	type Out<'t> = bool;
}
#[node]
impl Blind for Warm {
	type Deps = (CtlB,);

	const WHY: &'static str = "a gate fixture";

	fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> bool {
		c.last().is_some_and(|x| x.v > 0.0)
	}
}
impl Gate for Warm {}

/// A running sum over the whole run: a `Folding` dep hands every element exactly once, in order, so
/// what this emits is a function of the element sequence and of nothing else.
#[derive(Clone, Default)]
pub struct Total {
	sum: f64,
}
impl Cell for Total {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Runs for Total {
	type Deps = (Folding<Src, Unbounded>,);

	const WHY: &'static str = "a fold fixture";

	fn emit(&mut self, (items,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		for x in items {
			self.sum += x.v;
			// a negative element declines: an `Option` item is the absence spelling a rate-preserving
			// node reaches for, and a run of them is what `outs.absence.one-reading` is about.
			out.push((x.v >= 0.0).then_some(self.sum));
		}
	}
}
slice_nudge!(Total, Option<f64>);

/// A period accumulator: the same claim one rate up. It publishes only where a period closed, so
/// most ticks it emits the empty batch — the second absence spelling.
#[derive(Clone, Default)]
pub struct Bucket(Option<(i64, f64)>);
impl Cell for Bucket {
	type Out<'t> = &'t [f64];
}
#[node]
impl Runs for Bucket {
	type Deps = (Folding<Src, Over<{ TF_1MIN }>>,);

	const WHY: &'static str = "a fold fixture";

	fn emit(&mut self, (items,): DepOuts<'_, Self>, out: &mut Vec<f64>) {
		for x in items {
			let period = x.ts / (TF_1MIN.0 * 1_000_000) as i64;
			match &mut self.0 {
				Some((p, v)) if *p == period => *v += x.v,
				slot =>
					if let Some((_, done)) = slot.replace((period, x.v)) {
						out.push(done);
					},
			}
		}
	}
}
slice_nudge!(Bucket, f64);

/// How many elements [`Warmup`] is cold for.
pub const WARM: usize = 3;

/// A fold with a warm-up: absent for exactly its first [`WARM`] elements, a number from there on.
/// Rate-preserving, so that is a claim about *which* elements rather than about how many there are —
/// and the element sequence is invariant under grouping (`rates.folds.exactly-once`), which is what
/// makes the presence boundary a fuzzable property rather than a per-tick one.
#[derive(Clone, Default)]
pub struct Warmup(Carried);
impl Cell for Warmup {
	type Out<'t> = &'t [Reading];
}
#[node]
impl Folds for Warmup {
	type Deps = (Folding<Src, Unbounded>,);

	/// The running sum, and how many elements have reached it.
	const STATE: usize = 2;

	fn read<W: Witness>((src,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (x, lag) = src.at(i)?;
		env.dep(0).lag(lag).put(x);
		Some(0)
	}

	fn step(&self, v: Vars) -> impl Slots {
		(v.get::<1>() + v.get::<0>(), min(v.get::<2>() + constant(1.0), constant(WARM as f64)))
	}

	fn value(&self, v: Vars) -> impl Slots {
		select(lt(v.get::<2>(), constant(WARM as f64)), absent(), v.get::<1>())
	}

	fn carried(&self) -> &Carried {
		&self.0
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.0
	}
}
slice_nudge!(Warmup, Reading);

/// **The counter-example, and it is here on purpose.** A running extremum over the last element of
/// each *tick* is a reading of how the feed batched, not of the series — `rates.deps.tick-opaque`'s
/// closing note and ARCHITECTURE.md both say this is allowed to differ under a coarser clock, and
/// §2 warns against asserting it away. So no oracle reads this, and its presence is what stops the
/// schedule target from being trivially true of every node in the graph.
#[derive(Clone, Default)]
pub struct Peak(Option<f64>);
impl Cell for Peak {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Peak {
	type Deps = (Src,);

	const WHY: &'static str = "a batching-sensitive fixture";

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Option<f64> {
		if let Some(x) = src.last() {
			self.0 = Some(self.0.map_or(x.v, |p: f64| p.max(x.v)));
		}
		self.0
	}
}
value_nudge!(Peak);

/// History the *engine* holds, read behind a gate. Stateless in itself, which is what lets a sleep
/// re-warm: everything it reads is in the frame's retention, and the retention is pinned.
#[derive(Clone, Default)]
pub struct Win;
impl Cell for Win {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Win {
	type Deps = (Gating<Hot>, Buffering<Src, Elems<4>>);

	const WHY: &'static str = "a re-warm fixture";

	fn advance<'t>(&'t mut self, (_, hist): DepOuts<'t, Self>) -> Option<f64> {
		let w = hist.all();
		(!w.is_empty()).then(|| w.iter().map(|x| x.v).sum())
	}
}
value_nudge!(Win);

/// The other engine-held reading: a level rather than a window.
#[derive(Clone, Default)]
pub struct Lvl;
impl Cell for Lvl {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Lvl {
	type Deps = (Gating<Warm>, Sampling<Src>);

	const WHY: &'static str = "a re-warm fixture";

	fn advance<'t>(&'t mut self, (_, level): DepOuts<'t, Self>) -> Option<f64> {
		level.map(|x| x.v)
	}
}
value_nudge!(Lvl);

/// No output reaches this, so its demand is whatever its readers' gates say: `Hot ⋁ Warm`.
#[derive(Clone, Default)]
pub struct Mid;
impl Cell for Mid {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Mid {
	type Deps = (Src,);

	const WHY: &'static str = "a demand fixture";

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Option<f64> {
		src.last().map(|x| x.v)
	}
}
value_nudge!(Mid);

#[derive(Clone, Default)]
pub struct Ra;
impl Cell for Ra {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Ra {
	type Deps = (Gating<Hot>, Mid);

	const WHY: &'static str = "a demand fixture";

	fn advance<'t>(&'t mut self, (_, m): DepOuts<'t, Self>) -> Option<f64> {
		m
	}
}
value_nudge!(Ra);

#[derive(Clone, Default)]
pub struct Rb;
impl Cell for Rb {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Rb {
	type Deps = (Gating<Warm>, Mid);

	const WHY: &'static str = "a demand fixture";

	fn advance<'t>(&'t mut self, (_, m): DepOuts<'t, Self>) -> Option<f64> {
		m
	}
}
value_nudge!(Rb);

graph! {
	pub struct G;
	batches Batches;
	roots { src: Src[Tick], ctl_a: CtlA[u8], ctl_b: CtlB[u16] };
	out GOut;
	outputs { total: Total, bucket: Bucket, warm: Warmup, peak: Peak, win: Win, lvl: Lvl, ra: Ra, rb: Rb }
}

/// Every out of one tick, owned and comparable. `Peak` is here so a verbose replay prints it; which
/// fields an oracle may read is the oracle's own business, and the schedule one reads neither `peak`
/// nor the two gated readings.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Outs {
	pub total: Vec<Option<f64>>,
	pub bucket: Vec<f64>,
	pub warm: Vec<Reading>,
	pub peak: Option<f64>,
	pub win: Option<f64>,
	pub lvl: Option<f64>,
	pub ra: Option<f64>,
	pub rb: Option<f64>,
}

impl From<GOut<'_>> for Outs {
	fn from(o: GOut<'_>) -> Self {
		Self {
			total: o.total.to_vec(),
			bucket: o.bucket.to_vec(),
			warm: o.warm.to_vec(),
			peak: *o.peak,
			win: *o.win,
			lvl: *o.lvl,
			ra: *o.ra,
			rb: *o.rb,
		}
	}
}

/// One tick's inputs, owned so a generated trace can be re-driven under a second grouping.
pub struct Step {
	pub src: Vec<Tick>,
	pub ctl_a: Vec<Tick>,
	pub ctl_b: Vec<Tick>,
}

impl Step {
	pub fn gates(&self) -> (bool, bool) {
		let open = |c: &[Tick]| c.last().is_some_and(|x| x.v > 0.0);
		(open(&self.ctl_a), open(&self.ctl_b))
	}
}

pub fn tick(g: &mut G, s: &Step) -> Outs {
	g.tick(
		0,
		Batches {
			src: &s.src,
			ctl_a: &s.ctl_a,
			ctl_b: &s.ctl_b,
		},
	)
	.into()
}

pub fn tick_obs(g: &mut G, s: &Step, obs: &mut impl trading_data::Observer) -> Outs {
	g.tick_obs(
		0,
		Batches {
			src: &s.src,
			ctl_a: &s.ctl_a,
			ctl_b: &s.ctl_b,
		},
		obs,
	)
	.into()
}
