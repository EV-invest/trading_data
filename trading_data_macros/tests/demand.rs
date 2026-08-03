//! Demand: `graph!` skips a node whose every consumer sits behind one gate, and does not skip one
//! whose out is still read from somewhere the gate does not dominate.
//!
//! Each node bumps a file-local `AtomicUsize` when it runs, which is how "did it run" is observed
//! without giving the node declared state — declared state is exactly what would pin it.

use core::sync::atomic::{AtomicUsize, Ordering};

use trading_data_dag::{Buffering, Bump, Cell, DepOuts, Emit, EmitOuts, Episode, Flat, Gate, Gating, Glance, Hist, Horizon, Latch, Node, Stamped, slice_nudge};
use trading_data_macros::{graph, node};

/// One unit of `v` is one second of `ts`, so a fixture's numbers double as its timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
struct P {
	ts: i64,
	v: f64,
}
fn p(v: f64) -> P {
	P { ts: (v.abs() * 1e9) as i64, v }
}
impl Stamped for P {
	fn ts_ns(&self) -> i64 {
		self.ts
	}
}
impl Flat for P {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.v;
		true
	}
}
impl Bump for P {
	fn bump(mut self, _: usize, h: f64) -> (Self, f64) {
		self.v += h;
		(self, h)
	}
}
impl Glance for P {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.v)
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [P];
}
slice_nudge!(Src, P);

// ---- graph one: a hard gate, one node it dominates and one it does not ----

#[derive(Clone, Default)]
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
#[node]
impl Node for Hot {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Self::Out<'t> {
		src.iter().any(|x| x.v > 0.0)
	}
}
impl Gate for Hot {}

static COUNTED: AtomicUsize = AtomicUsize::new(0);
static KEPT: AtomicUsize = AtomicUsize::new(0);

/// Read by `Sink` alone, which `Hot` dominates.
#[derive(Clone, Default)]
struct Counted;
impl Cell for Counted {
	type Out<'t> = &'t [P];
}
slice_nudge!(Counted, P);
#[node]
impl Emit for Counted {
	type Deps = (Src,);

	fn emit(&mut self, (src,): EmitOuts<'_, Self>, out: &mut Vec<P>) {
		COUNTED.fetch_add(1, Ordering::Relaxed);
		out.extend_from_slice(src);
	}
}

/// Read by `Sink` *and* retained for `Watch`: the retention is a standing demand no gate covers.
#[derive(Clone, Default)]
struct Kept;
impl Cell for Kept {
	type Out<'t> = &'t [P];
}
slice_nudge!(Kept, P);
#[node]
impl Emit for Kept {
	type Deps = (Src,);

	fn emit(&mut self, (src,): EmitOuts<'_, Self>, out: &mut Vec<P>) {
		KEPT.fetch_add(1, Ordering::Relaxed);
		out.extend_from_slice(src);
	}
}

#[derive(Clone, Default)]
struct Sink;
impl Cell for Sink {
	type Out<'t> = Option<f64>;
}
#[node]
impl Node for Sink {
	type Deps = (Gating<Hot>, Counted, Kept);

	fn advance<'t>(&'t mut self, (hot, c, k): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hot, "a gating dep reads true inside `advance`");
		Some(c.len() as f64 + k.len() as f64)
	}
}

#[derive(Clone, Default)]
struct Watch;
impl Cell for Watch {
	type Out<'t> = Option<f64>;
}
#[node]
impl Node for Watch {
	type Deps = (Buffering<Kept, { Horizon::Elems(2) }>,);

	fn advance<'t>(&'t mut self, (h,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let h: Hist<'t, P> = h;
		h.all().last().map(|x| x.v)
	}
}

graph! {
	struct Gated;
	batches GatedBatches;
	roots { src: Src[P] };
	out GatedOut;
	outputs { sink: Sink, watch: Watch }
}

#[test]
fn a_gate_dominating_every_consumer_skips_the_node_but_not_a_retained_sibling() {
	let mut g = Gated::default();
	let (c0, k0) = (COUNTED.load(Ordering::Relaxed), KEPT.load(Ordering::Relaxed));

	// gate shut: `Counted` is read by nobody, `Kept` still feeds the buffer.
	let b = [p(-1.0)];
	let o = g.tick(0, GatedBatches { src: &b });
	assert_eq!(o.sink, None);
	assert_eq!(o.watch, Some(-1.0));
	assert_eq!((COUNTED.load(Ordering::Relaxed) - c0, KEPT.load(Ordering::Relaxed) - k0), (0, 1));

	// gate open: both run, and `Sink` sees a full run out of the node that was dark last tick.
	let b = [p(2.0)];
	let o = g.tick(0, GatedBatches { src: &b });
	assert_eq!(o.sink, Some(2.0));
	assert_eq!((COUNTED.load(Ordering::Relaxed) - c0, KEPT.load(Ordering::Relaxed) - k0), (1, 2));

	let b = [p(-3.0)];
	let o = g.tick(0, GatedBatches { src: &b });
	assert_eq!(o.sink, None);
	assert_eq!((COUNTED.load(Ordering::Relaxed) - c0, KEPT.load(Ordering::Relaxed) - k0), (1, 3));
}

// ---- graph two: a latch is momentary, so it never suppresses ----

#[derive(Clone, Copy, Debug, PartialEq)]
struct Phase(u32);
impl Episode for Phase {
	fn terminal(&self) -> bool {
		self.0 >= 2
	}
}
impl Flat for Phase {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.0 as f64;
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
		write!(f, "{}", self.0)
	}
}

#[derive(Clone, Default)]
struct Live {
	armed: bool,
}
impl Cell for Live {
	type Out<'t> = bool;
}
#[node(latch)]
impl Node for Live {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.armed |= src.iter().any(|x| x.v > 0.0);
		self.armed
	}
}
impl Gate for Live {}
impl Latch for Live {
	type Cut = Ep;

	fn commutate(&mut self) {
		self.armed = false;
	}
}

static WARM: AtomicUsize = AtomicUsize::new(0);

/// The episode's only data dep — what it reads must be warm *before* the latch arms.
#[derive(Clone, Default)]
struct Warm;
impl Cell for Warm {
	type Out<'t> = &'t [P];
}
slice_nudge!(Warm, P);
#[node]
impl Emit for Warm {
	type Deps = (Src,);

	fn emit(&mut self, (src,): EmitOuts<'_, Self>, out: &mut Vec<P>) {
		WARM.fetch_add(1, Ordering::Relaxed);
		out.extend_from_slice(src);
	}
}

#[derive(Clone, Default)]
struct Ep {
	t: u32,
}
impl Cell for Ep {
	type Out<'t> = Option<Phase>;
}
#[node]
impl Node for Ep {
	type Deps = (Gating<Live>, Warm);

	fn advance<'t>(&'t mut self, (live, _): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(live, "a gating dep reads true inside `advance`");
		self.t += 1;
		Some(Phase(self.t))
	}
}

graph! {
	struct Episodic;
	batches EpisodicBatches;
	roots { src: Src[P] };
	out EpisodicOut;
	outputs { ep: Ep }
}

#[test]
fn a_latch_gate_does_not_suppress_what_stands_behind_it() {
	let mut g = Episodic::default();
	let w0 = WARM.load(Ordering::Relaxed);

	// latch down for two ticks: the episode is latent, but its dep keeps warming.
	for i in 1..=2 {
		let b = [p(-1.0)];
		let o = g.tick(0, EpisodicBatches { src: &b });
		assert_eq!(o.ep, None);
		assert_eq!(WARM.load(Ordering::Relaxed) - w0, i);
	}

	let b = [p(1.0)];
	let o = g.tick(0, EpisodicBatches { src: &b });
	assert_eq!(o.ep, Some(Phase(1)));
	assert_eq!(WARM.load(Ordering::Relaxed) - w0, 3);
}
