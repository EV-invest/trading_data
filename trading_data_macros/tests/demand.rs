//! Demand: `graph!` skips a node whose every consumer sits behind one gate, and does not skip one
//! whose out is still read from somewhere the gate does not dominate.
//!
//! Each node bumps a file-local `AtomicUsize` when it runs, which is how "did it run" is observed
//! without giving the node declared state — declared state is exactly what would pin it.

use core::sync::atomic::{AtomicUsize, Ordering};

use trading_data_dag::{Blind, Buffering, Bump, Cell, DepOuts, Elems, Episode, Flat, Gate, Gating, Glance, Hist, Latch, RunOuts, Runs, Stamped, slice_nudge, value_nudge};
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
impl Blind for Hot {
	type Deps = (Src,);

	const WHY: &'static str = "a hot-path fixture";

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
impl Runs for Counted {
	type Deps = (Src,);

	const WHY: &'static str = "a demand fixture";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<P>) {
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
impl Runs for Kept {
	type Deps = (Src,);

	const WHY: &'static str = "a demand fixture";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<P>) {
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
impl Blind for Sink {
	type Deps = (Gating<Hot>, Counted, Kept);

	const WHY: &'static str = "a demand fixture";

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
impl Blind for Watch {
	type Deps = (Buffering<Kept, Elems<2>>,);

	const WHY: &'static str = "a demand fixture";

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
impl Blind for Live {
	type Deps = (Src,);

	const WHY: &'static str = "a latch fixture";

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

	fn standing(&self) -> bool {
		self.armed
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
impl Runs for Warm {
	type Deps = (Src,);

	const WHY: &'static str = "a demand fixture";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<P>) {
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
impl Blind for Ep {
	type Deps = (Gating<Live>, Warm);

	const WHY: &'static str = "an episode fixture";

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

// ---- graph three: two consumers behind *different* gates ----

#[derive(Clone, Default)]
struct Jumpy;
impl Cell for Jumpy {
	type Out<'t> = bool;
}
#[node]
impl Blind for Jumpy {
	type Deps = (Src,);

	const WHY: &'static str = "an any-over-batch is a quantifier, not a scalar function";

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Self::Out<'t> {
		src.iter().any(|x| x.v.abs() > 10.0)
	}
}
impl Gate for Jumpy {}
value_nudge!(Jumpy);

/// Reads `Jumpy` as data, not permission — which is also what puts *both* gates ahead of what they
/// demand, since a gate the sweep has not resolved yet cannot suppress anything.
#[derive(Clone, Default)]
struct Rising;
impl Cell for Rising {
	type Out<'t> = bool;
}
#[node]
impl Blind for Rising {
	type Deps = (Src, Jumpy);

	const WHY: &'static str = "an any-over-batch is a quantifier, not a scalar function";

	fn advance<'t>(&'t mut self, (src, jumpy): DepOuts<'t, Self>) -> Self::Out<'t> {
		!jumpy && src.iter().any(|x| x.v > 0.0)
	}
}
impl Gate for Rising {}

static SHARED: AtomicUsize = AtomicUsize::new(0);

/// Read from behind `Rising` and from behind `Jumpy`. Their intersection is empty, so a demand that is a
/// set can only call this unconditional; the disjunction is the true condition.
#[derive(Clone, Default)]
struct Shared;
impl Cell for Shared {
	type Out<'t> = &'t [P];
}
slice_nudge!(Shared, P);
#[node]
impl Runs for Shared {
	type Deps = (Src,);

	const WHY: &'static str = "a pass-through of its dep's run";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<P>) {
		SHARED.fetch_add(1, Ordering::Relaxed);
		out.extend_from_slice(src);
	}
}

#[derive(Clone, Default)]
struct Left;
impl Cell for Left {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Left {
	type Deps = (Gating<Rising>, Shared);

	const WHY: &'static str = "a batch length is a count, not a function of the elements counted";

	fn advance<'t>(&'t mut self, (rising, s): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(rising, "a gating dep reads true inside `advance`");
		Some(s.len() as f64)
	}
}

#[derive(Clone, Default)]
struct Right;
impl Cell for Right {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Right {
	type Deps = (Gating<Jumpy>, Shared);

	const WHY: &'static str = "a batch length is a count, not a function of the elements counted";

	fn advance<'t>(&'t mut self, (jumpy, s): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(jumpy, "a gating dep reads true inside `advance`");
		Some(s.len() as f64)
	}
}

graph! {
	struct Forked;
	batches ForkedBatches;
	roots { src: Src[P] };
	out ForkedOut;
	outputs { left: Left, right: Right }
}

#[test]
fn two_consumers_behind_different_gates_demand_their_disjunction() {
	let mut g = Forked::default();
	let s0 = SHARED.load(Ordering::Relaxed);
	let mut tick = |v: f64| {
		let b = [p(v)];
		let o = g.tick(0, ForkedBatches { src: &b });
		(o.left, o.right, SHARED.load(Ordering::Relaxed) - s0)
	};

	// neither gate open: nobody reads it, so it does not run.
	assert_eq!(tick(-1.0), (None, None, 0));
	// `Rising` alone: it runs, and the reader that asked sees a full batch.
	assert_eq!(tick(2.0), (Some(1.0), None, 1));
	// `Jumpy` alone — the case a set-valued demand could not express, since the intersection of the two
	// gates is empty and would have read as "always demanded".
	assert_eq!(tick(-20.0), (None, Some(1.0), 2));
	assert_eq!(tick(20.0), (None, Some(1.0), 3));
	assert_eq!(tick(-1.0), (None, None, 3));
}

// ---- graph four: a latch suppresses what stands behind it, once that says it re-warms ----

#[derive(Clone, Default)]
struct Held {
	armed: bool,
}
impl Cell for Held {
	type Out<'t> = bool;
}
#[node(latch)]
impl Blind for Held {
	type Deps = (Src,);

	const WHY: &'static str = "a latch is state over the run, not a function of this tick";

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.armed |= src.iter().any(|x| x.v > 0.0);
		self.armed
	}
}
impl Gate for Held {}
impl Latch for Held {
	type Cut = Managed;

	fn commutate(&mut self) {
		self.armed = false;
	}

	fn standing(&self) -> bool {
		self.armed
	}
}

static REBUILT: AtomicUsize = AtomicUsize::new(0);

/// Same shape as `Warm`, but it says a skipped tick is one it can recover — so the latch may darken
/// it, a tick behind the arming.
#[derive(Clone, Default)]
struct Rebuilt;
impl Cell for Rebuilt {
	type Out<'t> = &'t [P];

	const REWARMS: bool = true;
}
slice_nudge!(Rebuilt, P);
#[node]
impl Runs for Rebuilt {
	type Deps = (Src,);

	const WHY: &'static str = "a pass-through of its dep's run";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<P>) {
		REBUILT.fetch_add(1, Ordering::Relaxed);
		out.extend_from_slice(src);
	}
}

#[derive(Clone, Default)]
struct Managed {
	t: u32,
}
impl Cell for Managed {
	type Out<'t> = Option<Phase>;
}
#[node]
impl Blind for Managed {
	type Deps = (Gating<Held>, Rebuilt);

	const WHY: &'static str = "a tick counter is state, not a function of its deps";

	fn advance<'t>(&'t mut self, (held, _): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(held, "a gating dep reads true inside `advance`");
		self.t += 1;
		Some(Phase(self.t))
	}
}

graph! {
	struct Latched;
	batches LatchedBatches;
	roots { src: Src[P] };
	out LatchedOut;
	outputs { ep: Managed }
}

#[test]
fn a_latch_suppresses_what_declares_it_re_warms() {
	let mut g = Latched::default();
	let r0 = REBUILT.load(Ordering::Relaxed);
	let mut tick = |v: f64| {
		let b = [p(v)];
		let o = g.tick(0, LatchedBatches { src: &b });
		(o.ep, REBUILT.load(Ordering::Relaxed) - r0)
	};

	assert_eq!(tick(-1.0), (None, 0));
	assert_eq!(tick(-1.0), (None, 0));
	// the arming tick: `standing` is read before the sweep, so the contact is still open here and the
	// episode's first tick reads an empty run. One tick of lag, and the graph says so.
	assert_eq!(tick(1.0), (Some(Phase(1)), 0));
	// terminal, so the contact drops — but only on the next tick's `apply_pending`.
	assert_eq!(tick(-1.0), (Some(Phase(2)), 1));
	assert_eq!(tick(-1.0), (None, 1));
}

// ---- graph three: a retention read only by anchored nodes goes dark with them ----

/// The relaxation Phase 6 of the anchoring work turned on, and the only one there is to the rule
/// that frame retention must be hole-free. What justifies it is not that the hole is harmless — it
/// is a hole, and this test is written to show it — but that a rewind fetches those rows back out of
/// the recorded past on the tick something asks. That is the driver's promise, not the graph's, so
/// the whole relaxation is conditioned on `Rewound::REWINDS`.
#[derive(Clone, Default)]
struct Summed;
impl Cell for Summed {
	type Out<'t> = Option<f64>;
}
#[node(anchored)]
impl Blind for Summed {
	type Deps = (Gating<Hot>, Buffering<Src, Elems<2>>);

	const WHY: &'static str = "a demand fixture";

	fn advance<'t>(&'t mut self, (hot, h): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hot, "a gating dep reads true inside `advance`");
		let h: Hist<'t, P> = h;
		Some(h.all().iter().map(|x| x.v).sum())
	}
}

graph! {
	struct Anchored;
	batches AnchoredBatches;
	roots { src: Src[P] };
	out AnchoredOut;
	outputs { summed: Summed }
}

/// The fixture holds no state to restore — what is under test is which nodes the sweep steps, not
/// what a replay puts back.
struct Tape;
impl trading_data_dag::Rewound<Summed> for Tape {
	fn rewind(&mut self, _: &mut Summed) {}
}

#[test]
fn a_retention_read_only_by_anchored_nodes_sleeps_with_them() {
	let mut g = Anchored::default();
	let tick = |g: &mut Anchored, v: f64, past: bool| {
		let b = [p(v)];
		match past {
			true => g.tick_rewind(0, AnchoredBatches { src: &b }, &mut Tape).summed,
			false => g.tick(0, AnchoredBatches { src: &b }).summed,
		}
	};

	// gate open, then shut, then open. With a past to seek, the shut tick's row never entered the
	// window at all: what the third tick sums is 1 and 3, and -2 is on disk for whoever asks.
	assert_eq!(tick(&mut g, 1.0, true), Some(1.0));
	assert_eq!(tick(&mut g, -2.0, true), None);
	assert_eq!(tick(&mut g, 3.0, true), Some(4.0), "the retention skipped the tick its only reader skipped");

	// and `Awake` — a live feed's past, which is no past — pins the retention through the same shut
	// tick, because nothing would ever fetch that row back: 1, -2 and 3 are all still in the window.
	let mut g = Anchored::default();
	assert_eq!(tick(&mut g, 1.0, false), Some(1.0));
	assert_eq!(tick(&mut g, -2.0, false), None);
	assert_eq!(tick(&mut g, 3.0, false), Some(2.0), "pinned awake, the retention folded the tick its reader skipped");
}

/// The relaxation above, as the graph states it rather than as a feed exercises it: with the gate
/// shut, both the anchored reader and the retention it is the only reader of go dark — and both are
/// marked as sleeping only behind a past that rewinds, which is the whole of what the rule is
/// conditioned on.
#[test]
fn anchored_shape() {
	insta::assert_snapshot!(Anchored::SHAPE, @r#"
	graph Anchored

	╷ Src                        root src
	├─╮
	│ ● Hot                        gate  pin·gate  Opaque("a hot-path fixture")
	╰ │
	● │ Buffer<Src, Elems(2)>      buffer  ⟳Src@Elems(2)  Opaque("a retention window is the engine's bookkeeping over a run, not a function of its elements")
	╰─╌
	● Summed                     node  pin·output  ⊣Hot ⌸@Elems(2)  →out summed  Opaque("a demand fixture")

	legend  ╷root ●live ░dark ⟲needs-a-rewinding-past  ⊣gating ⟳folding ⌸buffering ·sampling
	"#);
	insta::assert_snapshot!(Anchored::SHAPE.under(&[("Hot", false)]), @r#"
	graph Anchored  ·  Hot=closed

	╷ Src                         root src
	├─╮
	│ ● Hot                         gate  pin·gate  Opaque("a hot-path fixture")
	╰ │
	░⟲ │ Buffer<Src, Elems(2)>        buffer  ⟳Src@Elems(2)  Opaque("a retention window is the engine's bookkeeping over a run, not a function of its elements")
	╰─╌
	░⟲ Summed                       node  pin·output  ⊣Hot ⌸@Elems(2)  →out summed  Opaque("a demand fixture")

	legend  ╷root ●live ░dark ⟲needs-a-rewinding-past  ⊣gating ⟳folding ⌸buffering ·sampling
	2 of 4 dark
	"#);
}
