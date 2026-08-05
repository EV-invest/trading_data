//! End-to-end: the proc-macro `graph!` wires a real latch graph and drives it. Mirrors
//! `trading_data_dag/tests/latch.rs` (external trigger arms, episode runs to its terminal out,
//! deferred commutation drops the latch and resets the gated node next tick), plus a
//! `required_events` check over the generated impl.

use core::any::TypeId;

use trading_data_dag::{Blind, Buffering, Bump, Cell, DepOuts, Episode, Flat, Gate, Gating, Glance, Horizon, Latch, Roots, Stamped, Symbolic, slice_nudge, value_nudge};
use trading_data_expr::{Expr, Vars, constant};
use trading_data_macros::{graph, node};

#[derive(Clone, Copy, Debug)]
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

struct Trig;
impl Cell for Trig {
	type Out<'t> = &'t [Pulse];
}
slice_nudge!(Trig, Pulse);

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

#[derive(Clone, Default)]
struct Live {
	armed: bool,
}
impl Cell for Live {
	type Out<'t> = bool;
}
#[node(latch)]
impl Blind for Live {
	type Deps = (Trig,);

	const WHY: &'static str = "a latch fixture";

	fn advance<'t>(&'t mut self, (pulses,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.armed |= !pulses.is_empty();
		self.armed
	}
}
impl Gate for Live {}
impl Latch for Live {
	type Cut = Deprec;

	fn commutate(&mut self) {
		self.armed = false;
	}

	fn standing(&self) -> bool {
		self.armed
	}
}

/// Episode: two degrading ticks, terminal on the third. Fresh from `Default`.
#[derive(Clone, Default)]
struct Deprec {
	t: u32,
}
impl Cell for Deprec {
	type Out<'t> = Option<Phase>;
}
#[node]
impl Blind for Deprec {
	type Deps = (Gating<Live>, Trig);

	const WHY: &'static str = "a latch fixture";

	fn advance<'t>(&'t mut self, (live, _): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(live, "a gating dep reads true inside `advance`");
		self.t += 1;
		Some(if self.t >= 3 { Phase::Done } else { Phase::Degrading(self.t) })
	}
}

/// Ungated bystander: commutation must not reset it.
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

	fn advance<'t>(&'t mut self, _: DepOuts<'t, Self>) -> Self::Out<'t> {
		self.n += 1;
		self.n as f64
	}
}

graph! {
	struct G;
	batches Batches;
	roots { trig: Trig[Pulse] };
	out GOut;
	outputs { deprec: Deprec, live: Live, ticks: Ticks }
}

const PULSE: &[Pulse] = &[Pulse];
const IDLE: &[Pulse] = &[];

#[test]
fn arm_terminal_commutate_rearm() {
	let mut g = G::default();

	// idle: latch down, episode latent.
	let o = g.tick(0, Batches { trig: IDLE });
	assert!(!o.live);
	assert_eq!(o.deprec, None);

	// external trigger arms; episode starts.
	let o = g.tick(0, Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Degrading(1)));

	let o = g.tick(0, Batches { trig: IDLE });
	assert_eq!(o.deprec, Some(Phase::Degrading(2)));

	// terminal tick — its out is published; the trigger this tick is absorbed and lost.
	let o = g.tick(0, Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Done));

	// commutated: latch down, subtree latent, the during-episode trigger did not re-arm.
	let o = g.tick(0, Batches { trig: IDLE });
	assert!(!o.live);
	assert_eq!(o.deprec, None);

	// re-trigger: fresh episode from Default — reset observable, not a stale t=3.
	let o = g.tick(0, Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Degrading(1)));

	// bystander never reset: counted every tick.
	assert_eq!(o.ticks, 6.0);
}

#[test]
fn required_events_is_the_consumed_root_set() {
	// every node deps on Trig, so its event is required.
	assert_eq!(G::required_events(), vec![TypeId::of::<Pulse>()]);
}

/// A retained element is stamped: that is what a `Horizon` indexes by.
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

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// The shallow reader of the buffered series.
#[derive(Clone, Default)]
struct Last;
impl Cell for Last {
	type Out<'t> = f64;
}
#[node]
impl Blind for Last {
	type Deps = (Buffering<Src, { Horizon::Elems(1) }>,);

	const WHY: &'static str = "a graph fixture";

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		hist.all().last().map_or(f64::NAN, |t| t.v)
	}
}
value_nudge!(Last);

/// The deep one: the frame must retain the join of the two, and this is it.
#[derive(Clone, Default)]
struct Sum3;
impl Cell for Sum3 {
	type Out<'t> = f64;
}
#[node]
impl Blind for Sum3 {
	type Deps = (Buffering<Src, { Horizon::Elems(3) }>,);

	const WHY: &'static str = "a buffer fixture";

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		hist.all().iter().map(|t| t.v).sum()
	}
}
value_nudge!(Sum3);

#[derive(Clone, Default)]
struct Ratio;
impl Cell for Ratio {
	type Out<'t> = f64;
}
#[node]
impl Symbolic for Ratio {
	type Deps = (Last, Sum3);

	fn body(&self, v: Vars) -> impl Expr {
		constant(2.0) * v.get::<0>() + v.get::<1>()
	}
}

graph! {
	struct BG;
	batches BBatches;
	roots { src: Src[Tick] };
	out BOut;
	outputs { ratio: Ratio, last: Last, sum3: Sum3 }
}

#[test]
fn one_buffer_joins_both_reaches() {
	let mut g = BG::default();
	let (three, one) = ([t(1.0), t(2.0), t(3.0)], [t(4.0)]);
	let o = g.tick(0, BBatches { src: &three });
	assert_eq!(o.last, 3.0);
	assert_eq!(o.sum3, 6.0);
	assert_eq!(o.ratio, 12.0);

	// past + fresh: had the frame been sized to the shallow consumer, only 3 would stand behind the 4.
	let o = g.tick(0, BBatches { src: &one });
	assert_eq!(o.sum3, 10.0);
	assert_eq!(o.last, 4.0);

	assert!(BG::NODES.iter().any(|n| n.starts_with("Buffer<")), "the buffered series is a node of the frame: {:?}", BG::NODES);
}

/// The same node library asked for one output, and a root nothing reaches.
graph! {
	struct SmallG;
	batches SBatches;
	roots { src: Src[Tick], trig: Trig[Pulse] };
	out SOut;
	outputs { last: Last }
}

#[test]
fn an_output_nobody_reaches_is_not_built() {
	// the whole graph: `Sum3` and `Ratio` are not upstream of `Last`, so they are not built, and the
	// buffer they shared is sized off the one consumer left asking.
	assert_eq!(SmallG::NODES, ["Buffer<Src>", "Last"]);

	// the second root is declared, is a field of `SBatches`, and is never loaded.
	assert_eq!(SmallG::required_events(), vec![TypeId::of::<Tick>()]);
}
