//! Latch semantics through `graph!`: external trigger arms, the gated episode runs to its
//! terminal out (published that tick), the deferred commutation drops the latch and resets the
//! gated node to `Default` at the next tick's start, next trigger starts a fresh episode. A
//! trigger during a live episode — including its terminal tick — is absorbed and lost.
//!
//! The root is batch (`&[Pulse]`); the gate/latch/episode nodes stay scalar-out. `None` becomes
//! an empty root slice, `Some(Pulse)` a one-element slice.

use trading_data_dag::{Bump, Cell, DepOuts, Episode, Flat, Gate, Glance, Horizon, Latch, Node, graph, slice_nudge};

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
impl Node for Live {
	type Deps = (Trig,);

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
}

/// Episode: two degrading ticks, terminal on the third. Fresh from `Default`.
#[derive(Clone, Default)]
struct Deprec {
	t: u32,
}
impl Cell for Deprec {
	type Out<'t> = Option<Phase>;
}
impl Node for Deprec {
	type Deps = (Trig,);
	type When = (Live,);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, _: DepOuts<'t, Self>) -> Self::Out<'t> {
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
impl Node for Ticks {
	type Deps = (Trig,);

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
	latch { live: Live }
	live: Live,
	deprec: Deprec,
	ticks: Ticks,
}

const PULSE: &[Pulse] = &[Pulse];
const IDLE: &[Pulse] = &[];

#[test]
fn arm_terminal_commutate_rearm() {
	let mut g = G::default();

	// idle: latch down, episode latent.
	let o = g.tick(Batches { trig: IDLE });
	assert!(!o.live);
	assert_eq!(o.deprec, None);

	// external trigger arms; episode starts.
	let o = g.tick(Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Degrading(1)));

	let o = g.tick(Batches { trig: IDLE });
	assert_eq!(o.deprec, Some(Phase::Degrading(2)));

	// terminal tick — its out is published; the trigger this tick is absorbed and lost.
	let o = g.tick(Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Done));

	// commutated: latch down, subtree latent, the during-episode trigger did not re-arm.
	let o = g.tick(Batches { trig: IDLE });
	assert!(!o.live);
	assert_eq!(o.deprec, None);

	// re-trigger: fresh episode from Default — reset observable, not a stale t=3.
	let o = g.tick(Batches { trig: PULSE });
	assert!(o.live);
	assert_eq!(o.deprec, Some(Phase::Degrading(1)));

	// bystander never reset: counted every tick.
	assert_eq!(o.ticks, 6.0);
}
