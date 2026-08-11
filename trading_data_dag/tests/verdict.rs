//! Absence at the element boundary, from both sides: what a body may be read over, and what a
//! flattening may publish.

use trading_data_dag::{Bump, Cell, Cons, Decides, Fire, Flat, Glance, Nil, Observer, Sweep, Vars, Want, constant, gt, node, observe_root, step_obs, value_nudge};
use trading_data_expr::Expr;

struct Momentum;
impl Cell for Momentum {
	type Out<'t> = Option<f64>;
}
value_nudge!(Momentum);

#[derive(Clone)]
struct Screen;
impl Cell for Screen {
	type Out<'t> = bool;
}
#[node]
impl Decides for Screen {
	type Deps = (Momentum,);

	fn body(&self, v: Vars) -> impl Expr {
		gt(v.get::<0>(), constant(1.0))
	}
}

#[derive(Default)]
struct Readings(Vec<(bool, bool, bool)>);
impl Observer for Readings {
	fn want(&self, _: &'static str) -> Want {
		Want::Jac
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		self.0.push((format!("{}", fire.glance) == "true", fire.formula.is_some(), fire.trace.is_some()));
	}
}

// r[verify outs.absence.typed]
#[test]
fn a_verdict_over_an_absent_dep_is_not_traced() {
	let (mut seen, mut sweep, mut screen) = (Readings::default(), Sweep::default(), Screen);
	for m in [None, Some(2.0), Some(0.5)] {
		sweep.restart();
		step_obs(Cons::<Momentum, Nil> { out: m, tail: Nil }, &mut screen, &mut sweep, &mut seen);
	}
	assert_eq!(seen.0, [(false, true, false), (true, true, true), (false, true, true)]);
}

/// An out that spells absence the way `Reading` exists to replace: a NaN slot on a type whose
/// `ABSENTABLE` says it has none.
#[derive(Clone, Copy)]
struct Sneak;
impl Flat for Sneak {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = f64::NAN;
		true
	}
}
impl Bump for Sneak {
	fn bump(self, _: usize, h: f64) -> (Self, f64) {
		(self, h)
	}
}
impl Glance for Sneak {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.write_str("sneak")
	}
}
struct Convention;
impl Cell for Convention {
	type Out<'t> = Sneak;
}

// r[verify outs.absence.typed]
#[test]
#[should_panic = "no absence channel"]
fn a_flattening_that_spells_absence_by_convention_is_refused() {
	observe_root::<Convention, _>(Sneak, &mut Sweep::default(), &mut Readings::default());
}
