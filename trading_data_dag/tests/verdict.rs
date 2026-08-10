//! A verdict over a dep that has not stood: the kernel answers `false` without reading the body, and
//! the observation plane's algebra readings say the same.

use trading_data_dag::{Cell, Cons, Decides, Fire, Nil, Observer, Sweep, Vars, Want, constant, gt, node, step_obs, value_nudge};
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
