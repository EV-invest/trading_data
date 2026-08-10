//@error-in-other-file: a Symbolic node reads levels
//! A `Symbolic` node is a level throughout, so reading a run at its last element would make its
//! value a function of how the feed grouped its messages (`r[rates.deps.tick-opaque]`). The reach is
//! named through a wrapper — `Sampling<C>` for the standing level, `Buffering<C, R>` for a window —
//! and never by taking the bare run.
use trading_data_dag::{Cell, Cons, Expr, Nil, Symbolic, Vars, constant, slice_nudge, step, value_nudge};
use trading_data_macros::node;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

#[derive(Clone, Default)]
struct Blend;
impl Cell for Blend {
	type Out<'t> = f64;
}
value_nudge!(Blend);
#[node]
impl Symbolic for Blend {
	type Deps = (Src,);

	fn body(&self, v: Vars) -> impl Expr {
		v.get::<0>() * constant(2.0)
	}
}

fn main() {
	let f = Cons::<Src, Nil> { out: &[], tail: Nil };
	step(f, &mut Blend);
}
