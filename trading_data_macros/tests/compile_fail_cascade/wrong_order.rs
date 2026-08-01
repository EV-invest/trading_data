//! `b: B` is listed before its dep `a: A`, so `B` is stepped before `A` is in the frame — the
//! engine's `Pull`/`Has` bound rejects the wrong topological order.

use trading_data_dag::{Bump, Cell, DepOuts, Flat, Glance, Node, slice_nudge, value_nudge};
use trading_data_macros::graph;

#[derive(Clone, Copy, Debug)]
struct Ev;
impl Flat for Ev {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}
}

impl Bump for Ev {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}
impl Glance for Ev {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.write_str("ev")
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Ev];
}
slice_nudge!(Src, Ev);

#[derive(Clone, Default)]
struct A;
impl Cell for A {
	type Out<'t> = f64;
}
impl Node for A {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}
value_nudge!(A);

#[derive(Clone, Default)]
struct B;
impl Cell for B {
	type Out<'t> = f64;
}
impl Node for B {
	type Deps = (A,);

	fn advance<'t>(&'t mut self, (a,): DepOuts<'t, Self>) -> Self::Out<'t> {
		a + 1.0
	}
}

graph! {
	struct G;
	batches Batches;
	roots { src: Src[Ev] };
	out GOut;
	outputs { b }
	b: B,
	a: A,
}

fn main() {}
