//! `b: B` is listed before its dep `a: A`, so `B` is stepped before `A` is in the frame — the
//! engine's `Pull`/`Has` bound rejects the wrong topological order.

use trading_data_dag::{Cell, DepOuts, Flat, Glance, Node, Nudge};
use trading_data_macros::graph;

#[derive(Clone, Copy, Debug)]
struct Ev;
impl Flat for Ev {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}

	fn nudge(&self, _: usize, _: f64) -> Self {
		*self
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
impl Nudge for Src {
	type Scratch = Vec<Ev>;

	fn stage<'t>(out: &'t [Ev], s: &mut Vec<Ev>, _: Option<usize>, _: f64) {
		s.clear();
		s.extend_from_slice(out);
	}

	fn view<'l>(s: &'l Vec<Ev>) -> &'l [Ev] {
		s
	}
}

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
impl Nudge for A {
	type Scratch = f64;

	fn stage<'t>(out: f64, s: &mut f64, bump: Option<usize>, h: f64) {
		*s = match bump {
			Some(slot) => Flat::nudge(&out, slot, h),
			None => out,
		};
	}

	fn view<'l>(s: &'l f64) -> f64 {
		*s
	}
}

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
	b: B,
	a: A,
}

fn main() {}
