//! `Cur` is current, ungated, and its only consumer `Sink` sits behind gate `Hot` — sampling it
//! while `Hot` is closed is pure waste, so the macro's `shadowed` completeness assert must reject
//! the graph.

use trading_data_dag::{Cell, DepOuts, Flat, Gate, Glance, Node, Nudge};
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
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
impl Node for Hot {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		!s.is_empty()
	}
}
impl Gate for Hot {}

#[derive(Clone, Default)]
struct Cur;
impl Cell for Cur {
	type Out<'t> = Option<f64>;
}
impl Node for Cur {
	type Deps = (Src,);

	const HISTORIC: bool = false;

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		Some(s.len() as f64)
	}
}
impl Nudge for Cur {
	type Scratch = Option<f64>;

	fn stage<'t>(out: Option<f64>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) {
		*s = match bump {
			Some(slot) => Flat::nudge(&out, slot, h),
			None => out,
		};
	}

	fn view<'l>(s: &'l Self::Scratch) -> Option<f64> {
		*s
	}
}

#[derive(Clone, Default)]
struct Sink;
impl Cell for Sink {
	type Out<'t> = Option<f64>;
}
impl Node for Sink {
	type Deps = (Cur,);
	type When = (Hot,);

	const HISTORIC: bool = false;

	fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> Self::Out<'t> {
		c
	}
}

graph! {
	struct G;
	batches Batches;
	roots { src: Src[Ev] };
	out GOut;
	hot: Hot,
	cur: Cur,
	sink: Sink,
}

fn main() {}
