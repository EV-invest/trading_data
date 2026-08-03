//! A node whose every reader sits behind one gate is skipped while that gate is false, so its out
//! has to have an unfired reading. A bare `f64` has none; the fix the message names is `Option<f64>`.
use trading_data_dag::{Bump, Cell, DepOuts, Flat, Gate, Gating, Glance, Node, slice_nudge, value_nudge};
use trading_data_macros::{graph, node};

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
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
#[node]
impl Node for Hot {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		!s.is_empty()
	}
}
impl Gate for Hot {}

/// Read by `Sink` alone, which `Hot` dominates — so the graph will want it to decline.
#[derive(Clone, Default)]
struct Counted;
impl Cell for Counted {
	type Out<'t> = f64;
}
#[node]
impl Node for Counted {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}
value_nudge!(Counted);

#[derive(Clone, Default)]
struct Sink;
impl Cell for Sink {
	type Out<'t> = Option<f64>;
}
#[node]
impl Node for Sink {
	type Deps = (Gating<Hot>, Counted);

	fn advance<'t>(&'t mut self, (_, c): DepOuts<'t, Self>) -> Self::Out<'t> {
		Some(c)
	}
}
value_nudge!(Sink);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[Ev] };
	out GOut;
	outputs { sink: Sink }
}

fn main() {
	let mut g = G::default();
	let b = [Ev];
	let _ = g.tick(0, Batches { src: &b });
}
