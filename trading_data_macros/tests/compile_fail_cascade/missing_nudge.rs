//! `Counted` never declared its finite-difference witness, which every observed cell owes. The
//! bound lands on the consumer's whole dep tuple, so the message has to name the macro that writes
//! the impl — the cell it names is nowhere near the `graph!` that reports it.
use trading_data_dag::{Blind, Bump, Cell, DepOuts, Flat, Glance, slice_nudge, value_nudge};
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

/// The one cell in the graph with no `value_nudge!`.
#[derive(Clone, Default)]
struct Counted;
impl Cell for Counted {
	type Out<'t> = f64;
}
#[node]
impl Blind for Counted {
	type Deps = (Src,);

	const WHY: &'static str = "a witness fixture";

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}

#[derive(Clone, Default)]
struct Sink;
impl Cell for Sink {
	type Out<'t> = f64;
}
#[node]
impl Blind for Sink {
	type Deps = (Counted,);

	const WHY: &'static str = "a witness fixture";

	fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> Self::Out<'t> {
		c
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
