//! `L`'s `Cut` is a root, which is never gated — so nothing can ever cut `L` from within. The old
//! `Latch::Cut: Node` bound stood in for this; `cut_gated` reads the derived node set instead, and
//! says it in the graph's own words.

use trading_data_dag::{Bump, Cell, DepOuts, Episode, Flat, Gate, Gating, Glance, Latch, Node, slice_nudge, value_nudge};
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
impl Episode for Ev {
	fn terminal(&self) -> bool {
		true
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Ev];
}
slice_nudge!(Src, Ev);

#[derive(Clone, Default)]
struct L(bool);
impl Cell for L {
	type Out<'t> = bool;
}
#[node(latch)]
impl Node for L {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0 |= !s.is_empty();
		self.0
	}
}
impl Gate for L {}
impl Latch for L {
	type Cut = Src;

	fn commutate(&mut self) {
		self.0 = false;
	}

	fn standing(&self) -> bool {
		self.0
	}
}

#[derive(Clone, Default)]
struct Sink;
impl Cell for Sink {
	type Out<'t> = Option<f64>;
}
#[node]
impl Node for Sink {
	type Deps = (Gating<L>, Src);

	fn advance<'t>(&'t mut self, (_, s): DepOuts<'t, Self>) -> Self::Out<'t> {
		Some(s.len() as f64)
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

fn main() {}
