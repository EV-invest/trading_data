//! An anchored node reading something derived. A rewind reads a node's inputs back out of the past,
//! and a past is read per lane — so the dep has to be a root, or the replay would have to reconstruct
//! the producer first. The macro says so by name rather than through a const assert, which could not
//! name the offending dep.
use trading_data_dag::{Blind, Cell, DepOuts};
use trading_data_macros::{graph, node};

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [u8];
}

#[derive(Clone, Default)]
struct Mid;
impl Cell for Mid {
	type Out<'t> = f64;
}
#[node]
impl Blind for Mid {
	type Deps = (Src,);

	const WHY: &'static str = "an anchoring fixture";

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}

#[derive(Clone, Default)]
struct Held;
impl Cell for Held {
	type Out<'t> = f64;
}
#[node(anchored)]
impl Blind for Held {
	type Deps = (Mid,);

	const WHY: &'static str = "an anchoring fixture";

	fn advance<'t>(&'t mut self, (m,): DepOuts<'t, Self>) -> Self::Out<'t> {
		m
	}
}

graph! { //~ ERROR: `Held` is `#[node(anchored)]` and reads `Mid`, which is no root of this graph
	struct G;
	batches Batches;
	roots { src: Src[u8] };
	out GOut;
	outputs { held: Held }
}

fn main() {}
