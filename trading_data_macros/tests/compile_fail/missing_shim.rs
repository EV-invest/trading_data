//! `Plain` is a node but never said so where a macro can read it, so the graph has no way to ask
//! what it depends on. The name in the message is the one `#[node]` would have written.
use trading_data_dag::{Blind, Cell, DepOuts, Node, Opaque, Wired};
use trading_data_macros::graph;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [u8];
}

#[derive(Clone, Default)]
struct Plain;
impl Cell for Plain {
	type Out<'t> = f64;
}
impl Wired for Plain {
	type Deps = (Src,);
}
impl Blind for Plain {
	const WHY: &'static str = "a shim-resolution fixture";

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}
// what `#[node]` would have written, minus the shim this test is about.
impl Node for Plain {
	type Kernel = Opaque;
}

graph! {
	struct G;
	batches Batches;
	roots { src: Src[u8] };
	out GOut;
	outputs { plain: Plain } //~ ERROR: cannot find macro `__td_node_Plain` in this scope
}

fn main() {}
