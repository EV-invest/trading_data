//! `Plain` is a node but never said so where a macro can read it, so the graph has no way to ask
//! what it depends on. The name in the message is the one `#[node]` would have written.
use trading_data_dag::{Cell, DepOuts, Node};
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
impl Node for Plain {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}

graph! {
	struct G;
	batches Batches;
	roots { src: Src[u8] };
	out GOut;
	outputs { plain: Plain } //~ ERROR: cannot find macro `__td_node_Plain` in this scope
}

fn main() {}
