//! `impl Node` is the macro's output, not an author's input. Writing one by hand would let a node
//! name a kernel it has no body for, which is the whole thing the seal is for.
#[expect(unused_imports, reason = "the impl the macro rejects is what names them")]
use trading_data_dag::{Cell, DepOuts, Node};
use trading_data_macros::node;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [u8];
}

#[derive(Clone, Default)]
struct Plain;
impl Cell for Plain {
	type Out<'t> = f64;
}

#[node]
impl Node for Plain {
	//~ ERROR: `impl Node` is written by `#[node]`, not by hand
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64
	}
}

fn main() {}
