//! Naming a kernel is naming its body trait. `Pure` differentiates a `Symbolic` body, so a node that
//! has none cannot claim it — the impl that would make `Pure` a `Level` for this node does not exist.
use trading_data_dag::{Cell, Node, Pure};

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [u8];
}

#[derive(Clone, Default)]
struct NoAlgebra;
impl Cell for NoAlgebra {
	type Out<'t> = f64;
}

impl Node for NoAlgebra {
	type Deps = (Src,);
	type Kernel = Pure; //~ ERROR: the trait bound `NoAlgebra: Symbolic` is not satisfied
}

fn main() {}
