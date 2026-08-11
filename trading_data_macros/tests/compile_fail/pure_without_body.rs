//! Naming a kernel is naming its body trait. `Pure` differentiates a `Symbolic` body, so a node that
//! has none cannot claim it — the impl that would make `Pure` a `Level` for this node does not exist.
use trading_data_dag::{Cell, Node, Pure, Wired, value_nudge};

// a dep this node could actually have flattened, so the one error left is the one this fixture is
// about: `Deps` is declared on `Wired` and reads without `Symbolic`, so an unflattenable dep here
// would report its own two errors beside it.
struct Src;
impl Cell for Src {
	type Out<'t> = f64;
}
value_nudge!(Src);

#[derive(Clone, Default)]
struct NoAlgebra;
impl Cell for NoAlgebra {
	type Out<'t> = f64;
}

impl Wired for NoAlgebra {
	type Deps = (Src,);
}
impl Node for NoAlgebra {
	type Kernel = Pure; //~ ERROR: the trait bound `NoAlgebra: Symbolic` is not satisfied
}

fn main() {}
