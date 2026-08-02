use trading_data_dag::Cell;
use trading_data_macros::graph;

struct N;
impl Cell for N {
	type Out<'t> = f64;
}

// empty `roots { }` group
graph! {
	struct G;
	batches Batches;
	roots {};
	out GOut; //~ ERROR: graph! needs at least one root
	outputs { n: N }
}

fn main() {}
