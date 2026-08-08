use trading_data_dag::Cell;
use trading_data_macros::graph;

struct Root;
impl Cell for Root {
	type Out<'t> = &'t [u8];
}

// nothing is asked for, so nothing is built
graph! {
	struct G;
	batches Batches;
	roots { r: Root[u8] };
	out GOut;
	outputs {} //~ ERROR: graph! needs at least one output
}

fn main() {}
