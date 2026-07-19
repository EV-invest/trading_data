use trading_data_dag::Cell;
use trading_data_macros::graph;

struct Root;
impl Cell for Root {
	type Out<'t> = &'t [u8];
}

struct N;
impl Cell for N {
	type Out<'t> = f64;
}

// missing the `batches Ident;` section
graph! {
	struct G;
	roots { r: Root[u8] }; //~ ERROR: expected `batches`
	out GOut;
	n: N,
}

fn main() {}
