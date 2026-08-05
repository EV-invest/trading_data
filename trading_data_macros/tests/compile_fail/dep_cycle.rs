//! Two nodes reading each other. The trampoline would otherwise walk them until the recursion
//! limit; the driver notices the node it is already inside of and names the path.
use trading_data_dag::{Blind, Cell, DepOuts};
use trading_data_macros::{graph, node};

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [u8];
}

#[derive(Clone, Default)]
struct A;
impl Cell for A {
	type Out<'t> = f64;
}
#[node]
impl Blind for A {
	type Deps = (Src, B);

	const WHY: &'static str = "a diamond fixture";

	fn advance<'t>(&'t mut self, (s, b): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.len() as f64 + b
	}
}

#[derive(Clone, Default)]
struct B;
impl Cell for B {
	type Out<'t> = f64;
}
#[node]
impl Blind for B {
	type Deps = (A,);

	const WHY: &'static str = "a diamond fixture";

	fn advance<'t>(&'t mut self, (a,): DepOuts<'t, Self>) -> Self::Out<'t> {
		a
	}
}

graph! { //~ ERROR: dep cycle: A → B → A
	struct G;
	batches Batches;
	roots { src: Src[u8] };
	out GOut;
	outputs { a: A }
}

fn main() {}
