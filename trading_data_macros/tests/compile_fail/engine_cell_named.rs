//! `Buffer` is a frame cell `graph!` grows, not a dep an author states: its reach is the join of
//! every read of that series in the whole graph, so no single dep site could state it correctly even
//! in principle. `Buffering` is the spelling, and it resolves against exactly that field.
#[expect(unused_imports, reason = "the dep the macro rejects is what names it")]
use trading_data_dag::{Blind, Buffer, Cell, DepOuts, Horizon};
use trading_data_macros::node;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}

#[derive(Clone, Default)]
struct Windowed;
impl Cell for Windowed {
	type Out<'t> = f64;
}
#[node]
impl Blind for Windowed {
	//~v ERROR: `Buffer<C, H>` is the frame cell `graph!` grows for you
	type Deps = (Buffer<Src, { Horizon::Elems(3) }>,);

	const WHY: &'static str = "a fixture";

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.all().len() as f64
	}
}

fn main() {}
