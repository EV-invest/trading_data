//! `Buffer` and `Latest` are frame cells `graph!` grows, not deps an author states. A `Buffer`'s
//! reach is the join of every read of that series in the whole graph, so no single dep site could
//! state it correctly even in principle — `Buffering`/`Sampling` are the spellings, and they resolve
//! against exactly these fields.
#[expect(unused_imports, reason = "the deps the macro rejects are what name them")]
use trading_data_dag::{Blind, Buffer, Cell, DepOuts, Horizon, Latest};
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

#[derive(Clone, Default)]
struct Level;
impl Cell for Level {
	type Out<'t> = f64;
}
#[node]
impl Blind for Level {
	//~v ERROR: `Latest<C>` is the frame cell `graph!` grows for you
	type Deps = (Latest<Src>,);

	const WHY: &'static str = "a fixture";

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		s.unwrap_or_default()
	}
}

fn main() {}
