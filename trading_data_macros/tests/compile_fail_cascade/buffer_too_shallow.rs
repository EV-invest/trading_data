//! The declared depth must dominate every request: `Buffering<Src, 5>` against a `Buffer<Src, 3>`.
//! The assert is a monomorphization-time one, so the graph has to be ticked for it to bite — which
//! every real graph does.
use trading_data_dag::{Buffer, Buffering, Cell, DepOuts, Node, slice_nudge};
use trading_data_macros::graph;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

#[derive(Clone, Default)]
struct Deep {
	buf: Vec<f64>,
}
impl Cell for Deep {
	type Out<'t> = &'t [f64];
}
impl Node for Deep {
	type Deps = (Buffering<Src, 5>,);

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		self.buf.extend_from_slice(hist.fresh());
		&self.buf
	}
}
slice_nudge!(Deep, f64);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	hist: Buffer<Src, 3>,
	deep: Deep,
}

fn main() {
	let mut g = G::default();
	let _ = g.tick(Batches { src: &[1.0] });
}
