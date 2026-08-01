//! The declared reach must serve every request: `Buffering<Src, Elems(5)>` against a
//! `Buffer<Src, Elems(3)>`. The assert is a monomorphization-time one, so the graph has to be ticked
//! for it to bite — which every real graph does.
use trading_data_dag::{Buffer, Buffering, Bump, Cell, Emit, EmitOuts, Flat, Glance, Horizon, Stamped, slice_nudge};
use trading_data_macros::graph;

#[derive(Clone, Copy, Debug)]
struct Tick(i64);
impl Stamped for Tick {
	fn ts_ns(&self) -> i64 {
		self.0
	}
}
impl Bump for Tick {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}
impl Flat for Tick {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.0 as f64;
		true
	}
}
impl Glance for Tick {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{self:?}")
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

#[derive(Clone, Default)]
struct Deep;
impl Cell for Deep {
	type Out<'t> = &'t [Tick];
}
impl Emit for Deep {
	type Deps = (Buffering<Src, { Horizon::Elems(5) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Tick>) {
		out.extend_from_slice(hist.fresh());
	}
}
slice_nudge!(Deep, Tick);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[Tick] };
	out GOut;
	outputs { deep }
	hist: Buffer<Src, { Horizon::Elems(3) }>,
	emit deep: Deep,
}

fn main() {
	let mut g = G::default();
	let _ = g.tick(Batches { src: &[Tick(0)] });
}
