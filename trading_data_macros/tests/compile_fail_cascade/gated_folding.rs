//! A gated node may not hold its own reach: a closed gate pulls no deps, so `Folding` is exactly
//! the state nothing re-warms. The fix the message names is to move the reach into the frame —
//! a `Buffer<Src, K>` field and the dep restated as `Buffering<Src, H>`.
use trading_data_dag::{Bump, Cell, DepOuts, Flat, Folding, Gate, Glance, Horizon, Node, slice_nudge, value_nudge};
use trading_data_macros::graph;

#[derive(Clone, Copy, Debug)]
struct Ev;
impl Flat for Ev {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}
}
impl Bump for Ev {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}
impl Glance for Ev {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.write_str("ev")
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Ev];
}
slice_nudge!(Src, Ev);

#[derive(Clone, Default)]
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
impl Node for Hot {
	type Deps = (Src,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		!s.is_empty()
	}
}
impl Gate for Hot {}

/// Counts the last three batches itself — a window the gate would blank out.
#[derive(Clone, Default)]
struct Windowed {
	seen: Vec<usize>,
}
impl Cell for Windowed {
	type Out<'t> = Option<f64>;
}
impl Node for Windowed {
	type Deps = (Folding<Src, { Horizon::Elems(3) }>,);
	type When = (Hot,);

	fn advance<'t>(&'t mut self, (s,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.seen.push(s.len());
		Some(self.seen.iter().rev().take(3).sum::<usize>() as f64)
	}
}
value_nudge!(Windowed);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[Ev] };
	out GOut;
	hot: Hot,
	windowed: Windowed,
}

fn main() {
	let mut g = G::default();
	let _ = g.tick(Batches { src: &[Ev] });
}
