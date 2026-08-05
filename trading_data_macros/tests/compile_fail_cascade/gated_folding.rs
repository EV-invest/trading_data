//! A gated node may not hold its own reach: a closed gate pulls no deps, so `Folding` is exactly
//! the state nothing re-warms. The fix the message names is to move the reach into the frame —
//! restate the dep as `Buffering<Src, R>` and the graph grows the `Buffer` itself.
use trading_data_dag::{Blind, Bump, Cell, DepOuts, Elems, Flat, Folding, Gate, Gating, Glance, slice_nudge, value_nudge};
use trading_data_macros::{graph, node};

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
#[node]
impl Blind for Hot {
	type Deps = (Src,);

	const WHY: &'static str = "a hot-path fixture";

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
#[node]
impl Blind for Windowed {
	type Deps = (Gating<Hot>, Folding<Src, Elems<3>>);

	const WHY: &'static str = "a sampling fixture";

	fn advance<'t>(&'t mut self, (_, s): DepOuts<'t, Self>) -> Self::Out<'t> {
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
	outputs { windowed: Windowed }
}

fn main() {
	let mut g = G::default();
	let _ = g.tick(0, Batches { src: &[Ev] });
}
