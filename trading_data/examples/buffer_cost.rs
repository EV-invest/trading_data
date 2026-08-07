//! What retention costs per tick. `Buffer` trims to its reach before appending, and trimming by
//! `drain(..n)` moves the whole retained window every time — a window that is deep precisely because
//! something needs it deep. Drive a deep one at one element a tick and count.
//!
//! Measure with `perf stat -e instructions:u`, not the clock: this box is shared.

use std::hint::black_box;

use trading_data::{Blind, Buffering, Bump, Cell, DepOuts, Elems, Flat, Glance, Reach, Stamped, graph, node, slice_nudge};

const TICKS: usize = 200_000;
/// Deep enough that the memmove dominates, and of the order an indicator's window actually is.
type Win = Elems<512>;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Tick {
	ts: i64,
	v: f64,
}
impl Stamped for Tick {
	fn ts_ns(&self) -> i64 {
		self.ts
	}
}
impl Flat for Tick {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.v;
		true
	}
}
impl Bump for Tick {
	fn bump(mut self, _: usize, h: f64) -> (Self, f64) {
		self.v += h;
		(self, h)
	}
}
impl Glance for Tick {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.v)
	}
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// Reads the window's ends only, so what this measures is the retention and not a consumer's fold.
#[derive(Clone, Default)]
struct Ends {
	out: Vec<f64>,
}
impl Cell for Ends {
	type Out<'t> = &'t [f64];
}
#[node]
impl Blind for Ends {
	type Deps = (Buffering<Src, Win>,);

	const WHY: &'static str = "the bench's consumer: what it measures is the window's retention, not the arithmetic it reads off the ends";

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let all = hist.all();
		self.out.clear();
		self.out.push(all.first().map_or(0.0, |t| t.v) + all.last().map_or(0.0, |t| t.v));
		&self.out
	}
}
slice_nudge!(Ends, f64);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	outputs { ends: Ends, hist: Buffering<Src, Win> }
}

fn main() {
	let mut g = G::default();
	for i in 0..TICKS {
		let one = [Tick {
			ts: i as i64 * 1_000_000_000,
			v: i as f64,
		}];
		black_box(g.tick(one[0].ts, Batches { src: &one }));
	}
	println!("{TICKS} ticks over a {:?} window", <Win as Reach>::HORIZON);
}
