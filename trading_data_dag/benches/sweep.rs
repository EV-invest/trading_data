//! What one sweep costs, before any strategy exists: a chain of trivial nodes, stepped the four
//! ways the engine offers.
//!
//! The node bodies are one multiply-add each, so what these count is the *plumbing* — the frame
//! cons, the `Pull`, the monomorphized dispatch — divided by nodes × ticks. That is the number an
//! implementor sizing a graph wants, and no app bench can isolate it.
//!
//! The observed legs price the observation seam itself: `Want::Vals` adds one out+dep flatten per
//! node over `Want::Nothing`, and `Want::Jac` adds the pre-advance clone and one re-advance per dep
//! slot on top of that.
//!
//! `nothing` is deliberately *not* the free case: `Probe::want` reads a field, so the level is only
//! known at runtime and the whole seam stays in codegen. What erases is the unit observer, whose
//! `want()` is a constant — `unit` and `bare` are one number, and that equality is what every
//! `--headless` path in this repo is standing on.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use trading_data_dag::{Blind, Cell, Cons, DepOuts, Fire, Nil, Node, Observer, Opaque, Sweep, Want, Wired, step, step_obs, value_nudge};

const TICKS: usize = 1_000;

struct Root;
impl Cell for Root {
	type Out<'t> = Option<f64>;
}
value_nudge!(Root);

/// The chain, its per-node declarations and its two sweeps from one list — a hand-written sweep of
/// this depth would be the bench measuring my typing.
macro_rules! chain {
	($($n:ident $f:ident : $d:ty),+ $(,)?) => {
		$(
			#[derive(Clone, Default)]
			struct $n;
			impl Cell for $n {
				type Out<'t> = Option<f64>;
			}
			value_nudge!($n);
			impl Wired for $n {
				type Deps = ($d,);
			}
			impl Blind for $n {
				const WHY: &'static str = "a bench link: the sweep it measures is the shape, not the arithmetic";

				fn advance<'t>(&'t mut self, (x,): DepOuts<'t, Self>) -> Self::Out<'t> {
					x.map(|v| v * 1.000_000_1 + 1.0)
				}
			}
			// hand-written, not `#[node]`: the dep arrives as a `:ty` fragment, which the shim cannot
			// take apart into the cell it names.
			impl Node for $n {
				type Kernel = Opaque;
			}
		)+

		#[derive(Default)]
		struct Chain {
			$($f: $n,)+
			sweep: Sweep,
		}
		impl Chain {
			fn tick(&mut self, root: Option<f64>) -> Option<f64> {
				let f = Cons::<Root, Nil> { out: root, tail: Nil };
				$(let f = step(f, &mut self.$f);)+
				f.head()
			}

			fn tick_obs<O: Observer>(&mut self, root: Option<f64>, obs: &mut O) -> Option<f64> {
				let sweep = &mut self.sweep;
				let f = Cons::<Root, Nil> { out: root, tail: Nil };
				$(let f = step_obs(f, &mut self.$f, sweep, obs);)+
				f.head()
			}
		}
	};
}

chain!(
	N01 n01: Root, N02 n02: N01, N03 n03: N02, N04 n04: N03, N05 n05: N04, N06 n06: N05, N07 n07: N06, N08 n08: N07,
	N09 n09: N08, N10 n10: N09, N11 n11: N10, N12 n12: N11, N13 n13: N12, N14 n14: N13, N15 n15: N14, N16 n16: N15,
);

/// An observer that keeps nothing, so what it costs is what the sweep spends *reaching* it. The sum
/// is what stops the flatten and the finite difference being eliminated as dead.
struct Probe {
	want: Want,
	sink: f64,
}
impl Observer for Probe {
	fn want(&self, _: &'static str) -> Want {
		self.want
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		self.sink += fire.vals.map_or(0.0, |v| v[0]) + fire.jac.map_or(0.0, |j| j[0]);
	}
}

/// The root's reading, cycling so nothing along the chain is constant-folded.
fn root(i: usize) -> Option<f64> {
	Some((i % 97) as f64)
}

#[library_benchmark]
fn bare() {
	let mut chain = Chain::default();
	for i in 0..TICKS {
		black_box(chain.tick(black_box(root(i))));
	}
}

// `tick_obs` over `()`, which is what a headless run selects. Its whole content is that the number
// matches `bare`. A plain comment, not a doc one: `library_benchmark` rejects every attribute but
// its own, and a doc comment is one.
#[library_benchmark]
fn unit() {
	let mut chain = Chain::default();
	for i in 0..TICKS {
		black_box(chain.tick_obs(black_box(root(i)), &mut ()));
	}
}

#[library_benchmark]
#[bench::nothing(args = (Want::Nothing))]
#[bench::vals(args = (Want::Vals))]
#[bench::jac(args = (Want::Jac))]
fn observed(want: Want) {
	let mut chain = Chain::default();
	let mut probe = Probe { want, sink: 0.0 };
	for i in 0..TICKS {
		black_box(chain.tick_obs(black_box(root(i)), &mut probe));
	}
	black_box(probe.sink);
}

library_benchmark_group!(name = sweep; benchmarks = bare, unit, observed);
main!(library_benchmark_groups = sweep);
