//! What the algebra costs on the compute path: the same arithmetic through a `Symbolic` body and
//! written by hand, side by side.
//!
//! This is the bench `r[kernels.pure.zero-cost]` in `docs/spec/kernels.md` is verified by, and the
//! reason the rule can be stated at all. A `Symbolic` node's `advance` flattens its pulled deps into
//! a stack env and evaluates a nested tree of `Copy` markers over it; the hand-written node indexes
//! the pulled tuple directly. In principle SROA plus dead-store elimination erase the difference; in
//! principle is what callgrind is here to check, since making the algebra the *only* way a node
//! computes is only free if that erasure actually happens.
//!
//! Read it as a pair: `pure` and `hand` must report the same `Ir`. A divergence is the signal to
//! give `Expr` a typed env (`Env`/`Slots`, spec step 0b) so the buffer is *gone* rather than
//! optimized away. The soft limit is the ratchet against later drift in either leg.
//!
//! Both legs sweep one node over three roots so the arithmetic is non-trivial (two multiplies, an
//! add, a subtract) and the dep flattening has more than one slot to do.

use std::hint::black_box;

use iai_callgrind::{Callgrind, EventKind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use trading_data_dag::{Blind, Cell, Cons, DepOuts, Nil, Node, Opaque, Pure, Symbolic, step, value_nudge};
use trading_data_expr::{Expr, Vars, constant};

const TICKS: usize = 1_000;

macro_rules! root {
	($($n:ident),+) => { $(
		struct $n;
		impl Cell for $n {
			type Out<'t> = f64;
		}
		value_nudge!($n);
	)+ };
}
root!(Lambda, Vol, Cvd);

#[derive(Clone, Default)]
struct Algebraic;
impl Cell for Algebraic {
	type Out<'t> = f64;
}
value_nudge!(Algebraic);
impl Symbolic for Algebraic {
	type Deps = (Lambda, Vol, Cvd);

	fn body(&self, v: Vars) -> impl Expr {
		let (lambda, vol, cvd) = (v.get::<0>(), v.get::<1>(), v.get::<2>());
		constant(1e6) * lambda + constant(1e-6) * (cvd - vol)
	}
}
// what `#[node]` writes; spelled out here so the two legs differ in the body trait and nothing else.
impl Node for Algebraic {
	type Deps = <Self as Symbolic>::Deps;
	type Kernel = Pure;
}

#[derive(Clone, Default)]
struct Handwritten;
impl Cell for Handwritten {
	type Out<'t> = f64;
}
value_nudge!(Handwritten);
impl Blind for Handwritten {
	type Deps = (Lambda, Vol, Cvd);

	const WHY: &'static str = "the hand-written leg of the zero-cost pair — being outside the algebra is its whole job";

	fn advance<'t>(&'t mut self, (lambda, vol, cvd): DepOuts<'t, Self>) -> Self::Out<'t> {
		1e6 * lambda + 1e-6 * (cvd - vol)
	}
}
impl Node for Handwritten {
	type Deps = <Self as Blind>::Deps;
	type Kernel = Opaque;
}

/// One tick of a one-node graph. Generic over the node so the two legs differ in nothing but which
/// node they instantiate — the frame cons, the `Pull` and the `step` are one body.
fn tick<N>(node: &mut N, (lambda, vol, cvd): (f64, f64, f64)) -> f64
where
	N: Node<Deps = (Lambda, Vol, Cvd)>,
	for<'t> N: Cell<Out<'t> = f64>, {
	let f = Cons::<Lambda, Nil> { out: lambda, tail: Nil };
	let f = Cons::<Vol, _> { out: vol, tail: f };
	let f = Cons::<Cvd, _> { out: cvd, tail: f };
	step(f, node).head()
}

/// The roots' readings, cycling so nothing is constant-folded across ticks.
fn roots(i: usize) -> (f64, f64, f64) {
	let x = (i % 97) as f64;
	(x * 1e-7, x + 1.0, x * 2.0)
}

#[library_benchmark]
fn pure() {
	let mut node = Algebraic;
	for i in 0..TICKS {
		black_box(tick(&mut node, black_box(roots(i))));
	}
}

#[library_benchmark]
fn hand() {
	let mut node = Handwritten;
	for i in 0..TICKS {
		black_box(tick(&mut node, black_box(roots(i))));
	}
}

library_benchmark_group!(name = kernel_cost; benchmarks = pure, hand);
main!(
	config = LibraryBenchmarkConfig::default().tool(Callgrind::default().soft_limits([(EventKind::Ir, 5f64)]));
	library_benchmark_groups = kernel_cost
);
