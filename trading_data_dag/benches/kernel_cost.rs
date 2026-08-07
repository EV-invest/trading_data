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
//!
//! `scan`/`raw` is the same pair on the run side: one out element per element of a driving series,
//! the arithmetic identical, through a `Scans` body and through a hand-written `emit`. It asks the
//! harder question — a per-element kernel refills a stack env once per element, where the level pair
//! does it once per tick — and the answer has to be the same equality.

use std::hint::black_box;

use iai_callgrind::{Callgrind, EventKind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main};
use trading_data_dag::{
	Blind, Cell, Cons, DepOuts, Emit, Emitter, Env, Lagged, Nil, Node, Opaque, Pure, Raw, RunOuts, Runs, Scan, ScanOuts, Scans, Slots, Symbolic, Witness, step, step_emit, value_nudge,
};
use trading_data_expr::{Expr, Vars, constant};

const TICKS: usize = 1_000;
/// Elements per tick on the run legs — enough that the per-element work dominates the per-call
/// setup, which is the thing being asked about.
const RUN: usize = 8;

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

/// The driving series both run legs walk: three slots per element, the same three the level pair
/// reads off three roots.
struct Src;
impl Cell for Src {
	type Out<'t> = &'t [[f64; 3]];
}
trading_data_dag::slice_nudge!(Src, [f64; 3]);

#[derive(Clone, Default)]
struct Scanned;
impl Cell for Scanned {
	type Out<'t> = &'t [f64];
}
trading_data_dag::slice_nudge!(Scanned, f64);
impl Scans for Scanned {
	type Deps = (Src,);

	fn read<W: Witness>((src,): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (e, lag) = src.at(i)?;
		env.dep(0).lag(lag).put(e);
		Some(0)
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (lambda, vol, cvd) = (v.get::<0>(), v.get::<1>(), v.get::<2>());
		constant(1e6) * lambda + constant(1e-6) * (cvd - vol)
	}
}
impl Emit for Scanned {
	type Deps = <Self as Scans>::Deps;
	type Kernel = Scan;
}

#[derive(Clone, Default)]
struct Handrun;
impl Cell for Handrun {
	type Out<'t> = &'t [f64];
}
trading_data_dag::slice_nudge!(Handrun, f64);
impl Runs for Handrun {
	type Deps = (Src,);

	const WHY: &'static str = "the hand-written leg of the zero-cost pair — being outside the algebra is its whole job";

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<f64>) {
		for e in src {
			out.push(1e6 * e[0] + 1e-6 * (e[2] - e[1]));
		}
	}
}
impl Emit for Handrun {
	type Deps = <Self as Runs>::Deps;
	type Kernel = Raw;
}

/// One tick of a one-node run graph, generic for the reason [`tick`] is.
fn tick_run<E>(e: &mut Emitter<E>, src: &[[f64; 3]]) -> usize
where
	E: Emit<Deps = (Src,)>,
	for<'t> E: Cell<Out<'t> = &'t [<E as trading_data_dag::Series>::Item]>, {
	let f = Cons::<Src, Nil> { out: src, tail: Nil };
	step_emit(f, e, true, 0).head().len()
}

fn run_roots(i: usize) -> [[f64; 3]; RUN] {
	core::array::from_fn(|k| {
		let x = ((i + k) % 97) as f64;
		[x * 1e-7, x + 1.0, x * 2.0]
	})
}

#[library_benchmark]
fn scan() {
	let mut node = Emitter::<Scanned>::default();
	for i in 0..TICKS {
		black_box(tick_run(&mut node, &black_box(run_roots(i))));
	}
}

#[library_benchmark]
fn raw() {
	let mut node = Emitter::<Handrun>::default();
	for i in 0..TICKS {
		black_box(tick_run(&mut node, &black_box(run_roots(i))));
	}
}

library_benchmark_group!(name = kernel_cost; benchmarks = pure, hand, scan, raw);
main!(
	config = LibraryBenchmarkConfig::default().tool(Callgrind::default().soft_limits([(EventKind::Ir, 5f64)]));
	library_benchmark_groups = kernel_cost
);
