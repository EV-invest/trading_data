//! One expression, read the four ways the crate advertises.
//!
//! `eval` and `grad` run on the nested `Copy` types — the compute path, where the whole tree should
//! monomorphize to straight-line FMA and the only interesting number is how far `grad`'s
//! `T(s) = s + 2·T(s/2)` re-walk is from it. `lower` and everything past it is the documentation
//! path, which allocates by construction; keeping the two in one bench is what makes the gap
//! between them visible rather than assumed.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use trading_data_expr::{Ast, Expr, Vars, abs, constant, square};

const ENV: [f64; 4] = [1.5, -0.75, 3.25, 0.5];
const NAMES: [&str; 4] = ["price", "drift", "vol", "decay"];

/// A z-score against a shrunk variance, plus a damped drift term: nested enough to be worth
/// simplifying, shallow enough to be the shape a `Symbolic` node body actually has.
fn tree() -> impl Expr {
	let v = Vars;
	let (price, drift, vol, decay) = (v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>());
	(price - drift) / (square(vol) + constant(1e-9)) + abs(drift * decay) - square(price * decay)
}

#[library_benchmark]
fn eval() {
	let t = tree();
	black_box(t.eval(black_box(&ENV)));
}

#[library_benchmark]
fn diff() {
	let t = tree();
	let mut grad = [0.0f64; ENV.len()];
	black_box(t.grad(black_box(&ENV), 1.0, &mut grad));
	black_box(grad);
}

// The `Ast` projection plus the per-var symbolic derivative and its simplification — what a `diff`
// node pays once per observed fire, not per tick. A `///` here is rejected by `library_benchmark`,
// which reads every attribute on the item as one of its own.
#[library_benchmark]
fn docs() {
	let ast = tree().lower();
	for i in 0..ENV.len() {
		black_box(ast.diff(i).simplify().latex(&NAMES));
	}
	black_box(ast.latex(&NAMES));
}

// The value-annotated intermediate-value tree, rendered — the "debug themselves" reading.
#[library_benchmark]
fn debug() {
	let ast: Ast = tree().lower();
	black_box(ast.trace(black_box(&ENV)).to_string());
}

library_benchmark_group!(name = algebra; benchmarks = eval, diff, docs, debug);
main!(library_benchmark_groups = algebra);
