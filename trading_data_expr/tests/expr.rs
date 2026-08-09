//! One kernel, `(x0 + x1)² + |x0 − 1|`, read four ways: value, exact gradient (vs central FD),
//! symbolic derivative (vs the gradient), and the LaTeX/trace renderings.

use trading_data_expr::{Expr, Vars, abs, constant, square};

/// The shared kernel; returns the typed `impl Expr` so every test reads the same source of truth.
fn kernel() -> impl Expr {
	let v = Vars;
	let (x0, x1) = (v.get::<0>(), v.get::<1>());
	square(x0 + x1) + abs(x0 - constant(1.0))
}

fn central_fd(e: &impl Expr, env: &[f64], i: usize) -> f64 {
	let h = 1e-6;
	let (mut ep, mut em) = (env.to_vec(), env.to_vec());
	ep[i] += h;
	em[i] -= h;
	(e.eval(&ep) - e.eval(&em)) / (2.0 * h)
}

#[test]
fn eval_matches_hand_computation() {
	let e = kernel();
	// (3 + 5)² + |3 − 1| = 64 + 2
	assert_eq!(e.eval(&[3.0, 5.0]), 66.0);
	assert_eq!(e.eval(&[0.0, 0.0]), 1.0);
}

#[test]
fn grad_matches_central_difference() {
	let e = kernel();
	let env = [3.0, 5.0];
	let mut g = [0.0; 2];
	let val = e.grad(&env, 1.0, &mut g);
	assert_eq!(val, 66.0);
	// ∂/∂x0 = 2(x0+x1) + sign(x0−1) = 17, ∂/∂x1 = 2(x0+x1) = 16
	assert!((g[0] - 17.0).abs() < 1e-9, "{g:?}");
	assert!((g[1] - 16.0).abs() < 1e-9, "{g:?}");
	for i in 0..2 {
		assert!((g[i] - central_fd(&e, &env, i)).abs() < 1e-4, "grad {i} vs FD: {g:?}");
	}
}

#[test]
fn symbolic_derivative_matches_grad_on_random_envs() {
	let e = kernel();
	let ast = e.lower();
	// deterministic LCG; x0 kept clear of the |·| kink at 1.
	let mut s: u64 = 0x9e37_79b9_7f4a_7c15;
	let mut rng = || {
		s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		(s >> 33) as f64 / (1u64 << 31) as f64
	};
	for _ in 0..64 {
		let env = [2.0 + rng() * 5.0, rng() * 6.0 - 3.0];
		let mut g = [0.0; 2];
		e.grad(&env, 1.0, &mut g);
		for i in 0..2 {
			let d = ast.diff(i).simplify();
			assert!((d.eval(&env) - g[i]).abs() < 1e-9, "∂{i} {d} at {env:?}: {} vs {}", d.eval(&env), g[i]);
		}
	}
}

#[test]
fn latex_and_infix_render() {
	let ast = kernel().lower();
	assert_eq!(ast.to_string(), "((x0 + x1)^2 + |(x0 - 1)|)");
	assert_eq!(ast.latex(&["x", "y"]), "\\left(\\left(x + y\\right)^{2} + \\left|\\left(x - 1\\right)\\right|\\right)");
}

#[test]
fn trace_renders_intermediate_values() {
	let trace = kernel().lower().trace(&[3.0, 5.0]);
	let expected = "\
◉ +: 66
├──◉ sq: 64
│  └──◉ +: 8
│     ├──◉ var: 3
│     └──◉ var: 5
└──◉ abs: 2
   └──◉ -: 2
      ├──◉ var: 3
      └──◉ const: 1";
	assert_eq!(trace.to_string(), expected);
}

#[test]
fn simplify_clears_derivative_litter() {
	// d/dx1 of the kernel is 2(x0+x1)·1 with the |·| term vanishing — no 0-terms, no ×1 left.
	let d = kernel().lower().diff(1).simplify();
	assert_eq!(d.to_string(), "(2 * (x0 + x1))");
}

/// Every operator, differentiated two ways that share no code: `grad`'s chain rule, and a central
/// difference of `eval`. This is where the algebra is checked — a fire carries one array for the
/// one-step reading whichever way the kernel reached it (`r[kernels.jac.two-quantities]`), so the
/// engine has nothing to compare a `Pure` node's Jacobian against at runtime, deliberately, and the
/// comparison runs once here instead of every tick.
///
/// `Cmp`/`Select` carry a step, so they are sampled away from it: a difference across a jump is not
/// a slope, and neither reading claims one.
#[test]
fn every_operator_agrees_with_a_numeric_difference() {
	use trading_data_expr::{exp, gt, lt, max, min, powi_of, select, sqrt, sum};

	/// `(kernel, env)` pairs; the kernel is boxed through `lower` so one loop covers all of them.
	macro_rules! check {
		($($label:literal: |$v:ident| $body:expr, at $env:expr;)+) => {$({
			let $v = Vars;
			let e = $body;
			let env: [f64; 2] = $env;
			let mut g = [0.0; 2];
			let val = e.grad(&env, 1.0, &mut g);
			assert!((val - e.eval(&env)).abs() < 1e-12, "{}: grad's value disagrees with eval", $label);
			for i in 0..2 {
				let fd = central_fd(&e, &env, i);
				assert!((g[i] - fd).abs() < 1e-4 * fd.abs().max(1.0), "{} ∂{i}: grad {} vs central difference {fd}", $label, g[i]);
			}
			// and the symbolic derivative is a third reading of the same number
			let ast = e.lower();
			for i in 0..2 {
				let d = ast.diff(i).simplify();
				assert!((d.eval(&env) - g[i]).abs() < 1e-9, "{} ∂{i}: `diff` {} vs `grad` {}", $label, d.eval(&env), g[i]);
			}
		})+};
	}

	check! {
		"add":    |v| v.get::<0>() + v.get::<1>(),                        at [1.5, 2.5];
		"sub":    |v| v.get::<0>() - v.get::<1>(),                        at [1.5, 2.5];
		"mul":    |v| v.get::<0>() * v.get::<1>(),                        at [1.5, 2.5];
		"div":    |v| v.get::<0>() / v.get::<1>(),                        at [1.5, 2.5];
		"neg":    |v| -(v.get::<0>() * v.get::<1>()),                     at [1.5, 2.5];
		"square": |v| square(v.get::<0>() + v.get::<1>()),                at [1.5, 2.5];
		"abs":    |v| abs(v.get::<0>() - v.get::<1>()),                   at [1.5, 2.5];
		"sum":    |v| sum([v.get::<0>(), v.get::<0>()]) + v.get::<1>(),   at [1.5, 2.5];
		"sqrt":   |v| sqrt(v.get::<0>() * v.get::<1>()),                  at [1.5, 2.5];
		"exp":    |v| exp(v.get::<0>() - v.get::<1>()),                   at [1.5, 2.5];
		"powi":   |v| powi_of::<_, 3>(v.get::<0>() + v.get::<1>()),       at [1.5, 2.5];
		"powi-":  |v| powi_of::<_, -2>(v.get::<0>() + v.get::<1>()),      at [1.5, 2.5];
		"min":    |v| min(v.get::<0>(), v.get::<1>()) * constant(2.0),    at [1.5, 2.5];
		"max":    |v| max(v.get::<0>(), v.get::<1>()) * constant(2.0),    at [1.5, 2.5];
		"cmp":    |v| lt(v.get::<0>(), v.get::<1>()),                     at [1.5, 2.5];
		"cmp-gt": |v| gt(v.get::<0>(), v.get::<1>()),                     at [1.5, 2.5];
		"select": |v| select(lt(v.get::<0>(), v.get::<1>()), square(v.get::<0>()), v.get::<1>()), at [1.5, 2.5];
	}
}

/// `min`/`max` are branches, and both readings must take the *same* branch — otherwise the exact
/// Jacobian and the documented derivative would disagree exactly where a screener sits.
#[test]
fn min_and_max_break_ties_the_same_way_in_both_readings() {
	use trading_data_expr::{max, min};

	for build in [0, 1] {
		let v = Vars;
		let (mut g, env) = ([0.0; 2], [2.0, 2.0]);
		let ast = match build {
			0 => {
				let e = min(v.get::<0>(), v.get::<1>());
				e.grad(&env, 1.0, &mut g);
				e.lower()
			}
			_ => {
				let e = max(v.get::<0>(), v.get::<1>());
				e.grad(&env, 1.0, &mut g);
				e.lower()
			}
		};
		// the tie goes right, so the whole seed lands on x1 and none on x0
		assert_eq!(g, [0.0, 1.0], "build {build}");
		for i in 0..2 {
			assert_eq!(ast.diff(i).simplify().eval(&env), g[i], "build {build} ∂{i}");
		}
	}
}

/// **Phase 5 — the algebra against a finite difference, over random trees.** `kernels.jac` makes
/// exactness normative, and the two readings that must agree are computed by completely different
/// code: [`Expr::grad`] accumulates partials by the chain rule down the typed tree, [`Ast::diff`]
/// rewrites the lowered tree symbolically. `eval` differenced numerically is the third opinion, and
/// the one neither of them can have talked itself into.
///
/// The typed tree is nested marker structs, so its *shape* is fixed at compile time and no runtime
/// generator can vary it. `Ast` is a plain enum, so this fuzzes shapes there and keeps the typed
/// path pinned by the fixed kernel above — which is the split the type-level algebra forces.
mod fuzz {
	use trading_data_expr::Ast;

	/// Deterministic, ten lines, no dependency. The crate is `#![no_std]` and zero-dep on purpose;
	/// a fuzz loop is not a reason to put `rand` in its dev-tree.
	struct Lcg(u64);
	impl Lcg {
		fn next(&mut self) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			self.0 >> 33
		}

		fn below(&mut self, n: u64) -> u64 {
			self.next() % n
		}

		fn span(&mut self, lo: f64, hi: f64) -> f64 {
			lo + (hi - lo) * (self.next() % 4096) as f64 / 4095.0
		}
	}

	const VARS: usize = 3;

	/// A random tree, bounded in depth. Every node kind is in the draw, kinks included: a kink is not
	/// avoided, it is *detected* below, because avoiding it would be avoiding `Min`/`Max`/`Select`
	/// altogether and those are exactly where a tie-break has to resolve the same way in the value and
	/// in the derivative.
	///
	/// What *is* avoided is leaving the algebra's domain: `Sqrt` takes an `Abs`, `Div` a denominator
	/// that cannot be zero, `Powi` a non-negative exponent, and `Exp` an argument bounded above. NaN
	/// is a declination and `Min`/`Max` refuse one in debug, so a generator that produced NaN would be
	/// testing the assert rather than the algebra. This is deterministic at a fixed seed, so passing
	/// once is passing.
	fn tree(r: &mut Lcg, depth: u32) -> Ast {
		let leaf = |r: &mut Lcg| match r.below(3) {
			0 => Ast::Const(r.span(-3.0, 3.0)),
			_ => Ast::Var(r.below(VARS as u64) as usize),
		};
		if depth == 0 {
			return leaf(r);
		}
		fn sub(r: &mut Lcg, depth: u32) -> Box<Ast> {
			Box::new(tree(r, depth - 1))
		}
		match r.below(16) {
			0 => Ast::Add(sub(r, depth), sub(r, depth)),
			1 => Ast::Sub(sub(r, depth), sub(r, depth)),
			2 => Ast::Mul(sub(r, depth), sub(r, depth)),
			3 => Ast::Div(sub(r, depth), Box::new(Ast::Add(Box::new(Ast::Abs(sub(r, depth))), Box::new(Ast::Const(1.0))))),
			4 => Ast::Neg(sub(r, depth)),
			5 => Ast::Square(sub(r, depth)),
			6 => Ast::Abs(sub(r, depth)),
			7 => Ast::Sqrt(Box::new(Ast::Abs(sub(r, depth)))),
			// `exp` of an unbounded subtree overflows to infinity, and two infinities meeting is a NaN
			8 => Ast::Exp(Box::new(Ast::Min(sub(r, depth), Box::new(Ast::Const(4.0))))),
			9 => Ast::Powi(sub(r, depth), r.below(4) as i32),
			10 => Ast::Min(sub(r, depth), sub(r, depth)),
			11 => Ast::Max(sub(r, depth), sub(r, depth)),
			12 => Ast::Cmp(sub(r, depth), sub(r, depth)),
			13 => Ast::Select(sub(r, depth), sub(r, depth), sub(r, depth)),
			14 => Ast::Sum((0..2 + r.below(2)).map(|_| tree(r, depth - 1)).collect()),
			_ => leaf(r),
		}
	}

	/// Whether every *subexpression* is finite here, not merely the root — an infinity swallowed by a
	/// `Min` still poisons the difference taken around it, and the root would never say so.
	fn finite_everywhere(e: &Ast, env: &[f64]) -> bool {
		if !e.eval(env).is_finite() {
			return false;
		}
		let kids: Vec<&Ast> = match e {
			Ast::Const(_) | Ast::Var(_) => vec![],
			Ast::Neg(a) | Ast::Square(a) | Ast::Abs(a) | Ast::Sqrt(a) | Ast::Exp(a) | Ast::Powi(a, _) => vec![&**a],
			Ast::Add(a, b) | Ast::Sub(a, b) | Ast::Mul(a, b) | Ast::Div(a, b) | Ast::Min(a, b) | Ast::Max(a, b) | Ast::Cmp(a, b) => vec![&**a, &**b],
			Ast::Select(a, b, c) => vec![&**a, &**b, &**c],
			Ast::Sum(xs) => xs.iter().collect(),
		};
		kids.into_iter().all(|k| finite_everywhere(k, env))
	}

	/// The central difference at `h` and at `2h`, Richardson-extrapolated — and `None` where the two
	/// disagree, which is the whole conditioning test.
	///
	/// This is what keeps the tolerance from being a tuning exercise. A point near a kink, a division
	/// by something near zero, or an `Exp` on its way to infinity all show up as the two step sizes
	/// disagreeing, and a point the *difference* cannot resolve is no evidence about the derivative.
	/// A well-conditioned point, meanwhile, is resolved to far better than the tolerance below.
	fn difference(e: &Ast, env: &[f64], i: usize) -> Option<f64> {
		let at = |h: f64| {
			let (mut p, mut m) = (env.to_vec(), env.to_vec());
			p[i] += h;
			m[i] -= h;
			let (a, b) = (e.eval(&p), e.eval(&m));
			(a.is_finite() && b.is_finite() && a.abs() < 1e9 && b.abs() < 1e9).then(|| (a - b) / (2.0 * h))
		};
		let h = 1e-5;
		let (fine, coarse) = (at(h)?, at(2.0 * h)?);
		((fine - coarse).abs() <= 1e-4 * (1.0 + fine.abs())).then(|| (4.0 * fine - coarse) / 3.0)
	}

	// r[verify kernels.jac.two-quantities]
	/// Every reading of one expression agrees: the symbolic derivative, the simplified symbolic
	/// derivative, and the numeric difference.
	#[test]
	fn the_symbolic_derivative_is_the_numeric_one() {
		let mut r = Lcg(0x9e37_79b9_7f4a_7c15);
		let (mut checked, mut skipped, mut recovered) = (0u32, 0u32, 0u32);
		for _ in 0..2_000 {
			let depth = 1 + r.below(4) as u32;
			let e = tree(&mut r, depth);
			// positive and away from 0, so `Sqrt` and `Div` have a fighting chance; the conditioning
			// test above is what handles the points where they do not.
			let env: Vec<f64> = (0..VARS).map(|_| r.span(0.4, 2.6)).collect();
			if !finite_everywhere(&e, &env) {
				continue;
			}
			for i in 0..VARS {
				let d = e.diff(i);
				let (raw, exact) = (d.eval(&env), d.simplify().eval(&env));
				// Simplification is a rewrite of the same expression, so where both readings are
				// numbers they are the same number. Where the raw one is *not* a number and the
				// simplified one is, simplify has removed an indeterminacy the chain rule wrote down
				// and cannot cancel by evaluating — `∂sqrt(u) = u'/(2·sqrt(u))` is `0/0` where `u` is a
				// flat `Cmp` reading zero, and the composite's derivative is the `0` the fold finds.
				// That is why the crate's own tests evaluate `diff(i).simplify()` and not `diff(i)`,
				// and it is the contract this checks against rather than one it argues with.
				if raw.is_finite() {
					assert!((raw - exact).abs() <= 1e-9 * (1.0 + raw.abs()), "simplify moved ∂{i} of {e} from {raw} to {exact} at {env:?}");
				} else {
					recovered += 1;
				}
				let Some(fd) = difference(&e, &env, i) else {
					skipped += 1;
					continue;
				};
				if !exact.is_finite() {
					skipped += 1;
					continue;
				}
				checked += 1;
				assert!(
					(exact - fd).abs() <= 1e-4 * (1.0 + exact.abs().max(fd.abs())),
					"∂{i} of {e} at {env:?}: algebra says {exact}, the difference says {fd}"
				);
			}
		}
		// Loud, because a conditioning test that rejected everything would report green.
		eprintln!("expr fuzz: {checked} derivatives checked, {skipped} points too ill-conditioned to be evidence, {recovered} indeterminacies simplify removed");
		assert!(checked > 2_000, "only {checked} of {} points were well-conditioned enough to check", checked + skipped);
	}
}
