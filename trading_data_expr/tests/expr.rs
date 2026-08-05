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
/// difference of `eval`. This is where the algebra is checked — `r[kernels.jac.one-reading]` leaves
/// the engine with nothing to compare a `Pure` node's Jacobian against at runtime, deliberately, so
/// the comparison runs once here instead of every tick.
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
