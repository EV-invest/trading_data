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
