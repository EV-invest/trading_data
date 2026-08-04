//! A typed primitive algebra: one expression value, four readings — evaluate, differentiate
//! (exact), document (LaTeX/infix), debug (value-annotated trace). à la Jane Street's
//! "Computations that differentiate, debug and document themselves".
//!
//! The tree is nested `Copy` marker structs ([`Var`]/[`Const`]/[`Add`]/…), zero heap on the
//! compute path; [`Expr::eval`] monomorphizes to straight-line FMA and [`Expr::grad`] accumulates
//! exact partials by the chain rule. [`Expr::lower`] projects to the runtime [`Ast`] enum for the
//! documentation/debug readings, where `alloc` is fine (a LaTeX string allocates anyway).
//!
//! [`Const`] is the sole `f64 → Expr` door; there is no `Expr → f64 → Expr` escape, so a
//! `trading_data_dag::Symbolic` body *cannot* compute any other way — the algebra is load-bearing.
#![no_std]

extern crate alloc;

use alloc::{boxed::Box, format, string::String, vec::Vec};
use core::{fmt, ops};

/// One expression node: evaluate, accumulate exact partials, or project to [`Ast`].
pub trait Expr: Copy {
	fn eval(&self, env: &[f64]) -> f64;
	/// Chain-rule pass: returns `self`'s value and adds `seed · ∂self/∂env[i]` into `grad[i]`
	/// (`grad.len() == env.len()`). `seed` is `∂output/∂self` — 1.0 at the root.
	///
	/// ponytail: `Mul`/`Div`/`Square`/`Abs` re-`eval` a subtree then `grad` re-walks it —
	/// `T(s) = s + 2·T(s/2)`, i.e. O(n·log n), exact and fine for the shallow scalar kernels this
	/// serves; switch to single-pass reverse-mode over a small tape (cached node values) only if a
	/// genuinely large/deep kernel appears.
	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64;
	fn lower(&self) -> Ast;
}

/// Zero-size leaf reading `env[I]`; `I` is the dep slot it stands for.
#[derive(Clone, Copy)]
pub struct Var<const I: usize>;
/// The only `f64` entry point into the algebra.
#[derive(Clone, Copy)]
pub struct Const(pub f64);
#[derive(Clone, Copy)]
pub struct Add<L, R>(pub L, pub R);
#[derive(Clone, Copy)]
pub struct Sub<L, R>(pub L, pub R);
#[derive(Clone, Copy)]
pub struct Mul<L, R>(pub L, pub R);
#[derive(Clone, Copy)]
pub struct Div<L, R>(pub L, pub R);
#[derive(Clone, Copy)]
pub struct Neg<E>(pub E);
#[derive(Clone, Copy)]
pub struct Square<E>(pub E);
#[derive(Clone, Copy)]
pub struct Abs<E>(pub E);
/// Homogeneous fold; heterogeneous sums chain via `+`.
#[derive(Clone, Copy)]
pub struct Sum<E, const N: usize>(pub [E; N]);

fn sign(x: f64) -> f64 {
	if x > 0.0 {
		1.0
	} else if x < 0.0 {
		-1.0
	} else {
		0.0
	}
}

impl<const I: usize> Expr for Var<I> {
	fn eval(&self, env: &[f64]) -> f64 {
		env[I]
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		grad[I] += seed;
		env[I]
	}

	fn lower(&self) -> Ast {
		Ast::Var(I)
	}
}

impl Expr for Const {
	fn eval(&self, _: &[f64]) -> f64 {
		self.0
	}

	fn grad(&self, _: &[f64], _: f64, _: &mut [f64]) -> f64 {
		self.0
	}

	fn lower(&self) -> Ast {
		Ast::Const(self.0)
	}
}

impl<L: Expr, R: Expr> Expr for Add<L, R> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env) + self.1.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		self.0.grad(env, seed, grad) + self.1.grad(env, seed, grad)
	}

	fn lower(&self) -> Ast {
		Ast::Add(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<L: Expr, R: Expr> Expr for Sub<L, R> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env) - self.1.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		self.0.grad(env, seed, grad) - self.1.grad(env, -seed, grad)
	}

	fn lower(&self) -> Ast {
		Ast::Sub(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<L: Expr, R: Expr> Expr for Mul<L, R> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env) * self.1.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let (lv, rv) = (self.0.eval(env), self.1.eval(env));
		self.0.grad(env, seed * rv, grad);
		self.1.grad(env, seed * lv, grad);
		lv * rv
	}

	fn lower(&self) -> Ast {
		Ast::Mul(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<L: Expr, R: Expr> Expr for Div<L, R> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env) / self.1.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let (lv, rv) = (self.0.eval(env), self.1.eval(env));
		self.0.grad(env, seed / rv, grad);
		self.1.grad(env, -seed * lv / (rv * rv), grad);
		lv / rv
	}

	fn lower(&self) -> Ast {
		Ast::Div(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<E: Expr> Expr for Neg<E> {
	fn eval(&self, env: &[f64]) -> f64 {
		-self.0.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		-self.0.grad(env, -seed, grad)
	}

	fn lower(&self) -> Ast {
		Ast::Neg(Box::new(self.0.lower()))
	}
}

impl<E: Expr> Expr for Square<E> {
	fn eval(&self, env: &[f64]) -> f64 {
		let v = self.0.eval(env);
		v * v
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let v = self.0.eval(env);
		self.0.grad(env, seed * 2.0 * v, grad);
		v * v
	}

	fn lower(&self) -> Ast {
		Ast::Square(Box::new(self.0.lower()))
	}
}

impl<E: Expr> Expr for Abs<E> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env).abs()
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		// at the kink `sign(0)=0` picks subgradient 0; `diff().eval` gives `0/0 = NaN` there —
		// the two agree only off the kink.
		let v = self.0.eval(env);
		self.0.grad(env, seed * sign(v), grad);
		v.abs()
	}

	fn lower(&self) -> Ast {
		Ast::Abs(Box::new(self.0.lower()))
	}
}

impl<E: Expr, const N: usize> Expr for Sum<E, N> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.iter().map(|e| e.eval(env)).sum()
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		self.0.iter().map(|e| e.grad(env, seed, grad)).sum()
	}

	fn lower(&self) -> Ast {
		Ast::Sum(self.0.iter().map(Expr::lower).collect())
	}
}

/// Operator-ergonomics handle: `x*y + constant(2.0)` builds the nested type. Delegates every
/// [`Expr`] method to its inner primitive.
#[derive(Clone, Copy)]
pub struct Ex<T: Expr>(pub T);

impl<T: Expr> Expr for Ex<T> {
	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		self.0.grad(env, seed, grad)
	}

	fn lower(&self) -> Ast {
		self.0.lower()
	}
}

/// The dep-slot dispenser handed to a `trading_data_dag::Symbolic` body: `v.get::<I>()` is the
/// `Var` reading dep `I` (bounds enforced at eval time by `env` length).
#[derive(Clone, Copy)]
pub struct Vars;
impl Vars {
	pub fn get<const I: usize>(self) -> Ex<Var<I>> {
		Ex(Var)
	}
}

pub fn constant(c: f64) -> Ex<Const> {
	Ex(Const(c))
}
pub fn square<T: Expr>(x: Ex<T>) -> Ex<Square<T>> {
	Ex(Square(x.0))
}
pub fn abs<T: Expr>(x: Ex<T>) -> Ex<Abs<T>> {
	Ex(Abs(x.0))
}
pub fn sum<T: Expr, const N: usize>(xs: [Ex<T>; N]) -> Ex<Sum<T, N>> {
	Ex(Sum(xs.map(|x| x.0)))
}

impl<L: Expr, R: Expr> ops::Add<Ex<R>> for Ex<L> {
	type Output = Ex<Add<L, R>>;

	fn add(self, rhs: Ex<R>) -> Self::Output {
		Ex(Add(self.0, rhs.0))
	}
}
impl<L: Expr, R: Expr> ops::Sub<Ex<R>> for Ex<L> {
	type Output = Ex<Sub<L, R>>;

	fn sub(self, rhs: Ex<R>) -> Self::Output {
		Ex(Sub(self.0, rhs.0))
	}
}
impl<L: Expr, R: Expr> ops::Mul<Ex<R>> for Ex<L> {
	type Output = Ex<Mul<L, R>>;

	fn mul(self, rhs: Ex<R>) -> Self::Output {
		Ex(Mul(self.0, rhs.0))
	}
}
impl<L: Expr, R: Expr> ops::Div<Ex<R>> for Ex<L> {
	type Output = Ex<Div<L, R>>;

	fn div(self, rhs: Ex<R>) -> Self::Output {
		Ex(Div(self.0, rhs.0))
	}
}
impl<E: Expr> ops::Neg for Ex<E> {
	type Output = Ex<Neg<E>>;

	fn neg(self) -> Self::Output {
		Ex(Neg(self.0))
	}
}

/// The runtime projection of an [`Expr`], for the documentation/debug readings only:
/// [`Ast::diff`] (exact symbolic derivative), [`Ast::simplify`], [`Ast::latex`]/[`fmt::Display`],
/// and [`Ast::trace`].
#[derive(Clone, Debug, PartialEq)]
pub enum Ast {
	Const(f64),
	Var(usize),
	Add(Box<Ast>, Box<Ast>),
	Sub(Box<Ast>, Box<Ast>),
	Mul(Box<Ast>, Box<Ast>),
	Div(Box<Ast>, Box<Ast>),
	Neg(Box<Ast>),
	Square(Box<Ast>),
	Abs(Box<Ast>),
	Sum(Vec<Ast>),
}

impl Ast {
	pub fn eval(&self, env: &[f64]) -> f64 {
		match self {
			Ast::Const(c) => *c,
			Ast::Var(i) => env[*i],
			Ast::Add(l, r) => l.eval(env) + r.eval(env),
			Ast::Sub(l, r) => l.eval(env) - r.eval(env),
			Ast::Mul(l, r) => l.eval(env) * r.eval(env),
			Ast::Div(l, r) => l.eval(env) / r.eval(env),
			Ast::Neg(e) => -e.eval(env),
			Ast::Square(e) => {
				let v = e.eval(env);
				v * v
			}
			Ast::Abs(e) => e.eval(env).abs(),
			Ast::Sum(xs) => xs.iter().map(|e| e.eval(env)).sum(),
		}
	}

	/// Exact symbolic derivative wrt dep `var`, chain rule per node. Call [`Ast::simplify`] to
	/// clear the `0`/`1` litter it leaves. `sign(e)` is rendered as `e / |e|` (exact off the kink).
	pub fn diff(&self, var: usize) -> Ast {
		let b = |a: Ast| Box::new(a);
		match self {
			Ast::Const(_) => Ast::Const(0.0),
			Ast::Var(i) => Ast::Const((*i == var) as u8 as f64),
			Ast::Add(l, r) => Ast::Add(b(l.diff(var)), b(r.diff(var))),
			Ast::Sub(l, r) => Ast::Sub(b(l.diff(var)), b(r.diff(var))),
			Ast::Mul(l, r) => Ast::Add(b(Ast::Mul(b(l.diff(var)), r.clone())), b(Ast::Mul(l.clone(), b(r.diff(var))))),
			Ast::Div(l, r) => Ast::Div(
				b(Ast::Sub(b(Ast::Mul(b(l.diff(var)), r.clone())), b(Ast::Mul(l.clone(), b(r.diff(var)))))),
				b(Ast::Square(r.clone())),
			),
			Ast::Neg(e) => Ast::Neg(b(e.diff(var))),
			Ast::Square(e) => Ast::Mul(b(Ast::Const(2.0)), b(Ast::Mul(e.clone(), b(e.diff(var))))),
			// `e/|e|` is `0/0 = NaN` at `e = 0`; `Div`-by-zero propagates `inf`/`NaN` — the honest
			// numeric result, matching the FD path's own NaN handling.
			Ast::Abs(e) => Ast::Mul(b(Ast::Div(e.clone(), b(Ast::Abs(e.clone())))), b(e.diff(var))),
			Ast::Sum(xs) => Ast::Sum(xs.iter().map(|e| e.diff(var)).collect()),
		}
	}

	/// Const-fold + `0`/`1` identities, so a differentiated tree renders clean.
	pub fn simplify(&self) -> Ast {
		let konst = |a: &Ast| match a {
			Ast::Const(c) => Some(*c),
			_ => None,
		};
		match self {
			Ast::Const(_) | Ast::Var(_) => self.clone(),
			Ast::Add(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a + b),
					(Some(0.0), _) => r,
					(_, Some(0.0)) => l,
					_ => Ast::Add(Box::new(l), Box::new(r)),
				}
			}
			Ast::Sub(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a - b),
					(_, Some(0.0)) => l,
					(Some(0.0), _) => Ast::Neg(Box::new(r)).simplify(),
					_ => Ast::Sub(Box::new(l), Box::new(r)),
				}
			}
			Ast::Mul(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a * b),
					(Some(0.0), _) | (_, Some(0.0)) => Ast::Const(0.0),
					(Some(1.0), _) => r,
					(_, Some(1.0)) => l,
					_ => Ast::Mul(Box::new(l), Box::new(r)),
				}
			}
			Ast::Div(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a / b),
					(Some(0.0), _) => Ast::Const(0.0),
					(_, Some(1.0)) => l,
					_ => Ast::Div(Box::new(l), Box::new(r)),
				}
			}
			Ast::Neg(e) => {
				let e = e.simplify();
				match e {
					Ast::Const(c) => Ast::Const(-c),
					Ast::Neg(inner) => *inner,
					_ => Ast::Neg(Box::new(e)),
				}
			}
			Ast::Square(e) => {
				let e = e.simplify();
				match konst(&e) {
					Some(c) => Ast::Const(c * c),
					None => Ast::Square(Box::new(e)),
				}
			}
			Ast::Abs(e) => {
				let e = e.simplify();
				match konst(&e) {
					Some(c) => Ast::Const(c.abs()),
					None => Ast::Abs(Box::new(e)),
				}
			}
			Ast::Sum(xs) => {
				let mut acc = 0.0;
				let mut rest: Vec<Ast> = Vec::new();
				for x in xs.iter().map(Ast::simplify) {
					match x {
						Ast::Const(c) => acc += c,
						other => rest.push(other),
					}
				}
				if acc != 0.0 || rest.is_empty() {
					rest.push(Ast::Const(acc));
				}
				if rest.len() == 1 { rest.pop().expect("len == 1") } else { Ast::Sum(rest) }
			}
		}
	}

	pub fn latex(&self, names: &[&str]) -> String {
		match self {
			Ast::Const(c) => format!("{c}"),
			Ast::Var(i) => names.get(*i).map_or_else(|| format!("x_{{{i}}}"), |n| String::from(*n)),
			Ast::Add(l, r) => format!("\\left({} + {}\\right)", l.latex(names), r.latex(names)),
			Ast::Sub(l, r) => format!("\\left({} - {}\\right)", l.latex(names), r.latex(names)),
			Ast::Mul(l, r) => format!("\\left({} \\cdot {}\\right)", l.latex(names), r.latex(names)),
			Ast::Div(l, r) => format!("\\frac{{{}}}{{{}}}", l.latex(names), r.latex(names)),
			Ast::Neg(e) => format!("-{}", e.latex(names)),
			Ast::Square(e) => format!("{}^{{2}}", e.latex(names)),
			Ast::Abs(e) => format!("\\left|{}\\right|", e.latex(names)),
			Ast::Sum(xs) => {
				let parts: Vec<String> = xs.iter().map(|e| e.latex(names)).collect();
				format!("\\left({}\\right)", parts.join(" + "))
			}
		}
	}

	/// Value-annotated tree: every node paired with its value under `env`, for the box-drawing
	/// debug view.
	pub fn trace(&self, env: &[f64]) -> Trace {
		let val = self.eval(env);
		let (op, kids) = match self {
			Ast::Const(_) => ("const", Vec::new()),
			Ast::Var(_) => ("var", Vec::new()),
			Ast::Add(l, r) => ("+", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Sub(l, r) => ("-", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Mul(l, r) => ("*", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Div(l, r) => ("/", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Neg(e) => ("neg", alloc::vec![e.trace(env)]),
			Ast::Square(e) => ("sq", alloc::vec![e.trace(env)]),
			Ast::Abs(e) => ("abs", alloc::vec![e.trace(env)]),
			Ast::Sum(xs) => ("sum", xs.iter().map(|e| e.trace(env)).collect()),
		};
		Trace { op, val, kids }
	}
}

/// Plain infix over `x_i` metavariables (parens on every compound), the display-dual of
/// [`Ast::latex`].
impl fmt::Display for Ast {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Ast::Const(c) => write!(f, "{c}"),
			Ast::Var(i) => write!(f, "x{i}"),
			Ast::Add(l, r) => write!(f, "({l} + {r})"),
			Ast::Sub(l, r) => write!(f, "({l} - {r})"),
			Ast::Mul(l, r) => write!(f, "({l} * {r})"),
			Ast::Div(l, r) => write!(f, "({l} / {r})"),
			Ast::Neg(e) => write!(f, "(-{e})"),
			Ast::Square(e) => write!(f, "{e}^2"),
			Ast::Abs(e) => write!(f, "|{e}|"),
			Ast::Sum(xs) => {
				f.write_str("(")?;
				for (i, e) in xs.iter().enumerate() {
					if i > 0 {
						f.write_str(" + ")?;
					}
					write!(f, "{e}")?;
				}
				f.write_str(")")
			}
		}
	}
}

/// An [`Ast`] node paired with its evaluated value; [`fmt::Display`] renders the box-drawing
/// intermediate-value tree from the post.
pub struct Trace {
	pub op: &'static str,
	pub val: f64,
	pub kids: Vec<Trace>,
}

impl Trace {
	fn fmt_kids(&self, f: &mut fmt::Formatter<'_>, prefix: &str) -> fmt::Result {
		for (i, k) in self.kids.iter().enumerate() {
			let last = i == self.kids.len() - 1;
			let conn = if last { "└──" } else { "├──" };
			write!(f, "\n{prefix}{conn}◉ {}: {}", k.op, k.val)?;
			let child = format!("{prefix}{}", if last { "   " } else { "│  " });
			k.fmt_kids(f, &child)?;
		}
		Ok(())
	}
}

impl fmt::Display for Trace {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "◉ {}: {}", self.op, self.val)?;
		self.fmt_kids(f, "")
	}
}
