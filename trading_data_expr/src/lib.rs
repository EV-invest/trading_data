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
	/// Whether this expression may evaluate to no number at all — a declination the out plane reads
	/// back as absence (`r[impl outs.absence.typed]`). [`Absent`] is the only leaf that sets it; every
	/// operator derives it, by `|` where absence propagates and by `&` where it is skipped.
	const MAYBE: bool = false;
	/// So a kernel holding an opaque `impl Expr` can ask, as it asks [`Slots::len`]: RPITIT leaves
	/// `<Body as Expr>::MAYBE` unnameable in a bound at the site that would state it.
	fn maybe(&self) -> bool {
		Self::MAYBE
	}

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
/// No number — how a body inside the algebra declines. A leaf of its own rather than
/// `Const(f64::NAN)` because [`Expr::MAYBE`] is what the comparing operators refuse, and a `Const`
/// carrying a NaN would say nothing about which trees can reach one.
#[derive(Clone, Copy)]
pub struct Absent;
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
#[derive(Clone, Copy)]
pub struct Sqrt<E>(pub E);
#[derive(Clone, Copy)]
pub struct Exp<E>(pub E);
/// Integer power, so the exponent stays exact and the derivative stays in the algebra.
#[derive(Clone, Copy)]
pub struct Powi<E, const N: i32>(pub E);
#[derive(Clone, Copy)]
pub struct Min<L, R>(pub L, pub R);
#[derive(Clone, Copy)]
pub struct Max<L, R>(pub L, pub R);
/// `(l < r) as f64` — the algebra's only door out of the reals, and a flat one: a predicate has no
/// slope, which is exactly why a `Gate` node needs a kernel of its own rather than this.
/// `gt`/`ge` are this with the arguments swapped, so there is one comparison to differentiate.
#[derive(Clone, Copy)]
pub struct Cmp<L, R>(pub L, pub R);
/// `e.is_nan() as f64` — [`Cmp`]'s sibling over the one thing `Cmp` may not read. Flat for the same
/// reason: presence is not a quantity with a slope. This is what lets the algebra *reason* about a
/// declination instead of only carrying one — [`present`], [`or`], and the branch [`Ast::diff`] takes
/// through a skipping [`Min`] are all it.
#[derive(Clone, Copy)]
pub struct IsNan<E>(pub E);
/// `if c != 0 { a } else { b }`. The seed reaches the taken branch only, so a `Select` over a `Cmp`
/// is a piecewise expression whose derivative is the taken piece's.
#[derive(Clone, Copy)]
pub struct Select<C, A, B>(pub C, pub A, pub B);
/// `x` where it is a number, `fallback` where it declines — the door out of absence, and the reason
/// it is a node rather than a `Select` written out: `Select`'s definedness is the union of its
/// branches, where this one's is the fallback's alone.
#[derive(Clone, Copy)]
pub struct Or<X, F>(pub X, pub F);
/// Homogeneous fold; heterogeneous sums chain via `+`.
#[derive(Clone, Copy)]
pub struct Sum<E, const N: usize>(pub [E; N]);

/// `f64::powi` is std-only; repeated multiplication is exact for the small exponents this serves and
/// keeps the crate `no_std` without a second libm call.
fn powi(x: f64, n: i32) -> f64 {
	let mut acc = 1.0;
	for _ in 0..n.unsigned_abs() {
		acc *= x;
	}
	match n < 0 {
		true => 1.0 / acc,
		false => acc,
	}
}

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

impl Expr for Absent {
	const MAYBE: bool = true;

	fn eval(&self, _: &[f64]) -> f64 {
		f64::NAN
	}

	fn grad(&self, _: &[f64], _: f64, _: &mut [f64]) -> f64 {
		f64::NAN
	}

	fn lower(&self) -> Ast {
		Ast::Const(f64::NAN)
	}
}

impl<L: Expr, R: Expr> Expr for Add<L, R> {
	const MAYBE: bool = L::MAYBE | R::MAYBE;

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
	const MAYBE: bool = L::MAYBE | R::MAYBE;

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
	const MAYBE: bool = L::MAYBE | R::MAYBE;

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
	const MAYBE: bool = L::MAYBE | R::MAYBE;

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
	const MAYBE: bool = E::MAYBE;

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
	const MAYBE: bool = E::MAYBE;

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
	const MAYBE: bool = E::MAYBE;

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

impl<E: Expr> Expr for Sqrt<E> {
	const MAYBE: bool = E::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		libm::sqrt(self.0.eval(env))
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		// at `v = 0` the slope is infinite and `seed / 0.0` says so, which is the honest reading and
		// the one `diff().eval` gives too.
		let r = libm::sqrt(self.0.eval(env));
		self.0.grad(env, seed / (2.0 * r), grad);
		r
	}

	fn lower(&self) -> Ast {
		Ast::Sqrt(Box::new(self.0.lower()))
	}
}

impl<E: Expr> Expr for Exp<E> {
	const MAYBE: bool = E::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		libm::exp(self.0.eval(env))
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let r = libm::exp(self.0.eval(env));
		self.0.grad(env, seed * r, grad);
		r
	}

	fn lower(&self) -> Ast {
		Ast::Exp(Box::new(self.0.lower()))
	}
}

impl<E: Expr, const N: i32> Expr for Powi<E, N> {
	const MAYBE: bool = E::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		powi(self.0.eval(env), N)
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let v = self.0.eval(env);
		self.0.grad(env, seed * f64::from(N) * powi(v, N - 1), grad);
		powi(v, N)
	}

	fn lower(&self) -> Ast {
		Ast::Powi(Box::new(self.0.lower()), N)
	}
}

/// A NaN operand has no agreed branch under `Cmp` and under a `Select` *condition*: one reads it as
/// false and the other as *taken*, so the value and the slope come off different branches and
/// nothing in the array says so. [`Expr::MAYBE`] is what keeps a declination out of both at compile
/// time; this is the backstop for the NaN no type can predict — the one arithmetic produces, `0/0`
/// inside a tree.
///
/// [`Min`]/[`Max`] are *not* here: they are defined over an absent operand, they skip it, and
/// `MAYBE` says which side comes out (`r[impl outs.absence.typed]`).
///
/// Debug-only because `r[kernels.pure.zero-cost]` is an equality in retired instructions: a release
/// build of a `Pure` node costs what the same arithmetic written by hand costs, and a branch here
/// would be a branch the hand-written version does not have.
macro_rules! defined_over {
	($op:literal, $($v:expr),+ $(,)?) => {$(
		debug_assert!(
			!$v.is_nan(),
			concat!($op, " over a NaN operand ({}): an absent reading is a declination and belongs at the element boundary, where it is the out — inside a tree it has no agreed branch"),
			$v
		);
	)+};
}

/// Whether a `Min`/`Max` takes its **left** operand, given the strict comparison that decides it
/// where both are numbers. The tie goes right; an absent right is skipped, and an absent left is
/// skipped by the comparison already being false. One function, so the value, the gradient and
/// [`Ast::diff`] cannot drift apart at the branch.
fn takes_left(strictly: bool, rv: f64) -> bool {
	strictly || rv.is_nan()
}

impl<L: Expr, R: Expr> Expr for Min<L, R> {
	/// Skipped, not propagated: `min(absent, x)` is `x`, so the result is absent only where both
	/// operands are.
	const MAYBE: bool = L::MAYBE & R::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		// `f64::min` already skips a NaN operand, which is the semantics `MAYBE` states.
		self.0.eval(env).min(self.1.eval(env))
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let (lv, rv) = (self.0.eval(env), self.1.eval(env));
		match takes_left(lv < rv, rv) {
			true => self.0.grad(env, seed, grad),
			false => self.1.grad(env, seed, grad),
		}
	}

	fn lower(&self) -> Ast {
		Ast::Min(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<L: Expr, R: Expr> Expr for Max<L, R> {
	const MAYBE: bool = L::MAYBE & R::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		self.0.eval(env).max(self.1.eval(env))
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let (lv, rv) = (self.0.eval(env), self.1.eval(env));
		match takes_left(rv < lv, rv) {
			true => self.0.grad(env, seed, grad),
			false => self.1.grad(env, seed, grad),
		}
	}

	fn lower(&self) -> Ast {
		Ast::Max(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<E: Expr> Expr for IsNan<E> {
	fn eval(&self, env: &[f64]) -> f64 {
		f64::from(self.0.eval(env).is_nan())
	}

	fn grad(&self, env: &[f64], _: f64, _: &mut [f64]) -> f64 {
		// flat, exactly as `Cmp` is: presence is piecewise constant, and the step where it changes is
		// not a slope any reading may claim.
		self.eval(env)
	}

	fn lower(&self) -> Ast {
		Ast::IsNan(Box::new(self.0.lower()))
	}
}

impl<X: Expr, F: Expr> Expr for Or<X, F> {
	const MAYBE: bool = F::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		let x = self.0.eval(env);
		match x.is_nan() {
			true => self.1.eval(env),
			false => x,
		}
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		match self.0.eval(env).is_nan() {
			true => self.1.grad(env, seed, grad),
			false => self.0.grad(env, seed, grad),
		}
	}

	/// As the `Select` a reader would have written: the presence test is the condition, and the
	/// fallback is the branch it takes.
	fn lower(&self) -> Ast {
		let x = self.0.lower();
		Ast::Select(Box::new(Ast::IsNan(Box::new(x.clone()))), Box::new(self.1.lower()), Box::new(x))
	}
}

impl<L: Expr, R: Expr> Expr for Cmp<L, R> {
	fn eval(&self, env: &[f64]) -> f64 {
		let (lv, rv) = (self.0.eval(env), self.1.eval(env));
		defined_over!("cmp", lv, rv);
		f64::from(lv < rv)
	}

	fn grad(&self, env: &[f64], _: f64, _: &mut [f64]) -> f64 {
		// no seed reaches either side: the value is piecewise constant, so every partial is 0 and the
		// step at the boundary is not a slope any reading may claim.
		self.eval(env)
	}

	fn lower(&self) -> Ast {
		Ast::Cmp(Box::new(self.0.lower()), Box::new(self.1.lower()))
	}
}

impl<C: Expr, A: Expr, B: Expr> Expr for Select<C, A, B> {
	/// The branches, and not the condition: a `Select` is where a body *writes* a declination, and
	/// which branch is live is not a thing the type knows.
	const MAYBE: bool = A::MAYBE | B::MAYBE;

	fn eval(&self, env: &[f64]) -> f64 {
		let c = self.0.eval(env);
		defined_over!("select's condition", c);
		match c != 0.0 {
			true => self.1.eval(env),
			false => self.2.eval(env),
		}
	}

	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64 {
		let c = self.0.eval(env);
		defined_over!("select's condition", c);
		match c != 0.0 {
			true => self.1.grad(env, seed, grad),
			false => self.2.grad(env, seed, grad),
		}
	}

	fn lower(&self) -> Ast {
		Ast::Select(Box::new(self.0.lower()), Box::new(self.1.lower()), Box::new(self.2.lower()))
	}
}

impl<E: Expr, const N: usize> Expr for Sum<E, N> {
	const MAYBE: bool = E::MAYBE;

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

/// One expression per slot of a multi-slot out — what a per-element kernel evaluates to fill an
/// item. Heterogeneous, so a tuple rather than [`Sum`]'s homogeneous array: the slots of an item are
/// different functions of one env, and a single-slot out is the one-tuple spelled without brackets.
pub trait Slots: Copy {
	const LEN: usize;
	/// Whether *any* slot may decline — [`Expr::MAYBE`] over the tuple, which is what the item being
	/// filled owes an absence channel for.
	const MAYBE: bool = false;
	/// So a kernel holding an opaque `impl Slots` can check its width against the item it fills.
	fn len(&self) -> usize {
		Self::LEN
	}

	fn maybe(&self) -> bool {
		Self::MAYBE
	}

	fn eval_slots(&self, env: &[f64], out: &mut [f64]);
	/// Row-major `LEN × env.len()`, accumulated — the caller zeroes.
	fn grad_slots(&self, env: &[f64], jac: &mut [f64]);
	fn lower_slots(&self) -> Vec<Ast>;
}

/// [`Ex`] rather than a blanket over [`Expr`]: a downstream crate may implement this crate's `Expr`
/// for a tuple, which would make the blanket overlap the tuple impls below.
impl<T: Expr> Slots for Ex<T> {
	const LEN: usize = 1;
	const MAYBE: bool = T::MAYBE;

	fn eval_slots(&self, env: &[f64], out: &mut [f64]) {
		out[0] = self.eval(env);
	}

	fn grad_slots(&self, env: &[f64], jac: &mut [f64]) {
		self.grad(env, 1.0, jac);
	}

	fn lower_slots(&self) -> Vec<Ast> {
		alloc::vec![self.lower()]
	}
}

macro_rules! slots_tuple {
	($n:expr; $($T:ident $i:tt),+) => {
		impl<$($T: Expr),+> Slots for ($(Ex<$T>,)+) {
			const LEN: usize = $n;
			const MAYBE: bool = $($T::MAYBE)|+;

			fn eval_slots(&self, env: &[f64], out: &mut [f64]) {
				$(out[$i] = self.$i.eval(env);)+
			}

			fn grad_slots(&self, env: &[f64], jac: &mut [f64]) {
				let w = env.len();
				$(self.$i.grad(env, 1.0, &mut jac[$i * w..($i + 1) * w]);)+
			}

			fn lower_slots(&self) -> Vec<Ast> {
				alloc::vec![$(self.$i.lower()),+]
			}
		}
	};
}
slots_tuple!(2; A 0, B 1);
slots_tuple!(3; A 0, B 1, C 2);
slots_tuple!(4; A 0, B 1, C 2, D 3);
slots_tuple!(5; A 0, B 1, C 2, D 3, E 4);

/// Operator-ergonomics handle: `x*y + constant(2.0)` builds the nested type. Delegates every
/// [`Expr`] method to its inner primitive.
#[derive(Clone, Copy)]
pub struct Ex<T: Expr>(pub T);

impl<T: Expr> Expr for Ex<T> {
	const MAYBE: bool = T::MAYBE;

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
/// How a body inside the algebra declines, which the out plane reads back as absence
/// (`r[impl outs.absence.typed]`). Every tree reaching one carries [`Expr::MAYBE`], so the operators
/// that cannot answer over one refuse it where it is written rather than where it lands.
pub fn absent() -> Ex<Absent> {
	Ex(Absent)
}
/// `1.0` where `x` is a number, `0.0` where it declines — how a body *reads* an absence instead of
/// merely carrying one. Flat, and never absent itself, so it is a condition the comparing operators
/// accept.
pub fn present<T: Expr>(x: Ex<T>) -> Ex<Sub<Const, IsNan<T>>> {
	Ex(Sub(Const(1.0), IsNan(x.0)))
}
/// `x` where it stands, `fallback` where it declines. The way out of absence: what comes back is a
/// number exactly as often as `fallback` is one, which is what lets a body compare it.
pub fn or<T: Expr, F: Expr>(x: Ex<T>, fallback: Ex<F>) -> Ex<Or<T, F>> {
	Ex(Or(x.0, fallback.0))
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
pub fn sqrt<T: Expr>(x: Ex<T>) -> Ex<Sqrt<T>> {
	Ex(Sqrt(x.0))
}
pub fn exp<T: Expr>(x: Ex<T>) -> Ex<Exp<T>> {
	Ex(Exp(x.0))
}
pub fn powi_of<T: Expr, const N: i32>(x: Ex<T>) -> Ex<Powi<T, N>> {
	Ex(Powi(x.0))
}
pub fn min<L: Expr, R: Expr>(l: Ex<L>, r: Ex<R>) -> Ex<Min<L, R>> {
	Ex(Min(l.0, r.0))
}
pub fn max<L: Expr, R: Expr>(l: Ex<L>, r: Ex<R>) -> Ex<Max<L, R>> {
	Ex(Max(l.0, r.0))
}
/// A comparison is the one thing an absence has no answer for, so a tree that can carry one may not
/// reach here: `or` it against a number first, or leave the comparison out and let the absence
/// propagate (`r[impl outs.absence.typed]`).
pub fn lt<L: Expr, R: Expr>(l: Ex<L>, r: Ex<R>) -> Ex<Cmp<L, R>> {
	const {
		assert!(
			!L::MAYBE && !R::MAYBE,
			"nothing compares a reading that may not be there: `or(x, d)` it against a number, or drop the comparison and let the absence propagate"
		)
	}
	Ex(Cmp(l.0, r.0))
}
/// `l > r` is `r < l`: one comparison in the algebra, read from the other side.
pub fn gt<L: Expr, R: Expr>(l: Ex<L>, r: Ex<R>) -> Ex<Cmp<R, L>> {
	const {
		assert!(
			!L::MAYBE && !R::MAYBE,
			"nothing compares a reading that may not be there: `or(x, d)` it against a number, or drop the comparison and let the absence propagate"
		)
	}
	Ex(Cmp(r.0, l.0))
}
/// The **condition** may not decline — a `Select` is where a body writes an absence, and a branch
/// that cannot be chosen is no branch. The two branches stay permissive, which is the whole point.
pub fn select<C: Expr, A: Expr, B: Expr>(c: Ex<C>, a: Ex<A>, b: Ex<B>) -> Ex<Select<C, A, B>> {
	const {
		assert!(
			!C::MAYBE,
			"a `Select` condition that may not be there decides nothing: `present(x)` is the test over an absence, and the branches are where one is written"
		)
	}
	Ex(Select(c.0, a.0, b.0))
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
	Sqrt(Box<Ast>),
	Exp(Box<Ast>),
	Powi(Box<Ast>, i32),
	Min(Box<Ast>, Box<Ast>),
	Max(Box<Ast>, Box<Ast>),
	/// `(l < r) as f64`
	Cmp(Box<Ast>, Box<Ast>),
	/// `e.is_nan() as f64` — the presence reading, and the one predicate defined over an absence.
	IsNan(Box<Ast>),
	/// `if c != 0 { a } else { b }`
	Select(Box<Ast>, Box<Ast>, Box<Ast>),
	Sum(Vec<Ast>),
}

/// [`takes_left`] as a tree: the right operand absent takes the left, the left absent then takes the
/// right, and only where both are numbers does `strictly` decide. Nested rather than one disjunction
/// because `Select` evaluates the taken branch alone, which is what keeps the comparison off an
/// operand it refuses.
fn skipping(l: &Ast, r: &Ast, strictly: Ast, var: usize) -> Ast {
	let b = |a: Ast| Box::new(a);
	let (dl, dr) = (l.diff(var), r.diff(var));
	Ast::Select(
		b(Ast::IsNan(b(r.clone()))),
		b(dl.clone()),
		b(Ast::Select(b(Ast::IsNan(b(l.clone()))), b(dr.clone()), b(Ast::Select(b(strictly), b(dl), b(dr))))),
	)
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
			Ast::Sqrt(e) => libm::sqrt(e.eval(env)),
			Ast::Exp(e) => libm::exp(e.eval(env)),
			Ast::Powi(e, n) => powi(e.eval(env), *n),
			Ast::Min(l, r) => l.eval(env).min(r.eval(env)),
			Ast::Max(l, r) => l.eval(env).max(r.eval(env)),
			Ast::IsNan(e) => f64::from(e.eval(env).is_nan()),
			Ast::Cmp(l, r) => {
				let (lv, rv) = (l.eval(env), r.eval(env));
				defined_over!("cmp", lv, rv);
				f64::from(lv < rv)
			}
			Ast::Select(c, a, b) => {
				let c = c.eval(env);
				defined_over!("select's condition", c);
				match c != 0.0 {
					true => a.eval(env),
					false => b.eval(env),
				}
			}
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
			Ast::Sqrt(e) => Ast::Div(b(e.diff(var)), b(Ast::Mul(b(Ast::Const(2.0)), b(Ast::Sqrt(e.clone()))))),
			Ast::Exp(e) => Ast::Mul(b(Ast::Exp(e.clone())), b(e.diff(var))),
			Ast::Powi(e, n) => Ast::Mul(b(Ast::Mul(b(Ast::Const(f64::from(*n))), b(Ast::Powi(e.clone(), n - 1)))), b(e.diff(var))),
			// the kink is a branch, not a slope: which piece is live is what `Cmp` says, and the
			// derivative is that piece's. Strict `<` with the tie going right, matching `Expr::grad` —
			// and the two presence tests ahead of it are `takes_left`'s skip, spelled so that the `Cmp`
			// is never reached over an operand it may not read.
			Ast::Min(l, r) => skipping(l, r, Ast::Cmp(l.clone(), r.clone()), var),
			Ast::Max(l, r) => skipping(l, r, Ast::Cmp(r.clone(), l.clone()), var),
			Ast::Cmp(_, _) | Ast::IsNan(_) => Ast::Const(0.0),
			Ast::Select(c, a, b_) => Ast::Select(c.clone(), b(a.diff(var)), b(b_.diff(var))),
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
			Ast::Sqrt(e) => {
				let e = e.simplify();
				match konst(&e) {
					Some(c) => Ast::Const(libm::sqrt(c)),
					None => Ast::Sqrt(Box::new(e)),
				}
			}
			Ast::Exp(e) => {
				let e = e.simplify();
				match konst(&e) {
					Some(c) => Ast::Const(libm::exp(c)),
					None => Ast::Exp(Box::new(e)),
				}
			}
			Ast::Powi(e, n) => {
				let e = e.simplify();
				match (konst(&e), n) {
					(Some(c), _) => Ast::Const(powi(c, *n)),
					(None, 0) => Ast::Const(1.0),
					(None, 1) => e,
					(None, _) => Ast::Powi(Box::new(e), *n),
				}
			}
			Ast::Min(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a.min(b)),
					_ => Ast::Min(Box::new(l), Box::new(r)),
				}
			}
			Ast::Max(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(a.max(b)),
					_ => Ast::Max(Box::new(l), Box::new(r)),
				}
			}
			Ast::Cmp(l, r) => {
				let (l, r) = (l.simplify(), r.simplify());
				match (konst(&l), konst(&r)) {
					(Some(a), Some(b)) => Ast::Const(f64::from(a < b)),
					_ => Ast::Cmp(Box::new(l), Box::new(r)),
				}
			}
			// the fold that matters: a `Min`/`Max` against a literal threshold differentiates through
			// this, and folding it away is what leaves the derivative reading the one branch it had
			// before absence was expressible.
			Ast::IsNan(e) => {
				let e = e.simplify();
				match konst(&e) {
					Some(c) => Ast::Const(f64::from(c.is_nan())),
					None => Ast::IsNan(Box::new(e)),
				}
			}
			Ast::Select(c, a, b) => {
				let (c, a, b) = (c.simplify(), a.simplify(), b.simplify());
				match konst(&c) {
					Some(c) if c != 0.0 => a,
					Some(_) => b,
					// both arms the same is what a differentiated `Min(x, x)` collapses to
					None if a == b => a,
					None => Ast::Select(Box::new(c), Box::new(a), Box::new(b)),
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
			Ast::Sqrt(e) => format!("\\sqrt{{{}}}", e.latex(names)),
			Ast::Exp(e) => format!("e^{{{}}}", e.latex(names)),
			Ast::Powi(e, n) => format!("{}^{{{n}}}", e.latex(names)),
			Ast::Min(l, r) => format!("\\min\\left({}, {}\\right)", l.latex(names), r.latex(names)),
			Ast::Max(l, r) => format!("\\max\\left({}, {}\\right)", l.latex(names), r.latex(names)),
			Ast::Cmp(l, r) => format!("\\left[{} < {}\\right]", l.latex(names), r.latex(names)),
			Ast::IsNan(e) => format!("\\left[{} = \\varnothing\\right]", e.latex(names)),
			Ast::Select(c, a, b) => format!(
				"\\begin{{cases}}{} & {} \\\\ {} & \\text{{otherwise}}\\end{{cases}}",
				a.latex(names),
				c.latex(names),
				b.latex(names)
			),
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
			Ast::Sqrt(e) => ("sqrt", alloc::vec![e.trace(env)]),
			Ast::Exp(e) => ("exp", alloc::vec![e.trace(env)]),
			Ast::Powi(e, _) => ("powi", alloc::vec![e.trace(env)]),
			Ast::Min(l, r) => ("min", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Max(l, r) => ("max", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::Cmp(l, r) => ("<", alloc::vec![l.trace(env), r.trace(env)]),
			Ast::IsNan(e) => ("absent?", alloc::vec![e.trace(env)]),
			// only the taken branch: a trace shows what the value came from, and the other arm did not
			// contribute to it.
			Ast::Select(c, a, b) => (
				"select",
				match c.eval(env) != 0.0 {
					true => alloc::vec![c.trace(env), a.trace(env)],
					false => alloc::vec![c.trace(env), b.trace(env)],
				},
			),
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
			Ast::Sqrt(e) => write!(f, "sqrt({e})"),
			Ast::Exp(e) => write!(f, "exp({e})"),
			Ast::Powi(e, n) => write!(f, "{e}^{n}"),
			Ast::Min(l, r) => write!(f, "min({l}, {r})"),
			Ast::Max(l, r) => write!(f, "max({l}, {r})"),
			Ast::Cmp(l, r) => write!(f, "[{l} < {r}]"),
			Ast::IsNan(e) => write!(f, "[{e} absent]"),
			Ast::Select(c, a, b) => write!(f, "({a} if {c} else {b})"),
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
