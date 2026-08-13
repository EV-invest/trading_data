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
//!
//! The two planes are one declaration: the [`ops!`] table below emits the marker struct, its
//! [`Expr`] impl, the [`Ast`] variant and every mechanical walk over it from one row per operator,
//! so `Expr::eval` and `Ast::eval` cannot say different things about the same operator. What a row
//! cannot state — [`Ast::diff`]'s per-operator mathematics and [`Ast::simplify`]'s per-operator
//! identities — is written out below it, and the operators no row fits ([`Const`], [`Var`],
//! [`Powi`], [`Select`], [`Sum`], [`Absent`], [`Or`]) are declared beside the table as the
//! exceptions they are.
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
	/// ponytail: a `chain` row re-`eval`s a subtree then `grad` re-walks it — `T(s) = s + 2·T(s/2)`,
	/// i.e. O(n·log n), exact and fine for the shallow scalar kernels this serves; switch to
	/// single-pass reverse-mode over a small tape (cached node values) only if a genuinely
	/// large/deep kernel appears.
	fn grad(&self, env: &[f64], seed: f64, grad: &mut [f64]) -> f64;
	fn lower(&self) -> Ast;
}

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

/// The definedness lattice a row names, over the operand types it is written across
/// (`r[impl outs.absence.typed]`).
macro_rules! lattice {
	(propagates; $A:ident, $B:ident) => {
		<$A>::MAYBE | <$B>::MAYBE
	};
	(propagates; $A:ident) => {
		<$A>::MAYBE
	};
	(skips; $A:ident, $B:ident) => {
		<$A>::MAYBE & <$B>::MAYBE
	};
	(never; $($T:ident),+) => {
		false
	};
}

/// [`Expr::grad`] for one row, in the shape the row names. The four shapes are the four ways a seed
/// reaches an operand, and the shape is what decides how many passes it costs:
///
/// - `linear` — the partials are constants, so each child's own `grad` return *is* its value and
///   there is no evaluation pass at all;
/// - `chain` — the partials read the operand values, so those are evaluated first and the seed
///   expressions may name them and the node's own value;
/// - `skip` — the whole seed goes to the one operand that came out, absence skipped;
/// - `flat` — the value is piecewise constant, so no seed reaches either side.
macro_rules! grad_fn {
	(linear; ($a:ident, $b:ident) $body:block; $val:ident; [$ga:expr, $gb:expr]; $seed:ident) => {
		fn grad(&self, env: &[f64], $seed: f64, grad: &mut [f64]) -> f64 {
			let $a = self.0.grad(env, $ga, grad);
			let $b = self.1.grad(env, $gb, grad);
			$body
		}
	};
	(linear; ($a:ident) $body:block; $val:ident; [$ga:expr]; $seed:ident) => {
		fn grad(&self, env: &[f64], $seed: f64, grad: &mut [f64]) -> f64 {
			let $a = self.0.grad(env, $ga, grad);
			$body
		}
	};
	(chain; ($a:ident, $b:ident) $body:block; $val:ident; [$ga:expr, $gb:expr]; $seed:ident) => {
		fn grad(&self, env: &[f64], $seed: f64, grad: &mut [f64]) -> f64 {
			let $a = self.0.eval(env);
			let $b = self.1.eval(env);
			let $val = $body;
			self.0.grad(env, $ga, grad);
			self.1.grad(env, $gb, grad);
			$val
		}
	};
	(chain; ($a:ident) $body:block; $val:ident; [$ga:expr]; $seed:ident) => {
		fn grad(&self, env: &[f64], $seed: f64, grad: &mut [f64]) -> f64 {
			let $a = self.0.eval(env);
			let $val = $body;
			self.0.grad(env, $ga, grad);
			$val
		}
	};
	(skip; ($a:ident, $b:ident) $body:block; $val:ident; [$x:ident < $y:ident]; $seed:ident) => {
		fn grad(&self, env: &[f64], $seed: f64, grad: &mut [f64]) -> f64 {
			let $a = self.0.eval(env);
			let $b = self.1.eval(env);
			match takes_left($x < $y, $b) {
				true => self.0.grad(env, $seed, grad),
				false => self.1.grad(env, $seed, grad),
			}
		}
	};
	(flat; ($($a:ident),+) $body:block; $val:ident; []; $seed:ident) => {
		fn grad(&self, env: &[f64], _: f64, _: &mut [f64]) -> f64 {
			self.eval(env)
		}
	};
}

/// One row per operator, and both planes are read off it: the `Copy` marker struct and its [`Expr`]
/// impl on the compute path, the [`Ast`] variant and every mechanical walk over it on the
/// documentation path. A row is
///
/// ```text
/// Name(operands) lattice { value } grad-shape [seeds] "latex" "infix" "trace";
/// ```
///
/// The value expression is written **once** and emitted into `Expr::eval`, into the row's gradient,
/// and into `Ast::eval` — which is the point: two evaluators of one semantics that cannot be edited
/// apart. `Ast::diff` and `Ast::simplify` are not here because they are not mechanical: a
/// derivative is per-operator mathematics and an identity is a per-operator claim, and a row that
/// carried a closure for each would have bought nothing.
macro_rules! ops {
	(
		binds($seed:ident, $val:ident);
		binary { $(
			$(#[$bm:meta])*
			$B:ident($ba:ident, $bb:ident) $blat:tt $bbody:block $bshape:tt [$($bg:tt)*] $btex:literal $bfmt:literal $bop:literal;
		)+ }
		unary { $(
			$(#[$um:meta])*
			$U:ident($ua:ident) $ulat:tt $ubody:block $ushape:tt [$($ug:tt)*] $utex:literal $ufmt:literal $uop:literal;
		)+ }
		irregular { $(
			$(#[$im:meta])*
			$I:ident($($ity:ty),+);
		)+ }
		eval($eenv:ident) { $($earm:tt)* }
		latex($lnames:ident) { $($larm:tt)* }
		display($dout:ident) { $($darm:tt)* }
		trace($tenv:ident) { $($tarm:tt)* }
	) => {
		$(
			$(#[$bm])*
			#[derive(Clone, Copy)]
			pub struct $B<L, R>(pub L, pub R);

			impl<L: Expr, R: Expr> Expr for $B<L, R> {
				const MAYBE: bool = lattice!($blat; L, R);

				fn eval(&self, env: &[f64]) -> f64 {
					let $ba = self.0.eval(env);
					let $bb = self.1.eval(env);
					$bbody
				}

				grad_fn!($bshape; ($ba, $bb) $bbody; $val; [$($bg)*]; $seed);

				fn lower(&self) -> Ast {
					Ast::$B(Box::new(self.0.lower()), Box::new(self.1.lower()))
				}
			}
		)+

		$(
			$(#[$um])*
			#[derive(Clone, Copy)]
			pub struct $U<E>(pub E);

			impl<E: Expr> Expr for $U<E> {
				const MAYBE: bool = lattice!($ulat; E);

				fn eval(&self, env: &[f64]) -> f64 {
					let $ua = self.0.eval(env);
					$ubody
				}

				grad_fn!($ushape; ($ua) $ubody; $val; [$($ug)*]; $seed);

				fn lower(&self) -> Ast {
					Ast::$U(Box::new(self.0.lower()))
				}
			}
		)+

		/// The runtime projection of an [`Expr`], for the documentation/debug readings only:
		/// [`Ast::diff`] (exact symbolic derivative), [`Ast::simplify`], [`Ast::latex`]/[`fmt::Display`],
		/// and [`Ast::trace`].
		#[derive(Clone, Debug, PartialEq)]
		pub enum Ast {
			$($(#[$im])* $I($($ity),+),)+
			$($(#[$bm])* $B(Box<Ast>, Box<Ast>),)+
			$($(#[$um])* $U(Box<Ast>),)+
		}

		impl Ast {
			pub fn eval(&self, $eenv: &[f64]) -> f64 {
				match self {
					$(Ast::$B($ba, $bb) => { let $ba = $ba.eval($eenv); let $bb = $bb.eval($eenv); $bbody })+
					$(Ast::$U($ua) => { let $ua = $ua.eval($eenv); $ubody })+
					$($earm)*
				}
			}

			pub fn latex(&self, $lnames: &[&str]) -> String {
				match self {
					$(Ast::$B($ba, $bb) => format!($btex, $ba.latex($lnames), $bb.latex($lnames)),)+
					$(Ast::$U($ua) => format!($utex, $ua.latex($lnames)),)+
					$($larm)*
				}
			}

			/// Value-annotated tree: every node paired with its value under `env`, for the box-drawing
			/// debug view.
			pub fn trace(&self, $tenv: &[f64]) -> Trace {
				let val = self.eval($tenv);
				let (op, kids) = match self {
					$(Ast::$B($ba, $bb) => ($bop, alloc::vec![$ba.trace($tenv), $bb.trace($tenv)]),)+
					$(Ast::$U($ua) => ($uop, alloc::vec![$ua.trace($tenv)]),)+
					$($tarm)*
				};
				Trace { op, val, kids }
			}
		}

		/// Plain infix over `x_i` metavariables (parens on every compound), the display-dual of
		/// [`Ast::latex`].
		impl fmt::Display for Ast {
			fn fmt(&self, $dout: &mut fmt::Formatter<'_>) -> fmt::Result {
				match self {
					$(Ast::$B($ba, $bb) => write!($dout, $bfmt, $ba, $bb),)+
					$(Ast::$U($ua) => write!($dout, $ufmt, $ua),)+
					$($darm)*
				}
			}
		}
	};
}

ops! {
	// `seed` is `Expr::grad`'s incoming seed and `val` the node's own value; both are named at this
	// site because a binding a macro makes for itself is invisible to the tokens written here.
	binds(seed, val);

	binary {
		Add(l, r) propagates { l + r } linear [seed, seed] "\\left({} + {}\\right)" "({} + {})" "+";
		Sub(l, r) propagates { l - r } linear [seed, -seed] "\\left({} - {}\\right)" "({} - {})" "-";
		Mul(l, r) propagates { l * r } chain [seed * r, seed * l] "\\left({} \\cdot {}\\right)" "({} * {})" "*";
		Div(l, r) propagates { l / r } chain [seed / r, -seed * l / (r * r)] "\\frac{{{}}}{{{}}}" "({} / {})" "/";
		/// Skipped, not propagated: `min(absent, x)` is `x`, so the result is absent only where both
		/// operands are — and `f64::min` already skips a NaN operand, which is that semantics.
		Min(l, r) skips { l.min(r) } skip [l < r] "\\min\\left({}, {}\\right)" "min({}, {})" "min";
		Max(l, r) skips { l.max(r) } skip [r < l] "\\max\\left({}, {}\\right)" "max({}, {})" "max";
		/// `(l < r) as f64` — the algebra's only door out of the reals, and a flat one: a predicate has
		/// no slope, which is exactly why a `Gate` node needs a kernel of its own rather than this.
		/// `gt`/`ge` are this with the arguments swapped, so there is one comparison to differentiate.
		Cmp(l, r) never { defined_over!("cmp", l, r); f64::from(l < r) } flat [] "\\left[{} < {}\\right]" "[{} < {}]" "<";
	}

	unary {
		Neg(e) propagates { -e } linear [-seed] "-{}" "(-{})" "neg";
		Square(e) propagates { e * e } chain [seed * 2.0 * e] "{}^{{2}}" "{}^2" "sq";
		// at the kink `sign(0)=0` picks subgradient 0; `diff().eval` gives `0/0 = NaN` there — the two
		// agree only off the kink.
		Abs(e) propagates { e.abs() } chain [seed * sign(e)] "\\left|{}\\right|" "|{}|" "abs";
		// at `val = 0` the slope is infinite and `seed / 0.0` says so, which is the honest reading and
		// the one `diff().eval` gives too.
		Sqrt(e) propagates { libm::sqrt(e) } chain [seed / (2.0 * val)] "\\sqrt{{{}}}" "sqrt({})" "sqrt";
		Exp(e) propagates { libm::exp(e) } chain [seed * val] "e^{{{}}}" "exp({})" "exp";
		/// `e.is_nan() as f64` — [`Cmp`]'s sibling over the one thing `Cmp` may not read. Flat for the
		/// same reason: presence is not a quantity with a slope. This is what lets the algebra *reason*
		/// about a declination instead of only carrying one — [`present`], [`or`], and the branch
		/// [`Ast::diff`] takes through a skipping [`Min`] are all it.
		IsNan(e) never { f64::from(e.is_nan()) } flat [] "\\left[{} = \\varnothing\\right]" "[{} absent]" "absent?";
	}

	// No row fits these: a leaf carries a payload instead of operands, `Powi` carries its exponent in
	// the type, `Select` evaluates one branch rather than both, and `Sum` is variadic. Their `Expr`
	// impls are written out below the table, and the arms a walk needs are here.
	irregular {
		Const(f64);
		Var(usize);
		Powi(Box<Ast>, i32);
		/// `if c != 0 { a } else { b }`
		Select(Box<Ast>, Box<Ast>, Box<Ast>);
		Sum(Vec<Ast>);
	}

	eval(env) {
		Ast::Const(c) => *c,
		Ast::Var(i) => env[*i],
		Ast::Powi(e, n) => powi(e.eval(env), *n),
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

	latex(names) {
		Ast::Const(c) => format!("{c}"),
		Ast::Var(i) => names.get(*i).map_or_else(|| format!("x_{{{i}}}"), |n| String::from(*n)),
		Ast::Powi(e, n) => format!("{}^{{{n}}}", e.latex(names)),
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

	display(f) {
		Ast::Const(c) => write!(f, "{c}"),
		Ast::Var(i) => write!(f, "x{i}"),
		Ast::Powi(e, n) => write!(f, "{e}^{n}"),
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

	trace(env) {
		Ast::Const(_) => ("const", Vec::new()),
		Ast::Var(_) => ("var", Vec::new()),
		Ast::Powi(e, _) => ("powi", alloc::vec![e.trace(env)]),
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
	}
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
/// Integer power, so the exponent stays exact and the derivative stays in the algebra.
#[derive(Clone, Copy)]
pub struct Powi<E, const N: i32>(pub E);
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

/// A node whose operands all simplified to constants *is* its value, and that value comes from
/// [`Ast::eval`] rather than from a second copy of the operator's law.
fn fold1(e: Ast, mk: impl FnOnce(Box<Ast>) -> Ast) -> Ast {
	let konst = matches!(e, Ast::Const(_));
	let n = mk(Box::new(e));
	match konst {
		true => Ast::Const(n.eval(&[])),
		false => n,
	}
}

fn fold2(l: Ast, r: Ast, mk: impl FnOnce(Box<Ast>, Box<Ast>) -> Ast) -> Ast {
	let konst = matches!((&l, &r), (Ast::Const(_), Ast::Const(_)));
	let n = mk(Box::new(l), Box::new(r));
	match konst {
		true => Ast::Const(n.eval(&[])),
		false => n,
	}
}

/// The two walks no row states: a derivative is per-operator mathematics, and an identity is a
/// per-operator claim about what an operator may be rewritten to.
impl Ast {
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
			// derivative is that piece's. Strict `<` with the tie going right, matching the `skip` row's
			// `takes_left` — and the two presence tests ahead of it are that row's skip, spelled so that
			// the `Cmp` is never reached over an operand it may not read.
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
			// no identity to claim, so the fold is the whole of it — and it goes through `eval`, which is
			// the operator's own law rather than a third copy of it.
			Ast::Square(e) => fold1(e.simplify(), Ast::Square),
			Ast::Abs(e) => fold1(e.simplify(), Ast::Abs),
			Ast::Sqrt(e) => fold1(e.simplify(), Ast::Sqrt),
			Ast::Exp(e) => fold1(e.simplify(), Ast::Exp),
			Ast::Min(l, r) => fold2(l.simplify(), r.simplify(), Ast::Min),
			Ast::Max(l, r) => fold2(l.simplify(), r.simplify(), Ast::Max),
			Ast::Cmp(l, r) => fold2(l.simplify(), r.simplify(), Ast::Cmp),
			// the fold that matters: a `Min`/`Max` against a literal threshold differentiates through
			// this, and folding it away is what leaves the derivative reading the one branch it had
			// before absence was expressible.
			Ast::IsNan(e) => fold1(e.simplify(), Ast::IsNan),
			Ast::Powi(e, n) => {
				let e = e.simplify();
				match (konst(&e), n) {
					(None, 0) => Ast::Const(1.0),
					(None, 1) => e,
					_ => fold1(e, |e| Ast::Powi(e, *n)),
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
