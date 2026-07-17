//! Compile-time step-graph for derived values.
//!
//! Each derived value is a [`Node`] whose `type Deps` names its upstream cells. [`step`]'s
//! [`Pull`] bound makes a wrong topological order (or a cycle) a compile error, and a full
//! graph sweep monomorphizes to one straight-line function — no dispatch, no runtime graph.
//!
//! # Structural rules
//!
//! - **Roots vs nodes.** Heavy stateful reducers (e.g. an order book) are *roots*: updated
//!   before the frame is seeded, entering it as `&'t State`. [`Node::advance`] cannot return
//!   borrows of its own state — the signature forbids it, deliberately. Nodes compute `Copy`
//!   values, including `Option<&'t T>` of *root*-borrowed data.
//! - **Multi-rate = `Option` outs.** A root/node that didn't fire this tick yields `None`;
//!   dependents short-circuit. This is the entire "advance layer if not empty" semantics, and
//!   equality early-cutoff for free.
//! - **`advance` fires only on events.** Time-windowed logic with no event flow (expiry,
//!   decay) needs a `Time` root cell seeded each tick; it fits the framework unchanged.
//! - **Node identity = its type.** Two instances of one node type in a frame make `Has`
//!   resolution ambiguous — a compile error. Distinguish via newtypes or const generics
//!   (`Rsi<14>` vs `Rsi<28>`).
//! - **Universe/cross-sectional composition.** Per-symbol graphs are values; a universe-level
//!   graph ticks at bar cadence, its roots seeded from per-symbol graphs' collected outputs.
//!   No cross-symbol type-level machinery.
//! - **Parallelism is across symbols (live) / episodes (backtest) only** — one graph per
//!   unit, rayon across. Never intra-tick.
//!
//! Impls that write concrete dep types hit E0195 (lifetime binder mismatch); use [`DepOuts`]
//! so every impl is uniformly `fn advance<'t>(&mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>`.
//!
//! Trait-solver ceiling: frame-depth cost is fine for dozens of nodes; revisit around ~50+
//! (a `graph!` macro is the fix, not more arities).
//!
//! ```
//! use dep_dag::{Cell, Cons, DepOuts, Nil, Node, step};
//!
//! struct Price;
//! impl Cell for Price {
//! 	type Out<'t> = f64;
//! }
//!
//! struct Double;
//! impl Cell for Double {
//! 	type Out<'t> = f64;
//! }
//! impl Node for Double {
//! 	type Deps = (Price,);
//! 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
//! 		p * 2.0
//! 	}
//! }
//!
//! struct PlusOne;
//! impl Cell for PlusOne {
//! 	type Out<'t> = f64;
//! }
//! impl Node for PlusOne {
//! 	type Deps = (Double,);
//! 	fn advance<'t>(&mut self, (d,): DepOuts<'t, Self>) -> Self::Out<'t> {
//! 		d + 1.0
//! 	}
//! }
//!
//! let f = Cons::<Price, Nil> { out: 21.0, tail: Nil };
//! let f = step(f, &mut Double);
//! let f = step(f, &mut PlusOne);
//! assert_eq!(f.head(), 43.0);
//! ```
#![no_std]

/// A value slot in the frame. `Out<'t>: Copy` — references are `Copy`, so heavy root state
/// enters the frame as `&'t State`, a first-class dependency.
pub trait Cell {
	type Out<'t>: Copy;
}

pub trait DepSet {
	type Outs<'t>;
}

/// Extracts a [`DepSet`]'s outputs from frame `F`. `I` is the inferred index path — never
/// named by callers.
pub trait Pull<'t, F, I>: DepSet {
	fn pull(f: &F) -> Self::Outs<'t>
	where
		F: 't;
}

pub trait Node: Cell {
	type Deps: DepSet;
	fn advance<'t>(&mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>;
}

/// Uniform binder-correct dep-tuple type for `advance` impls (concrete types there hit E0195).
pub type DepOuts<'t, N> = <<N as Node>::Deps as DepSet>::Outs<'t>;

pub struct Nil;

/// A frame: type-indexed cons-list of cell outputs. Fields are `pub` — apps seed root frames
/// directly.
pub struct Cons<'t, N: Cell, T> {
	pub out: N::Out<'t>,
	pub tail: T,
}

impl<'t, N: Cell, T> Cons<'t, N, T> {
	pub fn head(&self) -> N::Out<'t> {
		self.out
	}
}

pub enum Here {}
pub struct There<I>(core::marker::PhantomData<I>);

pub trait Has<'t, N: Cell, I> {
	fn get(&self) -> N::Out<'t>;
}
impl<'t, N: Cell, T> Has<'t, N, Here> for Cons<'t, N, T> {
	fn get(&self) -> N::Out<'t> {
		self.out
	}
}
impl<'t, N: Cell, M: Cell, T, I> Has<'t, N, There<I>> for Cons<'t, M, T>
where
	T: Has<'t, N, I>,
{
	fn get(&self) -> N::Out<'t> {
		self.tail.get()
	}
}

impl DepSet for () {
	type Outs<'t> = ();
}
impl<'t, F> Pull<'t, F, ()> for () {
	fn pull(_: &F) {}
}

macro_rules! impl_arity {
	($($T:ident $I:ident),+) => {
		impl<$($T: Cell),+> DepSet for ($($T,)+) {
			type Outs<'t> = ($($T::Out<'t>,)+);
		}
		impl<'t, F, $($T: Cell, $I),+> Pull<'t, F, ($($I,)+)> for ($($T,)+)
		where F: $(Has<'t, $T, $I> +)+ {
			fn pull(f: &F) -> Self::Outs<'t> where F: 't {
				($(Has::<'t, $T, $I>::get(f),)+)
			}
		}
	};
}
impl_arity!(A Ia);
impl_arity!(A Ia, B Ib);
impl_arity!(A Ia, B Ib, C Ic);
impl_arity!(A Ia, B Ib, C Ic, D Id);
impl_arity!(A Ia, B Ib, C Ic, D Id, E Ie);
impl_arity!(A Ia, B Ib, C Ic, D Id, E Ie, G Ig);
impl_arity!(A Ia, B Ib, C Ic, D Id, E Ie, G Ig, H Ih);
impl_arity!(A Ia, B Ib, C Ic, D Id, E Ie, G Ig, H Ih, J Ij);

/// Advances `node` over `frame` and pushes its output. The `Pull` bound is the engine's
/// reason to exist: a node stepped before its deps are in the frame does not compile.
///
/// ```compile_fail,E0277
/// use dep_dag::{Cell, DepOuts, Nil, Node, step};
///
/// struct A;
/// impl Cell for A {
/// 	type Out<'t> = f64;
/// }
/// impl Node for A {
/// 	type Deps = ();
/// 	fn advance<'t>(&mut self, _: DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		1.0
/// 	}
/// }
///
/// struct B;
/// impl Cell for B {
/// 	type Out<'t> = f64;
/// }
/// impl Node for B {
/// 	type Deps = (A,);
/// 	fn advance<'t>(&mut self, (a,): DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		a
/// 	}
/// }
///
/// // B stepped before its dep A is in the frame: no `Has<A>` — does not compile.
/// let f = step(Nil, &mut B);
/// let f = step(f, &mut A);
/// ```
pub fn step<'t, N, F, I>(frame: F, node: &mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I>,
	F: 't, {
	let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));
	Cons { out, tail: frame }
}
