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
//! - **Gates.** A [`Gate`] outputs plain `bool`; nodes naming it in [`Node::When`] are not
//!   advanced at all while it is false — deps unpulled, out = [`Latent::latent`]. Laziness is
//!   transitive only by declaration: give the same `When` to every node that exclusively
//!   feeds gated nodes. A missed annotation is wasted work, never wrongness — except the
//!   all-consumers-behind-one-gate case, which [`graph!`] rejects at compile time.
//! - **Historic vs current.** Stateful derives (RSI, momentum) are *historic*: they must
//!   advance every tick to stay warm, so gating one is a compile error. Only nodes declaring
//!   [`Node::HISTORIC`]` = false` (*current*: skipping ticks is harmless) can be gated.
//! - **Latches.** A [`Latch`] is a [`Gate`] armed by an external event and cut from within:
//!   when its `Cut` node's out reads [`Episode::terminal`], [`graph!`] commutates it
//!   post-sweep and resets every node gated on it to `Default` — a declared one-tick
//!   back-edge (the synchronous-dataflow unit delay), never a `Deps` cycle.
//!
//! Impls that write concrete dep types hit E0195 (lifetime binder mismatch); use [`DepOuts`]
//! so every impl is uniformly `fn advance<'t>(&mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>`.
//!
//! Trait-solver ceiling: frame-depth cost is fine for dozens of nodes; revisit around ~50+
//! (a `graph!` macro is the fix, not more arities).
//REVIEW: `graph!` exists now (still fixed frame-depth per node; the ceiling note stands).
//!
//! ```
//! use trading_data_dag::{Cell, Cons, DepOuts, Nil, Node, step};
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
#![feature(associated_type_defaults)]
#![feature(const_type_name)]

extern crate alloc;

/// A value slot in the frame. `Out<'t>: Copy` — references are `Copy`, so heavy root state
/// enters the frame as `&'t State`, a first-class dependency.
pub trait Cell {
	type Out<'t>: Copy;
}

/// A cell output as a fixed-shape element array: the unit of observation and differentiation.
/// `DIMS` is the shape (`[]` scalar, `[n]` vector, `[r, c]` row-major, any rank).
pub trait Flat: Copy {
	const DIMS: &'static [usize];
	const LEN: usize = {
		let mut p = 1;
		let mut i = 0;
		while i < Self::DIMS.len() {
			p *= Self::DIMS[i];
			i += 1;
		}
		p
	};
	/// Writes all `LEN` slots of `out`; returns fired. `!fired` ⇒ NaN-filled.
	fn flat(&self, out: &mut [f64]) -> bool;
	/// Typed-space bump of one element. Discrete slots (bool, category) return `self` — an
	/// honest zero derivative — which is why no `unflat` inverse is ever needed.
	fn nudge(&self, slot: usize, h: f64) -> Self;
}

impl Flat for f64 {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = *self;
		true
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		debug_assert_eq!(slot, 0);
		self + h
	}
}

impl Flat for bool {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = u8::from(*self) as f64;
		true
	}

	fn nudge(&self, _: usize, _: f64) -> Self {
		*self
	}
}

impl<const N: usize> Flat for [f64; N] {
	const DIMS: &'static [usize] = &[N];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(self);
		true
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		let mut r = *self;
		r[slot] += h;
		r
	}
}

/// `Option` stays the multi-rate channel: `None` flattens to NaN + unfired.
impl<T: Flat> Flat for Option<T> {
	const DIMS: &'static [usize] = T::DIMS;

	fn flat(&self, out: &mut [f64]) -> bool {
		match self {
			Some(t) => t.flat(out),
			None => {
				out.fill(f64::NAN);
				false
			}
		}
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		self.map(|t| t.nudge(slot, h))
	}
}

/// The headline a human reads off a node at a glance — one compact line for the graph viz, the
/// display-dual of [`Flat`]'s numeric flattening. `Option::None` (off-cadence) renders `None`,
/// matching the observer's `fired` gate; scalars render themselves, domain structs their one
/// telling field (a bar's close, a print's qty).
pub trait Glance {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result;
}

impl Glance for f64 {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{self}")
	}
}

impl Glance for bool {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{self}")
	}
}

impl<T: Glance> Glance for Option<T> {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match self {
			Some(t) => t.glance(f),
			None => f.write_str("None"),
		}
	}
}

impl core::fmt::Display for dyn Glance + '_ {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		self.glance(f)
	}
}

/// l/c/a only — hue is the renderer's; a node can never claim one.
#[derive(Clone, Copy, Debug)]
pub struct Ink {
	pub l: f64,
	pub c: f64,
	pub a: f64,
}

impl Ink {
	pub const FAINT: Ink = Ink { l: 0.55, c: 0.03, a: 0.35 };
	pub const MAIN: Ink = Ink { l: 0.72, c: 0.13, a: 1.0 };
}

/// Constant horizontal guide line in the node's pane (e.g. RSI 30/70).
#[derive(Clone, Copy, Debug)]
pub struct Guide {
	pub label: &'static str,
	pub value: f64,
	pub ink: Ink,
}

/// Optional drawing hints a node declares about its own output — the renderer owns everything
/// else (hue above all). Defaults always suffice.
#[derive(Clone, Copy, Debug)]
pub struct Sketch {
	/// Fixed y-scale, e.g. RSI (0, 100).
	pub range: Option<(f64, f64)>,
	pub guides: &'static [Guide],
	/// Element names for vector outs; `[]` = indices.
	pub labels: &'static [&'static str],
	/// Per-element; `[]` = [`Ink::MAIN`] for all.
	pub inks: &'static [Ink],
}

impl Sketch {
	pub const DEFAULT: Sketch = Sketch {
		range: None,
		guides: &[],
		labels: &[],
		inks: &[],
	};
}

pub trait DepSet {
	type Outs<'t>;
	const NAMES: &'static [&'static str];
}

/// [`Flat`] over a whole dep tuple, elements concatenated in `Deps` order. Separate from
/// [`DepSet`] so `Pull`/[`step`] stay bound-free; per-dep columns recover via prefix sums of
/// `DIMS` products.
pub trait DepFlat: DepSet {
	const DIMS: &'static [&'static [usize]];
	const LEN: usize;
	fn flat(outs: &Self::Outs<'_>, dst: &mut [f64]);
	/// `slot` in concatenated element space; rebuilds the tuple replacing only the owning element.
	fn nudge<'t>(outs: &Self::Outs<'t>, slot: usize, h: f64) -> Self::Outs<'t>;
}

/// Extracts a [`DepSet`]'s outputs from frame `F`. `I` is the inferred index path — never
/// named by callers.
pub trait Pull<'t, F, I>: DepSet {
	fn pull(f: &F) -> Self::Outs<'t>
	where
		F: 't;
}

/// The "didn't run" value for gated nodes. Implemented for `Option` only, so gating a
/// non-`Option` node is a compile error — no dishonest zeros.
pub trait Latent: Copy {
	fn latent() -> Self;
}
impl<T: Copy> Latent for Option<T> {
	fn latent() -> Self {
		None
	}
}

/// The gates a node waits on: `()` or a small tuple of [`Gate`]s, all of which must be true
/// for its `advance` to run. Same arity ceiling note as deps.
pub trait GateSet {
	const NAMES: &'static [&'static str];
}
impl GateSet for () {
	const NAMES: &'static [&'static str] = &[];
}
impl<A: Gate> GateSet for (A,) {
	const NAMES: &'static [&'static str] = &[core::any::type_name::<A>()];
}
impl<A: Gate, B: Gate> GateSet for (A, B) {
	const NAMES: &'static [&'static str] = &[core::any::type_name::<A>(), core::any::type_name::<B>()];
}

pub trait Node: Cell {
	type Deps: DepSet;
	type When: GateSet = ();
	/// Historic nodes must advance every tick to stay warm; only current (`false`) nodes can
	/// be gated — a gated historic node is a compile error:
	///
	/// ```compile_fail,E0080
	/// use trading_data_dag::{Cell, Cons, DepOuts, Gate, Nil, Node, step};
	///
	/// struct Price;
	/// impl Cell for Price {
	/// 	type Out<'t> = f64;
	/// }
	///
	/// struct Hot;
	/// impl Cell for Hot {
	/// 	type Out<'t> = bool;
	/// }
	/// impl Node for Hot {
	/// 	type Deps = (Price,);
	/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
	/// 		p > 10.0
	/// 	}
	/// }
	/// impl Gate for Hot {}
	///
	/// struct Expensive;
	/// impl Cell for Expensive {
	/// 	type Out<'t> = Option<f64>;
	/// }
	/// impl Node for Expensive {
	/// 	type Deps = (Price,);
	/// 	type When = (Hot,);
	/// 	// HISTORIC left at its default `true`: does not compile.
	/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
	/// 		Some(p * 100.0)
	/// 	}
	/// }
	///
	/// let f = Cons::<Price, Nil> { out: 3.0, tail: Nil };
	/// let f = step(f, &mut Hot);
	/// let f = step(f, &mut Expensive);
	/// ```
	const HISTORIC: bool = true;
	const SKETCH: Sketch = Sketch::DEFAULT;
	fn advance<'t>(&mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>;
}

/// A binary control signal. Nodes naming it in [`Node::When`] are not advanced while it is
/// false: deps not pulled, no work done, out = [`Latent::latent`].
///
/// ```
/// use trading_data_dag::{Cell, Cons, DepOuts, Gate, Nil, Node, step};
///
/// struct Price;
/// impl Cell for Price {
/// 	type Out<'t> = f64;
/// }
///
/// struct Hot;
/// impl Cell for Hot {
/// 	type Out<'t> = bool;
/// }
/// impl Node for Hot {
/// 	type Deps = (Price,);
/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		p > 10.0
/// 	}
/// }
/// impl Gate for Hot {}
///
/// struct Expensive;
/// impl Cell for Expensive {
/// 	type Out<'t> = Option<f64>;
/// }
/// impl Node for Expensive {
/// 	type Deps = (Price,);
/// 	type When = (Hot,);
/// 	const HISTORIC: bool = false;
/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		Some(p * 100.0)
/// 	}
/// }
///
/// let f = Cons::<Price, Nil> { out: 3.0, tail: Nil };
/// let f = step(f, &mut Hot);
/// let f = step(f, &mut Expensive);
/// assert_eq!(f.head(), None); // gate closed: advance never ran
/// ```
pub trait Gate: Node
where
	for<'t> Self: Cell<Out<'t> = bool>, {
}

/// Episode lifecycle on an out value; the initial state is the node's `Default`.
pub trait Episode: Copy {
	fn terminal(&self) -> bool;
}

/// `None` (latent / off-cadence) is never terminal.
impl<T: Episode> Episode for Option<T> {
	fn terminal(&self) -> bool {
		match self {
			Some(t) => t.terminal(),
			None => false,
		}
	}
}

/// A [`Gate`] armed from outside and cut from within — the SCR/thyristor: an external event
/// (its `Deps`) sets it, conduction latches in its own state, and it turns off by natural
/// commutation when the episode it gates reaches a [`Episode::terminal`] out. No second
/// external signal ever closes it.
///
/// `Cut` is read *post-sweep* by [`graph!`]: tick T the episode publishes its terminal out,
/// the latch commutates after the sweep, tick T+1 the gated subtree is latent via the
/// ordinary [`Drive`] skip and every node gated on the latch is reset to `Default` for a
/// fresh episode. A trigger arriving during a live episode — including its terminal tick —
/// is absorbed and lost to commutation: one episode at a time.
pub trait Latch: Gate
where
	for<'t> Self: Cell<Out<'t> = bool>,
	for<'t> <Self::Cut as Cell>::Out<'t>: Episode, {
	/// The gated node whose terminal out commutates this latch — a declared one-tick
	/// back-edge, not a `Deps` cycle.
	type Cut: Cell;
	fn commutate(&mut self);
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

	const NAMES: &'static [&'static str] = &[];
}
impl<'t, F> Pull<'t, F, ()> for () {
	fn pull(_: &F) {}
}
impl DepFlat for () {
	const DIMS: &'static [&'static [usize]] = &[];
	const LEN: usize = 0;

	fn flat(_: &Self::Outs<'_>, dst: &mut [f64]) {
		debug_assert!(dst.is_empty());
	}

	fn nudge<'t>(_: &Self::Outs<'t>, _: usize, _: f64) -> Self::Outs<'t> {}
}

macro_rules! impl_arity {
	($($T:ident $I:ident $v:ident),+) => {
		impl<$($T: Cell),+> DepSet for ($($T,)+) {
			type Outs<'t> = ($($T::Out<'t>,)+);

			const NAMES: &'static [&'static str] = &[$(core::any::type_name::<$T>()),+];
		}
		impl<'t, F, $($T: Cell, $I),+> Pull<'t, F, ($($I,)+)> for ($($T,)+)
		where F: $(Has<'t, $T, $I> +)+ {
			fn pull(f: &F) -> Self::Outs<'t> where F: 't {
				($(Has::<'t, $T, $I>::get(f),)+)
			}
		}
		impl<$($T: Cell),+> DepFlat for ($($T,)+)
		where $(for<'x> <$T as Cell>::Out<'x>: Flat),+ {
			const DIMS: &'static [&'static [usize]] = &[$(<<$T as Cell>::Out<'static> as Flat>::DIMS),+];
			const LEN: usize = 0 $(+ <<$T as Cell>::Out<'static> as Flat>::LEN)+;

			fn flat(outs: &Self::Outs<'_>, dst: &mut [f64]) {
				assert_eq!(dst.len(), Self::LEN);
				let ($($v,)+) = outs;
				let mut off = 0;
				$(
					$v.flat(&mut dst[off..off + <<$T as Cell>::Out<'static> as Flat>::LEN]);
					off += <<$T as Cell>::Out<'static> as Flat>::LEN;
				)+
				debug_assert_eq!(off, Self::LEN);
			}

			fn nudge<'t>(outs: &Self::Outs<'t>, slot: usize, h: f64) -> Self::Outs<'t> {
				let ($($v,)+) = outs;
				let mut off = 0;
				let nudged = ($(
					{
						let len = <<$T as Cell>::Out<'static> as Flat>::LEN;
						let r = if (off..off + len).contains(&slot) { $v.nudge(slot - off, h) } else { *$v };
						off += len;
						r
					},
				)+);
				debug_assert_eq!(off, Self::LEN);
				nudged
			}
		}
	};
}
impl_arity!(A Ia a);
impl_arity!(A Ia a, B Ib b);
impl_arity!(A Ia a, B Ib b, C Ic c);
impl_arity!(A Ia a, B Ib b, C Ic c, D Id d);
impl_arity!(A Ia a, B Ib b, C Ic c, D Id d, E Ie e);
impl_arity!(A Ia a, B Ib b, C Ic c, D Id d, E Ie e, G Ig g);
impl_arity!(A Ia a, B Ib b, C Ic c, D Id d, E Ie e, G Ig g, H Ih h);
impl_arity!(A Ia a, B Ib b, C Ic c, D Id d, E Ie e, G Ig g, H Ih h, J Ij j);

/// [`step`]'s evaluation dispatch, keyed on the node's [`Node::When`]: ungated nodes advance
/// unconditionally; gated nodes advance only while every gate reads true in the frame,
/// yielding [`Latent::latent`] otherwise — deps not pulled. The `Has` bound on gate impls
/// keeps the ordering guarantee: a node stepped before its gate does not compile, same story
/// as [`Pull`]. `I`/`J` are inferred index paths — never named by callers.
///
/// ```compile_fail,E0277
/// use trading_data_dag::{Cell, Cons, DepOuts, Gate, Nil, Node, step};
///
/// struct Price;
/// impl Cell for Price {
/// 	type Out<'t> = f64;
/// }
///
/// struct Hot;
/// impl Cell for Hot {
/// 	type Out<'t> = bool;
/// }
/// impl Node for Hot {
/// 	type Deps = (Price,);
/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		p > 10.0
/// 	}
/// }
/// impl Gate for Hot {}
///
/// struct Expensive;
/// impl Cell for Expensive {
/// 	type Out<'t> = Option<f64>;
/// }
/// impl Node for Expensive {
/// 	type Deps = (Price,);
/// 	type When = (Hot,);
/// 	const HISTORIC: bool = false;
/// 	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
/// 		Some(p * 100.0)
/// 	}
/// }
///
/// // gated node stepped before its gate is in the frame: no `Has<Hot>` — does not compile.
/// let f = Cons::<Price, Nil> { out: 3.0, tail: Nil };
/// let f = step(f, &mut Expensive);
/// let f = step(f, &mut Hot);
/// ```
pub trait Drive<'t, N: Node, F, I, J>: GateSet {
	fn open(f: &F) -> bool
	where
		F: 't;
	fn drive(node: &mut N, f: &F) -> N::Out<'t>
	where
		F: 't;
}

impl<'t, N, F, I> Drive<'t, N, F, I, ()> for ()
where
	N: Node<When = ()>,
	N::Deps: Pull<'t, F, I>,
{
	fn open(_: &F) -> bool
	where
		F: 't, {
		true
	}

	fn drive(node: &mut N, f: &F) -> N::Out<'t>
	where
		F: 't, {
		node.advance(<N::Deps as Pull<'t, F, I>>::pull(f))
	}
}

impl<'t, N, F, I, A, Ia> Drive<'t, N, F, I, (Ia,)> for (A,)
where
	A: Gate,
	N: Node<When = (A,)>,
	N::Deps: Pull<'t, F, I>,
	F: Has<'t, A, Ia>,
	for<'x> N::Out<'x>: Latent,
{
	fn open(f: &F) -> bool
	where
		F: 't, {
		Has::<'t, A, Ia>::get(f)
	}

	fn drive(node: &mut N, f: &F) -> N::Out<'t>
	where
		F: 't, {
		const { assert!(!N::HISTORIC, "historic node cannot be gated; declare `const HISTORIC: bool = false` or drop `When`") }
		if !<Self as Drive<'t, N, F, I, (Ia,)>>::open(f) {
			return Latent::latent();
		}
		node.advance(<N::Deps as Pull<'t, F, I>>::pull(f))
	}
}

impl<'t, N, F, I, A, Ia, B, Ib> Drive<'t, N, F, I, (Ia, Ib)> for (A, B)
where
	A: Gate,
	B: Gate,
	N: Node<When = (A, B)>,
	N::Deps: Pull<'t, F, I>,
	F: Has<'t, A, Ia> + Has<'t, B, Ib>,
	for<'x> N::Out<'x>: Latent,
{
	fn open(f: &F) -> bool
	where
		F: 't, {
		Has::<'t, A, Ia>::get(f) && Has::<'t, B, Ib>::get(f)
	}

	fn drive(node: &mut N, f: &F) -> N::Out<'t>
	where
		F: 't, {
		const { assert!(!N::HISTORIC, "historic node cannot be gated; declare `const HISTORIC: bool = false` or drop `When`") }
		if !<Self as Drive<'t, N, F, I, (Ia, Ib)>>::open(f) {
			return Latent::latent();
		}
		node.advance(<N::Deps as Pull<'t, F, I>>::pull(f))
	}
}

/// Advances `node` over `frame` and pushes its output. The `Pull` bound is the engine's
/// reason to exist: a node stepped before its deps are in the frame does not compile.
///
/// ```compile_fail,E0277
/// use trading_data_dag::{Cell, DepOuts, Nil, Node, step};
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
pub fn step<'t, N, F, I, J>(frame: F, node: &mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::When: Drive<'t, N, F, I, J>,
	F: 't, {
	let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
	Cons { out, tail: frame }
}

/// One node firing, flattened: values and the finite-difference local Jacobian wrt its deps,
/// à la Jane Street's "Computations that differentiate, debug and document themselves".
pub struct Fire<'a> {
	pub debug: &'a dyn core::fmt::Debug,
	/// Compact one-liner for viz cards; `debug` stays the full-detail view (hover/tooltip).
	pub glance: &'a dyn Glance,
	pub dims: &'static [usize],
	pub sketch: &'static Sketch,
	/// `None` = didn't fire.
	pub vals: Option<&'a [f64]>,
	pub dep_dims: &'a [&'static [usize]],
	/// Row-major `out_len × sum(dep lens)`, deps concatenated in `Deps` order. NaN = no signal
	/// (dep unfired / bump crossed a firing branch). `None` when the node didn't fire.
	pub jac: Option<&'a [f64]>,
}

/// Sees every [`step_obs`] as it happens: one interpretation choke point, many interpretations
/// (eval is `step`; debug-tree/replay is an impl of this). Step order IS topo order, so the
/// observed sequence doubles as the graph's static topology; dep names never seen as stepped
/// nodes are roots — apps seed root activations via [`observe_root`].
///
/// `node`/`deps` strings come from [`core::any::type_name`]: build-local, display-only — never
/// persist them.
pub trait Observer {
	/// Gates all flattening/FD work in [`step_obs`]; monomorphized away when `false`.
	const ACTIVE: bool = true;
	/// `gates` are the node's [`Node::When`] control edges (empty for ungated nodes and roots).
	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [&'static str], fire: Fire<'_>);
}

impl Observer for () {
	const ACTIVE: bool = false;

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [&'static str], _: Fire<'_>) {}
}

/// [`step`] + [`Observer::on`] before the push. The `()` observer erases to exactly `step` —
/// its `!ACTIVE` branch neither flattens nor calls `on` (a [`Fire`] without computed vals would
/// conflate "not computed" with "not fired").
///
/// Under an active observer, each fired node's Jacobian is finite-differenced: clone the
/// pre-advance node, [`Flat::nudge`] one dep element, re-advance the clone, diff. This is why
/// the bounds here require owned value outs — reference outs (`&'t Root`) can't nudge; graphs
/// carrying those use [`step`].
pub fn step_obs<'t, N, F, I, J, O: Observer>(frame: F, node: &mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::When: Drive<'t, N, F, I, J>,
	N::Deps: Pull<'t, F, I> + DepFlat,
	N::Out<'t>: Flat + core::fmt::Debug + Glance,
	F: 't, {
	if !O::ACTIVE {
		let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
		return Cons { out, tail: frame };
	}

	// gate closed: no advance, no dep flatten, no FD — an unfired `Fire` is the honest view.
	if !<N::When as Drive<'t, N, F, I, J>>::open(&frame) {
		let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
		obs.on(
			core::any::type_name::<N>(),
			<N::Deps as DepSet>::NAMES,
			<N::When as GateSet>::NAMES,
			Fire {
				debug: &out,
				glance: &out,
				dims: <N::Out<'t> as Flat>::DIMS,
				sketch: &N::SKETCH,
				vals: None,
				dep_dims: <N::Deps as DepFlat>::DIMS,
				jac: None,
			},
		);
		return Cons { out, tail: frame };
	}

	let pre = node.clone();
	let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));

	let out_len = <N::Out<'t> as Flat>::LEN;
	let mut out_buf = alloc::vec![f64::NAN; out_len];
	let fired = out.flat(&mut out_buf);

	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	let dep_len = <N::Deps as DepFlat>::LEN;
	let mut dep_buf = alloc::vec![f64::NAN; dep_len];
	<N::Deps as DepFlat>::flat(&deps, &mut dep_buf);

	let jac = fired.then(|| {
		let mut jac = alloc::vec![f64::NAN; out_len * dep_len];
		let mut bumped = alloc::vec![f64::NAN; out_len];
		for slot in 0..dep_len {
			let x = dep_buf[slot];
			if x.is_nan() {
				continue;
			}
			let h = (x.abs() * 1e-6).max(1e-9);
			let mut clone = pre.clone();
			let bout = clone.advance(<N::Deps as DepFlat>::nudge(&deps, slot, h));
			if !bout.flat(&mut bumped) {
				continue; // bump crossed a firing branch — column stays NaN
			}
			for i in 0..out_len {
				jac[i * dep_len + slot] = (bumped[i] - out_buf[i]) / h;
			}
		}
		jac
	});

	obs.on(
		core::any::type_name::<N>(),
		<N::Deps as DepSet>::NAMES,
		<N::When as GateSet>::NAMES,
		Fire {
			debug: &out,
			glance: &out,
			dims: <N::Out<'t> as Flat>::DIMS,
			sketch: &N::SKETCH,
			vals: fired.then_some(out_buf.as_slice()),
			dep_dims: <N::Deps as DepFlat>::DIMS,
			jac: jac.as_deref(),
		},
	);
	Cons { out, tail: frame }
}

/// Replay events carry their own clock.
pub trait Stamped {
	fn ts_ns(&self) -> i64;
}

/// A steppable event-graph: the whole surface a replayer/visualizer needs. [`graph!`] generates
/// impls; the richer typed out-struct stays on the inherent methods.
pub trait Dag: Default {
	type Event: Copy + Stamped;
	fn tick_obs(&mut self, ev: Option<Self::Event>, obs: &mut impl Observer);
}

/// `const_type_name` is feature-gated at the call site; this wrapper keeps [`graph!`] users
/// off nightly feature attrs.
#[doc(hidden)]
pub const fn node_name<T>() -> &'static str {
	core::any::type_name::<T>()
}

/// One node's compile-time shape, as [`graph!`] sees it. `name`/`deps`/`gates` are
/// [`core::any::type_name`] strings: const-comparable, never persisted.
#[doc(hidden)]
pub struct NodeMeta {
	pub name: &'static str,
	pub deps: &'static [&'static str],
	pub historic: bool,
	pub gates: &'static [&'static str],
}

const fn str_eq(a: &str, b: &str) -> bool {
	let (a, b) = (a.as_bytes(), b.as_bytes());
	if a.len() != b.len() {
		return false;
	}
	let mut i = 0;
	while i < a.len() {
		if a[i] != b[i] {
			return false;
		}
		i += 1;
	}
	true
}

#[doc(hidden)]
pub const fn contains(set: &[&str], name: &str) -> bool {
	let mut i = 0;
	while i < set.len() {
		if str_eq(set[i], name) {
			return true;
		}
		i += 1;
	}
	false
}

/// [`graph!`]'s completeness check: true when `name` is current, ungated, has in-graph
/// consumers, and all of them sit behind one common gate (other than `name` itself) — sampling
/// it while that gate is closed is pure waste, so it must be gated too or marked historic.
/// Leaves (no in-graph consumers) are graph outputs — exempt.
#[doc(hidden)]
pub const fn shadowed(name: &'static str, nodes: &[NodeMeta]) -> bool {
	let mut me = 0;
	while me < nodes.len() && !str_eq(nodes[me].name, name) {
		me += 1;
	}
	assert!(me < nodes.len(), "shadowed: name must be one of nodes");
	if nodes[me].historic || !nodes[me].gates.is_empty() {
		return false;
	}
	let mut fc = 0;
	while fc < nodes.len() && !contains(nodes[fc].deps, name) {
		fc += 1;
	}
	if fc == nodes.len() {
		return false;
	}
	// a common gate must appear on the first consumer; try each of its gates against the rest
	let mut g = 0;
	while g < nodes[fc].gates.len() {
		let gate = nodes[fc].gates[g];
		if !str_eq(gate, name) {
			let mut all = true;
			let mut j = fc + 1;
			while j < nodes.len() {
				if contains(nodes[j].deps, name) && !contains(nodes[j].gates, gate) {
					all = false;
					break;
				}
				j += 1;
			}
			if all {
				return true;
			}
		}
		g += 1;
	}
	false
}

/// Wires a declared node list into a graph struct + typed out-struct + [`Dag`] impl. Fields in
/// topo order — a wrong order fails the existing `Pull`/`Has` bounds at compile time. The root
/// cell's `Out` must be `Option<Event>`. Out-struct fields keep each node's `Cell::Out`
/// verbatim: Option-ness IS the multi-rate non-firing channel.
///
/// An optional `latch { field: Type, ... }` group names [`Latch`] fields (which also appear
/// in the node list, at their topo position). Post-sweep, a latch whose `Cut` out reads
/// [`Episode::terminal`] is commutated and every field gated on it reset to `Default` — the
/// out-struct still carries the terminal tick's values.
#[macro_export]
macro_rules! graph {
	// cross-product (each latch × every field) exceeds macro_rules' lockstep repetition rule;
	// this tt-muncher peels one latch per step, re-passing the full field list.
	(@commutate $self:ident, $f:ident, [] [$($field:ident: $Node:ty),*]) => {};
	(@commutate $self:ident, $f:ident,
		[$lfield:ident: $Latch:ty $(, $lrest:ident: $LRest:ty)*]
		[$($field:ident: $Node:ty),*]
	) => {
		if $crate::Episode::terminal(&$crate::Has::<<$Latch as $crate::Latch>::Cut, _>::get(&$f)) {
			<$Latch as $crate::Latch>::commutate(&mut $self.$lfield);
			$(
				if const {
					$crate::contains(<<$Node as $crate::Node>::When as $crate::GateSet>::NAMES, $crate::node_name::<$Latch>())
				} {
					$self.$field = ::core::default::Default::default();
				}
			)*
		}
		$crate::graph!(@commutate $self, $f, [$($lrest: $LRest),*] [$($field: $Node),*]);
	};
	(
		$vis:vis struct $Graph:ident;
		root $Root:ty, event $Event:ty;
		out $Out:ident;
		$($field:ident: $Node:ty),+ $(,)?
	) => {
		$crate::graph! {
			$vis struct $Graph;
			root $Root, event $Event;
			out $Out;
			latch {}
			$($field: $Node),+
		}
	};
	(
		$vis:vis struct $Graph:ident;
		root $Root:ty, event $Event:ty;
		out $Out:ident;
		latch { $($lfield:ident: $Latch:ty),* $(,)? }
		$($field:ident: $Node:ty),+ $(,)?
	) => {
		#[derive(Default)]
		$vis struct $Graph {
			$($field: $Node,)+
		}

		const _: () = {
			const METAS: &[$crate::NodeMeta] = &[$(
				$crate::NodeMeta {
					name: $crate::node_name::<$Node>(),
					deps: <<$Node as $crate::Node>::Deps as $crate::DepSet>::NAMES,
					historic: <$Node as $crate::Node>::HISTORIC,
					gates: <<$Node as $crate::Node>::When as $crate::GateSet>::NAMES,
				},
			)+];
			$(assert!(
				!$crate::shadowed($crate::node_name::<$Node>(), METAS),
				concat!(stringify!($Node), " is only consumed under a gate: gate it too, or mark it historic")
			);)+
		};

		#[derive(Clone, Copy, Debug)]
		$vis struct $Out {
			$(pub $field: <$Node as $crate::Cell>::Out<'static>,)+
		}

		impl $Graph {
			$vis fn tick(&mut self, ev: Option<$Event>) -> $Out {
				self.tick_obs(ev, &mut ())
			}

			$vis fn tick_obs(&mut self, ev: Option<$Event>, obs: &mut impl $crate::Observer) -> $Out {
				$crate::observe_root::<$Root, _>(ev, obs);
				let f = $crate::Cons::<$Root, $crate::Nil> { out: ev, tail: $crate::Nil };
				$(let f = $crate::step_obs(f, &mut self.$field, obs);)+
				$crate::graph!(@commutate self, f, [$($lfield: $Latch),*] [$($field: $Node),*]);
				$Out {
					$($field: $crate::Has::<$Node, _>::get(&f),)+
				}
			}
		}

		impl $crate::Dag for $Graph {
			type Event = $Event;

			fn tick_obs(&mut self, ev: Option<Self::Event>, obs: &mut impl $crate::Observer) {
				// inherent method shadows the trait one; typed out dropped.
				Self::tick_obs(self, ev, obs);
			}
		}
	};
}

/// The root half of the observation choke point: flatten a seeded root value and emit its
/// [`Fire`] (no deps, no jac). No-op under an inactive observer.
pub fn observe_root<'t, C, O>(out: C::Out<'t>, obs: &mut O)
where
	C: Cell,
	C::Out<'t>: Flat + core::fmt::Debug + Glance,
	O: Observer, {
	if !O::ACTIVE {
		return;
	}
	let mut buf = alloc::vec![f64::NAN; <C::Out<'t> as Flat>::LEN];
	let fired = out.flat(&mut buf);
	obs.on(
		core::any::type_name::<C>(),
		&[],
		&[],
		Fire {
			debug: &out,
			glance: &out,
			dims: <C::Out<'t> as Flat>::DIMS,
			sketch: &Sketch::DEFAULT,
			vals: fired.then_some(buf.as_slice()),
			dep_dims: &[],
			jac: None,
		},
	);
}
