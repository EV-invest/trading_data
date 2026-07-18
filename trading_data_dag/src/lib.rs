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
pub fn step<'t, N, F, I>(frame: F, node: &mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I>,
	F: 't, {
	let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));
	Cons { out, tail: frame }
}

/// One node firing, flattened: values and the finite-difference local Jacobian wrt its deps,
/// à la Jane Street's "Computations that differentiate, debug and document themselves".
pub struct Fire<'a> {
	pub debug: &'a dyn core::fmt::Debug,
	/// Compact one-liner for viz cards; `debug` stays the full-detail view (hover/tooltip).
	pub glance: &'a dyn Glance,
	pub dims: &'static [usize],
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
	fn on(&mut self, node: &'static str, deps: &'static [&'static str], fire: Fire<'_>);
}

impl Observer for () {
	const ACTIVE: bool = false;

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: Fire<'_>) {}
}

/// [`step`] + [`Observer::on`] before the push. The `()` observer erases to exactly `step` —
/// its `!ACTIVE` branch neither flattens nor calls `on` (a [`Fire`] without computed vals would
/// conflate "not computed" with "not fired").
///
/// Under an active observer, each fired node's Jacobian is finite-differenced: clone the
/// pre-advance node, [`Flat::nudge`] one dep element, re-advance the clone, diff. This is why
/// the bounds here require owned value outs — reference outs (`&'t Root`) can't nudge; graphs
/// carrying those use [`step`].
pub fn step_obs<'t, N, F, I, O: Observer>(frame: F, node: &mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	N::Out<'t>: Flat + core::fmt::Debug + Glance,
	F: 't, {
	if !O::ACTIVE {
		let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));
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
		Fire {
			debug: &out,
			glance: &out,
			dims: <N::Out<'t> as Flat>::DIMS,
			vals: fired.then_some(out_buf.as_slice()),
			dep_dims: <N::Deps as DepFlat>::DIMS,
			jac: jac.as_deref(),
		},
	);
	Cons { out, tail: frame }
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
		Fire {
			debug: &out,
			glance: &out,
			dims: <C::Out<'t> as Flat>::DIMS,
			vals: fired.then_some(buf.as_slice()),
			dep_dims: &[],
			jac: None,
		},
	);
}
