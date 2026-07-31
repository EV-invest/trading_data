//! Compile-time step-graph for derived values, batch-native.
//!
//! Each derived value is a [`Node`] whose `type Deps` names its upstream cells. [`step`]'s
//! [`Pull`] bound makes a wrong topological order (or a cycle) a compile error, and a full
//! graph sweep monomorphizes to one straight-line function — no dispatch, no runtime graph.
//!
//! # Batches, not events
//!
//! A router slices the merged timeline into runs of same-type events; the graph consumes and
//! produces *batches* natively. An event-emitting node holds a `Vec<T>` buffer, clears it as
//! [`Node::advance`]'s first act, appends its emissions, and returns `&self.buf` — its
//! `Cell::Out<'t>` is `&'t [T]`. Because `advance<'t>(&'t mut self, ..)` lends the node for the
//! whole tick, the frame transitively holds those borrows; the "nodes are Copy values" doctrine
//! is dead. Level (state-view) nodes may still return plain `Copy` values.
//!
//! **Rate is slice length, firing is element `Option`-ness.** A rate-*preserving* node emits
//! exactly one element per element of its driving dep, `Option`-valued where it can decline
//! (warmup) — so same-rate deps zip by index with a `assert_eq!` on len, warmup included. A
//! rate-*changing* node (trades→bars) emits one non-optional element per own event. Cross-rate
//! reads take the level view with `.last()`.
//!
//! # Structural rules
//!
//! - **Roots vs nodes.** Roots are the router's slices, entering each frame as `&'t [Event]`
//!   (see [`graph!`]'s `roots { .. }` group). A node's dep tree — computed first, in isolation —
//!   decides which roots are *required* ([`graph!`] exposes `required_events()`).
//! - **Node identity = its type.** Two instances of one node type in a frame make [`Has`]
//!   resolution ambiguous — a compile error. Distinguish via newtypes or const generics.
//! - **Gates operate on scalar cells only.** A [`Gate`] outputs plain `bool`; nodes naming it in
//!   [`Node::When`] are not advanced while it is false. Batch-out nodes cannot be gates or gated
//!   (tick-level gating on a batch is lossy, and a self-borrowing batch out can't be reset
//!   post-sweep) — this is a load-bearing invariant, see [`graph!`].
//! - **Horizon.** Every node declares how far back its own state reaches ([`Node::HORIZON`]), and
//!   every buffered dep how far back it reads ([`Buffering`]). A [`Horizon::Unbounded`] node — a
//!   recurrence (Wilder RSI) or a running sum (CVD) — must advance every tick to stay warm, so
//!   gating one is a compile error; a bounded horizon is state its inputs can reconstitute.
//! - **Latches.** A [`Latch`] is a [`Gate`] armed externally and cut from within: when its `Cut`
//!   node's out reads [`Episode::terminal`], [`graph!`] commutates it and resets every node gated
//!   on it to `Default` — deferred to the *next* tick's start (the frame still borrows batch
//!   fields at end-of-tick, so the reset can't run in place).
//!
//! Impls that write concrete dep types hit E0195 (lifetime binder mismatch); use [`DepOuts`]
//! so every impl is uniformly `fn advance<'t>(&'t mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>`.
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
//! 	fn advance<'t>(&'t mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
//! 		p * 2.0
//! 	}
//! }
//!
//! let mut double = Double;
//! let f = Cons::<Price, Nil> { out: 21.0, tail: Nil };
//! let f = step(f, &mut double);
//! assert_eq!(f.head(), 42.0);
//! ```
#![no_std]
#![feature(adt_const_params)]
#![feature(associated_type_defaults)]
#![feature(const_type_name)]

extern crate alloc;

use core::any::TypeId;

mod expr;
pub use expr::{Abs, Add, Ast, Const, Div, Ex, Expr, Mul, Neg, Square, Sub, Sum, Trace, Var, Vars, abs, constant, square, sum};

/// How far back something reaches: a node's own state ([`Node::HORIZON`]), or a dep's retained
/// history ([`Buffering`]). One vocabulary for both, so the reach a node reads and the reach it
/// holds are stated the same way — and a `const` of it drops straight into const-generic position.
#[derive(Clone, core::marker::ConstParamTy, Copy, Debug, Eq, PartialEq)]
pub enum Horizon {
	/// The current value only — no history at all.
	Unit,
	Elems(usize),
	/// Wall-clock milliseconds — `Timeframe`'s own unit, so `Timeframe::from_naive(..).0` is the
	/// literal you write.
	Span(u64),
	/// Reaches to the start of the run: a recurrence (Wilder RSI) or a running sum (CVD). Nothing
	/// recovers such a node, so it must advance every tick.
	Unbounded,
}

impl Horizon {
	/// Whether history retained at `self` serves a read at `req`. A span serves any count — what it
	/// dropped is strictly older than anything it kept, so `n` elements ending at a retained one are
	/// either all present or the read declines. A count cannot promise a span.
	pub const fn serves(self, req: Horizon) -> bool {
		match (self, req) {
			(Horizon::Elems(k), Horizon::Elems(j)) => k >= j,
			(Horizon::Span(k), Horizon::Span(j)) => k >= j,
			(Horizon::Span(_), Horizon::Elems(_)) => true,
			_ => false,
		}
	}

	/// Only a [`Horizon::Span`] has one; the caller has already matched the variant.
	const fn ns(ms: u64) -> i64 {
		(ms * 1_000_000) as i64
	}
}

/// A retained item's own event time. Required of every [`Buffer`]ed item — a history you cannot
/// index by time is one you can only read at an assumed cadence, which is the bug [`Horizon::Span`]
/// replaces.
pub trait Stamped {
	fn ts_ns(&self) -> i64;
}

/// A value slot in the frame. `Out<'t>: Copy` — references are `Copy`, so a batch out enters the
/// frame as `&'t [T]` and heavy root/node state is lent as `&'t State`.
pub trait Cell {
	type Out<'t>: Copy;

	/// What this cell is called wherever a human reads the graph — cards, edges, `step_until`.
	/// Defaults to the Rust path, which is right for a type whose *identity* is its name. A cell
	/// parameterised by a bare number overrides it: `Bars<1>` leaves the reader to guess the unit,
	/// where `Bar:1m` states it.
	const NAME: &'static str = core::any::type_name::<Self>();
}

/// A cell output as a fixed-shape element array: the unit of observation and differentiation.
/// `DIMS` is the shape (`[]` scalar, `[n]` vector, `[r, c]` row-major, any rank). A batch out
/// (`&[T]`) flattens to its *last* element — the observer sees end-of-batch.
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
	/// How many elements this out fired: scalars/arrays 1, `Option` 0/1, `&[T]` its len.
	fn fires(&self) -> usize {
		1
	}
}

/// Typed-space perturbation of one flattened element, for the finite-difference witness. Returns
/// the perturbation **actually applied**, in the element's own units — a quantized column can only
/// move in whole ticks, and pretending otherwise divides the difference by a step never taken.
/// `0.0` ⇒ this slot cannot be perturbed (discrete, structural), so its Jacobian column stays NaN
/// rather than a fabricated zero.
pub trait Bump: Copy {
	fn bump(self, slot: usize, h: f64) -> (Self, f64);
}

impl Flat for f64 {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = *self;
		true
	}
}

impl Bump for f64 {
	fn bump(self, slot: usize, h: f64) -> (Self, f64) {
		debug_assert_eq!(slot, 0);
		(self + h, h)
	}
}

impl Flat for bool {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = u8::from(*self) as f64;
		true
	}
}

impl Bump for bool {
	fn bump(self, _: usize, _: f64) -> (Self, f64) {
		(self, 0.0)
	}
}

impl<const N: usize> Flat for [f64; N] {
	const DIMS: &'static [usize] = &[N];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(self);
		true
	}
}

impl<const N: usize> Bump for [f64; N] {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		self[slot] += h;
		(self, h)
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

	fn fires(&self) -> usize {
		self.is_some() as usize
	}
}

impl<T: Bump> Bump for Option<T> {
	fn bump(self, slot: usize, h: f64) -> (Self, f64) {
		match self {
			Some(t) => {
				let (t, dh) = t.bump(slot, h);
				(Some(t), dh)
			}
			None => (None, 0.0),
		}
	}
}

/// A batch out flattens to its *last* element (empty ⇒ NaN + unfired); its rate is its len.
impl<T: Flat> Flat for &[T] {
	const DIMS: &'static [usize] = T::DIMS;

	fn flat(&self, out: &mut [f64]) -> bool {
		match self.last() {
			Some(t) => t.flat(out),
			None => {
				out.fill(f64::NAN);
				false
			}
		}
	}

	fn fires(&self) -> usize {
		self.len()
	}
}

/// The headline a human reads off a node at a glance — one compact line for the graph viz, the
/// display-dual of [`Flat`]'s numeric flattening. A batch renders its *last* element (empty
/// ⇒ `[]`), matching the observer's end-of-batch view.
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

impl<T: Glance> Glance for &[T] {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match self.last() {
			Some(t) => t.glance(f),
			None => f.write_str("[]"),
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

/// Optional drawing hints a node declares about one group of its [`Flat`] slots — the renderer owns
/// everything else (hue above all). Defaults always suffice.
///
/// A node declares a list of these because scale, not authorship, is what shares an axis: an out
/// carrying both a quantity and a price is two plots, and splitting it into two *nodes* to say so
/// would put a step in the topology that computes nothing.
#[derive(Clone, Copy, Debug)]
pub struct Plot {
	/// Which [`Flat`] slots of the out this plot draws; `[]` = all of them, which only reads
	/// unambiguously when the node declares a single plot.
	pub slots: &'static [usize],
	/// Fixed y-scale, e.g. RSI (0, 100).
	pub range: Option<(f64, f64)>,
	pub guides: &'static [Guide],
	/// Names for `slots`, positionally; `[]` = indices.
	pub labels: &'static [&'static str],
	/// Per-element; `[]` = [`Ink::MAIN`] for all.
	pub inks: &'static [Ink],
	/// Draw on the price pane instead of an own indicator pane; price-denominated.
	pub overlay: bool,
}

impl Plot {
	pub const DEFAULT: Plot = Plot {
		slots: &[],
		range: None,
		guides: &[],
		labels: &[],
		inks: &[],
		overlay: false,
	};

	/// `[]` slots means "every slot", which two plots cannot both claim.
	const fn coherent(plots: &'static [Plot]) -> bool {
		let mut i = 0;
		while i < plots.len() {
			if plots[i].slots.is_empty() && plots.len() > 1 {
				return false;
			}
			i += 1;
		}
		true
	}
}

pub trait DepSet {
	type Outs<'t>;
	const NAMES: &'static [&'static str];
}

/// [`Flat`] over a whole dep tuple, elements concatenated in `Deps` order (each batch dep as its
/// last element). Separate from [`DepSet`] so `Pull`/[`step`] stay bound-free; per-dep columns
/// recover via prefix sums of `DIMS` products.
pub trait DepFlat: DepSet {
	const DIMS: &'static [&'static [usize]];
	const LEN: usize;
	/// Per-dep scratch for the finite-difference re-advance (slice deps copy their batch here).
	type Scratch: Default;
	fn flat(outs: &Self::Outs<'_>, dst: &mut [f64]);
	/// Materializes the pulled outs into owned `scratch`, bumping the element owning `slot` by `h`
	/// and returning the perturbation that dep actually applied (see [`Bump`]).
	/// Consumes the pulled outs at their lifetime; the scratch owns copies, so [`DepFlat::view`]
	/// hands them back at a fresh, independent lifetime — that untying is what lets the
	/// self-borrowing re-`advance` on a short-lived clone typecheck.
	fn stage<'t>(outs: Self::Outs<'t>, scratch: &mut Self::Scratch, slot: usize, h: f64) -> f64;
	/// Views staged `scratch` as dep outs at the borrow's own lifetime `'l`.
	fn view<'l>(scratch: &'l Self::Scratch) -> Self::Outs<'l>;
}

/// A cell's finite-difference witness: materialize a pulled out into owned scratch ([`Nudge::stage`],
/// bumping one element when `bump` is `Some`), then hand it back at a fresh borrow lifetime
/// ([`Nudge::view`]). The materialize step *owns* the data — a slice cell copies its batch into
/// `Scratch = Vec<T>`, a value cell stores the value in `Scratch` — which unties the re-advance
/// lifetime from the pulled `'t`, so re-`advance`ing a short-lived clone typechecks. No blanket
/// impl — the value/slice cases overlap — so every observed cell writes its own short impl.
pub trait Nudge: Cell {
	type Scratch: Default;
	/// Materialize `out` into `scratch`; if `bump` is `Some(slot)`, perturb that element by about
	/// `h`. Returns the perturbation actually applied, in the dep's own units — see [`Bump`].
	fn stage<'t>(out: Self::Out<'t>, scratch: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64;
	/// View staged `scratch` as this cell's out at the borrow lifetime `'l`.
	fn view<'l>(scratch: &'l Self::Scratch) -> Self::Out<'l>;
}

/// A slice-out cell's finite-difference witness: copy the batch into `Vec<$E>` scratch, bump the
/// last element when asked, view it back at the borrow's own lifetime. Also the cell's [`Series`]
/// declaration — "this out is a run of `$E`" is exactly what both traits need to know.
#[macro_export]
macro_rules! slice_nudge {
	($C:ty, $E:ty) => {
		impl $crate::Series for $C {
			type Item = $E;
		}

		impl $crate::Nudge for $C {
			type Scratch = $crate::MacroVec<$E>;

			fn stage<'t>(out: &'t [$E], s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
				s.clear();
				s.extend_from_slice(out);
				match (bump, s.last_mut()) {
					(Some(slot), Some(last)) => {
						let (e, dh) = $crate::Bump::bump(*last, slot, h);
						*last = e;
						dh
					}
					_ => 0.0,
				}
			}

			fn view<'l>(s: &'l Self::Scratch) -> &'l [$E] {
				s
			}
		}
	};
}

/// A value-out cell's finite-difference witness: the scratch is just the value itself.
#[macro_export]
macro_rules! value_nudge {
	($C:ty) => {
		impl $crate::Nudge for $C {
			type Scratch = <$C as $crate::Cell>::Out<'static>;

			fn stage<'t>(out: <$C as $crate::Cell>::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
				match bump {
					Some(slot) => {
						let (v, dh) = $crate::Bump::bump(out, slot, h);
						*s = v;
						dh
					}
					None => {
						*s = out;
						0.0
					}
				}
			}

			fn view<'l>(s: &'l Self::Scratch) -> <$C as $crate::Cell>::Out<'l> {
				*s
			}
		}
	};
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
	const NAMES: &'static [&'static str] = &[A::NAME];
}
impl<A: Gate, B: Gate> GateSet for (A, B) {
	const NAMES: &'static [&'static str] = &[A::NAME, B::NAME];
}

pub trait Node: Cell {
	type Deps: DepSet;
	type When: GateSet = ();
	/// How far back this node's own state reaches. [`Horizon::Unbounded`] cannot be gated — nothing
	/// re-warms it. Anything else can: [`Horizon::Unit`] holds no state at all, and a bounded horizon
	/// is state the node's inputs can reconstitute.
	const HORIZON: Horizon = Horizon::Unbounded;
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn advance<'t>(&'t mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>;
}

/// A scalar-out node whose per-tick value is a pure [`Expr`] of its (scalar / last-element) deps —
/// each dep read at its [`Flat`] scalar, a batch dep as its last element, matching the observer's
/// end-of-batch view. [`Symbolic`] earns [`Node`] for free via the blanket below: its `advance` is
/// `body().eval(env)`, so it *cannot* compute any other way — the algebra is load-bearing.
///
/// `Out = f64` has no `None` channel: reading historic (warmup) deps emits `NaN` yet still reports
/// `fired = true`, so don't route a warmup-sensitive consumer off a Symbolic node unguarded.
pub trait Symbolic: Cell
where
	for<'t> Self: Cell<Out<'t> = f64>, {
	type Deps: DepSet;
	fn body(&self, vars: Vars) -> impl Expr;
}

/// scalar deps ⇒ one env slot each; the `impl_arity` tuple ceiling caps arity, so a fixed stack
/// array holds the whole env — zero heap on the compute path.
const MAX_VARS: usize = 8;

impl<S> Node for S
where
	S: Symbolic,
	for<'t> S: Cell<Out<'t> = f64>,
	<S as Symbolic>::Deps: DepFlat,
{
	type Deps = <S as Symbolic>::Deps;

	fn advance<'t>(&'t mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t> {
		const {
			let deps = <<S as Symbolic>::Deps as DepSet>::NAMES.len();
			assert!(
				<<S as Symbolic>::Deps as DepFlat>::LEN == deps,
				"Symbolic deps must be scalar (one env slot each): a vector-valued dep desyncs Var<I> from dep I"
			);
			assert!(deps <= MAX_VARS, "Symbolic arity exceeds the env buffer (MAX_VARS)");
		}
		let n = <<S as Symbolic>::Deps as DepFlat>::LEN;
		let mut env = [0.0f64; MAX_VARS];
		<<S as Symbolic>::Deps as DepFlat>::flat(&deps, &mut env[..n]);
		self.body(Vars).eval(&env[..n])
	}
}

/// The exactness hook [`step_exact`] consumes: exact partials (replacing the FD guess) plus the
/// equation as an [`Ast`] for documentation. Blanket-impl'd for every [`Symbolic`] node from its
/// [`Expr`] body; hand-impl'able for a black-box stateful node with analytic partials + a formula.
pub trait Diff: Node {
	/// Exact partials wrt deps, same row-major `out_len × dep_len` layout as [`Fire::jac`].
	fn exact_jac(&self, deps: DepOuts<'_, Self>, out: &mut [f64]);
	fn formula(&self) -> Ast;
}

impl<S> Diff for S
where
	S: Symbolic + Node<Deps = <S as Symbolic>::Deps>,
	for<'t> S: Cell<Out<'t> = f64>,
	<S as Symbolic>::Deps: DepFlat,
{
	fn exact_jac(&self, deps: DepOuts<'_, Self>, out: &mut [f64]) {
		let n = <<S as Symbolic>::Deps as DepFlat>::LEN;
		let mut env = [0.0f64; MAX_VARS];
		<<S as Symbolic>::Deps as DepFlat>::flat(&deps, &mut env[..n]);
		self.body(Vars).grad(&env[..n], 1.0, out);
	}

	fn formula(&self) -> Ast {
		self.body(Vars).lower()
	}
}

/// A binary control signal. Nodes naming it in [`Node::When`] are not advanced while it is
/// false: deps not pulled, no work done, out = [`Latent::latent`]. Gates are scalar-out.
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
/// commutation when the episode it gates reaches a [`Episode::terminal`] out. No second external
/// signal ever closes it. `Cut` is read post-sweep; commutation + the gated-node resets are
/// deferred to the next tick's start (the frame still borrows batch fields at end-of-tick).
pub trait Latch: Gate
where
	for<'t> Self: Cell<Out<'t> = bool>,
	for<'t> <Self::Cut as Cell>::Out<'t>: Episode, {
	/// The gated node whose terminal out commutates this latch.
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

/// A cell whose out is a run of `Item`s — the bufferable shape. The associated `Item` is what keeps
/// the [`Buffering`] `Has` impl below free of an unconstrained element parameter (E0207).
/// [`slice_nudge!`] declares it.
pub trait Series
where
	for<'x> Self: Cell<Out<'x> = &'x [Self::Item]>, {
	/// `'static` because [`Cell::Out`] carries no where-clause an impl could widen: an element that
	/// itself borrows the tick could never satisfy `Hist<'t, Item>`.
	type Item: Copy + 'static;
}

/// A [`Buffering`] dep's out: `all = past ++ fresh`, where `fresh` is byte-identical to the
/// unbuffered series out and `past` is what stood behind this tick's batch. `horizon` is the
/// *consumer's* declared one, so a window wider than it stated trips regardless of how far the
/// frame's buffer happens to reach.
#[derive(Debug)]
pub struct Hist<'t, T> {
	all: &'t [T],
	fresh: usize,
	horizon: Horizon,
	/// The retaining buffer's highest dropped `ts_ns` — see [`Buffer::watermark`].
	watermark: i64,
}

impl<T> Clone for Hist<'_, T> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<T> Copy for Hist<'_, T> {}

impl<'t, T> Hist<'t, T> {
	/// This tick's emissions — identical to the unbuffered series out.
	pub fn fresh(self) -> &'t [T] {
		&self.all[self.all.len() - self.fresh..]
	}

	/// What stood behind this tick's batch.
	pub fn past(self) -> &'t [T] {
		&self.all[..self.all.len() - self.fresh]
	}

	/// The whole retained run, `past ++ fresh` — the cross-rate view, for a consumer clocked by some
	/// faster series that must find the run standing at its own deadline.
	pub fn all(self) -> &'t [T] {
		self.all
	}
}

impl<'t, T: Stamped> Hist<'t, T> {
	/// The window ending at `fresh()[i]`, per the declared [`Horizon`]; `None` when it is incomplete
	/// — fewer than `Elems(n)` retained, or a `Span` reaching past what the buffer has dropped.
	pub fn trailing_at(self, i: usize) -> Option<&'t [T]> {
		let end = self.all.len() - self.fresh + i + 1;
		assert!(end <= self.all.len(), "trailing_at: {i} past this tick's {} fresh elements", self.fresh);
		match self.horizon {
			Horizon::Elems(n) => {
				assert!(n >= 1, "a window includes the current element, so Elems(n >= 1)");
				(end >= n).then(|| &self.all[end - n..end])
			}
			// exclusive: the window is the last `ms` of wall clock, the same predicate the buffer trims
			// on — so what it retains and what a consumer reads are one statement.
			Horizon::Span(ms) => {
				let cut = self.all[end - 1].ts_ns() - Horizon::ns(ms);
				(cut >= self.watermark).then(|| &self.all[self.all[..end].partition_point(|x| x.ts_ns() <= cut)..end])
			}
			h => unreachable!("a Buffering is const-asserted bounded, and `narrowed` re-asserts it: {h:?}"),
		}
	}

	/// One window per fresh element — rate preservation for free.
	pub fn trailing(self) -> impl Iterator<Item = Option<&'t [T]>> {
		(0..self.fresh).map(move |i| self.trailing_at(i))
	}

	/// A view at a shallower horizon, for a window whose size is a runtime knob.
	pub fn narrowed(self, h: Horizon) -> Self {
		assert!(self.horizon.serves(h), "history retained at {:?} does not serve a read at {h:?}", self.horizon);
		Self { horizon: h, ..self }
	}
}

/// Reads `fresh` only: a buffer adds no signal, so its [`Fire`] is indistinguishable from the
/// series it retains.
impl<T: Flat> Flat for Hist<'_, T> {
	const DIMS: &'static [usize] = T::DIMS;

	fn flat(&self, out: &mut [f64]) -> bool {
		self.fresh().flat(out)
	}

	fn fires(&self) -> usize {
		self.fresh
	}
}

impl<T: Glance> Glance for Hist<'_, T> {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		self.fresh().glance(f)
	}
}

/// Engine-owned retention over a [`Series`] — an ordinary node (`Deps = (C,)`, ungated,
/// [`Horizon::Unbounded`]) sitting *next to* its source in the frame, not over it. It advances every
/// tick regardless of what is dark downstream, because being warm is its whole job: a consumer
/// switched off and revived reads a full window on its first tick back, where a client-owned window
/// would come back cold.
///
/// `H` is the reach *inclusive of the current element*, so `Buffer<C, {Horizon::Elems(14)}>` serves
/// exactly a 14-long indicator. The retention invariant is: **whatever `H` reaches back over from
/// before this tick's batch, plus the whole batch** — trimmed against the pre-batch newest, because
/// a *cross-rate* consumer (one clocked by a faster series, searching [`Hist::all`] for the run
/// standing at its own deadline) needs the whole reach on a tick where this series emitted nothing.
///
/// One `Buffer<C, _>` per series per frame — two make every `Buffering<C, _>` ambiguous, the same
/// failure as two instances of one node type.
pub struct Buffer<C: Series, const H: Horizon> {
	buf: alloc::vec::Vec<C::Item>,
	/// The highest `ts_ns` this buffer cannot speak for: the last one dropped, or — before the first
	/// drop — the first one ever seen, since nothing proves the run reached back past it. A
	/// [`Horizon::Span`] window is complete iff it begins strictly after this, which is exact where
	/// "have I been running long enough" is a guess.
	watermark: i64,
}

// hand-written: `derive` would demand `C: Default` / `C: Clone`, which the source node need not be.
impl<C: Series, const H: Horizon> Default for Buffer<C, H> {
	fn default() -> Self {
		Self {
			buf: alloc::vec::Vec::new(),
			watermark: i64::MIN,
		}
	}
}
impl<C: Series, const H: Horizon> Clone for Buffer<C, H> {
	fn clone(&self) -> Self {
		Self {
			buf: self.buf.clone(),
			watermark: self.watermark,
		}
	}
}

impl<C: Series, const H: Horizon> Cell for Buffer<C, H> {
	type Out<'t> = Hist<'t, C::Item>;
}

impl<C: Series, const H: Horizon> Node for Buffer<C, H>
where
	C::Item: Stamped,
{
	type Deps = (C,);

	fn advance<'t>(&'t mut self, (fresh,): DepOuts<'t, Self>) -> Self::Out<'t> {
		const {
			assert!(
				match H {
					Horizon::Elems(k) => k >= 1,
					Horizon::Span(ms) => ms > 0,
					_ => false,
				},
				"a buffer retains a bounded reach: Horizon::Elems(k >= 1) or Horizon::Span(ms > 0)"
			)
		}
		// Trim *before* the append: `past` must be what stood behind this tick's batch, or an
		// intra-batch cursor walking several elements reads a window already trimmed by its own tail.
		let drop = match H {
			Horizon::Elems(k) => self.buf.len().saturating_sub(k),
			Horizon::Span(ms) => match self.buf.last() {
				Some(newest) => {
					let cut = newest.ts_ns() - Horizon::ns(ms);
					self.buf.partition_point(|x| x.ts_ns() <= cut)
				}
				None => 0,
			},
			_ => unreachable!(),
		};
		if drop > 0 {
			self.watermark = self.watermark.max(self.buf[drop - 1].ts_ns());
			self.buf.drain(..drop);
		}
		if self.watermark == i64::MIN
			&& let Some(first) = fresh.first()
		{
			self.watermark = first.ts_ns();
		}
		self.buf.extend_from_slice(fresh);
		Hist {
			all: &self.buf,
			fresh: fresh.len(),
			horizon: H,
			watermark: self.watermark,
		}
	}
}

/// Dep position only, never a frame field: "this series, retained at least `H` back". Resolves
/// against the frame's [`Buffer<C, K>`] through the [`Has`] impl below, whose const-assert proves
/// the declared reach [`serves`](Horizon::serves) the request.
pub struct Buffering<C: Series, const H: Horizon>(core::marker::PhantomData<C>);

impl<C: Series, const H: Horizon> Cell for Buffering<C, H> {
	type Out<'t> = Hist<'t, C::Item>;
}

impl<'t, C: Series, const K: Horizon, const H: Horizon, T> Has<'t, Buffering<C, H>, Here> for Cons<'t, Buffer<C, K>, T> {
	fn get(&self) -> Hist<'t, C::Item> {
		const {
			assert!(!matches!(H, Horizon::Unit), "Buffering at Unit is the bare dep C — drop the wrapper");
			assert!(!matches!(H, Horizon::Unbounded), "a buffer is a bounded thing; Unbounded names no window");
			assert!(K.serves(H), "the frame's Buffer<C, K> does not reach as far back as this Buffering<C, H> asks for");
		}
		Hist { horizon: H, ..self.out }
	}
}

impl<C: Series, const H: Horizon> Nudge for Buffering<C, H>
where
	C::Item: Bump,
{
	type Scratch = (alloc::vec::Vec<C::Item>, usize, i64);

	fn stage<'t>(out: Hist<'t, C::Item>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		s.0.clear();
		s.0.extend_from_slice(out.all);
		s.1 = out.fresh;
		s.2 = out.watermark;
		match (bump, s.0.last_mut()) {
			(Some(slot), Some(last)) => {
				let (e, dh) = Bump::bump(*last, slot, h);
				*last = e;
				dh
			}
			_ => 0.0,
		}
	}

	fn view<'l>(s: &'l Self::Scratch) -> Hist<'l, C::Item> {
		Hist {
			all: &s.0,
			fresh: s.1,
			horizon: H,
			watermark: s.2,
		}
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
	type Scratch = ();

	const DIMS: &'static [&'static [usize]] = &[];
	const LEN: usize = 0;

	fn flat(_: &Self::Outs<'_>, dst: &mut [f64]) {
		debug_assert!(dst.is_empty());
	}

	fn stage<'t>(_: Self::Outs<'t>, _: &mut Self::Scratch, _: usize, _: f64) -> f64 {
		0.0
	}

	fn view<'l>(_: &'l Self::Scratch) -> Self::Outs<'l> {}
}

macro_rules! impl_arity {
	($($T:ident $I:ident $v:ident $s:ident),+) => {
		impl<$($T: Cell),+> DepSet for ($($T,)+) {
			type Outs<'t> = ($($T::Out<'t>,)+);

			const NAMES: &'static [&'static str] = &[$($T::NAME),+];
		}
		impl<'t, F, $($T: Cell, $I),+> Pull<'t, F, ($($I,)+)> for ($($T,)+)
		where F: $(Has<'t, $T, $I> +)+ {
			fn pull(f: &F) -> Self::Outs<'t> where F: 't {
				($(Has::<'t, $T, $I>::get(f),)+)
			}
		}
		impl<$($T: Nudge),+> DepFlat for ($($T,)+)
		where $(for<'x> <$T as Cell>::Out<'x>: Flat),+ {
			const DIMS: &'static [&'static [usize]] = &[$(<<$T as Cell>::Out<'static> as Flat>::DIMS),+];
			const LEN: usize = 0 $(+ <<$T as Cell>::Out<'static> as Flat>::LEN)+;

			type Scratch = ($($T::Scratch,)+);

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

			fn stage<'t>(outs: Self::Outs<'t>, scratch: &mut Self::Scratch, slot: usize, h: f64) -> f64 {
				let ($($v,)+) = outs;
				let ($($s,)+) = scratch;
				let (mut off, mut realized) = (0, 0.0);
				$(
					{
						let len = <<$T as Cell>::Out<'static> as Flat>::LEN;
						let bump = if (off..off + len).contains(&slot) { Some(slot - off) } else { None };
						let dh = <$T as Nudge>::stage($v, $s, bump, h);
						if bump.is_some() {
							realized = dh;
						}
						off += len;
					}
				)+
				debug_assert_eq!(off, Self::LEN);
				realized
			}

			fn view<'l>(scratch: &'l Self::Scratch) -> Self::Outs<'l> {
				let ($($s,)+) = scratch;
				($(<$T as Nudge>::view($s),)+)
			}
		}
	};
}
impl_arity!(A Ia a sa);
impl_arity!(A Ia a sa, B Ib b sb);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd, E Ie e se);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd, E Ie e se, G Ig g sg);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd, E Ie e se, G Ig g sg, H Ih h sh);
impl_arity!(A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd, E Ie e se, G Ig g sg, H Ih h sh, J Ij j sj);

/// [`step`]'s evaluation dispatch, keyed on the node's [`Node::When`]: ungated nodes advance
/// unconditionally; gated nodes advance only while every gate reads true in the frame,
/// yielding [`Latent::latent`] otherwise — deps not pulled. `I`/`J` are inferred index paths.
pub trait Drive<'t, N: Node, F, I, J>: GateSet {
	fn open(f: &F) -> bool
	where
		F: 't;
	fn drive(node: &'t mut N, f: &F) -> N::Out<'t>
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

	fn drive(node: &'t mut N, f: &F) -> N::Out<'t>
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

	fn drive(node: &'t mut N, f: &F) -> N::Out<'t>
	where
		F: 't, {
		const {
			assert!(
				!matches!(N::HORIZON, Horizon::Unbounded),
				"an unbounded node cannot be gated: nothing re-warms it — declare a bounded `HORIZON` or drop `When`"
			)
		}
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

	fn drive(node: &'t mut N, f: &F) -> N::Out<'t>
	where
		F: 't, {
		const {
			assert!(
				!matches!(N::HORIZON, Horizon::Unbounded),
				"an unbounded node cannot be gated: nothing re-warms it — declare a bounded `HORIZON` or drop `When`"
			)
		}
		if !<Self as Drive<'t, N, F, I, (Ia, Ib)>>::open(f) {
			return Latent::latent();
		}
		node.advance(<N::Deps as Pull<'t, F, I>>::pull(f))
	}
}

/// Advances `node` over `frame` and pushes its output. The `Pull` bound is the engine's reason to
/// exist: a node stepped before its deps are in the frame does not compile.
pub fn step<'t, N, F, I, J>(frame: F, node: &'t mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::When: Drive<'t, N, F, I, J>,
	F: 't, {
	let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
	Cons { out, tail: frame }
}

/// One node firing, flattened: values and the finite-difference local Jacobian wrt its deps,
/// à la Jane Street's "Computations that differentiate, debug and document themselves".
#[derive(Clone, Copy)]
pub struct Fire<'a> {
	pub debug: &'a dyn core::fmt::Debug,
	/// Compact one-liner for viz cards; `debug` stays the full-detail view (hover/tooltip).
	pub glance: &'a dyn Glance,
	pub dims: &'static [usize],
	pub plots: &'static [Plot],
	/// Elements the node fired this tick: slice len, or 0/1 for scalar/`Option` outs.
	pub fires: usize,
	/// Flattened *last* element; `None` = didn't fire.
	pub vals: Option<&'a [f64]>,
	pub dep_dims: &'a [&'static [usize]],
	/// Row-major `out_len × sum(dep lens)`, deps concatenated in `Deps` order (each batch dep as
	/// its last element). NaN = no signal. `None` when the node didn't fire.
	pub jac: Option<&'a [f64]>,
	/// Exact partials, same layout as [`jac`](Self::jac) — [`Diff`] nodes only; agrees with `jac`
	/// within FD tolerance where both are present. `None` for FD-only nodes.
	pub exact_jac: Option<&'a [f64]>,
	/// The node's equation rendered as a formula (LaTeX/infix), [`Diff`] nodes only.
	pub formula: Option<&'a dyn core::fmt::Display>,
	/// Simplified `∂out/∂dep` formulas, [`Diff`] nodes only.
	pub deriv: Option<&'a dyn core::fmt::Display>,
	/// Value-annotated intermediate-value tree ([`Ast::trace`]) over this tick's deps, [`Diff`]
	/// nodes only — the "debug themselves" reading.
	pub trace: Option<&'a dyn core::fmt::Display>,
}

/// Sees every [`step_obs`] as it happens: one interpretation choke point, many interpretations.
/// Step order IS topo order, so the observed sequence doubles as the graph's static topology; dep
/// names never seen as stepped nodes are roots — apps seed root activations via [`observe_root`].
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

/// Two interpretations of the same sweep — e.g. an app's own assertions next to a viz recorder.
impl<A: Observer, B: Observer> Observer for (A, B) {
	const ACTIVE: bool = A::ACTIVE || B::ACTIVE;

	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [&'static str], fire: Fire<'_>) {
		self.0.on(node, deps, gates, fire);
		self.1.on(node, deps, gates, fire);
	}
}

/// One finite-difference column: re-advance a fresh clone on `deps` with element `slot` bumped by
/// about `h`, writing the bumped out into `bumped`; returns whether it fired and the perturbation
/// the dep actually applied. Isolated from [`step_obs`] so the re-advance lifetime is purely local
/// — the clone and its nudged deps never escape, which keeps the self-borrowing `advance` from
/// pinning them to the caller's tick lifetime.
fn fd_col<'d, N>(pre: &N, deps: DepOuts<'d, N>, slot: usize, h: f64, bumped: &mut [f64]) -> (bool, f64)
where
	N: Node + Clone,
	N::Deps: DepFlat,
	DepOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let mut scratch = <N::Deps as DepFlat>::Scratch::default();
	let dh = <N::Deps as DepFlat>::stage(deps, &mut scratch, slot, h);
	let mut clone = pre.clone();
	(clone.advance(<N::Deps as DepFlat>::view(&scratch)).flat(bumped), dh)
}

/// The full finite-difference Jacobian: one [`fd_col`] per dep element, NaN columns where a dep is
/// unfired or the bump crossed a firing branch. `out_buf`/`dep_buf` are the un-bumped flattenings.
fn fd_jac<'d, N>(pre: &N, deps: DepOuts<'d, N>, dep_buf: &[f64], out_buf: &[f64]) -> alloc::vec::Vec<f64>
where
	N: Node + Clone,
	N::Deps: DepFlat,
	DepOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let (out_len, dep_len) = (out_buf.len(), dep_buf.len());
	let mut jac = alloc::vec![f64::NAN; out_len * dep_len];
	let mut bumped = alloc::vec![f64::NAN; out_len];
	for slot in 0..dep_len {
		let x = dep_buf[slot];
		if x.is_nan() {
			continue;
		}
		let h = (x.abs() * 1e-6).max(1e-9);
		// `dh`, not `h`: a quantized dep moves in whole ticks, and dividing by a step it never took
		// is a fabricated slope. `0.0` ⇒ the slot has no derivative at all.
		let (fired, dh) = fd_col::<N>(pre, deps, slot, h, &mut bumped);
		if !fired || dh == 0.0 {
			continue; // bump crossed a firing branch, or the slot is discrete — column stays NaN
		}
		for i in 0..out_len {
			jac[i * dep_len + slot] = (bumped[i] - out_buf[i]) / dh;
		}
	}
	jac
}

/// [`step`] + [`Observer::on`] before the push. The `()` observer erases to exactly `step`.
///
/// Under an active observer, each fired node's Jacobian is finite-differenced: clone the
/// pre-advance node, [`Nudge`] the *last* element of one dep (batch deps copied into scratch),
/// re-advance the clone at a shorter lifetime, diff the last out elements.
pub fn step_obs<'t, N, F, I, J, O: Observer>(frame: F, node: &'t mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::When: Drive<'t, N, F, I, J>,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance,
	F: 't, {
	const { assert!(Plot::coherent(N::PLOTS), "a multi-plot node must name each plot's slots; `[]` claims all of them") }
	if !O::ACTIVE {
		let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
		return Cons { out, tail: frame };
	}

	// gate closed: no advance, no dep flatten, no FD — an unfired `Fire` is the honest view.
	if !<N::When as Drive<'t, N, F, I, J>>::open(&frame) {
		let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
		obs.on(
			N::NAME,
			<N::Deps as DepSet>::NAMES,
			<N::When as GateSet>::NAMES,
			Fire {
				debug: &out,
				glance: &out,
				dims: <N::Out<'t> as Flat>::DIMS,
				plots: N::PLOTS,
				fires: out.fires(),
				vals: None,
				dep_dims: <N::Deps as DepFlat>::DIMS,
				jac: None,
				exact_jac: None,
				formula: None,
				deriv: None,
				trace: None,
			},
		);
		return Cons { out, tail: frame };
	}

	let pre = node.clone();
	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	let out = node.advance(deps);

	let out_len = <N::Out<'t> as Flat>::LEN;
	let mut out_buf = alloc::vec![f64::NAN; out_len];
	let fired = out.flat(&mut out_buf);
	let fires = out.fires();

	let dep_len = <N::Deps as DepFlat>::LEN;
	let mut dep_buf = alloc::vec![f64::NAN; dep_len];
	<N::Deps as DepFlat>::flat(&deps, &mut dep_buf);

	let jac = fired.then(|| fd_jac::<N>(&pre, deps, &dep_buf, &out_buf));

	obs.on(
		N::NAME,
		<N::Deps as DepSet>::NAMES,
		<N::When as GateSet>::NAMES,
		Fire {
			debug: &out,
			glance: &out,
			dims: <N::Out<'t> as Flat>::DIMS,
			plots: N::PLOTS,
			fires,
			vals: fired.then_some(out_buf.as_slice()),
			dep_dims: <N::Deps as DepFlat>::DIMS,
			jac: jac.as_deref(),
			exact_jac: None,
			formula: None,
			deriv: None,
			trace: None,
		},
	);
	Cons { out, tail: frame }
}

/// [`step_obs`]'s sibling for a [`Diff`] node: the same advance + FD momentary Jacobian, plus the
/// *exact* partials, the equation formula, and its simplified per-dep derivatives — the graph's
/// "differentiate + document themselves" reading. The `graph!` `diff { }` group routes fields here.
pub fn step_exact<'t, N, F, I, J, O: Observer>(frame: F, node: &'t mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Diff + Clone,
	N::When: Drive<'t, N, F, I, J>,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance,
	F: 't, {
	if !O::ACTIVE {
		let out = <N::When as Drive<'t, N, F, I, J>>::drive(node, &frame);
		return Cons { out, tail: frame };
	}

	let pre = node.clone();
	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	let out = node.advance(deps);

	let out_len = <N::Out<'t> as Flat>::LEN;
	let mut out_buf = alloc::vec![f64::NAN; out_len];
	let fired = out.flat(&mut out_buf);
	let fires = out.fires();

	let dep_len = <N::Deps as DepFlat>::LEN;
	let mut dep_buf = alloc::vec![f64::NAN; dep_len];
	<N::Deps as DepFlat>::flat(&deps, &mut dep_buf);

	let jac = fired.then(|| fd_jac::<N>(&pre, deps, &dep_buf, &out_buf));

	// zeroed, not NaN-filled: `grad` accumulates (`+=`) into it, and an absent var's partial is 0.
	let mut exact = alloc::vec![0.0f64; out_len * dep_len];
	pre.exact_jac(deps, &mut exact);
	let formula = pre.formula();
	let deriv = Derivs {
		names: <N::Deps as DepSet>::NAMES,
		parts: (0..dep_len).map(|i| formula.diff(i).simplify()).collect(),
	};
	let trace = formula.trace(&dep_buf);

	obs.on(
		N::NAME,
		<N::Deps as DepSet>::NAMES,
		<N::When as GateSet>::NAMES,
		Fire {
			debug: &out,
			glance: &out,
			dims: <N::Out<'t> as Flat>::DIMS,
			plots: N::PLOTS,
			fires,
			vals: fired.then_some(out_buf.as_slice()),
			dep_dims: <N::Deps as DepFlat>::DIMS,
			jac: jac.as_deref(),
			exact_jac: Some(&exact),
			formula: Some(&formula),
			deriv: Some(&deriv),
			trace: Some(&trace),
		},
	);
	Cons { out, tail: frame }
}

/// The per-dep simplified derivatives of a [`Diff`] node, `∂out/∂dep` one per line — the `deriv`
/// field's [`fmt::Display`](core::fmt::Display).
struct Derivs {
	names: &'static [&'static str],
	parts: alloc::vec::Vec<Ast>,
}

impl core::fmt::Display for Derivs {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		for (i, part) in self.parts.iter().enumerate() {
			if i > 0 {
				f.write_str("\n")?;
			}
			let dep = self.names.get(i).and_then(|n| n.rsplit("::").next()).unwrap_or("?");
			write!(f, "∂/∂{dep} = {part}")?;
		}
		Ok(())
	}
}

/// A cell's [`Cell::NAME`], reached through a function so [`graph!`] users stay off nightly
/// feature attrs.
#[doc(hidden)]
pub const fn node_name<T: Cell>() -> &'static str {
	T::NAME
}

/// The `TypeId` of a root's event type, for [`graph!`]'s `required_events`.
#[doc(hidden)]
pub fn event_id<T: 'static>() -> TypeId {
	TypeId::of::<T>()
}

/// A graph's required root events — its dep tree, computed in isolation: the `TypeId` of each
/// declared root event whose root is consumed by some node. [`graph!`] implements it; the facade
/// maps the `TypeId`s to source lanes.
pub trait Roots {
	fn required_events() -> alloc::vec::Vec<TypeId>;
}

#[doc(hidden)]
pub use alloc::vec::Vec as MacroVec;

/// One node's compile-time shape, as [`graph!`] sees it. `name`/`deps`/`gates` are
/// [`core::any::type_name`] strings: const-comparable, never persisted.
#[doc(hidden)]
pub struct NodeMeta {
	pub name: &'static str,
	pub deps: &'static [&'static str],
	pub horizon: Horizon,
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

/// [`graph!`]'s completeness check: true when `name` is bounded-horizon, ungated, has in-graph
/// consumers, and all of them sit behind one common gate (other than `name` itself) — sampling
/// it while that gate is closed is pure waste, so it must be gated too or declare
/// [`Horizon::Unbounded`]. Leaves (no in-graph consumers) are graph outputs — exempt.
#[doc(hidden)]
pub const fn shadowed(name: &'static str, nodes: &[NodeMeta]) -> bool {
	let mut me = 0;
	while me < nodes.len() && !str_eq(nodes[me].name, name) {
		me += 1;
	}
	assert!(me < nodes.len(), "shadowed: name must be one of nodes");
	if matches!(nodes[me].horizon, Horizon::Unbounded) || !nodes[me].gates.is_empty() {
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

/// Wires a declared node list into a graph struct + typed out-struct + batch-native `tick`. Fields
/// in topo order — a wrong order fails the existing `Pull`/`Has` bounds at compile time.
///
/// ```ignore
/// graph! {
///     pub struct Graph;
///     batches Batches;                       // name of the generated root-slices struct
///     roots { trades: Trades[Trade], oi: OiRoot[Oi] };
///     out TickOut;
///     bar: Bar1m, cvd: Cvd, ...
/// }
/// ```
///
/// `Batches<'t>` gets one field per root, of that root cell's `Out<'t>` — deliberately not
/// `Default`: every field is filled explicitly from a woven step, and a silently-empty root is a
/// footgun. `tick<'t>(&'t mut self, b: Batches<'t>) -> TickOut<'t>` seeds the frame with every root
/// out and sweeps. `required_events()` returns the `TypeId`s of the events whose root is consumed
/// by some node — the dep tree, computed in isolation.
///
/// An optional `latch { field: Type, .. }` group names [`Latch`] fields (also in the node list).
/// A latch whose `Cut` out reads [`Episode::terminal`] is commutated and its gated fields reset
/// to `Default` at the *next* tick's start (deferred: the frame still borrows batch fields).
/// **Every gate/latch/gated field must be scalar-out** — a batch-out gate is out of contract.
///
/// An optional `diff { field: Type, .. }` group names [`Diff`] fields (also in the node list):
/// they sweep via [`step_exact`], emitting exact partials + formula + derivatives.
pub use trading_data_macros::graph;

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
		C::NAME,
		&[],
		&[],
		Fire {
			debug: &out,
			glance: &out,
			dims: <C::Out<'t> as Flat>::DIMS,
			plots: &[Plot::DEFAULT],
			fires: out.fires(),
			vals: fired.then_some(buf.as_slice()),
			dep_dims: &[],
			jac: None,
			exact_jac: None,
			formula: None,
			deriv: None,
			trace: None,
		},
	);
}
