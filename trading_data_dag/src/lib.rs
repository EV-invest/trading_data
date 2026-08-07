//! Compile-time step-graph for derived values, batch-native.
//!
//! Each derived value is a [`Node`] whose `type Deps` names its upstream cells. [`step`]'s
//! [`Pull`] bound makes a wrong topological order (or a cycle) a compile error, and a full
//! graph sweep monomorphizes to one straight-line function — no dispatch, no runtime graph.
//!
//! # Batches, not events
//!
//! A router slices the merged timeline into runs of same-type events; the graph consumes and
//! produces *batches* natively. An event-emitting node is an [`Emit`]: the engine owns the run and
//! lends it as `&mut Vec<T>`, so the node struct holds only what it remembers between ticks, and its
//! `Cell::Out<'t>` is `&'t [T]`. The frame transitively holds those borrows, so the "nodes are Copy
//! values" doctrine is dead. Level (state-view) nodes are [`Node`]s returning plain `Copy` values.
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
//! - **A gate is scalar-out; a gated node may be batch-out.** A [`Gate`] outputs plain `bool`; a
//!   node naming it through a [`Gating`] dep is not advanced while it is false — a [`Node`] reads
//!   [`Latent::latent`], a dark [`Emit`] is simply the empty run. The gate resolves once per tick,
//!   so a gated batch node's episode boundary is quantized to its batch window.
//! - **Gating is a dep kind.** A gate is an input like any other — the one that *dominates* — so it
//!   is named in `Deps`, wrapped in [`Gating`], beside [`Buffering`] and [`Folding`]. One channel,
//!   not two: what gates a node is also what it depends on, and no reader of the graph has to learn
//!   a second edge set to see that.
//! - **Horizon.** Reach is stated per *dep*, because "how far back must this be looked at" is a
//!   question about an input: a bare dep reaches [`Horizon::Unit`] (this tick's batch), a
//!   [`Buffering`] dep names the reach the *engine* retains for it, and a [`Folding`] dep names the
//!   reach the *node itself* holds. A closed gate pulls no deps, so node-held state cannot survive
//!   one: a [`Folding`] dep on a gated node is a compile error, and a gated node's reach is
//!   therefore retained in the frame by construction.
//! - **Demand.** Gating states what a node needs; demand is the same edge read backwards. [`graph!`]
//!   derives, per node, the gates that dominate *every* path from it to an output, and skips it
//!   while any of them is false — so a node whose only readers are shut computes nothing, without
//!   its author restating their gate. A latch never dominates (it is momentary: what a latch-gated
//!   node reads must be warm before the episode arms), and nothing that holds history is ever
//!   skipped: a [`Folding`] dep, a [`Buffer`], a latch, or being a gate all pin a node to every
//!   tick. A skipped node reads [`Latent::latent`], which is the whole of what it must earn.
//! - **Latches.** A [`Latch`] is a [`Gate`] armed externally and cut from within: when its `Cut`
//!   node's out reads [`Episode::terminal`], [`graph!`] commutates it and resets every node gated
//!   on it to `Default` — deferred to the *next* tick's start (the frame still borrows batch
//!   fields at end-of-tick, so the reset can't run in place).
//!
//! Impls that write concrete dep types hit E0195 (lifetime binder mismatch); use [`DepOuts`] so
//! every impl is uniformly `fn advance<'t>(&'t mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>`,
//! and [`EmitOuts`] so every [`Emit`] is `fn emit(&mut self, deps: EmitOuts<'_, Self>, out: ..)`.
//!
//! A node does not carry its own compute method: it names the [`Level`] kernel that computes it, and
//! `#[node]` writes the [`Node`] impl from whichever body trait was implemented. [`Symbolic`] (an
//! [`Expr`] body) earns [`Pure`]; [`Blind`] — the stated hatch — earns [`Opaque`].
//!
//! ```
//! use trading_data_dag::{Blind, Cell, Cons, DepOuts, Nil, Node, Opaque, step};
//!
//! struct Price;
//! impl Cell for Price {
//! 	type Out<'t> = f64;
//! }
//!
//! #[derive(Clone)]
//! struct Double;
//! impl Cell for Double {
//! 	type Out<'t> = f64;
//! }
//! impl Blind for Double {
//! 	type Deps = (Price,);
//! 	const WHY: &'static str = "the doc example predates the algebra it would be written in";
//! 	fn advance<'t>(&'t mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
//! 		p * 2.0
//! 	}
//! }
//! // `#[node]` writes this; spelled out here because the doctest has no graph to reach it from.
//! impl Node for Double {
//! 	type Deps = <Self as Blind>::Deps;
//! 	type Kernel = Opaque;
//! }
//!
//! let mut double = Double;
//! let f = Cons::<Price, Nil> { out: 21.0, tail: Nil };
//! let f = step(f, &mut double);
//! assert_eq!(f.head(), 42.0);
//! ```
#![feature(adt_const_params)]
#![feature(associated_type_defaults)]
#![feature(const_type_name)]

extern crate alloc;

use core::any::TypeId;

use trading_data_expr::Ast;
/// The algebra a body is written in, re-exported so a crate declaring nodes needs only this one.
pub use trading_data_expr::{Ex, Expr, Slots, Vars, abs, constant, exp, gt, lt, max, min, powi_of, select, sqrt, square, sum};
use v_utils::Timeframe;

/// How far back a dep position reaches: nothing at all (a bare dep), the engine's retention
/// ([`Buffering`]), or the consumer's own state ([`Folding`]). One vocabulary for all three, so the
/// reach a node reads and the reach it holds are stated the same way — and a `const` of it drops
/// straight into const-generic position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, core::marker::ConstParamTy)]
pub enum Horizon {
	/// The current value only — no history at all.
	Unit,
	Elems(usize),
	/// A window of wall clock, stated as the timeframe it is — the unit travels with the number
	/// instead of being a convention the reader has to know. *Un*located, unlike `core`'s `ts::Span`,
	/// which this used to shadow: a duration, not an `Instant..Instant`.
	Over(Timeframe),
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
			(Horizon::Over(k), Horizon::Over(j)) => k.0 >= j.0,
			(Horizon::Over(_), Horizon::Elems(_)) => true,
			_ => false,
		}
	}

	/// The reach that serves both — what one [`Buffer`] must retain to satisfy every consumer of the
	/// series. A span outranks any count, which is why the four variants are totally ordered and a
	/// graph can never ask for two reaches that cannot be met at once.
	pub const fn join(self, other: Horizon) -> Horizon {
		match (self, other) {
			(Horizon::Unbounded, _) | (_, Horizon::Unbounded) => Horizon::Unbounded,
			(Horizon::Over(a), Horizon::Over(b)) => Horizon::Over(if a.0 >= b.0 { a } else { b }),
			(Horizon::Over(s), _) | (_, Horizon::Over(s)) => Horizon::Over(s),
			(Horizon::Elems(a), Horizon::Elems(b)) => Horizon::Elems(if a >= b { a } else { b }),
			(Horizon::Elems(k), Horizon::Unit) | (Horizon::Unit, Horizon::Elems(k)) => Horizon::Elems(k),
			(Horizon::Unit, Horizon::Unit) => Horizon::Unit,
		}
	}

	/// The reach as a [`Cell::NAME`] fragment. Unqualified: the only place a reach is spelled is a
	/// retaining wrapper's parameter list, where the position already says what it is.
	pub const fn tag(self) -> Tag {
		let (mut buf, mut len) = ([0u8; 256], 0);
		len = match self {
			Horizon::Unit => write(&mut buf, len, b"Unit"),
			Horizon::Unbounded => write(&mut buf, len, b"Unbounded"),
			Horizon::Elems(n) => {
				let len = write(&mut buf, len, b"Elems(");
				let len = digits(&mut buf, len, n as u64);
				write(&mut buf, len, b")")
			}
			Horizon::Over(tf) => {
				let len = write(&mut buf, len, b"Over(");
				let len = timeframe(&mut buf, len, tf);
				write(&mut buf, len, b")")
			}
		};
		Tag { buf, len }
	}

	/// The wall-clock depth this reach comes to against a producer publishing every `clock` — the
	/// direction warmup is measured in. What a replay must preload is a *duration*, and `Elems(n)`
	/// becomes one only once the producer's rate is known, which is what [`Emit::CLOCK`] supplies:
	/// with every producer's clock static, a consumer's declared reach resolves to a depth at compile
	/// time instead of being a hand-picked replay range.
	///
	/// The inverse is not the useful one. `Over → Elems` yields a buffer capacity, and [`Buffer`]
	/// already trims on timestamps (see [`Hist::all`]) without ever needing a count.
	pub const fn span(self, clock: Timeframe) -> Timeframe {
		match self {
			Horizon::Unit => clock,
			Horizon::Elems(n) => Timeframe(clock.0 * n as u64),
			Horizon::Over(tf) => tf,
			Horizon::Unbounded => panic!("an unbounded reach is no duration: nothing recovers such a node, so no depth warms it"),
		}
	}

	/// Only a [`Horizon::Over`] has one; the caller has already matched the variant. Public because a
	/// [`Batch`] written outside this crate has to turn its declared reach into a period.
	pub const fn ns(tf: Timeframe) -> i64 {
		(tf.0 * 1_000_000) as i64
	}
}

/// A reach in *dep* position, where [`Horizon`] is the same thing as a value — what the engine joins
/// and compares. The two vocabularies coexist deliberately: [`Buffer<C, K>`](Buffer) takes a
/// `Horizon` because [`graph!`] computes `K` as the join of every read of the series, and no type
/// could name a join over reads that have not been seen yet.
///
/// This exists because a braced const argument may not mention a generic parameter, so
/// `Folding<C, { Horizon::Over(TF) }>` does not parse — but an associated const on a *type* may read
/// its impl's generics freely.
pub trait Reach {
	const HORIZON: Horizon;
}

/// A window of wall clock — [`Horizon::Over`] as a type.
pub struct Over<const TF: Timeframe>;
impl<const TF: Timeframe> Reach for Over<TF> {
	const HORIZON: Horizon = Horizon::Over(TF);
}

/// A count of elements — [`Horizon::Elems`] as a type.
pub struct Elems<const N: usize>;
impl<const N: usize> Reach for Elems<N> {
	const HORIZON: Horizon = Horizon::Elems(N);
}

/// Reaches to the start of the run — [`Horizon::Unbounded`] as a type.
pub struct Unbounded;
impl Reach for Unbounded {
	const HORIZON: Horizon = Horizon::Unbounded;
}

/// The identity lift, and the one place the two vocabularies have to meet: [`Buffer`] is
/// const-generic over the reach it retains (the join `graph!` computed, which no type can name) and
/// still has to state that reach in dep position. Nothing else should reach for this — a dep that
/// knows its own reach spells it [`Over`] or [`Elems`].
#[doc(hidden)]
pub struct At<const H: Horizon>;
impl<const H: Horizon> Reach for At<H> {
	const HORIZON: Horizon = H;
}

/// A [`Cell::NAME`] a parameter can reach. `NAME` is a `const &'static str`, and nothing that
/// formats at runtime can produce one — so the parts are spelled into a fixed buffer, in an
/// *associated* const, whose value outlives the `NAME` borrowing it. That an associated const may
/// read its impl's generics, where a free `const` item may not, is the whole reason this works.
///
/// Overflowing the buffer trips an assert during const eval, i.e. fails the build.
pub struct Tag {
	buf: [u8; 256],
	len: usize,
}

impl Tag {
	/// A cell parameterised by a bare number: `Bars<1>` leaves the reader to guess the unit, where
	/// `Bar:1m` states it.
	pub const fn new(prefix: &str, tf: Timeframe) -> Self {
		let (mut buf, mut len) = ([0u8; 256], 0);
		len = write(&mut buf, len, prefix.as_bytes());
		Self {
			len: timeframe(&mut buf, len, tf),
			buf,
		}
	}

	/// A cell parameterised by other cells, named from theirs: `Rsi<Bar:1m, Len14>` rather than the
	/// `type_name` default's `Rsi<Bars<Timeframe(60000)>, …>`, which spells a dep differently from
	/// the way the same graph's other cards spell it.
	pub const fn of(prefix: &str, params: &[&str]) -> Self {
		assert!(!params.is_empty(), "a tag over no parameters is a string literal");
		let (mut buf, mut len) = ([0u8; 256], 0);
		len = write(&mut buf, len, prefix.as_bytes());
		len = write(&mut buf, len, b"<");
		let mut i = 0;
		while i < params.len() {
			if i > 0 {
				len = write(&mut buf, len, b", ");
			}
			len = write(&mut buf, len, params[i].as_bytes());
			i += 1;
		}
		Self {
			len: write(&mut buf, len, b">"),
			buf,
		}
	}

	/// A second period, for a cell whose identity is the pair: `Change:1m/3m` reads off 1m bars over
	/// three minutes, and is not the same series as `Change:3m/1m`.
	pub const fn then(mut self, tf: Timeframe) -> Self {
		let len = write(&mut self.buf, self.len, b"/");
		Self {
			len: timeframe(&mut self.buf, len, tf),
			buf: self.buf,
		}
	}

	/// A count of elements, where a period alone would leave the window unsaid: `Momentum:5mx181`.
	pub const fn count(mut self, n: usize) -> Self {
		let len = write(&mut self.buf, self.len, b"x");
		Self {
			len: digits(&mut self.buf, len, n as u64),
			buf: self.buf,
		}
	}

	pub const fn as_str(&self) -> &str {
		match core::str::from_utf8(self.buf.split_at(self.len).0) {
			Ok(s) => s,
			Err(_) => panic!("a name is utf8, and is spelled here one whole str at a time"),
		}
	}
}

const fn write(buf: &mut [u8; 256], mut len: usize, src: &[u8]) -> usize {
	// should this ever fire, the alternative to raising the cap is truncating the tail with an ellipsis.
	assert!(
		len + src.len() <= buf.len(),
		"a cell name overflowed Tag's 256-byte buffer: shorten the Cell::NAME of the deps in this cell's tag, or raise the buffer in trading_data_dag::Tag"
	);
	let mut i = 0;
	while i < src.len() {
		buf[len] = src[i];
		len += 1;
		i += 1;
	}
	len
}

const fn digits(buf: &mut [u8; 256], mut len: usize, mut n: u64) -> usize {
	let (mut d, mut rev) = (0, [0u8; 20]);
	while {
		rev[d] = b'0' + (n % 10) as u8;
		n /= 10;
		d += 1;
		n > 0
	} {}
	assert!(
		len + d <= buf.len(),
		"a cell name overflowed Tag's 256-byte buffer: shorten the Cell::NAME of the deps in this cell's tag, or raise the buffer in trading_data_dag::Tag"
	);
	while d > 0 {
		d -= 1;
		buf[len] = rev[d];
		len += 1;
	}
	len
}

const fn timeframe(buf: &mut [u8; 256], len: usize, tf: Timeframe) -> usize {
	let designator = tf.designator();
	let len = digits(buf, len, tf.0 / designator.as_millis());
	write(buf, len, designator.as_str().as_bytes())
}

/// A retained item's own event time. Required of every [`Buffer`]ed item — a history you cannot
/// index by time is one you can only read at an assumed cadence, which is the bug [`Horizon::Over`]
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
	/// carrying parameters overrides it through [`Tag`], because `type_name` renders those the
	/// compiler's way and the rest of the graph spells the same cells its own.
	const NAME: &'static str = core::any::type_name::<Self>();

	/// How far back a consumer naming this in dep position reads. A bare cell is this tick's batch
	/// and nothing more; the wrappers ([`Buffering`], [`Folding`]) are what state anything else. This
	/// lives on [`Cell`] rather than a `Dep` trait because [`DepSet`] is implemented over tuples of
	/// cells, and a blanket `impl<C: Cell> Dep for C` would conflict with the wrapper impls.
	const REACH: Horizon = Horizon::Unit;

	/// Whether [`REACH`](Cell::REACH) is the *node's* to hold — true of [`Folding`] alone. A closed
	/// gate pulls no deps, so a folded reach is the one thing gating cannot re-warm.
	const FOLDED: bool = false;

	/// Whether reading this in dep position reads something the *engine* keeps — [`Buffering`] and
	/// [`Sampling`], against the [`Buffer`]/[`Latest`] beside the source. Everything else is this
	/// tick's batch and nothing more: a bare cell, a [`Folding`] reach the node holds, a
	/// [`Gating`] permission.
	///
	/// This is what says whether a tick may be *withheld* from a consumer. A retained dep is there
	/// again next tick, so skipping one costs nothing; a pass-through dep skipped is a batch nobody
	/// ever sees again. Hence [`Emitter::opens`]: the engine enforces a declared rate only where it
	/// holds every input the node would have read.
	const RETAINED: bool = false;

	/// Whether a tick skipped costs this cell nothing a later tick cannot recover. False by default: a
	/// node that folds a batch it will never see again is the usual case, and a pass-through is the
	/// same thing one hop on.
	///
	/// This is what lets a *latch* enter the demand formula of something upstream of it. A gate stepped
	/// earlier is read off the frame, so it darkens its producers on the same tick its consumer is
	/// dark; a latch is read from [`Latch::standing`] at tick start, so it darkens them one tick ahead
	/// of the consumer arming. Only a cell that says it survives that lost tick may be suppressed by
	/// one.
	///
	/// Here rather than on [`Node`] for the reason [`CLOCK`](Cell::CLOCK) is — an [`Emit`] node has no
	/// [`Node`] impl to state it on, and whether a skip is survivable is the cell's own to say either
	/// way, not something each consumer restates.
	const REWARMS: bool = false;

	/// How often this cell publishes, stated on the cell because the rate is a property of what a
	/// thing *is* and of nothing it reads (`rates.node.declared`). `None` — whenever its inputs do.
	/// `Some(tf)` — over elements whose `tf` period has closed, never re-entered while one is in
	/// progress (`rates.node.whole-elements`).
	///
	/// Here rather than on [`Emit`] for the reason [`REACH`](Cell::REACH) is: [`DepSet`] is over
	/// tuples of cells, so this is what lets a consumer's declared reach be read against its
	/// producer's rate — the `Elems(n)` → duration conversion of [`Horizon::span`] — and what lets
	/// [`graph!`] check a node's clock against the ones feeding it.
	///
	/// Who *enforces* it follows [`RETAINED`](Cell::RETAINED), not the declaration: see
	/// [`Emitter::opens`].
	const CLOCK: Option<Timeframe> = None;

	/// Whether a consumer naming this in dep position is *dominated* by it — [`Gating`] alone. A type
	/// rather than a `const` because it is what dispatches the dark branch ([`Dark`]), and only a type
	/// can demand a [`Latent`] out of gated nodes alone.
	type Gates: Bit = No;

	/// Whether this dep, read here, lets its consumer advance. `true` for every dep but a [`Gating`]
	/// one, whose out is permission rather than data.
	fn opens(_: Self::Out<'_>) -> bool {
		true
	}
}

/// A `bool` in type position, so a dep kind's domination can both dispatch an impl ([`Dark`]) and
/// land in [`graph!`]'s `const` manifest.
pub trait Bit {
	const VALUE: bool;
}
pub struct Yes;
pub struct No;
impl Bit for Yes {
	const VALUE: bool = true;
}
impl Bit for No {
	const VALUE: bool = false;
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

/// [`Flat`]'s inverse: an item rebuilt from the slots a per-element kernel computed, plus the event
/// time. The time is the kernel's to carry rather than a slot, because a slot is a differentiation
/// variable and an index may not be one (`r[kernels.selection.index-is-not-a-variable]`).
pub trait Unflat: Flat {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self;
}

impl Unflat for f64 {
	fn unflat(_: i64, slots: &[f64]) -> Self {
		slots[0]
	}
}

impl<const N: usize> Unflat for [f64; N] {
	fn unflat(_: i64, slots: &[f64]) -> Self {
		core::array::from_fn(|i| slots[i])
	}
}

/// NaN is how a per-element body declines, which the out plane already reads as absence
/// (`r[impl outs.absence.one-reading]`) — so the two spellings of "nothing here" are one.
impl<T: Unflat> Unflat for Option<T> {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		slots.iter().all(|v| !v.is_nan()).then(|| T::unflat(ts_ns, slots))
	}
}

/// A [`Flat`] out whose slots are probabilities over a named outcome space. Evidence reaches one as
/// points rather than shares: a read argues only for what it names, and what nothing argued for is
/// ignorance rather than a uniform guess — so the space has to own a slot to be ignorant in, which
/// is what the `Default` bound buys.
pub trait ProbabilisticDistribution: Flat {
	type Outcome: Copy + Default + PartialEq + 'static;
	/// The outcome space, in the order [`ProbabilisticDistribution::certainty`] reads points in.
	const OUTCOMES: &'static [Self::Outcome];
	/// Every point the evidence could ever have scored. Certainty is measured against its *root*
	/// rather than against itself: one read out of a hundred declared is a whisper, while a handful
	/// that agree is already most of what could be said at all.
	const POINTS: f64;

	/// Points per outcome to probabilities, in place; the unallocated remainder to `Default`'s slot.
	fn certainty(points: &mut [f64]) {
		assert_eq!(points.len(), Self::OUTCOMES.len(), "points are scored against the outcome space");
		assert!(Self::POINTS > 0.0, "nothing can be evidenced in a space nothing argues over");
		assert!(points.iter().all(|p| *p >= 0.0), "evidence against an outcome is the absence of a point, not a point");
		let scored: f64 = points.iter().sum();
		// past the root the evidence is overwhelming and the space is fully allocated, so the
		// division is a plain normalisation from there on.
		let divisor = Self::POINTS.sqrt().max(scored);
		for p in points.iter_mut() {
			*p /= divisor;
		}
		let d = Self::OUTCOMES
			.iter()
			.position(|o| *o == Self::Outcome::default())
			.expect("an outcome space holds its own default");
		points[d] += (1.0 - scored / divisor).max(0.0);
	}
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
// r[impl outs.absence.one-reading]
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
// r[impl outs.absence.one-reading]
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
	/// Names for `slots`, one list per axis of the plot's shape; `[]` = indices. The axis lengths
	/// multiply out to the plot's slot count, in the same row-major order the out flattens in.
	pub labels: &'static [&'static [&'static str]],
	/// Per-element; `[]` = [`Ink::MAIN`] for all.
	pub inks: &'static [Ink],
	/// Draw on the price pane instead of an own indicator pane; price-denominated.
	pub overlay: bool,
	/// Claim a whole pane, placed under the layer pane this plot would otherwise share. For plots
	/// whose shape is the point (a sparse bar column, a step wave) and that neighbours bury.
	/// Meaningless under `overlay`.
	pub solo: bool,
	/// Draw as bars (stacked, when the plot has several slots) instead of lines. For discrete,
	/// sparse acts — a continuous series drawn this way is a wall of ink that hides its neighbours.
	pub bars: bool,
	/// The plot's four slots are o·h·l·c, drawn as candle outlines rather than four lines.
	/// Price-denominated by construction, so `overlay`.
	pub candles: bool,
}

impl Plot {
	pub const DEFAULT: Plot = Plot {
		slots: &[],
		range: None,
		guides: &[],
		labels: &[],
		inks: &[],
		overlay: false,
		solo: false,
		bars: false,
		candles: false,
	};

	/// `[]` slots means "every slot", which two plots cannot both claim. A candle plot is four
	/// price-denominated slots, so it names them and rides the price pane. `dims` is the out's own
	/// shape, which a plot claiming all of it is named against.
	const fn coherent(plots: &'static [Plot], dims: &'static [usize]) -> bool {
		let mut i = 0;
		while i < plots.len() {
			if plots[i].slots.is_empty() && plots.len() > 1 {
				return false;
			}
			if plots[i].candles && (!plots[i].overlay || plots[i].slots.len() != 4) {
				return false;
			}
			if !plots[i].labels.is_empty() {
				let mut named = 1;
				let mut a = 0;
				while a < plots[i].labels.len() {
					named *= plots[i].labels[a].len();
					a += 1;
				}
				let mut drawn = 1;
				if plots[i].slots.is_empty() {
					let mut d = 0;
					while d < dims.len() {
						drawn *= dims[d];
						d += 1;
					}
				} else {
					drawn = plots[i].slots.len();
				}
				if named != drawn {
					return false;
				}
			}
			i += 1;
		}
		true
	}
}

pub trait DepSet {
	type Outs<'t>;
	const NAMES: &'static [&'static str];
	/// Per-dep [`Cell::REACH`], positionally — how far back a revived node must look, input by input.
	const REACH: &'static [Horizon];
	/// Per-dep [`Cell::FOLDED`], positionally. With `NAMES` and `REACH` this is what picks the frame
	/// field a dep resolves against: folded or `Unit` reads the cell itself, anything else reads the
	/// [`Buffer`] retaining it.
	const FOLDS: &'static [bool];
	/// Per-dep [`Cell::RETAINED`], positionally — which of these inputs survive a tick the node is
	/// not run on, and so whether a declared rate is the engine's to enforce.
	const RETAINS: &'static [bool];
	/// Per-dep [`Cell::CLOCK`], positionally — the rates feeding this node, which is what a
	/// consumer's own rate is checked against and what turns its `Elems(n)` reach into a duration.
	const CLOCKS: &'static [Option<Timeframe>];
	/// Per-dep [`Cell::Gates`], positionally — which of these inputs are the node's gates.
	const GATES: &'static [bool];
	/// The leading dep's [`Cell::Gates`], which — gating deps leading, as [`Pull::open`] const-asserts
	/// — is "is this node gated at all", in the type position [`Dark`] dispatches on.
	type Lead: Bit;
}

const fn any(flags: &[bool]) -> bool {
	let mut i = 0;
	while i < flags.len() {
		if flags[i] {
			return true;
		}
		i += 1;
	}
	false
}

const fn all(flags: &[bool]) -> bool {
	let mut i = 0;
	while i < flags.len() {
		if !flags[i] {
			return false;
		}
		i += 1;
	}
	true
}

/// Whether every gating dep precedes every plain one — see [`Pull::open`].
const fn gating_leads(gates: &[bool]) -> bool {
	let mut i = 0;
	while i < gates.len() && gates[i] {
		i += 1;
	}
	while i < gates.len() {
		if gates[i] {
			return false;
		}
		i += 1;
	}
	true
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
	/// How many elements the *leading* dep fired. That dep is the one a per-element kernel walks —
	/// its rate is the node's — so which dep drives is structural rather than a per-node declaration.
	fn lead_fires(outs: &Self::Outs<'_>) -> usize;
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
///
/// A generic node writes its parameters (bounds and all) in leading brackets:
/// `slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, f64)`. A third argument names a [`Batch`]
/// other than the default [`Rows`].
#[macro_export]
macro_rules! slice_nudge {
	([$($g:tt)*] $C:ty, $E:ty, $B:ty) => {
		impl<$($g)*> $crate::Series for $C {
			type Item = $E;
			type Batch = $B;
		}

		impl<$($g)*> $crate::Nudge for $C {
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
	([$($g:tt)*] $C:ty, $E:ty) => {
		$crate::slice_nudge!([$($g)*] $C, $E, $crate::Rows<$E>);
	};
	($C:ty, $E:ty, $B:ty) => {
		$crate::slice_nudge!([] $C, $E, $B);
	};
	($C:ty, $E:ty) => {
		$crate::slice_nudge!([] $C, $E, $crate::Rows<$E>);
	};
}

/// A value-out cell's finite-difference witness: the scratch is just the value itself.
#[macro_export]
macro_rules! value_nudge {
	([$($g:tt)*] $C:ty) => {
		impl<$($g)*> $crate::Nudge for $C {
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
	($C:ty) => {
		$crate::value_nudge!([] $C);
	};
}

/// Extracts a [`DepSet`]'s outputs from frame `F`. `I` is the inferred index path — never
/// named by callers.
pub trait Pull<'t, F, I>: DepSet {
	fn pull(f: &F) -> Self::Outs<'t>
	where
		F: 't;

	/// Whether every [`Gating`] dep of this set reads true — the one question answerable without
	/// pulling anything else, and the reason gating deps must *lead* the tuple: the conjunction runs
	/// in dep order, so a closed gate short-circuits before a plain dep is so much as read.
	fn open(f: &F) -> bool
	where
		F: 't;
}

#[diagnostic::on_unimplemented(
	message = "`{Self}` has no unfired reading — a node read only behind a gate must be able to decline: make its out `Option`-valued, or give it a consumer that is not gated"
)]
/// No `Copy` supertrait: a [`Cell::Out`] is `Copy` already, and stating it twice makes every
/// signature that bounds an out by both this and [`Flat`] ambiguous over which clause proves it.
pub trait Latent {
	fn latent() -> Self;
}
impl<T: Copy> Latent for Option<T> {
	fn latent() -> Self {
		None
	}
}
/// A dark batch node emits nothing, which [`Flat`] already reads as `fires() == 0` / unfired.
impl<T> Latent for &[T] {
	fn latent() -> Self {
		// core's `impl<T> Default for &[T]`; a bare `&[]` would lean on promoting `[T; 0]`.
		Default::default()
	}
}

/// What a node reads on a tick its gate refused, dispatched on [`DepSet::Lead`] so the [`Latent`]
/// bound lands on gated nodes alone — an ungated node is never dark, and demanding an unfired
/// reading of it would rule out every scalar-out cell in the graph.
pub trait Dark<B: Bit> {
	fn dark() -> Self;
}
impl<T> Dark<No> for T {
	fn dark() -> Self {
		unreachable!("an ungated node's `open` is const-true, so nothing reaches its dark branch")
	}
}
impl<T: Latent> Dark<Yes> for T {
	fn dark() -> Self {
		T::latent()
	}
}

/// A cell the sweep advances. It carries no compute method of its own: it names the [`Level`] kernel
/// that computes it, and every reading the engine offers — the value, the formula, the Jacobian, the
/// value-annotated trace — comes off that one declaration (`r[kernels.closed]`).
///
/// Nobody writes this impl by hand; `#[node]` writes it from the body trait ([`Symbolic`] ⇒ [`Pure`],
/// [`Blind`] ⇒ [`Opaque`]), which is also where `Deps` and `PLOTS` are stated.
pub trait Node: Cell {
	type Deps: DepSet;
	type Kernel: Level<Self>;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep, and
	/// the viz can still be asked for it.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
}

/// [`Node`]'s run-shaped sibling: the out is the run of items filled each tick, and like `Node` it
/// carries no compute method — it names the [`Run`] kernel that computes it, and every reading comes
/// off that one declaration (`r[kernels.closed]`).
///
/// Nobody writes this impl by hand either; `#[node]` writes it from the body trait ([`Runs`] ⇒
/// [`Raw`]), which is also where `Deps` and `PLOTS` are stated.
///
/// [`Series`]' where-clause is an obligation at each use of the bound, not an implied one (see
/// [`Episodic`]), so it is repeated wherever this bound is used.
pub trait Emit: Series
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	type Kernel: Run<Self>;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep, and
	/// the viz can still be asked for it.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
}

/// The run side's stated hatch — [`Blind`]'s sibling: a node that fills its run in Rust and
/// therefore has no algebra reading. The engine owns the run, so the struct holds only what it
/// remembers between ticks, and `emit` cannot read what it wrote last tick. `&mut self`, not
/// `&'t mut self`: the node is not lent for the tick, only the engine's buffer is.
///
/// A gated one goes dark by emitting nothing, so it needs no [`Latent`] reading — not emitting *is*
/// the latent reading.
pub trait Runs: Series + Clone
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	/// Why this node has no formula. Written for whoever asks why it is not a per-element kernel.
	const WHY: &'static str;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep, and
	/// the viz can still be asked for it.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn emit<'t>(&mut self, deps: RunOuts<'t, Self>, out: &mut alloc::vec::Vec<Self::Item>);
}

/// Uniform binder-correct dep-tuple type for an [`Emit`]'s kernel, as [`DepOuts`] is for a `Node`'s.
pub type EmitOuts<'t, E> = <<E as Emit>::Deps as DepSet>::Outs<'t>;

/// The same, spelled through the body trait — [`BlindOuts`]' sibling, and the same type, so a
/// [`Runs`] impl may write either.
pub type RunOuts<'t, R> = <<R as Runs>::Deps as DepSet>::Outs<'t>;

/// The engine-owned buffer an [`Emit`] fills, and the node itself. Never typed by a human —
/// [`graph!`]'s `emit` keyword wraps the declared node type in one, and [`Deref`](core::ops::Deref)
/// makes the wrapper invisible to every read of the graph field.
#[doc(hidden)]
pub struct Emitter<E: Emit>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>, {
	node: E,
	buf: alloc::vec::Vec<E::Item>,
	/// The [`Emit::CLOCK`] period `emit` last ran in; `i64::MIN` is "none yet", which no timestamp
	/// divides to. Carried by the wrapper rather than the node for the same reason the buffer is:
	/// the rate is declared, and what enforces it is not the declarer's to fiddle with.
	last_period: i64,
}

// hand-written: `derive` would demand `E::Item: Default`, which no buffer needs.
impl<E: Emit + Default> Default for Emitter<E>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	fn default() -> Self {
		Self {
			node: E::default(),
			buf: alloc::vec::Vec::new(),
			last_period: i64::MIN,
		}
	}
}

/// The buffer is not cloned: [`step_emit`] clears it before `emit` runs and `emit` only ever sees
/// `&mut Vec`, so prior contents are unreachable by construction.
impl<E: Emit + Clone> Clone for Emitter<E>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	fn clone(&self) -> Self {
		Self {
			node: self.node.clone(),
			buf: alloc::vec::Vec::new(),
			last_period: self.last_period,
		}
	}
}

impl<E: Emit + Default> Emitter<E>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	/// [`graph!`]'s commutation reset, where a plain field takes `Default::default()`. The buffer is
	/// cleared rather than replaced: an episode's run length is about what the last one's was, and
	/// the capacity is the one thing worth carrying across the dark.
	#[doc(hidden)]
	pub fn reset(&mut self) {
		self.node = E::default();
		self.buf.clear();
		self.last_period = i64::MIN;
	}
}

impl<E: Emit> Emitter<E>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	/// Whether [`Emit::CLOCK`] admits this tick: an unclocked node every one, a clocked one the first
	/// of each of its periods. Read after the gate, so a shut node consumes no period and the first
	/// tick it is let through is a boundary rather than the remainder of one it slept out.
	///
	/// Only a node reading what the engine keeps ([`Cell::RETAINED`]) is one the engine may withhold
	/// a tick from: a withheld tick is a batch never delivered, and a pass-through dep — a bare cell,
	/// a [`Folding`] reach, a [`Gating`] permission — has no second showing of it. Such a
	/// node is clocked by the element walk it already runs (`rates.folds.exactly-once`), and the
	/// declaration is all the engine takes from it.
	fn opens(&mut self, ts: i64) -> bool {
		let Some(tf) = <E as Cell>::CLOCK else { return true };
		if !all(<<E as Emit>::Deps as DepSet>::RETAINS) {
			return true;
		}
		let period = ts / Horizon::ns(tf);
		core::mem::replace(&mut self.last_period, period) != period
	}
}

impl<E: Emit> core::ops::Deref for Emitter<E>
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	type Target = E;

	fn deref(&self) -> &E {
		&self.node
	}
}

/// A scalar-out node whose per-tick value is a pure [`Expr`] of its (scalar / last-element) deps —
/// each dep read at its [`Flat`] scalar, a batch dep as its last element, matching the observer's
/// end-of-batch view. Earns the [`Pure`] kernel, which is the whole of how it computes: there is no
/// second way to state the value, so the algebra is load-bearing.
///
/// `Out = f64` has no `None` channel: reading historic (warmup) deps emits `NaN` yet still reports
/// `fired = true`, so don't route a warmup-sensitive consumer off a Symbolic node unguarded.
pub trait Symbolic: Cell
where
	for<'t> Self: Cell<Out<'t> = f64>, {
	type Deps: DepSet;
	/// `&[]` draws nothing at all — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn body(&self, vars: Vars) -> impl Expr;
}

/// The stated hatch: a node that computes in Rust and therefore has no algebra reading. `WHY` is not
/// documentation, it is the cost of using it (`r[kernels.opaque.stated]`) — `graph!` counts these,
/// and a graph that pins the count only lets it fall.
///
/// [`Clone`] because the finite-difference witness is what stands in for the missing derivative: it
/// re-advances a clone restored from the pre-advance state, once per dep slot, which is exactly the
/// price a [`Symbolic`] body does not pay.
pub trait Blind: Cell + Clone {
	type Deps: DepSet;
	/// Why this node has no formula. Written for whoever asks why it is not `Symbolic`.
	const WHY: &'static str;
	/// `&[]` draws nothing at all — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn advance<'t>(&'t mut self, deps: BlindOuts<'t, Self>) -> Self::Out<'t>;
}

/// Uniform binder-correct dep-tuple type for [`Blind::advance`] impls — [`DepOuts`] spelled through
/// the body trait, and the same type, so an impl may write either.
pub type BlindOuts<'t, B> = <<B as Blind>::Deps as DepSet>::Outs<'t>;

/// scalar deps ⇒ one env slot each; the `impl_arity` tuple ceiling caps arity, so a fixed stack
/// array holds the whole env — zero heap on the compute path. A per-element kernel reads whole
/// items rather than scalars, which is what put this past a dep count.
const MAX_VARS: usize = 16;

/// The widest item a per-element kernel fills, for the same reason [`MAX_VARS`] exists: the slot
/// buffer is a stack array, so the compute path allocates nothing.
const MAX_SLOTS: usize = 8;

/// `impl_arity`'s ceiling, so the per-dep bookkeeping an exact block needs is a stack array too.
const MAX_DEPS: usize = 12;

mod sealed {
	pub trait Kernel {}
	pub trait Witness {}
}

/// Where one env slot came from. Every reading a `read` takes *copies* an element's slot into an env
/// slot and never computes one, so `∂env/∂element` is a 0/1 selection — which is what lets a body's
/// gradient scatter over a dep's whole reach in one pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Source {
	/// A discrete attribute, a threshold, a kernel's own state — nothing a column stands for.
	#[default]
	Opaque,
	/// Slot `slot` of the element `lag` back of dep `dep`, `lag` 0 being the element that dep's
	/// [`Jac`] column describes.
	Dep { dep: usize, lag: usize, slot: usize },
}

/// Whether a reading is being watched. A type parameter rather than a flag, so the compute path
/// monomorphizes the recording away entirely — `kernel_cost`'s `scan == raw` is the check that it
/// did. Sealed: both answers are the engine's.
pub trait Witness: sealed::Witness + Default {
	fn see(&mut self, slot: usize, src: Source);
}

/// The compute path's witness: it sees nothing, and costs nothing.
#[derive(Default)]
pub(crate) struct Blindfold;
impl sealed::Witness for Blindfold {}
impl Witness for Blindfold {
	fn see(&mut self, _: usize, _: Source) {}
}

/// What an exact Jacobian is scattered through: where each env slot of one reading came from.
#[derive(Clone, Copy)]
pub(crate) struct Provenance([Source; MAX_VARS]);
impl Default for Provenance {
	fn default() -> Self {
		Self([Source::Opaque; MAX_VARS])
	}
}
impl sealed::Witness for Provenance {}
impl Witness for Provenance {
	fn see(&mut self, slot: usize, src: Source) {
		self.0[slot] = src;
	}
}

/// What a per-element kernel's `read` fills: the body's env, and — where one is watching — which
/// element of which dep each slot was copied from.
///
/// Slots are appended in the order they are put, so a site names the dep and the lag rather than
/// counting offsets by hand.
pub struct Env<'a, W: Witness> {
	slots: &'a mut [f64],
	at: usize,
	witness: W,
}

impl<'a, W: Witness> Env<'a, W> {
	/// The values written next come off dep `dep`, at [`lag`](Put::lag) 0 until said otherwise.
	pub fn dep(&mut self, dep: usize) -> Put<'_, 'a, W> {
		Put { env: self, src: Some((dep, 0, 0)) }
	}

	/// A reading no element stands behind — a side, a count, a threshold. Written like any other,
	/// and claimed by no column.
	pub fn opaque(&mut self) -> Put<'_, 'a, W> {
		Put { env: self, src: None }
	}

	pub(crate) fn new(slots: &'a mut [f64]) -> Self {
		Self {
			slots,
			at: 0,
			witness: W::default(),
		}
	}

	/// Back to the start, for the next element of the walk. The witness is left standing: only the
	/// slots the next reading fills are ever read off it.
	pub(crate) fn rewind(&mut self) {
		self.at = 0;
	}

	/// What the reading filled — the env the body is evaluated over, and its width.
	pub(crate) fn filled(&self) -> &[f64] {
		&self.slots[..self.at]
	}

	/// The whole buffer, for a kernel appending its own state past the reading.
	pub(crate) fn buf(&mut self) -> &mut [f64] {
		self.slots
	}

	pub(crate) fn seen(&self) -> &W {
		&self.witness
	}
}

/// One reading being written into an [`Env`], and where it came from.
pub struct Put<'e, 'a, W: Witness> {
	env: &'e mut Env<'a, W>,
	/// `(dep, lag, slot)`, or nothing a derivative has a variable for.
	src: Option<(usize, usize, usize)>,
}

impl<W: Witness> Put<'_, '_, W> {
	/// How far back the element read stands from the one this dep's [`Jac`] column describes.
	pub fn lag(mut self, lag: usize) -> Self {
		self.src = self.src.map(|(d, _, s)| (d, lag, s));
		self
	}

	/// Which slot of the dep's element the first value written is — for a reading that takes one
	/// field rather than the whole element.
	pub fn slot(mut self, slot: usize) -> Self {
		self.src = self.src.map(|(d, l, _)| (d, l, slot));
		self
	}

	/// Appends `v`'s slots, provenance and all.
	pub fn put<T: Flat>(self, v: &T) {
		let Put { env, src } = self;
		let (at, n) = (env.at, T::LEN);
		v.flat(&mut env.slots[at..at + n]);
		for k in 0..n {
			env.witness.see(
				at + k,
				match src {
					Some((dep, lag, slot)) => Source::Dep { dep, lag, slot: slot + k },
					None => Source::Opaque,
				},
			);
		}
		env.at = at + n;
	}
}

/// How much of what a node's body read the kernel's *fullest* reading covers
/// (`r[kernels.fidelity.stated]`) — [`Level::jac`] where there is nothing wider, and
/// [`Level::exact`] where there is. Three answers rather than two, because the middle one is the
/// case nothing else can show: a derivative may be algebraic and still be narrower than the reading
/// it was taken from, and a partial derivative of a body that reads a window looks exactly like an
/// exact one of a body that reads a point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fidelity {
	/// Every element the body read is covered.
	Exact,
	/// Algebraic, but narrower than the body's reach — the string says what it omits.
	Partial(&'static str),
	/// No algebra at all — the string says why (`r[kernels.opaque.stated]`).
	Opaque(&'static str),
}

/// How a node computes, and therefore what can be read off it. Sealed: the kernel set is the
/// framework's, so a node's only choice is which one it names and there is no way to supply a body
/// the engine cannot also read (`r[kernels.closed]`).
///
/// Each kernel demands its own body trait, so naming one without the body does not compile. The
/// split into [`pre`](Level::pre) and [`jac`](Level::jac) is what lets the two readings cost their
/// own price: [`Pure`] captures one `grad` pass and nothing else, where [`Opaque`] captures the
/// pre-advance clone its finite differences re-advance.
pub trait Level<N: Node + ?Sized>: sealed::Kernel {
	/// How much of what the body read [`jac`](Level::jac) covers.
	const FIDELITY: Fidelity;

	fn advance<'t>(n: &'t mut N, deps: DepOuts<'t, N>) -> N::Out<'t>;

	/// Whatever the derivative reading needs off the node *before* `advance` consumes it for the
	/// tick. Below [`Want::Jac`] every kernel captures nothing, which is what makes an unobserved
	/// sweep the bare chain.
	type Pre;
	fn pre<'d>(n: &N, want: Want, deps: DepOuts<'d, N>) -> Self::Pre;

	/// The equation, where there is one — [`None`] under [`Want::Jac`] means this kernel has no
	/// algebra, not that the node did not fire.
	fn formula(pre: &Self::Pre) -> Option<&Ast>;

	/// Fills the Jacobian and reports whether it is exact. Called once, for a node that fired at
	/// [`Want::Jac`]. What it fills is the *one-step* reading — each dep's last element perturbed,
	/// prior state held fixed — however the kernel arrived there; the derivative over a dep's whole
	/// reach is a different quantity and is not this one (`r[kernels.jac.two-quantities]`).
	///
	/// The observation bounds sit here rather than on the trait, so a bare [`step`] — which never asks
	/// for a derivative — carries none of them.
	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat;

	/// Fills the exact block and reports whether it did. The default fills nothing and says so — a
	/// kernel that cannot answer over a dep's whole reach has to be able to decline rather than
	/// fabricate a block.
	fn exact<'d>(_: &Self::Pre, _: Exact<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		false
	}
}

/// What a Jacobian is read against: this tick's pulled deps, their un-bumped flattenings, and the
/// row-major `out_len × dep_len` buffer to fill. Generic over the dep tuple rather than the node, so
/// the level side and the run side read against one type.
pub struct Jac<'a, D> {
	pub deps: D,
	pub dep_buf: &'a [f64],
	pub out_buf: &'a [f64],
	pub out: &'a mut [f64],
	/// The sweep's re-advance scratch, for a kernel that has to bump its way to a column. Empty of
	/// meaning between calls — it is here so a finite difference costs no allocation per node.
	pub bumped: &'a mut alloc::vec::Vec<f64>,
}

/// What an *exact* Jacobian is read against — [`Jac`]'s sibling, and generic over the dep tuple for
/// the same reason, so one type serves the level side and the run side.
///
/// Where [`Jac`] has one column group per dep, describing that dep's newest element, this has one
/// per **lag** of the dep's reach: a body reading a 181-bar window gets 181 groups where the
/// Jacobian gets one (`r[kernels.jac.two-quantities]`).
pub struct Exact<'a, D> {
	pub deps: D,
	pub dep_buf: &'a [f64],
	pub out_buf: &'a [f64],
	/// Per dep in `Deps` order, `out_len × widths[d]` row-major, appended: the kernel grows it,
	/// because a wall-clock window over an aperiodic series has no static element count ever.
	/// Within a dep, oldest lag first — so a dep's last element-group is exactly its [`Jac`] column.
	pub out: &'a mut alloc::vec::Vec<f64>,
	/// `lags(d) * slots(d)`, pushed as the kernel goes; prefix sums recover `(dep, lag, slot)`.
	pub widths: &'a mut alloc::vec::Vec<usize>,
}

/// The block of a kernel that read every dep at one lag: its Jacobian, re-laid per dep — a block
/// groups by dep where a Jacobian row spans all of them. `grad` is row-major `out_len × stride`
/// with the deps' own slots leading each row, exactly as `jac` reads it.
fn point_block(grad: &[f64], stride: usize, out_len: usize, dims: &'static [&'static [usize]], out: &mut alloc::vec::Vec<f64>, widths: &mut alloc::vec::Vec<usize>) {
	widths.clear();
	widths.extend(dims.iter().map(|d| d.iter().product::<usize>()));
	out.clear();
	let mut off = 0;
	for w in &**widths {
		for row in 0..out_len {
			out.extend_from_slice(&grad[row * stride + off..row * stride + off + w]);
		}
		off += w;
	}
}

/// The block of a kernel whose reading reaches past the point: the body's gradient scattered through
/// the provenance of the env it was taken over. One pass places all of it, because every slot of
/// that env is a copy of one element slot and nothing else.
fn lagged_block(grad: &[f64], stride: usize, out_len: usize, dims: &'static [&'static [usize]], prov: &Provenance, out: &mut alloc::vec::Vec<f64>, widths: &mut alloc::vec::Vec<usize>) {
	debug_assert!(dims.len() <= MAX_DEPS, "a dep tuple is capped at `impl_arity`'s ceiling");
	// one lag at minimum: a dep nothing named still owns the column its `Jac` entry stands in.
	let mut lags = [1usize; MAX_DEPS];
	for src in &prov.0[..stride] {
		if let Source::Dep { dep, lag, .. } = *src {
			lags[dep] = lags[dep].max(lag + 1);
		}
	}
	widths.clear();
	widths.extend(dims.iter().enumerate().map(|(d, dim)| lags[d] * dim.iter().product::<usize>()));
	out.clear();
	// zero, not the NaN a Jacobian leaves: a lag the body did not read has a partial *of zero*, and
	// saying so is the whole of what the block adds over the one-step column.
	out.resize(widths.iter().map(|w| out_len * w).sum(), 0.0);
	let (mut base, mut acc) = ([0usize; MAX_DEPS], 0);
	for (d, w) in widths.iter().enumerate() {
		base[d] = acc;
		acc += out_len * w;
	}
	for (slot, src) in prov.0[..stride].iter().enumerate() {
		let Source::Dep { dep, lag, slot: s } = *src else { continue };
		let per = widths[dep] / lags[dep];
		let col = (lags[dep] - 1 - lag) * per + s;
		for row in 0..out_len {
			out[base[dep] + row * widths[dep] + col] += grad[row * stride + slot];
		}
	}
}

/// The kernel of a [`Symbolic`] node: the value is `body().eval`, the Jacobian is `body().grad` —
/// exact, and one pass rather than a clone and a re-advance per dep slot.
pub struct Pure;
impl sealed::Kernel for Pure {}

/// The kernel of a [`Blind`] node: the value is the node's own `advance`, and the Jacobian is the
/// finite-difference witness taken off a clone.
pub struct Opaque;
impl sealed::Kernel for Opaque {}

/// A [`Symbolic`] node's pre-advance capture: the equation, and its gradient at this tick's deps.
/// Both are read *before* `advance`, for the same reason the [`Opaque`] clone is — the derivative
/// belongs to the state the value was computed from.
pub struct Algebra {
	formula: Ast,
	grad: [f64; MAX_VARS],
}

/// The env a [`Symbolic`] body is read over: one scalar slot per dep, in `Deps` order.
fn env_of<S>(deps: &<<S as Symbolic>::Deps as DepSet>::Outs<'_>) -> [f64; MAX_VARS]
where
	S: Symbolic,
	for<'t> S: Cell<Out<'t> = f64>,
	<S as Symbolic>::Deps: DepFlat, {
	const {
		let n = <<S as Symbolic>::Deps as DepSet>::NAMES.len();
		assert!(
			<<S as Symbolic>::Deps as DepFlat>::LEN == n,
			"Symbolic deps must be scalar (one env slot each): a vector-valued dep desyncs Var<I> from dep I"
		);
		assert!(n <= MAX_VARS, "Symbolic arity exceeds the env buffer (MAX_VARS)");
	}
	let mut env = [0.0f64; MAX_VARS];
	<<S as Symbolic>::Deps as DepFlat>::flat(deps, &mut env[..<<S as Symbolic>::Deps as DepFlat>::LEN]);
	env
}

impl<N> Level<N> for Pure
where
	N: Symbolic + Node<Deps = <N as Symbolic>::Deps>,
	for<'t> N: Cell<Out<'t> = f64>,
	<N as Symbolic>::Deps: DepFlat,
{
	type Pre = Option<Algebra>;

	/// `env_of` const-asserts one scalar env slot per dep, so a `Symbolic` body's reach is a point
	/// whatever the dep wrapper retains — and a gradient at a point covers all of it.
	const FIDELITY: Fidelity = Fidelity::Exact;

	fn advance<'t>(n: &'t mut N, deps: DepOuts<'t, N>) -> N::Out<'t> {
		let env = env_of::<N>(&deps);
		n.body(Vars).eval(&env[..<<N as Symbolic>::Deps as DepFlat>::LEN])
	}

	fn pre<'d>(n: &N, want: Want, deps: DepOuts<'d, N>) -> Self::Pre {
		if want < Want::Jac {
			return None;
		}
		let env = env_of::<N>(&deps);
		// zeroed, not NaN-filled: `grad` accumulates (`+=`), and a var the body never reads has a
		// partial of 0 rather than no partial.
		let mut grad = [0.0f64; MAX_VARS];
		let body = n.body(Vars);
		body.grad(&env[..<<N as Symbolic>::Deps as DepFlat>::LEN], 1.0, &mut grad);
		Some(Algebra { formula: body.lower(), grad })
	}

	fn formula(pre: &Self::Pre) -> Option<&Ast> {
		pre.as_ref().map(|a| &a.formula)
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		let Some(alg) = pre else { return false };
		// scalar out ⇒ the whole Jacobian is the one gradient row.
		j.out.copy_from_slice(&alg.grad[..j.out.len()]);
		true
	}

	/// One lag per dep, which is the point the body read: `env_of` const-asserts a scalar env slot
	/// each, so the block is the Jacobian row and the widths are `DepFlat::DIMS`' products.
	fn exact<'d>(pre: &Self::Pre, x: Exact<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		let Some(alg) = pre else { return false };
		point_block(&alg.grad, MAX_VARS, 1, <<N as Node>::Deps as DepFlat>::DIMS, x.out, x.widths);
		true
	}
}

impl<N> Level<N> for Opaque
where
	N: Blind + Node<Deps = <N as Blind>::Deps>,
{
	type Pre = Option<N>;

	const FIDELITY: Fidelity = Fidelity::Opaque(<N as Blind>::WHY);

	fn advance<'t>(n: &'t mut N, deps: DepOuts<'t, N>) -> N::Out<'t> {
		<N as Blind>::advance(n, deps)
	}

	fn pre<'d>(n: &N, want: Want, _: DepOuts<'d, N>) -> Self::Pre {
		(want >= Want::Jac).then(|| n.clone())
	}

	fn formula(_: &Self::Pre) -> Option<&Ast> {
		None
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		if let Some(pre) = pre {
			fd_jac::<N>(pre, j);
		}
		false
	}
}

/// A scalar `bool` node: a *driving* dep that clocks it — the leading one, whose having fired at all
/// is half the verdict — and whatever it reads at their last elements, compared by an [`Expr`].
///
/// That split is [`Scans`]', one out element instead of a run: a screen over a series that closed
/// nothing this tick has screened nothing, and saying so through the driver's rate rather than
/// through an emptiness test inside the body is what keeps the gate a pulse instead of a level.
pub trait Decides: Cell
where
	for<'t> Self: Cell<Out<'t> = bool>, {
	type Deps: DepSet;
	/// `&[]` draws nothing at all — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	/// True is any non-zero. The deps are read at their [`Flat`] last elements, in `Deps` order.
	fn body(&self, vars: Vars) -> impl Expr;
}

/// The kernel of a [`Decides`] node.
pub struct Predicate;
impl sealed::Kernel for Predicate {}

/// The deps' last elements, as the env a [`Decides`] body is read over.
fn verdict_env<N>(deps: &DepOuts<'_, N>) -> [f64; MAX_VARS]
where
	N: Node,
	<N as Node>::Deps: DepFlat, {
	const {
		assert!(<<N as Node>::Deps as DepFlat>::LEN <= MAX_VARS, "a Decides env outgrew MAX_VARS: raise it in trading_data_dag");
	}
	let mut env = [f64::NAN; MAX_VARS];
	<<N as Node>::Deps as DepFlat>::flat(deps, &mut env[..<<N as Node>::Deps as DepFlat>::LEN]);
	env
}

impl<N> Level<N> for Predicate
where
	N: Decides + Node<Deps = <N as Decides>::Deps>,
	for<'t> N: Cell<Out<'t> = bool>,
	<N as Decides>::Deps: DepFlat,
{
	type Pre = Option<Ast>;

	/// A predicate's slope is zero off the boundary, and at the boundary it is a step no reading may
	/// call a slope — so an all-zero Jacobian is the whole truth about it, not an omission.
	const FIDELITY: Fidelity = Fidelity::Exact;

	fn advance<'t>(n: &'t mut N, deps: DepOuts<'t, N>) -> N::Out<'t> {
		let len = <<N as Node>::Deps as DepFlat>::lead_fires(&deps);
		let env = verdict_env::<N>(&deps);
		len > 0 && n.body(Vars).eval(&env[..<<N as Node>::Deps as DepFlat>::LEN]) != 0.0
	}

	fn pre<'d>(n: &N, want: Want, _: DepOuts<'d, N>) -> Self::Pre {
		(want >= Want::Jac).then(|| n.body(Vars).lower())
	}

	fn formula(pre: &Self::Pre) -> Option<&Ast> {
		pre.as_ref()
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		if pre.is_none() {
			return false;
		}
		// zero, not the NaN the caller filled: a predicate *has* a derivative here and it is zero,
		// which is a different statement from having no signal (§1.6).
		j.out.fill(0.0);
		true
	}

	/// A slope of zero at every lag as much as at the point — same reading, more of it.
	fn exact<'d>(pre: &Self::Pre, x: Exact<'_, DepOuts<'d, N>>) -> bool
	where
		<N as Node>::Deps: DepFlat,
		DepOuts<'d, N>: Copy,
		for<'x> N::Out<'x>: Flat, {
		if pre.is_none() {
			return false;
		}
		point_block(&[0.0; MAX_VARS], MAX_VARS, 1, <<N as Node>::Deps as DepFlat>::DIMS, x.out, x.widths);
		true
	}
}

/// [`Level`]'s run-shaped sibling: how an [`Emit`] fills its run, and therefore what can be read off
/// it. Sealed by the same [`sealed::Kernel`], and split into [`pre`](Run::pre) and [`jac`](Run::jac)
/// for the same reason — so a sweep nobody is observing captures nothing.
pub trait Run<E: Emit + ?Sized>: sealed::Kernel
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>, {
	/// How much of what the body read [`jac`](Run::jac) covers.
	const FIDELITY: Fidelity;

	fn emit<'t>(e: &mut E, deps: EmitOuts<'t, E>, out: &mut alloc::vec::Vec<E::Item>);

	/// Whatever the derivative reading needs off the node *before* `emit` consumes it for the tick.
	type Pre;
	fn pre<'d>(e: &E, want: Want, deps: EmitOuts<'d, E>) -> Self::Pre;

	/// The equation, where there is one — see [`Level::formula`].
	fn formula(pre: &Self::Pre) -> Option<&Ast>;

	/// Fills the Jacobian and reports whether it is exact — the *one-step* reading, exactly as
	/// [`Level::jac`] is (`r[kernels.jac.two-quantities]`).
	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat;

	/// The exact reading, exactly as [`Level::exact`] is — and defaulting to the same refusal.
	fn exact<'d>(_: &Self::Pre, _: Exact<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		false
	}
}

/// The kernel of a [`Runs`] node: the run is the node's own `emit`, and the Jacobian is the
/// finite-difference witness taken off a clone. [`Opaque`]'s run-shaped sibling.
pub struct Raw;
impl sealed::Kernel for Raw {}

impl<E> Run<E> for Raw
where
	E: Runs + Emit<Deps = <E as Runs>::Deps>,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
{
	type Pre = Option<E>;

	const FIDELITY: Fidelity = Fidelity::Opaque(<E as Runs>::WHY);

	fn emit<'t>(e: &mut E, deps: EmitOuts<'t, E>, out: &mut alloc::vec::Vec<E::Item>) {
		<E as Runs>::emit(e, deps, out)
	}

	fn pre<'d>(e: &E, want: Want, _: EmitOuts<'d, E>) -> Self::Pre {
		(want >= Want::Jac).then(|| e.clone())
	}

	fn formula(_: &Self::Pre) -> Option<&Ast> {
		None
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		if let Some(pre) = pre {
			fd_jac_emit::<E>(pre, j);
		}
		false
	}
}

/// A run-shaped node computing one out element per element of its **driving** dep — the leading one
/// in `Deps`, whose rate it therefore preserves — carrying nothing between elements. Every number is
/// an [`Expr`], one per slot of the item; *which* elements of which deps the expression reads is
/// Rust, licensed by `r[kernels.selection.index-is-not-a-variable]`: an index is constant wrt every
/// differentiation variable, so it has no slope to state and the body differentiates only values.
///
/// Earns the [`Scan`] kernel, which is the whole of how it computes. Declining is `NaN`, which
/// [`Unflat`] reads back as absence.
pub trait Scans: Series + Clone
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];

	/// Fills the env for out element `i` and returns that element's own event time — `None` where the
	/// driving element carried nothing, which is absence arriving rather than the body declining, and
	/// is answered without evaluating anything.
	///
	/// Slots are appended in the order the body reads them, each naming the dep and the lag it came
	/// off ([`Env::dep`]) or standing for no element at all ([`Env::opaque`]). Leading with the deps'
	/// own elements at lag 0, in `Deps` order, is what lines the body's gradient up with the
	/// Jacobian's columns; naming the lag is what lets [`Run::exact`] cover the rest of the reach.
	fn read<W: Witness>(deps: &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64>;

	/// One expression per slot of [`Series::Item`], over the env [`read`](Scans::read) filled.
	fn body(&self, vars: Vars) -> impl Slots;
}

/// Uniform binder-correct dep-tuple type for [`Scans`] impls — [`EmitOuts`] spelled through the body
/// trait, and the same type.
pub type ScanOuts<'t, S> = <<S as Scans>::Deps as DepSet>::Outs<'t>;

/// The kernel of a [`Scans`] node: one out element per driving element, each slot `body().eval`, and
/// the Jacobian `body().grad` at the last element — exact, and one pass rather than a clone and a
/// re-emit per dep slot.
pub struct Scan;
impl sealed::Kernel for Scan {}

/// A [`Scan`]'s pre-emit capture: the slot equations, and their gradients at the env of the element
/// the out will flatten to. Read *before* `emit` for the reason [`Algebra`] is — the derivative
/// belongs to the state the value was computed from.
pub struct PerElem {
	formulas: alloc::vec::Vec<Ast>,
	/// Row-major `MAX_SLOTS × MAX_VARS`; only the leading `Item::LEN × DepFlat::LEN` corner is a
	/// Jacobian, the rest being the env slots no column stands for.
	grad: [f64; MAX_SLOTS * MAX_VARS],
	/// `grad`'s row width — the env the body was read over. A [`Scans`] reading is as wide as it
	/// filled, where a [`Closes`]/[`Folds`] one is its two consts.
	stride: usize,
}

/// The slot count a [`Scans`] body fills, checked once against the buffer that holds it.
const fn scan_slots<E: Scans>() -> usize
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Flat, {
	assert!(<E::Item as Flat>::LEN <= MAX_SLOTS, "a Scans item outgrew MAX_SLOTS: raise it in trading_data_dag");
	<E::Item as Flat>::LEN
}

impl<E> Run<E> for Scan
where
	E: Scans + Emit<Deps = <E as Scans>::Deps>,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Unflat,
	<E as Emit>::Deps: DepFlat,
{
	type Pre = Option<PerElem>;

	/// Every slot of a per-element env is a copy of one element slot, and [`Env`] reports which — so
	/// whatever the body reached back over, [`exact`](Run::exact) covers it.
	const FIDELITY: Fidelity = Fidelity::Exact;

	fn emit<'t>(e: &mut E, deps: EmitOuts<'t, E>, out: &mut alloc::vec::Vec<E::Item>) {
		let slots_w = const { scan_slots::<E>() };
		let body = e.body(Vars);
		debug_assert_eq!(body.len(), slots_w, "a Scans body states one expression per slot of its item");
		let (mut buf, mut slots) = ([f64::NAN; MAX_VARS], [f64::NAN; MAX_SLOTS]);
		let mut env = Env::<Blindfold>::new(&mut buf);
		for i in 0..<<E as Emit>::Deps as DepFlat>::lead_fires(&deps) {
			env.rewind();
			match <E as Scans>::read(&deps, i, &mut env) {
				Some(ts) => {
					body.eval_slots(env.filled(), &mut slots[..slots_w]);
					out.push(<E::Item as Unflat>::unflat(ts, &slots[..slots_w]));
				}
				// rate-preserving: an element that carried nothing still occupies its place in the run.
				None => out.push(<E::Item as Unflat>::unflat(0, &[f64::NAN; MAX_SLOTS][..slots_w])),
			}
		}
	}

	fn pre<'d>(e: &E, want: Want, deps: EmitOuts<'d, E>) -> Self::Pre {
		if want < Want::Jac {
			return None;
		}
		// the out flattens to its *last* element, so that is the one the derivative belongs to; an
		// empty run has none, and there is nothing to differentiate.
		let last = <<E as Emit>::Deps as DepFlat>::lead_fires(&deps).checked_sub(1)?;
		// zeroed, not NaN-filled: `grad` accumulates, and a slot the body never reads has a partial of
		// 0 rather than no partial.
		let mut buf = [0.0f64; MAX_VARS];
		let mut env = Env::<Blindfold>::new(&mut buf);
		<E as Scans>::read(&deps, last, &mut env)?;
		let stride = env.filled().len();
		let body = e.body(Vars);
		let mut grad = [0.0f64; MAX_SLOTS * MAX_VARS];
		body.grad_slots(env.filled(), &mut grad[..body.len() * stride]);
		Some(PerElem {
			formulas: body.lower_slots(),
			grad,
			stride,
		})
	}

	/// Slot 0's equation. A multi-slot item has one per slot, and the reading that carries all of
	/// them is the Jacobian.
	fn formula(pre: &Self::Pre) -> Option<&Ast> {
		pre.as_ref()?.formulas.first()
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		let dep_w = j.dep_buf.len();
		// the body's rows are `stride` wide and the Jacobian's are `dep_w`; the prefix `read` filled
		// from the deps' own elements is what makes the copy a truncation rather than a remap.
		for (row, out) in j.out.chunks_exact_mut(dep_w).enumerate() {
			out.copy_from_slice(&alg.grad[row * alg.stride..row * alg.stride + dep_w]);
		}
		true
	}

	/// The reading again, watched: `read` names the dep and the lag of every slot it fills, so the
	/// gradient `pre` took over that env scatters into the block in one pass. `widths[d]` comes out
	/// as `(deepest lag touched + 1) × slots(d)` — grown here rather than declared, because a
	/// wall-clock window over an aperiodic series has no static element count.
	fn exact<'d>(pre: &Self::Pre, x: Exact<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		let Some(last) = <<E as Emit>::Deps as DepFlat>::lead_fires(&x.deps).checked_sub(1) else {
			return false;
		};
		let mut buf = [0.0f64; MAX_VARS];
		let mut env = Env::<Provenance>::new(&mut buf);
		if <E as Scans>::read(&x.deps, last, &mut env).is_none() {
			return false;
		}
		debug_assert_eq!(env.filled().len(), alg.stride, "the reading `pre` differentiated and the one scattered here are one");
		lagged_block(&alg.grad, alg.stride, x.out_buf.len(), <<E as Emit>::Deps as DepFlat>::DIMS, env.seen(), x.out, x.widths);
		true
	}
}

/// The period a [`Closes`] node is part way through: the accumulator's slots and the time they close
/// at. Held by the node because the engine owns no storage for one, and read by nothing but the
/// kernel — the body sees the slots as an env and never this struct.
#[derive(Clone, Copy, Default)]
pub struct Pending {
	slots: [f64; MAX_SLOTS],
	ts_close: i64,
	/// A period nothing has opened yet, which is not the same as one whose slots happen to be zero.
	live: bool,
}

/// A run-shaped node whose elements are *periods*, closed by the first driving element past a
/// boundary. The kernel owns the walk and the close time; the body owns the numbers, as two
/// expressions over one env — what a period's first element opens with, and what a further element
/// folds into it.
///
/// Rate-changing by construction: a batch spanning two boundaries emits two elements, one spanning
/// none emits nothing, and the period still open stays in [`Pending`].
pub trait Closes: Series + Clone
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	/// The period the walk closes on, floored from the epoch.
	const PERIOD: Timeframe;

	/// Fills the driving element's own slots — `DepFlat::LEN` of them, the same prefix
	/// [`Scans::read`] fills — and returns its event time, which is what the walk is keyed on and is
	/// deliberately not a slot (`r[kernels.selection.index-is-not-a-variable]`).
	fn read<W: Witness>(deps: &CloseOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64>;

	/// What a period's first element opens the accumulator with, over the element's slots alone.
	fn open(&self, vars: Vars) -> impl Slots;

	/// What a further element folds into it. The accumulator's own slots follow the element's, at
	/// `DepFlat::LEN..`, so both bodies read the element at one set of indices.
	fn fold(&self, vars: Vars) -> impl Slots;

	fn pending(&self) -> &Pending;
	fn pending_mut(&mut self) -> &mut Pending;
}

/// Uniform binder-correct dep-tuple type for [`Closes`] impls.
pub type CloseOuts<'t, C> = <<C as Closes>::Deps as DepSet>::Outs<'t>;

/// The kernel of a [`Closes`] node.
pub struct Close;
impl sealed::Kernel for Close {}

/// The element width, the slot width, and the env width the two bodies share.
const fn close_widths<E: Closes>() -> (usize, usize, usize)
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	<E as Closes>::Deps: DepFlat,
	E::Item: Flat, {
	let (elem, slots) = (<<E as Closes>::Deps as DepFlat>::LEN, <E::Item as Flat>::LEN);
	assert!(elem + slots <= MAX_VARS, "a Closes env outgrew MAX_VARS: raise it in trading_data_dag");
	assert!(slots <= MAX_SLOTS, "a Closes accumulator outgrew MAX_SLOTS: raise it in trading_data_dag");
	(elem, slots, elem + slots)
}

impl<E> Run<E> for Close
where
	E: Closes + Emit<Deps = <E as Closes>::Deps>,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Unflat,
	<E as Emit>::Deps: DepFlat,
{
	type Pre = Option<PerElem>;

	/// The one-step column stands for the trade that *closed* the reported element, and the elements
	/// earlier in its period reached it only through the accumulator, which is no dep and has no
	/// column. Narrower than [`Fold`]'s omission — a period is discharged at its boundary, where a
	/// recurrence is not — but the same kind of omission, and `r[kernels.fidelity.stated]` is exactly
	/// the rule against calling it exact.
	const FIDELITY: Fidelity = Fidelity::Partial("the elements earlier in the period, which the accumulator carries and no column stands for");

	fn emit<'t>(e: &mut E, deps: EmitOuts<'t, E>, out: &mut alloc::vec::Vec<E::Item>) {
		let (elem_w, slots_w, env_w) = const { close_widths::<E>() };
		let step = Horizon::ns(<E as Closes>::PERIOD);
		let mut p = *e.pending();
		let (mut buf, mut slots) = ([f64::NAN; MAX_VARS], [f64::NAN; MAX_SLOTS]);
		{
			let mut env = Env::<Blindfold>::new(&mut buf);
			let (open, fold) = (e.open(Vars), e.fold(Vars));
			debug_assert_eq!(open.len(), slots_w, "a Closes body states one expression per slot of its item");
			debug_assert_eq!(fold.len(), slots_w, "a Closes body states one expression per slot of its item");
			for i in 0..<<E as Emit>::Deps as DepFlat>::lead_fires(&deps) {
				env.rewind();
				let Some(ts) = <E as Closes>::read(&deps, i, &mut env) else { continue };
				debug_assert_eq!(env.filled().len(), elem_w, "a Closes reading is the driving element and nothing else");
				let ts_close = ts - ts.rem_euclid(step) + step;
				let env = env.buf();
				match p.live && p.ts_close == ts_close {
					true => {
						env[elem_w..env_w].copy_from_slice(&p.slots[..slots_w]);
						fold.eval_slots(&env[..env_w], &mut slots[..slots_w]);
					}
					false => {
						if p.live {
							out.push(<E::Item as Unflat>::unflat(p.ts_close, &p.slots[..slots_w]));
						}
						open.eval_slots(&env[..env_w], &mut slots[..slots_w]);
					}
				}
				p.slots[..slots_w].copy_from_slice(&slots[..slots_w]);
				p.ts_close = ts_close;
				p.live = true;
			}
		}
		*e.pending_mut() = p;
	}

	/// The walk again, for its indices alone: the out flattens to the last element this batch
	/// *closed*, and the derivative belongs to the element that closed it — not to the batch's
	/// newest, whose period nothing has emitted yet.
	fn pre<'d>(e: &E, want: Want, deps: EmitOuts<'d, E>) -> Self::Pre {
		if want < Want::Jac {
			return None;
		}
		let (elem_w, slots_w, env_w) = const { close_widths::<E>() };
		let step = Horizon::ns(<E as Closes>::PERIOD);
		let mut p = *e.pending();
		let (mut buf, mut slots) = ([0.0f64; MAX_VARS], [0.0f64; MAX_SLOTS]);
		let mut env = Env::<Blindfold>::new(&mut buf);
		let (open, fold) = (e.open(Vars), e.fold(Vars));
		// the newest element to have touched the open period, and whether it folded or opened it —
		// which is what the element closed by the *next* boundary was last computed from.
		let (mut newest, mut closed) = (None, None);
		for i in 0..<<E as Emit>::Deps as DepFlat>::lead_fires(&deps) {
			env.rewind();
			let Some(ts) = <E as Closes>::read(&deps, i, &mut env) else { continue };
			let ts_close = ts - ts.rem_euclid(step) + step;
			let folding = p.live && p.ts_close == ts_close;
			let cur = env.buf();
			match folding {
				true => {
					cur[elem_w..env_w].copy_from_slice(&p.slots[..slots_w]);
					fold.eval_slots(&cur[..env_w], &mut slots[..slots_w]);
				}
				false => {
					if p.live {
						closed = newest;
					}
					open.eval_slots(&cur[..env_w], &mut slots[..slots_w]);
				}
			}
			let mut held = [0.0f64; MAX_VARS];
			held.copy_from_slice(cur);
			newest = Some((held, folding));
			p.slots[..slots_w].copy_from_slice(&slots[..slots_w]);
			p.ts_close = ts_close;
			p.live = true;
		}
		// nothing closed this batch, or what closed was carried in from before it — either way no
		// element of this tick's deps is the one to differentiate against.
		let (env, folding) = closed?;
		let mut grad = [0.0f64; MAX_SLOTS * MAX_VARS];
		match folding {
			true => fold.grad_slots(&env[..env_w], &mut grad[..slots_w * env_w]),
			false => open.grad_slots(&env[..env_w], &mut grad[..slots_w * env_w]),
		}
		Some(PerElem {
			formulas: fold.lower_slots(),
			grad,
			stride: env_w,
		})
	}

	/// The fold's slot 0, which is the equation a period accumulates by.
	fn formula(pre: &Self::Pre) -> Option<&Ast> {
		pre.as_ref()?.formulas.first()
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		let dep_w = j.dep_buf.len();
		for (row, out) in j.out.chunks_exact_mut(dep_w).enumerate() {
			out.copy_from_slice(&alg.grad[row * alg.stride..row * alg.stride + dep_w]);
		}
		true
	}

	/// Lag 0 and no further, which is the [`jac`](Run::jac) column re-laid — and why this kernel
	/// stays [`Fidelity::Partial`] with a block in hand. The period's earlier elements *are* in the
	/// dep, but in its **declaration** (`Folding<Trades, Over<TF>>`) rather than in its out: the
	/// accumulator holds them, and an out that never carried them has no lag to index them at.
	/// Writing the dep as a `Buffering` would put them in reach, at the price of retaining the tape.
	fn exact<'d>(pre: &Self::Pre, x: Exact<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		point_block(&alg.grad, alg.stride, x.out_buf.len(), <<E as Emit>::Deps as DepFlat>::DIMS, x.out, x.widths);
		true
	}
}

/// What a [`Folds`] node carries between its elements. Zero at the start of a run and at every
/// reset — a recurrence that needs a different zero says so with a counter slot and a `select`,
/// which is also how it says it is not warm yet.
#[derive(Clone, Copy, Default)]
pub struct Carried {
	slots: [f64; MAX_SLOTS],
}

/// A run-shaped node computing one out element per element of its driving dep, carrying state
/// between them. Two expressions over one env: what the state becomes, and what the element then is.
///
/// The state is not a dep and has no column, so what the Jacobian omits is every earlier element's
/// reach through it — permanently, by design (`r[kernels.fidelity.stated]`). A derivative carrying
/// accumulated state sensitivity is a different quantity, and this kernel will never claim it.
pub trait Folds: Series + Clone
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	/// `&[]` draws nothing *by default* — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	/// Env slots past the deps' own flattenings — a lag, a window's edge, or a discrete attribute
	/// [`Flat`] leaves out because it is no derivative's variable.
	const EXTRA: usize = 0;
	/// How many slots the recurrence carries.
	const STATE: usize;

	/// Fills the driving element's own slots and then [`EXTRA`](Folds::EXTRA) more, exactly as
	/// [`Scans::read`] does; the state follows at `DepFlat::LEN + EXTRA`. `None` declines the element
	/// *and leaves the state where it stood* — an average is not advanced by an absence.
	fn read<W: Witness>(deps: &FoldOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64>;

	/// What the state becomes, over the env with the state as it stood.
	fn step(&self, vars: Vars) -> impl Slots;

	/// What the element is, over the same env with the *stepped* state in place.
	fn value(&self, vars: Vars) -> impl Slots;

	fn carried(&self) -> &Carried;
	fn carried_mut(&mut self) -> &mut Carried;
}

/// Uniform binder-correct dep-tuple type for [`Folds`] impls.
pub type FoldOuts<'t, F> = <<F as Folds>::Deps as DepSet>::Outs<'t>;

/// The kernel of a [`Folds`] node.
pub struct Fold;
impl sealed::Kernel for Fold {}

/// The element width, where the state starts, the env width, and the item's slot count.
const fn fold_widths<E: Folds>() -> (usize, usize, usize, usize)
where
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	<E as Folds>::Deps: DepFlat,
	E::Item: Flat, {
	let dep = <<E as Folds>::Deps as DepFlat>::LEN;
	let base = dep + <E as Folds>::EXTRA;
	let (env, slots) = (base + <E as Folds>::STATE, <E::Item as Flat>::LEN);
	assert!(env <= MAX_VARS, "a Folds env outgrew MAX_VARS: raise it in trading_data_dag");
	assert!(
		slots <= MAX_SLOTS && <E as Folds>::STATE <= MAX_SLOTS,
		"a Folds item or state outgrew MAX_SLOTS: raise it in trading_data_dag"
	);
	(dep, base, env, slots)
}

impl<E> Run<E> for Fold
where
	E: Folds + Emit<Deps = <E as Folds>::Deps>,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Unflat,
	<E as Emit>::Deps: DepFlat,
{
	type Pre = Option<PerElem>;

	const FIDELITY: Fidelity = Fidelity::Partial("state history, by design");

	fn emit<'t>(e: &mut E, deps: EmitOuts<'t, E>, out: &mut alloc::vec::Vec<E::Item>) {
		let (_, base, env_w, slots_w) = const { fold_widths::<E>() };
		let carry = <E as Folds>::STATE;
		let mut c = *e.carried();
		let (mut buf, mut slots) = ([f64::NAN; MAX_VARS], [f64::NAN; MAX_SLOTS]);
		{
			let mut env = Env::<Blindfold>::new(&mut buf);
			let (step, value) = (e.step(Vars), e.value(Vars));
			debug_assert_eq!(step.len(), carry, "a Folds `step` states one expression per state slot");
			debug_assert_eq!(value.len(), slots_w, "a Folds `value` states one expression per slot of its item");
			for i in 0..<<E as Emit>::Deps as DepFlat>::lead_fires(&deps) {
				env.rewind();
				let Some(ts) = <E as Folds>::read(&deps, i, &mut env) else {
					// rate-preserving, and the state untouched: nothing arrived to advance it.
					out.push(<E::Item as Unflat>::unflat(0, &[f64::NAN; MAX_SLOTS][..slots_w]));
					continue;
				};
				debug_assert_eq!(env.filled().len(), base, "a Folds reading is the driving element and its EXTRA, and the state follows");
				let cur = env.buf();
				cur[base..env_w].copy_from_slice(&c.slots[..carry]);
				step.eval_slots(&cur[..env_w], &mut slots[..carry]);
				c.slots[..carry].copy_from_slice(&slots[..carry]);
				cur[base..env_w].copy_from_slice(&c.slots[..carry]);
				value.eval_slots(&cur[..env_w], &mut slots[..slots_w]);
				out.push(<E::Item as Unflat>::unflat(ts, &slots[..slots_w]));
			}
		}
		*e.carried_mut() = c;
	}

	fn pre<'d>(e: &E, want: Want, deps: EmitOuts<'d, E>) -> Self::Pre {
		if want < Want::Jac {
			return None;
		}
		let (dep_w, base, env_w, slots_w) = const { fold_widths::<E>() };
		let carry = <E as Folds>::STATE;
		let mut c = *e.carried();
		let (mut buf, mut slots) = ([0.0f64; MAX_VARS], [0.0f64; MAX_SLOTS]);
		let mut env = Env::<Blindfold>::new(&mut buf);
		let (step, value) = (e.step(Vars), e.value(Vars));
		// the walk again, for the last element's two envs: a recurrence's derivative is of the state
		// the value was computed from, and that state is only reached by replaying the batch.
		let mut last = None;
		for i in 0..<<E as Emit>::Deps as DepFlat>::lead_fires(&deps) {
			env.rewind();
			if <E as Folds>::read(&deps, i, &mut env).is_none() {
				continue;
			}
			let cur = env.buf();
			cur[base..env_w].copy_from_slice(&c.slots[..carry]);
			let mut before = [0.0f64; MAX_VARS];
			before.copy_from_slice(cur);
			step.eval_slots(&cur[..env_w], &mut slots[..carry]);
			c.slots[..carry].copy_from_slice(&slots[..carry]);
			cur[base..env_w].copy_from_slice(&c.slots[..carry]);
			let mut after = [0.0f64; MAX_VARS];
			after.copy_from_slice(cur);
			last = Some((before, after));
		}
		let (before, after) = last?;

		// the chain rule across the two bodies: the element reaches the out directly and through the
		// state it just moved, and holding the *prior* state fixed is what makes the sum the one-step
		// reading rather than the recurrence's full derivative.
		let (mut ds, mut dv) = ([0.0f64; MAX_SLOTS * MAX_VARS], [0.0f64; MAX_SLOTS * MAX_VARS]);
		step.grad_slots(&before[..env_w], &mut ds[..carry * env_w]);
		value.grad_slots(&after[..env_w], &mut dv[..slots_w * env_w]);
		let mut grad = [0.0f64; MAX_SLOTS * MAX_VARS];
		for row in 0..slots_w {
			for col in 0..dep_w {
				grad[row * env_w + col] = dv[row * env_w + col] + (0..carry).map(|s| dv[row * env_w + base + s] * ds[s * env_w + col]).sum::<f64>();
			}
		}
		Some(PerElem {
			formulas: value.lower_slots(),
			grad,
			stride: env_w,
		})
	}

	fn formula(pre: &Self::Pre) -> Option<&Ast> {
		pre.as_ref()?.formulas.first()
	}

	fn jac<'d>(pre: &Self::Pre, j: Jac<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		let dep_w = j.dep_buf.len();
		for (row, out) in j.out.chunks_exact_mut(dep_w).enumerate() {
			out.copy_from_slice(&alg.grad[row * alg.stride..row * alg.stride + dep_w]);
		}
		true
	}

	/// Lag 0 and no further, which is the [`jac`](Run::jac) column re-laid: what this kernel omits is
	/// the state, which is no dep and has no reach to index — by design, and permanently.
	fn exact<'d>(pre: &Self::Pre, x: Exact<'_, EmitOuts<'d, E>>) -> bool
	where
		<E as Emit>::Deps: DepFlat,
		EmitOuts<'d, E>: Copy,
		E::Item: Flat, {
		let Some(alg) = pre else { return false };
		point_block(&alg.grad, alg.stride, x.out_buf.len(), <<E as Emit>::Deps as DepFlat>::DIMS, x.out, x.widths);
		true
	}
}

/// A binary control signal. A node naming it through a [`Gating`] dep is not advanced while it is
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

/// A batch ends the episode if *any* element does. Deliberately not `.last()` like [`Flat`]: that
/// reads the value standing at end-of-batch, where this asks whether the boundary was crossed
/// anywhere in the run — a rate-preserving node keeps emitting past its own terminal element.
/// Empty ⇒ false, so a dark node never self-commutates.
impl<T: Episode> Episode for &[T] {
	fn terminal(&self) -> bool {
		self.iter().any(Episode::terminal)
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
	/// The gated cell whose terminal out commutates this latch. That it *is* gated on this latch is
	/// [`cut_gated`]'s to say — a bound here could only ask for [`Node`], which names the cut's gates
	/// through one of the two traits that declare them and rules out an [`Emit`] for nothing.
	type Cut: Cell;
	fn commutate(&mut self);
	/// The contact as it stands *before* this tick's sweep — the one gate reading whose value does not
	/// depend on sweep order, which is what lets it suppress what feeds it. Read after the deferred
	/// commutation, so a spent episode is already open.
	fn standing(&self) -> bool;
}

/// A node that runs a self-terminating episode, latched from inside the graph. `Trigger` is the one
/// dep that stays live while the episode is dark — the arm — and the node's own [`Episode::terminal`]
/// out drops the contact. Where a hand-written [`Latch`] leaves the loop open (nothing checks that
/// the gate you armed is the gate your episode cuts), this closes it in the type: [`Armed<Self>`] is
/// the only gate it can be.
pub trait Episodic: Cell
where
	for<'t> <Self as Cell>::Out<'t>: Episode, {
	type Trigger: Cell;
	fn arms<'t>(trigger: TriggerOut<'t, Self>) -> bool;
}

/// Binder-correct trigger-out type for [`Episodic::arms`] impls — the arm-side [`DepOuts`]. Writing
/// the concrete type hits E0195 whenever the trigger's out carries no lifetime of its own.
pub type TriggerOut<'t, N> = <<N as Episodic>::Trigger as Cell>::Out<'t>;

/// An [`Episodic`] node's own contact, sealed in: an ordinary node stepped before `N`, so `N` and any
/// leg meant to go dark with it name it in a [`Gating`] dep. It folds its trigger at
/// [`Horizon::Unbounded`] by construction — a latched bit is exactly the state nothing reconstitutes
/// — which is also why it can never itself be gated.
///
/// [`Episodic`]'s `where` is repeated on every impl below: a trait's where-clause is an obligation at
/// each use of the bound, not an implied one, and [`Latch`] needs exactly it to accept `Cut = N`.
pub struct Armed<N: Episodic>(bool, core::marker::PhantomData<N>)
where
	for<'t> N::Out<'t>: Episode;

// hand-written: `derive` would demand `N: Default + Clone`, which the episode need not be.
impl<N: Episodic> Default for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	fn default() -> Self {
		Self(false, core::marker::PhantomData)
	}
}
impl<N: Episodic> Clone for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	fn clone(&self) -> Self {
		Self(self.0, core::marker::PhantomData)
	}
}

impl<N: Episodic> Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	const TAG: Tag = Tag::of("Armed", &[N::NAME]);
}

impl<N: Episodic> Cell for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	type Out<'t> = bool;

	const NAME: &'static str = Self::TAG.as_str();
}

impl<N: Episodic> Blind for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	type Deps = (Folding<N::Trigger, Unbounded>,);

	const WHY: &'static str = "a latch is a bit that stays set: `arms` is a predicate over an episode, not a value with a slope";

	fn advance<'t>(&'t mut self, (trigger,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0 |= N::arms(trigger);
		self.0
	}
}

impl<N: Episodic> Node for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	type Deps = <Self as Blind>::Deps;
	type Kernel = Opaque;
}

impl<N: Episodic> Gate for Armed<N> where for<'t> N::Out<'t>: Episode {}

impl<N: Episodic> Latch for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	type Cut = N;

	fn commutate(&mut self) {
		self.0 = false;
	}

	fn standing(&self) -> bool {
		self.0
	}
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
	/// How a [`Buffer`] over this series accumulates its reach. [`Rows`] — every row, handed out as a
	/// [`Hist`] — unless the series says otherwise. Unbounded here: only a *buffered* series owes a
	/// working [`Batch`], and the `Item: Stamped` [`Rows`] needs is not every series' to carry.
	type Batch: 'static;
}

/// How a [`Buffer`] holds a [`Series`]' retained reach. The default [`Rows`] keeps every row; a
/// series whose rows collapse — a book's levels keep only the last qty per price — implements its
/// own and pays its own state rather than the row count.
///
/// **[`Horizon`] means whatever the impl makes it mean.** [`Rows`] slides: it trims by `ts_ns` on
/// every tick, so `Over(tf)` is the last `tf` of wall clock ending now. A batch that folds cannot
/// un-fold, so it has nothing to trim and can only *tumble* — reset on the absolute period boundary,
/// floored from the epoch. Both are the same declaration and different windows. What contains it is
/// that the views are different types, so no consumer can read one as the other.
/// `Clone` because [`fd_col`] re-owns the whole frame once per dep element — a retention that
/// cannot be copied cannot be differenced.
pub trait Batch<T>: Clone + Default {
	type View<'t>: Copy
	where
		Self: 't,
		T: 't;

	/// Trim to `h`, then append `fresh`. Trimming *before* the append is what makes `past` mean "what
	/// stood behind this tick's batch" — see the note at [`Buffer`].
	fn advance(&mut self, fresh: &[T], h: Horizon);

	/// What stands retained, read at the reach `h` its consumer declared.
	fn view(&self, h: Horizon) -> Self::View<'_>;

	/// The same, for a consumer holding only a view — [`Buffering`]'s read of the frame's [`Buffer`],
	/// whose `K` the const-assert there has already proven [`serves`](Horizon::serves) `h`.
	fn narrow<'t>(view: Self::View<'t>, h: Horizon) -> Self::View<'t>
	where
		Self: 't,
		T: 't;

	/// [`Nudge::stage`] for the retention: re-own `view`, perturbing its newest item when asked.
	fn stage(&mut self, view: Self::View<'_>, bump: Option<usize>, h: f64) -> f64
	where
		T: Bump;
}

/// The default [`Batch`]: every row, trimmed on timestamps, handed out as a [`Hist`].
pub struct Rows<T> {
	buf: alloc::vec::Vec<T>,
	/// Where the retained window starts. Trimming by `drain(..n)` memmoved the whole window every
	/// tick — the window is the point, so it is never small — where a cursor moves nothing and the
	/// window stays the one contiguous slice [`Hist`] hands out.
	cur: usize,
	/// The highest `ts_ns` this batch cannot speak for: the last one dropped, or — before the first
	/// drop — the first one ever seen, since nothing proves the run reached back past it. A
	/// [`Horizon::Over`] window is complete iff it begins strictly after this, which is exact where
	/// "have I been running long enough" is a guess.
	watermark: i64,
	fresh: usize,
}

// hand-written: `derive` would demand `T: Default` / `T: Clone`, which a retained item need not be.
impl<T> Default for Rows<T> {
	fn default() -> Self {
		Self {
			buf: alloc::vec::Vec::new(),
			cur: 0,
			watermark: i64::MIN,
			fresh: 0,
		}
	}
}
impl<T: Clone> Clone for Rows<T> {
	fn clone(&self) -> Self {
		Self {
			buf: self.buf[self.cur..].to_vec(),
			cur: 0,
			watermark: self.watermark,
			fresh: self.fresh,
		}
	}

	/// The one thing `derive(Clone)` would not have given: its `clone_from` is `*self = clone()`, and
	/// a batch holding a whole reach is what [`fd_col`] re-clones once per dep element. Only the
	/// retained window is carried — the dropped prefix is waiting to be reclaimed, not state.
	fn clone_from(&mut self, src: &Self) {
		self.buf.clear();
		self.buf.extend_from_slice(&src.buf[src.cur..]);
		self.cur = 0;
		self.watermark = src.watermark;
		self.fresh = src.fresh;
	}
}

impl<T: Copy + Stamped> Batch<T> for Rows<T> {
	type View<'t>
		= Hist<'t, T>
	where
		T: 't;

	fn advance(&mut self, fresh: &[T], h: Horizon) {
		let drop = {
			let win = &self.buf[self.cur..];
			match h {
				Horizon::Elems(k) => win.len().saturating_sub(k),
				Horizon::Over(tf) => match win.last() {
					Some(newest) => {
						let cut = newest.ts_ns() - Horizon::ns(tf);
						win.partition_point(|x| x.ts_ns() <= cut)
					}
					None => 0,
				},
				_ => unreachable!("a buffer is const-asserted bounded"),
			}
		};
		if drop > 0 {
			self.watermark = self.watermark.max(self.buf[self.cur + drop - 1].ts_ns());
			self.cur += drop;
			// Reclaim only once the dropped prefix dominates, as `sync::Lane::compact` does: amortized
			// O(1) an element, against a memmove of the whole window on every tick.
			if self.cur * 2 >= self.buf.len() {
				self.buf.drain(..self.cur);
				self.cur = 0;
			}
		}
		if self.watermark == i64::MIN
			&& let Some(first) = fresh.first()
		{
			self.watermark = first.ts_ns();
		}
		self.buf.extend_from_slice(fresh);
		self.fresh = fresh.len();
	}

	fn view(&self, h: Horizon) -> Hist<'_, T> {
		Hist {
			all: &self.buf[self.cur..],
			fresh: self.fresh,
			horizon: h,
			watermark: self.watermark,
		}
	}

	fn narrow<'t>(view: Hist<'t, T>, h: Horizon) -> Hist<'t, T>
	where
		T: 't, {
		Hist { horizon: h, ..view }
	}

	fn stage(&mut self, view: Hist<'_, T>, bump: Option<usize>, h: f64) -> f64
	where
		T: Bump, {
		self.buf.clear();
		self.buf.extend_from_slice(view.all);
		self.cur = 0;
		self.fresh = view.fresh;
		self.watermark = view.watermark;
		match (bump, self.buf.last_mut()) {
			(Some(slot), Some(last)) => {
				let (e, dh) = Bump::bump(*last, slot, h);
				*last = e;
				dh
			}
			_ => 0.0,
		}
	}
}

/// A [`Series`] item read as "did this element carry anything" — what [`Latest`] must ask before it
/// keeps one as a level. The dominant item in this codebase is `Option<f64>`, a rate-preserving
/// decline; retaining one of those as the standing value would hold an absence forever.
pub trait Present: Copy {
	type Val: Copy;
	fn present(self) -> Option<Self::Val>;
}

impl<T: Copy> Present for Option<T> {
	type Val = T;

	fn present(self) -> Option<T> {
		self
	}
}

/// The identity [`Present`]: an item that *is* its value, absent only by not being emitted. One line
/// per type because the blanket impl is spent on `Option` and Rust has no specialization.
#[macro_export]
macro_rules! always_present {
	($($T:ty),+ $(,)?) => {$(
		impl $crate::Present for $T {
			type Val = Self;

			fn present(self) -> Option<Self> {
				Some(self)
			}
		}
	)+};
}

always_present!(f64);

/// A run read for one of its elements, lag and all — so no per-element kernel counts `len - 1 - i`
/// on its own. The lag is from the run's newest, which is the element a dep's [`Jac`] column stands
/// for; [`Hist`] hands its own back from [`lagged_at`](Hist::lagged_at).
pub trait Lagged<T> {
	fn at(&self, i: usize) -> Option<(&T, usize)>;

	fn newest(&self) -> Option<(&T, usize)>;
}

impl<T> Lagged<T> for [T] {
	fn at(&self, i: usize) -> Option<(&T, usize)> {
		Some((self.get(i)?, self.len() - 1 - i))
	}

	fn newest(&self) -> Option<(&T, usize)> {
		self.at(self.len().checked_sub(1)?)
	}
}

/// A *prefix* of a run, carrying how far its own end stands behind the run's newest. A cross-rate
/// reader cuts a series at its own deadline and then picks inside the cut; without this the pick
/// would report a lag against the cut instead of against the column it lands in.
#[derive(Clone, Copy, Debug)]
pub struct Tail<'t, T> {
	xs: &'t [T],
	back: usize,
}

impl<'t, T> Tail<'t, T> {
	/// The elements themselves, for a search that reads more than one of them.
	pub fn as_slice(self) -> &'t [T] {
		self.xs
	}

	/// The longest prefix `f` holds over — `f` monotone, as a deadline over a sorted run is.
	pub fn upto(self, f: impl FnMut(&T) -> bool) -> Self {
		let n = self.xs.partition_point(f);
		Self {
			back: self.back + self.xs.len() - n,
			xs: &self.xs[..n],
		}
	}
}

impl<T> Lagged<T> for Tail<'_, T> {
	fn at(&self, i: usize) -> Option<(&T, usize)> {
		let (x, lag) = self.xs.at(i)?;
		Some((x, lag + self.back))
	}

	fn newest(&self) -> Option<(&T, usize)> {
		self.at(self.xs.len().checked_sub(1)?)
	}
}

impl<'t, T> From<&'t [T]> for Tail<'t, T> {
	fn from(xs: &'t [T]) -> Self {
		Self { xs, back: 0 }
	}
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
}

impl<'t, T: Stamped> Hist<'t, T> {
	/// What stood behind this tick's batch, at the declared reach — see [`Hist::all`].
	pub fn past(self) -> &'t [T] {
		let all = self.all();
		&all[..all.len() - self.fresh]
	}

	/// `past ++ fresh` — the cross-rate view, for a consumer clocked by some faster series that must
	/// find the run standing at its own deadline.
	///
	/// Cut to the *declared* reach by the same predicate [`Buffer`] trims on, so a node reads what a
	/// frame buffering at exactly its `Buffering<C, R>` would hold. Without the cut, a run is only as
	/// long as the deepest unrelated consumer of the same series happens to ask for, and shortening
	/// that one silently changes this one's results.
	pub fn all(self) -> &'t [T] {
		let past = self.all.len() - self.fresh;
		// `fresh` is wholly in reach however shallow the declaration, so nothing behind the batch is
		// nothing to cut.
		let Some(newest) = past.checked_sub(1) else { return self.all };
		let drop = match self.horizon {
			Horizon::Elems(n) => past.saturating_sub(n),
			// keyed on the pre-batch newest rather than the batch's own tail — the reference [`Buffer`]
			// itself trims against, so what a frame retains at `H` and what a consumer reads at `H` are
			// one statement.
			Horizon::Over(tf) => {
				let cut = self.all[newest].ts_ns() - Horizon::ns(tf);
				self.all[..past].partition_point(|x| x.ts_ns() <= cut)
			}
			h => unreachable!("a Buffering is const-asserted bounded, and `narrowed` re-asserts it: {h:?}"),
		};
		&self.all[drop..]
	}

	/// The window ending at `fresh()[i]`, and the lag of its *first* element; `None` when the window
	/// is incomplete — fewer than `Elems(n)` retained, or an `Over` reaching past what the buffer has
	/// dropped.
	pub fn trailing_at(self, i: usize) -> Option<(&'t [T], usize)> {
		let end = self.all.len() - self.fresh + i + 1;
		assert!(end <= self.all.len(), "trailing_at: {i} past this tick's {} fresh elements", self.fresh);
		let win = match self.horizon {
			Horizon::Elems(n) => {
				assert!(n >= 1, "a window includes the current element, so Elems(n >= 1)");
				(end >= n).then(|| &self.all[end - n..end])
			}
			// exclusive: the window is the last `ms` of wall clock, the same predicate the buffer trims
			// on — so what it retains and what a consumer reads are one statement.
			Horizon::Over(tf) => {
				let cut = self.all[end - 1].ts_ns() - Horizon::ns(tf);
				(cut >= self.watermark).then(|| &self.all[self.all[..end].partition_point(|x| x.ts_ns() <= cut)..end])
			}
			h => unreachable!("a Buffering is const-asserted bounded, and `narrowed` re-asserts it: {h:?}"),
		}?;
		Some((win, self.fresh - 1 - i + win.len() - 1))
	}

	/// The element `n` back from `fresh()[i]`, over [`all`](Hist::all), and its lag; `None` where the
	/// retained run does not reach that far. `n == 0` is the fresh element itself, so a per-element
	/// kernel walking a retained series indexes every reading it takes through this one.
	///
	/// The lag is from `fresh()`'s newest — the element this dep's [`Jac`] column stands for — and so
	/// is `n` plus however far element `i` itself stands behind the batch's end. Handed back rather
	/// than left to the caller, because a site counting it by hand is a site that can get it wrong.
	///
	/// A lag indexes by count, so it is constant wrt everything being differentiated
	/// (`r[kernels.selection.index-is-not-a-variable]`).
	pub fn lagged_at(self, i: usize, n: usize) -> Option<(&'t T, usize)> {
		let all = self.all();
		assert!(i < self.fresh, "lagged_at: {i} past this tick's {} fresh elements", self.fresh);
		Some((all.get((all.len() - self.fresh + i).checked_sub(n)?)?, self.fresh - 1 - i + n))
	}

	/// One window per fresh element — rate preservation for free. The lag is [`trailing_at`](Hist::trailing_at)'s
	/// to hand back: a consumer folding whole windows has no column to index with it.
	pub fn trailing(self) -> impl Iterator<Item = Option<&'t [T]>> {
		(0..self.fresh).map(move |i| self.trailing_at(i).map(|(w, _)| w))
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

/// Engine-owned retention over a [`Series`] — an ordinary node (`Deps = (Folding<C, At<H>>,)`, ungated)
/// sitting *next to* its source in the frame, not over it. It advances every
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
#[doc(hidden)]
pub struct Buffer<C: Series, const H: Horizon> {
	batch: C::Batch,
}

// hand-written: `derive` would demand `C: Default` / `C: Clone`, which the source node need not be.
impl<C: Series, const H: Horizon> Default for Buffer<C, H>
where
	C::Batch: Batch<C::Item>,
{
	fn default() -> Self {
		Self { batch: C::Batch::default() }
	}
}
impl<C: Series, const H: Horizon> Clone for Buffer<C, H>
where
	C::Batch: Clone,
{
	fn clone(&self) -> Self {
		Self { batch: self.batch.clone() }
	}

	fn clone_from(&mut self, src: &Self) {
		self.batch.clone_from(&src.batch);
	}
}

impl<C: Series, const H: Horizon> Buffer<C, H> {
	const REACH_TAG: Tag = H.tag();
	const TAG: Tag = Tag::of("Buffer", &[C::NAME, Self::REACH_TAG.as_str()]);
}

impl<C: Series, const H: Horizon> Cell for Buffer<C, H>
where
	C::Batch: Batch<C::Item>,
{
	type Out<'t> = <C::Batch as Batch<C::Item>>::View<'t>;

	/// Unlike its [`Buffering`]/[`Folding`] siblings this is a frame cell of its own, so it takes a
	/// name of its own — and `exec_viz` finds the buffer serving a dep by matching `Buffer<C, `,
	/// which only lines up while both spell `C` the way the rest of the graph does.
	const NAME: &'static str = Self::TAG.as_str();
	const REACH: Horizon = H;
}

impl<C: Series, const H: Horizon> Node for Buffer<C, H>
where
	C::Batch: Batch<C::Item>,
{
	type Deps = <Self as Blind>::Deps;
	type Kernel = Opaque;
}

impl<C: Series, const H: Horizon> Blind for Buffer<C, H>
where
	C::Batch: Batch<C::Item>,
{
	type Deps = (Folding<C, At<H>>,);

	const WHY: &'static str = "a retention window is the engine's bookkeeping over a run, not a function of its elements";

	fn advance<'t>(&'t mut self, (fresh,): DepOuts<'t, Self>) -> Self::Out<'t> {
		const {
			assert!(
				match H {
					Horizon::Elems(k) => k >= 1,
					Horizon::Over(tf) => tf.0 > 0,
					_ => false,
				},
				"a buffer retains a bounded reach: Horizon::Elems(k >= 1) or Horizon::Over(tf > 0)"
			)
		}
		self.batch.advance(fresh, H);
		self.batch.view(H)
	}
}

/// Dep position only, never a frame field: "this series, retained at least `R` back". Resolves
/// against the frame's [`Buffer<C, K>`] through the [`Has`] impl below, whose const-assert proves
/// the declared reach [`serves`](Horizon::serves) the request.
pub struct Buffering<C: Series, R: Reach>(core::marker::PhantomData<(C, R)>);

impl<C: Series, R: Reach> Cell for Buffering<C, R>
where
	C::Batch: Batch<C::Item>,
{
	type Out<'t> = <C::Batch as Batch<C::Item>>::View<'t>;

	/// Forwarded, for the same reason [`Folding`]'s is: the graph predicates match dep names against
	/// frame cell names, and a wrapper that renamed its dep would drop out of every one of them.
	/// `REACH`/`FOLDED` are what then say it is the retention and not the cell itself being asked for.
	const CLOCK: Option<Timeframe> = C::CLOCK;
	const NAME: &'static str = C::NAME;
	const REACH: Horizon = R::HORIZON;
	const RETAINED: bool = true;
}

impl<'t, C: Series, const K: Horizon, R: Reach, T> Has<'t, Buffering<C, R>, Here> for Cons<'t, Buffer<C, K>, T>
where
	C::Batch: Batch<C::Item>,
{
	fn get(&self) -> <C::Batch as Batch<C::Item>>::View<'t> {
		const {
			assert!(!matches!(R::HORIZON, Horizon::Unit), "Buffering at Unit is the bare dep C — drop the wrapper");
			assert!(!matches!(R::HORIZON, Horizon::Unbounded), "a buffer is a bounded thing; Unbounded names no window");
			assert!(K.serves(R::HORIZON), "the frame's Buffer<C, K> does not reach as far back as this Buffering<C, R> asks for");
		}
		C::Batch::narrow(self.out, R::HORIZON)
	}
}

/// The scratch is the batch itself: what a retention hands out is its own to re-own, and the
/// three hand-decomposed `Hist` fields this replaces were only ever [`Rows`]' way of saying so.
impl<C: Series, R: Reach> Nudge for Buffering<C, R>
where
	C::Item: Bump,
	C::Batch: Batch<C::Item>,
{
	type Scratch = C::Batch;

	fn stage<'t>(out: Self::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		s.stage(out, bump, h)
	}

	fn view<'l>(s: &'l Self::Scratch) -> Self::Out<'l> {
		s.view(R::HORIZON)
	}
}

/// Engine-owned point-level over a [`Series`] — [`Buffer`]'s sibling, an ordinary node (ungated,
/// `Deps = (Folding<C, Unbounded>,)`) sitting *next to* its source in the frame. Unbounded
/// because a level it never saw is one it can never stand on, and it retains nothing: one item, not
/// a window.
///
/// The invariant is monotone — once it holds a value it holds one forever. That is what a consumer
/// clocked by *another* series needs: on its own ticks this one has emitted nothing, and reading the
/// empty run there would read absence where a standing level is the truth.
#[doc(hidden)]
pub struct Latest<C: Series>
where
	C::Item: Present, {
	held: Option<<C::Item as Present>::Val>,
}

// hand-written for the same reason [`Buffer`]'s are: `derive` would demand them of the source node.
impl<C: Series> Default for Latest<C>
where
	C::Item: Present,
{
	fn default() -> Self {
		Self { held: None }
	}
}
impl<C: Series> Clone for Latest<C>
where
	C::Item: Present,
{
	fn clone(&self) -> Self {
		Self { held: self.held }
	}
}

impl<C: Series> Latest<C>
where
	C::Item: Present,
{
	const TAG: Tag = Tag::of("Latest", &[C::NAME]);
}

impl<C: Series> Cell for Latest<C>
where
	C::Item: Present,
{
	type Out<'t> = Option<<C::Item as Present>::Val>;

	/// Unlike its [`Sampling`] dep this is a frame cell of its own, so it takes a name of its own.
	const NAME: &'static str = Self::TAG.as_str();
}

impl<C: Series> Node for Latest<C>
where
	C::Item: Present,
{
	type Deps = <Self as Blind>::Deps;
	type Kernel = Opaque;
}

impl<C: Series> Blind for Latest<C>
where
	C::Item: Present,
{
	type Deps = (Folding<C, Unbounded>,);

	const WHY: &'static str = "holding the last value across a silence is a carry, and a carry has no slope of its own";

	fn advance<'t>(&'t mut self, (fresh,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if let Some(v) = fresh.iter().rev().find_map(|x| x.present()) {
			self.held = Some(v);
		}
		self.held
	}
}

/// Dep position only, never a frame field: "the last value `C` produced, whenever that was".
/// Resolves against the frame's [`Latest<C>`] through the [`Has`] impl below.
///
/// [`Buffering`]'s third sibling, and the point where the other two have a window: that one is a
/// reach the engine retains, [`Folding`] a reach the node retains, this one a single level the
/// engine carries across every tick the series was silent.
pub struct Sampling<C: Series>(core::marker::PhantomData<C>);

impl<C: Series> Cell for Sampling<C>
where
	C::Item: Present,
{
	type Out<'t> = Option<<C::Item as Present>::Val>;

	/// Forwarded, for the same reason [`Buffering`]'s is: the graph predicates match dep names against
	/// frame cell names, and a wrapper that renamed its dep would drop out of every one of them.
	const CLOCK: Option<Timeframe> = C::CLOCK;
	const NAME: &'static str = C::NAME;
	const RETAINED: bool = true;
}

impl<'t, C: Series, T> Has<'t, Sampling<C>, Here> for Cons<'t, Latest<C>, T>
where
	C::Item: Present,
{
	fn get(&self) -> Option<<C::Item as Present>::Val> {
		self.out
	}
}

impl<C: Series> Nudge for Sampling<C>
where
	C::Item: Present,
	<C::Item as Present>::Val: Bump,
{
	type Scratch = Option<<C::Item as Present>::Val>;

	fn stage<'t>(out: Self::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		match bump {
			Some(slot) => {
				let (v, dh) = Bump::bump(out, slot, h);
				*s = v;
				dh
			}
			None => {
				*s = out;
				0.0
			}
		}
	}

	fn view<'l>(s: &'l Self::Scratch) -> Self::Out<'l> {
		*s
	}
}

/// Dep position only, never a frame field: "this dep, `H` of which I hold myself". The out is the
/// bare cell's — nothing wraps the data and nothing is retained for it, so this is a pure
/// declaration, the node's own recurrence or window stated where it is actually about.
///
/// [`Buffering`]'s sibling, differing only in who holds the history. That difference is the whole
/// reason they are two types: a gate can re-warm what the engine holds and cannot re-warm what the
/// node does, and their `Has` routes resolve off different frame cells anyway — one `Buffer<C, K>`,
/// the other `C` — which a single wrapper could not disambiguate in a frame carrying both.
pub struct Folding<C: Cell, R: Reach>(core::marker::PhantomData<(C, R)>);

impl<C: Cell, R: Reach> Cell for Folding<C, R> {
	type Out<'t> = C::Out<'t>;

	const CLOCK: Option<Timeframe> = C::CLOCK;
	const FOLDED: bool = true;
	/// Forwarded: `Roots::required_events` matches dep names against frame cell names, and a wrapper
	/// that renamed its dep would drop out of it.
	const NAME: &'static str = C::NAME;
	const REACH: Horizon = R::HORIZON;
}

impl<'t, C: Cell, R: Reach, T> Has<'t, Folding<C, R>, Here> for Cons<'t, C, T> {
	fn get(&self) -> C::Out<'t> {
		const { assert!(!matches!(R::HORIZON, Horizon::Unit), "a Folding at Unit is the bare dep C — drop the wrapper") }
		self.out
	}
}

impl<C: Nudge, R: Reach> Nudge for Folding<C, R> {
	type Scratch = C::Scratch;

	fn stage<'t>(out: C::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		C::stage(out, s, bump, h)
	}

	fn view<'l>(s: &'l Self::Scratch) -> C::Out<'l> {
		C::view(s)
	}
}

/// Dep position only, never a frame field: the input that *dominates*. While it reads false its
/// consumer does not advance — no other dep is pulled, and the out is [`Latent::latent`].
///
/// [`Buffering`]/[`Folding`]'s third sibling. Those two say how far back an input is read; this one
/// says the input is permission rather than data. Being a dep is the point: what gates a node is
/// also what it depends on, and stating it anywhere else makes an edge no reader of `Deps` can see.
///
/// Gating deps lead the tuple — see [`Pull::open`].
pub struct Gating<C: Gate>(core::marker::PhantomData<C>);

impl<C: Gate> Cell for Gating<C> {
	type Gates = Yes;
	type Out<'t> = bool;

	/// Forwarded, for the same reason [`Folding`]'s is: the graph predicates match dep names against
	/// frame cell names, and a wrapper that renamed its dep would drop out of every one of them.
	const CLOCK: Option<Timeframe> = C::CLOCK;
	const NAME: &'static str = C::NAME;

	fn opens(out: <Self as Cell>::Out<'_>) -> bool {
		out
	}
}

impl<'t, C: Gate, T> Has<'t, Gating<C>, Here> for Cons<'t, C, T> {
	fn get(&self) -> bool {
		self.out
	}
}

/// A gate carries no signal to differentiate against — [`Bump`] for `bool` already reads as much —
/// so the column its slot owns stays NaN.
impl<C: Gate> Nudge for Gating<C> {
	type Scratch = bool;

	fn stage<'t>(out: <Self as Cell>::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		match bump {
			Some(slot) => {
				let (v, dh) = Bump::bump(out, slot, h);
				*s = v;
				dh
			}
			None => {
				*s = out;
				0.0
			}
		}
	}

	fn view<'l>(s: &'l Self::Scratch) -> <Self as Cell>::Out<'l> {
		*s
	}
}

impl DepSet for () {
	type Lead = No;
	type Outs<'t> = ();

	const CLOCKS: &'static [Option<Timeframe>] = &[];
	const FOLDS: &'static [bool] = &[];
	const GATES: &'static [bool] = &[];
	const NAMES: &'static [&'static str] = &[];
	const REACH: &'static [Horizon] = &[];
	const RETAINS: &'static [bool] = &[];
}
impl<'t, F> Pull<'t, F, ()> for () {
	fn pull(_: &F) {}

	fn open(_: &F) -> bool {
		true
	}
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

	fn lead_fires(_: &Self::Outs<'_>) -> usize {
		panic!("a per-element kernel walks its leading dep, and a node depending on nothing has none")
	}
}

macro_rules! impl_arity {
	// every arity at once: impl the whole list, then recurse on its tail. Names are macro-local, so a
	// suffix of the alphabet is as good a type list as a prefix.
	(@all) => {};
	(@all $Th:ident $Ih:ident $vh:ident $sh:ident $(, $T:ident $I:ident $v:ident $s:ident)*) => {
		impl_arity!($Th $Ih $vh $sh $(, $T $I $v $s)*);
		impl_arity!(@all $($T $I $v $s),*);
	};
	// the head is split out because two things read it and nothing else: [`DepSet::Lead`] (only the
	// leading dep can gate) and [`DepFlat::lead_fires`] (only the leading dep drives).
	($Th:ident $Ih:ident $vh:ident $sh:ident $(, $T:ident $I:ident $v:ident $s:ident)*) => {
		impl<$Th: Cell $(, $T: Cell)*> DepSet for ($Th, $($T,)*) {
			type Outs<'t> = ($Th::Out<'t>, $($T::Out<'t>,)*);
			type Lead = <$Th as Cell>::Gates;

			const CLOCKS: &'static [Option<Timeframe>] = &[$Th::CLOCK $(, $T::CLOCK)*];
			const FOLDS: &'static [bool] = &[$Th::FOLDED $(, $T::FOLDED)*];
			const GATES: &'static [bool] = &[<$Th::Gates as Bit>::VALUE $(, <$T::Gates as Bit>::VALUE)*];
			const NAMES: &'static [&'static str] = &[$Th::NAME $(, $T::NAME)*];
			const REACH: &'static [Horizon] = &[$Th::REACH $(, $T::REACH)*];
			const RETAINS: &'static [bool] = &[$Th::RETAINED $(, $T::RETAINED)*];
		}
		impl<'t, F, $Th: Cell, $Ih $(, $T: Cell, $I)*> Pull<'t, F, ($Ih, $($I,)*)> for ($Th, $($T,)*)
		where F: Has<'t, $Th, $Ih> $(+ Has<'t, $T, $I>)* {
			fn pull(f: &F) -> Self::Outs<'t> where F: 't {
				(Has::<'t, $Th, $Ih>::get(f), $(Has::<'t, $T, $I>::get(f),)*)
			}

			fn open(f: &F) -> bool where F: 't {
				const {
					assert!(
						gating_leads(<Self as DepSet>::GATES),
						"a `Gating` dep precedes the plain ones: a closed gate must pull nothing, and `open` reads the tuple left to right"
					);
					assert!(
						!any(<Self as DepSet>::GATES) || !any(<Self as DepSet>::FOLDS),
						"a gated node cannot hold its own reach: a closed gate pulls no deps, so a `Folding` dep never re-warms — retain it in the frame instead (write the dep as `Buffering<C, R>`), or drop the `Gating` dep"
					);
				}
				(!<$Th::Gates as Bit>::VALUE || $Th::opens(Has::<'t, $Th, $Ih>::get(f)))
					$(&& (!<$T::Gates as Bit>::VALUE || $T::opens(Has::<'t, $T, $I>::get(f))))*
			}
		}
		impl<$Th: Nudge $(, $T: Nudge)*> DepFlat for ($Th, $($T,)*)
		where for<'x> <$Th as Cell>::Out<'x>: Flat $(, for<'x> <$T as Cell>::Out<'x>: Flat)* {
			const DIMS: &'static [&'static [usize]] = &[<<$Th as Cell>::Out<'static> as Flat>::DIMS $(, <<$T as Cell>::Out<'static> as Flat>::DIMS)*];
			const LEN: usize = <<$Th as Cell>::Out<'static> as Flat>::LEN $(+ <<$T as Cell>::Out<'static> as Flat>::LEN)*;

			type Scratch = ($Th::Scratch, $($T::Scratch,)*);

			fn flat(outs: &Self::Outs<'_>, dst: &mut [f64]) {
				assert_eq!(dst.len(), Self::LEN);
				let ($vh, $($v,)*) = outs;
				let mut off = 0;
				$vh.flat(&mut dst[off..off + <<$Th as Cell>::Out<'static> as Flat>::LEN]);
				off += <<$Th as Cell>::Out<'static> as Flat>::LEN;
				$(
					$v.flat(&mut dst[off..off + <<$T as Cell>::Out<'static> as Flat>::LEN]);
					off += <<$T as Cell>::Out<'static> as Flat>::LEN;
				)*
				debug_assert_eq!(off, Self::LEN);
			}

			fn stage<'t>(outs: Self::Outs<'t>, scratch: &mut Self::Scratch, slot: usize, h: f64) -> f64 {
				let ($vh, $($v,)*) = outs;
				let ($sh, $($s,)*) = scratch;
				let (mut off, mut realized) = (0, 0.0);
				{
					let len = <<$Th as Cell>::Out<'static> as Flat>::LEN;
					let bump = if (off..off + len).contains(&slot) { Some(slot - off) } else { None };
					let dh = <$Th as Nudge>::stage($vh, $sh, bump, h);
					if bump.is_some() {
						realized = dh;
					}
					off += len;
				}
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
				)*
				debug_assert_eq!(off, Self::LEN);
				realized
			}

			fn view<'l>(scratch: &'l Self::Scratch) -> Self::Outs<'l> {
				let ($sh, $($s,)*) = scratch;
				(<$Th as Nudge>::view($sh), $(<$T as Nudge>::view($s),)*)
			}

			fn lead_fires(outs: &Self::Outs<'_>) -> usize {
				let ($vh, ..) = outs;
				$vh.fires()
			}
		}
	};
}
// 12 is std's ceiling, not ours: `Scratch` is a tuple of the deps' own scratches, and `Default` (like
// the rest of std's tuple impls) stops at 12. `F` is skipped because it names the frame — type names,
// unlike bindings, aren't macro-hygienic.
impl_arity!(@all
	A Ia a sa, B Ib b sb, C Ic c sc, D Id d sd, E Ie e se, G Ig g sg, H Ih h sh, J Ij j sj, K Ik k sk,
	L Il l sl, M Im m sm, N In n sn
);

/// Advances `node` over `frame` and pushes its output — unless a [`Gating`] dep reads false, when
/// nothing is pulled and the out is the node's [`Dark`] reading. The `Pull` bound is the engine's
/// reason to exist: a node stepped before its deps are in the frame does not compile.
// `profile` (off by default): a sweep is generic free fns, so `step::<Screener, _, _>` already
// carries the node type in its mangled symbol — but each is small enough that LLVM folds the whole
// DAG into one frame and an external profiler has nothing to attribute to. This is an attribute, not
// a branch: without the feature it is not in the source the compiler sees, so the framework's
// contribution to measurement costs a shipped build nothing.
//
// It sits on the entry points `graph!` emits and nowhere else, so a fired node is *one* box. A
// shared leg that took its own frame would name the same node a second time and — since the frame is
// passed by value and returned grown — pay the `Cons` copy twice, which reads on the flamegraph as
// the node costing more the later it sits in the sweep.
#[cfg_attr(feature = "profile", inline(never))]
pub fn step<'t, N, F, I>(frame: F, node: &'t mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I>,
	N::Out<'t>: Dark<<N::Deps as DepSet>::Lead>,
	F: 't, {
	let out = match <N::Deps as Pull<'t, F, I>>::open(&frame) {
		true => <N::Kernel as Level<N>>::advance(node, <N::Deps as Pull<'t, F, I>>::pull(&frame)),
		false => Dark::dark(),
	};
	Cons { out, tail: frame }
}

/// [`step`] for an [`Emit`]: clear the engine's buffer, let the node fill it, push it as the frame's
/// out. The frame is keyed on `E` itself — the [`Emitter`] is storage, not a cell — so a dep naming
/// `E` resolves through the ordinary [`Has`] impl. A dark one needs no [`Dark`] reading: not
/// emitting *is* the empty run — which is equally the reading of an undemanded one, and of one whose
/// [`Emit::CLOCK`] has not come round.
///
/// `ts` is the tick's event time, which is what a clock is read against: a node's rate must be a
/// statement about the market, and the count of ticks is a statement about the feed's batching.
#[cfg_attr(feature = "profile", inline(never))]
pub fn step_emit<'t, E, F, I>(frame: F, e: &'t mut Emitter<E>, demanded: bool, ts: i64) -> Cons<'t, E, F>
where
	E: Emit,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Deps: Pull<'t, F, I>,
	F: 't, {
	e.buf.clear();
	if demanded && <E::Deps as Pull<'t, F, I>>::open(&frame) && e.opens(ts) {
		<E::Kernel as Run<E>>::emit(&mut e.node, <E::Deps as Pull<'t, F, I>>::pull(&frame), &mut e.buf);
	}
	Cons { out: &e.buf, tail: frame }
}

/// One node firing, flattened: values and the finite-difference local Jacobian wrt its deps,
/// à la Jane Street's "Computations that differentiate and document themselves".
#[derive(Clone, Copy)]
pub struct Fire<'a> {
	/// How the node says what it looks like. [`Glance`] rather than `Debug`: an out's `Glance` is
	/// hand-written where its `Debug` is whatever the derive emitted, and a recorder that reaches for
	/// the derive pays for it once per node per tick.
	pub glance: &'a dyn Glance,
	pub dims: &'static [usize],
	pub plots: &'static [Plot],
	/// The node's [`Cell::CLOCK`] — the period one of its elements *covers*, which is what a viz needs
	/// to draw a slower series at its own width instead of the chart's.
	pub clock: Option<Timeframe>,
	/// Elements the node fired this tick: slice len, or 0/1 for scalar/`Option` outs.
	pub fires: usize,
	/// Flattened *last* element; `None` = didn't fire.
	pub vals: Option<&'a [f64]>,
	pub dep_dims: &'a [&'static [usize]],
	/// Row-major `out_len × sum(dep lens)`, deps concatenated in `Deps` order (each batch dep as
	/// its last element). NaN = no signal. `None` when the node didn't fire.
	///
	/// The *one-step* reading: each dep's last element perturbed, prior state held fixed.
	/// Differentiating the body and finite-differencing it both land on that same number, which is
	/// why one array carries both and [`exact`](Self::exact) only says how it was reached. What
	/// neither says is anything about the rest of a dep's reach — a node reading `.trailing()` over
	/// 181 bars has one column here describing bar 180 and silence about bars 0–179. That is what
	/// [`exact_block`](Self::exact_block) is asked for separately
	/// (`r[kernels.jac.two-quantities]`).
	pub jac: Option<&'a [f64]>,
	/// The reading [`jac`](Self::jac) is silent about, where the kernel has one: per dep in `Deps`
	/// order, `out_len × widths[d]` row-major and appended, oldest lag first — so a dep's *last*
	/// element-group is its `jac` column and the groups before it are the rest of the reach that
	/// column says nothing about (`r[kernels.jac.two-quantities]`). `None` below [`Want::Exact`], and
	/// for a kernel that cannot answer.
	pub exact_block: Option<(&'a [f64], &'a [usize])>,
	/// Whether [`jac`](Self::jac) was differentiated or guessed — the difference between labelling a
	/// viz `∂ (exact)` and `∂ (±h)`. Not a claim that the column covers the dep's reach: that is
	/// [`fidelity`](Self::fidelity), which the kernel states once rather than per tick.
	pub exact: bool,
	/// How much of what the body read the Jacobian covers — the kernel's own claim, constant across
	/// ticks. Carried here so a reader holding one fire needs no second lookup against
	/// `Graph::FIDELITY` to say what the column is worth.
	pub fidelity: Fidelity,
	/// The node's equation rendered as a formula (LaTeX/infix), [`Pure`] nodes only.
	pub formula: Option<&'a dyn core::fmt::Display>,
	/// Simplified `∂out/∂dep` formulas, [`Pure`] nodes only.
	pub deriv: Option<&'a dyn core::fmt::Display>,
	/// Value-annotated intermediate-value tree ([`Ast::trace`]) over this tick's deps, [`Pure`]
	/// nodes only.
	pub trace: Option<&'a dyn core::fmt::Display>,
}

impl<'a> Fire<'a> {
	/// Everything a stepped node reads off its out and its [`Flats`]. The three algebra fields are
	/// the exception rather than the shape, spliced in with `..` at the one site that states them.
	/// `flat: None` is the unfired reading — no flattening happened, so there is none to report.
	#[inline]
	fn of<T: Flat + Glance>(out: &'a T, plots: &'static [Plot], clock: Option<Timeframe>, fidelity: Fidelity, dep_dims: &'a [&'static [usize]], flat: Option<Flats<'a>>) -> Self {
		Fire {
			glance: out,
			dims: T::DIMS,
			plots,
			clock,
			fidelity,
			fires: out.fires(),
			vals: flat.and_then(|f| f.fired.then_some(f.vals)),
			dep_dims,
			jac: flat.and_then(|f| f.jac),
			exact_block: flat.and_then(|f| f.block),
			exact: flat.is_some_and(|f| f.exact),
			formula: None,
			deriv: None,
			trace: None,
		}
	}
}

/// The buffers the observed sweep writes each node's flattenings into. Every one of them is
/// `Flat::LEN` wide — a per-node constant — so they settle at the graph's widest node within the
/// first tick and never allocate again; owning them per node instead put a malloc/free pair on
/// each, which at graph scale is most of what reaching an observer costs.
#[derive(Default)]
pub struct Sweep {
	vals: alloc::vec::Vec<f64>,
	deps: alloc::vec::Vec<f64>,
	jac: alloc::vec::Vec<f64>,
	bumped: alloc::vec::Vec<f64>,
	/// The exact block and its per-dep widths. Grown rather than sized, for the reason
	/// [`Exact::out`] is — but reused across nodes and ticks like every buffer beside it.
	block: alloc::vec::Vec<f64>,
	widths: alloc::vec::Vec<usize>,
}

/// The un-bumped flattenings of one node's out and deps, plus the Jacobian read off them — the
/// observed leg every stepped node shares, whatever kind of node it is. A view into the [`Sweep`]
/// rather than an owner: the next node overwrites all of it.
#[derive(Clone, Copy)]
struct Flats<'s> {
	fired: bool,
	vals: &'s [f64],
	deps: &'s [f64],
	jac: Option<&'s [f64]>,
	exact: bool,
	block: Option<(&'s [f64], &'s [usize])>,
}

/// The buffers one node's derivative readings are written into — [`Jac`]'s and [`Exact`]'s halves
/// of the [`Sweep`], handed to the one closure that knows which kernel to ask.
struct Reads<'a> {
	dep_buf: &'a [f64],
	out_buf: &'a [f64],
	jac: &'a mut [f64],
	bumped: &'a mut alloc::vec::Vec<f64>,
	block: &'a mut alloc::vec::Vec<f64>,
	widths: &'a mut alloc::vec::Vec<usize>,
}

impl<'s> Flats<'s> {
	/// `fill` writes the node's derivative readings and reports `(the Jacobian is differentiated,
	/// the exact block was filled)`; `None` is an observer that reads no further than
	/// [`Want::Vals`], and it is asked for only where there is an out to differentiate.
	#[inline]
	fn of<'d, O: Flat, D: DepFlat>(sweep: &'s mut Sweep, out: &O, deps: &D::Outs<'d>, fill: Option<impl FnOnce(Reads<'_>) -> (bool, bool)>) -> Self {
		// r[impl outs.flat.nonempty]
		const {
			assert!(
				O::LEN > 0,
				"an out flattens to at least one slot: a fire is read downstream as the slots being there, so a zero-slot out would be indistinguishable from not firing"
			);
		}
		let Sweep {
			vals,
			deps: dep_buf,
			jac,
			bumped,
			block,
			widths,
		} = sweep;
		vals.clear();
		vals.resize(O::LEN, f64::NAN);
		let fired = out.flat(vals);
		dep_buf.clear();
		dep_buf.resize(D::LEN, f64::NAN);
		D::flat(deps, dep_buf);
		let (jac, exact, block) = match fill.filter(|_| fired) {
			// NaN, not zero: a column the kernel leaves alone is one it has no signal for, and the
			// absorbing element is what says so (§1.6).
			Some(fill) => {
				jac.clear();
				jac.resize(O::LEN * D::LEN, f64::NAN);
				// `block`/`widths` are left as the last node found them: a kernel filling them clears
				// them itself, and one that does not must cost `Want::Jac` nothing at all.
				let (exact, filled) = fill(Reads {
					dep_buf,
					out_buf: vals,
					jac,
					bumped,
					block,
					widths,
				});
				(Some(&**jac), exact, filled.then(|| (&**block, &**widths)))
			}
			None => (None, false, None),
		};
		Flats {
			fired,
			vals,
			deps: dep_buf,
			jac,
			exact,
			block,
		}
	}
}

/// How much of a fire the observer reads. Everything above [`Want::Vals`] is second order — an
/// [`Opaque`] node's Jacobian is one `clone_from` plus one re-`advance` per scalar dep slot, and a
/// [`Pure`] one's is a `grad` pass over the pre-advance body — so it is priced per tick, never per
/// build.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Want {
	Nothing,
	Vals,
	Jac,
	/// One column group per *lag* of each dep's reach instead of one for its newest — the reading
	/// [`Jac`] is silent about (`r[kernels.jac.two-quantities]`). A kernel that cannot answer fills
	/// nothing, so asking costs only the kernels that can.
	Exact,
}

/// Sees every [`step_obs`] as it happens: one interpretation choke point, many interpretations.
/// Step order IS topo order, so the observed sequence doubles as the graph's static topology; dep
/// names never seen as stepped nodes are roots — apps seed root activations via [`observe_root`].
pub trait Observer {
	/// Asked once per step, of the node about to run. No default: an observer that has not said what
	/// it reads is one nobody priced, and the answer can cost that node a re-advance per dep slot.
	///
	/// Per-node, so only the node actually being inspected pays. Sound because a Jacobian is
	/// node-local — nothing downstream can see whether an upstream node was observed
	/// (`r[observe.noninvasive]`) — which is what lets a consumer schedule the expensive reading
	/// instead of amortizing it.
	fn want(&self, node: &'static str) -> Want;
	/// `gates` is [`DepSet::GATES`]: positional with `deps`, marking the ones that are control edges
	/// rather than data. All-`false` for ungated nodes, empty for roots.
	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>);
}

impl Observer for () {
	fn want(&self, _: &'static str) -> Want {
		Want::Nothing
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], _: Fire<'_>) {}
}

/// So a long-lived observer can be composed into a per-tick pair without being moved into it.
impl<O: Observer + ?Sized> Observer for &mut O {
	fn want(&self, node: &'static str) -> Want {
		(**self).want(node)
	}

	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>) {
		(**self).on(node, deps, gates, fire);
	}
}

/// Two interpretations of the same sweep — e.g. an app's own assertions next to a viz recorder.
impl<A: Observer, B: Observer> Observer for (A, B) {
	fn want(&self, node: &'static str) -> Want {
		self.0.want(node).max(self.1.want(node))
	}

	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>) {
		self.0.on(node, deps, gates, fire);
		self.1.on(node, deps, gates, fire);
	}
}

/// One finite-difference column: re-advance `clone`, restored to `pre`, on `deps` with element
/// `slot` bumped by about `h`, writing the bumped out into `bumped`; returns whether it fired and
/// the perturbation the dep actually applied. Isolated from [`step_obs`] so the re-advance lifetime
/// is purely local — the clone and its nudged deps never escape, which keeps the self-borrowing
/// `advance` from pinning them to the caller's tick lifetime.
/// `clone` and `scratch` are both the caller's, each overwritten wholly per column, so allocating
/// either per column would be dead work.
fn fd_col<'d, N>(pre: &N, clone: &mut N, deps: BlindOuts<'d, N>, scratch: &mut <<N as Blind>::Deps as DepFlat>::Scratch, slot: usize, h: f64, bumped: &mut [f64]) -> (bool, f64)
where
	N: Blind,
	<N as Blind>::Deps: DepFlat,
	BlindOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let dh = <<N as Blind>::Deps as DepFlat>::stage(deps, scratch, slot, h);
	clone.clone_from(pre);
	(clone.advance(<<N as Blind>::Deps as DepFlat>::view(scratch)).flat(bumped), dh)
}

/// The full finite-difference Jacobian, one column per dep element: `col` re-steps the node with
/// that element bumped by about `h`, writing the bumped out into `bumped` and returning whether it
/// fired and the perturbation the dep actually applied. Columns the caller's NaN fill is left
/// standing in, where a dep is unfired or the bump crossed a firing branch. `out_buf`/`dep_buf` are
/// the un-bumped flattenings; `bumped` is the caller's scratch, overwritten wholly.
fn fd_cols(dep_buf: &[f64], out_buf: &[f64], jac: &mut [f64], bumped: &mut alloc::vec::Vec<f64>, mut col: impl FnMut(usize, f64, &mut [f64]) -> (bool, f64)) {
	let (out_len, dep_len) = (out_buf.len(), dep_buf.len());
	debug_assert_eq!(jac.len(), out_len * dep_len, "the Jacobian buffer is row-major out_len × dep_len");
	bumped.clear();
	bumped.resize(out_len, f64::NAN);
	for slot in 0..dep_len {
		let x = dep_buf[slot];
		if x.is_nan() {
			continue;
		}
		// `dh`, not `h`: a quantized dep moves in whole ticks, and dividing by a step it never took
		// is a fabricated slope. `0.0` ⇒ the slot has no derivative at all.
		let (fired, dh) = col(slot, (x.abs() * 1e-6).max(1e-9), bumped);
		if !fired || dh == 0.0 {
			continue; // bump crossed a firing branch, or the slot is discrete — column stays NaN
		}
		// `chunks_exact_mut` rather than `jac[i * dep_len + slot]`: the row is the unit being written,
		// and spelling it as one drops both the multiply and the bounds check the optimizer would have
		// had to reassociate through it to elide.
		for (row, (b, o)) in jac.chunks_exact_mut(dep_len).zip(bumped.iter().zip(out_buf)) {
			row[slot] = (b - o) / dh;
		}
	}
}

/// [`fd_cols`] over a [`Blind`] node's re-[`advance`](Blind::advance), via [`fd_col`] — the whole of
/// what [`Opaque`] has instead of a derivative.
fn fd_jac<'d, N>(pre: &N, j: Jac<'_, DepOuts<'d, N>>)
where
	N: Blind + Node<Deps = <N as Blind>::Deps>,
	<N as Blind>::Deps: DepFlat,
	BlindOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let mut scratch = <<N as Blind>::Deps as DepFlat>::Scratch::default();
	let mut clone = pre.clone();
	let deps = j.deps;
	fd_cols(j.dep_buf, j.out_buf, j.out, j.bumped, |slot, h, bumped| {
		fd_col::<N>(pre, &mut clone, deps, &mut scratch, slot, h, bumped)
	})
}

/// [`step`] + [`Observer::on`] before the push. The `()` observer erases to exactly `step`.
///
/// Under an active observer, each fired node's Jacobian comes from its [`Level`] kernel: a [`Pure`]
/// node differentiates its body, a [`Blind`] one is finite-differenced — clone the pre-advance node,
/// [`Nudge`] the *last* element of one dep (batch deps copied into scratch), re-advance the clone at
/// a shorter lifetime, diff the last out elements.
#[cfg_attr(feature = "profile", inline(never))]
pub fn step_obs<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, sweep: &mut Sweep, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: Glance + Dark<<N::Deps as DepSet>::Lead>,
	F: 't, {
	let run = <N::Deps as Pull<'t, F, I>>::open(&frame);
	step_seen(frame, node, run, <N::Out<'t> as Dark<<N::Deps as DepSet>::Lead>>::dark, sweep, obs)
}

/// [`step_obs`] for a node the graph derived no standing demand for: it advances only while every
/// gate dominating its readers is open *and* its own gates are. The bound is [`Latent`] rather than
/// [`Dark`] because an undemanded node is dark whatever its own `Deps` say — which is precisely the
/// obligation `graph!` puts on a node it suppresses.
#[doc(hidden)]
#[cfg_attr(feature = "profile", inline(never))]
pub fn step_when_obs<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, demanded: bool, sweep: &mut Sweep, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: Glance + Latent,
	F: 't, {
	let run = demanded && <N::Deps as Pull<'t, F, I>>::open(&frame);
	step_seen(frame, node, run, <N::Out<'t> as Latent>::latent, sweep, obs)
}

/// The observed sweep of one level node, given whether it runs at all and what it reads if it does
/// not. `unrun` is a thunk rather than a value because [`Dark<No>::dark`](Dark) is unreachable by
/// construction — an ungated node has no dark branch to evaluate.
#[cfg_attr(feature = "profile", inline(always))]
fn step_seen<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, run: bool, unrun: impl FnOnce() -> N::Out<'t>, sweep: &mut Sweep, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: Glance,
	F: 't, {
	const {
		assert!(
			Plot::coherent(N::PLOTS, <N::Out<'static> as Flat>::DIMS),
			"a multi-plot node must name each plot's slots (`[]` claims all of them); a candle plot must name four and be an overlay; a plot's `labels` axes must multiply out to its slot count"
		)
	}

	let want = obs.want(N::NAME);

	// gate closed or nobody reading: no advance, no dep flatten, no derivative — an unfired `Fire` is
	// the honest view.
	if !run {
		let out: N::Out<'t> = unrun();
		if want != Want::Nothing {
			let fire = Fire::of(&out, N::PLOTS, <N as Cell>::CLOCK, <N::Kernel as Level<N>>::FIDELITY, <N::Deps as DepFlat>::DIMS, None);
			obs.on(N::NAME, <N::Deps as DepSet>::NAMES, <N::Deps as DepSet>::GATES, fire);
		}
		return Cons { out, tail: frame };
	}

	if want == Want::Nothing {
		let out = <N::Kernel as Level<N>>::advance(node, <N::Deps as Pull<'t, F, I>>::pull(&frame));
		return Cons { out, tail: frame };
	}

	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	// before the advance, which lends the node for the rest of the tick: the derivative reading is of
	// the state the value was computed from, and `r[observe.noninvasive]` is what makes taking it
	// invisible to the run.
	let pre = <N::Kernel as Level<N>>::pre(node, want, deps);
	let out = <N::Kernel as Level<N>>::advance(node, deps);
	let flat = Flats::of::<_, N::Deps>(
		sweep,
		&out,
		&deps,
		(want >= Want::Jac).then_some(|r: Reads<'_>| {
			let exact = <N::Kernel as Level<N>>::jac(
				&pre,
				Jac {
					deps,
					dep_buf: r.dep_buf,
					out_buf: r.out_buf,
					out: r.jac,
					bumped: r.bumped,
				},
			);
			let block = want >= Want::Exact
				&& <N::Kernel as Level<N>>::exact(
					&pre,
					Exact {
						deps,
						dep_buf: r.dep_buf,
						out_buf: r.out_buf,
						out: r.block,
						widths: r.widths,
					},
				);
			(exact, block)
		}),
	);

	// the algebra reading, where the kernel has one: `formula` is `None` for every `Opaque` node and
	// for every node below `Want::Jac`, so both fall out of the same `map`.
	let formula = <N::Kernel as Level<N>>::formula(&pre);
	let deriv = formula.map(|f| Derivs {
		names: <N::Deps as DepSet>::NAMES,
		parts: (0..<N::Deps as DepFlat>::LEN).map(|i| f.diff(i).simplify()).collect(),
	});
	let trace = formula.map(|f| f.trace(flat.deps));

	let fire = Fire {
		formula: formula.map(|f| f as &dyn core::fmt::Display),
		deriv: deriv.as_ref().map(|d| d as &dyn core::fmt::Display),
		trace: trace.as_ref().map(|t| t as &dyn core::fmt::Display),
		..Fire::of(&out, N::PLOTS, <N as Cell>::CLOCK, <N::Kernel as Level<N>>::FIDELITY, <N::Deps as DepFlat>::DIMS, Some(flat))
	};
	obs.on(N::NAME, <N::Deps as DepSet>::NAMES, <N::Deps as DepSet>::GATES, fire);
	Cons { out, tail: frame }
}

/// [`fd_cols`] over an [`Emit`]'s re-`emit`. It stays separate from [`fd_jac`] because the two
/// re-step through different traits — merging them would cost a vtable, which is the one thing this
/// leg cannot pay. The re-`emit` needs no [`fd_col`]-style lifetime isolation (`&mut self` never
/// lends the node), so its column body is inline; everything a column overwrites wholly — the deps'
/// scratch, the clone, its output run — is hoisted across the loop.
fn fd_jac_emit<'d, E>(pre: &E, j: Jac<'_, EmitOuts<'d, E>>)
where
	E: Runs + Emit<Deps = <E as Runs>::Deps>,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Flat,
	<E as Emit>::Deps: DepFlat,
	EmitOuts<'d, E>: Copy, {
	let mut scratch = <<E as Emit>::Deps as DepFlat>::Scratch::default();
	let mut emitted = alloc::vec::Vec::new();
	let mut clone = pre.clone();
	let deps = j.deps;
	fd_cols(j.dep_buf, j.out_buf, j.out, j.bumped, |slot, h, bumped| {
		let dh = <<E as Emit>::Deps as DepFlat>::stage(deps, &mut scratch, slot, h);
		emitted.clear();
		clone.clone_from(pre);
		<E as Runs>::emit(&mut clone, <<E as Emit>::Deps as DepFlat>::view(&scratch), &mut emitted);
		(emitted.as_slice().flat(bumped), dh)
	});
}

/// [`step_emit`] + [`Observer::on`] before the push — [`step_obs`]'s sibling, with the engine's
/// buffer standing in for the node's out. A gate-closed, undemanded or off-clock emit node is simply
/// the empty run, which is why this one needs no [`Latent`] sibling the way [`step_when_obs`] is one.
#[cfg_attr(feature = "profile", inline(never))]
pub fn step_emit_obs<'t, E, F, I, O: Observer>(frame: F, e: &'t mut Emitter<E>, demanded: bool, ts: i64, sweep: &mut Sweep, obs: &mut O) -> Cons<'t, E, F>
where
	E: Emit,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Flat + Glance,
	E::Deps: Pull<'t, F, I> + DepFlat,
	EmitOuts<'t, E>: Copy,
	F: 't, {
	const {
		assert!(
			Plot::coherent(<E as Emit>::PLOTS, <E::Item as Flat>::DIMS),
			"a multi-plot node must name each plot's slots (`[]` claims all of them); a candle plot must name four and be an overlay; a plot's `labels` axes must multiply out to its slot count"
		)
	}
	e.buf.clear();
	let want = obs.want(E::NAME);

	// gate closed, nobody reading, or the period still running: no emit, no dep flatten, no FD — the
	// empty run is the honest view.
	if !demanded || !<E::Deps as Pull<'t, F, I>>::open(&frame) || !e.opens(ts) {
		let out: &'t [E::Item] = &e.buf;
		if want != Want::Nothing {
			let fire = Fire::of(&out, <E as Emit>::PLOTS, <E as Cell>::CLOCK, <E::Kernel as Run<E>>::FIDELITY, <E::Deps as DepFlat>::DIMS, None);
			obs.on(E::NAME, <E::Deps as DepSet>::NAMES, <E::Deps as DepSet>::GATES, fire);
		}
		return Cons { out, tail: frame };
	}

	if want == Want::Nothing {
		<E::Kernel as Run<E>>::emit(&mut e.node, <E::Deps as Pull<'t, F, I>>::pull(&frame), &mut e.buf);
		return Cons { out: &e.buf, tail: frame };
	}

	let deps = <E::Deps as Pull<'t, F, I>>::pull(&frame);
	// before the emit, for the same reason [`step_seen`]'s is: the derivative belongs to the state
	// the run was filled from.
	let pre = <E::Kernel as Run<E>>::pre(&e.node, want, deps);
	<E::Kernel as Run<E>>::emit(&mut e.node, deps, &mut e.buf);
	let out: &'t [E::Item] = &e.buf;
	let flat = Flats::of::<_, E::Deps>(
		sweep,
		&out,
		&deps,
		(want >= Want::Jac).then_some(|r: Reads<'_>| {
			let exact = <E::Kernel as Run<E>>::jac(
				&pre,
				Jac {
					deps,
					dep_buf: r.dep_buf,
					out_buf: r.out_buf,
					out: r.jac,
					bumped: r.bumped,
				},
			);
			let block = want >= Want::Exact
				&& <E::Kernel as Run<E>>::exact(
					&pre,
					Exact {
						deps,
						dep_buf: r.dep_buf,
						out_buf: r.out_buf,
						out: r.block,
						widths: r.widths,
					},
				);
			(exact, block)
		}),
	);

	let fire = Fire::of(
		&out,
		<E as Emit>::PLOTS,
		<E as Cell>::CLOCK,
		<E::Kernel as Run<E>>::FIDELITY,
		<E::Deps as DepFlat>::DIMS,
		Some(flat),
	);
	obs.on(E::NAME, <E::Deps as DepSet>::NAMES, <E::Deps as DepSet>::GATES, fire);
	Cons { out, tail: frame }
}

/// The per-dep simplified derivatives of a [`Pure`] node, `∂out/∂dep` one per line — the `deriv`
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

/// One node's compile-time shape, as [`graph!`] sees it. `name`/`deps` are
/// [`core::any::type_name`] strings: const-comparable, never persisted.
#[doc(hidden)]
pub struct NodeMeta {
	pub name: &'static str,
	pub deps: &'static [&'static str],
	/// Per-dep, positionally with `deps`: which of them are gates.
	pub gates: &'static [bool],
	/// A `latch { }` field. A latch is momentary by nature, so a consumer behind one is not standing
	/// demand: what it reads must be warm *before* the episode arms.
	pub latch: bool,
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

/// Whether a node clocked at `clock` can be fed by producers clocked at `deps` — every one of them
/// a whole divisor of it. A rate is still the node's own to declare ([`Cell::CLOCK`]); this is what
/// stops the declaration from being one the inputs cannot deliver, and it is `rates.node.whole-elements`
/// in arithmetic: a node publishing every `tf` observes whole elements of its input only where that
/// input's period tiles `tf`.
///
/// It is also what pins a period spelled twice. `Bars<TF>` names `TF` in its type and reads
/// `Ohlcs<TF>`/`Volumes<TF>`, so a clock of anything but `TF` is a build error rather than a bar
/// silently published at a rate its type denies.
#[doc(hidden)]
pub const fn clock_divides(clock: Option<Timeframe>, deps: &[Option<Timeframe>]) -> bool {
	let Some(tf) = clock else {
		// unclocked is "whenever my inputs do", which every input delivers by definition.
		return true;
	};
	let mut i = 0;
	while i < deps.len() {
		if let Some(d) = deps[i]
			&& (d.0 == 0 || tf.0 % d.0 != 0)
		{
			return false;
		}
		i += 1;
	}
	tf.0 > 0
}

/// Whether every name in `set` occurs once — [`graph!`]'s backstop on its own dedup.
#[doc(hidden)]
pub const fn distinct(set: &[&str]) -> bool {
	let mut i = 0;
	while i < set.len() {
		let mut j = i + 1;
		while j < set.len() {
			if str_eq(set[i], set[j]) {
				return false;
			}
			j += 1;
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

/// Whether `deps` names `gate` in a gating position — the const mirror of a [`Gating`] dep.
#[doc(hidden)]
pub const fn gates_on(deps: &[&str], gates: &[bool], gate: &str) -> bool {
	let mut i = 0;
	while i < deps.len() {
		if gates[i] && str_eq(deps[i], gate) {
			return true;
		}
		i += 1;
	}
	false
}

/// A latch is cut from within, so its [`Cut`](Latch::Cut) must be a stepped field naming it in a
/// [`Gating`] dep. Absent from the manifest ⇒ a root, and a root is never gated — which is exactly
/// what the old `Cut: Node` bound stood in for, minus its blindness to an [`Emit`].
#[doc(hidden)]
pub const fn cut_gated(cut: &'static str, latch: &'static str, nodes: &[NodeMeta]) -> bool {
	let mut i = 0;
	while i < nodes.len() {
		if str_eq(nodes[i].name, cut) {
			return gates_on(nodes[i].deps, nodes[i].gates, latch);
		}
		i += 1;
	}
	false
}

/// A latch whose arm is a node it also gates can never re-arm: the arm is dark exactly when the
/// latch is down. Roots are always live, so an external latch is exempt by construction — which is
/// the whole of the difference between the two flavours.
#[doc(hidden)]
pub const fn deadlocked(latch: &'static str, arms: &'static [&'static str], nodes: &[NodeMeta]) -> bool {
	let mut a = 0;
	while a < arms.len() {
		let mut i = 0;
		while i < nodes.len() {
			// a name absent from `nodes` is a root — external, always live, exempt.
			if str_eq(nodes[i].name, arms[a]) && gates_on(nodes[i].deps, nodes[i].gates, latch) {
				return true;
			}
			i += 1;
		}
		a += 1;
	}
	false
}

pub use trading_data_macros::{__graph_resolve, graph, node, node_alias};

/// The root half of the observation choke point: flatten a seeded root value and emit its
/// [`Fire`] (no deps, no jac). No-op under an inactive observer.
pub fn observe_root<'t, C, O>(out: C::Out<'t>, sweep: &mut Sweep, obs: &mut O)
where
	C: Cell,
	C::Out<'t>: Flat + Glance,
	O: Observer, {
	if obs.want(C::NAME) == Want::Nothing {
		return;
	}
	sweep.vals.clear();
	sweep.vals.resize(<C::Out<'t> as Flat>::LEN, f64::NAN);
	// a root is pulled from nothing, so there are no deps to flatten and nothing to differentiate.
	let fired = out.flat(&mut sweep.vals);
	sweep.deps.clear();
	let flat = Flats {
		fired,
		vals: &sweep.vals,
		deps: &sweep.deps,
		jac: None,
		exact: false,
		block: None,
	};
	// a root is not computed here at all: nothing read it, so nothing was omitted from a reading.
	obs.on(C::NAME, &[], &[], Fire::of(&out, &[Plot::DEFAULT], C::CLOCK, Fidelity::Exact, &[], Some(flat)));
}
