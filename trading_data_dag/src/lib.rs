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
#![feature(adt_const_params)]
#![feature(associated_type_defaults)]
#![feature(const_type_name)]

extern crate alloc;

use core::any::TypeId;

use trading_data_expr::{Ast, Expr, Vars};
use v_utils::Timeframe;

/// How far back a dep position reaches: nothing at all (a bare dep), the engine's retention
/// ([`Buffering`]), or the consumer's own state ([`Folding`]). One vocabulary for all three, so the
/// reach a node reads and the reach it holds are stated the same way — and a `const` of it drops
/// straight into const-generic position.
#[derive(Clone, core::marker::ConstParamTy, Copy, Debug, Eq, PartialEq)]
pub enum Horizon {
	/// The current value only — no history at all.
	Unit,
	Elems(usize),
	/// A window of wall clock, stated as the timeframe it is — the unit travels with the number
	/// instead of being a convention the reader has to know.
	Span(Timeframe),
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
			(Horizon::Span(k), Horizon::Span(j)) => k.0 >= j.0,
			(Horizon::Span(_), Horizon::Elems(_)) => true,
			_ => false,
		}
	}

	/// The reach that serves both — what one [`Buffer`] must retain to satisfy every consumer of the
	/// series. A span outranks any count, which is why the four variants are totally ordered and a
	/// graph can never ask for two reaches that cannot be met at once.
	pub const fn join(self, other: Horizon) -> Horizon {
		match (self, other) {
			(Horizon::Unbounded, _) | (_, Horizon::Unbounded) => Horizon::Unbounded,
			(Horizon::Span(a), Horizon::Span(b)) => Horizon::Span(if a.0 >= b.0 { a } else { b }),
			(Horizon::Span(s), _) | (_, Horizon::Span(s)) => Horizon::Span(s),
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
			Horizon::Span(tf) => {
				let len = write(&mut buf, len, b"Span(");
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
	/// The inverse is not the useful one. `Span → Elems` yields a buffer capacity, and [`Buffer`]
	/// already trims on timestamps (see [`Hist::all`]) without ever needing a count.
	pub const fn span(self, clock: Timeframe) -> Timeframe {
		match self {
			Horizon::Unit => clock,
			Horizon::Elems(n) => Timeframe(clock.0 * n as u64),
			Horizon::Span(tf) => tf,
			Horizon::Unbounded => panic!("an unbounded reach is no duration: nothing recovers such a node, so no depth warms it"),
		}
	}

	/// Only a [`Horizon::Span`] has one; the caller has already matched the variant.
	const fn ns(tf: Timeframe) -> i64 {
		(tf.0 * 1_000_000) as i64
	}
}

/// A [`Cell::NAME`] a parameter can reach. `NAME` is a `const &'static str`, and nothing that
/// formats at runtime can produce one — so the parts are spelled into a fixed buffer, in an
/// *associated* const, whose value outlives the `NAME` borrowing it. That an associated const may
/// read its impl's generics, where a free `const` item may not, is the whole reason this works.
///
/// Overflowing the buffer indexes out of bounds during const eval, i.e. fails the build.
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

	pub const fn as_str(&self) -> &str {
		match core::str::from_utf8(self.buf.split_at(self.len).0) {
			Ok(s) => s,
			Err(_) => panic!("a name is utf8, and is spelled here one whole str at a time"),
		}
	}
}

const fn write(buf: &mut [u8; 256], mut len: usize, src: &[u8]) -> usize {
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
	/// tick's batch and nothing more: a bare cell, a [`Folding`]/[`Spanning`] reach the node holds, a
	/// [`Gating`] permission.
	///
	/// This is what says whether a tick may be *withheld* from a consumer. A retained dep is there
	/// again next tick, so skipping one costs nothing; a pass-through dep skipped is a batch nobody
	/// ever sees again. Hence [`Emitter::opens`]: the engine enforces a declared rate only where it
	/// holds every input the node would have read.
	const RETAINED: bool = false;

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
	/// price-denominated slots, so it names them and rides the price pane.
	const fn coherent(plots: &'static [Plot]) -> bool {
		let mut i = 0;
		while i < plots.len() {
			if plots[i].slots.is_empty() && plots.len() > 1 {
				return false;
			}
			if plots[i].candles && (!plots[i].overlay || plots[i].slots.len() != 4) {
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
/// `slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, f64)`.
#[macro_export]
macro_rules! slice_nudge {
	([$($g:tt)*] $C:ty, $E:ty) => {
		impl<$($g)*> $crate::Series for $C {
			type Item = $E;
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
	($C:ty, $E:ty) => {
		$crate::slice_nudge!([] $C, $E);
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

pub trait Node: Cell {
	type Deps: DepSet;
	/// `&[]` draws nothing at all — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn advance<'t>(&'t mut self, deps: DepOuts<'t, Self>) -> Self::Out<'t>;
}

/// A node whose out is the run of items it fills each tick. The engine owns the run, so the struct
/// holds only what it remembers between ticks — and `emit` cannot read what it wrote last tick,
/// which [`Node::advance`]'s `self.buf.clear()` convention could only ask for. `&mut self`, not
/// `&'t mut self`: the node is not lent for the tick, only the engine's buffer is.
///
/// A gated one goes dark by emitting nothing, so it needs no [`Latent`] reading — not emitting *is*
/// the latent reading.
///
/// [`Series`]' where-clause is an obligation at each use of the bound, not an implied one (see
/// [`Episodic`]), so it is repeated wherever this bound is used.
pub trait Emit: Series
where
	for<'x> Self: Cell<Out<'x> = &'x [<Self as Series>::Item]>, {
	type Deps: DepSet;
	/// `&[]` draws nothing at all — the node stays in the topology and resolvable as a dep.
	const PLOTS: &'static [Plot] = &[Plot::DEFAULT];
	fn emit<'t>(&mut self, deps: EmitOuts<'t, Self>, out: &mut alloc::vec::Vec<Self::Item>);
}

/// Uniform binder-correct dep-tuple type for [`Emit::emit`] impls, as [`DepOuts`] is for `advance`.
pub type EmitOuts<'t, E> = <<E as Emit>::Deps as DepSet>::Outs<'t>;

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
	/// a [`Folding`]/[`Spanning`] reach, a [`Gating`] permission — has no second showing of it. Such a
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

impl<N: Episodic> Node for Armed<N>
where
	for<'t> N::Out<'t>: Episode,
{
	type Deps = (Folding<N::Trigger, { Horizon::Unbounded }>,);

	fn advance<'t>(&'t mut self, (trigger,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0 |= N::arms(trigger);
		self.0
	}
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
	/// frame buffering at exactly its `Buffering<C, H>` would hold. Without the cut, a run is only as
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
			Horizon::Span(tf) => {
				let cut = self.all[newest].ts_ns() - Horizon::ns(tf);
				self.all[..past].partition_point(|x| x.ts_ns() <= cut)
			}
			h => unreachable!("a Buffering is const-asserted bounded, and `narrowed` re-asserts it: {h:?}"),
		};
		&self.all[drop..]
	}

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
			Horizon::Span(tf) => {
				let cut = self.all[end - 1].ts_ns() - Horizon::ns(tf);
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

/// Engine-owned retention over a [`Series`] — an ordinary node (`Deps = (Folding<C, H>,)`, ungated)
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

	/// The one thing `derive(Clone)` would not have given: its `clone_from` is `*self = clone()`, and
	/// a buffer holding a whole reach is what [`fd_col`] re-clones once per dep element.
	fn clone_from(&mut self, src: &Self) {
		self.buf.clone_from(&src.buf);
		self.watermark = src.watermark;
	}
}

impl<C: Series, const H: Horizon> Buffer<C, H> {
	const REACH_TAG: Tag = H.tag();
	const TAG: Tag = Tag::of("Buffer", &[C::NAME, Self::REACH_TAG.as_str()]);
}

impl<C: Series, const H: Horizon> Cell for Buffer<C, H> {
	type Out<'t> = Hist<'t, C::Item>;

	/// Unlike its [`Buffering`]/[`Folding`] siblings this is a frame cell of its own, so it takes a
	/// name of its own — and `exec_viz` finds the buffer serving a dep by matching `Buffer<C, `,
	/// which only lines up while both spell `C` the way the rest of the graph does.
	const NAME: &'static str = Self::TAG.as_str();
	const REACH: Horizon = H;
}

impl<C: Series, const H: Horizon> Node for Buffer<C, H>
where
	C::Item: Stamped,
{
	type Deps = (Folding<C, H>,);

	fn advance<'t>(&'t mut self, (fresh,): DepOuts<'t, Self>) -> Self::Out<'t> {
		const {
			assert!(
				match H {
					Horizon::Elems(k) => k >= 1,
					Horizon::Span(tf) => tf.0 > 0,
					_ => false,
				},
				"a buffer retains a bounded reach: Horizon::Elems(k >= 1) or Horizon::Span(tf > 0)"
			)
		}
		// Trim *before* the append: `past` must be what stood behind this tick's batch, or an
		// intra-batch cursor walking several elements reads a window already trimmed by its own tail.
		let drop = match H {
			Horizon::Elems(k) => self.buf.len().saturating_sub(k),
			Horizon::Span(tf) => match self.buf.last() {
				Some(newest) => {
					let cut = newest.ts_ns() - Horizon::ns(tf);
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

	/// Forwarded, for the same reason [`Folding`]'s is: the graph predicates match dep names against
	/// frame cell names, and a wrapper that renamed its dep would drop out of every one of them.
	/// `REACH`/`FOLDED` are what then say it is the retention and not the cell itself being asked for.
	const CLOCK: Option<Timeframe> = C::CLOCK;
	const NAME: &'static str = C::NAME;
	const REACH: Horizon = H;
	const RETAINED: bool = true;
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

/// Engine-owned point-level over a [`Series`] — [`Buffer`]'s sibling, an ordinary node (ungated,
/// `Deps = (Folding<C, {Horizon::Unbounded}>,)`) sitting *next to* its source in the frame. Unbounded
/// because a level it never saw is one it can never stand on, and it retains nothing: one item, not
/// a window.
///
/// The invariant is monotone — once it holds a value it holds one forever. That is what a consumer
/// clocked by *another* series needs: on its own ticks this one has emitted nothing, and reading the
/// empty run there would read absence where a standing level is the truth.
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
	type Deps = (Folding<C, { Horizon::Unbounded }>,);

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
pub struct Folding<C: Cell, const H: Horizon>(core::marker::PhantomData<C>);

impl<C: Cell, const H: Horizon> Cell for Folding<C, H> {
	type Out<'t> = C::Out<'t>;

	const CLOCK: Option<Timeframe> = C::CLOCK;
	const FOLDED: bool = true;
	/// Forwarded: `Roots::required_events` matches dep names against frame cell names, and a wrapper
	/// that renamed its dep would drop out of it.
	const NAME: &'static str = C::NAME;
	const REACH: Horizon = H;
}

impl<'t, C: Cell, const H: Horizon, T> Has<'t, Folding<C, H>, Here> for Cons<'t, C, T> {
	fn get(&self) -> C::Out<'t> {
		const { assert!(!matches!(H, Horizon::Unit), "a Folding at Unit is the bare dep C — drop the wrapper") }
		self.out
	}
}

impl<C: Nudge, const H: Horizon> Nudge for Folding<C, H> {
	type Scratch = C::Scratch;

	fn stage<'t>(out: C::Out<'t>, s: &mut Self::Scratch, bump: Option<usize>, h: f64) -> f64 {
		C::stage(out, s, bump, h)
	}

	fn view<'l>(s: &'l Self::Scratch) -> C::Out<'l> {
		C::view(s)
	}
}

/// [`Folding`] with the span spelled as the period it is — what a node parameterised by a timeframe
/// writes in dep position. Its own type because `Folding<C, { Horizon::Span(TF) }>` does not parse:
/// an enum constructor applied to a generic parameter is rejected in const-argument position, so the
/// construction moves into an associated const, which is a type.
pub struct Spanning<C: Cell, const TF: Timeframe>(core::marker::PhantomData<C>);

impl<C: Cell, const TF: Timeframe> Cell for Spanning<C, TF> {
	type Out<'t> = C::Out<'t>;

	const CLOCK: Option<Timeframe> = C::CLOCK;
	const FOLDED: bool = true;
	const NAME: &'static str = C::NAME;
	const REACH: Horizon = Horizon::Span(TF);
}

impl<'t, C: Cell, const TF: Timeframe, T> Has<'t, Spanning<C, TF>, Here> for Cons<'t, C, T> {
	fn get(&self) -> C::Out<'t> {
		self.out
	}
}

impl<C: Nudge, const TF: Timeframe> Nudge for Spanning<C, TF> {
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
}

/// The head of a type list, for [`DepSet::Lead`] — only the leading dep can gate.
macro_rules! head {
	($A:ty $(, $T:ty)*) => {
		$A
	};
}

macro_rules! impl_arity {
	// every arity at once: impl the whole list, then recurse on its tail. Names are macro-local, so a
	// suffix of the alphabet is as good a type list as a prefix.
	(@all) => {};
	(@all $Th:ident $Ih:ident $vh:ident $sh:ident $(, $T:ident $I:ident $v:ident $s:ident)*) => {
		impl_arity!($Th $Ih $vh $sh $(, $T $I $v $s)*);
		impl_arity!(@all $($T $I $v $s),*);
	};
	($($T:ident $I:ident $v:ident $s:ident),+) => {
		impl<$($T: Cell),+> DepSet for ($($T,)+) {
			type Outs<'t> = ($($T::Out<'t>,)+);
			type Lead = <head!($($T),+) as Cell>::Gates;

			const CLOCKS: &'static [Option<Timeframe>] = &[$($T::CLOCK),+];
			const FOLDS: &'static [bool] = &[$($T::FOLDED),+];
			const GATES: &'static [bool] = &[$(<$T::Gates as Bit>::VALUE),+];
			const NAMES: &'static [&'static str] = &[$($T::NAME),+];
			const REACH: &'static [Horizon] = &[$($T::REACH),+];
			const RETAINS: &'static [bool] = &[$($T::RETAINED),+];
		}
		impl<'t, F, $($T: Cell, $I),+> Pull<'t, F, ($($I,)+)> for ($($T,)+)
		where F: $(Has<'t, $T, $I> +)+ {
			fn pull(f: &F) -> Self::Outs<'t> where F: 't {
				($(Has::<'t, $T, $I>::get(f),)+)
			}

			fn open(f: &F) -> bool where F: 't {
				const {
					assert!(
						gating_leads(<Self as DepSet>::GATES),
						"a `Gating` dep precedes the plain ones: a closed gate must pull nothing, and `open` reads the tuple left to right"
					);
					assert!(
						!any(<Self as DepSet>::GATES) || !any(<Self as DepSet>::FOLDS),
						"a gated node cannot hold its own reach: a closed gate pulls no deps, so a `Folding` dep never re-warms — retain it in the frame instead (write the dep as `Buffering<C, H>`), or drop the `Gating` dep"
					);
				}
				true $(&& (!<$T::Gates as Bit>::VALUE || $T::opens(Has::<'t, $T, $I>::get(f))))+
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
pub fn step<'t, N, F, I>(frame: F, node: &'t mut N) -> Cons<'t, N, F>
where
	N: Node,
	N::Deps: Pull<'t, F, I>,
	N::Out<'t>: Dark<<N::Deps as DepSet>::Lead>,
	F: 't, {
	let out = match <N::Deps as Pull<'t, F, I>>::open(&frame) {
		true => node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame)),
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
pub fn step_emit<'t, E, F, I>(frame: F, e: &'t mut Emitter<E>, demanded: bool, ts: i64) -> Cons<'t, E, F>
where
	E: Emit,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Deps: Pull<'t, F, I>,
	F: 't, {
	e.buf.clear();
	if demanded && <E::Deps as Pull<'t, F, I>>::open(&frame) && e.opens(ts) {
		e.node.emit(<E::Deps as Pull<'t, F, I>>::pull(&frame), &mut e.buf);
	}
	Cons { out: &e.buf, tail: frame }
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

impl<'a> Fire<'a> {
	/// Everything a stepped node reads off its out and its [`Flats`]. The four [`Diff`] fields are
	/// the exception rather than the shape, spliced in with `..` at the one site that states them.
	/// `flat: None` is the unfired reading — no flattening happened, so there is none to report.
	#[inline]
	fn of<T: Flat + core::fmt::Debug + Glance>(out: &'a T, plots: &'static [Plot], dep_dims: &'a [&'static [usize]], flat: Option<&'a Flats>) -> Self {
		Fire {
			debug: out,
			glance: out,
			dims: T::DIMS,
			plots,
			fires: out.fires(),
			vals: flat.and_then(|f| f.fired.then_some(f.vals.as_slice())),
			dep_dims,
			jac: flat.and_then(|f| f.jac.as_deref()),
			exact_jac: None,
			formula: None,
			deriv: None,
			trace: None,
		}
	}
}

/// The un-bumped flattenings of one node's out and deps, plus the finite difference taken off them
/// — the observed leg every stepped node shares, whatever kind of node it is.
struct Flats {
	fired: bool,
	vals: alloc::vec::Vec<f64>,
	deps: alloc::vec::Vec<f64>,
	jac: Option<alloc::vec::Vec<f64>>,
}

impl Flats {
	/// `fd` is the node's Jacobian over `(dep_buf, out_buf)`; `None` is an observer that reads no
	/// further than [`Want::Vals`], and it is asked for only where there is an out to differentiate.
	#[inline]
	fn of<'d, O: Flat, D: DepFlat>(out: &O, deps: &D::Outs<'d>, fd: Option<impl FnOnce(&[f64], &[f64]) -> alloc::vec::Vec<f64>>) -> Self {
		let mut vals = alloc::vec![f64::NAN; O::LEN];
		let fired = out.flat(&mut vals);
		let mut dep_buf = alloc::vec![f64::NAN; D::LEN];
		D::flat(deps, &mut dep_buf);
		let jac = fd.filter(|_| fired).map(|fd| fd(&dep_buf, &vals));
		Flats { fired, vals, deps: dep_buf, jac }
	}
}

/// How much of a fire the observer reads. Everything above [`Want::Vals`] is second order — the
/// Jacobian is one `clone_from` plus one re-`advance` per scalar dep slot, and a [`Diff`] node's
/// exact partials need the pre-advance node too — so it is priced per tick, never per build.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Want {
	Nothing,
	Vals,
	Jac,
}

/// Sees every [`step_obs`] as it happens: one interpretation choke point, many interpretations.
/// Step order IS topo order, so the observed sequence doubles as the graph's static topology; dep
/// names never seen as stepped nodes are roots — apps seed root activations via [`observe_root`].
pub trait Observer {
	/// Asked once per step. No default: an observer that has not said what it reads is one nobody
	/// priced, and the answer costs the graph a re-advance per dep slot.
	fn want(&self) -> Want;
	/// `gates` is [`DepSet::GATES`]: positional with `deps`, marking the ones that are control edges
	/// rather than data. All-`false` for ungated nodes, empty for roots.
	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>);
}

impl Observer for () {
	fn want(&self) -> Want {
		Want::Nothing
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], _: Fire<'_>) {}
}

/// So a long-lived observer can be composed into a per-tick pair without being moved into it.
impl<O: Observer + ?Sized> Observer for &mut O {
	fn want(&self) -> Want {
		(**self).want()
	}

	fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>) {
		(**self).on(node, deps, gates, fire);
	}
}

/// Two interpretations of the same sweep — e.g. an app's own assertions next to a viz recorder.
impl<A: Observer, B: Observer> Observer for (A, B) {
	fn want(&self) -> Want {
		self.0.want().max(self.1.want())
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
fn fd_col<'d, N>(pre: &N, clone: &mut N, deps: DepOuts<'d, N>, scratch: &mut <N::Deps as DepFlat>::Scratch, slot: usize, h: f64, bumped: &mut [f64]) -> (bool, f64)
where
	N: Node + Clone,
	N::Deps: DepFlat,
	DepOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let dh = <N::Deps as DepFlat>::stage(deps, scratch, slot, h);
	clone.clone_from(pre);
	(clone.advance(<N::Deps as DepFlat>::view(scratch)).flat(bumped), dh)
}

/// The full finite-difference Jacobian, one column per dep element: `col` re-steps the node with
/// that element bumped by about `h`, writing the bumped out into `bumped` and returning whether it
/// fired and the perturbation the dep actually applied. NaN columns where a dep is unfired or the
/// bump crossed a firing branch. `out_buf`/`dep_buf` are the un-bumped flattenings.
fn fd_cols(dep_buf: &[f64], out_buf: &[f64], mut col: impl FnMut(usize, f64, &mut [f64]) -> (bool, f64)) -> alloc::vec::Vec<f64> {
	let (out_len, dep_len) = (out_buf.len(), dep_buf.len());
	let mut jac = alloc::vec![f64::NAN; out_len * dep_len];
	let mut bumped = alloc::vec![f64::NAN; out_len];
	for slot in 0..dep_len {
		let x = dep_buf[slot];
		if x.is_nan() {
			continue;
		}
		// `dh`, not `h`: a quantized dep moves in whole ticks, and dividing by a step it never took
		// is a fabricated slope. `0.0` ⇒ the slot has no derivative at all.
		let (fired, dh) = col(slot, (x.abs() * 1e-6).max(1e-9), &mut bumped);
		if !fired || dh == 0.0 {
			continue; // bump crossed a firing branch, or the slot is discrete — column stays NaN
		}
		for i in 0..out_len {
			jac[i * dep_len + slot] = (bumped[i] - out_buf[i]) / dh;
		}
	}
	jac
}

/// [`fd_cols`] over a level node's re-[`advance`](Node::advance), via [`fd_col`].
fn fd_jac<'d, N>(pre: &N, deps: DepOuts<'d, N>, dep_buf: &[f64], out_buf: &[f64]) -> alloc::vec::Vec<f64>
where
	N: Node + Clone,
	N::Deps: DepFlat,
	DepOuts<'d, N>: Copy,
	for<'x> N::Out<'x>: Flat, {
	let mut scratch = <N::Deps as DepFlat>::Scratch::default();
	let mut clone = pre.clone();
	fd_cols(dep_buf, out_buf, |slot, h, bumped| fd_col::<N>(pre, &mut clone, deps, &mut scratch, slot, h, bumped))
}

/// [`step`] + [`Observer::on`] before the push. The `()` observer erases to exactly `step`.
///
/// Under an active observer, each fired node's Jacobian is finite-differenced: clone the
/// pre-advance node, [`Nudge`] the *last* element of one dep (batch deps copied into scratch),
/// re-advance the clone at a shorter lifetime, diff the last out elements.
pub fn step_obs<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance + Dark<<N::Deps as DepSet>::Lead>,
	F: 't, {
	let run = <N::Deps as Pull<'t, F, I>>::open(&frame);
	step_seen(frame, node, run, <N::Out<'t> as Dark<<N::Deps as DepSet>::Lead>>::dark, obs)
}

/// [`step_obs`] for a node the graph derived no standing demand for: it advances only while every
/// gate dominating its readers is open *and* its own gates are. The bound is [`Latent`] rather than
/// [`Dark`] because an undemanded node is dark whatever its own `Deps` say — which is precisely the
/// obligation `graph!` puts on a node it suppresses.
#[doc(hidden)]
pub fn step_when_obs<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, demanded: bool, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance + Latent,
	F: 't, {
	let run = demanded && <N::Deps as Pull<'t, F, I>>::open(&frame);
	step_seen(frame, node, run, <N::Out<'t> as Latent>::latent, obs)
}

/// The observed sweep of one level node, given whether it runs at all and what it reads if it does
/// not. `unrun` is a thunk rather than a value because [`Dark<No>::dark`](Dark) is unreachable by
/// construction — an ungated node has no dark branch to evaluate.
fn step_seen<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, run: bool, unrun: impl FnOnce() -> N::Out<'t>, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance,
	F: 't, {
	const {
		assert!(
			Plot::coherent(N::PLOTS),
			"a multi-plot node must name each plot's slots (`[]` claims all of them); a candle plot must name four and be an overlay"
		)
	}

	let want = obs.want();

	// gate closed or nobody reading: no advance, no dep flatten, no FD — an unfired `Fire` is the
	// honest view.
	if !run {
		let out: N::Out<'t> = unrun();
		if want != Want::Nothing {
			let fire = Fire::of(&out, N::PLOTS, <N::Deps as DepFlat>::DIMS, None);
			obs.on(N::NAME, <N::Deps as DepSet>::NAMES, <N::Deps as DepSet>::GATES, fire);
		}
		return Cons { out, tail: frame };
	}

	if want == Want::Nothing {
		let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));
		return Cons { out, tail: frame };
	}

	let pre = (want == Want::Jac).then(|| node.clone());
	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	let out = node.advance(deps);
	let flat = Flats::of::<_, N::Deps>(
		&out,
		&deps,
		pre.as_ref().map(|pre| move |dep_buf: &[f64], out_buf: &[f64]| fd_jac::<N>(pre, deps, dep_buf, out_buf)),
	);

	let fire = Fire::of(&out, N::PLOTS, <N::Deps as DepFlat>::DIMS, Some(&flat));
	obs.on(N::NAME, <N::Deps as DepSet>::NAMES, <N::Deps as DepSet>::GATES, fire);
	Cons { out, tail: frame }
}

/// [`fd_cols`] over an [`Emit`]'s re-`emit`. It stays separate from [`fd_jac`] because the two
/// re-step through different traits — merging them would cost a vtable, which is the one thing this
/// leg cannot pay. The re-`emit` needs no [`fd_col`]-style lifetime isolation (`&mut self` never
/// lends the node), so its column body is inline; everything a column overwrites wholly — the deps'
/// scratch, the clone, its output run — is hoisted across the loop.
fn fd_jac_emit<'d, E>(pre: &E, deps: EmitOuts<'d, E>, dep_buf: &[f64], out_buf: &[f64]) -> alloc::vec::Vec<f64>
where
	E: Emit + Clone,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Flat,
	E::Deps: DepFlat,
	EmitOuts<'d, E>: Copy, {
	let mut scratch = <E::Deps as DepFlat>::Scratch::default();
	let mut emitted = alloc::vec::Vec::new();
	let mut clone = pre.clone();
	fd_cols(dep_buf, out_buf, |slot, h, bumped| {
		let dh = <E::Deps as DepFlat>::stage(deps, &mut scratch, slot, h);
		emitted.clear();
		clone.clone_from(pre);
		clone.emit(<E::Deps as DepFlat>::view(&scratch), &mut emitted);
		(emitted.as_slice().flat(bumped), dh)
	})
}

/// [`step_emit`] + [`Observer::on`] before the push — [`step_obs`]'s sibling, with the engine's
/// buffer standing in for the node's out. A gate-closed, undemanded or off-clock emit node is simply
/// the empty run, which is why this one needs no [`Latent`] sibling the way [`step_when_obs`] is one.
pub fn step_emit_obs<'t, E, F, I, O: Observer>(frame: F, e: &'t mut Emitter<E>, demanded: bool, ts: i64, obs: &mut O) -> Cons<'t, E, F>
where
	E: Emit + Clone,
	for<'x> E: Cell<Out<'x> = &'x [<E as Series>::Item]>,
	E::Item: Flat + core::fmt::Debug + Glance,
	E::Deps: Pull<'t, F, I> + DepFlat,
	EmitOuts<'t, E>: Copy,
	F: 't, {
	const {
		assert!(
			Plot::coherent(<E as Emit>::PLOTS),
			"a multi-plot node must name each plot's slots (`[]` claims all of them); a candle plot must name four and be an overlay"
		)
	}
	e.buf.clear();
	let want = obs.want();

	// gate closed, nobody reading, or the period still running: no emit, no dep flatten, no FD — the
	// empty run is the honest view.
	if !demanded || !<E::Deps as Pull<'t, F, I>>::open(&frame) || !e.opens(ts) {
		let out: &'t [E::Item] = &e.buf;
		if want != Want::Nothing {
			let fire = Fire::of(&out, <E as Emit>::PLOTS, <E::Deps as DepFlat>::DIMS, None);
			obs.on(E::NAME, <E::Deps as DepSet>::NAMES, <E::Deps as DepSet>::GATES, fire);
		}
		return Cons { out, tail: frame };
	}

	if want == Want::Nothing {
		e.node.emit(<E::Deps as Pull<'t, F, I>>::pull(&frame), &mut e.buf);
		return Cons { out: &e.buf, tail: frame };
	}

	let pre = (want == Want::Jac).then(|| e.node.clone());
	let deps = <E::Deps as Pull<'t, F, I>>::pull(&frame);
	e.node.emit(deps, &mut e.buf);
	let out: &'t [E::Item] = &e.buf;
	let flat = Flats::of::<_, E::Deps>(
		&out,
		&deps,
		pre.as_ref().map(|pre| move |dep_buf: &[f64], out_buf: &[f64]| fd_jac_emit::<E>(pre, deps, dep_buf, out_buf)),
	);

	let fire = Fire::of(&out, <E as Emit>::PLOTS, <E::Deps as DepFlat>::DIMS, Some(&flat));
	obs.on(E::NAME, <E::Deps as DepSet>::NAMES, <E::Deps as DepSet>::GATES, fire);
	Cons { out, tail: frame }
}

/// [`step_obs`]'s sibling for a [`Diff`] node: the same advance + FD momentary Jacobian, plus the
/// *exact* partials, the equation formula, and its simplified per-dep derivatives — the graph's
/// "differentiate + document themselves" reading. The `graph!` `diff { }` group routes fields here.
pub fn step_exact<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Diff + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance,
	F: 't, {
	const {
		assert!(
			!any(<N::Deps as DepSet>::GATES),
			"a `diff` node is ungated: its exact partials are stated over deps it pulls every tick"
		)
	}
	let want = obs.want();
	if want == Want::Nothing {
		let out = node.advance(<N::Deps as Pull<'t, F, I>>::pull(&frame));
		return Cons { out, tail: frame };
	}

	let pre = (want == Want::Jac).then(|| node.clone());
	let deps = <N::Deps as Pull<'t, F, I>>::pull(&frame);
	let out = node.advance(deps);
	let flat = Flats::of::<_, N::Deps>(
		&out,
		&deps,
		pre.as_ref().map(|pre| move |dep_buf: &[f64], out_buf: &[f64]| fd_jac::<N>(pre, deps, dep_buf, out_buf)),
	);

	// the exact partials read off the *pre*-advance node, so they belong to the same want as the FD:
	// asking for them under `Vals` would reintroduce the clone the level this exists to skip.
	let exacts = pre.map(|pre| {
		let dep_len = <N::Deps as DepFlat>::LEN;
		// zeroed, not NaN-filled: `grad` accumulates (`+=`) into it, and an absent var's partial is 0.
		let mut exact = alloc::vec![0.0f64; <N::Out<'t> as Flat>::LEN * dep_len];
		pre.exact_jac(deps, &mut exact);
		let formula = pre.formula();
		let deriv = Derivs {
			names: <N::Deps as DepSet>::NAMES,
			parts: (0..dep_len).map(|i| formula.diff(i).simplify()).collect(),
		};
		let trace = formula.trace(&flat.deps);
		(exact, formula, deriv, trace)
	});

	let base = Fire::of(&out, N::PLOTS, <N::Deps as DepFlat>::DIMS, Some(&flat));
	let fire = match &exacts {
		Some((exact, formula, deriv, trace)) => Fire {
			exact_jac: Some(exact),
			formula: Some(formula),
			deriv: Some(deriv),
			trace: Some(trace),
			..base
		},
		None => base,
	};
	obs.on(N::NAME, <N::Deps as DepSet>::NAMES, <N::Deps as DepSet>::GATES, fire);
	Cons { out, tail: frame }
}

/// [`step_exact`] for a node the graph derived no standing demand for — [`step_when_obs`]'s
/// [`Diff`] sibling. The exact partials have nothing to state about a tick the node did not take,
/// so an undemanded one reads exactly as a suppressed level node does.
#[doc(hidden)]
pub fn step_exact_when<'t, N, F, I, O: Observer>(frame: F, node: &'t mut N, demanded: bool, obs: &mut O) -> Cons<'t, N, F>
where
	N: Node + Diff + Clone,
	N::Deps: Pull<'t, F, I> + DepFlat,
	DepOuts<'t, N>: Copy,
	for<'x> N::Out<'x>: Flat,
	N::Out<'t>: core::fmt::Debug + Glance + Latent,
	F: 't, {
	match demanded {
		true => step_exact(frame, node, obs),
		false => step_seen(frame, node, false, <N::Out<'t> as Latent>::latent, obs),
	}
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
pub fn observe_root<'t, C, O>(out: C::Out<'t>, obs: &mut O)
where
	C: Cell,
	C::Out<'t>: Flat + core::fmt::Debug + Glance,
	O: Observer, {
	if obs.want() == Want::Nothing {
		return;
	}
	let mut vals = alloc::vec![f64::NAN; <C::Out<'t> as Flat>::LEN];
	// a root is pulled from nothing, so there are no deps to flatten and nothing to differentiate.
	let fired = out.flat(&mut vals);
	let flat = Flats {
		fired,
		vals,
		deps: alloc::vec::Vec::new(),
		jac: None,
	};
	obs.on(C::NAME, &[], &[], Fire::of(&out, &[Plot::DEFAULT], &[], Some(&flat)));
}
