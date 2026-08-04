//! Every timestamp is a pair: **who read the clock**, and **what they were doing when they read
//! it**. The actor is the type parameter, the action is the field name.
//!
//! Three actions — `exec` (this actor is the origin of the fact), `send` (put it on the wire),
//! `recv` (took it off the wire). There is no `init`: it was a positional alias for "the local
//! anchor", which is `recv` for inbound data and `exec` for anything we originate, and it has no
//! meaning of its own.

use std::{
	fmt,
	hash::{Hash, Hasher},
	marker::PhantomData,
	ops::{Add, Sub},
};

use derive_more::{Deref, DerefMut};

/// Our own process.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Local;
/// The exchange a fact originates from.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Venue;
/// A party that relays facts it did not originate.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Vendor;

/// Nanoseconds since the Unix epoch, tagged with the actor whose clock produced it.
///
/// Wraps `i64` rather than [`jiff::Timestamp`] (16 bytes) so one type serves both the domain layer
/// and the row layer, where per-row width drives rotation sizing.
pub struct Ts<A>(i64, PhantomData<A>);

// Hand-written, not derived: `#[derive]` on a `PhantomData<A>` struct emits spurious `A: Trait`
// bounds that propagate into every `#[derive(Default)]` on a struct holding one.
impl<A> Copy for Ts<A> {}
impl<A> Clone for Ts<A> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<A> Default for Ts<A> {
	fn default() -> Self {
		Self(0, PhantomData)
	}
}
impl<A> PartialEq for Ts<A> {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}
impl<A> Eq for Ts<A> {}
impl<A> PartialOrd for Ts<A> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl<A> Ord for Ts<A> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.0.cmp(&other.0)
	}
}
impl<A> Hash for Ts<A> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}
impl<A> fmt::Debug for Ts<A> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match jiff::Timestamp::from_nanosecond(self.0 as i128) {
			Ok(t) => write!(f, "{t}"),
			Err(_) => write!(f, "Ts({})", self.0),
		}
	}
}

impl<A> Ts<A> {
	pub const MAX: Self = Self(i64::MAX, PhantomData);
	pub const MIN: Self = Self(i64::MIN, PhantomData);

	pub const fn from_nanos(nanos: i64) -> Self {
		Self(nanos, PhantomData)
	}

	pub const fn as_nanos(self) -> i64 {
		self.0
	}

	pub fn to_jiff(self) -> jiff::Timestamp {
		jiff::Timestamp::from_nanosecond(self.0 as i128).expect("i64 nanos are always in jiff's range")
	}

	/// Round down to a multiple of `step`, measured from the epoch.
	pub fn floor(self, step: Exact) -> Self {
		assert!(step.0 > 0, "floor step must be positive, got {}", step.0);
		Self(self.0 - self.0.rem_euclid(step.0), PhantomData)
	}
}

impl<A> From<jiff::Timestamp> for Ts<A> {
	fn from(t: jiff::Timestamp) -> Self {
		Self(t.as_nanosecond() as i64, PhantomData)
	}
}

/// Weaver ordering key. Monotonic **by construction**, not by measurement: when events arrive
/// faster than the clock resolves, it is the previous key plus one. So it is deliberately not
/// subtractable — the gap between two arrivals is not an elapsed duration.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Arrival(i64);

impl Arrival {
	pub const MIN: Self = Self(i64::MIN);

	pub const fn from_nanos(nanos: i64) -> Self {
		Self(nanos)
	}

	pub const fn as_nanos(self) -> i64 {
		self.0
	}
}

/// The rate a feed is read at. Arrival time is cut into cells of this length, and one tick carries
/// at most one cell: the reader never *waits* for a cell to fill, so a cell that already holds data
/// is handed over as it stands rather than batched further.
///
/// Cells are absolute — floored from the epoch, not from the last emission — so which cell an event
/// falls in is a property of the event alone, identical across runs and independent of what the
/// feed did before it. Always set: the rate a graph is stepped at is a thing the caller states, not
/// one that falls out of which lane happened to tick.
///
/// A newtype rather than a bare [`Exact`] so that quantizing an [`Arrival`] is the *only*
/// arithmetic an arrival admits.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReadClock(Exact);

impl ReadClock {
	/// The feed's own rate: every distinct arrival is its own cell. The degenerate case, not a
	/// special path.
	pub const EVENT: Self = Self(Exact(0));

	pub const fn from(e: Exact) -> Self {
		Self(e)
	}

	/// The last arrival still inside `a`'s cell — the inclusive end of a tick that opens at `a`.
	/// Saturating, because [`Arrival::MIN`] is a real key (state carried in ahead of a range) and
	/// flooring it must not wrap.
	pub fn cell_end(self, a: Arrival) -> Arrival {
		let step = self.0.0;
		assert!(step >= 0, "a read clock cannot run backwards, got {step}ns");
		if step == 0 {
			return a;
		}
		Arrival(a.0.saturating_sub(a.0.rem_euclid(step)).saturating_add(step - 1))
	}
}

/// Same-actor difference: one clock read twice, so no skew term.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Exact(i64);

impl Exact {
	pub const fn from_nanos(nanos: i64) -> Self {
		Self(nanos)
	}

	pub const fn as_nanos(self) -> i64 {
		self.0
	}
}

impl From<std::time::Duration> for Exact {
	fn from(d: std::time::Duration) -> Self {
		Self(d.as_nanos() as i64)
	}
}

/// A cross-actor quantity: two clocks, so the true value is only known to lie in `[lo, hi]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Bounded {
	pub lo: i64,
	pub hi: i64,
}

// Cross-actor `Sub` is deliberately absent: subtracting a `Ts<Venue>` from a `Ts<Local>` yields a
// number contaminated by clock skew, and the type system is the only place that can say so.
impl<A> Sub for Ts<A> {
	type Output = Exact;

	fn sub(self, rhs: Self) -> Exact {
		Exact(self.0 - rhs.0)
	}
}

impl<A> Add<Exact> for Ts<A> {
	type Output = Self;

	fn add(self, rhs: Exact) -> Self {
		Self(self.0 + rhs.0, PhantomData)
	}
}

impl<A> Sub<Exact> for Ts<A> {
	type Output = Self;

	fn sub(self, rhs: Exact) -> Self {
		Self(self.0 - rhs.0, PhantomData)
	}
}

/// The window one actor's readings of a folded object span. `first` is the start of the **current
/// accumulation epoch** — it resets on resync, it is not "when the object was allocated".
pub struct Span<A> {
	pub first: Ts<A>,
	pub last: Ts<A>,
}

impl<A> Copy for Span<A> {}
impl<A> Clone for Span<A> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<A> Default for Span<A> {
	fn default() -> Self {
		Self {
			first: Ts::default(),
			last: Ts::default(),
		}
	}
}
impl<A> PartialEq for Span<A> {
	fn eq(&self, other: &Self) -> bool {
		self.first == other.first && self.last == other.last
	}
}
impl<A> Eq for Span<A> {}
impl<A> fmt::Debug for Span<A> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Span").field("first", &self.first).field("last", &self.last).finish()
	}
}

impl<A> Span<A> {
	pub fn new(first: Ts<A>, last: Ts<A>) -> Self {
		assert!(first <= last, "span runs backwards: {first:?} > {last:?}");
		Self { first, last }
	}

	/// A single reading is the degenerate span.
	pub fn at(t: Ts<A>) -> Self {
		Self { first: t, last: t }
	}
}

/// A fold over many wire messages: one span per actor, `first` resetting on every resync.
///
/// An aggregate has no `exec` of its own — *we* built it, the venue never performed it — so it is
/// not a [`Leg`] or a [`RoundTrip`] and carries none of that machinery. What it has is the window
/// of venue events that fed it and the window in which we took them off the wire.
///
/// `venue_exec` is the venue's reading of when the fact happened. Where a venue reports only when
/// it *sent* the frame, that send stands in: what we need is when the event occurred and when it
/// reached us, and the send-vs-exec difference is a debugging signal about the venue's internal
/// latency, not a third thing the model is built around.
///
/// Neither field is optional. Every message has a venue reading of some kind, and the adapter that
/// took it off the wire is itself the local actor — so it knows its own reception time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Aggregate {
	pub venue_exec: Span<Venue>,
	pub local_recv: Span<Local>,
}

/// Transmitted it, and *may* attest to being its origin. Covers a relaying vendor and a venue that
/// reports only an envelope time.
///
/// `recv` is absent for a static endpoint pulled on demand: the reading we'd record is when the
/// download landed, which says nothing about the fact and is not a wire arrival.
#[derive(Clone, Copy, Debug, Eq, PartialEq, derive_more::Deref)]
pub struct Relay<From, To> {
	/// `None` when the origin does not attest when it acted — a relaying vendor, or a run whose
	/// elements each attest their own.
	pub exec: Option<Ts<From>>,
	#[deref]
	pub send: Ts<From>,
	pub recv: Option<Ts<To>>,
}

/// [`Relay`]'s columnar twin: per element where the element attests, on the holder where the run
/// does. Both shapes stay — [`Relay`] for a single event, this for a run.
#[derive(Debug)]
pub struct RelayCols<'a, From, To> {
	pub exec: Option<&'a [Ts<From>]>,
	/// `None` when the venue reports no wire time distinct from `exec`. A per-venue property, so it
	/// is all-or-nothing across a run — asserted on decode rather than mixed per element.
	pub send: Option<&'a [Ts<From>]>,
	/// The run's reception window; `None` for historic ingest — we were not there. A [`Span`], not a
	/// stamp: a merged run covers several venue messages, each stamped once on arrival.
	pub recv: Option<Span<To>>,
}

// Hand-written for the same reason [`Ts`]'s are: derives would demand `From: Copy, To: Copy`.
impl<From, To> Copy for RelayCols<'_, From, To> {}
impl<From, To> Clone for RelayCols<'_, From, To> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<From, To> PartialEq for RelayCols<'_, From, To> {
	fn eq(&self, other: &Self) -> bool {
		self.exec == other.exec && self.send == other.send && self.recv == other.recv
	}
}
impl<From, To> Eq for RelayCols<'_, From, To> {}

/// The origin acts, then sends; the destination receives. [`Relay`] plus an attested origin.
#[derive(Clone, Copy, Debug, Eq, PartialEq, derive_more::Deref)]
pub struct Leg<From, To> {
	#[deref]
	pub exec: Ts<From>,
	pub send: Ts<From>,
	pub recv: Ts<To>,
}

pub type External = Leg<Venue, Local>;
pub type Outbound = Leg<Local, Venue>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoundTrip {
	pub out: Outbound,
	pub back: External,
}

/// The weaker bound: it crossed a wire. Generic code bounds on this and accepts either shape; code
/// that needs origin attestation takes [`Leg`] concretely and a [`Relay`] will not compile.
pub trait Wire<F, T> {
	fn send(&self) -> Ts<F>;
	fn recv(&self) -> Option<Ts<T>>;
}

impl<F, T> Wire<F, T> for Relay<F, T> {
	fn send(&self) -> Ts<F> {
		self.send
	}

	fn recv(&self) -> Option<Ts<T>> {
		self.recv
	}
}

impl<F, T> Wire<F, T> for Leg<F, T> {
	fn send(&self) -> Ts<F> {
		self.send
	}

	fn recv(&self) -> Option<Ts<T>> {
		Some(self.recv)
	}
}

/// Boundary view for serialisation, logging and inspection. Objects keep their concrete shape;
/// this is what they flatten to when something generic has to look at them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Timestamps {
	Relay(Relay<Venue, Local>),
	External(External),
	/// A fold over many wire messages. `local` is `None` for historic ingest — we were not there.
	Accumulator {
		venue: Span<Venue>,
		local: Option<Span<Local>>,
	},
	RoundTrip(RoundTrip),
	/// We are the origin and nothing crossed a wire.
	Origin(Ts<Local>),
	// Simplified accounting: for large Accumulator primitives, which are too long to care for minute deltas, - so we just persist the last origin action on it. Example: Klines.
	Simple(Ts<Venue>),
}

pub trait Timestamped {
	fn ts(&self) -> Timestamps;
}

/// How far a remote clock runs ahead of ours, and how well we know it.
pub struct Offset<A> {
	est: Exact,
	uncertainty: Exact,
	_actor: PhantomData<A>,
}

impl<A> Copy for Offset<A> {}
impl<A> Clone for Offset<A> {
	fn clone(&self) -> Self {
		*self
	}
}
impl<A> fmt::Debug for Offset<A> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Offset({}ns ±{}ns)", self.est.0, self.uncertainty.0)
	}
}

/// NTP's estimator. The two `recv - send` terms straddle actors, which is exactly why this is the
/// one place that touches raw nanos and the only thing that can mint an [`Offset`].
pub fn offset(rt: &RoundTrip) -> Offset<Venue> {
	let (t1, t2) = (rt.out.send.0, rt.out.recv.0);
	let (t3, t4) = (rt.back.send.0, rt.back.recv.0);
	assert!(t4 >= t1, "round trip ends before it starts");
	let delay = (t4 - t1) - (t3 - t2);
	Offset {
		est: Exact(((t2 - t1) + (t3 - t4)) / 2),
		uncertainty: Exact(delay / 2),
		_actor: PhantomData,
	}
}

/// Project a venue reading onto our clock. The result is [`Bounded`], never a `Ts<Local>`: the
/// skew is estimated, and collapsing the interval would re-hide exactly what the estimate cost.
pub fn to_local(ts: Ts<Venue>, o: Offset<Venue>) -> Bounded {
	let centre = ts.0 - o.est.0;
	Bounded {
		lo: centre - o.uncertainty.0,
		hi: centre + o.uncertainty.0,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The pre-range anchor is keyed at [`Arrival::MIN`], so every replay that folds a book quantizes
	/// it on its first tick.
	#[test]
	fn cells_are_absolute_and_survive_the_extremes() {
		let second = ReadClock::from(Exact::from_nanos(1_000_000_000));
		let a = Arrival::from_nanos(1_500_000_000);
		assert_eq!(second.cell_end(a), Arrival::from_nanos(1_999_999_999));
		assert_eq!(second.cell_end(Arrival::from_nanos(1_999_999_999)), Arrival::from_nanos(1_999_999_999));
		assert!(second.cell_end(Arrival::MIN) < Arrival::from_nanos(0));
		assert_eq!(ReadClock::EVENT.cell_end(a), a, "an event-rate clock groups nothing");
	}

	#[test]
	fn floor_buckets_below_and_is_idempotent() {
		let minute = Exact::from_nanos(60_000_000_000);
		let t = Ts::<Venue>::from_nanos(125_000_000_000);
		let f = t.floor(minute);
		assert_eq!(f.as_nanos(), 120_000_000_000);
		assert_eq!(f.floor(minute), f);
		assert!(f <= t && t - f < minute);
	}

	#[test]
	fn offset_recovers_a_known_skew() {
		// venue clock runs 5s ahead; 1s each way on the wire.
		let skew = 5_000_000_000;
		let rt = RoundTrip {
			out: Leg {
				exec: Ts::from_nanos(0),
				send: Ts::from_nanos(0),
				recv: Ts::from_nanos(1_000_000_000 + skew),
			},
			back: Leg {
				exec: Ts::from_nanos(1_000_000_000 + skew),
				send: Ts::from_nanos(1_000_000_000 + skew),
				recv: Ts::from_nanos(2_000_000_000),
			},
		};
		let o = offset(&rt);
		let b = to_local(Ts::<Venue>::from_nanos(skew), o);
		assert!(b.lo <= 0 && 0 <= b.hi, "true local time 0 must lie in {b:?}");
	}
}
