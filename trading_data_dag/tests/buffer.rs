//! `Buffer`/`Buffering` through `graph!`: the past/fresh split across a multi-element batch (the
//! invariant intra-batch cursors rest on), `Flat` reading `fresh` only, span retention against an
//! irregular stream, and a latch commutation that resets a consumer while leaving the buffer whole —
//! the point of engine-owned retention.

use trading_data_dag::{
	Buffer, Buffering, Bump, Cell, DepOuts, Emit, EmitOuts, Episode, Fire, Flat, Gate, Gating, Glance, Horizon, Latch, Node, Observer, Stamped, graph, node, slice_nudge,
};
use v_utils::{Timeframe, TimeframeDesignator};

/// A retained element is stamped: that is what a [`Horizon`] indexes by. One unit of `v` is one
/// second of `ts`, so a fixture's numbers double as its timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Tick {
	ts: i64,
	v: f64,
}
fn t(v: f64) -> Tick {
	Tick { ts: (v * 1e9) as i64, v }
}
/// The shape `Split` reports: past count in `ts`, fresh count in `v`.
fn split(past: i64, fresh: f64) -> Tick {
	Tick { ts: past, v: fresh }
}
impl Stamped for Tick {
	fn ts_ns(&self) -> i64 {
		self.ts
	}
}
impl Flat for Tick {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.v;
		true
	}
}
impl Bump for Tick {
	fn bump(mut self, _: usize, h: f64) -> (Self, f64) {
		self.v += h;
		(self, h)
	}
}
impl Glance for Tick {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.v)
	}
}

/// The root series: whatever the caller hands in this tick.
struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// Unlike its `Buffering`/`Folding` siblings a buffer is a frame cell of its own, so it carries a
/// name of its own — and a name is both a `graph!` manifest key and what a chart prints, so it is
/// composed from its source's and its reach's rather than rendered by the compiler, which would
/// spell the same series differently here than on every other card.
#[test]
fn a_buffer_names_itself_from_its_source_and_its_reach() {
	assert_eq!(Horizon::Unit.tag().as_str(), "Unit");
	assert_eq!(Horizon::Unbounded.tag().as_str(), "Unbounded");
	assert_eq!(Horizon::Elems(61).tag().as_str(), "Elems(61)");
	assert_eq!(Horizon::Span(Timeframe::from_naive(3, TimeframeDesignator::Minutes)).tag().as_str(), "Span(3m)");
	assert_eq!(<Buffer<Src, { Horizon::Elems(61) }> as Cell>::NAME, format!("Buffer<{}, Elems(61)>", Src::NAME));
}

/// Reads a 3-deep window per fresh element: rate-preserving, `None` while short.
#[derive(Clone, Default)]
struct Sum3;
impl Cell for Sum3 {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Sum3 {
	type Deps = (Buffering<Src, { Horizon::Elems(3) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.trailing().map(|w| w.map(|w| w.iter().map(|x| x.v).sum())));
	}
}
slice_nudge!(Sum3, Option<f64>);

/// Reports the past/fresh split it saw, so the test can assert on the *shape* of the dep out and
/// not merely on a derived number: one element per batch, `ts` the past count and `v` the fresh one.
#[derive(Clone, Default)]
struct Split;
impl Cell for Split {
	type Out<'t> = &'t [Tick];
}
#[node]
impl Emit for Split {
	type Deps = (Buffering<Src, { Horizon::Elems(3) }>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Tick>) {
		out.push(Tick {
			ts: hist.past().len() as i64,
			v: hist.fresh().len() as f64,
		});
	}
}
slice_nudge!(Split, Tick);

/// A cross-rate reader: the level standing at its own deadline, one element of slack behind the
/// batch. Declares a *shallower* reach than the frame buffers, which is the case `all()` has to
/// answer at the declaration rather than at whatever the frame happens to hold.
#[derive(Clone, Default)]
struct Level {
	all: Vec<Tick>,
}
impl Cell for Level {
	type Out<'t> = &'t [Tick];
}
#[node]
impl Node for Level {
	type Deps = (Buffering<Src, { Horizon::Elems(1) }>,);

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.all.clear();
		self.all.extend_from_slice(hist.all());
		&self.all
	}
}
slice_nudge!(Level, Tick);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	// the frame's retention is the join of every read of it, and `Elems(3)` is the deepest here.
	outputs { sum3: Sum3, split: Split, level: Level, hist: Buffer<Src, { Horizon::Elems(3) }> }
}

/// Two consumers of one series at different reaches. `all()` must answer at the *declared* one, or a
/// cross-rate reader's results are a function of how deep some unrelated node asked the frame to
/// buffer — and shrinking that one silently changes this one.
#[test]
fn all_answers_at_the_declared_reach_not_the_frame_s() {
	let mut g = G::default();
	let (three, one) = ([t(1.0), t(2.0), t(3.0)], [t(4.0)]);

	// nothing stands behind the first batch, so both readers see the same thing: `fresh` is always
	// wholly in reach, however shallow the declaration.
	let o = g.tick(0, Batches { src: &three });
	assert_eq!(o.hist.all(), &three);
	assert_eq!(o.level, &three);

	let o = g.tick(0, Batches { src: &one });
	assert_eq!(o.hist.all(), &[t(1.0), t(2.0), t(3.0), t(4.0)], "the frame retains 3 behind the batch");
	assert_eq!(o.level, &[t(3.0), t(4.0)], "one declared, one behind the batch");
}

#[test]
fn past_fresh_split_and_trailing() {
	let mut g = G::default();
	let (one, three, five, empty) = ([t(1.0)], [t(2.0), t(3.0), t(4.0)], [t(5.0)], []);

	// cold: one element, no window yet.
	let o = g.tick(0, Batches { src: &one });
	assert_eq!(o.sum3, &[None]);
	assert_eq!(o.hist.past(), &[] as &[Tick]);
	assert_eq!(o.hist.fresh(), &one);
	assert_eq!(o.split, &[split(0, 1.0)]);

	// a batch carrying several elements: `past` is what stood behind the *whole* batch, so the
	// per-element cursors each see their own trailing window.
	let o = g.tick(0, Batches { src: &three });
	assert_eq!(o.hist.past(), &one);
	assert_eq!(o.hist.fresh(), &three);
	assert_eq!(o.sum3, &[None, Some(6.0), Some(9.0)]);
	assert_eq!(o.split, &[split(1, 3.0)]);

	// trim happens before the append, and keeps 3 elements behind.
	let o = g.tick(0, Batches { src: &five });
	assert_eq!(o.hist.past(), &three);
	assert_eq!(o.sum3, &[Some(12.0)]);
	assert_eq!(o.split, &[split(3, 1.0)]);

	// an empty batch still advances the buffer, and a whole window survives it — that is what a
	// cross-rate reader of `all()` rests on.
	let o = g.tick(0, Batches { src: &empty });
	assert_eq!(o.hist.all(), &[t(3.0), t(4.0), t(5.0)]);
	assert_eq!(o.hist.fresh(), &[] as &[Tick]);
	assert_eq!(o.sum3, &[] as &[Option<f64>]);
	assert_eq!(o.split, &[split(3, 0.0)]);
}

/// `Flat`/`Glance` see `fresh` only — a buffer must be indistinguishable from the series it retains.
#[test]
fn flat_reads_fresh_only() {
	#[derive(Default)]
	struct Rec {
		hist: Option<(usize, Vec<f64>)>,
		src: Option<(usize, Vec<f64>)>,
	}
	impl Observer for Rec {
		fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
			let slot = if node.contains("Buffer") {
				&mut self.hist
			} else if node.ends_with("Src") {
				&mut self.src
			} else {
				return;
			};
			*slot = Some((fire.fires, fire.vals.map(<[f64]>::to_vec).unwrap_or_default()));
		}
	}

	let mut g = G::default();
	let (warm, observed) = ([t(1.0), t(2.0)], [t(7.0), t(8.0)]);
	g.tick(0, Batches { src: &warm });

	let mut rec = Rec::default();
	g.tick_obs(0, Batches { src: &observed }, &mut rec);
	assert_eq!(rec.hist, rec.src, "a buffer's Fire must match its source's");
	assert_eq!(rec.hist, Some((2, vec![8.0])));
}

/// Span retention over an irregular stream: what a window reaches back over is wall clock, so a gap
/// wider than the span leaves the current element alone in it rather than a stale one that would be
/// read as a whole span's worth of change.
mod span {
	use super::*;

	graph! {
		struct S;
		batches SBatches;
		roots { src: Src[f64] };
		out SOut;
		outputs { hist: Buffer<Src, { Horizon::Span(Timeframe::from_naive(10, TimeframeDesignator::Seconds)) }> }
	}

	#[test]
	fn window_is_wall_clock_and_declines_until_the_watermark_clears() {
		let mut s = S::default();
		let (start, on, after_gap) = ([t(0.0), t(3.0), t(6.0)], [t(12.0)], [t(40.0)]);

		// nothing before the first element seen is claimed, so a 10s window over a 6s-old run is
		// incomplete — where "have I been running long enough" would be a guess.
		let o = s.tick(0, SBatches { src: &start });
		assert_eq!(o.hist.trailing_at(2), None);

		// a whole span past the first element: the window begins strictly inside `t - 10s`.
		let o = s.tick(0, SBatches { src: &on });
		assert_eq!(o.hist.trailing_at(0), Some(&[t(3.0), t(6.0), t(12.0)] as &[Tick]));

		// a gap wider than the span: one element, not a window reaching 28s back.
		let o = s.tick(0, SBatches { src: &after_gap });
		assert_eq!(o.hist.past(), &[t(3.0), t(6.0), t(12.0)], "retention keys on the pre-batch newest");
		assert_eq!(o.hist.trailing_at(0), Some(&[t(40.0)] as &[Tick]));
	}
}

/// A latch commutation resets the gated consumer and leaves the buffer whole. Its own module: each
/// `graph!` owns a `__Pending` at module scope.
mod revive {
	use super::*;

	/// Arming is its own lane, so the series can keep flowing while the consumer is dark.
	struct Trig;
	impl Cell for Trig {
		type Out<'t> = &'t [f64];
	}
	slice_nudge!(Trig, f64);

	/// `(tick within the episode, elements the window stood at)` — the second is what makes a *cold*
	/// buffer observable from outside the graph.
	#[derive(Clone, Copy, Debug, PartialEq)]
	struct Phase(u32, usize);
	impl Episode for Phase {
		fn terminal(&self) -> bool {
			self.0 >= 2
		}
	}
	impl Flat for Phase {
		const DIMS: &'static [usize] = &[];

		fn flat(&self, out: &mut [f64]) -> bool {
			out[0] = self.0 as f64;
			true
		}
	}
	impl Bump for Phase {
		fn bump(self, _: usize, _: f64) -> (Self, f64) {
			(self, 0.0)
		}
	}
	impl Glance for Phase {
		fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
			write!(f, "{self:?}")
		}
	}

	#[derive(Clone, Default)]
	struct Live {
		armed: bool,
	}
	impl Cell for Live {
		type Out<'t> = bool;
	}
	#[node(latch)]
	impl Node for Live {
		type Deps = (Trig,);

		fn advance<'t>(&'t mut self, (trig,): DepOuts<'t, Self>) -> Self::Out<'t> {
			self.armed |= !trig.is_empty();
			self.armed
		}
	}
	impl Gate for Live {}
	impl Latch for Live {
		type Cut = Episodic;

		fn commutate(&mut self) {
			self.armed = false;
		}
	}

	/// The revivable consumer: `t` makes a reset observable, the window length it reports makes a
	/// *cold* buffer observable.
	#[derive(Clone, Default)]
	struct Episodic {
		t: u32,
	}
	impl Cell for Episodic {
		type Out<'t> = Option<Phase>;
	}
	#[node]
	impl Node for Episodic {
		type Deps = (Gating<Live>, Buffering<Src, { Horizon::Elems(3) }>);

		fn advance<'t>(&'t mut self, (live, hist): DepOuts<'t, Self>) -> Self::Out<'t> {
			assert!(live, "a gating dep reads true inside `advance`");
			self.t += 1;
			Some(Phase(self.t, hist.trailing_at(0).map_or(0, <[Tick]>::len)))
		}
	}

	graph! {
		struct L;
		batches LBatches;
		roots { src: Src[f64], trig: Trig[u32] };
		out LOut;
		outputs { episodic: Episodic, hist: Buffer<Src, { Horizon::Elems(3) }> }
	}

	const ARM: &[f64] = &[1.0];
	const IDLE: &[f64] = &[];

	#[test]
	fn latch_resets_consumer_not_buffer() {
		let mut l = L::default();
		let src: Vec<[Tick; 1]> = (1..=7).map(|i| [t(f64::from(i))]).collect();

		// dark: the consumer never advances, yet the buffer fills — that is the whole feature.
		for x in &src[..3] {
			let o = l.tick(0, LBatches { src: x, trig: IDLE });
			assert_eq!(o.episodic, None);
		}

		// armed: full window on its very first tick back, no re-warm.
		let o = l.tick(0, LBatches { src: &src[3], trig: ARM });
		assert_eq!(o.episodic, Some(Phase(1, 3)));
		assert_eq!(o.hist.all(), &[t(1.0), t(2.0), t(3.0), t(4.0)]);
		let o = l.tick(0, LBatches { src: &src[4], trig: IDLE });
		assert_eq!(o.episodic, Some(Phase(2, 3)));

		// commutated: consumer reset to Default, buffer untouched.
		let o = l.tick(0, LBatches { src: &src[5], trig: IDLE });
		assert_eq!(o.episodic, None);
		assert_eq!(o.hist.all(), &[t(3.0), t(4.0), t(5.0), t(6.0)]);

		// revived: `t` restarted from Default, the window did not.
		let o = l.tick(0, LBatches { src: &src[6], trig: ARM });
		assert_eq!(o.episodic, Some(Phase(1, 3)), "revived warm — a client-owned window would read 0 here");
	}
}
