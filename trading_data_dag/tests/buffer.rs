//! `Buffer`/`Buffering` through `graph!`: the past/fresh split across a multi-element batch (the
//! invariant intra-batch cursors rest on), `Flat` reading `fresh` only, and a latch commutation
//! that resets a consumer while leaving the buffer whole — the point of engine-owned retention.

use trading_data_dag::{Buffer, Buffering, Bump, Cell, DepOuts, Episode, Fire, Flat, Gate, Glance, Latch, Node, Observer, graph, slice_nudge};

/// The root series: whatever the caller hands in this tick.
struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

/// Reads a 3-deep window per fresh element: rate-preserving, `None` while short.
#[derive(Clone, Default)]
struct Sum3 {
	buf: Vec<Option<f64>>,
}
impl Cell for Sum3 {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Sum3 {
	type Deps = (Buffering<Src, 3>,);

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		self.buf.extend(hist.trailing(3).map(|w| w.map(|w| w.iter().sum())));
		&self.buf
	}
}
slice_nudge!(Sum3, Option<f64>);

/// Records the past/fresh split it saw, so the test can assert on the *shape* of the dep out and
/// not merely on a derived number.
#[derive(Clone, Default)]
struct Split {
	seen: Vec<(usize, usize)>,
	buf: Vec<f64>,
}
impl Cell for Split {
	type Out<'t> = &'t [f64];
}
impl Node for Split {
	type Deps = (Buffering<Src, 3>,);

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.seen.push((hist.past().len(), hist.fresh().len()));
		self.buf.clear();
		self.buf.extend_from_slice(hist.fresh());
		&self.buf
	}
}
slice_nudge!(Split, f64);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	hist: Buffer<Src, 3>,
	sum3: Sum3,
	split: Split,
}

#[test]
fn past_fresh_split_and_trailing() {
	let mut g = G::default();

	// cold: one element, no window yet.
	let o = g.tick(Batches { src: &[1.0] });
	assert_eq!(o.sum3, &[None]);
	assert_eq!(o.hist.past(), &[] as &[f64]);
	assert_eq!(o.hist.fresh(), &[1.0]);

	// a batch carrying several elements: `past` is what stood behind the *whole* batch, so the
	// per-element cursors each see their own trailing window.
	let o = g.tick(Batches { src: &[2.0, 3.0, 4.0] });
	assert_eq!(o.hist.past(), &[1.0]);
	assert_eq!(o.hist.fresh(), &[2.0, 3.0, 4.0]);
	assert_eq!(o.sum3, &[None, Some(6.0), Some(9.0)]);

	// trim happens before the append, and keeps K = 3 behind.
	let o = g.tick(Batches { src: &[5.0] });
	assert_eq!(o.hist.past(), &[2.0, 3.0, 4.0]);
	assert_eq!(o.sum3, &[Some(12.0)]);

	// an empty batch still advances the buffer, and a whole K survives it — that is what a
	// cross-rate reader of `all()` rests on.
	let o = g.tick(Batches { src: &[] });
	assert_eq!(o.hist.all(), &[3.0, 4.0, 5.0]);
	assert_eq!(o.hist.fresh(), &[] as &[f64]);
	assert_eq!(o.sum3, &[] as &[Option<f64>]);

	assert_eq!(g.split.seen, &[(0, 1), (1, 3), (3, 1), (3, 0)]);
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
		fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [&'static str], fire: Fire<'_>) {
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
	g.tick(Batches { src: &[1.0, 2.0] });

	let mut rec = Rec::default();
	g.tick_obs(Batches { src: &[7.0, 8.0] }, &mut rec);
	assert_eq!(rec.hist, rec.src, "a buffer's Fire must match its source's");
	assert_eq!(rec.hist, Some((2, vec![8.0])));
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

	#[derive(Clone, Copy, Debug, PartialEq)]
	struct Phase(u32);
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

	/// The revivable consumer: `t` makes a reset observable, `seen` makes a *cold* buffer observable.
	#[derive(Clone, Default)]
	struct Episodic {
		t: u32,
		seen: Vec<usize>,
	}
	impl Cell for Episodic {
		type Out<'t> = Option<Phase>;
	}
	impl Node for Episodic {
		type Deps = (Buffering<Src, 3>,);
		type When = (Live,);

		const HISTORIC: bool = false;

		fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
			self.t += 1;
			self.seen.push(hist.trailing_at(0, 3).map_or(0, <[f64]>::len));
			Some(Phase(self.t))
		}
	}

	graph! {
		struct L;
		batches LBatches;
		roots { src: Src[f64], trig: Trig[u32] };
		out LOut;
		latch { live: Live }
		hist: Buffer<Src, 3>,
		live: Live,
		episodic: Episodic,
	}

	const ARM: &[f64] = &[1.0];
	const IDLE: &[f64] = &[];

	#[test]
	fn latch_resets_consumer_not_buffer() {
		let mut l = L::default();

		// dark: the consumer never advances, yet the buffer fills — that is the whole feature.
		for x in [&[1.0f64] as &[f64], &[2.0], &[3.0]] {
			let o = l.tick(LBatches { src: x, trig: IDLE });
			assert_eq!(o.episodic, None);
		}
		assert_eq!(l.episodic.seen, &[] as &[usize]);

		// armed: full window on its very first tick back, no re-warm.
		let o = l.tick(LBatches { src: &[4.0], trig: ARM });
		assert_eq!(o.episodic, Some(Phase(1)));
		assert_eq!(o.hist.all(), &[1.0, 2.0, 3.0, 4.0]);
		let o = l.tick(LBatches { src: &[5.0], trig: IDLE });
		assert_eq!(o.episodic, Some(Phase(2)));
		assert_eq!(l.episodic.seen, &[3, 3]);

		// commutated: consumer reset to Default, buffer untouched.
		let o = l.tick(LBatches { src: &[6.0], trig: IDLE });
		assert_eq!(o.episodic, None);
		assert_eq!(o.hist.all(), &[3.0, 4.0, 5.0, 6.0]);

		// revived: `t` and `seen` restarted from Default, the window did not.
		let o = l.tick(LBatches { src: &[7.0], trig: ARM });
		assert_eq!(o.episodic, Some(Phase(1)));
		assert_eq!(l.episodic.seen, &[3], "revived warm — a client-owned window would read 0 here");
	}
}
