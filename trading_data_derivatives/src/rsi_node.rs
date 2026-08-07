use core::{fmt, marker::PhantomData};

use trading_data_dag::{
	Buffering, Bump, Carried, Cell, Elems, Env, Ex, Expr, Flat, FoldOuts, Folding, Folds, Glance, Lagged, Plot, Rows, ScanOuts, Scans, Series, Slots, Stamped, Tag, Unbounded, Unflat, Vars,
	Witness, constant, gt, lt, max, min, node, select, slice_nudge,
};

use crate::{bar::Bar, wilder};

/// The two Wilder lengths. Everything else about an RSI chain is wiring — the series it runs on is
/// the `B` parameter — but these are numbers an app that reads them from a config file has no const
/// to give, so they arrive through a type it implements once.
pub trait RsiSpec: 'static {
	/// How the chain below spells this spec in its own [`Cell::NAME`]. Deliberately without a
	/// default: `type_name` would put a module path inside every RSI card, and a spec that could
	/// silently go unnamed is how this trait came to be the one parameter a name could not be
	/// composed from.
	const NAME: &'static str;

	fn base_len() -> usize;
	fn smooth_len() -> usize;
}

/// Close-to-close change on `B` — the one series both Wilder averages are taken of.
///
/// Rate-*preserving*, and the first bar declines: a change needs two closes, and the bar that has
/// only one is an element that carried nothing rather than an element that is not there. The lag is
/// a reading off the retention (`Buffering<B, Elems<2>>`) rather than a close the node remembers,
/// which is what leaves nothing for a state to be carried in.
pub struct RsiDelta<B>(PhantomData<B>);
impl<B> Clone for RsiDelta<B> {
	fn clone(&self) -> Self {
		Self(PhantomData)
	}
}
impl<B> Default for RsiDelta<B> {
	fn default() -> Self {
		Self(PhantomData)
	}
}
impl<B: Series<Item = Bar>> RsiDelta<B> {
	const TAG: Tag = Tag::of("RsiDelta", &[B::NAME]);
}
impl<B: Series<Item = Bar>> Cell for RsiDelta<B> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
/// `Rows` because the lag is read off the retention's own history, which is what a row-keeping
/// batch is; a bar series that folded its rows would have none to look one back through.
impl<B: Series<Item = Bar, Batch = Rows<Bar>>> Scans for RsiDelta<B> {
	/// Two elements, because the lag reaches one behind the element being computed — and it is the
	/// engine's retention rather than the node's, so nothing here survives a tick.
	type Deps = (Buffering<B, Elems<2>>,);

	fn read<W: Witness>((bars,): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (b, lag) = bars.lagged_at(i, 0).expect("element i of this tick's own fresh run");
		env.dep(0).lag(lag).put(b);
		match bars.lagged_at(i, 1) {
			// `close` is slot 3 of a bar, and saying so is what puts this partial in the lagged
			// element's own column rather than in a column of its own.
			Some((p, lag)) => env.dep(0).lag(lag).slot(3).put(&p.close),
			// the first bar of a run has nothing behind it, and NaN is how that declines.
			None => env.opaque().put(&f64::NAN),
		}
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<3>() - v.get::<5>()
	}
}
slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, Option<f64>);

/// The two halves of RSI's ratio: the Wilder average of the up moves, and of the down moves as a
/// positive magnitude. Both are warm after `S::base_len()` deltas, and both are the same fold over
/// opposite signs — which is the whole of what differs between them.
macro_rules! wilder_half {
	($ty:ident, $sign:literal, $doc:literal) => {
		#[doc = $doc]
		pub struct $ty<B, S> {
			avg: Carried,
			_wiring: PhantomData<(B, S)>,
		}
		impl<B, S> Clone for $ty<B, S> {
			fn clone(&self) -> Self {
				Self {
					avg: self.avg,
					_wiring: PhantomData,
				}
			}
		}
		impl<B, S> Default for $ty<B, S> {
			fn default() -> Self {
				Self {
					avg: Carried::default(),
					_wiring: PhantomData,
				}
			}
		}
		impl<B: Series<Item = Bar>, S: RsiSpec> $ty<B, S> {
			const TAG: Tag = Tag::of(stringify!($ty), &[B::NAME, S::NAME]);
		}
		impl<B: Series<Item = Bar>, S: RsiSpec> Cell for $ty<B, S> {
			type Out<'t> = &'t [Option<f64>];

			const NAME: &'static str = Self::TAG.as_str();
		}
		#[node]
		impl<B: Series<Item = Bar>, S: RsiSpec> Folds for $ty<B, S> {
			/// A Wilder recurrence reaches to the start of the run.
			type Deps = (Folding<crate::RsiDelta<B>, Unbounded>,);

			/// Wilder's running mean and how many samples have gone into it.
			const STATE: usize = 2;

			/// The first bar has no delta, and an average is not advanced by an absence — which is
			/// exactly what declining leaves the state doing.
			fn read<W: Witness>((deltas,): &FoldOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
				let (d, lag) = deltas.at(i)?;
				env.dep(0).lag(lag).put(&(*d)?);
				Some(0)
			}

			fn step(&self, v: Vars) -> impl Slots {
				let half = max(constant($sign) * v.get::<0>(), constant(0.0));
				wilder(v.get::<1>(), v.get::<2>(), half, S::base_len() as f64)
			}

			fn value(&self, v: Vars) -> impl Slots {
				select(lt(v.get::<2>(), constant(S::base_len() as f64)), constant(f64::NAN), v.get::<1>())
			}

			fn carried(&self) -> &Carried {
				&self.avg
			}

			fn carried_mut(&mut self) -> &mut Carried {
				&mut self.avg
			}
		}
		slice_nudge!([B: Series<Item = Bar>, S: RsiSpec] $ty<B, S>, Option<f64>);
	};
}
wilder_half!(AvgGain, 1.0, "RSI's numerator.");
wilder_half!(AvgLoss, -1.0, "RSI's denominator.");

#[derive(Clone, Copy, Debug)]
pub struct RsiValues {
	pub actual: f64,
	pub smooth: f64,
}

impl Flat for RsiValues {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.actual, self.smooth]);
		true
	}
}
impl Bump for RsiValues {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		*[&mut self.actual, &mut self.smooth][slot] += h;
		(self, h)
	}
}

impl Unflat for RsiValues {
	fn unflat(_: i64, slots: &[f64]) -> Self {
		Self { actual: slots[0], smooth: slots[1] }
	}
}

impl Glance for RsiValues {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.1}", self.actual)
	}
}

/// Wilder RSI, EMA-smoothed. Warmth is `base_len + smooth_len` closed bars, which is exactly when
/// both stages are warm: the averages need `base_len` deltas, and only then does the EMA start
/// seeing values.
pub struct Rsi<B, S> {
	/// Nautilus's `ExponentialMovingAverage`, as two slots: the running value, and how many samples
	/// have reached it — seeded on the first, warm after `smooth_len` of them.
	smooth: Carried,
	_wiring: PhantomData<(B, S)>,
}

impl<B, S> Clone for Rsi<B, S> {
	fn clone(&self) -> Self {
		Self {
			smooth: self.smooth,
			_wiring: PhantomData,
		}
	}
}
impl<B, S> Default for Rsi<B, S> {
	fn default() -> Self {
		Self {
			smooth: Carried::default(),
			_wiring: PhantomData,
		}
	}
}
impl<B: Series<Item = Bar>, S: RsiSpec> Rsi<B, S> {
	const TAG: Tag = Tag::of("Rsi", &[B::NAME, S::NAME]);
}
impl<B: Series<Item = Bar>, S: RsiSpec> Cell for Rsi<B, S> {
	type Out<'t> = &'t [Option<RsiValues>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<B: Series<Item = Bar>, S: RsiSpec> Folds for Rsi<B, S> {
	/// The smoothing EMA is a recurrence over both legs, so it reaches to the start of the run.
	type Deps = (Folding<crate::AvgGain<B, S>, Unbounded>, Folding<crate::AvgLoss<B, S>, Unbounded>);

	// No threshold guide: whoever wires this owns the trigger and `Plot` is a const, so drawing one
	// here would pin a number the wiring is free to move.
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 100.0)),
		labels: &[&["actual", "smooth"]],
		..Plot::DEFAULT
	}];
	const STATE: usize = 2;

	/// Both legs warm together, so an element either carries both averages or neither.
	fn read<W: Witness>((gain, loss): &FoldOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		assert_eq!(gain.len(), loss.len(), "AvgGain/AvgLoss rate mismatch");
		let ((g, g_lag), (l, l_lag)) = (gain.at(i)?, loss.at(i)?);
		env.dep(0).lag(g_lag).put(&(*g)?);
		env.dep(1).lag(l_lag).put(&(*l)?);
		Some(0)
	}

	fn step(&self, v: Vars) -> impl Slots {
		let (ema, seen) = (v.get::<2>(), v.get::<3>());
		let alpha = 2.0 / (S::smooth_len() as f64 + 1.0);
		(
			select(lt(seen, constant(1.0)), actual(v), constant(alpha) * actual(v) + constant(1.0 - alpha) * ema),
			min(seen + constant(1.0), constant(S::smooth_len() as f64)),
		)
	}

	fn value(&self, v: Vars) -> impl Slots {
		(actual(v), select(lt(v.get::<3>(), constant(S::smooth_len() as f64)), constant(f64::NAN), v.get::<2>()))
	}

	fn carried(&self) -> &Carried {
		&self.smooth
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.smooth
	}
}

/// [`rsi`] as an expression over the two averages a [`Rsi`] body reads at slots 0 and 1. A loss of
/// zero is every gain and no loss, which is the top of the scale rather than a division.
fn actual(v: Vars) -> Ex<impl Expr> {
	let (gain, loss) = (v.get::<0>(), v.get::<1>());
	select(gt(loss, constant(0.0)), constant(100.0) - constant(100.0) / (constant(1.0) + gain / loss), constant(100.0))
}
slice_nudge!([B: Series<Item = Bar>, S: RsiSpec] Rsi<B, S>, Option<RsiValues>);
