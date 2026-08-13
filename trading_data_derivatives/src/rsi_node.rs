use core::{fmt, marker::PhantomData};

use trading_data_dag::{
	Buffering, Bump, Carried, Cell, DepOuts, Elems, Env, Ex, Expr, Flat, Folding, Folds, Glance, Lagged, Plot, Present, Reading, Rows, Scans, Series, Slots, Stamped, Tag, Unbounded, Unflat,
	Vars, Witness, absent, constant, gt, lt, max, min, node, select, slice_nudge,
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
	type Out<'t> = &'t [Reading];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
/// `Rows` because the lag is read off the retention's own history, which is what a row-keeping
/// batch is; a bar series that folded its rows would have none to look one back through.
impl<B: Series<Item = Bar, Batch = Rows<Bar>>> Scans for RsiDelta<B> {
	/// Two elements, because the lag reaches one behind the element being computed — and it is the
	/// engine's retention rather than the node's, so nothing here survives a tick.
	type Deps = (Buffering<B, Elems<2>>,);

	fn read<W: Witness>((bars,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (b, lag) = bars.lagged_at(i, 0).expect("element i of this tick's own fresh run");
		// the first bar of a run has nothing behind it, and an absence is declined rather than put:
		// a NaN in the env is an operand, where a decline is an out.
		let (p, p_lag) = bars.lagged_at(i, 1)?;
		env.dep(0).lag(lag).put(b);
		// `close` is slot 3 of a bar, and saying so is what puts this partial in the lagged element's
		// own column rather than in a column of its own.
		env.dep(0).lag(p_lag).slot(3).put(&p.close);
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<3>() - v.get::<5>()
	}
}
slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, Reading);

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
			type Out<'t> = &'t [Reading];

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
			fn read<W: Witness>((deltas,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
				let (d, lag) = deltas.at(i)?;
				env.dep(0).lag(lag).put(&d.present()?);
				Some(0)
			}

			fn step(&self, v: Vars) -> impl Slots {
				let half = max(constant($sign) * v.get::<0>(), constant(0.0));
				wilder(v.get::<1>(), v.get::<2>(), half, S::base_len() as f64)
			}

			fn value(&self, v: Vars) -> impl Slots {
				select(lt(v.get::<2>(), constant(S::base_len() as f64)), absent(), v.get::<1>())
			}

			fn carried(&self) -> &Carried {
				&self.avg
			}

			fn carried_mut(&mut self) -> &mut Carried {
				&mut self.avg
			}
		}
		slice_nudge!([B: Series<Item = Bar>, S: RsiSpec] $ty<B, S>, Reading);
	};
}
wilder_half!(AvgGain, 1.0, "RSI's numerator.");
wilder_half!(AvgLoss, -1.0, "RSI's denominator.");

/// Two readings on one element, and one presence over the pair: the RSI proper is a number from the
/// bar the averages warm on, where the EMA over it needs `smooth_len` of those — so an element is
/// half-warm for a stretch, and publishes nothing until it is whole.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RsiValues {
	pub actual: Reading,
	pub smooth: Reading,
}

impl Flat for RsiValues {
	/// Both slots are [`Reading`]s, and the `value` body declines into each on its own schedule.
	const ABSENTABLE: bool = true;
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		let (a, s) = out.split_at_mut(1);
		// `&`, not `&&`: both slots are written either way, and only the answer is the conjunction.
		self.actual.flat(a) & self.smooth.flat(s)
	}

	fn fires(&self) -> usize {
		self.present().is_some() as usize
	}
}
impl Bump for RsiValues {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		let leg: &mut Reading = [&mut self.actual, &mut self.smooth][slot];
		let (bumped, dh) = leg.bump(0, h);
		*leg = bumped;
		(self, dh)
	}
}

impl Unflat for RsiValues {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		Self {
			actual: Reading::unflat(ts_ns, &slots[..1]),
			smooth: Reading::unflat(ts_ns, &slots[1..]),
		}
	}
}

/// Any slot absent ⇒ the element is absent — the rule `Unflat for Option` used to apply on the way
/// out of every per-element kernel, kept where it is actually about this item.
// r[impl outs.absence.typed]
impl Present for RsiValues {
	type Val = Self;

	fn present(self) -> Option<Self> {
		self.actual.get().zip(self.smooth.get()).map(|_| self)
	}
}

impl Glance for RsiValues {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.actual.get() {
			Some(v) => write!(f, "{v:.1}"),
			None => f.write_str("None"),
		}
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
	type Out<'t> = &'t [RsiValues];

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
	fn read<W: Witness>((gain, loss): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		assert_eq!(gain.len(), loss.len(), "AvgGain/AvgLoss rate mismatch");
		let ((g, g_lag), (l, l_lag)) = (gain.at(i)?, loss.at(i)?);
		let (g, l) = (g.present()?, l.present()?);
		env.dep(0).lag(g_lag).put(&g);
		env.dep(1).lag(l_lag).put(&l);
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
		(actual(v), select(lt(v.get::<3>(), constant(S::smooth_len() as f64)), absent(), v.get::<2>()))
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
slice_nudge!([B: Series<Item = Bar>, S: RsiSpec] Rsi<B, S>, RsiValues);
