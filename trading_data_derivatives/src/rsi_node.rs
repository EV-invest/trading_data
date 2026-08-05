use core::{fmt, marker::PhantomData};

use trading_data_dag::{Bump, Cell, Emit, EmitOuts, Flat, Folding, Glance, Horizon, Plot, Series, Tag, node, slice_nudge};

use crate::{Wilder, bar::Bar, rsi};

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
/// Rate-changing on the very first bar: a change needs two closes.
pub struct RsiDelta<B> {
	prev_close: Option<f64>,
	_series: PhantomData<B>,
}
impl<B> Clone for RsiDelta<B> {
	fn clone(&self) -> Self {
		Self {
			prev_close: self.prev_close,
			_series: PhantomData,
		}
	}
}
impl<B> Default for RsiDelta<B> {
	fn default() -> Self {
		Self {
			prev_close: None,
			_series: PhantomData,
		}
	}
}
impl<B: Series<Item = Bar>> RsiDelta<B> {
	const TAG: Tag = Tag::of("RsiDelta", &[B::NAME]);
}
impl<B: Series<Item = Bar>> Cell for RsiDelta<B> {
	type Out<'t> = &'t [f64];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<B: Series<Item = Bar>> Emit for RsiDelta<B> {
	type Deps = (Folding<B, { Horizon::Elems(1) }>,);

	const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

	fn emit(&mut self, (bars,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		for b in bars {
			if let Some(prev) = self.prev_close.replace(b.close) {
				out.push(b.close - prev);
			}
		}
	}
}
slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, f64);

/// The two halves of RSI's ratio: the Wilder average of the up moves, and of the down moves as a
/// positive magnitude. Both are warm after `S::base_len()` deltas, and both are the same fold over
/// opposite signs — which is the whole of what differs between them.
macro_rules! wilder_half {
	($ty:ident, $sign:literal, $doc:literal) => {
		#[doc = $doc]
		pub struct $ty<B, S> {
			avg: Wilder,
			_wiring: PhantomData<(B, S)>,
		}
		impl<B, S> Clone for $ty<B, S> {
			fn clone(&self) -> Self {
				Self {
					avg: self.avg.clone(),
					_wiring: PhantomData,
				}
			}
		}
		impl<B, S: RsiSpec> Default for $ty<B, S> {
			fn default() -> Self {
				Self {
					avg: Wilder::new(S::base_len()),
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
		impl<B: Series<Item = Bar>, S: RsiSpec> Emit for $ty<B, S> {
			/// A Wilder recurrence reaches to the start of the run.
			type Deps = (Folding<crate::RsiDelta<B>, { Horizon::Unbounded }>,);
			const WHY: &'static str = "a recurrence carried across elements, which the `Fold` kernel is not built for yet";

			fn emit(&mut self, (deltas,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
				out.extend(deltas.iter().map(|d| self.avg.update(($sign * d).max(0.0))));
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

impl Glance for RsiValues {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.1}", self.actual)
	}
}

/// Wilder RSI, EMA-smoothed. Warmth is `base_len + smooth_len` closed bars, which is exactly when
/// both stages are warm: the averages need `base_len` deltas, and only then does the EMA start
/// seeing values.
pub struct Rsi<B, S> {
	smooth: Ema,
	_wiring: PhantomData<(B, S)>,
}
/// Nautilus's `ExponentialMovingAverage`: seeded on the first sample, warm after `period` of them.
#[derive(Clone)]
struct Ema {
	period: usize,
	value: f64,
	seen: usize,
}
impl Ema {
	fn new(period: usize) -> Self {
		assert!(period > 0);
		Self { period, value: 0.0, seen: 0 }
	}

	fn update(&mut self, x: f64) -> Option<f64> {
		let alpha = 2.0 / (self.period as f64 + 1.0);
		self.value = if self.seen == 0 { x } else { alpha * x + (1.0 - alpha) * self.value };
		self.seen += 1;
		(self.seen >= self.period).then_some(self.value)
	}
}

impl<B, S> Clone for Rsi<B, S> {
	fn clone(&self) -> Self {
		Self {
			smooth: self.smooth.clone(),
			_wiring: PhantomData,
		}
	}
}
impl<B, S: RsiSpec> Default for Rsi<B, S> {
	fn default() -> Self {
		Self {
			smooth: Ema::new(S::smooth_len()),
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
impl<B: Series<Item = Bar>, S: RsiSpec> Emit for Rsi<B, S> {
	/// The smoothing EMA is a recurrence over both legs, so it reaches to the start of the run.
	type Deps = (Folding<crate::AvgGain<B, S>, { Horizon::Unbounded }>, Folding<crate::AvgLoss<B, S>, { Horizon::Unbounded }>);

	// No threshold guide: whoever wires this owns the trigger and `Plot` is a const, so drawing one
	// here would pin a number the wiring is free to move.
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 100.0)),
		labels: &[&["actual", "smooth"]],
		..Plot::DEFAULT
	}];
	const WHY: &'static str = "a recurrence carried across elements, which the `Fold` kernel is not built for yet";

	fn emit(&mut self, (gain, loss): EmitOuts<'_, Self>, out: &mut Vec<Option<RsiValues>>) {
		assert_eq!(gain.len(), loss.len(), "AvgGain/AvgLoss rate mismatch");
		for (g, l) in gain.iter().zip(loss) {
			out.push(g.zip(*l).and_then(|(g, l)| {
				let actual = rsi(g, l);
				self.smooth.update(actual).map(|smooth| RsiValues { actual, smooth })
			}));
		}
	}
}
slice_nudge!([B: Series<Item = Bar>, S: RsiSpec] Rsi<B, S>, Option<RsiValues>);
