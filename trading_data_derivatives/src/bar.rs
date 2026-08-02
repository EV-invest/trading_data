use core::fmt;

use trading_data_core::{Exact, TradeCols, Trades};
use trading_data_dag::{Bump, Cell, Emit, EmitOuts, Flat, Folding, Glance, Horizon, Stamped, node, slice_nudge};
use v_utils::Timeframe;

#[derive(Clone, Copy, Debug)]
pub struct Ohlc {
	pub ts_close: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Volume {
	pub ts_close: i64,
	/// Base-denominated: a quote-denominated reader multiplies by a price of its choosing.
	pub base: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_close: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	/// Base-denominated: a volume indie wanting quote reads `vol_base * close`, the close standing in
	/// for vwap.
	pub vol_base: f64,
}

impl Flat for Ohlc {
	const DIMS: &'static [usize] = &[4];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.open, self.high, self.low, self.close]);
		true
	}
}
impl Bump for Ohlc {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		*[&mut self.open, &mut self.high, &mut self.low, &mut self.close][slot] += h;
		(self, h)
	}
}
impl Glance for Ohlc {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "close {}", self.close)
	}
}
impl Stamped for Ohlc {
	fn ts_ns(&self) -> i64 {
		self.ts_close
	}
}

impl Flat for Volume {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.base;
		true
	}
}
impl Bump for Volume {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		debug_assert_eq!(slot, 0);
		self.base += h;
		(self, h)
	}
}
impl Glance for Volume {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.base)
	}
}
impl Stamped for Volume {
	fn ts_ns(&self) -> i64 {
		self.ts_close
	}
}

impl Flat for Bar {
	const DIMS: &'static [usize] = &[5];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.open, self.high, self.low, self.close, self.vol_base]);
		true
	}
}
impl Bump for Bar {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		*[&mut self.open, &mut self.high, &mut self.low, &mut self.close, &mut self.vol_base][slot] += h;
		(self, h)
	}
}

impl Glance for Bar {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "close {}", self.close)
	}
}

impl Stamped for Bar {
	fn ts_ns(&self) -> i64 {
		self.ts_close
	}
}

/// Trades → one item per period *closed*. Rate-changing: a batch spanning two periods emits two, a
/// partial period emits none (it stays in `state`). The whole of an accumulator is this boundary
/// walk — `open` and `fold` are all that tells one apart from another.
fn accumulate<T: Stamped>(
	state: &mut Option<T>,
	trades: TradeCols<'_>,
	tf: Timeframe,
	out: &mut Vec<T>,
	open: impl Fn(i64, f64, f64) -> T,
	fold: impl Fn(&mut T, f64, f64),
) {
	// precision is the run's, so the two scales are hoisted once instead of read per trade.
	let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
	let step = Exact::from_nanos(tf.duration().as_nanos() as i64);
	for (i, exec) in trades.exec().iter().enumerate() {
		let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
		let ts_close = exec.floor(step).as_nanos() + step.as_nanos();
		match &mut *state {
			Some(acc) if acc.ts_ns() == ts_close => fold(acc, price, qty),
			slot => {
				if let Some(done) = slot.take() {
					out.push(done);
				}
				*slot = Some(open(ts_close, price, qty));
			}
		}
	}
}

fn ohlc(state: &mut Option<Ohlc>, trades: TradeCols<'_>, tf: Timeframe, out: &mut Vec<Ohlc>) {
	accumulate(
		state,
		trades,
		tf,
		out,
		|ts_close, price, _| Ohlc {
			ts_close,
			open: price,
			high: price,
			low: price,
			close: price,
		},
		|o, price, _| {
			o.high = o.high.max(price);
			o.low = o.low.min(price);
			o.close = price;
		},
	);
}

fn volume(state: &mut Option<Volume>, trades: TradeCols<'_>, tf: Timeframe, out: &mut Vec<Volume>) {
	accumulate(state, trades, tf, out, |ts_close, _, qty| Volume { ts_close, base: qty }, |v, _, qty| v.base += qty);
}

/// The prefix of a slower series that has *closed* by `deadline` — the cross-rate read a node
/// clocked by a faster series makes against a [`trading_data_dag::Buffering`] dep.
pub fn closed_by(bars: &[Bar], deadline: i64) -> &[Bar] {
	&bars[..bars.partition_point(|b| b.ts_ns() <= deadline)]
}

/// One named series per timeframe, in three: the two accumulators over trades, and the bar that is
/// their join. A distinct type per period is what the graph already demands (node identity *is* its
/// type); naming it after the timeframe is what makes the period legible everywhere the name
/// surfaces — DAG cards, dep edges, `step_until`. The period is stated once per row here, which is
/// what makes "same timeframe" a property of the wiring rather than of a consumer's care.
macro_rules! bars {
	($($ohlc:ident + $vol:ident => $bars:ident = $tf:literal),+ $(,)?) => { $(
		#[derive(Clone, Default)]
		pub struct $ohlc(Option<Ohlc>);
		impl $ohlc {
			pub const TF: Timeframe = Timeframe::from_str_const($tf);
		}
		impl Cell for $ohlc {
			type Out<'t> = &'t [Ohlc];

			const NAME: &'static str = concat!("Ohlc:", $tf);
		}
		#[node]
		impl Emit for $ohlc {
			/// The partial bar is the whole of the state, so the trades it holds reach back exactly
			/// one period.
			type Deps = (Folding<Trades, { Horizon::Span(Self::TF) }>,);

			fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Ohlc>) {
				ohlc(&mut self.0, trades, Self::TF, out);
			}
		}
		slice_nudge!($ohlc, Ohlc);

		#[derive(Clone, Default)]
		pub struct $vol(Option<Volume>);
		impl $vol {
			pub const TF: Timeframe = Timeframe::from_str_const($tf);
		}
		impl Cell for $vol {
			type Out<'t> = &'t [Volume];

			const NAME: &'static str = concat!("Vol:", $tf);
		}
		#[node]
		impl Emit for $vol {
			type Deps = (Folding<Trades, { Horizon::Span(Self::TF) }>,);

			fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Volume>) {
				volume(&mut self.0, trades, Self::TF, out);
			}
		}
		slice_nudge!($vol, Volume);

		/// Stateless: both accumulators close a period on the same trade, so the join is this tick's
		/// two batches zipped.
		#[derive(Clone, Default)]
		pub struct $bars;
		impl $bars {
			pub const TF: Timeframe = Timeframe::from_str_const($tf);
		}
		impl Cell for $bars {
			type Out<'t> = &'t [Bar];

			const NAME: &'static str = concat!("Bar:", $tf);
		}
		#[node]
		impl Emit for $bars {
			type Deps = (crate::$ohlc, crate::$vol);

			fn emit(&mut self, (ohlc, vol): EmitOuts<'_, Self>, out: &mut Vec<Bar>) {
				assert_eq!(ohlc.len(), vol.len(), "one Ohlc and one Volume per period closed");
				out.extend(ohlc.iter().zip(vol).map(|(o, v)| {
					assert_eq!(o.ts_close, v.ts_close, "the two accumulators walk one boundary");
					Bar {
						ts_close: o.ts_close,
						open: o.open,
						high: o.high,
						low: o.low,
						close: o.close,
						vol_base: v.base,
					}
				}));
			}
		}
		slice_nudge!($bars, Bar);
	)+ };
}
bars!(
	Ohlc1m + Vol1m => Bar1m = "1m",
	Ohlc5m + Vol5m => Bar5m = "5m",
	Ohlc15m + Vol15m => Bar15m = "15m",
	Ohlc1h + Vol1h => Bar1h = "1h",
	Ohlc4h + Vol4h => Bar4h = "4h",
);
