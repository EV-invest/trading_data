use core::fmt;

use trading_data_core::{Exact, Timestamped, Timestamps, TradeCols, Trades, Ts, Venue};
use trading_data_dag::{Bump, Cell, Emit, EmitOuts, Flat, Glance, Plot, Spanning, Stamped, Tag, always_present, node, slice_nudge};
use v_utils::Timeframe;

#[derive(Clone, Copy, Debug)]
pub struct Ohlc {
	pub ts_close: Ts<Venue>,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Volume {
	pub ts_close: Ts<Venue>,
	/// Base-denominated: a quote-denominated reader multiplies by a price of its choosing.
	pub base: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_close: Ts<Venue>,
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
		self.ts_close.as_nanos()
	}
}
impl Timestamped for Ohlc {
	fn ts(&self) -> Timestamps {
		Timestamps::Simple(self.ts_close)
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
		self.ts_close.as_nanos()
	}
}
impl Timestamped for Volume {
	fn ts(&self) -> Timestamps {
		Timestamps::Simple(self.ts_close)
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
		self.ts_close.as_nanos()
	}
}
impl Timestamped for Bar {
	fn ts(&self) -> Timestamps {
		Timestamps::Simple(self.ts_close)
	}
}

// a period that closed had trades in it, so these three are absent only by not being emitted.
always_present!(Ohlc, Volume, Bar);

/// Trades → one item per period *closed*. Rate-changing: a batch spanning two periods emits two, a
/// partial period emits none (it stays in `state`). The whole of an accumulator is this boundary
/// walk — `open` and `fold` are all that tells one apart from another.
fn accumulate<T: Timestamped>(state: &mut Option<T>, trades: TradeCols<'_>, tf: Timeframe, out: &mut Vec<T>, open: impl Fn(Ts<Venue>, f64, f64) -> T, fold: impl Fn(&mut T, f64, f64)) {
	// precision is the run's, so the two scales are hoisted once instead of read per trade.
	let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
	let step = Exact::from_nanos(tf.duration().as_nanos() as i64);
	for (i, exec) in trades.exec().iter().enumerate() {
		let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
		let ts_close = exec.floor(step) + step;
		match &mut *state {
			Some(acc) if acc.ts() == Timestamps::Simple(ts_close) => fold(acc, price, qty),
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
pub fn closed_by(bars: &[Bar], deadline: Ts<Venue>) -> &[Bar] {
	&bars[..bars.partition_point(|b| b.ts_close <= deadline)]
}

/// The series over a period, in three: the two accumulators over trades, and the bar that is their
/// join. The period is a parameter rather than a name the framework pre-blessed — a node's identity
/// is still its type, and a `Bars` over one minute is as distinct from one over five as two newtypes
/// were. Only
/// [`Tag`] is new: the period has to reach [`Cell::NAME`] for the DAG card to keep saying `Bar:1m`.
#[derive(Clone, Default)]
pub struct Ohlcs<const TF: Timeframe>(Option<Ohlc>);
impl<const TF: Timeframe> Ohlcs<TF> {
	const TAG: Tag = Tag::new("Ohlc:", TF);
}
impl<const TF: Timeframe> Cell for Ohlcs<TF> {
	type Out<'t> = &'t [Ohlc];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Emit for Ohlcs<TF> {
	/// The partial bar is the whole of the state, so the trades it holds reach back exactly one
	/// period.
	type Deps = (Spanning<Trades, TF>,);

	/// [`Bars`] joins this with [`Volumes`] and draws for all three.
	const PLOTS: &'static [Plot] = &[];

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Ohlc>) {
		ohlc(&mut self.0, trades, TF, out);
	}
}
slice_nudge!([const TF: Timeframe] Ohlcs<TF>, Ohlc);

#[derive(Clone, Default)]
pub struct Volumes<const TF: Timeframe>(Option<Volume>);
impl<const TF: Timeframe> Volumes<TF> {
	const TAG: Tag = Tag::new("Vol:", TF);
}
impl<const TF: Timeframe> Cell for Volumes<TF> {
	type Out<'t> = &'t [Volume];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Emit for Volumes<TF> {
	type Deps = (Spanning<Trades, TF>,);

	/// [`Bars`] joins this with [`Ohlcs`] and draws for all three.
	const PLOTS: &'static [Plot] = &[];

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Volume>) {
		volume(&mut self.0, trades, TF, out);
	}
}
slice_nudge!([const TF: Timeframe] Volumes<TF>, Volume);

/// Stateless: both accumulators close a period on the same trade, so the join is this tick's two
/// batches zipped.
#[derive(Clone, Default)]
pub struct Bars<const TF: Timeframe>;
impl<const TF: Timeframe> Bars<TF> {
	const TAG: Tag = Tag::new("Bar:", TF);
}
impl<const TF: Timeframe> Cell for Bars<TF> {
	type Out<'t> = &'t [Bar];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Emit for Bars<TF> {
	type Deps = (crate::Ohlcs<TF>, crate::Volumes<TF>);

	/// Slot 4 (`vol_base`) goes undrawn: a histogram of it would claim an indicator pane, and the
	/// price pane draws its own volume off the price node directly.
	const PLOTS: &'static [Plot] = &[Plot {
		slots: &[0, 1, 2, 3],
		overlay: true,
		candles: true,
		..Plot::DEFAULT
	}];

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
slice_nudge!([const TF: Timeframe] Bars<TF>, Bar);
