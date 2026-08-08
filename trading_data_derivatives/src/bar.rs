use core::fmt;

use trading_data_core::{Timestamped, Timestamps, TradeCols, Trades, Ts, Venue};
use trading_data_dag::{
	Bump, Cell, CloseOuts, Closes, Env, Flat, Folding, Glance, Ink, Lagged, Over, Pending, Plot, ScanOuts, Scans, Slots, Stamped, Tag, Tail, Unflat, Vars, Witness, always_present, max, min,
	node, slice_nudge,
};
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
impl Unflat for Ohlc {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		Self {
			ts_close: Ts::from_nanos(ts_ns),
			open: slots[0],
			high: slots[1],
			low: slots[2],
			close: slots[3],
		}
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
impl Unflat for Volume {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		Self {
			ts_close: Ts::from_nanos(ts_ns),
			base: slots[0],
		}
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

impl Unflat for Bar {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		Self {
			ts_close: Ts::from_nanos(ts_ns),
			open: slots[0],
			high: slots[1],
			low: slots[2],
			close: slots[3],
			vol_base: slots[4],
		}
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

/// One trade's price and quantity, at the run's own precision — the element a period accumulates.
/// The whole of what tells one accumulator from another is its two bodies; this is what they are
/// bodies *of*, and it is the same reading for both.
fn trade<W: Witness>(trades: TradeCols<'_>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
	let (exec, lag) = trades.exec().at(i)?;
	env.dep(0)
		.lag(lag)
		.put(&[trades.price[i] as f64 / trades.prec.price.scale(), trades.qty[i] as f64 / trades.prec.qty.scale()]);
	Some(exec.as_nanos())
}

/// The prefix of a slower series that has *closed* by `deadline` — the cross-rate read a node
/// clocked by a faster series makes against a [`trading_data_dag::Buffering`] dep. A [`Tail`] and
/// not a slice: the cut hides however many elements stand past the deadline, and a pick inside it
/// still has to report its lag against the column it lands in.
pub fn closed_by(bars: &[Bar], deadline: Ts<Venue>) -> Tail<'_, Bar> {
	Tail::from(bars).upto(|b| b.ts_close <= deadline)
}

/// The series over a period, in three: the two accumulators over trades, and the bar that is their
/// join. The period is a parameter rather than a name the framework pre-blessed — a node's identity
/// is still its type, and a `Bars` over one minute is as distinct from one over five as two newtypes
/// were. Only
/// [`Tag`] is new: the period has to reach [`Cell::NAME`] for the DAG card to keep saying `Bar:1m`.
#[derive(Clone, Default)]
pub struct Ohlcs<const TF: Timeframe>(Pending);
impl<const TF: Timeframe> Ohlcs<TF> {
	const TAG: Tag = Tag::new("Ohlc:", TF);
}
impl<const TF: Timeframe> Cell for Ohlcs<TF> {
	type Out<'t> = &'t [Ohlc];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Closes for Ohlcs<TF> {
	/// The partial bar is the whole of the state, so the trades it holds reach back exactly one
	/// period.
	type Deps = (Folding<Trades, Over<TF>>,);

	const PERIOD: Timeframe = TF;
	/// [`Bars`] joins this with [`Volumes`] and draws for all three.
	const PLOTS: &'static [Plot] = &[];

	fn read<W: Witness>((trades,): &CloseOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		trade(*trades, i, env)
	}

	fn open(&self, v: Vars) -> impl Slots {
		let price = v.get::<0>();
		(price, price, price, price)
	}

	/// `high`/`low` are the one value-dependent pick in the workspace, and they live inside the
	/// algebra as `Max`/`Min` with a pinned tie-break — each branch differentiable, and the tie
	/// resolving the same way in the value and in the derivative
	/// (`r[kernels.selection.index-is-not-a-variable]`).
	fn fold(&self, v: Vars) -> impl Slots {
		let (price, open, high, low) = (v.get::<0>(), v.get::<2>(), v.get::<3>(), v.get::<4>());
		(open, max(high, price), min(low, price), price)
	}

	fn pending(&self) -> &Pending {
		&self.0
	}

	fn pending_mut(&mut self) -> &mut Pending {
		&mut self.0
	}
}
slice_nudge!([const TF: Timeframe] Ohlcs<TF>, Ohlc);

#[derive(Clone, Default)]
pub struct Volumes<const TF: Timeframe>(Pending);
impl<const TF: Timeframe> Volumes<TF> {
	const TAG: Tag = Tag::new("Vol:", TF);
}
impl<const TF: Timeframe> Cell for Volumes<TF> {
	type Out<'t> = &'t [Volume];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Closes for Volumes<TF> {
	type Deps = (Folding<Trades, Over<TF>>,);

	const PERIOD: Timeframe = TF;
	/// [`Bars`] joins this with [`Ohlcs`] and draws for all three.
	const PLOTS: &'static [Plot] = &[];

	fn read<W: Witness>((trades,): &CloseOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		trade(*trades, i, env)
	}

	fn open(&self, v: Vars) -> impl Slots {
		v.get::<1>()
	}

	fn fold(&self, v: Vars) -> impl Slots {
		v.get::<2>() + v.get::<1>()
	}

	fn pending(&self) -> &Pending {
		&self.0
	}

	fn pending_mut(&mut self) -> &mut Pending {
		&mut self.0
	}
}
slice_nudge!([const TF: Timeframe] Volumes<TF>, Volume);

/// Candles are the ground every overlay is read against, so they carry near-zero chroma: hue on the
/// price pane belongs to what a node has to say about the price, not to the price itself.
const CANDLE: Ink = Ink { c: 0.008, ..Ink::MAIN };

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
impl<const TF: Timeframe> Scans for Bars<TF> {
	/// Both deps drive, at one rate — the `assert_eq` below is that claim, and it is what makes
	/// element `i` of the second dep its newest exactly when element `i` of the first is.
	type Deps = (crate::Ohlcs<TF>, crate::Volumes<TF>);

	/// Slot 4 (`vol_base`) goes undrawn: a histogram of it would claim an indicator pane, and the
	/// price pane draws its own volume off the price node directly.
	const PLOTS: &'static [Plot] = &[Plot {
		slots: &[0, 1, 2, 3],
		inks: &[CANDLE; 4],
		overlay: true,
		candles: true,
		..Plot::DEFAULT
	}];

	fn read<W: Witness>((ohlc, vol): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		assert_eq!(ohlc.len(), vol.len(), "one Ohlc and one Volume per period closed");
		let ((o, o_lag), (v, v_lag)) = (ohlc.at(i)?, vol.at(i)?);
		assert_eq!(o.ts_close, v.ts_close, "the two accumulators walk one boundary");
		env.dep(0).lag(o_lag).put(o);
		env.dep(1).lag(v_lag).put(v);
		Some(o.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		(v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>(), v.get::<4>())
	}
}
slice_nudge!([const TF: Timeframe] Bars<TF>, Bar);
