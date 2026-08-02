use core::fmt;

use trading_data_core::{Exact, TradeCols, Trades};
use trading_data_dag::{Bump, Cell, Emit, EmitOuts, Flat, Folding, Glance, Horizon, Stamped, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

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

/// Trades → OHLCV bars at one period. Rate-changing: one non-optional bar per boundary crossed, so
/// a batch spanning two periods emits two; a partial period emits none (it stays in `state`).
/// Shared by every series: only the period and the name are per-type.
fn accumulate(state: &mut Option<Bar>, trades: TradeCols<'_>, tf: Timeframe, out: &mut Vec<Bar>) {
	// precision is the run's, so the two scales are hoisted once instead of read per trade.
	let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
	let step = Exact::from_nanos(tf.duration().as_nanos() as i64);
	for (i, exec) in trades.exec().iter().enumerate() {
		let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
		let ts_close = exec.floor(step).as_nanos() + step.as_nanos();
		match &mut *state {
			Some(b) if b.ts_close == ts_close => {
				b.high = b.high.max(price);
				b.low = b.low.min(price);
				b.close = price;
				b.vol_base += qty;
			}
			acc => {
				if let Some(done) = acc.take() {
					out.push(done);
				}
				*acc = Some(Bar {
					ts_close,
					open: price,
					high: price,
					low: price,
					close: price,
					vol_base: qty,
				});
			}
		}
	}
}

/// The prefix of a slower series that has *closed* by `deadline` — the cross-rate read a node
/// clocked by a faster series makes against a [`trading_data_dag::Buffering`] dep.
pub fn closed_by(bars: &[Bar], deadline: i64) -> &[Bar] {
	&bars[..bars.partition_point(|b| b.ts_ns() <= deadline)]
}

/// [`Timeframe`]'s parse rules in const position. Parsing is the cheap direction — const-formatting
/// a number back out is not — so the literal that *names* a series is the same one that *defines*
/// it, and `Bar:1m` cannot drift into being a one-*second* series.
const fn tf(s: &str) -> Timeframe {
	let b = s.as_bytes();
	let (mut n, mut i) = (0u64, 0);
	while i < b.len() && b[i].is_ascii_digit() {
		n = n * 10 + (b[i] - b'0') as u64;
		i += 1;
	}
	assert!(n > 0, "a timeframe leads with its count");
	let (_, designator) = b.split_at(i);
	Timeframe::from_naive(
		n,
		match designator {
			b"s" => TimeframeDesignator::Seconds,
			b"m" => TimeframeDesignator::Minutes,
			b"h" => TimeframeDesignator::Hours,
			b"d" => TimeframeDesignator::Days,
			_ => panic!("timeframe designator is one of s/m/h/d"),
		},
	)
}

/// One named series per timeframe. A distinct type per period is what the graph already demands
/// (node identity *is* its type); naming it after the timeframe is what makes the period legible
/// everywhere the name surfaces — DAG cards, dep edges, `step_until`.
macro_rules! bars {
	($($ty:ident = $tf:literal),+ $(,)?) => { $(
		#[derive(Clone, Default)]
		pub struct $ty(Option<Bar>);
		impl $ty {
			pub const TF: Timeframe = tf($tf);
		}
		impl Cell for $ty {
			type Out<'t> = &'t [Bar];

			const NAME: &'static str = concat!("Bar:", $tf);
		}
		impl Emit for $ty {
			/// The partial bar is the whole of the state, so the trades it holds reach back exactly
			/// one period.
			type Deps = (Folding<Trades, { Horizon::Span(Self::TF) }>,);

			fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Bar>) {
				accumulate(&mut self.0, trades, Self::TF, out);
			}
		}
		slice_nudge!($ty, Bar);
	)+ };
}
bars!(Bar1m = "1m", Bar5m = "5m", Bar15m = "15m", Bar1h = "1h", Bar4h = "4h");
