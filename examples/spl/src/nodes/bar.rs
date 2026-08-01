use core::fmt;

use trading_data::{Cell, DepOuts, Exact, Glance, Horizon, Node, Stamped, Trades, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_close: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	/// Base-denominated. SPL's volume indie reads `volume * close` — the close standing in for vwap.
	pub vol_base: f64,
}

flat_fields!(Bar[open, high, low, close, vol_base]);

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

/// The prefix of a slower series that has *closed* by `deadline` — the cross-rate read a node
/// clocked by a faster series makes against a [`trading_data::Buffering`] dep.
pub(super) fn closed_by(bars: &[Bar], deadline: i64) -> &[Bar] {
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

/// Trades → OHLCV bars at one period. Rate-changing: one non-optional bar per boundary crossed, so
/// a batch spanning two periods emits two; a partial period emits none (its bar stays in `acc`).
/// Shared by every series: only the period and the name are per-type.
#[derive(Clone, Default)]
pub struct BarAcc {
	acc: Option<Bar>,
	buf: Vec<Bar>,
}
impl BarAcc {
	fn advance<'t>(&'t mut self, trades: <Trades as Cell>::Out<'_>, tf: Timeframe) -> &'t [Bar] {
		self.buf.clear();
		// precision is the run's, so the two scales are hoisted once instead of read per trade.
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		let step = Exact::from_nanos(tf.duration().as_nanos() as i64);
		for (i, exec) in trades.exec().iter().enumerate() {
			let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
			let ts_close = exec.floor(step).as_nanos() + step.as_nanos();
			match &mut self.acc {
				Some(b) if b.ts_close == ts_close => {
					b.high = b.high.max(price);
					b.low = b.low.min(price);
					b.close = price;
					b.vol_base += qty;
				}
				acc => {
					if let Some(done) = acc.take() {
						self.buf.push(done);
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
		&self.buf
	}
}

/// One named series per timeframe. A distinct type per period is what the graph already demands
/// (node identity *is* its type); naming it after the timeframe is what makes the period legible
/// everywhere the name surfaces — DAG cards, dep edges, `step_until`.
macro_rules! bars {
	($($ty:ident = $tf:literal),+ $(,)?) => { $(
		#[derive(Clone, Default)]
		pub struct $ty(BarAcc);
		impl $ty {
			pub const TF: Timeframe = tf($tf);
		}
		impl Cell for $ty {
			type Out<'t> = &'t [Bar];

			const NAME: &'static str = concat!("Bar:", $tf);
		}
		impl Node for $ty {
			type Deps = (Trades,);

			/// Only the partial bar is held, so the state reaches back exactly one period.
			const HORIZON: Horizon = Horizon::Span(Self::TF);

			fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
				self.0.advance(trades, Self::TF)
			}
		}
		slice_nudge!($ty, Bar);
	)+ };
}
bars!(Bar1m = "1m", Bar5m = "5m", Bar15m = "15m", Bar1h = "1h", Bar4h = "4h");
