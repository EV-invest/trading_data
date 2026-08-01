use core::fmt;

use trading_data::{Buffering, Cell, DepOuts, Flat, Glance, Horizon, Node, Plot, slice_nudge};
use v_utils::Timeframe;

use super::bar::{Bar, Bar4h, Bar5m, closed_by};
use crate::config::strategy;

/// Pine's `* 365`, kept verbatim regardless of bar timeframe — which is why the 4h and 5m Sharpe
/// scales differ and each gets its own threshold.
const PINE_PERIODS_PER_YEAR: f64 = 365.0;
/// Periods retained behind `indies.momentum.lookback`, which is a runtime knob where a buffer's
/// reach is a const — so this is a *capacity*, checked against the configured lookback in
/// [`crate::config::Config::load`]. Raising it costs `2 * (MOM_PERIODS - lookback)` retained bars.
pub const MOM_PERIODS: u64 = 256;
/// That capacity as the reach it is: `MOM_PERIODS` of the series' own periods.
pub const fn mom_cap(tf: Timeframe) -> Horizon {
	Horizon::Span(Timeframe(MOM_PERIODS * tf.0))
}

#[derive(Clone, Copy, Debug)]
pub struct MomSnap {
	pub fast: f64,
	/// `None` when `indies.momentum.slow` names no second leg: the slot is never created, so there is
	/// no window to compute from.
	pub slow: Option<f64>,
}

impl Flat for MomSnap {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		// An unconfigured slow leg has no value, which is the empty slot the flattening already spells.
		out.copy_from_slice(&[self.fast, self.slow.unwrap_or(f64::NAN)]);
		true
	}
}
structural_bump!(MomSnap);

impl Glance for MomSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let cfg = strategy().indies.momentum;
		write!(f, "{} {:.2}", cfg.fast, self.fast)?;
		match (cfg.slow, self.slow) {
			(Some(tf), Some(x)) => write!(f, " {tf} {x:.2}"),
			_ => Ok(()),
		}
	}
}

/// Sharpe over a window of `lookback + 1` closes, per `bullmart_sri.pine`. `None` when stdev is
/// zero — all returns identical is degenerate, not corrupt.
fn sharpe(window: &[Bar]) -> Option<f64> {
	let n = window.len() - 1;
	let ret = |w: &[Bar]| {
		assert!(w[0].close > 0.0, "non-positive close inside window");
		(w[1].close - w[0].close) / w[0].close
	};
	let mean = window.windows(2).map(ret).sum::<f64>() / n as f64;
	let var = window.windows(2).map(|w| (ret(w) - mean).powi(2)).sum::<f64>() / n as f64;
	// Pine: `stdev * sqrt(lookback)` — non-standard, kept verbatim.
	let stdev_ann = var.sqrt() * (n as f64).sqrt();
	if stdev_ann == 0.0 {
		return None;
	}
	Some((mean * PINE_PERIODS_PER_YEAR - strategy().indies.momentum.risk_free_rate) / stdev_ann)
}

/// Sharpe on the timeframe `indies.momentum.fast` names, plus an optional slower leg. Both wired
/// series are candidate inputs, which is why both are deps; the config picks which is which.
#[derive(Clone, Default)]
pub struct Momentum {
	buf: Vec<Option<MomSnap>>,
}
impl Cell for Momentum {
	type Out<'t> = &'t [Option<MomSnap>];
}
impl Node for Momentum {
	type Deps = (Buffering<Bar5m, { mom_cap(Bar5m::TF) }>, Buffering<Bar4h, { mom_cap(Bar4h::TF) }>);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["fast", "slow"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (m5, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let cfg = strategy().indies.momentum;
		let n = cfg.lookback + 1;
		// Only these two series are retained a momentum window deep, so naming any other timeframe is a
		// config bug rather than a leg that silently never fills.
		let series = |tf| match tf {
			Bar5m::TF => m5,
			Bar4h::TF => h4,
			_ => panic!(
				"indies.momentum names {tf}, over which no {n}-bar window is retained — the graph buffers {} and {}",
				Bar5m::TF,
				Bar4h::TF
			),
		};
		// the lookback is a runtime knob, so the window is narrowed off the retained reach here.
		let fast = series(cfg.fast).narrowed(Horizon::Elems(n));
		let slow = cfg.slow.map(|tf| (tf, series(tf)));
		for (b, w) in fast.fresh().iter().zip(fast.trailing()) {
			let slow = slow.map(|(tf, s)| {
				let closed = closed_by(s.all(), tf, b.close_ns(cfg.fast));
				(closed.len() >= n).then(|| &closed[closed.len() - n..]).and_then(sharpe)
			});
			// A degenerate (zero-stdev) window skips the publish rather than fabricating a Sharpe.
			self.buf.push(match (w.and_then(sharpe), slow) {
				(Some(fast), None) => Some(MomSnap { fast, slow: None }),
				(Some(fast), Some(Some(slow))) => Some(MomSnap { fast, slow: Some(slow) }),
				_ => None,
			});
		}
		&self.buf
	}
}
slice_nudge!(Momentum, Option<MomSnap>);
