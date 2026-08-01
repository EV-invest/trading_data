use trading_data::{Buffering, Cell, DepOuts, Horizon, Node, Plot, slice_nudge};
use v_utils::Timeframe;

use super::bar::{Bar, Bar4h, Bar5m};
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

/// Sharpe on the timeframe `indies.momentum.fast` names. Both wired series are candidate inputs,
/// which is why both are deps; the config picks which one.
///
/// `None` means the one thing it should: the window is not full, or its returns were all identical.
/// Pine's second, slower leg is not ported — see the commit that removed it.
#[derive(Clone)]
pub struct Momentum {
	buf: Vec<Option<f64>>,
}
/// `graph!` builds through `Default` and `main` builds the graph right after `Config::load`, so this
/// is the first instant a config naming a series no momentum window is retained over can be
/// rejected — rather than at the first close of whichever series *is* wired.
impl Default for Momentum {
	fn default() -> Self {
		let tf = strategy().indies.momentum.fast;
		assert!(
			[Bar5m::TF, Bar4h::TF].contains(&tf),
			"indies.momentum.fast = {tf}, over which no window is retained — the graph buffers {} and {}",
			Bar5m::TF,
			Bar4h::TF
		);
		Self { buf: Vec::new() }
	}
}
impl Cell for Momentum {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Momentum {
	type Deps = (Buffering<Bar5m, { mom_cap(Bar5m::TF) }>, Buffering<Bar4h, { mom_cap(Bar4h::TF) }>);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["sharpe"],
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (m5, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let cfg = strategy().indies.momentum;
		let series = match cfg.fast {
			Bar5m::TF => m5,
			Bar4h::TF => h4,
			_ => unreachable!("`Default` asserted the leg against the series this node buffers"),
		};
		// the lookback is a runtime knob, so the window is narrowed off the retained reach here.
		self.buf.extend(series.narrowed(Horizon::Elems(cfg.lookback + 1)).trailing().map(|w| w.and_then(sharpe)));
		&self.buf
	}
}
slice_nudge!(Momentum, Option<f64>);
