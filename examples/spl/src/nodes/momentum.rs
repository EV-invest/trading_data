use trading_data::{Buffering, Cell, Emit, EmitOuts, Hist, Horizon, Plot, node, slice_nudge};
use v_utils::Timeframe;

use super::{Bar, Bar4h, Bar5m, TF_4H, TF_5MIN};
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

/// Sharpe on the timeframe `indies.momentum.fast` names. Both wired series are candidate inputs,
/// which is why both are deps; the config picks which one.
///
/// `None` means the one thing it should: the window is not full, or its returns were all identical.
/// Pine's second, slower leg is not ported — see the commit that removed it.
#[derive(Clone)]
pub struct Momentum;
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

/// The leg the config names, of the two every reader has to carry as deps.
fn leg<'t>(m5: Hist<'t, Bar>, h4: Hist<'t, Bar>) -> Hist<'t, Bar> {
	match strategy().indies.momentum.fast {
		TF_5MIN => m5,
		TF_4H => h4,
		_ => unreachable!("`Default` asserted the leg against the series this node buffers"),
	}
}

/// The Sharpe standing at this instant rather than one per fresh close — for a reader clocked by
/// something other than the leg, which has no fresh element of its own to hang a window off.
pub(super) fn standing(m5: Hist<'_, Bar>, h4: Hist<'_, Bar>) -> Option<f64> {
	let lookback = strategy().indies.momentum.lookback;
	let all = leg(m5, h4).all();
	(all.len() > lookback).then(|| sharpe(&all[all.len() - lookback - 1..])).flatten()
}

/// `graph!` builds through `Default` and `main` builds the graph right after `Config::load`, so this
/// is the first instant a config naming a series no momentum window is retained over can be
/// rejected — rather than at the first close of whichever series *is* wired.
impl Default for Momentum {
	fn default() -> Self {
		let tf = strategy().indies.momentum.fast;
		assert!(
			[TF_5MIN, TF_4H].contains(&tf),
			"indies.momentum.fast = {tf}, over which no window is retained — the graph buffers {TF_5MIN} and {TF_4H}"
		);
		Self
	}
}
impl Cell for Momentum {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Momentum {
	type Deps = (Buffering<Bar5m, { mom_cap(TF_5MIN) }>, Buffering<Bar4h, { mom_cap(TF_4H) }>);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &["sharpe"],
		..Plot::DEFAULT
	}];

	fn emit(&mut self, (m5, h4): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		// the lookback is a runtime knob, so the window is narrowed off the retained reach here.
		let lookback = strategy().indies.momentum.lookback;
		out.extend(leg(m5, h4).narrowed(Horizon::Elems(lookback + 1)).trailing().map(|w| w.and_then(sharpe)));
	}
}
slice_nudge!(Momentum, Option<f64>);
