use trading_data::{Buffering, Cell, Emit, EmitOuts, Hist, Horizon, Plot, node, slice_nudge};
use v_utils::*;

use super::Bar;
use crate::config::strategy;

/// The two series a window is retained over, and so the whole of what `indies.momentum.fast` may
/// name — checked there, in [`crate::config::Config::load`].
pub const LEGS: [Timeframe; 2] = [TF_5MIN, TF_4H];
/// Sharpe on the timeframe `indies.momentum.fast` names. Both wired series are candidate inputs,
/// which is why both are deps; the config picks which one.
///
/// The window is 181 closes — 180 returns — and is not a knob: it *is* the reach,
/// `Horizon::Elems(181)`. A reader clocked by something else takes `Sampling<Momentum>` rather than
/// re-declaring these deps.
///
/// `None` means the one thing it should: the window is not full, or its returns were all identical.
/// Pine's second, slower leg is not ported — see the commit that removed it.
#[derive(Clone, Default)]
pub struct Momentum;
/// Sharpe over the whole retained window, per `bullmart_sri.pine`. `None` when stdev is zero — all
/// returns identical is degenerate, not corrupt.
fn sharpe(window: &[Bar]) -> Option<f64> {
	let n = window.len() - 1;
	let ret = |w: &[Bar]| {
		assert!(w[0].close > 0.0, "non-positive close inside window");
		(w[1].close - w[0].close) / w[0].close
	};
	let mean = window.windows(2).map(ret).sum::<f64>() / n as f64;
	let var = window.windows(2).map(|w| (ret(w) - mean).powi(2)).sum::<f64>() / n as f64;
	// Pine: `stdev * sqrt(lookback)`, and `* 365` regardless of bar timeframe — both non-standard,
	// both kept verbatim, which is why the 4h and 5m Sharpe scales differ.
	let stdev_ann = var.sqrt() * (n as f64).sqrt();
	if stdev_ann == 0.0 {
		return None;
	}
	Some((mean * 365.0 - strategy().indies.momentum.risk_free_rate) / stdev_ann)
}

/// The leg the config names, of the two every reader has to carry as deps.
fn leg<'t>(m5: Hist<'t, Bar>, h4: Hist<'t, Bar>) -> Hist<'t, Bar> {
	let tf = strategy().indies.momentum.fast;
	if tf == LEGS[0] {
		m5
	} else if tf == LEGS[1] {
		h4
	} else {
		unreachable!("`Config::load` checked the leg against the series this node buffers")
	}
}

impl Cell for Momentum {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Momentum {
	type Deps = (
		Buffering<trading_data::Bars<{ TF_5MIN }>, { Horizon::Elems(181) }>,
		Buffering<trading_data::Bars<{ TF_4H }>, { Horizon::Elems(181) }>,
	);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &[&["sharpe"]],
		..Plot::DEFAULT
	}];

	fn emit(&mut self, (m5, h4): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(leg(m5, h4).trailing().map(|w| w.and_then(sharpe)));
	}
}
slice_nudge!(Momentum, Option<f64>);
