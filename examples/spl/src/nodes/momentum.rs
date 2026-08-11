use trading_data::{Buffering, Cell, Elems, Plot, DepOuts, Runs, Tag, Timeframe, node, slice_nudge};

use super::Bar;
use crate::config::strategy;

/// Sharpe over the trailing `WIN` closes of the `TF` series — `WIN - 1` returns. The window is not a
/// knob, it *is* the reach, `Horizon::Elems(WIN)`; a reader clocked by something else takes
/// `Sampling<Momentum<..>>` rather than re-declaring this dep.
///
/// `None` means the one thing it should: the window is not full, or its returns were all identical.
/// Pine's second, slower leg is not ported — see the commit that removed it.
#[derive(Clone, Default)]
pub struct Momentum<const TF: Timeframe, const WIN: usize>;
impl<const TF: Timeframe, const WIN: usize> Momentum<TF, WIN> {
	const TAG: Tag = Tag::new("Momentum:", TF).count(WIN);
}
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

impl<const TF: Timeframe, const WIN: usize> Cell for Momentum<TF, WIN> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe, const WIN: usize> Runs for Momentum<TF, WIN> {
	type Deps = (Buffering<trading_data::Bars<TF>, Elems<WIN>>,);

	const PLOTS: &'static [Plot] = &[Plot {
		labels: &[&["sharpe"]],
		..Plot::DEFAULT
	}];
	const WHY: &'static str = "a Sharpe over a whole retained window, and no kernel indexes a window: `Scan` reads a point, `Close` a period, `Fold` all history through state. A window \
	                          body would also want one `Var` per retained element, and this one runs at `WIN = 181` against a `MAX_VARS` of 16";

	fn emit(&mut self, (bars,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(bars.trailing().map(|w| w.and_then(sharpe)));
	}
}
slice_nudge!([const TF: Timeframe, const WIN: usize] Momentum<TF, WIN>, Option<f64>);
