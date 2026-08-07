use trading_data::{Buffering, Cell, Env, Over, ScanOuts, Scans, Slots, Stamped, Tag, Timeframe, Vars, Witness, constant, gt, node, select, slice_nudge};

/// Percent change over the trailing `OVER`, off the closed `TF` bars inside it. SPL's backtest mode:
/// reading it off a live Trades window instead is a live-only fidelity choice.
#[derive(Clone, Default)]
pub struct Change<const TF: Timeframe, const OVER: Timeframe>;
impl<const TF: Timeframe, const OVER: Timeframe> Change<TF, OVER> {
	const TAG: Tag = Tag::new("Change:", TF).then(OVER);
}
impl<const TF: Timeframe, const OVER: Timeframe> Cell for Change<TF, OVER> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe, const OVER: Timeframe> Scans for Change<TF, OVER> {
	type Deps = (Buffering<trading_data::Bars<TF>, Over<OVER>>,);

	fn read<W: Witness>((bars,): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (b, lag) = bars.lagged_at(i, 0).expect("element i of this tick's own fresh run");
		env.dep(0).lag(lag).put(b);
		match bars.trailing_at(i) {
			// `open` is slot 0 of a bar, which is the default this leaves unsaid.
			Some((w, lag)) => env.dep(0).lag(lag).put(&w[0].open),
			// an incomplete window declines, and NaN is how it says so.
			None => env.opaque().put(&f64::NAN),
		}
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, base_open) = (v.get::<3>(), v.get::<5>());
		select(gt(base_open, constant(0.0)), (close - base_open) / base_open * constant(100.0), constant(f64::NAN))
	}
}
slice_nudge!([const TF: Timeframe, const OVER: Timeframe] Change<TF, OVER>, Option<f64>);
