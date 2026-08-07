use trading_data::{Bar, Buffering, Cell, Env, Exact, Lagged, Over, ScanOuts, Scans, Slots, Stamped, Tag, Timeframe, Vars, Witness, abs, closed_by, constant, gt, node, select, slice_nudge};

/// Percent change against the `REF` close standing `BACK` back, asked once per closed `CLK` bar.
#[derive(Clone, Default)]
pub struct ChangeBack<const CLK: Timeframe, const REF: Timeframe, const BACK: Timeframe, const REACH: Timeframe>;
impl<const CLK: Timeframe, const REF: Timeframe, const BACK: Timeframe, const REACH: Timeframe> ChangeBack<CLK, REF, BACK, REACH> {
	/// `REACH` is `BACK + REF`, stated at the wiring site rather than computed from the two: a const
	/// argument doing arithmetic over its impl's own generics wants `generic_const_exprs`, which this
	/// tree does not turn on. An associated const may read those generics, so the tie is checked here
	/// and forced from `read`.
	const REACHES_A_PERIOD_PAST: () = assert!(
		REACH.0 == BACK.0 + REF.0,
		"a `ChangeBack`'s reach is the lookback plus one period of the series it reads: the bar asking the question stands up to a whole period past that series' newest close"
	);
	const TAG: Tag = Tag::new("ChangeBack:", CLK).then(BACK);
}
impl<const CLK: Timeframe, const REF: Timeframe, const BACK: Timeframe, const REACH: Timeframe> Cell for ChangeBack<CLK, REF, BACK, REACH> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const CLK: Timeframe, const REF: Timeframe, const BACK: Timeframe, const REACH: Timeframe> Scans for ChangeBack<CLK, REF, BACK, REACH> {
	/// `BACK` is wall clock, not a bar count: a period nothing traded emits no bar. The retained run
	/// is that lookback plus one period of the buffered series itself.
	type Deps = (trading_data::Bars<CLK>, Buffering<trading_data::Bars<REF>, Over<REACH>>);

	fn read<W: Witness>((clk, refs): &ScanOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let () = Self::REACHES_A_PERIOD_PAST;
		let (b, lag) = clk.at(i)?;
		env.dep(0).lag(lag).put(b);
		let closed = closed_by(refs.all(), b.ts_close);
		let back_to = b.ts_close - Exact::from(BACK.duration());
		// The close standing `BACK` back is the first one after `back_to`; index 0 means the retained
		// run does not reach behind it, so there is nothing that old to compare against yet.
		match closed.as_slice().iter().position(|h| h.ts_close > back_to).filter(|&i| i > 0).and_then(|i| closed.at(i)) {
			Some((h, lag)) => env.dep(1).lag(lag).put(h),
			None => env.opaque().put(&None::<Bar>),
		}
		Some(b.ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, oldest) = (v.get::<3>(), v.get::<8>());
		// `|oldest| > 0` rather than a bare comparison: a zero close divides, and an absent one is NaN,
		// and both are the same decline.
		select(gt(abs(oldest), constant(0.0)), (close - oldest) / oldest * constant(100.0), constant(f64::NAN))
	}
}
slice_nudge!([const CLK: Timeframe, const REF: Timeframe, const BACK: Timeframe, const REACH: Timeframe] ChangeBack<CLK, REF, BACK, REACH>, Option<f64>);
