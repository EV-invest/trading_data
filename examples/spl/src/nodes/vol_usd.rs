use trading_data::{Cell, Flat, ScanOuts, Scans, Slots, Stamped, Tag, Timeframe, Vars, node, slice_nudge};

/// A closed bar's notional, `volume * close` — the close standing in for vwap, as SPL's own volume
/// indie does. The period is a parameter, as [`trading_data::Bars`]'s is; a consumer clocked faster
/// than `TF` names `Sampling<VolUsd<TF>>` for the level standing at its own clock.
#[derive(Clone, Default)]
pub struct VolUsd<const TF: Timeframe>;
impl<const TF: Timeframe> VolUsd<TF> {
	const TAG: Tag = Tag::new("VolUsd:", TF);
}
impl<const TF: Timeframe> Cell for VolUsd<TF> {
	type Out<'t> = &'t [f64];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Scans for VolUsd<TF> {
	type Deps = (trading_data::Bars<TF>,);

	fn read((bars,): &ScanOuts<'_, Self>, i: usize, env: &mut [f64]) -> Option<i64> {
		bars[i].flat(env);
		Some(bars[i].ts_ns())
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (close, vol_base) = (v.get::<3>(), v.get::<4>());
		vol_base * close
	}
}
slice_nudge!([const TF: Timeframe] VolUsd<TF>, f64);
