use trading_data::{Cell, DepOuts, Horizon, Node, slice_nudge};

use super::bar::Bar1m;

/// The closed 1m bar's notional, `volume * close` — the close standing in for vwap, as SPL's own
/// volume indie does. Nothing to warm, so there is no declining.
#[derive(Clone, Default)]
pub struct Volume1m {
	buf: Vec<f64>,
}
impl Cell for Volume1m {
	type Out<'t> = &'t [f64];
}
impl Node for Volume1m {
	type Deps = (Bar1m,);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (m1,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			self.buf.push(b.vol_base * b.close);
		}
		&self.buf
	}
}
slice_nudge!(Volume1m, f64);
