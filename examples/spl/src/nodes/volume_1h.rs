use trading_data::{Buffering, Cell, DepOuts, Horizon, Node, slice_nudge};

use super::bar::{Bar1h, Bar1m, closed_by};

/// Notional of the newest 1h bar to have closed by each 1m close — the level standing at the
/// screening clock, retained across the minutes the hour publishes nothing.
#[derive(Clone, Default)]
pub struct Volume1h {
	buf: Vec<Option<f64>>,
}
impl Cell for Volume1h {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Volume1h {
	type Deps = (Bar1m, Buffering<Bar1h, { Horizon::Elems(1) }>);

	fn advance<'t>(&'t mut self, (m1, h1): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			self.buf.push(closed_by(h1.all(), b.ts_close).last().map(|h| h.vol_base * h.close));
		}
		&self.buf
	}
}
slice_nudge!(Volume1h, Option<f64>);
