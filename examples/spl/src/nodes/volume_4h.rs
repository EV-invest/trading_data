use trading_data::{Buffering, Cell, DepOuts, Horizon, Node, slice_nudge};

use super::bar::{Bar1m, Bar4h, closed_by};

/// Notional of the newest 4h bar to have closed by each 1m close.
#[derive(Clone, Default)]
pub struct Volume4h {
	buf: Vec<Option<f64>>,
}
impl Cell for Volume4h {
	type Out<'t> = &'t [Option<f64>];
}
impl Node for Volume4h {
	type Deps = (Bar1m, Buffering<Bar4h, { Horizon::Elems(1) }>);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (m1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			self.buf.push(closed_by(h4.all(), Bar4h::TF, b.close_ns(Bar1m::TF)).last().map(|h| h.vol_base * h.close));
		}
		&self.buf
	}
}
slice_nudge!(Volume4h, Option<f64>);
