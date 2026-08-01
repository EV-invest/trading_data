use core::fmt;

use trading_data::{Buffering, Cell, DepOuts, Glance, Horizon, Node, slice_nudge};
use v_utils::Timeframe;

use super::bar::{Bar, Bar1h, Bar1m, Bar4h, closed_by};

#[derive(Clone, Copy, Debug)]
pub struct VolSnap {
	pub volume_1m_usd: f64,
	pub volume_1h_usd: f64,
	pub volume_4h_usd: f64,
}

flat_fields!(VolSnap[volume_1m_usd, volume_1h_usd, volume_4h_usd]);

impl Glance for VolSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "1h ${:.3e}", self.volume_1h_usd)
	}
}

/// Latest closed bar per timeframe, notional as `volume * close`.
#[derive(Clone, Default)]
pub struct Volume {
	buf: Vec<Option<VolSnap>>,
}
impl Cell for Volume {
	type Out<'t> = &'t [Option<VolSnap>];
}
impl Node for Volume {
	// One element: the level standing at each 1m bar's close, retained across the ticks where the
	// slower series emits nothing.
	type Deps = (Bar1m, Buffering<Bar1h, { Horizon::Elems(1) }>, Buffering<Bar4h, { Horizon::Elems(1) }>);

	fn advance<'t>(&'t mut self, (m1, h1, h4): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for b in m1 {
			let usd = |bars: &[Bar], tf: Timeframe| closed_by(bars, tf, b.close_ns(Bar1m::TF)).last().map(|h| h.vol_base * h.close);
			self.buf
				.push(usd(h1.all(), Bar1h::TF).zip(usd(h4.all(), Bar4h::TF)).map(|(volume_1h_usd, volume_4h_usd)| VolSnap {
					volume_1m_usd: b.vol_base * b.close,
					volume_1h_usd,
					volume_4h_usd,
				}));
		}
		&self.buf
	}
}
slice_nudge!(Volume, Option<VolSnap>);
