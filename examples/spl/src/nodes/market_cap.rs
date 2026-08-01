use core::fmt;

use trading_data::{Cell, Emit, EmitOuts, Flat, Glance, McRoot, slice_nudge};

#[derive(Clone, Copy, Debug)]
pub struct McSnap {
	pub market_cap: f64,
	pub rank: Option<u32>,
}

impl Flat for McSnap {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.market_cap, self.rank.map_or(f64::NAN, f64::from)]);
		true
	}
}
structural_bump!(McSnap);

impl Glance for McSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:.3e} rank {:?}", self.market_cap, self.rank)
	}
}

#[derive(Clone, Default)]
pub struct MarketCap;
impl Cell for MarketCap {
	type Out<'t> = &'t [Option<McSnap>];
}
impl Emit for MarketCap {
	type Deps = (McRoot,);

	fn emit(&mut self, (mcs,): EmitOuts<'_, Self>, out: &mut Vec<Option<McSnap>>) {
		for m in mcs {
			out.push(Some(McSnap {
				market_cap: m.market_cap,
				rank: m.rank,
			}));
		}
	}
}
slice_nudge!(MarketCap, Option<McSnap>);
