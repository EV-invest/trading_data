use core::fmt;

use trading_data::{Cell, DepOuts, Flat, Glance, McRoot, Node, slice_nudge};

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
pub struct MarketCap {
	buf: Vec<Option<McSnap>>,
}
impl Cell for MarketCap {
	type Out<'t> = &'t [Option<McSnap>];
}
impl Node for MarketCap {
	type Deps = (McRoot,);

	fn advance<'t>(&'t mut self, (mcs,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for m in mcs {
			self.buf.push(Some(McSnap {
				market_cap: m.market_cap,
				rank: m.rank,
			}));
		}
		&self.buf
	}
}
slice_nudge!(MarketCap, Option<McSnap>);
