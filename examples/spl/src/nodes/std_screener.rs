use trading_data::{Cell, DepOuts, Node, slice_nudge};

use super::{
	bar::Bar1m,
	latest,
	momentum::{MomSnap, Momentum},
};
use crate::config::{Screen, strategy};

/// Pine's overvalued zone at both of momentum's legs. The slow leg is vacuously satisfied when the
/// config names no slow timeframe.
#[derive(Clone, Default)]
pub struct StdScreener {
	momentum: Option<MomSnap>,
	buf: Vec<Option<bool>>,
}
impl Cell for StdScreener {
	type Out<'t> = &'t [Option<bool>];
}
impl Node for StdScreener {
	type Deps = (Bar1m, Momentum);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` reads the two screeners as one
		// signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Std(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.momentum, momentum);
		let two_legged = strategy().indies.momentum.slow.is_some();
		let verdict = self.momentum.map(|m| m.fast > c.fast_overvalued && (!two_legged || m.slow > c.slow_overvalued));
		self.buf.resize(bars.len(), verdict);
		&self.buf
	}
}
slice_nudge!(StdScreener, Option<bool>);
