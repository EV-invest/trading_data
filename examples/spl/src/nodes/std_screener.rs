use trading_data::{Cell, DepOuts, Folding, Horizon, Node, slice_nudge};

use super::{bar::Bar1m, latest, momentum::Momentum};
use crate::config::{Screen, strategy};

/// Pine's overvalued zone at momentum's leg.
#[derive(Clone, Default)]
pub struct StdScreener {
	momentum: Option<f64>,
	buf: Vec<Option<bool>>,
}
impl Cell for StdScreener {
	type Out<'t> = &'t [Option<bool>];
}
impl Node for StdScreener {
	/// The cached momentum level stands until the next publish, however many minutes that takes.
	type Deps = (Bar1m, Folding<Momentum, { Horizon::Unbounded }>);

	fn advance<'t>(&'t mut self, (bars, momentum): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		// Inert unless configured, but still rate-preserving: `Classify` reads the two screeners as one
		// signal, so an empty slice here would read as a rate mismatch rather than as "no hits".
		let Screen::Std(c) = strategy().screen else {
			self.buf.resize(bars.len(), None);
			return &self.buf;
		};
		latest(&mut self.momentum, momentum, bars.len());
		self.buf.resize(bars.len(), self.momentum.map(|m| m > c.fast_overvalued));
		&self.buf
	}
}
slice_nudge!(StdScreener, Option<bool>);
