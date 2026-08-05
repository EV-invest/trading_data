use core::fmt;

use trading_data::{Cell, DepOuts, Direction, Flat, Glance, Node, Plot, Usd, node, value_nudge};

use super::classify::{Category, Classified, Classify};
use crate::config::strategy;

/// What to trade and how much — the distribution collapsed to one position. Sizing is quadratic in
/// certainty: a coin-flip modal slot is worth a quarter of a sure one, which is the whole of what
/// "probabilistic" buys over a label.
#[derive(Clone, Copy, Debug)]
pub struct Decided {
	pub direction: Direction,
	pub size: Usd,
}

impl From<Classified> for Decided {
	fn from(c: Classified) -> Self {
		let (category, quality, certainty) = c.modal();
		let direction = match category {
			Category::Liquidations | Category::Manipulation => Direction::Short,
			Category::Momentum => Direction::Long,
			Category::Indeterminate | Category::MmClosing => Direction::Flat,
		};
		Self {
			direction,
			size: quality.scale(strategy().classification.max_size) * certainty.powi(2),
		}
	}
}

impl Flat for Decided {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		let sign = match self.direction {
			Direction::Long => 1.0,
			Direction::Flat => 0.0,
			Direction::Short => -1.0,
		};
		out.copy_from_slice(&[sign, *self.size]);
		true
	}
}
structural_bump!(Decided);

impl Glance for Decided {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?} {:.0}", self.direction, self.size)
	}
}

/// Where the classification becomes a position.
#[derive(Clone, Copy, Default)]
pub struct Decision;
impl Cell for Decision {
	type Out<'t> = Option<Decided>;
}
#[node]
impl Node for Decision {
	type Deps = (Classify,);

	/// Two panes: the direction is a ±1 step and the size is dollars, and one shared scale would
	/// bury the former under the latter.
	const PLOTS: &'static [Plot] = &[
		Plot {
			slots: &[0],
			range: Some((-1.0, 1.0)),
			labels: &[&["direction"]],
			..Plot::DEFAULT
		},
		Plot {
			slots: &[1],
			labels: &[&["size$"]],
			..Plot::DEFAULT
		},
	];

	fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> Self::Out<'t> {
		c.map(Decided::from)
	}
}
value_nudge!(Decision);
