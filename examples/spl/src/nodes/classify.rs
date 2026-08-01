use core::fmt;

use trading_data::{Cell, DepOuts, Glance, Horizon, Node, value_nudge};

use super::{rsi_screener::RsiScreener, std_screener::StdScreener};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
	None,
	Liquidations,
	MmClosing,
	Manipulation,
}
/// Size scales exactly exponentially.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quality {
	A,
	B,
	C,
	D,
	E,
}
/// SPL's `ClassificationActor::classify` is still a stub returning one outcome at 100%; ported as
/// it stands rather than invented over.
#[derive(Clone, Copy, Debug)]
pub struct Classified {
	pub probability: f64,
	pub category: Category,
	pub quality: Quality,
}

flat_fields!(Classified[probability]);

impl Glance for Classified {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?}/{:?} {:.0}%", self.category, self.quality, self.probability * 100.0)
	}
}

/// SPL runs exactly one screener per config; both are wired here, so a hit on either classifies.
/// Classification is a per-hit act rather than a series, so the ticks nothing fired on publish
/// `None`, which is the same channel a gate would have latched.
#[derive(Clone, Copy, Default)]
pub struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = (RsiScreener, StdScreener);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (rsi, std): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert_eq!(rsi.len(), std.len(), "RsiScreener/StdScreener rate mismatch");
		rsi.iter().chain(std).any(|v| *v == Some(true)).then_some(Classified {
			probability: 1.0,
			category: Category::None,
			quality: Quality::A,
		})
	}
}
value_nudge!(Classify);
