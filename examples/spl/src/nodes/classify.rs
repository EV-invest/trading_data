use core::fmt;

use trading_data::{Cell, DepOuts, Glance, Node, value_nudge};

use super::Screener;

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

/// The seat of the classification subtree, and the anchor it hangs off: [`Screener`] is its gate, so
/// everything that grows here is dormant on the ticks nothing fired, and `shadowed` will force each
/// new node onto that same gate as it appears. Reads nothing — the gate *is* the hit.
//TODO: real selection over the full distribution.
#[derive(Clone, Copy, Default)]
pub struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = ();
	type When = (Screener,);

	fn advance<'t>(&'t mut self, (): DepOuts<'t, Self>) -> Self::Out<'t> {
		Some(Classified {
			probability: 1.0,
			category: Category::None,
			quality: Quality::A,
		})
	}
}
value_nudge!(Classify);
