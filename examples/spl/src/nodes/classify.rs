use core::fmt;

use trading_data::{Cell, DepOuts, Gating, Glance, Node, Plot, value_nudge};

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

/// The seat of the classification subtree, and the anchor it hangs off: [`Screener`] is its one
/// input and its gate, so everything that grows here is dormant on the ticks nothing fired, and
/// `shadowed` will force each new node onto that same gate as it appears. It reads the hit as
/// permission and nothing else — SPL's `ScreenerMeta` (the firing context the real selection wants)
/// arrives when the classifier does.
//TODO: real selection over the full distribution.
#[derive(Clone, Copy, Default)]
pub struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = (Gating<Screener>,);

	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 1.0)),
		bars: true,
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (hit,): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hit, "a gating dep reads true inside `advance`");
		Some(Classified {
			probability: 1.0,
			category: Category::None,
			quality: Quality::A,
		})
	}
}
value_nudge!(Classify);
