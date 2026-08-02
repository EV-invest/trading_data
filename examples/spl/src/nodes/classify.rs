use core::fmt;

use trading_data::{Buffering, Bump, Cell, DepOuts, Flat, Gating, Glance, Horizon, Ink, McRoot, Node, OiRoot, Plot, value_nudge};

use super::{
	Bar1m, Bar4h, Bar5m, Bar15m, Screener,
	change_3m::SPAN_3M,
	momentum::{self, mom_cap},
	oi_delta::OI_REACH,
};

/// The wire order of [`Classified`]'s slots, category-major.
const CATEGORIES: [Category; 5] = [Category::None, Category::Liquidations, Category::MmClosing, Category::Manipulation, Category::Momentum];
const QUALITIES: [Quality; 5] = [Quality::A, Quality::B, Quality::C, Quality::D, Quality::E];
const OUTCOMES: usize = CATEGORIES.len() * QUALITIES.len();
/// The traits answer *which* situation, never how good it would be, so grading is not something
/// this classifier can currently do at all — the value is pinned and every share lands in one
/// column. Held rather than dropped: sizing reads the quality, and a distribution with no quality
/// axis has nowhere to put the grader when it arrives.
//TODO: grade the hit.
const PINNED: Quality = Quality::A;
/// The momentum bands, as they stood before there was anything else to read.
const MOM_HIGH_BAND: f64 = 3.0;
const MOM_MID_BAND: f64 = 2.0;
/// Above this a pump is the market moving, not an instrument being worked.
const LARGE_CAP: f64 = 500e6;
/// A cascade is a drop, not a move: the liquidations trait wants direction, where the OI ratio it
/// pairs with is signless.
const CASCADE_DROP: f64 = -7.0;
/// The traits reading open interest and market cap weigh double the momentum bands: they see the
/// position stack the bands can only infer from price.
const TRAITS: &[Trait] = &[
	Trait {
		category: Category::None,
		relevance: 1,
		hits: |s| s.momentum.is_none(),
	},
	Trait {
		category: Category::Manipulation,
		relevance: 1,
		hits: |s| s.momentum.is_some_and(|m| m.abs() > MOM_HIGH_BAND),
	},
	Trait {
		category: Category::Liquidations,
		relevance: 1,
		hits: |s| s.momentum.is_some_and(|m| m.abs() > MOM_MID_BAND && m.abs() <= MOM_HIGH_BAND),
	},
	Trait {
		category: Category::MmClosing,
		relevance: 1,
		hits: |s| s.momentum.is_some_and(|m| m.abs() <= MOM_MID_BAND),
	},
	Trait {
		category: Category::Momentum,
		relevance: 2,
		hits: |s| s.market_cap.is_some_and(|mc| mc > LARGE_CAP),
	},
	Trait {
		category: Category::Manipulation,
		relevance: 2,
		hits: |s| matches!((s.oi_value, s.market_cap), (Some(oi), Some(mc)) if oi > mc),
	},
	Trait {
		category: Category::Liquidations,
		relevance: 2,
		hits: |s| matches!((s.oi_value, s.market_cap), (Some(oi), Some(mc)) if oi > mc / 3.0) && s.change_15m.is_some_and(|c| c < CASCADE_DROP),
	},
];
const LABELS: [&str; OUTCOMES] = [
	"None A", "None B", "None C", "None D", "None E", //
	"Liquidations A", "Liquidations B", "Liquidations C", "Liquidations D", "Liquidations E", //
	"MmClosing A", "MmClosing B", "MmClosing C", "MmClosing D", "MmClosing E", //
	"Manipulation A", "Manipulation B", "Manipulation C", "Manipulation D", "Manipulation E", //
	"Momentum A", "Momentum B", "Momentum C", "Momentum D", "Momentum E",
];
/// Quality darkens within its category's run, as it does in SPL's own chart. The hue is the
/// renderer's — one per slot — so the category reads off that.
const INKS: [Ink; OUTCOMES] = {
	let mut inks = [Ink::MAIN; OUTCOMES];
	let mut i = 0;
	while i < OUTCOMES {
		let k = 1.0 - 0.2 * (i % QUALITIES.len()) as f64;
		inks[i] = Ink {
			l: Ink::MAIN.l * k,
			c: Ink::MAIN.c * k,
			a: Ink::MAIN.a,
		};
		i += 1;
	}
	inks
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
	None,
	Liquidations,
	MmClosing,
	Manipulation,
	Momentum,
}

/// How good the situation is *given the category is right* — an axis of its own, orthogonal to the
/// certainty that it is. Size scales exactly exponentially.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quality {
	A,
	B,
	C,
	D,
	E,
}

/// SPL's `ProbabilisticDistribution<Classification>` — a probability per `(category, quality)`,
/// totalling 1. Dense where SPL is sparse: the flattening has to place every slot regardless, so an
/// outcome the classifier never names is the zero it already draws as.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Classified(pub [f64; OUTCOMES]);
impl Classified {
	fn vote(s: &Situation) -> Self {
		let mut w = [0.0; CATEGORIES.len()];
		for t in TRAITS {
			if (t.hits)(s) {
				w[CATEGORIES.iter().position(|c| *c == t.category).expect("CATEGORIES is the wire order")] += f64::from(t.relevance);
			}
		}
		let total: f64 = w.iter().sum();
		assert!(total > 0.0, "the None trait votes on exactly the reads no other trait can be evaluated against");
		let q = QUALITIES.iter().position(|x| *x == PINNED).expect("QUALITIES is the wire order");
		let mut p = [0.0; OUTCOMES];
		for (c, x) in w.iter().enumerate() {
			p[c * QUALITIES.len() + q] = x / total;
		}
		Self(p)
	}
}

/// The seat of the classification subtree, and the anchor it hangs off: [`Screener`] is its gate, so
/// everything that grows here is dormant on the ticks nothing fired, and `shadowed` will force each
/// new node onto that same gate as it appears. The rest of the deps are the situation the traits
/// read — buffered rather than folded, because a gated node cannot own a fold it would miss the
/// ticks of.
//TODO: real selection over the full distribution.
#[derive(Clone, Copy, Default)]
pub struct Classify;
/// What the traits are read against. Every field is optional because every one of them can be
/// genuinely absent at a hit — no market-cap publish yet, no open-interest lane, a momentum window
/// still cold — and a trait that cannot be evaluated is a trait that does not vote.
struct Situation {
	momentum: Option<f64>,
	market_cap: Option<f64>,
	/// Bybit reports open interest in base coin; the market cap is USD, so the comparison needs it
	/// valued.
	oi_value: Option<f64>,
	change_15m: Option<f64>,
}

/// One named situation, and what its presence is worth. `relevance` is a bare weight rather than a
/// probability: the distribution falls out of normalising the per-category sums, so these numbers
/// only have to be right against each other.
struct Trait {
	category: Category,
	relevance: u8,
	hits: fn(&Situation) -> bool,
}

impl Flat for Classified {
	const DIMS: &'static [usize] = &[CATEGORIES.len(), QUALITIES.len()];

	fn flat(&self, out: &mut [f64]) -> bool {
		self.0.flat(out)
	}
}

impl Bump for Classified {
	fn bump(self, slot: usize, h: f64) -> (Self, f64) {
		let (p, dh) = self.0.bump(slot, h);
		(Self(p), dh)
	}
}

impl Glance for Classified {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let (i, p) = self.0.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)).expect("OUTCOMES > 0");
		write!(f, "{:?}/{:?} {:.0}%", CATEGORIES[i / QUALITIES.len()], QUALITIES[i % QUALITIES.len()], p * 100.0)
	}
}

impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = (
		Gating<Screener>,
		Buffering<Bar1m, { Horizon::Span(SPAN_3M) }>,
		Buffering<Bar15m, { Horizon::Span(Bar15m::TF) }>,
		Buffering<Bar5m, { mom_cap(Bar5m::TF) }>,
		Buffering<Bar4h, { mom_cap(Bar4h::TF) }>,
		Buffering<McRoot, { Horizon::Elems(1) }>,
		Buffering<OiRoot, OI_REACH>,
	);

	/// The out is a distribution, so the slots stack to a full bar and the scale is fixed to it.
	const PLOTS: &'static [Plot] = &[Plot {
		range: Some((0.0, 1.0)),
		labels: &LABELS,
		inks: &INKS,
		solo: true,
		bars: true,
		..Plot::DEFAULT
	}];

	fn advance<'t>(&'t mut self, (hit, m1, m15, m5, h4, mc, oi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hit, "a gating dep reads true inside `advance`");
		let market_cap = mc.all().last().map(|m| m.market_cap);
		Some(Classified::vote(&Situation {
			momentum: momentum::standing(m5, h4),
			market_cap,
			// the freshest close there is: the ratio against a USD market cap is the reading, and a
			// stale leg of it would move the threshold rather than the measurement.
			oi_value: oi.all().last().zip(m1.all().last()).map(|(o, b)| o.oi * b.close),
			// the last *closed* 15m bar, so the span the change is over is exactly the one named.
			change_15m: m15.all().last().and_then(|b| (b.open > 0.0).then(|| (b.close - b.open) / b.open * 100.0)),
		}))
	}
}
value_nudge!(Classify);
