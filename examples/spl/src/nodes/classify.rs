use core::fmt;

use trading_data::{Buffering, Bump, Cell, DepOuts, Flat, Gating, Glance, Horizon, Ink, McRoot, Node, OiRoot, Plot, ProbabilisticDistribution, Sampling, Usd, node, value_nudge};

use super::{Bar1m, Bar4h, Bar5m, Change1d, Change3m, Imbalance, Screener, Spread, Volume1h, Volume1m, momentum, oi_delta::OI_REACH};

/// The wire order of [`Classified`]'s slots, category-major.
const CATEGORIES: [Category; 5] = [Category::Indeterminate, Category::Liquidations, Category::MmClosing, Category::Manipulation, Category::Momentum];
const QUALITIES: [Quality; 5] = [Quality::A, Quality::B, Quality::C, Quality::D, Quality::E];
const SLOTS: usize = CATEGORIES.len() * QUALITIES.len();
/// The traits answer *which* situation, never how good it would be, so grading is not something
/// this classifier can currently do at all — the value is pinned and every share lands in one
/// column. Held rather than dropped: sizing reads the quality, and a distribution with no quality
/// axis has nowhere to put the grader when it arrives.
const PINNED: Quality = Quality::A;
const MOM_HIGH_BAND: f64 = 3.0;
const MOM_MID_BAND: f64 = 2.0;
/// Above this a pump is the market moving, not an instrument being worked.
const LARGE_CAP: f64 = 500e6;
/// A cascade is a drop, not a move: the liquidations trait wants direction, where the OI ratio it
/// pairs with is signless.
const CASCADE_DROP: f64 = -7.0;
/// A day already this far extended has had its move.
const EXTENDED_1D: f64 = 20.0;
/// A book this wide and leaning this hard is being worked rather than traded.
const WIDE_SPREAD: f64 = 0.1;
const SKEWED_BOOK: f64 = 0.5;
/// The minute's notional against the standing hour's own per-minute rate.
const VOLUME_SURGE: f64 = 3.0;
/// The traits reading open interest, market cap and the book weigh double the momentum bands: they
/// see the position stack the bands can only infer from price. Nothing votes for
/// [`Category::Indeterminate`] — it is what the points the traits *fail* to score add up to.
const TRAITS: &[Trait] = &[
	Trait {
		category: Category::Manipulation,
		relevance: 1,
		invalidates_others: false,
		hits: |s| s.momentum.is_some_and(|m| m.abs() > MOM_HIGH_BAND),
	},
	Trait {
		category: Category::Liquidations,
		relevance: 1,
		invalidates_others: false,
		hits: |s| s.momentum.is_some_and(|m| m.abs() > MOM_MID_BAND && m.abs() <= MOM_HIGH_BAND),
	},
	Trait {
		category: Category::MmClosing,
		relevance: 1,
		invalidates_others: false,
		hits: |s| s.momentum.is_some_and(|m| m.abs() <= MOM_MID_BAND),
	},
	Trait {
		category: Category::Momentum,
		relevance: 2,
		invalidates_others: true,
		hits: |s| s.market_cap.is_some_and(|mc| mc > LARGE_CAP),
	},
	Trait {
		category: Category::Manipulation,
		relevance: 2,
		invalidates_others: false,
		hits: |s| matches!((s.oi_value, s.market_cap), (Some(oi), Some(mc)) if oi > mc),
	},
	Trait {
		category: Category::Liquidations,
		relevance: 2,
		invalidates_others: false,
		hits: |s| matches!((s.oi_value, s.market_cap), (Some(oi), Some(mc)) if oi > mc / 3.0) && s.change_3m.is_some_and(|c| c < CASCADE_DROP),
	},
	Trait {
		category: Category::Liquidations,
		relevance: 1,
		invalidates_others: false,
		hits: |s| s.change_1d.is_some_and(|c| c.abs() > EXTENDED_1D),
	},
	Trait {
		category: Category::Manipulation,
		relevance: 2,
		invalidates_others: false,
		hits: |s| s.spread.is_some_and(|x| x > WIDE_SPREAD) && s.imbalance.is_some_and(|x| x.abs() > SKEWED_BOOK),
	},
	Trait {
		category: Category::Momentum,
		relevance: 2,
		invalidates_others: false,
		hits: |s| matches!((s.volume_1m, s.volume_1h), (Some(m), Some(h)) if h > 0.0 && m > h / 60.0 * VOLUME_SURGE),
	},
];
const LABELS: [&str; SLOTS] = [
	"Indeterminate A", "Indeterminate B", "Indeterminate C", "Indeterminate D", "Indeterminate E", //
	"Liquidations A", "Liquidations B", "Liquidations C", "Liquidations D", "Liquidations E", //
	"MmClosing A", "MmClosing B", "MmClosing C", "MmClosing D", "MmClosing E", //
	"Manipulation A", "Manipulation B", "Manipulation C", "Manipulation D", "Manipulation E", //
	"Momentum A", "Momentum B", "Momentum C", "Momentum D", "Momentum E",
];
/// Quality darkens within its category's run, as it does in SPL's own chart. The hue is the
/// renderer's — one per slot — so the category reads off that.
const INKS: [Ink; SLOTS] = {
	let mut inks = [Ink::MAIN; SLOTS];
	let mut i = 0;
	while i < SLOTS {
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Category {
	#[default]
	Indeterminate,
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
impl Quality {
	/// Exactly exponential, as the doc above says: one grade down is one `e` less committed.
	pub fn scale(self, max: Usd) -> Usd {
		let steps = QUALITIES.iter().position(|q| *q == self).expect("QUALITIES is the wire order");
		max * (-(steps as f64)).exp()
	}
}

/// SPL's `ProbabilisticDistribution<Classification>` — a probability per `(category, quality)`,
/// totalling 1. Dense where SPL is sparse: the flattening has to place every slot regardless, so an
/// outcome the classifier never names is the zero it already draws as.
#[derive(Clone, Copy, PartialEq)]
pub struct Classified(pub [f64; SLOTS]);
impl Classified {
	fn vote(s: &Situation) -> Self {
		let mut w = [0.0; CATEGORIES.len()];
		for t in TRAITS {
			if !(t.hits)(s) {
				continue;
			}
			let own = CATEGORIES.iter().position(|c| *c == t.category).expect("CATEGORIES is the wire order");
			let r = f64::from(t.relevance);
			for (c, x) in w.iter_mut().enumerate() {
				*x += if c == own {
					r
				} else if t.invalidates_others {
					-r
				} else {
					0.0
				};
			}
		}
		let mut w = w.map(|x| x.max(0.0));
		Self::certainty(&mut w);
		let q = QUALITIES.iter().position(|x| *x == PINNED).expect("QUALITIES is the wire order");
		let mut p = [0.0; SLOTS];
		for (c, x) in w.iter().enumerate() {
			p[c * QUALITIES.len() + q] = *x;
		}
		Self(p)
	}

	/// The likeliest slot and what it is worth — the one read anything acting on the distribution
	/// takes off it.
	pub fn modal(&self) -> (Category, Quality, f64) {
		let (i, p) = self.0.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)).expect("OUTCOMES > 0");
		(CATEGORIES[i / QUALITIES.len()], QUALITIES[i % QUALITIES.len()], *p)
	}
}

/// The seat of the classification subtree, and the anchor it hangs off: [`Screener`] is its gate, so
/// everything that grows here is dormant on the ticks nothing fired. The rest of the deps are the
/// situation the traits read — buffered rather than folded, because a gated node cannot own a fold it
/// would miss the ticks of.
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
	change_1d: Option<f64>,
	change_3m: Option<f64>,
	volume_1m: Option<f64>,
	volume_1h: Option<f64>,
	imbalance: Option<f64>,
	spread: Option<f64>,
}

/// One named situation, and what its presence is worth. `relevance` is a bare point count rather
/// than a probability: the certainty of a hit is scored against the root of every point declared
/// here, so these numbers have to be right against each other *and* against how much this table
/// could ever say.
struct Trait {
	category: Category,
	relevance: u8,
	/// The read argues *against* every other category as hard as it argues for its own — a large cap
	/// is not evidence for momentum so much as evidence the rest are the wrong story. Floored at
	/// zero, so a category talked down past nothing is simply out.
	invalidates_others: bool,
	hits: fn(&Situation) -> bool,
}

/// The quality axis is pinned, so the space the points are scored over is the category one alone;
/// the joint slots the pinned grade never reaches stay the zero they draw as.
impl ProbabilisticDistribution for Classified {
	type Outcome = Category;

	const OUTCOMES: &'static [Category] = &CATEGORIES;
	const POINTS: f64 = {
		let (mut points, mut i) = (0u32, 0);
		while i < TRAITS.len() {
			points += TRAITS[i].relevance as u32;
			i += 1;
		}
		points as f64
	};
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

/// One line per axis instead of the 25 flat slots: the hover tip is where the distribution is
/// read, and a marginal is the only thing a row of raw joint probabilities is ever squinted at for.
impl fmt::Debug for Classified {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Classified\n  Category")?;
		for (c, cat) in CATEGORIES.iter().enumerate() {
			let p: f64 = self.0[c * QUALITIES.len()..][..QUALITIES.len()].iter().sum();
			write!(f, " {cat:?} {:.0}%", p * 100.0)?;
		}
		f.write_str("\n  Quality ")?;
		for (q, qual) in QUALITIES.iter().enumerate() {
			let p: f64 = self.0.iter().skip(q).step_by(QUALITIES.len()).sum();
			write!(f, " {qual:?} {:.0}%", p * 100.0)?;
		}
		Ok(())
	}
}

impl Glance for Classified {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let (c, q, p) = self.modal();
		write!(f, "{c:?}/{q:?} {:.0}%", p * 100.0)
	}
}

impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
#[node]
impl Node for Classify {
	type Deps = (
		Gating<Screener>,
		Bar1m,
		Buffering<Bar5m, { Horizon::Elems(181) }>,
		Buffering<Bar4h, { Horizon::Elems(181) }>,
		Change1d,
		Change3m,
		Volume1m,
		Volume1h,
		Imbalance,
		Spread,
		Sampling<McRoot>,
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

	fn advance<'t>(&'t mut self, (hit, m1, m5, h4, c1d, c3m, v1m, v1h, imb, spr, mc, oi): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hit, "a gating dep reads true inside `advance`");
		Some(Classified::vote(&Situation {
			momentum: momentum::standing(m5, h4),
			market_cap: mc.map(|m| m.market_cap),
			// the freshest close there is: the ratio against a USD market cap is the reading, and a
			// stale leg of it would move the threshold rather than the measurement.
			oi_value: oi.all().last().zip(m1.last()).map(|(o, b)| o.oi * b.close),
			change_1d: c1d.last().copied().flatten(),
			change_3m: c3m.last().copied().flatten(),
			volume_1m: v1m.last().copied(),
			volume_1h: v1h.last().copied().flatten(),
			imbalance: imb.last().copied().flatten(),
			spread: spr.last().copied().flatten(),
		}))
	}
}
value_nudge!(Classify);
