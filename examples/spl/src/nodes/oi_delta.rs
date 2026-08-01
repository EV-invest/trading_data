use core::fmt;

use trading_data::{Buffering, Cell, DepOuts, Glance, Horizon, Node, OiRoot, Stamped as _, slice_nudge};
use v_utils::{Timeframe, TimeframeDesignator};

/// Bybit's open-interest publish cadence: the deltas read the publish standing a whole number of
/// these back, so the retained reach is one past the longer leg.
const OI_STEP: Timeframe = Timeframe::from_naive(5, TimeframeDesignator::Minutes);
pub(super) const OI_REACH: Horizon = Horizon::Span(Timeframe(4 * OI_STEP.0));

#[derive(Clone, Copy, Debug)]
pub struct OiSnap {
	pub oi_delta_5m_pct: f64,
	pub oi_delta_15m_pct: f64,
}

flat_fields!(OiSnap[oi_delta_5m_pct, oi_delta_15m_pct]);

impl Glance for OiSnap {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "5m {:+.2}% 15m {:+.2}%", self.oi_delta_5m_pct, self.oi_delta_15m_pct)
	}
}

/// Bybit open interest against the publish standing 5 and 15 minutes back.
#[derive(Clone, Default)]
pub struct OiDelta {
	buf: Vec<Option<OiSnap>>,
}
impl Cell for OiDelta {
	type Out<'t> = &'t [Option<OiSnap>];
}
impl Node for OiDelta {
	type Deps = (Buffering<OiRoot, OI_REACH>,);

	/// Every input is read at a declared reach and nothing is accumulated, so this can be gated.
	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		let step_ns = OI_STEP.duration().as_nanos() as i64;
		for (i, cur) in hist.fresh().iter().enumerate() {
			self.buf.push(hist.trailing_at(i).and_then(|w| {
				// The publish standing `back` before this one. A gap that leaves none within a publish
				// interval of that instant declines, rather than passing a shorter delta off as this one.
				let ago = |back: i64| {
					let target = cur.ts_ns() - back;
					let o = w.iter().rev().find(|o| o.ts_ns() <= target)?;
					// SPL's own zero guard: an OI of exactly zero is a dead contract, reported as no change.
					(target - o.ts_ns() < step_ns).then(|| if o.oi != 0.0 { (cur.oi - o.oi) / o.oi * 100.0 } else { 0.0 })
				};
				Some(OiSnap {
					oi_delta_5m_pct: ago(step_ns)?,
					oi_delta_15m_pct: ago(3 * step_ns)?,
				})
			}));
		}
		&self.buf
	}
}
slice_nudge!(OiDelta, Option<OiSnap>);
