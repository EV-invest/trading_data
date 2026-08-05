use trading_data::{Buffering, Cell, Emit, EmitOuts, Hist, Horizon, Oi, OiRoot, Stamped as _, node, slice_nudge};
use v_utils::*;

/// `TF_5MIN` is Bybit's open-interest publish cadence: every leg reads the publish standing a whole
/// number of those back, so the retained reach is one past the longest one.
pub const OI_REACH: Horizon = Horizon::Span(Timeframe(4 * TF_5MIN.0));

/// Percent change of the `i`th fresh publish against the one standing `steps` cadences before it. A
/// gap that leaves none within a publish interval of that instant declines, rather than passing a
/// shorter delta off as this one.
fn delta_back(hist: &Hist<'_, Oi>, i: usize, steps: i64) -> Option<f64> {
	let step_ns = TF_5MIN.duration().as_nanos() as i64;
	let cur = &hist.fresh()[i];
	let target = cur.ts_ns() - steps * step_ns;
	let o = hist.trailing_at(i)?.iter().rev().find(|o| o.ts_ns() <= target)?;
	// SPL's own zero guard: an OI of exactly zero is a dead contract, reported as no change.
	(target - o.ts_ns() < step_ns).then(|| if o.oi != 0.0 { (cur.oi - o.oi) / o.oi * 100.0 } else { 0.0 })
}

/// One node per lookback. Independent readings of the same lane, so each declines on its own: a
/// window too short to answer the longer leg says nothing about the shorter one.
macro_rules! oi_deltas {
	($($ty:ident = $steps:literal @ $name:literal),+ $(,)?) => { $(
		#[derive(Clone, Default)]
		pub struct $ty;
		impl Cell for $ty {
			type Out<'t> = &'t [Option<f64>];

			const NAME: &'static str = concat!("OiDelta:", $name);
		}
		#[node]
		impl Emit for $ty {
			type Deps = (Buffering<OiRoot, OI_REACH>,);
			const WHY: &'static str = "element-wise arithmetic over a run, which the run side has no kernel for yet";

			fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
				for i in 0..hist.fresh().len() {
					out.push(delta_back(&hist, i, $steps));
				}
			}
		}
		slice_nudge!($ty, Option<f64>);
	)+ };
}
oi_deltas!(OiDelta5m = 1 @ "5m", OiDelta15m = 3 @ "15m");
