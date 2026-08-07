use trading_data::{Carried, Cell, Env, FoldOuts, Folding, Folds, Lagged, Slots, Stamped, Unbounded, Vars, Witness, abs, constant, lt, max, node, select, slice_nudge, wilder};
use v_utils::*;

use crate::config::strategy;

/// Wilder ATR(14) on 1m bars. An indie in its own right rather than an execution-owned indicator:
/// that is what removed SPL's per-situation bar subscribe/unsubscribe flicker.
#[derive(Clone, Default)]
pub struct Atr(Carried);
impl Cell for Atr {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Folds for Atr {
	/// A Wilder recurrence reaches to the start of the run.
	type Deps = (Folding<trading_data::Bars<{ TF_1MIN }>, Unbounded>,);

	/// The previous close, then Wilder's own two: the running mean and how many samples have gone
	/// into it. `warmed == 0` is what stands in for "no previous close", so nothing here needs a
	/// zero that is not zero.
	const STATE: usize = 3;

	fn read<W: Witness>((bars,): &FoldOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let (b, lag) = bars.at(i)?;
		env.dep(0).lag(lag).put(b);
		Some(b.ts_ns())
	}

	fn step(&self, v: Vars) -> impl Slots {
		let (high, low, close) = (v.get::<1>(), v.get::<2>(), v.get::<3>());
		let (prev, value, warmed) = (v.get::<5>(), v.get::<6>(), v.get::<7>());
		let range = high - low;
		// the first bar has no previous close, and a true range against a close that never happened
		// is not a smaller number, it is a different quantity.
		let tr = select(lt(warmed, constant(1.0)), range, max(max(range, abs(high - prev)), abs(low - prev)));
		let n = strategy().indies.atr.period as f64;
		let (next, warmed) = wilder(value, warmed, tr, n);
		(close, next, warmed)
	}

	fn value(&self, v: Vars) -> impl Slots {
		let n = strategy().indies.atr.period as f64;
		select(lt(v.get::<7>(), constant(n)), constant(f64::NAN), v.get::<6>())
	}

	fn carried(&self) -> &Carried {
		&self.0
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.0
	}
}
slice_nudge!(Atr, Option<f64>);
