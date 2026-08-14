use trading_data::{Cell, DepReads, Env, Reading, Scans, Slots, Vars, Witness, constant, node, slice_nudge};

use super::book_top::BookTop;

/// Bid-ask spread as a percentage of the bid.
#[derive(Clone, Default)]
pub struct Spread;
impl Cell for Spread {
	type Out<'t> = &'t [Reading];
}
#[node]
impl Scans for Spread {
	type Deps = (BookTop,);

	fn read<W: Witness>((top,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let t = top.at(i)?;
		// a book still filling has one side empty and no top to read: declined before the put, so no
		// absence reaches the body as an operand.
		let ts = t.as_ref()?.ts_ns;
		env.put(t);
		Some(ts)
	}

	fn body(&self, v: Vars) -> impl Slots {
		let (bid, ask) = (v.get::<0>(), v.get::<1>());
		(ask - bid) / bid * constant(100.0)
	}
}
slice_nudge!(Spread, Reading);
