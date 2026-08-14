//! A lag is how far the element handed over stands behind the one this dep's Jacobian column
//! describes. The dep counts it while it hands the element over, so there is nothing for a site to
//! state — and a stated one could disagree with the element it stands beside.
use trading_data_dag::{Cell, DepReads, Env, Pick, Scans, Slots, Vars, Witness, slice_nudge};
use trading_data_macros::node;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

#[derive(Clone, Default)]
struct Echo;
impl Cell for Echo {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Echo, f64);
#[node]
impl Scans for Echo {
	type Deps = (Src,);

	fn read<W: Witness>((src,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let x = src.at(i)?;
		env.put(Pick::<0, f64> { elem: *x, lag: 7 }); //~ ERROR: fields `elem` and `lag` of struct `Pick` are private
		Some(0)
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<0>()
	}
}

fn main() {}
