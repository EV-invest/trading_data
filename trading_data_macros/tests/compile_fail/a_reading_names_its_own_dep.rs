//! Which dep a reading came off is the position of the view it was taken from, so an element of dep
//! 0 cannot be handed over as dep 1's. The provenance records what happened, not what was claimed.
use trading_data_dag::{Cell, Dep, DepReads, Env, Scans, Slots, Vars, Witness, slice_nudge};
use trading_data_macros::node;

struct Fast;
impl Cell for Fast {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Fast, f64);

struct Slow;
impl Cell for Slow {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Slow, f64);

#[derive(Clone, Default)]
struct Ratio;
impl Cell for Ratio {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Ratio, f64);
#[node]
impl Scans for Ratio {
	type Deps = (Fast, Slow);

	fn read<W: Witness>((fast, slow): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		env.put(fast.at(i)?);
		// the slow series published nothing this tick, and the fast one's element is no reading of it.
		let as_slow: Dep<1, _> = Dep(fast); //~ ERROR: cannot initialize a tuple struct which contains private fields
		env.put(as_slow.at(i).or(slow.at(i))?);
		Some(0)
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<0>() / v.get::<1>()
	}
}

fn main() {}
