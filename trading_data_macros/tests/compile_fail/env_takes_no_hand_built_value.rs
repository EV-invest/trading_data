//! An env slot is a *copy* of one element slot and never a computation of one
//! (`r[kernels.selection.index-is-not-a-variable]`), so the only value it takes is one a dep minted.
//! A number the body built has one way in — `attr` — and what enters that way is claimed by no
//! column.
use trading_data_dag::{Cell, DepReads, Env, Scans, Slots, Vars, Witness, slice_nudge};
use trading_data_macros::node;

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

#[derive(Clone, Default)]
struct Doubled;
impl Cell for Doubled {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Doubled, f64);
#[node]
impl Scans for Doubled {
	type Deps = (Src,);

	fn read<W: Witness>((src,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let x = src.at(i)?;
		env.put(&(*x * 2.0)); //~ ERROR: the trait bound `&f64: Put` is not satisfied
		Some(0)
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<0>()
	}
}

fn main() {}
