//! A partial reading names a field of the item it was taken off. Numbering one is what let a body
//! claim a column that describes a different quantity, so `#[derive(Item)]` emits the fields and the
//! item travels with them.
use trading_data_dag::{Cell, DepReads, Env, Scans, Slots, Vars, Witness, slice_nudge};
use trading_data_macros::{Item, node};

/// The derive stamps through the field's own type, and this suite has no timestamp crate.
#[derive(Clone, Copy, Debug)]
struct Ns(i64);
impl Ns {
	const fn from_nanos(ns: i64) -> Self {
		Self(ns)
	}

	const fn as_nanos(self) -> i64 {
		self.0
	}
}

#[derive(Clone, Copy, Debug, Item)]
struct Level {
	#[stamp]
	at: Ns,
	#[slot]
	bid: f64,
	#[slot]
	ask: f64,
}

#[derive(Clone, Copy, Debug, Item)]
struct Trade {
	#[stamp]
	at: Ns,
	#[slot]
	price: f64,
}

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Level];
}
slice_nudge!(Src, Level);

#[derive(Clone, Default)]
struct Mid;
impl Cell for Mid {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Mid, f64);
#[node]
impl Scans for Mid {
	type Deps = (Src,);

	fn read<W: Witness>((src,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let level = src.at(i)?;
		env.put(level.slot(Trade::PRICE)); //~ ERROR: mismatched types
		Some(level.at.as_nanos())
	}

	fn body(&self, v: Vars) -> impl Slots {
		v.get::<0>()
	}
}

fn main() {}
