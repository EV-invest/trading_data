//! A body that multiplies by an attribute, read at `Want::Exact`. The attribute picks which way a
//! notional points and owns no column; the element's own slots own theirs, and the block has to say
//! so for a sell exactly as it does for a buy — which is what a reading whose provenance the author
//! could state was free to get wrong.

use trading_data_dag::{Carried, Cell, DepReads, Env, Fire, Folding, Folds, Glance, Item, Observer, Slots, Unbounded, Vars, Want, Witness, graph, node, slice_nudge};

/// The derive stamps through the field's own type, and this crate names no timestamp type.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Ns(i64);
impl Ns {
	const fn from_nanos(ns: i64) -> Self {
		Self(ns)
	}

	const fn as_nanos(self) -> i64 {
		self.0
	}
}

/// `sell` is carried rather than flattened: it is no column's, which is the whole of what the
/// reading below has to keep true.
#[derive(Clone, Copy, Debug, Item, PartialEq)]
struct Fill {
	#[stamp]
	at: Ns,
	#[slot]
	price: f64,
	#[slot]
	qty: f64,
	sell: bool,
}

fn fill(at: i64, price: f64, qty: f64, sell: bool) -> Fill {
	Fill {
		at: Ns::from_nanos(at),
		price,
		qty,
		sell,
	}
}

impl Glance for Fill {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}@{}", self.qty, self.price)
	}
}

struct Fills;
impl Cell for Fills {
	type Out<'t> = &'t [Fill];
}
slice_nudge!(Fills, Fill);

/// Running signed notional — the shape a run carrying both sides is read in.
#[derive(Clone, Default)]
struct Cvd(Carried);
impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
#[node]
impl Folds for Cvd {
	type Deps = (Folding<Fills, Unbounded>,);

	const STATE: usize = 1;

	fn read<W: Witness>((fills,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let f = fills.at(i)?;
		env.put(f);
		env.attr(if f.sell { -1.0 } else { 1.0 });
		Some(f.at.as_nanos())
	}

	fn step(&self, v: Vars) -> impl Slots {
		let (price, qty, sum, side) = (v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>());
		sum + side * (price * qty)
	}

	fn value(&self, v: Vars) -> impl Slots {
		v.get::<2>()
	}

	fn carried(&self) -> &Carried {
		&self.0
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.0
	}
}
slice_nudge!(Cvd, f64);

graph! {
	struct G;
	batches Batches;
	roots { fills: Fills[Fill] };
	out GOut;
	outputs { cvd: Cvd }
}

/// What the block says about the last element of `then`, and what the run itself last published.
#[derive(Default)]
struct Rec {
	block: Option<(Vec<f64>, Vec<usize>)>,
}
impl Observer for Rec {
	fn want(&self, _: &'static str) -> Want {
		Want::Exact
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		if node.ends_with("Cvd") {
			self.block = fire.exact_block.map(|(b, w)| (b.to_vec(), w.to_vec()));
		}
	}
}

/// The warm-up every leg shares: one buy, so the state the reading is differentiated against is not
/// the zero a fresh fold would hand it.
const WARM: [Fill; 1] = [Fill {
	at: Ns::from_nanos(0),
	price: 100.0,
	qty: 3.0,
	sell: false,
}];

fn last_out(then: &[Fill]) -> f64 {
	let mut g = G::default();
	g.tick(0, Batches { fills: &WARM });
	*g.tick(0, Batches { fills: then }).cvd.last().expect("one element per fill")
}

/// The block's `(price, qty)` columns for the last element of `then`, against the difference the
/// same perturbation actually makes.
fn columns_against_difference(then: &[Fill]) {
	let mut g = G::default();
	g.tick(0, Batches { fills: &WARM });
	let mut rec = Rec::default();
	g.tick_obs(0, Batches { fills: then }, &mut rec);
	let (block, widths) = rec.block.expect("a fold that fired answers over its deps' own reach");
	assert_eq!(widths, vec![2], "one lag of one two-slot dep — the attribute owns no column");

	let h = 1e-4;
	let bump = |f: &dyn Fn(&mut Fill)| {
		let mut moved = then.to_vec();
		f(moved.last_mut().expect("a run to differentiate against"));
		(last_out(&moved) - last_out(then)) / h
	};
	for (slot, name, fd) in [(0, "price", bump(&|f| f.price += h)), (1, "qty", bump(&|f| f.qty += h))] {
		assert!(
			(block[slot] - fd).abs() < 1e-6 * fd.abs().max(1.0),
			"the {name} column reads {} where moving {name} moves the out by {fd} per unit",
			block[slot]
		);
	}
}

/// A buy: the side the value plane agrees with whichever way the reading was attributed.
#[test]
fn a_buy_s_columns_are_the_difference_it_makes() {
	columns_against_difference(&[fill(1, 101.0, 2.0, false)]);
}

/// The same run ending in a sell. The attribute flipped, the element's own slots did not, and a
/// column that had followed the sign into the element would come out negated here.
#[test]
fn a_sell_s_columns_are_the_difference_it_makes() {
	columns_against_difference(&[fill(1, 101.0, 2.0, true)]);
}

/// Both sides in one run, so the element the block describes is reached through a state the other
/// side moved.
#[test]
fn a_run_carrying_both_sides_is_read_at_its_last_element() {
	columns_against_difference(&[fill(1, 101.0, 2.0, false), fill(2, 99.0, 5.0, true)]);
	columns_against_difference(&[fill(1, 101.0, 2.0, true), fill(2, 99.0, 5.0, false)]);
}
