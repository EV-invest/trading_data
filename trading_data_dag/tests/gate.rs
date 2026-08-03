//! Gate semantics at the engine boundary: a closed gate skips the gated node's `advance`
//! entirely, while its unbounded deps keep warming.

use trading_data_dag::{Cell, Cons, DepOuts, Gate, Gating, Nil, Node, step};

struct Feed;
impl Cell for Feed {
	type Out<'t> = f64;
}

#[derive(Clone, Default)]
struct Hist {
	calls: u32,
}
impl Cell for Hist {
	type Out<'t> = Option<f64>;
}
impl Node for Hist {
	type Deps = (Feed,);

	fn advance<'t>(&'t mut self, (feed,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.calls += 1;
		Some(feed * 2.0)
	}
}

#[derive(Clone)]
struct Hot;
impl Cell for Hot {
	type Out<'t> = bool;
}
impl Node for Hot {
	type Deps = (Feed,);

	fn advance<'t>(&'t mut self, (feed,): DepOuts<'t, Self>) -> Self::Out<'t> {
		feed > 0.0
	}
}
impl Gate for Hot {}

#[derive(Clone, Default)]
struct Gated {
	calls: u32,
}
impl Cell for Gated {
	type Out<'t> = Option<f64>;
}
impl Node for Gated {
	type Deps = (Gating<Hot>, Hist);

	fn advance<'t>(&'t mut self, (hot, hist): DepOuts<'t, Self>) -> Self::Out<'t> {
		assert!(hot, "a gating dep reads true inside `advance`");
		self.calls += 1;
		hist
	}
}

fn tick(feed: f64, hist: &mut Hist, gated: &mut Gated) -> Option<f64> {
	let mut hot = Hot;
	let f = Cons::<Feed, Nil> { out: feed, tail: Nil };
	let f = step(f, hist);
	let f = step(f, &mut hot);
	let f = step(f, gated);
	f.head()
}

#[test]
fn closed_gate_skips_gated_node_but_not_its_unbounded_dep() {
	let (mut hist, mut gated) = (Hist::default(), Gated::default());

	assert_eq!(tick(-1.0, &mut hist, &mut gated), None);
	assert_eq!((hist.calls, gated.calls), (1, 0));

	assert_eq!(tick(2.0, &mut hist, &mut gated), Some(4.0));
	assert_eq!((hist.calls, gated.calls), (2, 1));

	assert_eq!(tick(-3.0, &mut hist, &mut gated), None);
	assert_eq!((hist.calls, gated.calls), (3, 1));
}
