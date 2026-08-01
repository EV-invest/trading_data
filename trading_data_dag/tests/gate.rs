//! Gate semantics at the engine boundary: a closed gate skips the gated node's `advance`
//! entirely, while its unbounded deps keep warming. Plus the [`graph!`] completeness rule
//! ([`shadowed`]) over hand-built metas.

use trading_data_dag::{Cell, Cons, DepOuts, Gate, Horizon, Nil, Node, NodeMeta, shadowed, step};

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
	type Deps = (Hist,);
	type When = (Hot,);

	const HORIZON: Horizon = Horizon::Unit;

	fn advance<'t>(&'t mut self, (hist,): DepOuts<'t, Self>) -> Self::Out<'t> {
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

// the validator must run in const position — that's where graph! calls it.
const LEAF: NodeMeta = NodeMeta {
	name: "leaf",
	deps: &[],
	horizon: Horizon::Unit,
	gates: &[],
	latch: false,
};
const _: () = assert!(!shadowed("leaf", &[LEAF]));

#[test]
fn shadowed_flags_exactly_the_bounded_ungated_all_consumers_one_gate_case() {
	let nodes = &[
		NodeMeta {
			name: "hist",
			deps: &["root"],
			horizon: Horizon::Unbounded,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "gate",
			deps: &["hist"],
			horizon: Horizon::Unbounded,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "cur",
			deps: &["root"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "mixed",
			deps: &["root"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "cls",
			deps: &["cur", "hist", "mixed"],
			horizon: Horizon::Unit,
			gates: &["gate"],
			latch: false,
		},
		NodeMeta {
			name: "sink",
			deps: &["mixed"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
	];
	let check = |name, expect| assert_eq!(shadowed(name, nodes), expect, "{name}");

	check("cur", true); // bounded, ungated, only consumer sits behind "gate"
	check("hist", false); // unbounded: must stay warm regardless
	check("cls", false); // gated itself (and a leaf)
	check("gate", false); // leaf: graph output
	check("mixed", false); // one consumer gated, one not — sampling is not wasted
	check("sink", false); // leaf

	// a gate is its own reason to be sampled: consumers gated on X don't shadow X.
	let self_gate = &[
		NodeMeta {
			name: "g",
			deps: &["root"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "c",
			deps: &["g"],
			horizon: Horizon::Unit,
			gates: &["g"],
			latch: false,
		},
	];
	assert!(!shadowed("g", self_gate));

	// no single common gate across consumers.
	let split = &[
		NodeMeta {
			name: "x",
			deps: &["root"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "a",
			deps: &["x"],
			horizon: Horizon::Unit,
			gates: &["g1"],
			latch: false,
		},
		NodeMeta {
			name: "b",
			deps: &["x"],
			horizon: Horizon::Unit,
			gates: &["g2"],
			latch: false,
		},
	];
	assert!(!shadowed("x", split));

	// common gate buried in multi-gate Whens still counts.
	let multi = &[
		NodeMeta {
			name: "x",
			deps: &["root"],
			horizon: Horizon::Unit,
			gates: &[],
			latch: false,
		},
		NodeMeta {
			name: "a",
			deps: &["x"],
			horizon: Horizon::Unit,
			gates: &["g1", "g2"],
			latch: false,
		},
		NodeMeta {
			name: "b",
			deps: &["x"],
			horizon: Horizon::Unit,
			gates: &["g2"],
			latch: false,
		},
	];
	assert!(shadowed("x", multi));
}
