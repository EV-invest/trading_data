//! The outputs-plane `Moved` reading (`r[outs.moved.outputs-plane]`): a level output reports
//! whether this tick moved its flattening — absence transitions included, republished values
//! excluded — and a run output stays the bare slice it always was.

use trading_data_dag::{Blind, Cell, DepOuts, Runs, graph, node, slice_nudge, value_nudge};

struct Src;
impl Cell for Src {
	type Out<'t> = &'t [f64];
}
slice_nudge!(Src, f64);

/// Quantizes the last element to its floor, holds through empty batches, and declines on a
/// negative — one level whose value can repeat, move, and go absent on demand.
#[derive(Clone, Default)]
struct Quant {
	last: Option<f64>,
}
impl Cell for Quant {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Quant {
	type Deps = (Src,);

	const WHY: &'static str = "a moved fixture";

	fn advance<'t>(&'t mut self, (src,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if let Some(x) = src.last() {
			self.last = (*x >= 0.0).then(|| x.floor());
		}
		self.last
	}
}
value_nudge!(Quant);

#[derive(Clone, Default)]
struct Echo;
impl Cell for Echo {
	type Out<'t> = &'t [f64];
}
#[node]
impl Runs for Echo {
	type Deps = (Src,);

	const WHY: &'static str = "a moved fixture";

	fn emit(&mut self, (src,): DepOuts<'_, Self>, out: &mut Vec<f64>) {
		out.extend(src);
	}
}
slice_nudge!(Echo, f64);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64] };
	out GOut;
	outputs { level: Quant, echo: Echo }
}

/// One batch per tick in, the level output's `(out, moved)` reading per tick back.
fn check(ticks: &[&[f64]], want: &[(Option<f64>, bool)]) {
	let mut g = G::default();
	let got: Vec<(Option<f64>, bool)> = ticks
		.iter()
		.enumerate()
		.map(|(i, batch)| {
			let o = g.tick(i as i64 + 1, Batches { src: batch });
			let echoed: &[f64] = o.echo; // a run output stays a bare slice
			assert_eq!(echoed, *batch);
			(o.level.out, o.level.moved)
		})
		.collect();
	assert_eq!(got, want, "per tick: (standing out, moved)");
}

#[test]
fn moved_is_the_value_edge() {
	check(
		&[
			&[],     // nothing ever stood
			&[1.2],  // first value
			&[1.7],  // republished quantum: the node ran, the value did not move
			&[2.3],  // moved
			&[],     // held through a quiet tick
			&[-5.0], // went absent — a move `fired` would not report
			&[-6.0], // still absent
			&[2.0],  // and back
		],
		&[
			(None, false),
			(Some(1.0), true),
			(Some(1.0), false),
			(Some(2.0), true),
			(Some(2.0), false),
			(None, true),
			(None, false),
			(Some(2.0), true),
		],
	);
}

#[test]
fn moved_rides_the_out() {
	let mut g = G::default();
	let o = g.tick(1, Batches { src: &[3.9] });
	// the deref reading: `Moved` is the out plus one bit, not a second shape to destructure
	assert_eq!((*o.level, o.level.moved), (Some(3.0), true));
}
