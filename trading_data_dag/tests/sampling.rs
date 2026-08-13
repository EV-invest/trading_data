//! `Sampling` through `graph!`: the level a consumer clocked by *another* series stands on, and the
//! two ways the run it samples can say nothing — an empty batch, and a batch of items carrying their
//! own absence. Neither may unseat what is held.

use trading_data_dag::{Blind, Buffering, Bump, Cell, DepOuts, Elems, Fire, Flat, Glance, Observer, Runs, Sampling, Stamped, Want, always_present, graph, node, slice_nudge, value_nudge};

/// One unit of `v` is one second of `ts`, so a fixture's numbers double as its timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Tick {
	ts: i64,
	v: f64,
}
fn t(v: f64) -> Tick {
	Tick { ts: (v * 1e9) as i64, v }
}
impl Stamped for Tick {
	fn ts_ns(&self) -> i64 {
		self.ts
	}
}
impl Flat for Tick {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.v;
		true
	}
}
impl Bump for Tick {
	fn bump(mut self, _: usize, h: f64) -> (Self, f64) {
		self.v += h;
		(self, h)
	}
}
impl Glance for Tick {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.v)
	}
}
always_present!(Tick);

/// The slow series: publishes on its own schedule.
struct Src;
impl Cell for Src {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Src, Tick);

/// The fast series every consumer below is clocked by.
struct Clk;
impl Cell for Clk {
	type Out<'t> = &'t [Tick];
}
slice_nudge!(Clk, Tick);

/// Declines on a non-positive element: an `Option` item is what a rate-preserving node emits when it
/// has nothing for this bar, and that is an absence, not a level.
#[derive(Clone, Default)]
struct Sparse;
impl Cell for Sparse {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Runs for Sparse {
	type Deps = (Src,);

	const WHY: &'static str = "a sampling fixture";

	fn emit(&mut self, (src,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(src.iter().map(|x| (x.v > 0.0).then_some(x.v)));
	}
}
slice_nudge!(Sparse, Option<f64>);

/// The bug `Sampling` answers: on a tick its own clock fired and `Sparse` did not, the unwrapped dep
/// is the empty run and the reading is silently `None`.
#[derive(Clone, Default)]
struct Naive;
impl Cell for Naive {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Naive {
	type Deps = (Clk, Sparse);

	const WHY: &'static str = "a sampling fixture";

	fn advance<'t>(&'t mut self, (_, sparse): DepOuts<'t, Self>) -> Self::Out<'t> {
		sparse.last().copied().flatten()
	}
}
value_nudge!(Naive);

#[derive(Clone, Default)]
struct Sampled;
impl Cell for Sampled {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Sampled {
	type Deps = (Clk, Sampling<Sparse>);

	const WHY: &'static str = "a sampling fixture";

	fn advance<'t>(&'t mut self, (_, level): DepOuts<'t, Self>) -> Self::Out<'t> {
		level
	}
}
value_nudge!(Sampled);

/// The long spelling `Sampling` replaces where the item carries no absence, kept alongside it: a
/// series is buffered and sampled at once, and the frame holds a cell for each.
#[derive(Clone, Default)]
struct Windowed;
impl Cell for Windowed {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Windowed {
	type Deps = (Clk, Buffering<Src, Elems<1>>);

	const WHY: &'static str = "a sampling fixture";

	fn advance<'t>(&'t mut self, (_, hist): DepOuts<'t, Self>) -> Self::Out<'t> {
		hist.all().last().map(|x| x.v)
	}
}
value_nudge!(Windowed);

/// An item that is its own value: nothing to unwrap, so the level is the last element there was.
#[derive(Clone, Default)]
struct Whole;
impl Cell for Whole {
	type Out<'t> = Option<f64>;
}
#[node]
impl Blind for Whole {
	type Deps = (Clk, Sampling<Src>);

	const WHY: &'static str = "a sampling fixture";

	fn advance<'t>(&'t mut self, (_, level): DepOuts<'t, Self>) -> Self::Out<'t> {
		level.map(|x| x.v)
	}
}
value_nudge!(Whole);

graph! {
	struct G;
	batches Batches;
	roots { src: Src[f64], clk: Clk[u8] };
	out GOut;
	outputs { naive: Naive, sampled: Sampled, windowed: Windowed, whole: Whole }
}

/// A level is storage, not a node: it advances in its source's own sweep line, so it takes no name,
/// no card and no position of its own.
#[test]
fn a_level_is_no_node_of_the_frame() {
	assert_eq!(<Sampling<Sparse> as Cell>::NAME, Sparse::NAME);
	assert!(!G::NODES.iter().any(|n| n.contains("Latest")), "nothing is stepped for the carry: {:?}", G::NODES);
	assert_eq!(G::NODES.len(), 6, "two roots aside, exactly the four authored nodes and the one buffer: {:?}", G::NODES);
}

/// What a sampler is observed reading is the series it samples — which is the edge the sweep has,
/// now that the carry rides in that series' own line.
#[test]
fn a_sampler_is_observed_reading_its_source() {
	#[derive(Default)]
	struct Deps(Vec<(&'static str, Vec<&'static str>)>);
	impl Observer for Deps {
		fn want(&self, _: &'static str) -> Want {
			Want::Vals
		}

		fn on(&mut self, node: &'static str, deps: &'static [&'static str], _: &'static [bool], _: Fire<'_>) {
			self.0.push((node, deps.to_vec()));
		}
	}

	let mut seen = Deps::default();
	G::default().tick_obs(0, Batches { src: &[t(1.0)], clk: &[t(1.0)] }, &mut seen);
	let of = |n: &str| seen.0.iter().find(|(k, _)| *k == n).unwrap_or_else(|| panic!("`{n}` stepped, saw {:?}", seen.0)).1.clone();

	assert_eq!(of(Sampled::NAME), [Clk::NAME, Sparse::NAME]);
	// the long spelling reads the same: a `Buffer` is drawn on its source's own card too, and the
	// retention is read off `Cell::REACH` beside it.
	assert_eq!(of(Windowed::NAME), [Clk::NAME, Src::NAME]);
	assert!(!seen.0.iter().any(|(k, _)| k.contains("Latest")), "no third card: {:?}", seen.0);
}

#[test]
fn a_level_stands_through_the_ticks_its_source_is_silent() {
	let mut g = G::default();
	let (none, one, two, three, four, five, six) = ([], [t(1.0)], [t(2.0)], [t(3.0)], [t(4.0), t(5.0)], [t(5.0)], [t(6.0)]);

	// cold: nothing has been produced, so there is no level to stand on.
	let o = g.tick(0, Batches { src: &none, clk: &one });
	assert_eq!(o.sampled, None);
	assert_eq!(o.whole, None);

	let o = g.tick(0, Batches { src: &two, clk: &two });
	assert_eq!(o.naive, Some(2.0));
	assert_eq!(o.sampled, Some(2.0));

	// the tick the whole thing is about: this consumer's clock fired, its source did not.
	let o = g.tick(0, Batches { src: &none, clk: &three });
	assert_eq!(o.naive, None, "the unwrapped dep is this tick's run, and this tick's run is empty");
	assert_eq!(o.sampled, Some(2.0));
	assert_eq!(o.windowed, Some(2.0), "a one-deep window reaches behind the batch too");
	assert_eq!(o.whole, Some(2.0));

	// the last of a multi-element batch is the level, not the first.
	let o = g.tick(0, Batches { src: &four, clk: &five });
	assert_eq!(o.sampled, Some(5.0));
	let o = g.tick(0, Batches { src: &none, clk: &six });
	assert_eq!(o.sampled, Some(5.0));
	assert_eq!(o.windowed, Some(5.0));
}

/// The other way a run says nothing: it emitted, and every item declined. A level made of those is
/// an absence retained as a value, which is what `Present` exists to refuse.
#[test]
fn a_declining_emission_does_not_unseat_the_level() {
	let mut g = G::default();
	let (two, three, four, declines, mixed) = ([t(2.0)], [t(3.0)], [t(4.0)], [t(-1.0), t(-1.0)], [t(7.0), t(-1.0)]);
	g.tick(0, Batches { src: &two, clk: &two });

	let o = g.tick(0, Batches { src: &declines, clk: &three });
	assert_eq!(o.naive, None, "the newest emission is itself a decline");
	assert_eq!(o.sampled, Some(2.0));
	assert_eq!(o.whole, Some(-1.0), "`Src`'s own items carry no absence: every one of them is a level");

	// mixed: the last *present* one, not the last one.
	let o = g.tick(0, Batches { src: &mixed, clk: &four });
	assert_eq!(o.naive, None);
	assert_eq!(o.sampled, Some(7.0));
}

/// Every edge points at the cell its dep names; what the `·` says is that the read is of the level
/// the engine carries beside that cell rather than of this tick's run.
#[test]
fn shape() {
	insta::assert_snapshot!(G::SHAPE, @r#"
	graph G

	╷ Src                          root src
	│ ╷ Clk                          root clk
	├─┼─╮
	│ │ ● Sparse                       emit  Opaque("a sampling fixture")
	│ ├─├─╮
	│ │ │ ● Naive                        node  pin·output  →out naive  Opaque("a sampling fixture")
	│ ├─╯
	│ │ ● Sampled                      node  pin·output  ·Sparse  →out sampled  Opaque("a sampling fixture")
	├─┼─╮
	│ │ ● Buffer<Src, Elems(1)>        buffer  pin·retention  ⟳Src@Elems(1)  Opaque("a retention window is the engine's bookkeeping over a run, not a function of its elements")
	│ ├─╯
	│ │ ● Windowed                     node  pin·output  ⌸@Elems(1)  →out windowed  Opaque("a sampling fixture")
	╰─╯
	● Whole                        node  pin·output  ·Src  →out whole  Opaque("a sampling fixture")

	legend  ╷root ●live ░dark ⟲needs-a-rewinding-past  ⊣gating ⟳folding ⌸buffering ·sampling
	"#);
}
