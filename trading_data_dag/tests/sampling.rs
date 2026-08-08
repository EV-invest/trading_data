//! `Latest`/`Sampling` through `graph!`: the level a consumer clocked by *another* series stands on,
//! and the two ways the run it samples can say nothing — an empty batch, and a batch of items
//! carrying their own absence. Neither may unseat what is held.

use trading_data_dag::{
	Blind, Buffering, Bump, Cell, DepOuts, Elems, Fire, Flat, Glance, Latest, Observer, RunOuts, Runs, Sampling, Stamped, Want, always_present, graph, node, slice_nudge, value_nudge,
};

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

	fn emit(&mut self, (src,): RunOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
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

/// A level is a frame cell of its own, so it takes a name of its own — composed from its source's,
/// which is how every other card in the graph spells the same series.
#[test]
fn a_level_names_itself_from_its_source() {
	assert_eq!(<Latest<Sparse> as Cell>::NAME, format!("Latest<{}>", Sparse::NAME));
	assert!(G::NODES.contains(&"Latest<Sparse>"), "the sampled series is a node of the frame: {:?}", G::NODES);
	assert!(G::NODES.contains(&"Latest<Src>"), "one level per series, source and derived alike: {:?}", G::NODES);
}

/// What a sampler reads is the level, not the series behind it. An observer told otherwise draws a
/// graph the sweep does not have: the source looks read by its consumer directly, so it inherits
/// that consumer's gates instead of the level's, and the level itself looks read by nobody.
#[test]
fn a_sampler_is_observed_reading_the_level() {
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
	let level = <Latest<Sparse> as Cell>::NAME;

	assert_eq!(of(level), [Sparse::NAME], "a level reads the series it holds");
	assert_eq!(of(Sampled::NAME), [Clk::NAME, level]);
	// the long spelling is unchanged: a `Buffer` is drawn on its source's own card, so the dep names
	// the series and the retention is read off `Cell::REACH` beside it.
	assert_eq!(of(Windowed::NAME), [Clk::NAME, Src::NAME]);
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

/// A level is a node too, and a `Sampling` dep is the one edge that resolves against something other
/// than the cell it names.
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
	│ │ ╰
	│ │ ● Latest<Sparse>               latest  pin·fold  ⟳Sparse@Unbounded  Opaque("holding the last value across a silence is a carry, and a carry has no slope of its own")
	│ ├─╯
	│ │ ● Sampled                      node  pin·output  ·Latest<Sparse>  →out sampled  Opaque("a sampling fixture")
	├─┼─╮
	│ │ ● Buffer<Src, Elems(1)>        buffer  pin·retention  ⟳Src@Elems(1)  Opaque("a retention window is the engine's bookkeeping over a run, not a function of its elements")
	│ ├─╯
	│ │ ● Windowed                     node  pin·output  ⌸@Elems(1)  →out windowed  Opaque("a sampling fixture")
	╰ │
	● │ Latest<Src>                  latest  pin·fold  ⟳Src@Unbounded  Opaque("holding the last value across a silence is a carry, and a carry has no slope of its own")
	╰─╯
	● Whole                        node  pin·output  ·Latest<Src>  →out whole  Opaque("a sampling fixture")

	legend  ╷root ●live ░dark ⟲needs-a-rewinding-past  ⊣gating ⟳folding ⌸buffering ·sampling
	"#);
}
