//! **Phase 4 — `observe.noninvasive` and `outs.absence.one-reading`.** Two cheap oracles riding on
//! the phase 2/3 fixture.

use trading_data::{Fire, Observer, Want};

use crate::{
	fixture::{self, G, Outs, Step},
	frng::Frng,
};

pub const VERSION: u32 = crate::corpus::fnv(&[include_str!("frng.rs"), include_str!("fixture.rs"), include_str!("outs.rs")]);

fn trace(f: &mut Frng) -> Vec<Step> {
	let (mut out, mut ts) = (Vec::new(), 0i64);
	while f.remaining() > 3 {
		let n = f.below(4) as usize;
		let ctl = |f: &mut Frng| match f.below(3) {
			0 => Vec::new(),
			1 => vec![fixture::t(ts, -1.0)],
			_ => vec![fixture::t(ts, 1.0)],
		};
		out.push(Step {
			src: (0..n).map(|i| fixture::t(ts + i as i64 * 1_000_000_000, f.span(-50.0, 50.0))).collect(),
			ctl_a: ctl(f),
			ctl_b: ctl(f),
		});
		ts += 1 + f.span(0.0, 90_000_000_000.0) as i64;
	}
	out
}

/// A `Want` per node, drawn once per tick. `Observer::want` takes `&self`, which is what makes an
/// observer's appetite a property of the tick rather than of the order nodes happen to be stepped in
/// — so the draw happens up front and the observer only reads it back.
struct Appetite {
	wants: Vec<(&'static str, Want)>,
	fires: usize,
}

impl Appetite {
	fn draw(f: &mut Frng) -> Self {
		let mut wants: Vec<(&'static str, Want)> = G::SHAPE
			.nodes
			.iter()
			.map(|n| {
				(
					n.name,
					match f.below(4) {
						0 => Want::Nothing,
						1 => Want::Vals,
						2 => Want::Jac,
						_ => Want::Exact,
					},
				)
			})
			.collect();
		// `Nothing` is in the draw because a mixed appetite is the case the per-node `want` exists for
		// — but an all-`Nothing` tick observes nothing, and comparing an unobserved run against an
		// unobserved run is a green check that means nothing. One node is raised where that happens.
		if wants.iter().all(|(_, w)| *w == Want::Nothing)
			&& let Some(first) = wants.first_mut()
		{
			first.1 = Want::Jac;
		}
		Self { wants, fires: 0 }
	}
}

impl Observer for Appetite {
	fn want(&self, node: &'static str) -> Want {
		self.wants.iter().find(|(n, _)| *n == node).map_or(Want::Nothing, |(_, w)| *w)
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], _: Fire<'_>) {
		self.fires += 1;
	}
}

/// Bit-for-bit, which `PartialEq` on `f64` is not: an out that is `NaN` compares unequal to itself,
/// and "identical" here is a claim about the bytes the tick left behind.
fn bits(o: &Outs) -> Vec<u64> {
	let one = |v: Option<f64>| v.map_or(u64::MAX, f64::to_bits);
	let mut out = vec![o.total.len() as u64, o.bucket.len() as u64];
	out.extend(o.total.iter().map(|&v| one(v)));
	out.extend(o.bucket.iter().map(|&v| v.to_bits()));
	out.extend([o.peak, o.win, o.lvl, o.ra, o.rb].map(one));
	out
}

/// `r[observe.noninvasive]`: *"a tick MUST leave the graph in the same state regardless of what was
/// observed during it. The outs of a tick observed at `Want::Jac` MUST be identical, bit for bit, to
/// the outs of the same tick observed at `Want::Nothing`."*
///
/// The finite-difference witness re-advances a clone restored from the pre-advance state and never
/// the node itself, so this holds by construction — which is exactly why it is worth a fuzz target
/// rather than a comment: what is claimed is that no *later* kernel quietly borrows the real node's
/// state for its reading, and only running one can say.
pub fn run_noninvasive(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let steps = trace(f);
	if steps.is_empty() {
		return Ok(());
	}
	let appetites: Vec<Appetite> = steps.iter().map(|_| Appetite::draw(f)).collect();

	let mut unobserved = G::default();
	let quiet: Vec<Outs> = steps.iter().map(|s| fixture::tick(&mut unobserved, s)).collect();

	let mut watched = G::default();
	let mut looked = 0;
	let observed: Vec<Outs> = steps
		.iter()
		.zip(appetites)
		.map(|(s, mut a)| {
			let o = fixture::tick_obs(&mut watched, s, &mut a);
			looked += a.fires;
			o
		})
		.collect();

	if verbose {
		eprintln!("{} ticks, {looked} fires reached the observer", steps.len());
	}
	check!(looked > 0, "the observer was never called, so the comparison is vacuous");

	for (i, (a, b)) in quiet.iter().zip(&observed).enumerate() {
		check!(bits(a) == bits(b), "tick {i}: observing changed the outs\n  Want::Nothing {a:?}\n  observed      {b:?}");
	}
	Ok(())
}

/// Records the one thing `r[outs.absence.one-reading]` is about: how each fire spelled presence.
#[derive(Default)]
struct Readings(Vec<(&'static str, bool, usize, Option<bool>)>);

impl Observer for Readings {
	fn want(&self, _: &'static str) -> Want {
		Want::Vals
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		// `all_nan` is `None` where there are no slots to read, which is the unfired case itself.
		self.0.push((node, fire.ran, fire.fires, fire.vals.map(|v| v.iter().all(|x| x.is_nan()))));
	}
}

/// `r[outs.absence.one-reading]`: *"`None`, the empty batch, and an all-NaN flattening MUST all
/// report unfired... no 'fired, but empty' distinct from 'did not fire'"*.
///
/// So across a whole fuzzed run an out is unfired or it is not, and the three spellings all land in
/// the same one:
///
/// - **an out that fired nothing has no reading** — `fires == 0` ⇒ no slots, whether it was `None`
///   or the empty batch;
/// - **no slots that are all NaN** — the fourth state the rule exists to forbid, and the reason a
///   per-element kernel's `Unflat` maps an all-NaN element back to `None`;
/// - **a node the sweep skipped fired nothing** — a dark node and one that has never published are
///   the same state, deliberately and permanently (`rates.deps.tick-opaque`).
///
/// The converse of the first is *not* asserted, and that is not a gap. A batch out flattens to its
/// *last* element, so a run of three whose last one declines has `fires == 3` and no slots — which
/// is a reading of the last element, not a fourth state of the out. `outs.flat.nonempty`, the
/// sibling that keeps all of this decidable, is not re-checked at all: it is a `const` assert inside
/// `Flats::of`, so a zero-slot out does not compile and a runtime check would be testing that
/// `const {}` blocks run.
pub fn run_absence(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let steps = trace(f);
	if steps.is_empty() {
		return Ok(());
	}
	let mut g = G::default();
	let mut spellings = [0usize; 3]; // fired, absent, dark
	for (i, s) in steps.iter().enumerate() {
		let mut r = Readings::default();
		fixture::tick_obs(&mut g, s, &mut r);
		for &(node, ran, fires, all_nan) in &r.0 {
			let unfired = fires == 0;
			check!(!unfired || all_nan.is_none(), "tick {i}, `{node}`: fired nothing and still carries slots");
			check!(all_nan != Some(true), "tick {i}, `{node}`: an all-NaN flattening reported as fired");
			check!(ran || unfired, "tick {i}, `{node}`: the sweep skipped it and it fired {fires} elements");
			spellings[match (ran, unfired) {
				(_, false) => 0,
				(true, true) => 1,
				(false, true) => 2,
			}] += 1;
		}
	}
	if verbose {
		eprintln!("{} ticks; fires: {} fired, {} absent-while-live, {} dark", steps.len(), spellings[0], spellings[1], spellings[2]);
	}
	Ok(())
}
