//! **Phase 2 — schedule invariance.** `rates.folds.exactly-once` states it normatively: a fold sees
//! every element of what it folds exactly once, in order, and that is *"a statement about the
//! element sequence, which is identical under every grouping — not about ticks, which are not"*.
//!
//! So: one element sequence, K groupings of it into ticks, one graph per grouping, and the folds'
//! concatenated element streams must agree. This is `trading_data_core/tests/chunk_equivalence.rs`
//! generalized off its four hardcoded `(seed, n, tick)` tuples — same shape of claim, one tier up.
//!
//! **The scoping is the subtle part and it is deliberate.** `rates.deps.tick-opaque`'s closing note
//! and ARCHITECTURE.md both say a coarser grouping means fewer points at which nodes evaluate at
//! all, and that anything reading a running extremum over *evaluated* states legitimately sees that.
//! `Peak` in the fixture is exactly such a node and no oracle here reads it — asserting it away
//! would be asserting that a backtest reproduces a live run, which §2 warns against by name.

use trading_data::{Present, Reading};
use v_utils::fuzz::Frng;

use crate::fixture::{self, G, Outs, Step, Tick, WARM};

pub const VERSION: u32 = v_utils::fuzz::fnv(&[v_utils::fuzz::FRNG_SRC, include_str!("fixture.rs"), include_str!("schedule.rs")]);

/// Groupings per run. Two is enough for the claim; the third is what makes a *pair* of coarse
/// groupings comparable rather than only each against the degenerate one.
pub const K: usize = 4;

/// The element sequence, before anything has decided how it arrives. Timestamps advance by seconds,
/// so a run of a few dozen elements straddles the `Bucket` period boundary several times.
pub fn elements(f: &mut Frng) -> Vec<Tick> {
	let mut ts = 0i64;
	let mut out = Vec::new();
	while f.remaining() > 2 {
		// aimed at the period boundary as often as drawn away from it: a fold that publishes on the
		// close is only interesting where a close happens, and where it happens *between* two batches.
		ts += match f.below(4) {
			0 => 1_000_000_000,
			1 => 20_000_000_000,
			2 => 60_000_000_000 - ts.rem_euclid(60_000_000_000),
			_ => 1 + f.span(0.0, 5_000_000_000.0) as i64,
		};
		out.push(fixture::t(ts, f.span(-50.0, 50.0)));
	}
	out
}

/// One grouping of `els` into ticks, with the gates held open throughout. Empty batches are in the
/// draw: a tick carrying no element is the commonest tick there is, and a fold must be unmoved by
/// one.
pub fn grouping(f: &mut Frng, els: &[Tick], which: usize) -> Vec<Step> {
	let open = vec![fixture::t(0, 1.0)];
	let batch = |src: Vec<Tick>| Step {
		src,
		ctl_a: open.clone(),
		ctl_b: open.clone(),
	};
	match which {
		// the two degenerate ends, always present so a run always has the widest pair in it
		0 => vec![batch(els.to_vec())],
		1 => els.iter().map(|&x| batch(vec![x])).collect(),
		_ => {
			let (mut out, mut i) = (Vec::new(), 0);
			while i < els.len() {
				// the empty tick is drawn separately from the chunk size, so a chunk always makes
				// progress: a zero drawn as a size would leave the cursor where it was.
				if f.below(4) == 0 {
					out.push(batch(Vec::new()));
				}
				let end = (i + 1 + f.below(4) as usize).min(els.len());
				out.push(batch(els[i..end].to_vec()));
				i = end;
			}
			out.push(batch(Vec::new())); // and one trailing empty tick, which closes nothing
			out
		}
	}
}

/// What `rates.folds.exactly-once` makes invariant: the *element* streams, concatenated across
/// however many ticks the grouping happened to use. Not the per-tick shape, which is the grouping's.
#[derive(Debug, PartialEq)]
pub struct Folded {
	total: Vec<Option<f64>>,
	bucket: Vec<f64>,
	warm: Vec<Reading>,
}

pub fn fold(outs: &[Outs]) -> Folded {
	Folded {
		total: outs.iter().flat_map(|o| o.total.iter().copied()).collect(),
		bucket: outs.iter().flat_map(|o| o.bucket.iter().copied()).collect(),
		warm: outs.iter().flat_map(|o| o.warm.iter().copied()).collect(),
	}
}

pub fn run(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let els = elements(f);
	if els.is_empty() {
		return Ok(());
	}
	let runs: Vec<(Vec<Step>, Vec<Outs>)> = (0..K)
		.map(|k| {
			let steps = grouping(f, &els, k);
			let mut g = G::default();
			let outs = steps.iter().map(|s| fixture::tick(&mut g, s)).collect();
			(steps, outs)
		})
		.collect();

	if verbose {
		eprintln!("{} elements, {K} groupings", els.len());
		for (k, (steps, outs)) in runs.iter().enumerate() {
			eprintln!("  [{k}] {} ticks, sizes {:?}", steps.len(), steps.iter().map(|s| s.src.len()).collect::<Vec<_>>());
			eprintln!("      folded {:?}", fold(outs));
			eprintln!("      peak (not compared, and rightly) {:?}", outs.last().map(|o| o.peak));
		}
	}

	let first = fold(&runs[0].1);
	for (k, (steps, outs)) in runs.iter().enumerate().skip(1) {
		let got = fold(outs);
		check!(
			got == first,
			"grouping {k} ({} ticks, sizes {:?}) folds differently from grouping 0 ({} ticks):\n  0: {first:?}\n  {k}: {got:?}",
			steps.len(),
			steps.iter().map(|s| s.src.len()).collect::<Vec<_>>(),
			runs[0].0.len()
		);
	}
	Ok(())
}

// r[verify outs.absence.typed]
/// The presence boundary of a warm-up, under every grouping. A cold element is *absent* and not a
/// number — the state the representation change is about, since a NaN that leaked into the algebra
/// would come back out as some number the comparison happened to pick, and a run that went absent
/// again after warming would be a fold that lost its state.
///
/// The grouping axis is the point: `Warmup` folds a `Folding` dep, so its element sequence is
/// invariant under it (`rates.folds.exactly-once`) — and therefore so is *which* of those elements
/// carries a value. [`run`] above already compares the whole folded stream across groupings; what
/// this adds is the claim about the stream's shape, which a single grouping could satisfy while
/// disagreeing with every other one.
pub fn run_warmup(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let els = elements(f);
	if els.len() <= WARM {
		return Ok(()); // nothing warms, so there is no boundary to be wrong about
	}
	for k in 0..K {
		let steps = grouping(f, &els, k);
		let mut g = G::default();
		let outs: Vec<Outs> = steps.iter().map(|s| fixture::tick(&mut g, s)).collect();
		let run: Vec<Reading> = outs.iter().flat_map(|o| o.warm.iter().copied()).collect();

		check!(run.len() == els.len(), "grouping {k}: a rate-preserving fold published {} elements over {}", run.len(), els.len());
		for (i, r) in run.iter().enumerate() {
			// `i + 1` because the count the body reads is the one this element's own step wrote: the
			// `WARM`th element is the one that warms, and it publishes.
			match r.present() {
				Some(v) => {
					check!(i + 1 >= WARM, "grouping {k}, element {i}: warm before {WARM} elements had arrived, at {v}");
					check!(v.is_finite(), "grouping {k}, element {i}: a warm element is a number, not {v}");
				}
				None => check!(i + 1 < WARM, "grouping {k}, element {i}: cold again after warming\n  {run:?}"),
			}
		}
		if verbose {
			eprintln!("  [{k}] {} ticks, {} elements, absent for the first {}", steps.len(), run.len(), WARM - 1);
		}
	}
	Ok(())
}
