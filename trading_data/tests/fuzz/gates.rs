//! **Phase 3 — gate/latch re-warm, and the shape ↔ sweep correspondence.** Two targets over one
//! harness, because both need the gates driven to chosen states.
//!
//! Nothing here *sets* a gate. A gate is a marker on a node whose `Out<'t> = bool`, computed in
//! `advance` from its deps, and a setter would let the fuzzer reach states the type system says are
//! unreachable — burning triage on non-bugs and punching a permanent hole in the export boundary.
//! Both gates read a root the fuzzer feeds, so every state below is production-reachable.

use std::collections::BTreeMap;

use trading_data::{Fire, Observer, Want};

use crate::{
	fixture::{self, G, Outs, Step},
	frng::Frng,
};

pub const VERSION: u32 = crate::corpus::fnv(&[include_str!("frng.rs"), include_str!("fixture.rs"), include_str!("gates.rs")]);

/// Which fixture root drives which gate. The fuzzer owns the fixture, so this is a statement about
/// it rather than a guess — and `G::SHAPE.gates()` is what says the set is still these two.
const DRIVEN_BY: [&str; 2] = ["Hot", "Warm"];

/// How a level names itself: `Latest<{source}>`, composed rather than compiler-rendered, so a chart
/// spells the series the same way every other card does.
const LEVEL: &str = "Latest<";

/// **A known, unfixed divergence between the shape and the sweep, quarantined rather than papered
/// over.** The shape reports a `Latest` dark once the only consumer reading it through `Sampling` is
/// gated shut; the sweep advances it anyway, exactly like the `Buffer` beside it.
///
/// The cause is one arm: `trading_data_macros/src/demand.rs` pins a retention cell it finds in
/// `st.bufs` as `Pin::Retention`, and a level is not in `st.bufs`, so it gets `Pin::None` and its
/// demand is propagated from its consumers like any ordinary node. The picture then draws `░⟲` where
/// the emitted code runs the node.
///
/// **The sweep is the correct one.** A level that slept would wake its consumer onto a stale
/// reading, and `run_rewarm` shows it does not — so this is a wrong reading in `Under`, not a wrong
/// sweep. Fixing it is a macro change this fuzzer does not own.
///
/// This is not a blanket exemption: only *shape-dark, sweep-live* on a level is waved through, so
/// the opposite divergence still fails. And the count is asserted below — the day the pin lands, the
/// quarantine stops being reached and this target fails until someone deletes it.
const LEVEL_DIVERGES: usize = 2;

/// Records `ran` per node. `Fire::ran` is "whether the sweep advanced the node at all", reported for
/// skipped nodes too as long as the observer wants more than `Want::Nothing` — so which nodes
/// advanced needs no new `pub` and no counter threaded through the fixture.
#[derive(Default)]
struct Ran(BTreeMap<&'static str, bool>);

impl Observer for Ran {
	fn want(&self, _: &'static str) -> Want {
		Want::Vals
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		self.0.insert(node, fire.ran);
	}
}

fn step(f: &mut Frng, ts: i64, gates: Option<(bool, bool)>) -> Step {
	let n = f.below(4) as usize;
	let src = (0..n).map(|i| fixture::t(ts + i as i64 * 1_000_000_000, f.span(-50.0, 50.0))).collect();
	// a gate whose root batch is empty reads its dep as absent and stays shut, which is one of the
	// closed states and worth drawing rather than only ever the "negative value" one.
	let ctl = |f: &mut Frng, forced: Option<bool>| match forced {
		Some(true) => vec![fixture::t(ts, 1.0)],
		Some(false) => vec![fixture::t(ts, -1.0)],
		None => match f.below(3) {
			0 => Vec::new(),
			1 => vec![fixture::t(ts, -1.0)],
			_ => vec![fixture::t(ts, 1.0)],
		},
	};
	Step {
		src,
		ctl_a: ctl(f, gates.map(|g| g.0)),
		ctl_b: ctl(f, gates.map(|g| g.1)),
	}
}

fn trace(f: &mut Frng, gates: Option<(bool, bool)>) -> Vec<Step> {
	let mut out = Vec::new();
	let mut ts = 0i64;
	while f.remaining() > 3 {
		out.push(step(f, ts, gates));
		ts += 1 + f.span(0.0, 90_000_000_000.0) as i64;
	}
	out
}

/// **3a — re-warm.** A run whose gates went dark and re-woke must land, on every tick its gate was
/// open, exactly where a run with that gate pinned open landed. That is what "engine-held history
/// re-warms" *means*: `Win` reads a `Buffering` and `Lvl` a `Sampling`, both of them retention the
/// frame keeps hole-free whoever is asleep, so waking costs nothing the sleep took away.
///
/// The ungated nodes are compared on every tick, which is the other half of the same claim: a gate
/// somewhere else in the graph may not perturb a node it does not dominate.
pub fn run_rewarm(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let steps = trace(f, None);
	if steps.is_empty() {
		return Ok(());
	}
	let drive = |steps: &[Step]| {
		let mut g = G::default();
		steps.iter().map(|s| fixture::tick(&mut g, s)).collect::<Vec<Outs>>()
	};
	let pinned: Vec<Step> = steps
		.iter()
		.map(|s| Step {
			src: s.src.clone(),
			ctl_a: vec![fixture::t(0, 1.0)],
			ctl_b: vec![fixture::t(0, 1.0)],
		})
		.collect();

	let (dark, open) = (drive(&steps), drive(&pinned));
	if verbose {
		eprintln!("{} ticks", steps.len());
		for (i, (s, (a, b))) in steps.iter().zip(dark.iter().zip(&open)).enumerate() {
			eprintln!("  [{i:>3}] gates={:?} src={}\n        dark {a:?}\n        open {b:?}", s.gates(), s.src.len());
		}
	}

	let mut woke = 0;
	for (i, (s, (a, b))) in steps.iter().zip(dark.iter().zip(&open)).enumerate() {
		let (hot, warm) = s.gates();
		check!(a.total == b.total, "tick {i}: a gate elsewhere moved an ungated fold: {:?} vs {:?}", a.total, b.total);
		check!(a.bucket == b.bucket, "tick {i}: a gate elsewhere moved an ungated fold: {:?} vs {:?}", a.bucket, b.bucket);
		check!(a.peak == b.peak, "tick {i}: a gate elsewhere moved an ungated node: {:?} vs {:?}", a.peak, b.peak);
		if hot {
			woke += 1;
			check!(a.win == b.win, "tick {i}: a re-woken window reads {:?}, a never-dark one {:?}", a.win, b.win);
			check!(a.ra == b.ra, "tick {i}: a re-woken reader of a suppressed node reads {:?}, a never-dark one {:?}", a.ra, b.ra);
		}
		if warm {
			woke += 1;
			check!(a.lvl == b.lvl, "tick {i}: a re-woken level reads {:?}, a never-dark one {:?}", a.lvl, b.lvl);
			check!(a.rb == b.rb, "tick {i}: a re-woken reader of a suppressed node reads {:?}, a never-dark one {:?}", a.rb, b.rb);
		}
		// a gate that is shut has no reading of its own to make: absence is one fact, and a dark node
		// reports it as the same `None` a node that has never published does (`rates.deps.tick-opaque`).
		if !hot {
			check!(a.win.is_none() && a.ra.is_none(), "tick {i}: a shut gate left a reading behind: win={:?} ra={:?}", a.win, a.ra);
		}
		if !warm {
			check!(a.lvl.is_none() && a.rb.is_none(), "tick {i}: a shut gate left a reading behind: lvl={:?} rb={:?}", a.lvl, a.rb);
		}
	}
	check!(woke > 0 || steps.len() < 4, "the trace never opened a gate, so nothing re-warmed");
	Ok(())
}

/// **3b — shape ↔ sweep.** `Under::live` documents itself as *"a reading of the emitted code rather
/// than a second model of it"*, and nothing tests that claim: the insta snapshots pin the rendering,
/// not the correspondence. The compile-time derivation (the macro's DNF demand propagation) and the
/// runtime sweep (`Pull::open` short-circuiting) are computed by completely different code.
///
/// So: drive the graph into each gate assignment and assert exactly the nodes the shape reports live
/// are the nodes that advanced. Gates are few, so the assignments are enumerated rather than
/// sampled — the random part is the data and the tick schedule.
///
/// `Under::live` is private and stays private. Liveness is read off the `●`/`░` markers of the
/// `Under` Display, which is the same discipline dockviewers' oracle keeps when it parses rects out
/// of inline CSS rather than exposing a view-model internal.
pub fn run_shape_sweep(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let gates: Vec<String> = G::SHAPE.gates().collect();
	check!(gates == DRIVEN_BY, "the fixture's gates are {gates:?}, and this target drives {DRIVEN_BY:?}");

	// One trace, re-driven under each assignment: drawing a fresh one per assignment would spend the
	// buffer on the first and leave the rest running on an empty trace, which is coverage lost
	// silently.
	let data = trace(f, Some((true, true)));
	if data.is_empty() {
		return Ok(());
	}
	let mut diverged = 0;

	for bits in 0..1u32 << gates.len() {
		let want = (bits & 1 != 0, bits & 2 != 0);
		let hold = |open: bool, ts: i64| vec![fixture::t(ts, if open { 1.0 } else { -1.0 })];
		let steps: Vec<Step> = data
			.iter()
			.enumerate()
			.map(|(i, s)| Step {
				src: s.src.clone(),
				ctl_a: hold(want.0, i as i64),
				ctl_b: hold(want.1, i as i64),
			})
			.collect();
		let state: Vec<(&'static str, bool)> = vec![(DRIVEN_BY[0], want.0), (DRIVEN_BY[1], want.1)];
		let rendered = format!("{}", G::SHAPE.under(&state));
		let shape = live_from(&rendered)?;

		let mut g = G::default();
		let mut seen = Ran::default();
		for s in &steps {
			seen = Ran::default();
			fixture::tick_obs(&mut g, s, &mut seen);
			check!(s.gates() == want, "the trace failed to drive the gates to {want:?}, it drove them to {:?}", s.gates());
		}

		if verbose {
			eprintln!("--- {}={} {}={} over {} ticks ---\n{rendered}", DRIVEN_BY[0], want.0, DRIVEN_BY[1], want.1, steps.len());
			eprintln!("swept: {:?}", seen.0);
		}

		check!(
			shape.len() == G::SHAPE.nodes.len(),
			"the render gave {} node marks for {} shape nodes",
			shape.len(),
			G::SHAPE.nodes.len()
		);
		for (i, n) in G::SHAPE.nodes.iter().enumerate() {
			// Roots are prepended to `Shape::nodes` so every edge has somewhere to point, and the sweep
			// steps none of them — the one thing the two readings do not share. The render says which
			// they are, so reading it off there costs no second reading of `Kind`.
			let Some(want_live) = shape[i] else { continue };
			let Some(&ran) = seen.0.get(n.name) else {
				return Err(format!(
					"`{}` is a node of the shape and the sweep never stepped it; stepped {:?}",
					n.name,
					seen.0.keys().collect::<Vec<_>>()
				));
			};
			// ponytail: one known divergence, quarantined rather than fixed — see [`LEVEL_DIVERGES`].
			if n.name.starts_with(LEVEL) && !want_live && ran {
				diverged += 1;
				continue;
			}
			check!(
				ran == want_live,
				"under {}={} {}={}, the shape says `{}` is {} and the sweep {} it",
				DRIVEN_BY[0],
				want.0,
				DRIVEN_BY[1],
				want.1,
				n.name,
				if want_live { "live" } else { "dark" },
				if ran { "advanced" } else { "skipped" }
			);
		}
	}
	check!(
		diverged == LEVEL_DIVERGES,
		"the `Latest` shape/sweep divergence showed up {diverged} times, not {LEVEL_DIVERGES}: if it is fixed, delete the quarantine in gates.rs; if it spread, it is no longer the one case that was triaged"
	);
	Ok(())
}

/// Liveness per node, in `Shape::nodes` order, read off the rendered picture — `None` for a root,
/// which the sweep never steps. A node row carries exactly one of `╷`(root) `●`(live) `░`(dark); the
/// header and the legend carry all three, so both are skipped by position rather than by glyph.
fn live_from(rendered: &str) -> Result<Vec<Option<bool>>, String> {
	let mut out = Vec::new();
	for line in rendered.lines() {
		if line.starts_with("graph ") || line.starts_with("legend") {
			continue;
		}
		if !out.is_empty() && line.trim().is_empty() {
			break; // the blank before the legend block: everything after it is the summary
		}
		match (line.contains('╷'), line.contains('●'), line.contains('░')) {
			(true, false, false) => out.push(None),
			(false, true, false) => out.push(Some(true)),
			(false, false, true) => out.push(Some(false)),
			(false, false, false) => continue, // a lane row
			_ => return Err(format!("a node row carries more than one mark: {line:?}")),
		}
	}
	Ok(out)
}
