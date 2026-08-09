//! Deterministic metamorphic fuzzer for `trading_data`, driven entirely through the facade
//! (matklad FRNG + TigerBeetle VOPR, ported from `dockviewers_core/tests/integration/`).
//!
//! There is no oracle to write here: every normative MUST in `docs/spec/` that this binary covers is
//! an *invariance* claim over the schedule, so the oracle is another run of the same code under a
//! different one. What compile time already pins down is what is wired; what it leaves free is when
//! things arrive and how they group, and that is the axis below.
//!
//! One binary, several targets. `fuzz` drives random seeds against each, auto-minimizes the first
//! failure, and records its minimal `(target, seed, size)` to `CORPUS.txt`; `regressions` replays
//! every recorded case at the current generator fingerprint. Env-var replay:
//! `FUZZ_SEED=… FUZZ_SIZE=… FUZZ_TARGET=… cargo t -p trading_data --test fuzz -- --nocapture`
//! verbose-replays one case.

/// An oracle violation, as the value a target returns rather than a panic — a panic is reserved for
/// production code blowing up under the trace, and the two want telling apart in the report.
macro_rules! check {
	($cond:expr, $($arg:tt)*) => {
		if !$cond {
			return Err(format!($($arg)*));
		}
	};
}

mod corpus;
mod frng;
mod minimize;
mod stream;

mod fixture;
mod gates;
mod outs;
mod schedule;
mod weaver;

use std::cell::RefCell;

use frng::Frng;

/// Default fuzz budget. `FUZZ_RUNS` / `FUZZ_SIZE` override; bump locally for deeper runs.
const DEFAULT_RUNS: u64 = 512;
const DEFAULT_SIZE: usize = 256;

/// One metamorphic property, generator and oracle together. `run` reads the whole trace off the
/// buffer, so `(seed, size)` names it exactly; `version` fingerprints the sources that decide what
/// that pair means.
struct Target {
	name: &'static str,
	version: u32,
	run: fn(&mut Frng, bool) -> Result<(), String>,
}

const TARGETS: &[Target] = &[
	Target {
		name: "weave",
		version: weaver::VERSION,
		run: weaver::run,
	},
	Target {
		name: "round_trip",
		version: weaver::VERSION,
		run: weaver::run_round_trip,
	},
	Target {
		name: "schedule",
		version: schedule::VERSION,
		run: schedule::run,
	},
	Target {
		name: "rewarm",
		version: gates::VERSION,
		run: gates::run_rewarm,
	},
	Target {
		name: "shape_sweep",
		version: gates::VERSION,
		run: gates::run_shape_sweep,
	},
	Target {
		name: "noninvasive",
		version: outs::VERSION,
		run: outs::run_noninvasive,
	},
	Target {
		name: "absence",
		version: outs::VERSION,
		run: outs::run_absence,
	},
];

thread_local! {
	static LAST_PANIC: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Swallow the default panic backtrace (we catch and report ourselves), but stash the message so a
/// caught production blowup is still legible.
fn install_quiet_hook() {
	std::panic::set_hook(Box::new(|info| {
		LAST_PANIC.with(|c| *c.borrow_mut() = info.to_string());
	}));
}

fn env_usize(key: &str, default: usize) -> usize {
	std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Run one case under `catch_unwind`, so a production `expect`/`unreachable` blowup counts as a
/// failure exactly as a violated oracle does.
fn fails(t: &'static Target, seed: u64, size: usize) -> bool {
	std::panic::catch_unwind(|| (t.run)(&mut Frng::new(seed, size), false)).map(|r| r.is_err()).unwrap_or(true)
}

#[test]
fn fuzz() {
	install_quiet_hook();
	let size = env_usize("FUZZ_SIZE", DEFAULT_SIZE);
	let only = std::env::var("FUZZ_TARGET").ok();
	let picked = || TARGETS.iter().filter(|t| only.as_ref().is_none_or(|n| *n == t.name));
	assert!(
		picked().next().is_some(),
		"FUZZ_TARGET names no target; the set is {:?}",
		TARGETS.iter().map(|t| t.name).collect::<Vec<_>>()
	);

	if let Ok(s) = std::env::var("FUZZ_SEED") {
		let seed = s.parse().expect("FUZZ_SEED must be a u64");
		for t in picked() {
			replay(t, seed, size);
		}
		return;
	}

	let runs: u64 = std::env::var("FUZZ_RUNS").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_RUNS);
	for t in picked() {
		for seed in 0..runs {
			if !fails(t, seed, size) {
				continue;
			}
			let (ms, msz) = minimize::minimize(&|s, z| fails(t, s, z), seed, size);
			eprintln!("\n=== FUZZ FAILURE ({}, seed={seed}, size={size}) ===", t.name);
			eprintln!("minimal repro: (target={}, seed={ms}, size={msz})", t.name);
			let reason = replay(t, ms, msz).unwrap_or_else(|| "(no failure on replay)".to_string());
			corpus::record(t.name, ms, msz, t.version, &reason);
			eprintln!("recorded to CORPUS.txt — fix the bug, then commit the new line.");
			panic!("fuzz found a failure; minimal repro (target={}, seed={ms}, size={msz}): {reason}", t.name);
		}
		eprintln!("fuzz: {} clean over {runs} seeds at size {size}", t.name);
	}
}

#[test]
fn regressions() {
	install_quiet_hook();
	let all = corpus::load();
	let mut live = 0usize;
	for e in &all {
		let Some(t) = TARGETS.iter().find(|t| t.name == e.target) else {
			continue; // a target that was removed: the line stays as documentation
		};
		if e.version != t.version {
			continue;
		}
		live += 1;
		assert!(!fails(t, e.seed, e.size), "recorded regression ({}, seed={}, size={}) fails again", e.target, e.seed, e.size);
	}
	// Loud, because a corpus that has silently emptied itself still reports green.
	eprintln!(
		"corpus: replayed {live} of {} recorded cases; the rest were recorded under an older generator and are history, not tests",
		all.len()
	);
	for t in TARGETS {
		eprintln!("  {} at generator {:08x}", t.name, t.version);
	}
}

/// Verbose re-run of one case: the target prints its own trace, and this reports the failure (or the
/// panic). Returns the failure reason, or `None` if it didn't reproduce.
fn replay(t: &'static Target, seed: u64, size: usize) -> Option<String> {
	eprintln!("--- replay (target={}, seed={seed}, size={size}) ---", t.name);
	match std::panic::catch_unwind(|| (t.run)(&mut Frng::new(seed, size), true)) {
		Ok(Ok(())) => {
			eprintln!("(no failure on replay)");
			None
		}
		Ok(Err(what)) => {
			eprintln!("FAILURE: {what}");
			Some(what)
		}
		Err(_) => {
			let p = LAST_PANIC.with(|c| c.borrow().clone());
			eprintln!("PANIC: {p}");
			Some(format!("PANIC: {p}"))
		}
	}
}
