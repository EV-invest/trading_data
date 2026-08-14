//! Deterministic metamorphic fuzzer for `trading_data`, driven entirely through the facade.
//!
//! There is no oracle to write here: every normative MUST in `docs/spec/` that this binary covers is
//! an *invariance* claim over the schedule, so the oracle is another run of the same code under a
//! different one. What compile time already pins down is what is wired; what it leaves free is when
//! things arrive and how they group, and that is the axis below.
//!
//! The FRNG, the minimizer, the corpus format and the scan/record/replay loop are
//! [`v_utils::fuzz`] — shared with `dockviewers_core`, which is where this harness was ported from.
//! What is local is the table below: one binary, several targets, each its own generator over the
//! same buffer. Env-var replay:
//! `FUZZ_SEED=… FUZZ_SIZE=… FUZZ_TARGET=… cargo t -p trading_data --test fuzz -- --nocapture`
//! verbose-replays one case, and `FUZZ_FILM=<path>` draws one as an animated SVG — see [`film`].

/// An oracle violation, as the value a target returns rather than a panic — a panic is reserved for
/// production code blowing up under the trace, and the two want telling apart in the report.
macro_rules! check {
	($cond:expr, $($arg:tt)*) => {
		if !$cond {
			return Err(format!($($arg)*));
		}
	};
}

#[cfg(feature = "bench")]
mod film;
mod stream;

mod fixture;
mod gates;
mod latch;
mod outs;
mod rewind;
mod schedule;
mod weaver;

use v_utils::fuzz::{Frng, Suite, Target};

const TARGETS: &[Target] = &[
	Target {
		name: "weave",
		version: weaver::VERSION,
		run: |s, z, v| weaver::run(&mut Frng::new(s, z), v),
	},
	Target {
		name: "round_trip",
		version: weaver::VERSION,
		run: |s, z, v| weaver::run_round_trip(&mut Frng::new(s, z), v),
	},
	Target {
		name: "schedule",
		version: schedule::VERSION,
		run: |s, z, v| schedule::run(&mut Frng::new(s, z), v),
	},
	Target {
		name: "warmup",
		version: schedule::VERSION,
		run: |s, z, v| schedule::run_warmup(&mut Frng::new(s, z), v),
	},
	Target {
		name: "rewarm",
		version: gates::VERSION,
		run: |s, z, v| gates::run_rewarm(&mut Frng::new(s, z), v),
	},
	Target {
		name: "shape_sweep",
		version: gates::VERSION,
		run: |s, z, v| gates::run_shape_sweep(&mut Frng::new(s, z), v),
	},
	Target {
		name: "latch",
		version: latch::VERSION,
		run: |s, z, v| latch::run(&mut Frng::new(s, z), v),
	},
	Target {
		name: "rewind",
		version: rewind::VERSION,
		run: |s, z, v| rewind::run(&mut Frng::new(s, z), v),
	},
	Target {
		name: "noninvasive",
		version: outs::VERSION,
		run: |s, z, v| outs::run_noninvasive(&mut Frng::new(s, z), v),
	},
	Target {
		name: "absence",
		version: outs::VERSION,
		run: |s, z, v| outs::run_absence(&mut Frng::new(s, z), v),
	},
];

const SUITE: Suite = Suite {
	targets: TARGETS,
	corpus: concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fuzz/CORPUS.txt"),
};

#[test]
fn fuzz() {
	// Before the suite's quiet hook, and instead of a fuzz run: the film is an artifact this binary
	// writes, like `CORPUS.txt`, not a case it is checking — so what it panics with is a message for
	// whoever asked for it, and that hook exists to swallow exactly such messages.
	if let Ok(out) = std::env::var("FUZZ_FILM") {
		#[cfg(not(feature = "bench"))]
		panic!("FUZZ_FILM draws the `Census`, which is the facade's `bench` tier: `cargo t -p trading_data --features bench --test fuzz`");
		#[cfg(feature = "bench")]
		{
			film::film(std::path::Path::new(&out));
			return;
		}
	}

	SUITE.fuzz();
}

#[test]
fn regressions() {
	SUITE.regressions();
}
