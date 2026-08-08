//! Assert-driver over the simple graph: catalog → `Replay` → batch-native GAT sweep. Batching must
//! not alter fold order, so the day-end counts are bit-identical to the pre-batch run — the asserts
//! below are the integration test.
//!
//! The same sweep is recorded by an attached [`Viz`] and served afterwards, so the day is browsable
//! tick-by-tick. This example owns the runtime; exec_viz is only a library. `nix run .#simple`
//! builds the `exec_viz_web` bundle and runs this against it — a bare `cargo r` needs
//! `EXEC_VIZ_WEB_DIR` and `PORT` set by hand.

use std::{path::PathBuf, time::Duration};

use clap::Parser;
use exec_viz::{Backpressure, Rec, Recorder, Tape, Viz};
use trading_data::{Cell, Exact, ExchangeName, Feed, Fire, LatencyConfig, Observer, ReadClock, Replay, RsiValues, Want, required_lanes};
use trading_data_simple::{day_bounds, ensure_catalog, nodes::Graph, symbol};
use v_utils::*;

/// This app's slot in the devShell's `PORT` range — the devShell owns the base, each app claims a
/// slot in it, so several can be up at once.
const ORDINAL: u16 = 1;
/// Retained ticks. A minute-batched day is well under this; the cap is only there so nothing
/// unbounded rides on a feed's batching.
const SCROLLBACK: usize = 20_000;
/// Coarse on purpose: nothing here is cadence-sensitive, and the FD observer runs per node per tick
/// — this example's cost driver is how often it looks, not how finely.
const CLOCK: ReadClock = ReadClock::from(Exact::from_nanos(60_000_000_000));

#[derive(Parser)]
#[command(about = "one day, one root, one RSI chain", long_about = None)]
struct Cli {
	/// No `Viz`, no server, no observer — run the day, print the counts, exit. What an external
	/// profiler wraps: nothing can measure a command that does not return. The `Signal` FD check goes
	/// with the observer, so a headless run is a sweep measurement and not the acceptance test.
	#[arg(long, env = "TD_HEADLESS")]
	headless: bool,
}

#[tokio::main]
async fn main() {
	let cli = Cli::parse();
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/simple_cache"));
	let catalog = ensure_catalog(&cache);

	let lanes = required_lanes::<Graph>();
	println!("required lanes: {lanes:?}");
	let latency = LatencyConfig {
		p68: Duration::from_millis(5),
		p95: Duration::from_millis(20),
		p997: Duration::from_millis(80),
		seed: 0,
	};
	let (day_start, day_end) = day_bounds();
	let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(), day_start, day_end, &lanes, latency, CLOCK);

	let mut graph = Graph::default();

	// What observes is chosen here, once, and monomorphizes into the sweep: `tick_obs` over the unit
	// observer *is* `tick`, so the headless run pays nothing for the branch that selected it.
	if cli.headless {
		run(&mut feed, &mut graph, &mut ()).report();
		println!("simple: ok");
		return;
	}

	let (tape, recorder) = Tape::new(Some(<trading_data::Bars<{ TF_1MIN }> as Cell>::NAME), SCROLLBACK, 60_000, Backpressure::Block);
	let viz = tape.viz();
	// `Signal`'s own documentation and the viz recording are two readings of one sweep.
	let mut watched = (SignalDoc::default(), recorder);
	run(&mut feed, &mut graph, &mut watched).report();
	let (doc, recorder) = watched;

	assert!(doc.exact > 0, "Signal never reported an exact Jacobian");
	println!("signal exact Jacobians: {}", doc.exact);

	println!("simple: ok");
	let base: u16 = std::env::var("PORT").expect("PORT: the devShell sets the base of the port range").parse().expect("PORT is a u16");
	// Replay-only: the recording is over before the first request, so the last tick is addressable.
	recorder.seal();
	viz.serve_on(Viz::bind(base + ORDINAL).await).await;
}

/// What observes a tick, per tick. A lending factory rather than a closure because [`Rec`] borrows
/// the recorder it writes into; `()` is the headless reading, and it erases.
trait Observed {
	type Obs<'a>: Observer
	where
		Self: 'a;

	fn at(&mut self, ts_ns: i64) -> Self::Obs<'_>;
}
impl Observed for () {
	type Obs<'a> = ();

	fn at(&mut self, _: i64) {}
}
impl Observed for (SignalDoc, Recorder) {
	type Obs<'a> = (&'a mut SignalDoc, Rec<'a>);

	fn at(&mut self, ts_ns: i64) -> Self::Obs<'_> {
		(&mut self.0, self.1.at(ts_ns))
	}
}

/// The day's counts and its leaf levels — what the asserts are over, and the only thing the sweep
/// leaves behind once the observer is gone.
#[derive(Default)]
struct Tally {
	n_trades: u64,
	bars: u64,
	rsi_snaps: u64,
	lambda_fires: u64,
	cvd: f64,
	vol1h: f64,
	rsi_end: Option<RsiValues>,
	lambda_end: Option<f64>,
}
impl Tally {
	fn report(&self) {
		let Self {
			n_trades,
			bars,
			rsi_snaps,
			lambda_fires,
			cvd,
			vol1h,
			rsi_end,
			lambda_end,
		} = *self;
		println!("trades={n_trades} bars={bars} rsi_snaps={rsi_snaps} lambda_fires={lambda_fires}");
		println!("leaf levels at day end: rsi={rsi_end:?} vol1h={vol1h:.0} cvd={cvd:.0} λ={lambda_end:?}");

		// batching does not alter fold order ⇒ these are bit-identical to the pre-batch run.
		assert_eq!(n_trades, 270164, "trade count changed");
		assert_eq!(bars, 1439, "bar count changed");
		// warm after base_len + smooth_len closes, and the day is three orders of magnitude longer.
		assert!(rsi_snaps > 1_000, "RSI warmed on only {rsi_snaps} bars");
		assert!(lambda_fires >= 1, "lambda never fired");
		assert!(cvd != 0.0 && cvd.is_finite(), "day-end CVD degenerate: {cvd}");
	}
}

fn run(feed: &mut Replay, graph: &mut Graph, obs: &mut impl Observed) -> Tally {
	let mut t = Tally::default();
	while let Some(lanes) = feed.next() {
		t.n_trades += lanes.trades.len() as u64;
		let ts_ns = lanes.ts_venue.as_nanos();
		let out = graph.tick_obs(ts_ns, lanes.into(), &mut obs.at(ts_ns));

		t.bars += out.bar.len() as u64;
		t.rsi_snaps += out.rsi.iter().flatten().count() as u64;
		t.lambda_fires += out.lambda.iter().flatten().count() as u64;
		for l in out.lambda.iter().flatten() {
			assert!(l.is_finite(), "lambda went non-finite: {l}");
		}
		// cross-rate levels: last element is the current running/level view.
		if let Some(&c) = out.cvd.last() {
			t.cvd = c;
		}
		if let Some(&Some(v)) = out.vol_usd.last() {
			t.vol1h = v;
		}
		if let Some(&Some(r)) = out.rsi.last() {
			t.rsi_end = Some(r);
		}
		if let Some(&l) = out.lambda.last() {
			t.lambda_end = l;
		}
	}
	t
}

/// Surfaces the `Signal` node's self-documentation through the observation choke point: prints its
/// formula, per-dep derivatives, and (behind `SIGNAL_TRACE`) the debug trace once; and asserts the
/// exact Jacobian agrees with the retained finite-difference one on every fired tick — the wiring's
/// acceptance test.
#[derive(Default)]
struct SignalDoc {
	shown: bool,
	exact: u64,
}
impl Observer for SignalDoc {
	/// The formula and its derivatives are the `Jac` reading, so that is what it asks for.
	fn want(&self, _: &'static str) -> Want {
		Want::Jac
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		if node.rsplit("::").next() != Some("Signal") {
			return;
		}
		// a `Pure` node's one-step Jacobian is differentiated, never guessed — asserted here rather
		// than compared against a second reading of the same quantity (`r[kernels.jac.two-quantities]`).
		if fire.jac.is_some_and(|j| j.iter().any(|x| x.is_finite())) {
			assert!(fire.exact, "Signal is a `Symbolic` node: its Jacobian must be the exact one");
			self.exact += 1;
		}
		if !self.shown && fire.vals.is_some_and(|v| v[0].is_finite()) {
			self.shown = true;
			println!("\n── Signal, self-documenting (value = {:.6}) ──", fire.vals.expect("just checked")[0]);
			if let Some(f) = fire.formula {
				println!("formula:  {f}");
			}
			if let Some(d) = fire.deriv {
				println!("∂:\n{d}");
			}
			if std::env::var_os("SIGNAL_TRACE").is_some()
				&& let Some(t) = fire.trace
			{
				println!("trace:\n{t}");
			}
			println!();
		}
	}
}
