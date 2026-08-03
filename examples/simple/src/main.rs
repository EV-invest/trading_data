//! Assert-driver over the simple graph: catalog → `Replay` → batch-native GAT sweep. Batching must
//! not alter fold order, so the day-end counts are bit-identical to the pre-batch run — the asserts
//! below are the integration test.
//!
//! The same sweep is recorded by an attached [`Viz`] and served afterwards, so the day is browsable
//! tick-by-tick. This example owns the runtime; exec_viz is only a library. `nix run .#simple`
//! builds the `exec_viz_web` bundle and runs this against it — a bare `cargo r` needs
//! `EXEC_VIZ_WEB_DIR` and `PORT` set by hand.

use std::{path::PathBuf, time::Duration};

use exec_viz::Viz;
use trading_data::{Cell, Exact, ExchangeName, Feed, Fire, LatencyConfig, Observer, ReadClock, Replay, required_lanes};
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

#[tokio::main]
async fn main() {
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
	let viz = Viz::new(Some(<trading_data::Bars<{ TF_1MIN }> as Cell>::NAME), SCROLLBACK, 60_000);
	// `Signal`'s exact/FD agreement check and the viz recording are two readings of one sweep.
	let mut obs = (SignalDoc::default(), viz.clone());
	let (mut n_trades, mut bars, mut rsi_snaps, mut lambda_fires) = (0u64, 0u64, 0u64, 0u64);
	let (mut cvd, mut vol1h) = (0.0f64, 0.0f64);
	let (mut rsi_end, mut lambda_end) = (None, None);

	while let Some(lanes) = feed.next() {
		n_trades += lanes.trades.len() as u64;
		let ts_ns = lanes.ts_venue.as_nanos();
		obs.1.at(ts_ns);
		let out = graph.tick_obs(ts_ns, lanes.into(), &mut obs);

		bars += out.bar.len() as u64;
		rsi_snaps += out.rsi.iter().flatten().count() as u64;
		lambda_fires += out.lambda.iter().flatten().count() as u64;
		for l in out.lambda.iter().flatten() {
			assert!(l.is_finite(), "lambda went non-finite: {l}");
		}
		// cross-rate levels: last element is the current running/level view.
		if let Some(&c) = out.cvd.last() {
			cvd = c;
		}
		if let Some(&Some(v)) = out.vol_usd_1h.last() {
			vol1h = v;
		}
		if let Some(&Some(r)) = out.rsi.last() {
			rsi_end = Some(r);
		}
		if let Some(&l) = out.lambda.last() {
			lambda_end = l;
		}
	}

	println!("trades={n_trades} bars={bars} rsi_snaps={rsi_snaps} lambda_fires={lambda_fires}");
	println!("leaf levels at day end: rsi={rsi_end:?} vol1h={vol1h:.0} cvd={cvd:.0} λ={lambda_end:?}");

	// batching does not alter fold order ⇒ these are bit-identical to the pre-batch run.
	assert_eq!(n_trades, 270164, "trade count changed");
	assert_eq!(bars, 1439, "bar count changed");
	// warm after base_len + smooth_len closes, and the day is three orders of magnitude longer.
	assert!(rsi_snaps > 1_000, "RSI warmed on only {rsi_snaps} bars");
	assert!(lambda_fires >= 1, "lambda never fired");
	assert!(cvd != 0.0 && cvd.is_finite(), "day-end CVD degenerate: {cvd}");

	let doc = obs.0;
	println!("signal exact/FD agreement: checked={} max_rel={:.2e}", doc.checked, doc.max_rel);
	assert!(doc.checked > 0, "Signal never produced a finite Jacobian");
	assert!(doc.max_rel < 1e-3, "exact Jacobian disagrees with FD: max_rel={}", doc.max_rel);

	println!("simple: ok");
	let base: u16 = std::env::var("PORT").expect("PORT: the devShell sets the base of the port range").parse().expect("PORT is a u16");
	// Replay-only: the recording is over before the first request, so the last tick is addressable.
	viz.clone().seal();
	viz.serve_on(Viz::bind(base + ORDINAL).await).await;
}

/// Surfaces the `Signal` node's self-documentation through the observation choke point: prints its
/// formula, per-dep derivatives, and (behind `SIGNAL_TRACE`) the debug trace once; and asserts the
/// exact Jacobian agrees with the retained finite-difference one on every fired tick — the wiring's
/// acceptance test.
#[derive(Default)]
struct SignalDoc {
	shown: bool,
	checked: u64,
	max_rel: f64,
}
impl Observer for SignalDoc {
	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		if node.rsplit("::").next() != Some("Signal") {
			return;
		}
		if let (Some(jac), Some(exact)) = (fire.jac, fire.exact_jac) {
			for (fd, ex) in jac.iter().zip(exact) {
				if fd.is_finite() && ex.is_finite() {
					self.max_rel = self.max_rel.max((fd - ex).abs() / ex.abs().max(1e-9));
					self.checked += 1;
				}
			}
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
