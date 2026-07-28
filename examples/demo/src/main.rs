//! Assert-driver over the demo data layer: catalog → `Replay` weaves trades + OI → batch-native
//! GAT sweep → classifications. Batching must not alter fold order, so the day-end counts are
//! bit-identical to the pre-batch run — the asserts below are the integration test.
//!
//! The same sweep is recorded by an attached [`Viz`] and served afterwards, so the day is
//! browsable tick-by-tick. This example owns the runtime; exec_viz is only a library. `nix run
//! .#demo` builds the `exec_viz_web` bundle and runs this against it — a bare `cargo r` needs
//! `EXEC_VIZ_WEB_DIR` and `PORT` set by hand.

use std::{path::PathBuf, time::Duration};

use exec_viz::{Viz, api_types::BarOut};
use trading_data::{Feed, Fire, LatencyConfig, Mc, Observer, Oi, Replay, Ts, read_mc, read_oi, required_lanes};
use trading_data_core::ExchangeName;
use trading_data_demo::{
	asset, day_bounds, ensure_catalog, ensure_mc, ensure_oi,
	nodes::{Category, Graph},
	symbol,
};

/// This app's slot in the devShell's `PORT` range — the devShell owns the base, each app claims a
/// slot in it, so several can be up at once.
const ORDINAL: u16 = 1;
/// Retained ticks. `Replay` weaves the day into ~600 batches, so the whole run fits comfortably;
/// the cap is only there so nothing unbounded rides on a feed's batching.
const SCROLLBACK: usize = 20_000;

#[tokio::main]
async fn main() {
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/demo_cache"));
	let catalog = ensure_catalog(&cache);
	ensure_oi(&catalog);
	ensure_mc(&catalog);

	let lanes = required_lanes::<Graph>();
	println!("required lanes: {lanes:?}");
	let latency = LatencyConfig {
		p68: Duration::from_millis(5),
		p95: Duration::from_millis(20),
		p997: Duration::from_millis(80),
		seed: 0,
	};
	let (day_start, day_end) = day_bounds();
	let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(), day_start, day_end, &lanes, latency);

	let mut graph = Graph::default();
	let viz = Viz::new(Some("Bar1m"), SCROLLBACK, 60_000);
	// `Signal`'s exact/FD agreement check and the viz recording are two readings of one sweep.
	let mut obs = (SignalDoc::default(), viz.clone());
	let (mut n_trades, mut bars, mut hits, mut classifications, mut lambda_fires, mut oi_consumed) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
	let (mut cvd, mut vol1h) = (0.0f64, 0.0f64);
	let (mut atr_end, mut lambda_end, mut oi_chg) = (None, None, None);

	while let Some(lanes) = feed.next() {
		n_trades += lanes.trades.len() as u64;
		oi_consumed += lanes.oi.len() as u64;
		obs.1.at(lanes.ts_venue.as_nanos());
		let out = graph.tick_obs(lanes.into(), &mut obs);

		for b in out.bar {
			viz.bar(BarOut {
				ts_ms: b.ts_open / 1_000_000,
				open: b.open,
				high: b.high,
				low: b.low,
				close: b.close,
				volume: b.vol_quote,
			});
		}
		bars += out.bar.len() as u64;
		hits += out.screener.iter().filter(|&&h| h).count() as u64;
		lambda_fires += out.lambda.iter().filter(|o| o.is_some()).count() as u64;
		for l in out.lambda.iter().flatten() {
			assert!(l.is_finite(), "lambda went non-finite: {l}");
		}
		// cross-rate levels: last element is the current running/level view.
		if let Some(&c) = out.cvd.last() {
			cvd = c;
		}
		if let Some(&v) = out.vol_usd_1h.last() {
			vol1h = v;
		}
		if let Some(&a) = out.atr.last() {
			atr_end = a;
		}
		if let Some(&l) = out.lambda.last() {
			lambda_end = l;
		}
		if let Some(&o) = out.oi_change.last() {
			oi_chg = o;
		}

		// classified prints re-derive their per-bar context by zipping the same-rate bar slices.
		assert_eq!(out.classified.len(), out.bar.len(), "Classify/Bar rate mismatch");
		for i in 0..out.classified.len() {
			let Some(dist) = out.classified[i] else { continue };
			classifications += 1;
			let bar = out.bar[i];
			let ts = jiff::Timestamp::from_nanosecond(bar.ts_open as i128).expect("ts in range");
			println!(
				"{ts} {:?} dist={:?} o={:.3} c={:.3} mom={:.2} rsi={:.1} atr={:.4} vol1h={:.0} flow={:.0} cvd={cvd:.0} λ={:.3e} oiΔ={oi_chg:?}",
				Category::argmax(dist.0),
				dist.0,
				bar.open,
				bar.close,
				out.momentum[i].expect("warm"),
				out.rsi[i].expect("warm"),
				out.atr[i].expect("warm"),
				out.vol_usd_1h[i],
				bar.flow_quote,
				out.lambda[i].expect("warm"),
			);
		}
	}

	println!("trades={n_trades} bars={bars} screener_hits={hits} classifications={classifications} lambda_fires={lambda_fires} oi_consumed={oi_consumed}");
	println!("leaf levels at day end: atr={atr_end:?} vol1h={vol1h:.0} cvd={cvd:.0} λ={lambda_end:?}");

	// batching does not alter fold order ⇒ these are bit-identical to the pre-batch run.
	assert_eq!(n_trades, 270164, "trade count changed");
	assert_eq!(bars, 1439, "bar count changed");
	assert_eq!(hits, 135, "screener hit count changed");
	assert_eq!(classifications, 135, "classification count changed");
	assert!(lambda_fires >= 1, "lambda never fired");
	assert!(cvd != 0.0 && cvd.is_finite(), "day-end CVD degenerate: {cvd}");
	assert!(oi_consumed >= 1, "OiChange consumed no events — multi-lane replay broken");

	let doc = obs.0;
	println!("signal exact/FD agreement: checked={} max_rel={:.2e}", doc.checked, doc.max_rel);
	assert!(doc.checked > 0, "Signal never produced a finite Jacobian");
	assert!(doc.max_rel < 1e-3, "exact Jacobian disagrees with FD: max_rel={}", doc.max_rel);

	let oi: Vec<Oi> = read_oi(&catalog, ExchangeName::Bybit, symbol(), Ts::MIN, Ts::MAX).expect("open oi lane").collect();
	assert!(!oi.is_empty(), "oi lane empty after ensure_oi");
	assert!(oi.windows(2).all(|w| w[0].ts_venue_exec <= w[1].ts_venue_exec), "oi timestamps unordered");
	let mc: Vec<Mc> = read_mc(&catalog, asset(), Ts::MIN, Ts::MAX).expect("open mc lane").collect();
	assert!(!mc.is_empty(), "mc lane empty after ensure_mc");
	assert!(mc.windows(2).all(|w| w[0].ts_local_exec <= w[1].ts_local_exec), "mc timestamps unordered");
	println!("oi rows={} mc rows={} (mc={:.3e} rank={:?})", oi.len(), mc.len(), mc[0].market_cap, mc[0].rank);

	println!("demo: ok");
	let base: u16 = std::env::var("PORT").expect("PORT: the devShell sets the base of the port range").parse().expect("PORT is a u16");
	viz.serve(base + ORDINAL).await;
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
	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [&'static str], fire: Fire<'_>) {
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
