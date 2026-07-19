//! Assert-driver over the demo data layer: catalog → `Replay` weaves trades + OI → batch-native
//! GAT sweep → classifications. Batching must not alter fold order, so the day-end counts are
//! bit-identical to the pre-batch run — the asserts below are the integration test.

use std::{path::PathBuf, time::Duration};

use trading_data::{Batch, Feed, LatencyConfig, Mc, Oi, Replay, read_mc, read_oi, required_lanes};
use trading_data_demo::{
	asset, day_bounds, ensure_catalog, ensure_mc, ensure_oi,
	nodes::{Batches, Category, Graph},
	symbol,
};
use v_utils::trades::ExchangeName;

fn main() {
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
	let (mut n_trades, mut bars, mut hits, mut classifications, mut lambda_fires, mut oi_consumed) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
	let (mut cvd, mut vol1h) = (0.0f64, 0.0f64);
	let (mut atr_end, mut lambda_end, mut oi_chg) = (None, None, None);

	while let Some(b) = feed.next_batch() {
		let out = match b {
			Batch::Trades(ts) => {
				n_trades += ts.len() as u64;
				graph.tick(Batches { trades: ts, ..Default::default() })
			}
			Batch::Oi(os) => {
				oi_consumed += os.len() as u64;
				graph.tick(Batches { oi: os, ..Default::default() })
			}
			other => panic!("unexpected lane in demo replay: {other:?}"),
		};

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
				"{ts} {:?} dist={:?} o={:.3} c={:.3} mom={:.2} rsi={:.1} atr={:.4} vol1h={:.0} flow={:.0} cvd={:.0} λ={:.3e} oiΔ={:?}",
				Category::argmax(dist.0),
				dist.0,
				bar.open,
				bar.close,
				out.momentum[i].expect("warm"),
				out.rsi[i].expect("warm"),
				out.atr[i].expect("warm"),
				out.vol_usd_1h[i],
				bar.flow_quote,
				cvd,
				out.lambda[i].expect("warm"),
				oi_chg,
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

	let oi: Vec<Oi> = read_oi(&catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX).expect("open oi lane").collect();
	assert!(!oi.is_empty(), "oi lane empty after ensure_oi");
	assert!(oi.windows(2).all(|w| w[0].ts_event <= w[1].ts_event), "oi timestamps unordered");
	let mc: Vec<Mc> = read_mc(&catalog, asset(), 0, i64::MAX).expect("open mc lane").collect();
	assert!(!mc.is_empty(), "mc lane empty after ensure_mc");
	assert!(mc.windows(2).all(|w| w[0].ts_event <= w[1].ts_event), "mc timestamps unordered");
	println!("oi rows={} mc rows={} (mc={:.3e} rank={:?})", oi.len(), mc.len(), mc[0].market_cap, mc[0].rank);

	println!("demo: ok");
}
