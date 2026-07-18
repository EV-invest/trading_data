//! Assert-driver over the demo data layer: catalog → trades → GAT sweep → classifications.

use std::path::PathBuf;

use trading_data::{Mc, Oi, read_mc, read_oi};
use trading_data_demo::{
	asset, ensure_catalog, ensure_mc, ensure_oi,
	nodes::{Category, Graph},
	symbol, trades,
};
use v_utils::trades::ExchangeName;

fn main() {
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/demo_cache"));
	let catalog = ensure_catalog(&cache);
	ensure_oi(&catalog);
	ensure_mc(&catalog);

	let mut graph = Graph::default();
	let (mut n_trades, mut bars, mut hits, mut classifications, mut lambda_fires) = (0u64, 0u64, 0u64, 0u64, 0u64);
	let mut last = None;
	for trade in trades(&catalog) {
		n_trades += 1;
		let out = graph.tick(Some(trade));
		bars += out.bar.is_some() as u64;
		hits += (out.screener == Some(true)) as u64;
		if let Some(l) = out.lambda {
			lambda_fires += 1;
			assert!(l.is_finite(), "lambda went non-finite: {l}");
		}
		if let Some(dist) = out.classified {
			classifications += 1;
			let bar = out.bar.expect("Classify only fires on bar close");
			let ts = jiff::Timestamp::from_nanosecond(bar.ts_open as i128).expect("ts in range");
			println!(
				"{ts} {:?} dist={:?} o={:.3} c={:.3} mom={:.2} rsi={:.1} atr={:.4} vol1h={:.0} flow={:.0} cvd={:.0} λ={:.3e}",
				Category::argmax(dist.0),
				dist.0,
				bar.open,
				bar.close,
				out.momentum.expect("warm"),
				out.rsi.expect("warm"),
				out.atr.expect("warm"),
				out.vol_usd_1h,
				bar.flow_quote,
				out.cvd.expect("every trade moves cvd"),
				out.lambda.expect("warm"),
			);
		}
		last = Some(out);
	}
	let last = last.expect("day produced no trades");
	let cvd = last.cvd.expect("every trade moves cvd");
	println!("trades={n_trades} bars={bars} screener_hits={hits} classifications={classifications} lambda_fires={lambda_fires}");
	println!("leaf levels at day end: atr={:?} vol1h={:.0} cvd={cvd:.0} λ={:?}", last.atr, last.vol_usd_1h, last.lambda);

	assert!(n_trades > 0);
	assert!((1400..=1440).contains(&bars), "bars={bars} outside expected range for one full day");
	assert!(hits >= 1, "screener never fired — thresholds need lowering");
	assert!(cvd != 0.0 && cvd.is_finite(), "day-end CVD degenerate: {cvd}");
	assert!(lambda_fires >= 1, "lambda never fired");

	let oi: Vec<Oi> = read_oi(&catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX).expect("open oi lane").collect();
	assert!(!oi.is_empty(), "oi lane empty after ensure_oi");
	assert!(oi.windows(2).all(|w| w[0].ts_event <= w[1].ts_event), "oi timestamps unordered");
	let mc: Vec<Mc> = read_mc(&catalog, asset(), 0, i64::MAX).expect("open mc lane").collect();
	assert!(!mc.is_empty(), "mc lane empty after ensure_mc");
	assert!(mc.windows(2).all(|w| w[0].ts_event <= w[1].ts_event), "mc timestamps unordered");
	println!("oi rows={} mc rows={} (mc={:.3e} rank={:?})", oi.len(), mc.len(), mc[0].market_cap, mc[0].rank);

	println!("demo: ok");
}
