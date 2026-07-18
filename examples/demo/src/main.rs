//! Assert-driver over the demo data layer: catalog → prints → GAT sweep → classifications.

use std::path::PathBuf;

use trading_data_demo::{
	ensure_catalog, load_prints,
	nodes::{Category, Graph},
};

fn main() {
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/demo_cache"));
	let catalog = ensure_catalog(&cache);

	let mut graph = Graph::default();
	let (mut prints, mut bars, mut hits, mut classifications) = (0u64, 0u64, 0u64, 0u64);
	let mut last = None;
	for print in load_prints(&catalog) {
		prints += 1;
		let out = graph.tick(Some(print));
		bars += out.bar.is_some() as u64;
		hits += (out.screener == Some(true)) as u64;
		if let Some(dist) = out.classified {
			classifications += 1;
			let bar = out.bar.expect("Classify only fires on bar close");
			let ts = jiff::Timestamp::from_nanosecond(bar.ts_open as i128).expect("ts in range");
			println!(
				"{ts} {:?} dist={:?} o={:.3} c={:.3} mom={:.2} rsi={:.1} atr={:.4} vol1h={:.0}",
				Category::argmax(dist.0),
				dist.0,
				bar.open,
				bar.close,
				out.momentum.expect("warm"),
				out.rsi.expect("warm"),
				out.atr.expect("warm"),
				out.vol_usd_1h
			);
		}
		last = Some(out);
	}
	let last = last.expect("day produced no trades");
	println!("prints={prints} bars={bars} screener_hits={hits} classifications={classifications}");
	println!("leaf levels at day end: atr={:?} vol1h={:.0}", last.atr, last.vol_usd_1h);

	assert!(prints > 0);
	assert!((1400..=1440).contains(&bars), "bars={bars} outside expected range for one full day");
	assert!(hits >= 1, "screener never fired — thresholds need lowering");
	println!("demo: ok");
}
