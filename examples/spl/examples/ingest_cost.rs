//! Wall clock over one real day of archives, for the ingest optimizations.
//!
//! The ingest path has no bench and cannot get an honest one: a microbenchmark of it would measure
//! the page cache, and its two hot loops are private to the pump. So this drives the whole of
//! [`ensure_lanes`] over a scratch cache seeded with real archives and prints one number. Every
//! ingest-side change is measured as a delta on it.
//!
//! ```sh
//! d=2025-01-03; c=$(mktemp -d)
//! ln -s $PWD/tmp/spl_cache/TAOUSDT$d.csv.gz $c/
//! ln -s $PWD/tmp/spl_cache/${d}_TAOUSDT_ob500.data.zip $c/${d}_TAOUSDT_ob200.data.zip
//! cargo r --release --example ingest_cost -- $c
//! ```
//!
//! Seed the scratch catalog's `oi` and `mc` lanes too — both are HTTP fetches nothing here touches,
//! and CoinGecko's free tier will not serve a date this old at all. `mc` is one row a day, so the
//! harness writes its own rather than asking for one.

use std::{path::PathBuf, time::Instant};

use trading_data::{Asset, Catalog, Feather, Mc, Row as _, Ts};
use trading_data_spl::{config::Situation, day_bounds, ensure_lanes};

#[tokio::main]
async fn main() {
	let cache = PathBuf::from(std::env::args().nth(1).expect("usage: ingest_cost <cache-dir>"));
	let s = Situation {
		pair: "TAO-USDT".into(),
		coingecko_id: "bittensor".into(),
		start: "2025-01-03".parse().expect("start date"),
		end: "2025-01-04".parse().expect("end date"),
	};

	let mut mc = Feather::<Mc>::new(Asset::from("TAO"), Mc::POLICY);
	mc.push(Mc {
		ts_local_exec: Ts::from_nanos(day_bounds(s.start).0.as_nanos()),
		market_cap: 4.4e9,
		rank: None,
	});
	mc.flush(&Catalog::new(cache.join("catalog"))).expect("flush mc").expect("non-empty mc batch");

	let t = Instant::now();
	let catalog = ensure_lanes(&cache, &s).await;
	println!("ingest: {:.2?} into {}", t.elapsed(), catalog.root().display());
}
