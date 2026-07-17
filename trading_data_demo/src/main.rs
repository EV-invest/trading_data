//! Idempotent SPL demo over one day of real Bybit TAO-USDT trades:
//! download → parquet catalog → typed replay → GAT sweep → classifications.
//! Each step is skipped if its artifact exists; any failure is a loud panic — no fallbacks.

mod nodes;

use std::{
	fs,
	io::{BufRead as _, BufReader, Read as _},
	path::{Path, PathBuf},
	str::FromStr as _,
};

use nodes::{Graph, Print};
use trading_data::{Catalog, Feather, FileMetadata, Lane, LaneKey, RotationPolicy, Trade, read_trades};
use v_utils::trades::{ExchangeName, Instrument, Pair, Symbol};

const DAY: &str = "2025-01-03";
const BYBIT_SYMBOL: &str = "TAOUSDT";
const PRICE_PREC: u8 = 4;
const QTY_PREC: u8 = 3;

fn main() {
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../tmp/demo_cache"));
	fs::create_dir_all(&cache).expect("create demo cache dir");

	let gz = cache.join(format!("{BYBIT_SYMBOL}{DAY}.csv.gz"));
	if !gz.exists() {
		download(&gz);
	}

	let catalog = Catalog::new(cache.join("catalog"));
	let pair = Pair::from_str("TAO-USDT").expect("static pair");
	let symbol = Symbol::new(pair, Instrument::Perp);
	let key = LaneKey::book(Lane::Trades, ExchangeName::Bybit, symbol);
	if catalog.list(&key).expect("list trades lane").is_empty() {
		ingest(&gz, &catalog, key.clone());
	} else {
		println!("catalog already populated, skipping ingest");
	}

	let day_start = day_bounds().0;
	let day_end = day_bounds().1;
	let mut graph = Graph::default();
	let (mut prints, mut bars, mut hits, mut classifications) = (0u64, 0u64, 0u64, 0u64);
	let mut last = None;
	for t in read_trades(&catalog, ExchangeName::Bybit, symbol, day_start, day_end).expect("open trades lane") {
		prints += 1;
		let print = Print {
			ts: t.ts_event,
			price: t.price_raw as f64 / 10f64.powi(PRICE_PREC as i32),
			qty: t.qty_raw as f64 / 10f64.powi(QTY_PREC as i32),
		};
		let out = graph.tick(Some(print));
		bars += out.bar.is_some() as u64;
		hits += (out.screener == Some(true)) as u64;
		if let Some(c) = out.classified {
			classifications += 1;
			let bar = out.bar.expect("Classify only fires on bar close");
			let ts = jiff::Timestamp::from_nanosecond(bar.ts_open as i128).expect("ts in range");
			println!(
				"{ts} {:?} dist={:?} o={:.3} c={:.3} mom={:.2} rsi={:.1} atr={:.4} vol1h={:.0}",
				c.category,
				c.dist,
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
	assert!(hits >= 1, "screener never fired — thresholds need lowering for {DAY}");
	println!("demo: ok");
}

fn day_bounds() -> (i64, i64) {
	let date = jiff::civil::Date::from_str(DAY).expect("static date");
	let start = date.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(start, start + 24 * 3600 * 1_000_000_000)
}

fn download(gz: &Path) {
	let url = format!("https://public.bybit.com/trading/{BYBIT_SYMBOL}/{BYBIT_SYMBOL}{DAY}.csv.gz");
	println!("downloading {url}");
	let mut resp = ureq::get(&url).call().expect("bybit download");
	assert_eq!(resp.status(), 200, "bybit returned {}", resp.status());
	let mut body = Vec::new();
	resp.body_mut().as_reader().read_to_end(&mut body).expect("read body");
	assert!(body.len() > 1_000_000, "suspiciously small archive: {} bytes", body.len());
	let tmp = gz.with_extension("part");
	fs::write(&tmp, &body).expect("write archive");
	fs::rename(&tmp, gz).expect("move archive into place");
	println!("downloaded {} bytes", body.len());
}

fn ingest(gz: &Path, catalog: &Catalog, key: LaneKey) {
	let meta = FileMetadata {
		exchange: ExchangeName::Bybit.to_string(),
		pair: "TAO-USDT".into(),
		price_precision: PRICE_PREC,
		qty_precision: QTY_PREC,
	};
	let mut feather = Feather::new_trades(key, meta, RotationPolicy::trades());

	let file = fs::File::open(gz).expect("open archive");
	let mut lines = BufReader::new(flate2::read::GzDecoder::new(file)).lines();
	let header = lines.next().expect("empty archive").expect("read header");
	assert!(header.starts_with("timestamp,symbol,side,size,price"), "unexpected header: {header}");

	let mut prev_ts = i64::MIN;
	for (i, line) in lines.enumerate() {
		let line = line.expect("read line");
		let mut cols = line.split(',');
		let mut col = || cols.next().unwrap_or_else(|| panic!("malformed line {i}: {line}"));
		let ts_sec: f64 = col().parse().unwrap_or_else(|e| panic!("bad ts on line {i}: {e}"));
		assert_eq!(col(), BYBIT_SYMBOL, "foreign symbol on line {i}");
		let side = match col() {
			"Buy" => 0u8,
			"Sell" => 1,
			s => panic!("bad side {s:?} on line {i}"),
		};
		let qty_raw = decimal_raw(col(), QTY_PREC, i);
		let price_raw = decimal_raw(col(), PRICE_PREC, i);

		let ts = (ts_sec * 1e9).round() as i64;
		assert!(ts >= prev_ts, "trades not time-ordered at line {i}: {prev_ts} > {ts}");
		prev_ts = ts;

		feather.push_trade(Trade {
			ts_event: ts,
			ts_init: ts,
			monotonic_seq: i as u64,
			trade_id: i as u64,
			side,
			price_raw: i32::try_from(price_raw).unwrap_or_else(|_| panic!("price_raw overflow on line {i}")),
			qty_raw: u32::try_from(qty_raw).unwrap_or_else(|_| panic!("qty_raw overflow on line {i}")),
		});
	}
	let path = feather.flush(catalog).expect("flush day of trades").expect("non-empty day");
	println!("ingested to {}", path.display());
}

/// Exact scaled-integer parse: no float round-trip, so precision violations panic instead of
/// silently truncating.
fn decimal_raw(s: &str, prec: u8, line: usize) -> i64 {
	let (int, frac) = s.split_once('.').unwrap_or((s, ""));
	assert!(frac.len() <= prec as usize, "more than {prec} decimals on line {line}: {s}");
	let int: i64 = int.parse().unwrap_or_else(|e| panic!("bad decimal {s:?} on line {line}: {e}"));
	let frac_val: i64 = if frac.is_empty() {
		0
	} else {
		frac.parse().unwrap_or_else(|e| panic!("bad decimal {s:?} on line {line}: {e}"))
	};
	let scale = 10i64.pow(prec as u32);
	int * scale + frac_val * 10i64.pow(prec as u32 - frac.len() as u32)
}
