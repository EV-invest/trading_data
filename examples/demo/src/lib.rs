#![feature(default_field_values)]
//! Idempotent SPL demo data layer over one day of real Bybit TAO-USDT trades:
//! download → parquet catalog → typed trades, plus OI/MC lanes fetched live.
//! Each step is skipped if its artifact exists; any failure is a loud panic — no fallbacks.

pub mod nodes;

use std::{
	fs,
	io::{BufRead as _, BufReader, Read as _},
	path::Path,
	str::FromStr as _,
};

use trading_data::{Catalog, Feather, Mc, Oi, Row as _, Trade, read_mc, read_oi, read_trades};
use v_utils::trades::{Asset, ExchangeName, Instrument, Pair, PrecisionPriceQty, Side, Symbol};

const DAY: &str = "2025-01-03";
const BYBIT_SYMBOL: &str = "TAOUSDT";
const PREC: PrecisionPriceQty = PrecisionPriceQty { price: 4, qty: 3 };

pub fn symbol() -> Symbol {
	Symbol::new(Pair::from_str("TAO-USDT").expect("static pair"), Instrument::Perp)
}

pub fn asset() -> Asset {
	Asset::new("TAO")
}

/// Idempotent: downloads the day's archive and ingests it into a parquet catalog under
/// `cache`, skipping any step whose artifact already exists.
pub fn ensure_catalog(cache: &Path) -> Catalog {
	fs::create_dir_all(cache).expect("create demo cache dir");

	let gz = cache.join(format!("{BYBIT_SYMBOL}{DAY}.csv.gz"));
	if !gz.exists() {
		download(&gz);
	}

	let catalog = Catalog::new(cache.join("catalog"));
	let (day_start, day_end) = day_bounds();
	if read_trades(&catalog, ExchangeName::Bybit, symbol(), day_start, day_end)
		.expect("open trades lane")
		.next()
		.is_none()
	{
		ingest(&gz, &catalog);
	} else {
		println!("catalog already populated, skipping ingest");
	}
	catalog
}

/// UTC nanosecond bounds `[start, end)` of the demo day — the Replay range.
pub fn day_bounds() -> (i64, i64) {
	let date = jiff::civil::Date::from_str(DAY).expect("static date");
	let start = date.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(start, start + 24 * 3600 * 1_000_000_000)
}

/// Idempotent: fetches the demo day's Bybit open interest (5min ⇒ 288 rows ⇒ 2 pages) into the oi
/// lane so OI genuinely weaves into [`DAY`]. Historic ingest, so `ts_init = None`. Staging for
/// v_exchanges — self-contained and liftable.
pub fn ensure_oi(catalog: &Catalog) {
	if read_oi(catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX).expect("open oi lane").next().is_some() {
		println!("oi lane already populated, skipping fetch");
		return;
	}
	let (day_start, day_end) = day_bounds();
	let (start_ms, end_ms) = (day_start / 1_000_000, day_end / 1_000_000);

	let mut rows: Vec<Oi> = Vec::new();
	let mut cursor = String::new();
	loop {
		let mut url = format!("https://api.bybit.com/v5/market/open-interest?category=linear&symbol={BYBIT_SYMBOL}&intervalTime=5min&startTime={start_ms}&endTime={end_ms}&limit=200");
		if !cursor.is_empty() {
			url.push_str(&format!("&cursor={cursor}"));
		}
		println!("fetching {url}");
		let body = http_get(&url);
		let v: serde_json::Value = serde_json::from_slice(&body).expect("bybit oi json");
		assert_eq!(v["retCode"].as_i64(), Some(0), "bybit error: {v}");
		let list = v["result"]["list"].as_array().expect("oi list");
		for e in list {
			let ts_ms: i64 = e["timestamp"].as_str().expect("oi timestamp string").parse().expect("oi timestamp i64");
			if !(start_ms..end_ms).contains(&ts_ms) {
				continue;
			}
			let oi: f64 = e["openInterest"].as_str().expect("openInterest string").parse().expect("openInterest f64");
			rows.push(Oi {
				ts_event: ts_ms * 1_000_000,
				ts_init: None,
				oi,
			});
		}
		cursor = v["result"]["nextPageCursor"].as_str().unwrap_or("").to_string();
		if cursor.is_empty() || list.is_empty() {
			break;
		}
	}
	assert!(
		!rows.is_empty(),
		"Bybit returned no open interest inside {DAY} — its 5min OI retention likely doesn't reach that day. \
		 Surface this: the decision (not code) is either picking a recent day or dropping the Oi root."
	);
	rows.sort_by_key(|r| r.ts_event);
	rows.dedup_by_key(|r| r.ts_event);

	let mut feather = Feather::<Oi>::new(ExchangeName::Bybit, symbol(), Oi::POLICY);
	for row in rows {
		feather.push(row);
	}
	let path = feather.flush(catalog).expect("flush oi").expect("non-empty oi batch");
	println!("oi ingested to {}", path.display());
}

/// Idempotent: fetches current CoinGecko market cap into the mc lane. Staging for v_exchanges —
/// self-contained and liftable.
pub fn ensure_mc(catalog: &Catalog) {
	if read_mc(catalog, asset(), 0, i64::MAX).expect("open mc lane").next().is_some() {
		println!("mc lane already populated, skipping fetch");
		return;
	}
	let url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bittensor";
	println!("fetching {url}");
	let body = http_get(url);
	let v: serde_json::Value = serde_json::from_slice(&body).expect("coingecko json");
	let coin = v.as_array().and_then(|a| a.first()).expect("coingecko returned no coins");
	let market_cap = coin["market_cap"].as_f64().expect("market_cap f64");
	let rank = coin["market_cap_rank"].as_u64().map(|r| u32::try_from(r).expect("rank fits u32"));

	let now = jiff::Timestamp::now().as_nanosecond() as i64;
	let mut feather = Feather::<Mc>::new(asset(), Mc::POLICY);
	feather.push(Mc {
		ts_event: now,
		ts_init: None,
		market_cap,
		rank,
	});
	let path = feather.flush(catalog).expect("flush mc").expect("non-empty mc batch");
	println!("mc ingested to {}", path.display());
}

fn http_get(url: &str) -> Vec<u8> {
	let mut resp = ureq::get(url).call().unwrap_or_else(|e| panic!("GET {url}: {e}"));
	assert_eq!(resp.status(), 200, "GET {url} returned {}", resp.status());
	let mut body = Vec::new();
	resp.body_mut().as_reader().read_to_end(&mut body).expect("read body");
	body
}

fn download(gz: &Path) {
	let url = format!("https://public.bybit.com/trading/{BYBIT_SYMBOL}/{BYBIT_SYMBOL}{DAY}.csv.gz");
	println!("downloading {url}");
	let body = http_get(&url);
	assert!(body.len() > 1_000_000, "suspiciously small archive: {} bytes", body.len());
	let tmp = gz.with_extension("part");
	fs::write(&tmp, &body).expect("write archive");
	fs::rename(&tmp, gz).expect("move archive into place");
	println!("downloaded {} bytes", body.len());
}

fn ingest(gz: &Path, catalog: &Catalog) {
	let mut feather = Feather::<Trade>::new(ExchangeName::Bybit, symbol(), PREC, Trade::POLICY);
	let (p_scale, q_scale) = (10f64.powi(PREC.price as i32), 10f64.powi(PREC.qty as i32));

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
		let side: Side = col().parse().unwrap_or_else(|e| panic!("bad side on line {i}: {e}"));
		let qty_raw = decimal_raw(col(), PREC.qty, i);
		let price_raw = decimal_raw(col(), PREC.price, i);

		let ts = (ts_sec * 1e9).round() as i64;
		assert!(ts >= prev_ts, "trades not time-ordered at line {i}: {prev_ts} > {ts}");
		prev_ts = ts;

		// i64→f64 is exact at these magnitudes; feather's round-trip assert guards the rest.
		feather.push(Trade {
			ts_event: ts,
			ts_init: None,
			monotonic_seq: i as u64,
			trade_id: i as u64,
			side,
			price: price_raw as f64 / p_scale,
			qty: qty_raw as f64 / q_scale,
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
