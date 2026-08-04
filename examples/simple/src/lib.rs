#![feature(default_field_values)]
//! Idempotent data layer over one day of real Bybit TAO-USDT trades: download → parquet catalog →
//! typed trades. Each step is skipped if its artifact exists; any failure is a loud panic — no
//! fallbacks.

pub mod nodes;

use std::{
	fs,
	io::{BufRead as _, BufReader, Read as _},
	path::Path,
	str::FromStr as _,
};

use trading_data::{Catalog, ExchangeName, Feather, Instrument, Pair, Precision, PrecisionPriceQty, Row as _, Side, Symbol, Trade, TradeBuf, Ts, Venue, read_trades};

const DAY: &str = "2025-01-03";
const BYBIT_SYMBOL: &str = "TAOUSDT";
const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(4),
	qty: Precision(3),
};

pub fn symbol() -> Symbol {
	Symbol::new(Pair::from_str("TAO-USDT").expect("static pair"), Instrument::Perp)
}

/// Idempotent: downloads the day's archive and ingests it into a parquet catalog under
/// `cache`, skipping any step whose artifact already exists.
pub fn ensure_catalog(cache: &Path) -> Catalog {
	fs::create_dir_all(cache).expect("create cache dir");

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

/// UTC bounds `[start, end)` of the day — the Replay range, on the venue axis the trade lane is
/// stored against.
pub fn day_bounds() -> (Ts<Venue>, Ts<Venue>) {
	let date = jiff::civil::Date::from_str(DAY).expect("static date");
	let start = date.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(Ts::from_nanos(start), Ts::from_nanos(start + 24 * 3600 * 1_000_000_000))
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
	let mut day = TradeBuf::new(PREC);

	let file = fs::File::open(gz).expect("open archive");
	let mut reader = BufReader::new(flate2::read::GzDecoder::new(file));
	let mut line = String::new();
	reader.read_line(&mut line).expect("read header");
	assert!(line.starts_with("timestamp,symbol,side,size,price"), "unexpected header: {line}");

	let mut prev_ts = i64::MIN;
	for i in 0.. {
		line.clear();
		if reader.read_line(&mut line).expect("read line") == 0 {
			break;
		}
		let line = line.trim_end();
		let mut cols = line.split(',');
		let mut col = || cols.next().unwrap_or_else(|| panic!("malformed line {i}: {line}"));
		let ts_sec: f64 = col().parse().unwrap_or_else(|e| panic!("bad ts on line {i}: {e}"));
		assert_eq!(col(), BYBIT_SYMBOL, "foreign symbol on line {i}");
		let side: Side = col().parse().unwrap_or_else(|e| panic!("bad side on line {i}: {e}"));
		let qty_raw = PREC.qty.parse_u32(col());
		let price_raw = PREC.price.parse_i32(col());

		let ts = (ts_sec * 1e9).round() as i64;
		assert!(ts >= prev_ts, "trades not time-ordered at line {i}: {prev_ts} > {ts}");
		prev_ts = ts;

		// Historic ingest: no wire time, and we were not there to receive it.
		day.push(Ts::from_nanos(ts), None, None, i as u64, side, price_raw, qty_raw);
	}
	feather.extend(day.cols(0..day.len()));
	let path = feather.flush(catalog).expect("flush day of trades").expect("non-empty day");
	println!("ingested to {}", path.display());
}
