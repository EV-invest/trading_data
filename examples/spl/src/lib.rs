#![feature(default_field_values)]
//! Idempotent SPL data layer: 32 days of real Bybit TAO-USDT trades (the Sharpe(180) 4h horizon),
//! the measured day's L20 book off the historic ob500 archive, and the measured day's OI/MC.
//! Each step is skipped if its artifact exists; any failure is a loud panic — no fallbacks.
//!
//! Staging for v_exchanges — self-contained and liftable, like `examples/demo`'s.

pub mod nodes;

use std::{
	collections::BTreeMap,
	fs,
	io::{BufRead as _, BufReader, Read as _},
	path::{Path, PathBuf},
	str::FromStr as _,
	sync::{
		Arc,
		atomic::{AtomicI64, Ordering},
	},
};

use trading_data::{Aggregate, BookShape, BookUpdate, Catalog, Clock, Feather, Feed as _, Live, Mc, Oi, Row as _, Sink, Span, Trade, TradeBuf, read_mc, read_oi, read_trades};
use trading_data_core::{Asset, ExchangeName, Instrument, Local, Pair, PrecisionPriceQty, Side, Symbol, Ts, Venue};

/// First day of the trades range. `Momentum`'s Sharpe needs 181 closed 4h bars ≈ 30.2 days, so the
/// horizon is what makes the measured day's screeners warm at all — shortening it is not a knob.
const RANGE_START: &str = "2024-12-03";
/// The one day carrying book, OI and MC; everything before it is trades-only warmup.
pub const MEASURED_DAY: &str = "2025-01-03";
const BYBIT_SYMBOL: &str = "TAOUSDT";
const PREC: PrecisionPriceQty = PrecisionPriceQty { price: 4, qty: 3 };
/// SPL's `OrderBookActor::DEPTH`. The archive carries 500 levels a side; the strategy reads 20.
const DEPTH: usize = 20;
/// CloudFront 403s reqwest/ureq's default agent string on the quote-saver host; any custom one passes.
const UA: &str = "trading_data_spl";

pub fn symbol() -> Symbol {
	Symbol::new(Pair::from_str("TAO-USDT").expect("static pair"), Instrument::Perp)
}

pub fn asset() -> Asset {
	Asset::new("TAO")
}

/// Every day of the run, warmup first, measured last.
pub fn days() -> Vec<jiff::civil::Date> {
	let (mut d, last) = (date(RANGE_START), date(MEASURED_DAY));
	let mut out = Vec::new();
	while d <= last {
		out.push(d);
		d = d.tomorrow().expect("date in range");
	}
	out
}

/// UTC bounds `[start, end)` of one day — the Replay range, on the venue axis the lanes are stored
/// against.
pub fn day_bounds(d: jiff::civil::Date) -> (Ts<Venue>, Ts<Venue>) {
	let start = d.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(Ts::from_nanos(start), Ts::from_nanos(start + 24 * 3600 * 1_000_000_000))
}

/// Idempotent per day: downloads each day's archive and ingests it into a parquet catalog under
/// `cache`, skipping any day already present.
pub fn ensure_trades(cache: &Path) -> Catalog {
	fs::create_dir_all(cache).expect("create spl cache dir");
	let catalog = Catalog::new(cache.join("catalog"));
	for d in days() {
		let gz = cache.join(format!("{BYBIT_SYMBOL}{d}.csv.gz"));
		if !gz.exists() {
			download(&gz, &format!("https://public.bybit.com/trading/{BYBIT_SYMBOL}/{BYBIT_SYMBOL}{d}.csv.gz"));
		}
		let (start, end) = day_bounds(d);
		if read_trades(&catalog, ExchangeName::Bybit, symbol(), start, end).expect("open trades lane").next().is_some() {
			continue;
		}
		ingest_trades(&gz, &catalog);
	}
	catalog
}
/// Idempotent: folds the measured day's ob500 archive into the catalog's book lanes through
/// [`Live`]'s recording tee — the same delta+checkpoint pair a live session writes, so `Replay`
/// reads it back on the path `examples/live` asserts is identical.
///
/// ponytail: the delta lane has no public reader, so ingest is gated on a sentinel file rather than
/// a lane probe. Delete `.book_ingested` to force a re-ingest.
pub fn ensure_book(cache: &Path, catalog: &Catalog) {
	let sentinel = cache.join(".book_ingested");
	if sentinel.exists() {
		println!("book lane already populated, skipping ingest");
		return;
	}
	let zip = cache.join(format!("{MEASURED_DAY}_{BYBIT_SYMBOL}_ob500.data.zip"));
	if !zip.exists() {
		download(
			&zip,
			&format!("https://quote-saver.bycsi.com/orderbook/linear/{BYBIT_SYMBOL}/{MEASURED_DAY}_{BYBIT_SYMBOL}_ob500.data.zip"),
		);
	}

	// The archive's own timestamps are the only clock this session has; `Live` stamps arrivals off
	// it, and the recorded reception it writes is what `Replay` weaves on.
	let clock = Arc::new(ArchiveClock(AtomicI64::new(0)));
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), PREC, true, clock.clone());
	let sink = live.sink();
	let pump = {
		let (zip, clock) = (zip.clone(), clock.clone());
		std::thread::spawn(move || pump_archive(&zip, &sink, &clock))
	};
	while live.next().is_some() {}
	let (emissions, levels) = pump.join().expect("archive pump panicked");
	fs::write(&sentinel, format!("{emissions} emissions, {levels} levels\n")).expect("write book sentinel");
	println!("book ingested: {emissions} 1s emissions, {levels} level rows");
}
/// Idempotent: fetches the measured day's Bybit open interest (5min ⇒ 288 rows ⇒ 2 pages) into the
/// oi lane. Historic ingest, so there is no local reading.
pub fn ensure_oi(catalog: &Catalog) {
	if read_oi(catalog, ExchangeName::Bybit, symbol(), Ts::MIN, Ts::MAX).expect("open oi lane").next().is_some() {
		println!("oi lane already populated, skipping fetch");
		return;
	}
	let (day_start, day_end) = day_bounds(date(MEASURED_DAY));
	let (start_ms, end_ms) = (day_start.as_nanos() / 1_000_000, day_end.as_nanos() / 1_000_000);

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
				ts_venue_exec: Ts::from_nanos(ts_ms * 1_000_000),
				ts_venue_send: None,
				ts_local_recv: None,
				oi,
			});
		}
		// An absent cursor would end pagination early and leave a silently short day.
		cursor = v["result"]["nextPageCursor"].as_str().expect("bybit paginated responses always carry nextPageCursor").to_string();
		if cursor.is_empty() || list.is_empty() {
			break;
		}
	}
	rows.sort_by_key(|r| r.ts_venue_exec);
	rows.dedup_by_key(|r| r.ts_venue_exec);
	// A UTC day is exactly 288 five-minute buckets. Anything less is a hole in the input — most
	// likely Bybit's 5min OI retention no longer reaching {MEASURED_DAY}. The decision is data, not
	// code: pick a recent day, or drop the Oi root from the graph.
	assert_eq!(rows.len(), 288, "Bybit returned {} of 288 five-minute OI readings inside {MEASURED_DAY}", rows.len());

	let mut feather = Feather::<Oi>::new(ExchangeName::Bybit, symbol(), Oi::POLICY);
	for row in rows {
		feather.push(row);
	}
	let path = feather.flush(catalog).expect("flush oi").expect("non-empty oi batch");
	println!("oi ingested to {}", path.display());
}
/// Idempotent: fetches [`MEASURED_DAY`]'s CoinGecko market cap into the mc lane. `Mc` is part of
/// SPL's universal shallow set, so a missing reading is missing input, not a degraded mode — this
/// panics with the vendor's own error and the decisions available.
pub fn ensure_mc(catalog: &Catalog) {
	if read_mc(catalog, asset(), Ts::MIN, Ts::MAX).expect("open mc lane").next().is_some() {
		println!("mc lane already populated, skipping fetch");
		return;
	}
	let day = date(MEASURED_DAY);
	let url = format!("https://api.coingecko.com/api/v3/coins/bittensor/history?date={day}&localization=false");
	println!("fetching {url}");
	let (status, body) = http_try(&url);
	let v: serde_json::Value = serde_json::from_slice(&body).expect("coingecko json");
	let market_cap = v["market_data"]["market_cap"]["usd"].as_f64().unwrap_or_else(|| {
		panic!(
			"CoinGecko has no market cap for {MEASURED_DAY} (HTTP {status}): {}\n  \
			 `Mc` is a required shallow field — SPL's universal indie set — so the graph cannot warm without it, \
			 and nothing downstream of `Shallow` will ever fire. Fix the data, not this code: supply a CoinGecko \
			 Pro key (the free tier serves only the last 365 days), move `MEASURED_DAY` inside that window, or \
			 drop the `Mc` root from the graph and `mc` from `ShallowSnap`.",
			v["error"]["status"]["error_message"]
				.as_str()
				.or_else(|| v["status"]["error_message"].as_str())
				.unwrap_or("200 with no market_data.market_cap.usd in it")
		)
	});

	// The history endpoint attests the 00:00 UTC snapshot of the requested date, and reports no rank.
	let (start, _) = day_bounds(day);
	let mut feather = Feather::<Mc>::new(asset(), Mc::POLICY);
	feather.push(Mc {
		ts_local_exec: Ts::from_nanos(start.as_nanos()),
		market_cap,
		rank: None,
	});
	let path = feather.flush(catalog).expect("flush mc").expect("non-empty mc batch");
	println!("mc ingested to {}", path.display());
}
fn date(s: &str) -> jiff::civil::Date {
	jiff::civil::Date::from_str(s).expect("static date")
}

/// The pump's cursor over archive time, read by [`Live`] on the consuming thread.
///
/// ponytail: the two threads are unsynchronised, so a fast pump can leave the clock a few emissions
/// ahead of the event being stamped. Harmless — replay reorders on the *recorded* reception, which
/// is the stamp itself, so live≡replay holds regardless; the lead only jitters the 60s checkpoint
/// cadence. Rendezvous the two if that ever needs to be exact.
struct ArchiveClock(AtomicI64);

impl Clock for ArchiveClock {
	fn now_ns(&self) -> i64 {
		self.0.load(Ordering::Relaxed)
	}
}

/// Folds the whole ob500 stream into a local book and emits, once per second, the diff of the
/// top-`DEPTH`-per-side view against the last emitted one — levels dropping out of it as `qty = 0`
/// deletes. Returns `(emissions, level rows)`.
///
/// ponytail: L20 @ 1s is exactly what SPL's `OrderBookActor` consumes, and it is also the ceiling —
/// a node needing sub-second or deeper book raises `DEPTH` or the bucket here, at a linear cost in
/// delta-lane rows (the full 500-level stream is ~40× this and blows `Replay`'s eager buffer).
fn pump_archive(zip: &Path, sink: &Sink, clock: &ArchiveClock) -> (u64, u64) {
	let file = fs::File::open(zip).expect("open book archive");
	let mut archive = zip::ZipArchive::new(file).expect("book archive is a zip");
	assert_eq!(archive.len(), 1, "ob500 archive holds exactly one jsonl member");
	let entry = archive.by_index(0).expect("open zip member");

	let (mut bids, mut asks) = (BTreeMap::new(), BTreeMap::new());
	let (mut top_bids, mut top_asks) = (BTreeMap::new(), BTreeMap::new());
	let (mut cur_sec, mut last_ns) = (None, 0i64);
	let (mut emissions, mut levels) = (0u64, 0u64);

	for (i, line) in BufReader::new(entry).lines().enumerate() {
		let line = line.expect("read archive line");
		let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_else(|e| panic!("malformed archive line {i}: {e}"));
		let ts_ns = v["ts"].as_i64().unwrap_or_else(|| panic!("no ts on archive line {i}")) * 1_000_000;
		let sec = ts_ns.div_euclid(1_000_000_000);
		if cur_sec.is_some_and(|c| sec > c) {
			let n = emit(sink, clock, &bids, &asks, &mut top_bids, &mut top_asks, last_ns);
			emissions += u64::from(n > 0);
			levels += n;
		}
		cur_sec = Some(sec);

		let kind = v["type"].as_str().unwrap_or_else(|| panic!("no type on archive line {i}"));
		match kind {
			"snapshot" => {
				bids.clear();
				asks.clear();
			}
			"delta" => {}
			other => panic!("unknown archive record type `{other}` on line {i}"),
		}
		apply(&mut bids, &v["data"]["b"], i);
		apply(&mut asks, &v["data"]["a"], i);
		last_ns = ts_ns;
	}
	if cur_sec.is_some() {
		let n = emit(sink, clock, &bids, &asks, &mut top_bids, &mut top_asks, last_ns);
		emissions += u64::from(n > 0);
		levels += n;
	}
	(emissions, levels)
}

fn apply(side: &mut BTreeMap<i32, u32>, levels: &serde_json::Value, line: usize) {
	for l in levels.as_array().unwrap_or_else(|| panic!("book side is not an array on line {line}")) {
		let price = decimal_raw(l[0].as_str().unwrap_or_else(|| panic!("price is not a string on line {line}")), PREC.price, line);
		let qty = decimal_raw(l[1].as_str().unwrap_or_else(|| panic!("qty is not a string on line {line}")), PREC.qty, line);
		let price: i32 = price.try_into().unwrap_or_else(|_| panic!("book price out of i32 range on line {line}"));
		let qty: u32 = qty.try_into().unwrap_or_else(|_| panic!("book qty out of u32 range on line {line}"));
		match qty {
			0 => {
				side.remove(&price);
			}
			q => {
				side.insert(price, q);
			}
		}
	}
}

/// The top-`DEPTH` diff for one second. Returns the level rows emitted; `0` = the view is unchanged
/// and there is nothing to say.
fn emit(sink: &Sink, clock: &ArchiveClock, bids: &BTreeMap<i32, u32>, asks: &BTreeMap<i32, u32>, top_bids: &mut BTreeMap<i32, u32>, top_asks: &mut BTreeMap<i32, u32>, ts_ns: i64) -> u64 {
	let next_bids: BTreeMap<i32, u32> = bids.iter().rev().take(DEPTH).map(|(&p, &q)| (p, q)).collect();
	let next_asks: BTreeMap<i32, u32> = asks.iter().take(DEPTH).map(|(&p, &q)| (p, q)).collect();
	let diff = |old: &BTreeMap<i32, u32>, new: &BTreeMap<i32, u32>| {
		let mut out: BTreeMap<i32, u32> = new.iter().filter(|(p, q)| old.get(p) != Some(q)).map(|(&p, &q)| (p, q)).collect();
		out.extend(old.keys().filter(|p| !new.contains_key(p)).map(|&p| (p, 0)));
		out
	};
	let (d_bids, d_asks) = (diff(top_bids, &next_bids), diff(top_asks, &next_asks));
	let n = (d_bids.len() + d_asks.len()) as u64;
	if n == 0 {
		return 0;
	}
	*top_bids = next_bids;
	*top_asks = next_asks;

	// `local_recv` here is a placeholder: `Live` overwrites reception with its own ingest stamp.
	let ts = Ts::<Venue>::from_nanos(ts_ns);
	let shape = BookShape {
		ts: Aggregate {
			venue_exec: Span::at(ts),
			local_recv: Span::at(Ts::<Local>::from_nanos(ts_ns)),
		},
		prec: PREC,
		bids: d_bids,
		asks: d_asks,
	};
	clock.0.store(ts_ns, Ordering::Relaxed);
	// Our own fold, so there is no venue sequence to have broken.
	sink.book(BookUpdate::BatchDelta { shape, gapped: false });
	n
}

fn http_get(url: &str) -> Vec<u8> {
	let (status, body) = http_try(url);
	assert_eq!(status, 200, "GET {url} returned {status}");
	body
}

/// The status alongside the body, for the one caller whose failure message is the vendor's own
/// error text rather than the status line — which needs the 4xx body, not a status-as-error.
fn http_try(url: &str) -> (u16, Vec<u8>) {
	let mut resp = ureq::get(url)
		.config()
		.http_status_as_error(false)
		.build()
		.header("user-agent", UA)
		.call()
		.unwrap_or_else(|e| panic!("GET {url}: {e}"));
	let status = resp.status().as_u16();
	let mut body = Vec::new();
	resp.body_mut().as_reader().read_to_end(&mut body).expect("read body");
	(status, body)
}

fn download(to: &PathBuf, url: &str) {
	println!("downloading {url}");
	let body = http_get(url);
	assert!(body.len() > 1_000_000, "suspiciously small archive: {} bytes", body.len());
	let tmp = to.with_extension("part");
	fs::write(&tmp, &body).expect("write archive");
	fs::rename(&tmp, to).expect("move archive into place");
	println!("downloaded {} bytes", body.len());
}

fn ingest_trades(gz: &Path, catalog: &Catalog) {
	let mut feather = Feather::<Trade>::new(ExchangeName::Bybit, symbol(), PREC, Trade::POLICY);
	let mut day = TradeBuf::new(PREC);

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

		// Historic ingest: no wire time, and we were not there to receive it.
		day.push(
			Ts::from_nanos(ts),
			None,
			None,
			i as u64,
			side,
			price_raw.try_into().unwrap_or_else(|_| panic!("price out of i32 range on line {i}")),
			qty_raw.try_into().unwrap_or_else(|_| panic!("qty out of u32 range on line {i}")),
		);
	}
	feather.extend(day.cols(0..day.len()));
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
