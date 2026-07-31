#![feature(default_field_values)]
//! Idempotent SPL data layer over the window `config.nix` names: Bybit trades for every day of it,
//! and — for the trading days only, matching SPL's situation-gated Deep tier — the L20 book off the
//! historic ob200 archive plus OI and market cap. Each step is skipped if its artifact exists; any
//! failure is a loud panic, and missing input is a failure.
//!
//! Staging for v_exchanges — self-contained and liftable, like `examples/demo`'s.

pub mod config;
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

use crate::config::Situation;

/// SPL's `OrderBookActor::DEPTH`. The archive carries 200 levels a side; the strategy reads 20.
const DEPTH: usize = 20;
/// CloudFront 403s reqwest/ureq's default agent string on the quote-saver host; any custom one passes.
const UA: &str = "trading_data_spl";
/// Five-minute buckets in a UTC day — what a complete OI day must have.
const OI_PER_DAY: usize = 288;
const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

pub fn symbol(s: &Situation) -> Symbol {
	Symbol::new(Pair::from_str(&s.pair).unwrap_or_else(|e| panic!("situation.pair `{}`: {e}", s.pair)), Instrument::Perp)
}

pub fn asset(s: &Situation) -> Asset {
	Asset::new(symbol(s).pair.base())
}

/// UTC bounds `[start, end)` of one day — the Replay range, on the venue axis the lanes are stored
/// against.
pub fn day_bounds(d: jiff::civil::Date) -> (Ts<Venue>, Ts<Venue>) {
	let start = d.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(Ts::from_nanos(start), Ts::from_nanos(start + 24 * 3600 * 1_000_000_000))
}

/// The situation's trading days — `[start, end)`. Everything before them is warmup.
pub fn trading_days(s: &Situation) -> Vec<jiff::civil::Date> {
	let mut d = s.start;
	let mut out = Vec::new();
	while d < s.end {
		out.push(d);
		d = d.tomorrow().expect("date in range");
	}
	assert!(!out.is_empty(), "situation window is empty: start {} is not before end {}", s.start, s.end);
	out
}

/// Idempotent per day: downloads each day's archive and ingests it into a parquet catalog under
/// `cache`, skipping any day already present.
pub fn ensure_trades(cache: &Path, s: &Situation, days: &[jiff::civil::Date]) -> Catalog {
	fs::create_dir_all(cache).expect("create spl cache dir");
	let catalog = Catalog::new(cache.join("catalog"));
	let sym = &s.bybit_symbol;
	for &d in days {
		let gz = cache.join(format!("{sym}{d}.csv.gz"));
		if !gz.exists() {
			download(&gz, &format!("https://public.bybit.com/trading/{sym}/{sym}{d}.csv.gz"), GZIP_MAGIC);
		}
		let (start, end) = day_bounds(d);
		if read_trades(&catalog, ExchangeName::Bybit, symbol(s), start, end).expect("open trades lane").next().is_some() {
			continue;
		}
		ingest_trades(&gz, &catalog, s);
	}
	catalog
}
/// Idempotent: folds the trading days' ob200 archives into the catalog's book lanes through
/// [`Live`]'s recording tee — the same delta+checkpoint pair a live session writes, so `Replay`
/// reads it back on the path `examples/live` asserts is identical.
///
/// Only the trading days carry book, which is SPL exactly: `OrderBookActor` is the sole Deep indie,
/// subscribed only while a situation is open and never during warmup.
///
/// **One session for the whole window, not one per day.** `monotonic_seq` is the delta lane's chain
/// and it restarts with each [`Live`]; a day's archive also carries messages a fraction past
/// midnight, so per-day sessions put a `263774 → 1` discontinuity inside the next day's replay
/// range, which desyncs the folded book until the following checkpoint. The lane is one recollection
/// and has to be recorded as one.
///
/// ponytail: the delta lane has no public reader, so ingest is gated on a sentinel file rather than
/// a lane probe. Delete `.book_ingested_<start>_<end>` to force a re-ingest.
pub fn ensure_book(cache: &Path, catalog: &Catalog, s: &Situation) {
	let sentinel = cache.join(format!(".book_ingested_{}_{}", s.start, s.end));
	if sentinel.exists() {
		println!("book lane already populated, skipping ingest");
		return;
	}
	let sym = &s.bybit_symbol;
	let zips: Vec<PathBuf> = trading_days(s)
		.into_iter()
		.map(|d| {
			let zip = cache.join(format!("{d}_{sym}_ob200.data.zip"));
			if !zip.exists() {
				download(&zip, &format!("https://quote-saver.bycsi.com/orderbook/linear/{sym}/{d}_{sym}_ob200.data.zip"), ZIP_MAGIC);
			}
			zip
		})
		.collect();

	// The archives' own timestamps are the only clock this session has; `Live` stamps arrivals off
	// it, and the recorded reception it writes is what `Replay` weaves on.
	let clock = Arc::new(ArchiveClock(AtomicI64::new(0)));
	let prec = s.precision;
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(s), prec, true, clock.clone());
	let sink = live.sink();
	let pump = {
		let clock = clock.clone();
		std::thread::spawn(move || pump_archives(&zips, &sink, &clock, prec))
	};
	while live.next().is_some() {}
	let (emissions, levels) = pump.join().expect("archive pump panicked");
	fs::write(&sentinel, format!("{emissions} emissions, {levels} levels\n")).expect("write book sentinel");
	println!("book ingested: {emissions} 1s emissions, {levels} level rows");
}
/// Idempotent: fetches the trading days' Bybit open interest (5min ⇒ 288 rows/day ⇒ 2 pages) into
/// the oi lane. Historic ingest, so there is no local reading.
pub fn ensure_oi(catalog: &Catalog, s: &Situation) {
	if read_oi(catalog, ExchangeName::Bybit, symbol(s), Ts::MIN, Ts::MAX).expect("open oi lane").next().is_some() {
		println!("oi lane already populated, skipping fetch");
		return;
	}
	let sym = &s.bybit_symbol;
	let mut rows: Vec<Oi> = Vec::new();
	for d in trading_days(s) {
		let (day_start, day_end) = day_bounds(d);
		let (start_ms, end_ms) = (day_start.as_nanos() / 1_000_000, day_end.as_nanos() / 1_000_000);
		let before = rows.len();
		let mut cursor = String::new();
		loop {
			let mut url = format!("https://api.bybit.com/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&startTime={start_ms}&endTime={end_ms}&limit=200");
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
		// A UTC day is exactly 288 five-minute buckets. Anything less is a hole in the input — most
		// likely Bybit's 5min OI retention no longer reaching that far back. The decision is data,
		// not code: move the situation window, or drop the Oi root from the graph.
		assert_eq!(
			rows.len() - before,
			OI_PER_DAY,
			"Bybit returned {} of {OI_PER_DAY} five-minute OI readings inside {d}",
			rows.len() - before
		);
	}
	rows.sort_by_key(|r| r.ts_venue_exec);
	rows.dedup_by_key(|r| r.ts_venue_exec);

	let mut feather = Feather::<Oi>::new(ExchangeName::Bybit, symbol(s), Oi::POLICY);
	for row in rows {
		feather.push(row);
	}
	let path = feather.flush(catalog).expect("flush oi").expect("non-empty oi batch");
	println!("oi ingested to {}", path.display());
}
/// Idempotent: one CoinGecko market cap per trading day into the mc lane. No node reads it — SPL has
/// no screener that reads market cap either — but the `Mc` root is declared, so `required_lanes` asks
/// `Replay` for the lane and a missing reading is missing input rather than a degraded mode. This
/// panics with the vendor's own error and the decisions available.
pub fn ensure_mc(catalog: &Catalog, s: &Situation) {
	if read_mc(catalog, asset(s), Ts::MIN, Ts::MAX).expect("open mc lane").next().is_some() {
		println!("mc lane already populated, skipping fetch");
		return;
	}
	let id = &s.coingecko_id;
	let mut feather = Feather::<Mc>::new(asset(s), Mc::POLICY);
	for d in trading_days(s) {
		let url = format!("https://api.coingecko.com/api/v3/coins/{id}/history?date={d}&localization=false");
		println!("fetching {url}");
		let (status, body) = http_try(&url);
		let v: serde_json::Value = serde_json::from_slice(&body).expect("coingecko json");
		let market_cap = v["market_data"]["market_cap"]["usd"].as_f64().unwrap_or_else(|| {
			panic!(
				"CoinGecko has no market cap for {id} on {d} (HTTP {status}): {}\n  \
				 The `Mc` root is declared, so `required_lanes` demands the lane and `Replay` cannot open the \
				 window without it. Fix the data, not this code: supply a CoinGecko Pro key (the free tier serves \
				 only the last 365 days), move the situation window inside that window, or drop the `Mc` root \
				 from the graph.",
				v["error"]["status"]["error_message"]
					.as_str()
					.or_else(|| v["status"]["error_message"].as_str())
					.unwrap_or("200 with no market_data.market_cap.usd in it")
			)
		});
		// The history endpoint attests the 00:00 UTC snapshot of the date, and reports no rank.
		let (start, _) = day_bounds(d);
		feather.push(Mc {
			ts_local_exec: Ts::from_nanos(start.as_nanos()),
			market_cap,
			rank: None,
		});
	}
	let path = feather.flush(catalog).expect("flush mc").expect("non-empty mc batch");
	println!("mc ingested to {}", path.display());
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

/// Folds the whole ob200 stream into a local book and emits, once per second, the diff of the
/// top-`DEPTH`-per-side view against the last emitted one — levels dropping out of it as `qty = 0`
/// deletes. The book and the emitted view carry across archive files, so the day boundary is just
/// another second. Returns `(emissions, level rows)`.
///
/// ponytail: L20 @ 1s is exactly what SPL's `OrderBookActor` consumes, and it is also the ceiling —
/// a node needing sub-second or deeper book raises `DEPTH` or the bucket here, at a linear cost in
/// delta-lane rows (the full 200-level stream is ~10× this and blows `Replay`'s eager buffer).
fn pump_archives(zips: &[PathBuf], sink: &Sink, clock: &ArchiveClock, prec: PrecisionPriceQty) -> (u64, u64) {
	let (mut book, mut emitted) = (Levels::default(), Levels::default());
	let (mut cur_sec, mut last_ns) = (None, 0i64);
	let (mut emissions, mut levels) = (0u64, 0u64);

	for zip in zips {
		let file = fs::File::open(zip).expect("open book archive");
		let mut archive = zip::ZipArchive::new(file).expect("book archive is a zip");
		assert_eq!(archive.len(), 1, "ob200 archive holds exactly one jsonl member");
		let entry = archive.by_index(0).expect("open zip member");

		for (i, line) in BufReader::new(entry).lines().enumerate() {
			let line = line.expect("read archive line");
			let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_else(|e| panic!("malformed line {i} of {}: {e}", zip.display()));
			let ts_ns = v["ts"].as_i64().unwrap_or_else(|| panic!("no ts on line {i} of {}", zip.display())) * 1_000_000;
			assert!(ts_ns >= last_ns, "archives out of order at line {i} of {}: {ts_ns} < {last_ns}", zip.display());
			let sec = ts_ns.div_euclid(1_000_000_000);
			if cur_sec.is_some_and(|c| sec > c) {
				let n = emit(sink, clock, prec, &book, &mut emitted, last_ns);
				emissions += u64::from(n > 0);
				levels += n;
			}
			cur_sec = Some(sec);

			let kind = v["type"].as_str().unwrap_or_else(|| panic!("no type on line {i} of {}", zip.display()));
			match kind {
				"snapshot" => {
					book.bids.clear();
					book.asks.clear();
				}
				"delta" => {}
				other => panic!("unknown archive record type `{other}` on line {i} of {}", zip.display()),
			}
			apply(&mut book.bids, &v["data"]["b"], prec, i);
			apply(&mut book.asks, &v["data"]["a"], prec, i);
			last_ns = ts_ns;
		}
	}
	if cur_sec.is_some() {
		let n = emit(sink, clock, prec, &book, &mut emitted, last_ns);
		emissions += u64::from(n > 0);
		levels += n;
	}
	(emissions, levels)
}

/// One side each of a book view, keyed by raw price.
#[derive(Default)]
struct Levels {
	bids: BTreeMap<i32, u32>,
	asks: BTreeMap<i32, u32>,
}

fn apply(side: &mut BTreeMap<i32, u32>, levels: &serde_json::Value, prec: PrecisionPriceQty, line: usize) {
	for l in levels.as_array().unwrap_or_else(|| panic!("book side is not an array on line {line}")) {
		let price = prec.price.parse_i32(l[0].as_str().unwrap_or_else(|| panic!("price is not a string on line {line}")));
		let qty = prec.qty.parse_u32(l[1].as_str().unwrap_or_else(|| panic!("qty is not a string on line {line}")));
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
fn emit(sink: &Sink, clock: &ArchiveClock, prec: PrecisionPriceQty, book: &Levels, emitted: &mut Levels, ts_ns: i64) -> u64 {
	let (bids, asks) = (&book.bids, &book.asks);
	let (top_bids, top_asks) = (&mut emitted.bids, &mut emitted.asks);
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
		prec,
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

/// `magic` is the format's leading bytes. A size threshold would be a guess — a thin instrument's
/// day is legitimately tiny — whereas the wrong magic is exactly the truncation or error page we
/// must not cache, and the rename only happens once the body is what it claims to be.
fn download(to: &PathBuf, url: &str, magic: &[u8]) {
	println!("downloading {url}");
	let body = http_get(url);
	assert!(
		body.starts_with(magic),
		"GET {url} returned {} bytes that are not the expected archive format: starts {:?}",
		body.len(),
		&body[..body.len().min(8)]
	);
	let tmp = to.with_extension("part");
	fs::write(&tmp, &body).expect("write archive");
	fs::rename(&tmp, to).expect("move archive into place");
	println!("downloaded {} bytes", body.len());
}

fn ingest_trades(gz: &Path, catalog: &Catalog, s: &Situation) {
	let prec = s.precision;
	let mut feather = Feather::<Trade>::new(ExchangeName::Bybit, symbol(s), prec, Trade::POLICY);
	let mut day = TradeBuf::new(prec);

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
		assert_eq!(col(), s.bybit_symbol, "foreign symbol on line {i}");
		let side: Side = col().parse().unwrap_or_else(|e| panic!("bad side on line {i}: {e}"));
		let qty = prec.qty.parse_u32(col());
		let price = prec.price.parse_i32(col());

		let ts = (ts_sec * 1e9).round() as i64;
		assert!(ts >= prev_ts, "trades not time-ordered at line {i}: {prev_ts} > {ts}");
		prev_ts = ts;

		// Historic ingest: no wire time, and we were not there to receive it.
		day.push(Ts::from_nanos(ts), None, None, i as u64, side, price, qty);
	}
	feather.extend(day.cols(0..day.len()));
	let path = feather.flush(catalog).expect("flush day of trades").expect("non-empty day");
	println!("ingested to {}", path.display());
}
