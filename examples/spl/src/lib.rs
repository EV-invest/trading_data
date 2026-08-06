#![feature(adt_const_params)]
#![feature(default_field_values)]
// the graph resolves its node set by trampolining between `#[node]` shims and the driver.
#![recursion_limit = "1024"]
//! Idempotent SPL data layer over the window `config.nix` names: Bybit trades for every day of it,
//! and — for the trading days only, matching SPL's situation-gated Deep tier — the L20 book off the
//! historic ob200 archive plus OI and market cap. Each step is skipped if its artifact exists; any
//! failure is a loud panic, and missing input is a failure.
//!
//! Staging for v_exchanges — self-contained and liftable, like `examples/demo`'s.

pub mod config;
pub mod nodes;
pub mod ui;

use std::{
	collections::BTreeMap,
	fs,
	io::Read as _,
	path::Path,
	str::FromStr as _,
	sync::{
		Arc,
		atomic::{AtomicI64, Ordering},
	},
};

use indicatif::{MultiProgress, ProgressBar};
use trading_data::{
	Aggregate, Asset, BookShape, BookUpdate, Catalog, Clock, ExchangeName, Feather, Feed as _, Instrument, Live, Local, Mc, Oi, Pair, PrecisionPriceQty, Row as _, Side, Sink, Span, Symbol,
	Trade, TradeBuf, Ts, Venue, read_mc, read_oi, read_trades,
};
use v_exchanges::core::{Exchange, ExchangeInit as _, ExchangeStream, History, RequestRange};

use crate::config::Situation;

/// SPL's `OrderBookActor::DEPTH`. The archive carries 200 levels a side; the strategy reads 20.
pub const DEPTH: usize = 20;
/// Five-minute buckets in a UTC day — what a complete OI day must have.
const OI_PER_DAY: usize = 288;

/// How far the pump may lead the consumer, in archive time. It is the stamping error the leash
/// permits, so it belongs well *below* the finest batch window anything replays this lane with —
/// not at it. At the cell size, how much of a cell the slop eats is decided by how bursty the
/// producer happens to be, and the recorded arrivals stop being a function of the data.
const PUMP_LEAD_NS: i64 = 1_000_000;
pub fn symbol(s: &Situation) -> Symbol {
	Symbol::new(Pair::from_str(&s.pair).unwrap_or_else(|e| panic!("situation.pair `{}`: {e}", s.pair)), Instrument::Perp)
}

pub fn asset(s: &Situation) -> Asset {
	symbol(s).pair.base()
}

/// UTC bounds `[start, end)` of one day — the Replay range, on the venue axis the lanes are stored
/// against.
pub fn day_bounds(d: jiff::civil::Date) -> (Ts<Venue>, Ts<Venue>) {
	let start = d.in_tz("UTC").expect("UTC exists").timestamp().as_nanosecond() as i64;
	(Ts::from_nanos(start), Ts::from_nanos(start + 24 * 3600 * 1_000_000_000))
}

/// The situation's days — `[start, end)`.
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

/// Acquires all four lanes over the situation's window, each idempotent, under one set of progress
/// bars.
pub async fn ensure_lanes(cache: &Path, s: &Situation) -> Catalog {
	let mp = MultiProgress::new();
	let days = trading_days(s);
	// Prefixes are padded, so the green done-style — which recolours them — still lines the bars up.
	let pb_trades = ui::lane(&mp, "trades", days.len());
	let pb_book = ui::lane(&mp, "book  ", days.len());
	let pb_oi = ui::lane(&mp, "oi    ", days.len());
	let pb_mc = ui::lane(&mp, "mc    ", days.len());

	let bybit = ExchangeName::Bybit.init_client();
	// The tick every price and quantity in the catalog is stated in. It is the venue's to declare,
	// so it is read off the venue — a number restated in config is one that can silently disagree.
	let prec = precision(&*bybit, symbol(s)).await;

	fs::create_dir_all(cache).expect("create spl cache dir");
	let catalog = Catalog::new(cache.join("catalog"));
	ensure_trades(&*bybit, &catalog, s, &days, prec, &pb_trades).await;
	ensure_book(&*bybit, cache, &catalog, s, prec, &pb_book).await;
	ensure_oi(&*bybit, &catalog, s, &pb_oi).await;
	ensure_mc(&catalog, s, &pb_mc);
	catalog
}

async fn precision(bybit: &dyn Exchange, symbol: Symbol) -> PrecisionPriceQty {
	let info = bybit.exchange_info(symbol.instrument).await.expect("bybit instruments-info");
	let pi = info.pairs.get(&symbol.pair).unwrap_or_else(|| panic!("Bybit does not list {symbol}"));
	PrecisionPriceQty {
		price: pi.price_precision,
		qty: pi.qty_precision,
	}
}

/// The venue's archives, which is the only way this app acquires history.
fn history(bybit: &dyn Exchange) -> &dyn History {
	bybit.history().expect("Bybit publishes archives")
}

/// Idempotent per day: ingests each day of the venue's trade archive into the catalog, skipping any
/// day already present. One [`Feather`] flush per day, so a day is either wholly in the lane or
/// wholly absent — which is what makes the probe above a decision.
async fn ensure_trades(bybit: &dyn Exchange, catalog: &Catalog, s: &Situation, days: &[jiff::civil::Date], prec: PrecisionPriceQty, pb: &ProgressBar) {
	let mut ingested = 0;
	for &d in days {
		let (start, end) = day_bounds(d);
		if read_trades(catalog, ExchangeName::Bybit, symbol(s), start, end).expect("open trades lane").next().is_some() {
			pb.inc(1);
			continue;
		}
		pb.set_message(format!("ingest {d}"));

		let mut stream = history(bybit)
			.trades(symbol(s), start.to_jiff(), end.to_jiff())
			.await
			.unwrap_or_else(|e| panic!("open {d}'s trade archive: {e}"));
		let mut feather = Feather::<Trade>::new(ExchangeName::Bybit, symbol(s), prec, Trade::POLICY);
		let mut day = TradeBuf::new(prec);
		//LOOP: an archive is finite and says so with an empty batch — `ExchangeStream` has no `Option` to `while let` on, because a live socket has no end for one to mean.
		loop {
			let batches = stream.next().await.unwrap_or_else(|e| panic!("read {d}'s trade archive: {e}"));
			if batches.is_empty() {
				break;
			}
			for batch in batches {
				for t in batch.iter() {
					// Historic ingest: no wire time, and we were not there to receive it.
					day.push(t.time, None, None, day.len() as u64, t.side, t.price, t.qty);
				}
			}
		}
		feather.extend(day.cols(0..day.len()));
		feather.flush(catalog).expect("flush day of trades").expect("non-empty day");
		ingested += 1;
		pb.inc(1);
	}
	match ingested {
		0 => ui::finish(pb, "cached"),
		n => ui::finish(pb, format!("{n} ingested, {} cached", days.len() - n)),
	}
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
/// a lane probe. Its name carries everything that shapes the lane's content — range, depth, and the
/// emission policy — so a lane recorded under a different one cannot be silently reused.
async fn ensure_book(bybit: &dyn Exchange, cache: &Path, catalog: &Catalog, s: &Situation, prec: PrecisionPriceQty, pb: &ProgressBar) {
	let sentinel = cache.join(format!(".book_ingested_per_msg_top{DEPTH}_{}_{}", s.start, s.end));
	if sentinel.exists() {
		let what = fs::read_to_string(&sentinel).expect("read book sentinel");
		ui::finish(pb, format!("cached — {}", what.trim()));
		return;
	}
	let days = trading_days(s);
	let (window_start, _) = day_bounds(days[0]);
	let (_, window_end) = day_bounds(*days.last().expect("trading_days is non-empty"));
	let stream = history(bybit)
		.book(symbol(s), window_start.to_jiff(), window_end.to_jiff())
		.await
		.unwrap_or_else(|e| panic!("open the book archive: {e}"));

	// The archives' own timestamps are the only clock this session has; `Live` stamps arrivals off
	// it, and the recorded reception it writes is what `Replay` weaves on.
	let clock = Arc::new(ArchiveClock {
		pump: AtomicI64::new(0),
		consumed: AtomicI64::new(0),
	});
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(s), prec, true, clock.clone());
	let sink = live.sink();
	// Its own thread and its own runtime, because the fold has to run *while* this thread drains
	// `Live` — the leash below is a two-way conversation, and both halves block.
	let pump = {
		let clock = clock.clone();
		let lane = pb.clone();
		std::thread::spawn(move || {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("build the pump's runtime")
				.block_on(pump_archives(stream, &sink, &clock, prec, &lane))
		})
	};
	// Reporting what we consumed is what lets the pump be held to us — see `PUMP_LEAD_NS`.
	while let Some(l) = live.next() {
		clock.consumed.store(l.ts_venue.as_nanos(), Ordering::Relaxed);
	}
	let (emissions, levels) = pump.join().expect("archive pump panicked");
	fs::write(&sentinel, format!("{emissions} emissions, {levels} levels\n")).expect("write book sentinel");
	ui::finish(pb, format!("{emissions} top-{DEPTH} changes, {levels} level rows"));
}
/// Idempotent: fetches the trading days' Bybit open interest, 5min, into the oi lane. Historic
/// ingest, so there is no local reading.
async fn ensure_oi(bybit: &dyn Exchange, catalog: &Catalog, s: &Situation, pb: &ProgressBar) {
	if read_oi(catalog, ExchangeName::Bybit, symbol(s), Ts::MIN, Ts::MAX).expect("open oi lane").next().is_some() {
		ui::finish(pb, "cached");
		return;
	}
	let mut rows: Vec<Oi> = Vec::new();
	for d in trading_days(s) {
		pb.set_message(d.to_string());
		let (day_start, day_end) = day_bounds(d);
		let before = rows.len();
		let range = RequestRange::Span {
			since: day_start.to_jiff(),
			until: Some(day_end.to_jiff()),
		};
		let readings = bybit
			.open_interest(symbol(s), v_utils::TF_5MIN, range)
			.await
			.unwrap_or_else(|e| panic!("bybit open interest for {d}: {e}"));
		rows.extend(readings.iter().map(|r| Oi {
			ts_venue_exec: Ts::from_nanos(r.timestamp.as_nanosecond() as i64),
			ts_venue_send: None,
			ts_local_recv: None,
			oi: r.val_asset,
		}));
		// A UTC day is exactly 288 five-minute buckets. Anything less is a hole in the input — most
		// likely Bybit's 5min OI retention no longer reaching that far back. The decision is data,
		// not code: move the situation window, or drop the Oi root from the graph.
		assert_eq!(
			rows.len() - before,
			OI_PER_DAY,
			"Bybit returned {} of {OI_PER_DAY} five-minute OI readings inside {d}",
			rows.len() - before
		);
		pb.inc(1);
	}
	rows.sort_by_key(|r| r.ts_venue_exec);
	rows.dedup_by_key(|r| r.ts_venue_exec);

	let n = rows.len();
	let mut feather = Feather::<Oi>::new(ExchangeName::Bybit, symbol(s), Oi::POLICY);
	for row in rows {
		feather.push(row);
	}
	feather.flush(catalog).expect("flush oi").expect("non-empty oi batch");
	ui::finish(pb, format!("{n} readings"));
}
/// Idempotent: one CoinGecko market cap per trading day into the mc lane. No node reads it — SPL has
/// no screener that reads market cap either — but the `Mc` root is declared, so `required_lanes` asks
/// `Replay` for the lane and a missing reading is missing input rather than a degraded mode. This
/// panics with the vendor's own error and the decisions available.
fn ensure_mc(catalog: &Catalog, s: &Situation, pb: &ProgressBar) {
	if read_mc(catalog, asset(s), Ts::MIN, Ts::MAX).expect("open mc lane").next().is_some() {
		ui::finish(pb, "cached");
		return;
	}
	let id = &s.coingecko_id;
	let mut feather = Feather::<Mc>::new(asset(s), Mc::POLICY);
	let days = trading_days(s);
	for d in &days {
		pb.set_message(d.to_string());
		let url = format!("https://api.coingecko.com/api/v3/coins/{id}/history?date={d}&localization=false");
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
		let (start, _) = day_bounds(*d);
		feather.push(Mc {
			ts_local_exec: Ts::from_nanos(start.as_nanos()),
			market_cap,
			rank: None,
		});
		pb.inc(1);
	}
	feather.flush(catalog).expect("flush mc").expect("non-empty mc batch");
	ui::finish(pb, format!("{} readings", days.len()));
}

/// The pump's cursor over archive time, read by [`Live`] on the consuming thread, plus the
/// consumer's own cursor so the pump can be held to it: unheld, the pump runs the archive to its end
/// while the consumer is minutes behind, and every backlogged event is stamped with the same final
/// reading — arrivals become thread scheduling rather than archive time.
struct ArchiveClock {
	pump: AtomicI64,
	consumed: AtomicI64,
}

impl Clock for ArchiveClock {
	fn now_ns(&self) -> i64 {
		self.pump.load(Ordering::Relaxed)
	}
}

/// Folds the venue's whole book stream into a local book and emits, on every message that moves it,
/// the diff of the top-`DEPTH`-per-side view against the last emitted one — levels dropping out of
/// it as `qty = 0` deletes. A message touching only depth 21+ changes nothing the strategy can read
/// and emits nothing, so the lane carries changes in *what is read* and no more. Returns
/// `(emissions, level rows)`.
///
/// `DEPTH` is SPL's, not the venue's, which is why the fold stayed here when the transport and the
/// parsing left: what the strategy reads is the app's to decide.
async fn pump_archives(mut stream: Box<dyn ExchangeStream<Item = BookUpdate>>, sink: &Sink, clock: &ArchiveClock, prec: PrecisionPriceQty, lane: &ProgressBar) -> (u64, u64) {
	const DAY_NS: i64 = 24 * 3600 * 1_000_000_000;
	let (mut book, mut emitted) = (Levels::default(), Levels::default());
	let (mut emissions, mut levels) = (0u64, 0u64);
	let (mut i, mut day) = (0usize, 0i64);

	//LOOP: as in `ensure_trades` — the window is finite, and an empty batch is how it says so.
	loop {
		let batch = stream.next().await.expect("read the book archive");
		if batch.is_empty() {
			break;
		}
		for update in batch {
			let shape = match update {
				BookUpdate::Snapshot(shape) => {
					book.bids.clear();
					book.asks.clear();
					shape
				}
				BookUpdate::BatchDelta { shape, .. } => shape,
			};
			apply(&mut book.bids, Side::Buy, &shape.bids);
			apply(&mut book.asks, Side::Sell, &shape.asks);

			let ts_ns = shape.ts.venue_exec.first.as_nanos();
			let n = emit(sink, clock, prec, &book, &mut emitted, ts_ns);
			emissions += u64::from(n > 0);
			levels += n;
			// Formatting per message would cost more than the fold does.
			if i % 50_000 == 0 {
				lane.set_message(format!("{emissions} emissions, {levels} levels"));
			}
			if ts_ns / DAY_NS != day {
				lane.inc(1);
				day = ts_ns / DAY_NS;
			}
			i += 1;
		}
	}
	(emissions, levels)
}

/// One side each of a book view, sorted best-first — bids descending, asks ascending — so the
/// top-`DEPTH` view is a prefix slice rather than a fresh map per message.
#[derive(Default)]
struct Levels {
	bids: Vec<(i32, u32)>,
	asks: Vec<(i32, u32)>,
}

/// Where `price` sits in a side held best-first.
fn seek(levels: &[(i32, u32)], side: Side, price: i32) -> Result<usize, usize> {
	match side {
		Side::Buy => levels.binary_search_by(|&(p, _)| price.cmp(&p)),
		Side::Sell => levels.binary_search_by(|&(p, _)| p.cmp(&price)),
	}
}

fn apply(levels: &mut Vec<(i32, u32)>, side: Side, raw: &BTreeMap<i32, u32>) {
	for (&price, &qty) in raw {
		match (seek(levels, side, price), qty) {
			(Ok(j), 0) => {
				levels.remove(j);
			}
			(Ok(j), q) => levels[j].1 = q,
			(Err(_), 0) => (),
			(Err(j), q) => levels.insert(j, (price, q)),
		}
	}
}

/// The top-`DEPTH` diff against the last emitted view. Returns the level rows emitted; `0` = the
/// view is unchanged and there is nothing to say.
fn emit(sink: &Sink, clock: &ArchiveClock, prec: PrecisionPriceQty, book: &Levels, emitted: &mut Levels, ts_ns: i64) -> u64 {
	fn top(side: &[(i32, u32)]) -> &[(i32, u32)] {
		&side[..DEPTH.min(side.len())]
	}
	let (next_bids, next_asks) = (top(&book.bids), top(&book.asks));
	// The wire wants a `BTreeMap`, so these two are the only maps the fold still builds — and only
	// on a message that moves the view.
	let diff = |old: &[(i32, u32)], new: &[(i32, u32)], side: Side| {
		let mut out: BTreeMap<i32, u32> = new.iter().filter(|&&(p, q)| seek(old, side, p).map(|j| old[j].1) != Ok(q)).copied().collect();
		out.extend(old.iter().filter(|&&(p, _)| seek(new, side, p).is_err()).map(|&(p, _)| (p, 0)));
		out
	};
	let (d_bids, d_asks) = (diff(&emitted.bids, next_bids, Side::Buy), diff(&emitted.asks, next_asks, Side::Sell));
	let n = (d_bids.len() + d_asks.len()) as u64;
	if n == 0 {
		return 0;
	}
	emitted.bids.clear();
	emitted.bids.extend_from_slice(next_bids);
	emitted.asks.clear();
	emitted.asks.extend_from_slice(next_asks);

	// The leash. Compared against what we last *pushed*, not against `ts_ns`: a quiet stretch emits
	// nothing, and waiting on it would block for an event only this call can supply.
	while clock.pump.load(Ordering::Relaxed) - clock.consumed.load(Ordering::Relaxed) > PUMP_LEAD_NS {
		std::thread::sleep(std::time::Duration::from_micros(100));
	}

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
	clock.pump.store(ts_ns, Ordering::Relaxed);
	// Our own fold, so there is no venue sequence to have broken.
	sink.book(BookUpdate::BatchDelta { shape, gapped: false });
	n
}

/// CoinGecko's status alongside its body: the failure message we want is the vendor's own error
/// text rather than the status line, so this needs the 4xx body, not a status-as-error.
fn http_try(url: &str) -> (u16, Vec<u8>) {
	let mut resp = ureq::get(url).config().http_status_as_error(false).build().call().unwrap_or_else(|e| panic!("GET {url}: {e}"));
	let status = resp.status().as_u16();
	let mut body = Vec::new();
	resp.body_mut().as_reader().read_to_end(&mut body).expect("read body");
	(status, body)
}
