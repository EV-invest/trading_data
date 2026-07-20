//! The live≡backtest invariant: drive `Live` (record=true) with a synthetic multi-lane stream,
//! collect its woven batches, then `Replay` the just-recorded catalog over the same range and
//! assert the two batch streams are byte-identical — lane runs, contents, and effective-ts order,
//! including the `BookAnchor` from an in-range snapshot both feeds emit at the same weave position.
//!
//! Real `ts_init` is recorded, so replay short-circuits latency simulation and the equality is exact.

use std::sync::{
	Arc,
	atomic::{AtomicI64, Ordering},
};

use jiff::Timestamp;
use tempfile::tempdir;
use trading_data_persistence::{Batch, BookShape, BookUpdate, Catalog, Clock, Feed, LaneKind, LatencyConfig, Live, Oi, Replay, Trade};
use v_utils::trades::{ExchangeName, Instrument, PrecisionPriceQty, Side, Symbol};

/// Monotonic synthetic clock: each read advances 1ms, so live `ts_init` stamps are strictly
/// increasing across lanes and the recording replays deterministically.
struct EventClock(AtomicI64);
impl Clock for EventClock {
	fn now_ns(&self) -> i64 {
		self.0.fetch_add(1_000_000, Ordering::Relaxed)
	}
}

const PREC: PrecisionPriceQty = PrecisionPriceQty { price: 2, qty: 4 };

fn symbol() -> Symbol {
	Symbol::new("BTC-USDT".try_into().expect("static pair"), Instrument::Perp)
}

fn ts_ns(t: Timestamp) -> i64 {
	t.as_nanosecond() as i64
}

/// Owned, comparable projection of one woven batch — enough to catch any weave/content drift.
#[derive(Debug, PartialEq)]
enum Sum {
	Trades(Vec<(i64, u64, u64)>),
	Book(Vec<(i64, u64)>),
	Anchor(i64, usize, usize),
	Oi(Vec<i64>),
}

fn collect(feed: &mut impl Feed) -> Vec<Sum> {
	let mut out = Vec::new();
	while let Some(b) = feed.next_batch() {
		out.push(match b {
			Batch::Trades(ts) => Sum::Trades(ts.iter().map(|t| (t.ts_init.expect("recorded"), t.monotonic_seq, t.trade_id)).collect()),
			Batch::Book(ds) => Sum::Book(ds.iter().map(|d| (d.ts_init.expect("recorded"), d.monotonic_seq)).collect()),
			Batch::BookAnchor(s) => Sum::Anchor(ts_ns(s.ts_init), s.bids.len(), s.asks.len()),
			Batch::Oi(os) => Sum::Oi(os.iter().map(|o| o.ts_init.expect("recorded")).collect()),
			Batch::Mc(_) => panic!("mc lane not driven"),
		});
	}
	out
}

// ts_init is stamped at ingest, so a placeholder here is fine; ts_event is the exchange event time.
fn shape(bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	let t = Timestamp::from_nanosecond(1_000).expect("in range");
	BookShape {
		ts_event: t,
		ts_init: t,
		ts_last: t,
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
	}
}

fn main() {
	let dir = tempdir().expect("temp dir");
	let catalog = Catalog::new(dir.path());
	let clock = Arc::new(EventClock(AtomicI64::new(1_000_000_000)));

	// --- record a live session ---
	let live_summary = {
		let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), PREC, true, clock.clone());
		let sink = live.sink();

		let trade = |seq: u64| Trade {
			ts_event: 1_000,
			ts_init: None, // stamped on push
			monotonic_seq: seq,
			trade_id: seq,
			side: if seq % 2 == 0 { Side::Buy } else { Side::Sell },
			price: 100.0 + seq as f64,
			qty: 0.5,
		};

		// interleave lanes so the weaver actually merges non-trivially.
		sink.trade(trade(1));
		sink.oi(Oi {
			ts_event: 1_000,
			ts_init: None,
			oi: 42.0,
		});
		sink.trade(trade(2));
		sink.book(BookUpdate::Snapshot(shape(&[(10_000, 5), (9_999, 3)], &[(10_001, 4), (10_002, 6)])));
		sink.book(BookUpdate::BatchDelta {
			shape: shape(&[(9_998, 2)], &[(10_003, 1)]),
			gapped: false,
		});
		sink.trade(trade(3));
		sink.oi(Oi {
			ts_event: 1_000,
			ts_init: None,
			oi: 43.0,
		});
		sink.book(BookUpdate::BatchDelta {
			shape: shape(&[(9_997, 7)], &[]),
			gapped: false,
		});

		drop(sink);
		collect(&mut live)
	};

	// --- replay the recording over the same range ---
	let latency = LatencyConfig {
		p68: std::time::Duration::from_millis(10),
		p95: std::time::Duration::from_millis(30),
		p997: std::time::Duration::from_millis(90),
		seed: 1,
	};
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX, &[LaneKind::Trades, LaneKind::Book, LaneKind::Oi], latency);
	let replay_summary = collect(&mut replay);

	assert_eq!(live_summary, replay_summary, "live and replay batch streams diverged");
	assert!(
		live_summary.iter().any(|s| matches!(s, Sum::Anchor(..))),
		"the in-range snapshot's BookAnchor must appear in both streams"
	);
	assert!(live_summary.iter().filter(|s| matches!(s, Sum::Trades(_))).count() >= 1, "trades must weave");
	assert!(live_summary.iter().filter(|s| matches!(s, Sum::Book(_))).count() >= 1, "book deltas must weave");
	assert!(live_summary.iter().filter(|s| matches!(s, Sum::Oi(_))).count() >= 1, "oi must weave");

	println!("sync_round_trip: {} batches, identical live≡replay. ok", live_summary.len());

	concurrent_streaming_matches_replay();
}

/// The paramount path: consume a `Live` feed *concurrently* with a producer thread (bounded
/// streaming, blocking-recv drain), then replay the recording — the flattened event streams must
/// match. Deterministic: one producer sends in a fixed order, ingest stamps in receive order.
fn concurrent_streaming_matches_replay() {
	use std::{thread, time::Duration};

	let dir = tempdir().expect("temp dir");
	let catalog = Catalog::new(dir.path());
	let clock = Arc::new(EventClock(AtomicI64::new(1_000_000_000)));

	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), PREC, true, clock);
	let sink = live.sink();
	let producer = thread::spawn(move || {
		for i in 0..40u64 {
			sink.trade(Trade {
				ts_event: 1_000,
				ts_init: None,
				monotonic_seq: i,
				trade_id: i,
				side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
				price: 100.0 + i as f64,
				qty: 0.5,
			});
			if i % 4 == 0 {
				sink.oi(Oi {
					ts_event: 1_000,
					ts_init: None,
					oi: i as f64,
				});
			}
			thread::sleep(Duration::from_micros(100)); // let the consumer find the channel empty and block
		}
		// sink dropped here → channel disconnects once the feed's own tx is gone
	});

	let live_flat = flat(&mut live);
	producer.join().expect("producer panicked");

	let latency = LatencyConfig {
		p68: Duration::from_millis(10),
		p95: Duration::from_millis(30),
		p997: Duration::from_millis(90),
		seed: 2,
	};
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX, &[LaneKind::Trades, LaneKind::Oi], latency);
	let replay_flat = flat(&mut replay);

	assert_eq!(live_flat, replay_flat, "concurrent streaming diverged from replay");
	assert_eq!(live_flat.iter().filter(|(lane, _)| *lane == b't').count(), 40, "all 40 trades must weave");
	println!("concurrent streaming: {} events, live≡replay. ok", live_flat.len());
}

/// Flatten a feed to `(lane_tag, seq)` per event, in emission order — robust to batch chunking.
fn flat(feed: &mut impl Feed) -> Vec<(u8, u64)> {
	let mut out = Vec::new();
	while let Some(b) = feed.next_batch() {
		match b {
			Batch::Trades(ts) => out.extend(ts.iter().map(|t| (b't', t.monotonic_seq))),
			Batch::Oi(os) => out.extend(os.iter().map(|o| (b'o', o.ts_init.expect("recorded") as u64))),
			other => panic!("unexpected lane: {other:?}"),
		}
	}
	out
}
