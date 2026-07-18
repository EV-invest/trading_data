//! Record synthetic book updates through `LiveBook` and trades through `Feather<Trade>`,
//! flush, read back, and assert the persistence invariants. No exchange: `trading_data` is a
//! leaf and must never depend on a data source. The invariants under test are purely this
//! crate's own.

use std::{
	str::FromStr as _,
	sync::{Arc, atomic::AtomicI64},
};

use tempfile::tempdir;
use trading_data_persistence::{BookDelta, BookShape, Catalog, Clock, Feather, LiveBook, Row as _, Trade, read_book, read_trades};
use v_utils::trades::{ExchangeName, Instrument, Pair, PrecisionPriceQty, Side, Symbol};

const STEP_NS: i64 = 1_000_000_000; // 1s between events

/// Catalog files are indexed by ingest time; aligning it with synthetic event time keeps the
/// read ranges honest instead of racing the wall clock.
struct EventClock(AtomicI64);
impl Clock for EventClock {
	fn now_ns(&self) -> i64 {
		self.0.load(std::sync::atomic::Ordering::Relaxed)
	}
}

fn main() {
	v_utils::clientside!();

	let dir = tempdir().expect("tempdir");
	let catalog = Catalog::new(dir.path());

	let pair = Pair::from_str("BTCUSDT").unwrap();
	let instrument = Instrument::Perp;
	let prec = PrecisionPriceQty { price: 2, qty: 5 };
	let symbol = Symbol::new(pair, instrument);

	let base = jiff::Timestamp::now().as_nanosecond() as i64;
	let start_ns = base;
	let clock = Arc::new(EventClock(AtomicI64::new(base)));
	let mut live = LiveBook::persisting(catalog.clone(), ExchangeName::Binance, pair, instrument, prec, clock.clone());

	// One snapshot, then a handful of deltas at strictly increasing event times.
	live.snapshot(&shape(base, &[(10_000, 5), (9_999, 3)], &[(10_001, 4), (10_002, 6)]));
	let deltas = [
		shape(base + STEP_NS, &[(10_000, 7)], &[(10_002, 0)]), // requote bid, remove an ask
		shape(base + 2 * STEP_NS, &[(9_998, 2)], &[(10_003, 1)]),
		shape(base + 3 * STEP_NS, &[(9_999, 0)], &[(10_001, 9)]), // remove a bid
	];
	for d in &deltas {
		clock.0.store(d.ts_event.as_nanosecond() as i64, std::sync::atomic::Ordering::Relaxed);
		live.delta(d, false);
	}
	let end_ns = base + 4 * STEP_NS;

	live.flush().expect("flush");
	assert!(!live.bids().is_empty() && !live.asks().is_empty(), "live book state empty after recording");

	let mut trades = Feather::<Trade>::new(ExchangeName::Binance, symbol, prec, Trade::POLICY);
	for i in 0..3_i64 {
		trades.push(Trade {
			ts_event: base + i * STEP_NS,
			ts_init: base + i * STEP_NS,
			monotonic_seq: i as u64,
			trade_id: i as u64,
			side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
			price: 100.01 + i as f64,
			qty: (i + 1) as f64 / 100_000.0,
		});
	}
	trades.flush(&catalog).expect("flush trades");

	let (anchor, delta_stream) = read_book(&catalog, ExchangeName::Binance, symbol, start_ns, end_ns).expect("read_book");
	let anchor = anchor.expect("snapshot just written must anchor");
	assert_eq!(anchor.bids.len(), 2, "anchor bids");
	assert_eq!(anchor.asks.len(), 2, "anchor asks");

	let out: Vec<BookDelta> = delta_stream.collect();
	tracing::info!(rows = out.len(), "replayed deltas");
	assert_eq!(out.len(), 6, "3 shapes x 2 levels each");

	// Monotonic non-decreasing ts_event, strictly increasing monotonic_seq across the delta stream.
	let mut prev_ts = i64::MIN;
	let mut prev_seq: Option<u64> = None;
	for d in &out {
		assert!(d.ts_event >= prev_ts, "delta stream not monotonic: {prev_ts} > {}", d.ts_event);
		prev_ts = d.ts_event;
		if let Some(p) = prev_seq {
			assert!(d.monotonic_seq > p, "delta seq regression: {p} -> {}", d.monotonic_seq);
		}
		prev_seq = Some(d.monotonic_seq);
	}

	let replayed: Vec<Trade> = read_trades(&catalog, ExchangeName::Binance, symbol, start_ns, end_ns).expect("read_trades").collect();
	assert_eq!(replayed.len(), 3);
	assert!(replayed.windows(2).all(|w| w[0].ts_event <= w[1].ts_event && w[0].monotonic_seq < w[1].monotonic_seq), "trades unordered");
	assert_eq!(replayed[1].side, Side::Sell);
	assert_eq!(replayed[1].price, 101.01);

	tracing::info!("record_replay: ok ({} deltas, {} trades)", out.len(), replayed.len());
}
fn shape(ts_ns: i64, bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	let ts = jiff::Timestamp::from_nanosecond(ts_ns as i128).expect("valid timestamp");
	BookShape {
		ts_event: ts,
		ts_init: ts,
		ts_last: ts,
		prec: PrecisionPriceQty { price: 2, qty: 5 },
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
	}
}
