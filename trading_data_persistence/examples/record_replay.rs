//! Record synthetic book updates through `LiveBook`, flush, replay, and assert the
//! persistence/replay invariants. No exchange: `trading_data` is a leaf and must never
//! depend on a data source. The invariants under test are purely this crate's own.

use std::{str::FromStr as _, sync::Arc};

use tempfile::tempdir;
use trading_data_persistence::{BookDelta, BookShape, Catalog, Data, LiveBook, LiveClock, ReplayConfig, read_deltas, replay};
use v_utils::trades::{ExchangeName, Instrument, Pair, PrecisionPriceQty, Symbol};

const STEP_NS: i64 = 1_000_000_000; // 1s between events

fn main() {
	v_utils::clientside!();

	let dir = tempdir().expect("tempdir");
	let catalog = Catalog::new(dir.path());

	let pair = Pair::from_str("BTCUSDT").unwrap();
	let instrument = Instrument::Perp;
	let prec = PrecisionPriceQty { price: 2, qty: 5 };

	let mut live = LiveBook::persisting(catalog.clone(), ExchangeName::Binance, pair, instrument, prec, Arc::new(LiveClock));

	// Event time must track ingest time: the catalog indexes files by ingest wall-clock,
	// so synthetic events anchored far from `now` would never be selected on replay.
	let base = jiff::Timestamp::now().as_nanosecond() as i64;
	let start_ns = base - 1;

	// One snapshot, then a handful of deltas at strictly increasing event times.
	live.snapshot(&shape(base, &[(10_000, 5), (9_999, 3)], &[(10_001, 4), (10_002, 6)]));
	let deltas = [
		shape(base + STEP_NS, &[(10_000, 7)], &[(10_002, 0)]), // requote bid, remove an ask
		shape(base + 2 * STEP_NS, &[(9_998, 2)], &[(10_003, 1)]),
		shape(base + 3 * STEP_NS, &[(9_999, 0)], &[(10_001, 9)]), // remove a bid
	];
	for d in &deltas {
		live.delta(d, false);
	}
	let end_ns = base + 4 * STEP_NS;

	live.flush().expect("flush");
	assert!(!live.bids().is_empty() && !live.asks().is_empty(), "live book state empty after recording");

	let symbol = Symbol::new(pair, instrument);
	let cfg = ReplayConfig::new(start_ns, end_ns);
	let out: Vec<Data> = replay(&catalog, ExchangeName::Binance, symbol, &cfg).expect("replay").collect();
	tracing::info!(rows = out.len(), "replayed rows");

	// Strictly monotonic non-decreasing ts_event across the merged stream.
	let mut prev: i64 = i64::MIN;
	for d in &out {
		let ts = d.ts_event();
		assert!(ts >= prev, "merged stream not monotonic: {prev} > {ts}");
		prev = ts;
	}

	// Between two snapshots, delta monotonic_seq is strictly increasing.
	let mut last_delta_seq: Option<u64> = None;
	for d in &out {
		match d {
			Data::Delta(row) => {
				if let Some(prev) = last_delta_seq {
					assert!(row.monotonic_seq > prev, "delta seq regression: {prev} -> {}", row.monotonic_seq);
				}
				last_delta_seq = Some(row.monotonic_seq);
			}
			Data::Snapshot(_) => last_delta_seq = None,
			_ => {}
		}
	}

	// Typed fast-path read must equal the delta subsequence of the merged stream.
	let typed_deltas: Vec<BookDelta> = read_deltas(&catalog, ExchangeName::Binance, symbol, start_ns, end_ns).expect("read_deltas").collect();
	let merged_deltas: Vec<BookDelta> = out.iter().filter_map(|d| if let Data::Delta(r) = d { Some(*r) } else { None }).collect();
	assert_eq!(typed_deltas, merged_deltas, "typed read_deltas diverged from merged replay");

	tracing::info!("record_replay: ok ({} replayed rows)", out.len());
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
