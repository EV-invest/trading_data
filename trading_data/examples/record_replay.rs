use std::{str::FromStr as _, sync::Arc, time::Duration};

use tempfile::tempdir;
use trading_data::{BookDelta, Catalog, Data, LiveBook, LiveClock, ReplayConfig, read_deltas, replay};
use v_exchanges::{adapters::binance::BinanceOption, prelude::*};

const RECORD_DURATION: Duration = Duration::from_secs(60);
const SNAPSHOT_FREQ: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
	v_utils::clientside!();

	let dir = tempdir().expect("tempdir");
	let catalog = Catalog::new(dir.path());

	let pair = Pair::from_str("BTCUSDT").unwrap();
	let instrument = Instrument::Perp;

	let mut binance = Binance::default();
	binance.update_default_option(BinanceOption::BookSnapshotFreq(Some(SNAPSHOT_FREQ)));

	let mut conn = binance.book_connection(&[pair], instrument).await.expect("book_connection");
	let prec = conn.pair_precisions()[&pair];

	let mut live = LiveBook::persisting(catalog.clone(), ExchangeName::Binance, pair, instrument, prec, Arc::new(LiveClock));

	let start_ns = jiff::Timestamp::now().as_nanosecond() as i64;
	tracing::info!(start_ns, "recording {RECORD_DURATION:?} of Binance BTCUSDT perp book");

	let deadline = tokio::time::Instant::now() + RECORD_DURATION;
	let mut snapshots = 0_u32;
	let mut deltas = 0_u32;
	loop {
		tokio::select! {
			biased;
			_ = tokio::time::sleep_until(deadline) => break,
			res = conn.next() => { for update in res.expect("ws stream errored") {
				match update {
					BookUpdate::Snapshot(s) => { snapshots += 1; live.snapshot(&s); }
					BookUpdate::BatchDelta { shape, gapped } => { deltas += 1; live.delta(&shape, gapped); }
				}
			} }
		}
	}
	let end_ns = jiff::Timestamp::now().as_nanosecond() as i64;
	tracing::info!(snapshots, deltas, "stream closed; flushing + replaying");

	live.flush().expect("flush");

	assert!(snapshots >= 1, "expected at least one snapshot in 60s");
	assert!(deltas >= 1, "expected at least one delta in 60s");
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

	tracing::info!("record_replay: ok ({snapshots} snapshots, {deltas} deltas, {} replayed rows)", out.len());
}
