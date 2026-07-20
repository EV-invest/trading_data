#![feature(default_field_values)]
//! The end-to-end live proof: 15s of real Bybit BTC-USDT perp trades + book streamed through the
//! graph *concurrently* (the graph consumes on a blocking thread while ws pumps feed it — bounded
//! memory, no end-of-stream buffering), recorded (ts_init stamped at ingest), then replayed from
//! the recording. The two flattened event streams and graph outputs must be identical — the
//! live≡backtest invariant on real data.
//!
//! Concurrent chunking means Live's batch *boundaries* differ from Replay's (Replay sees the whole
//! range at once), so we compare the flattened per-event stream + fold outputs, not batch lengths —
//! that IS the meaningful invariant (batching never alters fold order).
//!
//! The v_exchanges → push-handle layer here is a temporary bridge; when v_exchanges learns a native
//! `Listener` it dies. Kept thin and self-contained accordingly.

mod nodes;

use std::{path::PathBuf, sync::Arc, time::Duration};

use nodes::{Batches, Graph};
use trading_data::{Batch, Catalog, Feed, LatencyConfig, Live, LiveClock, Replay, Sink, required_lanes, trades_from_batch};
use v_exchanges::prelude::*;

const SECONDS: u64 = 15;

#[tokio::main]
async fn main() {
	v_utils::clientside!();

	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/live_cache"));
	let _ = std::fs::remove_dir_all(&cache); // fresh session so replay reads only what we just recorded
	let catalog = Catalog::new(&cache);

	let mut bybit = Bybit::default();
	let info = bybit.exchange_info(Instrument::Perp).await.expect("bybit exchange_info");
	let pi = info.pairs.get(&pair()).expect("BTCUSDT listed on Bybit perp");
	let prec = PrecisionPriceQty {
		price: pi.price_precision,
		qty: pi.qty_precision,
	};

	// --- live: stream 15s of real trades + book through the graph concurrently with the pumps ---
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), prec, true, Arc::new(LiveClock));
	let trades_stream = bybit.ws_trades(&[pair()], Instrument::Perp).await.expect("open ws_trades");
	let book_stream = bybit.ws_book(&[pair()], Instrument::Perp).await.expect("open ws_book");

	// graph consumes on a blocking thread (blocking recv) while the async pumps feed it.
	let ts_sink = live.sink();
	let bk_sink = live.sink();
	let consumer = tokio::task::spawn_blocking(move || {
		let mut graph = Graph::default();
		run(&mut live, &mut graph)
	});

	let mut pumps = tokio::task::JoinSet::new();
	pumps.spawn(pump_trades(trades_stream, ts_sink));
	pumps.spawn(pump_book(book_stream, bk_sink));

	println!("streaming {SECONDS}s of Bybit BTCUSDT perp through the graph…");
	tokio::time::sleep(Duration::from_secs(SECONDS)).await;
	pumps.shutdown().await; // drop the pump sinks → channel disconnects → the consumer drains and returns

	let live_out = consumer.await.expect("consumer thread panicked");

	println!(
		"live: {} events, {} trades, {} book deltas, cvd={:.2} bookflow={:.4}",
		live_out.events.len(),
		live_out.n_trades,
		live_out.n_deltas,
		live_out.cvd,
		live_out.book_flow
	);
	assert!(live_out.n_trades > 0, "no trades arrived — Bybit ws_trades broken or market dead");
	assert!(live_out.n_deltas > 0, "no book deltas arrived — Bybit ws_book broken");

	// --- replay the recording; recorded ts_init ⇒ deterministic, no latency sim ---
	let lanes = required_lanes::<Graph>();
	let latency = LatencyConfig {
		p68: Duration::from_millis(10),
		p95: Duration::from_millis(30),
		p997: Duration::from_millis(90),
		seed: 0,
	};
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), 0, i64::MAX, &lanes, latency);
	let mut graph = Graph::default();
	let replay_out = run(&mut replay, &mut graph);

	assert_eq!(live_out.events, replay_out.events, "flattened event streams diverged");
	assert_eq!(live_out.n_trades, replay_out.n_trades, "trade count diverged");
	assert_eq!(live_out.n_deltas, replay_out.n_deltas, "delta count diverged");
	assert_eq!(live_out.cvd, replay_out.cvd, "cvd diverged");
	assert_eq!(live_out.book_flow, replay_out.book_flow, "book flow diverged");

	println!("live≡replay on {} real events. ok", replay_out.events.len());
}
fn pair() -> Pair {
	Pair::from_str("BTCUSDT").expect("static pair")
}

fn symbol() -> Symbol {
	Symbol::new(pair(), Instrument::Perp)
}

/// One emitted event, in emission order — robust to how the two feeds chunk into batches.
#[derive(Debug, PartialEq)]
enum Ev {
	Trade(u64),
	Delta(u64),
	Anchor(usize, usize),
}

struct RunOut {
	events: Vec<Ev>,
	cvd: f64,
	book_flow: f64,
	n_trades: u64,
	n_deltas: u64,
}

fn run(feed: &mut impl Feed, graph: &mut Graph) -> RunOut {
	let mut o = RunOut {
		events: Vec::new(),
		cvd: 0.0,
		book_flow: 0.0,
		n_trades: 0,
		n_deltas: 0,
	};
	while let Some(b) = feed.next_batch() {
		match b {
			Batch::Trades(ts) => {
				o.n_trades += ts.len() as u64;
				o.events.extend(ts.iter().map(|t| Ev::Trade(t.monotonic_seq)));
				let out = graph.tick(Batches { trades: ts, ..Default::default() });
				if let Some(&c) = out.cvd.last() {
					o.cvd = c;
				}
			}
			Batch::Book(ds) => {
				o.n_deltas += ds.len() as u64;
				o.events.extend(ds.iter().map(|d| Ev::Delta(d.monotonic_seq)));
				let out = graph.tick(Batches { book: ds, ..Default::default() });
				if let Some(&f) = out.book_flow.last() {
					o.book_flow = f;
				}
			}
			Batch::BookAnchor(s) => o.events.push(Ev::Anchor(s.bids.len(), s.asks.len())),
			other => panic!("unexpected lane in live replay: {other:?}"),
		}
	}
	o
}

async fn pump_trades(mut stream: Box<dyn ExchangeStream<Item = BatchTrades>>, sink: Sink) {
	let mut seq = 0u64;
	while let Ok(batch) = stream.next().await {
		for bt in &batch {
			for row in trades_from_batch(bt, &mut seq) {
				sink.trade(row);
			}
		}
	}
}

async fn pump_book(mut stream: Box<dyn ExchangeStream<Item = BookUpdate>>, sink: Sink) {
	while let Ok(batch) = stream.next().await {
		for update in batch {
			sink.book(update);
		}
	}
}

