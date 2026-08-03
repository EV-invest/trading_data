//! The end-to-end live proof: 15s of real Bybit BTC-USDT perp trades + book streamed through the
//! graph *concurrently* (the graph consumes on a blocking thread while ws pumps feed it — bounded
//! memory, no end-of-stream buffering), recorded (reception stamped at ingest), then replayed from
//! the recording. The two flattened event streams and graph outputs must be identical — the
//! live≡backtest invariant on real data.
//!
//! Concurrent chunking means Live's batch *boundaries* differ from Replay's (Replay sees the whole
//! range at once), so we compare the flattened per-event stream + fold outputs, not batch lengths —
//! that IS the meaningful invariant (batching never alters fold order).
//!
//! The graph and the ws plumbing are `trading_data_live_example`'s, so the thing proven equivalent
//! is the very thing the watchpoint binary streams. Nothing here serves: a proof that has to be
//! looked at is not a proof.

use std::{path::PathBuf, sync::Arc, time::Duration};

use trading_data::{Catalog, Exact, Feed, LatencyConfig, Live, LiveClock, ReadClock, Replay, Ts, required_lanes};
use trading_data_live_example::{nodes::Graph, pair, pump_book, pump_trades, symbol};
use v_exchanges::prelude::*;

const SECONDS: u64 = 15;
/// Small: this example asserts the live≡replay event stream, and short runs weave across lanes more
/// often — more boundaries is more of the invariant exercised.
const CLOCK: ReadClock = ReadClock::from(Exact::from_nanos(10_000_000));

/// What a consumer reads off the folded book: epoch, level count, best bid, best ask.
type BookRead = (u64, usize, Option<(Price, Qty)>, Option<(Price, Qty)>);
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
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), prec, true, Arc::new(LiveClock), CLOCK);
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
	println!("book: epoch={} levels={} bid={:?} ask={:?}", live_out.book.0, live_out.book.1, live_out.book.2, live_out.book.3);
	assert!(live_out.n_trades > 0, "no trades arrived — Bybit ws_trades broken or market dead");
	assert!(live_out.n_deltas > 0, "no book deltas arrived — Bybit ws_book broken");

	// --- replay the recording; recorded reception ⇒ deterministic, no latency sim ---
	let lanes = required_lanes::<Graph>();
	let latency = LatencyConfig {
		p68: Duration::from_millis(10),
		p95: Duration::from_millis(30),
		p997: Duration::from_millis(90),
		seed: 0,
	};
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), Ts::MIN, Ts::MAX, &lanes, latency, CLOCK);
	let mut graph = Graph::default();
	let replay_out = run(&mut replay, &mut graph);

	assert_eq!(live_out.events, replay_out.events, "flattened event streams diverged");
	assert_eq!(live_out.n_trades, replay_out.n_trades, "trade count diverged");
	assert_eq!(live_out.n_deltas, replay_out.n_deltas, "delta count diverged");
	assert_eq!(live_out.cvd, replay_out.cvd, "cvd diverged");
	assert_eq!(live_out.book_flow, replay_out.book_flow, "book flow diverged");
	assert_eq!(live_out.book, replay_out.book, "folded book diverged");
	assert!(live_out.book.2.is_some(), "the book never synced from our own checkpoints: {:?}", live_out.book);

	println!("live≡replay on {} real events. ok", replay_out.events.len());
}

/// One emitted event, in emission order — robust to how the two feeds chunk into runs.
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
	/// What a consumer reads off the folded book at the end — the shadow layer's acceptance test.
	book: BookRead,
}

fn run(feed: &mut impl Feed, graph: &mut Graph) -> RunOut {
	let mut o = RunOut {
		events: Vec::new(),
		cvd: 0.0,
		book_flow: 0.0,
		n_trades: 0,
		n_deltas: 0,
		book: (0, 0, None, None),
	};
	while let Some(l) = feed.next() {
		let d = l.deltas.cols();
		o.n_trades += l.trades.len() as u64;
		o.n_deltas += d.len() as u64;
		o.events.extend(l.trades.monotonic_seq.iter().map(|&s| Ev::Trade(s)));
		o.events.extend(d.monotonic_seq.iter().map(|&s| Ev::Delta(s)));
		o.events.extend(l.anchor.map(|s| Ev::Anchor(s.bids.len(), s.asks.len())));
		let out = graph.tick(l.ts_venue.as_nanos(), l.into());
		if let Some(&c) = out.cvd.last() {
			o.cvd = c;
		}
		if let Some(&f) = out.book_flow.last() {
			o.book_flow = f;
		}
		if let Some(b) = out.book {
			o.book = (b.epoch(), b.len(), b.best_bid(), b.best_ask());
		}
	}
	o
}
