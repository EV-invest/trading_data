#![feature(default_field_values)]
// the graph resolves its node set by trampolining between `#[node]` shims and the driver.
#![recursion_limit = "512"]
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
//! The v_exchanges → push-handle layer here is a temporary bridge; when v_exchanges learns a native
//! `Listener` it dies. Kept thin and self-contained accordingly.
//!
//! An [`exec_viz::Viz`] rides the live pass and its server runs alongside the feed, so the graph is
//! watchable as it fills; it outlives the asserts so the run stays browsable afterwards. This
//! example owns the runtime — exec_viz is only a library. `nix run .#live` builds the
//! `exec_viz_web` bundle and runs this against it.

mod nodes;

use std::{path::PathBuf, sync::Arc, time::Duration};

use exec_viz::Viz;
use nodes::Graph;
use trading_data::{BatchWindow, Catalog, Exact, Feed, LatencyConfig, Live, LiveClock, Replay, Sink, Ts, required_lanes};
use v_exchanges::prelude::*;

const SECONDS: u64 = 15;
/// This app's slot in the devShell's `PORT` range — the devShell owns the base, each app claims a
/// slot in it, so several can be up at once.
const ORDINAL: u16 = 2;
/// Small: this example asserts the live≡replay event stream, and short runs weave across lanes more
/// often — more boundaries is more of the invariant exercised.
const WINDOW: BatchWindow = BatchWindow::from(Exact::from_nanos(10_000_000));

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
	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), prec, true, Arc::new(LiveClock), WINDOW);
	let trades_stream = bybit.ws_trades(&[pair()], Instrument::Perp).await.expect("open ws_trades");
	let book_stream = bybit.ws_book(&[pair()], Instrument::Perp).await.expect("open ws_book");

	// No bars close in 15s, so there's no price node — the chart is the per-event series alone,
	// sampled every 100ms.
	let viz = Viz::new(None, 100_000, 100);
	let mut server = tokio::task::JoinSet::new();
	let base: u16 = std::env::var("PORT").expect("PORT: the devShell sets the base of the port range").parse().expect("PORT is a u16");
	// Never sealed: the feed *is* the recording, and the tape grows for as long as it runs.
	server.spawn(viz.clone().serve_on(Viz::bind(base + ORDINAL).await));

	// graph consumes on a blocking thread (blocking recv) while the async pumps feed it.
	let ts_sink = live.sink();
	let bk_sink = live.sink();
	let recorder = viz.clone();
	let consumer = tokio::task::spawn_blocking(move || {
		let mut graph = Graph::default();
		run(&mut live, &mut graph, &mut Some(recorder))
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
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), Ts::MIN, Ts::MAX, &lanes, latency, WINDOW);
	let mut graph = Graph::default();
	let replay_out = run(&mut replay, &mut graph, &mut None);

	assert_eq!(live_out.events, replay_out.events, "flattened event streams diverged");
	assert_eq!(live_out.n_trades, replay_out.n_trades, "trade count diverged");
	assert_eq!(live_out.n_deltas, replay_out.n_deltas, "delta count diverged");
	assert_eq!(live_out.cvd, replay_out.cvd, "cvd diverged");
	assert_eq!(live_out.book_flow, replay_out.book_flow, "book flow diverged");
	assert_eq!(live_out.book, replay_out.book, "folded book diverged");
	assert!(live_out.book.2.is_some(), "the book never synced from our own checkpoints: {:?}", live_out.book);

	println!("live≡replay on {} real events. ok", replay_out.events.len());
	server.join_all().await; // the recorded run stays browsable until ctrl-c
}
fn pair() -> Pair {
	Pair::from_str("BTCUSDT").expect("static pair")
}

fn symbol() -> Symbol {
	Symbol::new(pair(), Instrument::Perp)
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

/// `viz` is `Some` only for the live pass — the replay pass re-derives the same outputs and would
/// just double the tape.
fn run(feed: &mut impl Feed, graph: &mut Graph, viz: &mut Option<Viz>) -> RunOut {
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
		let ts_ns = l.ts_venue.as_nanos();
		let out = match viz {
			Some(v) => graph.tick_obs(l.into(), v.at(ts_ns)),
			None => graph.tick(l.into()),
		};
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

async fn pump_trades(mut stream: Box<dyn ExchangeStream<Item = BatchTrades>>, sink: Sink) {
	while let Ok(batch) = stream.next().await {
		for bt in batch {
			sink.trades(bt);
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
