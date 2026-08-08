//! A live watchpoint: real Bybit BTC-USDT perp trades + book streamed through the graph until
//! ctrl-c, recorded onto an [`exec_viz::Viz`] tape whose server runs alongside the feed. Nothing is
//! asserted and nothing is persisted — the point is a browser sitting at the tape's growing end,
//! watching the graph decide.
//!
//! The graph consumes on a blocking thread while the ws pumps feed it, so memory stays bounded to
//! the un-emitted window no matter how long the session runs; the tape is the only record kept, and
//! `Tape::thin` bounds that. It is never sealed — the recording is only interesting while it grows.
//!
//! This example owns the runtime — exec_viz is only a library. `nix run .#live` builds the
//! `exec_viz_web` bundle and runs this against it.

use std::{path::PathBuf, sync::Arc};

use exchange_interactions::prelude::*;
use exec_viz::{Backpressure, Tape, Viz};
use trading_data::{Catalog, Cell, Feed, Live, LiveClock};
use trading_data_live_example::{nodes::Graph, pair, pump_book, pump_trades, symbol};
use v_utils::*;

/// This app's slot in the devShell's `PORT` range — the devShell owns the base, each app claims a
/// slot in it, so several can be up at once.
const ORDINAL: u16 = 2;
/// Retained ticks. An indefinite run outlives any buffer, so the tape thins with age rather than
/// forgetting its front — the last minutes stay tick-exact and the first stay walkable.
const SCROLLBACK: usize = 100_000;

#[tokio::main]
async fn main() {
	v_utils::clientside!();

	// Nothing replays this run, so nothing is teed to disk: an indefinite session with `record: true`
	// grows the catalog without bound.
	let catalog = Catalog::new(&PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/live_cache")));

	let mut bybit = Bybit::default();
	let info = bybit.exchange_info(Instrument::Perp).await.expect("bybit exchange_info");
	let pi = info.pairs.get(&pair()).expect("BTCUSDT listed on Bybit perp");
	let prec = PrecisionPriceQty {
		price: pi.price_precision,
		qty: pi.qty_precision,
	};

	let mut live = Live::new(catalog, ExchangeName::Bybit, symbol(), prec, false, Arc::new(LiveClock));
	let trades_stream = bybit.ws_trades(&[pair()], Instrument::Perp).await.expect("open ws_trades");
	let book_stream = bybit.ws_book(&[pair()], Instrument::Perp).await.expect("open ws_book");

	// `Bars` only fires on a close, so a 100ms bucket still yields one candle per minute — it is the
	// per-event series either side of the price pane that wants the fine grain.
	// Dropping: a fill must never wait on a study aid. What was dropped is counted, not hidden —
	// `ActivationFrame::dropped`.
	let (tape, mut recorder) = Tape::new(Some(<trading_data::Bars<{ TF_1MIN }> as Cell>::NAME), SCROLLBACK, 100, Backpressure::Drop);
	let viz = tape.viz();
	let mut server = tokio::task::JoinSet::new();
	let base: u16 = std::env::var("PORT").expect("PORT: the devShell sets the base of the port range").parse().expect("PORT is a u16");
	server.spawn(viz.clone().serve_on(Viz::bind(base + ORDINAL).await));

	// graph consumes on a blocking thread (blocking recv) while the async pumps feed it.
	let ts_sink = live.sink();
	let bk_sink = live.sink();
	let consumer = tokio::task::spawn_blocking(move || {
		let mut graph = Graph::default();
		while let Some(l) = live.next() {
			let ts_ns = l.ts_venue.as_nanos();
			graph.tick_obs(ts_ns, l.into(), &mut recorder.at(ts_ns));
		}
	});

	let mut pumps = tokio::task::JoinSet::new();
	pumps.spawn(pump_trades(trades_stream, ts_sink));
	pumps.spawn(pump_book(book_stream, bk_sink));

	println!("streaming Bybit BTCUSDT perp through the graph on http://localhost:{} — ctrl-c to stop", base + ORDINAL);
	tokio::signal::ctrl_c().await.expect("ctrl-c handler installs once");
	pumps.shutdown().await; // drop the pump sinks → channel disconnects → the consumer drains and returns
	consumer.await.expect("consumer thread panicked");
}
