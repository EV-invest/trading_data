#![feature(default_field_values)]
// the graph resolves its node set by trampolining between `#[node]` shims and the driver.
#![recursion_limit = "512"]
//! The live graph and the ws plumbing that feeds it, for the watchpoint binary here.
//!
//! The exchange_interactions → push-handle layer is a temporary bridge; when exchange_interactions learns a native
//! `Listener` it dies. Kept thin and self-contained accordingly.

pub mod nodes;

use exchange_interactions::prelude::*;
use trading_data::Sink;

pub fn pair() -> Pair {
	Pair::from_str("BTCUSDT").expect("static pair")
}

pub fn symbol() -> Symbol {
	Symbol::new(pair(), Instrument::Perp)
}

pub async fn pump_trades(mut stream: Box<dyn ExchangeStream<Item = BatchTrades>>, sink: Sink) {
	while let Ok(batch) = stream.next().await {
		for bt in batch {
			sink.trades(bt);
		}
	}
}

pub async fn pump_book(mut stream: Box<dyn ExchangeStream<Item = BookUpdate>>, sink: Sink) {
	while let Ok(batch) = stream.next().await {
		for update in batch {
			sink.book(update);
		}
	}
}
