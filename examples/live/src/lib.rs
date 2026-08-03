#![feature(default_field_values)]
// the graph resolves its node set by trampolining between `#[node]` shims and the driver.
#![recursion_limit = "512"]
//! The live graph and the ws plumbing that feeds it, shared by the two things that want them: the
//! watchpoint binary here, and `trading_data_live_equiv`'s live≡replay proof.
//!
//! The v_exchanges → push-handle layer is a temporary bridge; when v_exchanges learns a native
//! `Listener` it dies. Kept thin and self-contained accordingly.

pub mod nodes;

use trading_data::Sink;
use v_exchanges::prelude::*;

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
