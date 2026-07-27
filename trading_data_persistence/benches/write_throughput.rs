use std::{hint::black_box, sync::Arc};

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use tempfile::TempDir;
use trading_data_core::{Aggregate, ExchangeName, Instrument, Local, PrecisionPriceQty, Side, Span, Symbol, Ts, Venue};
use trading_data_persistence::{BookShape, BookUpdate, Catalog, Feather, Feed, Live, LiveClock, RotationPolicy, Trade};

const N_TRADES: u64 = 100_000;
const N_SNAPSHOTS: u64 = 200;
const LEVELS: i32 = 200;

fn test_symbol() -> Symbol {
	Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
}

fn prec() -> PrecisionPriceQty {
	PrecisionPriceQty { price: 2, qty: 5 }
}

#[library_benchmark]
fn push_100k_trades() {
	let dir = TempDir::new().unwrap();
	let cat = Catalog::new(dir.path());
	let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), prec(), RotationPolicy { max_bytes: None, max_age: None });
	for i in 0..N_TRADES {
		f.push(Trade {
			ts_venue_exec: Ts::from_nanos(i as i64),
			ts_venue_send: None,
			ts_local_recv: Some(Ts::from_nanos(i as i64)),
			monotonic_seq: i,
			trade_id: i,
			side: if i & 1 == 0 { Side::Buy } else { Side::Sell },
			price: (i as i32) as f64 / 100.0,
			qty: (i as u32) as f64 / 100_000.0,
		});
		f.maybe_flush(&cat).unwrap();
	}
	black_box(&f);
}

#[library_benchmark]
fn push_200_snapshots() {
	let dir = TempDir::new().unwrap();
	let cat = Catalog::new(dir.path());
	let symbol = test_symbol();
	let mut live = Live::new(cat, ExchangeName::Binance, symbol, prec(), true, Arc::new(LiveClock));
	let sink = live.sink();
	for i in 0..N_SNAPSHOTS {
		let shape = BookShape {
			ts: Aggregate {
				venue_exec: Span::at(Ts::<Venue>::from_nanos((i as i64) * 1_000_000_000)),
				local_recv: Span::at(Ts::<Local>::from_nanos((i as i64) * 1_000_000_000)),
			},
			prec: prec(),
			bids: (0..LEVELS).map(|p| (p, p as u32 + 1)).collect(),
			asks: (LEVELS..2 * LEVELS).map(|p| (p, p as u32 + 1)).collect(),
		};
		sink.book(BookUpdate::Snapshot(shape));
	}
	drop(sink);
	while live.next_batch().is_some() {}
	black_box(&live);
}

library_benchmark_group!(name = feather_push; benchmarks = push_100k_trades, push_200_snapshots);
main!(library_benchmark_groups = feather_push);
