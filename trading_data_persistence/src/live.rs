use std::{collections::BTreeMap, sync::Arc};

use v_utils::trades::{ExchangeName, Instrument, Pair, PrecisionPriceQty, Side, Symbol};

use crate::{
	book::BookShape,
	catalog::{Catalog, CatalogError},
	clock::Clock,
	feather::Feather,
	row::{BookDelta, BookSnapshot, Row as _},
};

pub struct LiveBook {
	bids: BTreeMap<i32, u32>,
	asks: BTreeMap<i32, u32>,
	sink: Option<BookSink>,
}

impl LiveBook {
	pub fn in_memory() -> Self {
		Self {
			bids: BTreeMap::new(),
			asks: BTreeMap::new(),
			sink: None,
		}
	}

	pub fn persisting(catalog: Catalog, exchange: ExchangeName, pair: Pair, instrument: Instrument, prec: PrecisionPriceQty, clock: Arc<dyn Clock>) -> Self {
		let symbol = Symbol::new(pair, instrument);
		let sink = BookSink {
			snapshots: Feather::<BookSnapshot>::new(exchange, symbol, prec, BookSnapshot::POLICY),
			deltas: Feather::<BookDelta>::new(exchange, symbol, prec, BookDelta::POLICY),
			catalog,
			clock,
			prec,
			monotonic: 0,
		};
		Self {
			bids: BTreeMap::new(),
			asks: BTreeMap::new(),
			sink: Some(sink),
		}
	}

	pub fn snapshot(&mut self, shape: &BookShape) {
		self.bids = shape.bids.clone();
		self.asks = shape.asks.clone();
		if let Some(sink) = &mut self.sink {
			sink.persist_snapshot(shape);
		}
	}

	pub fn delta(&mut self, shape: &BookShape, gapped: bool) {
		apply(&mut self.bids, &shape.bids);
		apply(&mut self.asks, &shape.asks);
		if let Some(sink) = &mut self.sink {
			sink.persist_delta(shape, gapped);
		}
	}

	pub fn bids(&self) -> &BTreeMap<i32, u32> {
		&self.bids
	}

	pub fn asks(&self) -> &BTreeMap<i32, u32> {
		&self.asks
	}

	pub fn flush(&mut self) -> Result<(), CatalogError> {
		if let Some(sink) = &mut self.sink {
			sink.snapshots.flush(&sink.catalog)?;
			sink.deltas.flush(&sink.catalog)?;
		}
		Ok(())
	}
}

fn apply(side: &mut BTreeMap<i32, u32>, changes: &BTreeMap<i32, u32>) {
	for (&price, &qty) in changes {
		if qty == 0 {
			side.remove(&price);
		} else {
			side.insert(price, qty);
		}
	}
}

struct BookSink {
	catalog: Catalog,
	clock: Arc<dyn Clock>,
	prec: PrecisionPriceQty,
	monotonic: u64,
	snapshots: Feather<BookSnapshot>,
	deltas: Feather<BookDelta>,
}

impl BookSink {
	fn persist_snapshot(&mut self, shape: &BookShape) {
		let ts = shape.ts_event.as_nanosecond() as i64;
		let now = self.clock.now_ns();
		self.monotonic += 1;
		self.snapshots.push(BookSnapshot {
			ts_event: ts,
			ts_init: now,
			monotonic_seq: self.monotonic,
			bid_prices: shape.bids.keys().copied().collect(),
			bid_qtys: shape.bids.values().copied().collect(),
			ask_prices: shape.asks.keys().copied().collect(),
			ask_qtys: shape.asks.values().copied().collect(),
		});
		self.snapshots.maybe_flush(&self.catalog).expect("snapshot feather flush failed: catalog state corrupted");
	}

	fn persist_delta(&mut self, shape: &BookShape, gapped: bool) {
		let ts = shape.ts_event.as_nanosecond() as i64;
		let now = self.clock.now_ns();
		let p_scale = 10f64.powi(self.prec.price as i32);
		let q_scale = 10f64.powi(self.prec.qty as i32);
		let mut push = |side: Side, price: i32, qty: u32| {
			self.monotonic += 1;
			self.deltas.push(BookDelta {
				ts_event: ts,
				ts_init: now,
				monotonic_seq: self.monotonic,
				gapped,
				side,
				price: price as f64 / p_scale,
				qty: qty as f64 / q_scale,
			});
		};
		for (&price, &qty) in &shape.bids {
			push(Side::Buy, price, qty);
		}
		for (&price, &qty) in &shape.asks {
			push(Side::Sell, price, qty);
		}
		self.deltas.maybe_flush(&self.catalog).expect("delta feather flush failed: catalog state corrupted");
	}
}
