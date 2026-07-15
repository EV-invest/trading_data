use std::{collections::BTreeMap, sync::Arc};

use v_exchanges::{BookShape, ExchangeName, Instrument, PrecisionPriceQty, Symbol};
use v_utils::trades::Pair;

use crate::{
	catalog::{Catalog, CatalogError, Lane, LaneKey},
	clock::Clock,
	feather::{Feather, RotationPolicy},
	schema::{BookDelta, FileMetadata},
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
		let meta = FileMetadata {
			exchange: exchange.to_string(),
			pair: pair.to_string(),
			price_precision: prec.price,
			qty_precision: prec.qty,
		};
		let sink = BookSink {
			snapshots: Feather::new_snapshots(LaneKey::book(Lane::Snapshots, exchange, symbol), meta.clone(), RotationPolicy::snapshots()),
			deltas: Feather::new_deltas(LaneKey::book(Lane::Deltas, exchange, symbol), meta, RotationPolicy::deltas()),
			catalog,
			clock,
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
	monotonic: u64,
	snapshots: Feather,
	deltas: Feather,
}

impl BookSink {
	fn persist_snapshot(&mut self, shape: &BookShape) {
		let ts = shape.ts_event.as_nanosecond() as i64;
		let now = self.clock.now_ns();
		self.monotonic += 1;
		self.snapshots.push_snapshot(
			ts,
			now,
			self.monotonic,
			shape.bids.keys().copied(),
			shape.bids.values().copied(),
			shape.asks.keys().copied(),
			shape.asks.values().copied(),
		);
		self.snapshots.maybe_flush(&self.catalog).expect("snapshot feather flush failed: catalog state corrupted");
	}

	fn persist_delta(&mut self, shape: &BookShape, gapped: bool) {
		let ts = shape.ts_event.as_nanosecond() as i64;
		let now = self.clock.now_ns();
		for (&price, &qty) in &shape.bids {
			self.monotonic += 1;
			self.deltas.push_delta(BookDelta {
				ts_event: ts,
				ts_init: now,
				monotonic_seq: self.monotonic,
				gapped,
				side: 0,
				price_raw: price,
				qty_raw: qty,
			});
		}
		for (&price, &qty) in &shape.asks {
			self.monotonic += 1;
			self.deltas.push_delta(BookDelta {
				ts_event: ts,
				ts_init: now,
				monotonic_seq: self.monotonic,
				gapped,
				side: 1,
				price_raw: price,
				qty_raw: qty,
			});
		}
		self.deltas.maybe_flush(&self.catalog).expect("delta feather flush failed: catalog state corrupted");
	}
}
