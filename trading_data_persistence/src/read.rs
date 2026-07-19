use std::time::Duration;

use arrow::{
	array::RecordBatch,
	datatypes::{Schema, SchemaRef},
};
use v_utils::trades::{Asset, ExchangeName, Symbol};

use crate::{
	book::BookShape,
	catalog::{Catalog, CatalogError, FileEntry, LaneKey},
	row::{BookDelta, BookSnapshot, Mc, Oi, Row, SCHEMA_VERSION, Trade, UnixNanos, prec_from_schema, sealed::Sealed},
};

/// A stored file's `schema_version` must match ours exactly — no silent cross-version reads.
fn assert_schema_version(schema: &Schema) {
	let v = schema.metadata().get("schema_version").map(String::as_str);
	assert_eq!(
		v,
		Some(SCHEMA_VERSION),
		"catalog schema_version {v:?} != current {SCHEMA_VERSION:?}: nuke the cache and re-ingest"
	);
}

/// Snapshots older than this cannot seed a book: too much drift between anchor and range start.
const MAX_ANCHOR_AGE: Duration = Duration::from_secs(15 * 60);

/// Streams one lane's rows in `[start, end]`, one parquet file at a time. No whole-lane
/// materialization; a mid-stream read failure of a catalog-owned file is unrecoverable and panics.
pub struct LaneReader<T: Row> {
	catalog: Catalog,
	files: std::vec::IntoIter<FileEntry>,
	batches: std::vec::IntoIter<RecordBatch>,
	rows: std::vec::IntoIter<T>,
	// current file's schema: per-batch schemas drop the key-value metadata decode needs
	file_schema: Option<SchemaRef>,
	// file-metadata consistency across the read range (e.g. precisions)
	sig: Option<String>,
	start: UnixNanos,
	end: UnixNanos,
}

impl<T: Row> Iterator for LaneReader<T> {
	type Item = T;

	fn next(&mut self) -> Option<T> {
		loop {
			for r in self.rows.by_ref() {
				if r.ts_event() >= self.start && r.ts_event() <= self.end {
					return Some(r);
				}
			}
			if let Some(batch) = self.batches.next() {
				let schema = self.file_schema.as_ref().expect("set when file opened");
				self.rows = T::decode(&batch, schema).into_iter();
				continue;
			}
			let file = self.files.next()?;
			let (schema, batches) = self.catalog.read(&file.path).expect("catalog file unreadable during read");
			assert_schema_version(schema.as_ref());
			if let Some(sig) = T::file_sig(schema.as_ref()) {
				match &self.sig {
					Some(prev) => assert_eq!(prev, &sig, "inconsistent file metadata across read range"),
					None => self.sig = Some(sig),
				}
			}
			self.file_schema = Some(schema);
			self.batches = batches.into_iter();
		}
	}
}

fn lane_reader<T: Row>(catalog: &Catalog, key: LaneKey, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<T>, CatalogError> {
	let files = catalog.list_range(&key, start, end)?;
	Ok(LaneReader {
		catalog: catalog.clone(),
		files: files.into_iter(),
		batches: Vec::new().into_iter(),
		rows: Vec::new().into_iter(),
		file_schema: None,
		sig: None,
		start,
		end,
	})
}

pub fn read_trades(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<Trade>, CatalogError> {
	lane_reader(catalog, LaneKey::Trades { exchange, symbol }, start, end)
}

pub fn read_oi(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<Oi>, CatalogError> {
	lane_reader(catalog, LaneKey::Oi { exchange, symbol }, start, end)
}

pub fn read_mc(catalog: &Catalog, asset: Asset, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<Mc>, CatalogError> {
	lane_reader(catalog, LaneKey::Mc { asset }, start, end)
}

/// One symbol only. Seeds the book from the freshest snapshot at or before `start` (within
/// [`MAX_ANCHOR_AGE`]), then streams the post-`start` deltas. Mid-range snapshots stay internal.
/// Crate-private: the `sync` weaver is the public path; it additionally weaves in-range snapshots.
#[cfg(test)]
pub(crate) fn read_book(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<(Option<BookShape>, LaneReader<BookDelta>), CatalogError> {
	let anchor = pick_anchor(catalog, exchange, symbol, start)?;
	let deltas = lane_reader(catalog, LaneKey::BookDeltas { exchange, symbol }, start, end)?;
	Ok((anchor, deltas))
}

pub(crate) fn read_book_deltas(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<BookDelta>, CatalogError> {
	lane_reader(catalog, LaneKey::BookDeltas { exchange, symbol }, start, end)
}

pub(crate) fn read_book_snapshots(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<BookSnapshot>, CatalogError> {
	lane_reader(catalog, LaneKey::BookSnapshots { exchange, symbol }, start, end)
}

/// Price/qty precision stored in a book lane's files (deltas preferred, else snapshots). `None`
/// when neither lane has any file yet.
pub(crate) fn book_prec(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol) -> Result<Option<v_utils::trades::PrecisionPriceQty>, CatalogError> {
	for key in [LaneKey::BookDeltas { exchange, symbol }, LaneKey::BookSnapshots { exchange, symbol }] {
		if let Some(file) = catalog.list(&key)?.first() {
			let (schema, _) = catalog.read(&file.path)?;
			assert_schema_version(schema.as_ref());
			return Ok(Some(prec_from_schema(schema.as_ref())));
		}
	}
	Ok(None)
}

pub(crate) fn pick_anchor(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos) -> Result<Option<BookShape>, CatalogError> {
	let key = LaneKey::BookSnapshots { exchange, symbol };
	let files = catalog.list(&key)?;
	let max_age_ns = MAX_ANCHOR_AGE.as_nanos() as i64;

	let candidate = files.iter().rev().find(|f| f.ts_min <= start);

	if let Some(file) = candidate {
		let mut newest: Option<(BookSnapshot, BookShape)> = None;
		let (schema, batches) = catalog.read(&file.path)?;
		assert_schema_version(schema.as_ref());
		let prec = prec_from_schema(schema.as_ref());
		for batch in batches {
			for row in BookSnapshot::decode(&batch, schema.as_ref()) {
				if row.ts_event > start {
					continue;
				}
				let take = match &newest {
					None => true,
					Some((cur, _)) => row.ts_event > cur.ts_event,
				};
				if take {
					let ts_event = jiff::Timestamp::from_nanosecond(row.ts_event as i128).expect("stored ts in range");
					let ts_init = jiff::Timestamp::from_nanosecond(row.ts_init.unwrap_or(row.ts_event) as i128).expect("stored ts in range");
					let shape = BookShape {
						ts_event,
						ts_init,
						ts_last: ts_init,
						prec,
						bids: row.bid_prices.iter().copied().zip(row.bid_qtys.iter().copied()).collect(),
						asks: row.ask_prices.iter().copied().zip(row.ask_qtys.iter().copied()).collect(),
					};
					newest = Some((row, shape));
				}
			}
		}
		if let Some((row, shape)) = newest
			&& start - row.ts_event <= max_age_ns
		{
			return Ok(Some(shape));
		}
	}

	let first_in_range = files.iter().find(|f| f.ts_max >= start);
	if let Some(f) = first_in_range {
		tracing::warn!(
			file = %f.path.display(),
			start,
			max_anchor_age_secs = MAX_ANCHOR_AGE.as_secs(),
			"no recent anchor; book starts unseeded",
		);
	} else {
		tracing::warn!(start, "no snapshot files at or after start; book starts unseeded");
	}
	Ok(None)
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use v_utils::trades::{Instrument, PrecisionPriceQty, Side};

	use super::*;
	use crate::feather::{Feather, RotationPolicy};

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn prec() -> PrecisionPriceQty {
		PrecisionPriceQty { price: 2, qty: 5 }
	}

	const FOREVER: RotationPolicy = RotationPolicy { max_bytes: None, max_age: None };

	fn write_snapshot(cat: &Catalog, ts: i64) {
		let mut f = Feather::<BookSnapshot>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(BookSnapshot {
			ts_event: ts,
			ts_init: Some(ts),
			monotonic_seq: ts as u64,
			bid_prices: vec![100],
			bid_qtys: vec![10],
			ask_prices: vec![101],
			ask_qtys: vec![10],
		});
		f.flush(cat).unwrap();
	}

	fn write_delta(cat: &Catalog, ts: i64, mseq: u64) {
		let mut f = Feather::<BookDelta>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(BookDelta {
			ts_event: ts,
			ts_init: Some(ts),
			monotonic_seq: mseq,
			gapped: false,
			side: Side::Buy,
			price: 1.0,
			qty: 0.0,
		});
		f.flush(cat).unwrap();
	}

	#[test]
	fn anchor_within_window_seeds_book() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());

		let s = 1_000_000_000_i64;
		write_snapshot(&cat, 0);
		write_snapshot(&cat, 50 * s);
		write_snapshot(&cat, 120 * s);

		write_delta(&cat, 110 * s, 1);

		let (shape, deltas) = read_book(&cat, ExchangeName::Binance, test_symbol(), 100 * s, 200 * s).unwrap();
		let shape = shape.expect("anchor within window");
		assert_eq!(shape.ts_event.as_nanosecond() as i64, 50 * s);
		assert_eq!(shape.bids.get(&100), Some(&10));
		let deltas: Vec<BookDelta> = deltas.collect();
		assert_eq!(deltas.len(), 1);
		assert_eq!(deltas[0].ts_event, 110 * s);
	}

	#[test]
	fn anchor_out_of_window_skipped() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let s = 1_000_000_000_i64;
		write_snapshot(&cat, 0);
		write_snapshot(&cat, 120 * s);

		let (shape, deltas) = read_book(&cat, ExchangeName::Binance, test_symbol(), 60 * 60 * s, 2 * 60 * 60 * s).unwrap();
		assert!(shape.is_none());
		assert_eq!(deltas.count(), 0);
	}
}
