use std::time::Duration;

use arrow::{
	array::RecordBatch,
	datatypes::{Schema, SchemaRef},
};
use trading_data_core::{Accumulator, Asset, BookShape, Exact, ExchangeName, Local, Span, Symbol, Ts, Venue};

use crate::{
	catalog::{Catalog, CatalogError, FileEntry, LaneKey},
	row::{BookDelta, BookSnapshot, Mc, Oi, Row, SCHEMA_VERSION, Trade, prec_from_schema, sealed::Sealed},
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
	start: Ts<T::Axis>,
	end: Ts<T::Axis>,
}

impl<T: Row> Iterator for LaneReader<T> {
	type Item = T;

	fn next(&mut self) -> Option<T> {
		loop {
			for r in self.rows.by_ref() {
				if r.ts_axis() >= self.start && r.ts_axis() <= self.end {
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

fn lane_reader<T: Row>(catalog: &Catalog, key: LaneKey, start: Ts<T::Axis>, end: Ts<T::Axis>) -> Result<LaneReader<T>, CatalogError> {
	// The catalog is lane-agnostic, so file bounds are raw nanos there; the axis is re-attached
	// here, where the row type names it.
	let files = catalog.list_range(&key, start.as_nanos(), end.as_nanos())?;
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

pub fn read_trades(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>) -> Result<LaneReader<Trade>, CatalogError> {
	lane_reader(catalog, LaneKey::Trades { exchange, symbol }, start, end)
}

pub fn read_oi(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>) -> Result<LaneReader<Oi>, CatalogError> {
	lane_reader(catalog, LaneKey::Oi { exchange, symbol }, start, end)
}

/// Bounds are `Ts<Local>`: market cap has no venue event time, so this lane's axis is our clock.
pub fn read_mc(catalog: &Catalog, asset: Asset, start: Ts<Local>, end: Ts<Local>) -> Result<LaneReader<Mc>, CatalogError> {
	lane_reader(catalog, LaneKey::Mc { asset }, start, end)
}

/// One symbol only. Seeds the book from the freshest snapshot at or before `start` (within
/// [`MAX_ANCHOR_AGE`]), then streams the post-`start` deltas. Mid-range snapshots stay internal.
/// Crate-private: the `sync` weaver is the public path; it additionally weaves in-range snapshots.
#[cfg(test)]
pub(crate) fn read_book(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>) -> Result<(Option<BookShape>, LaneReader<BookDelta>), CatalogError> {
	let anchor = pick_anchor(catalog, exchange, symbol, start)?;
	let deltas = lane_reader(catalog, LaneKey::BookDeltas { exchange, symbol }, start, end)?;
	Ok((anchor, deltas))
}

pub(crate) fn read_book_deltas(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>) -> Result<LaneReader<BookDelta>, CatalogError> {
	lane_reader(catalog, LaneKey::BookDeltas { exchange, symbol }, start, end)
}

pub(crate) fn read_book_snapshots(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>) -> Result<LaneReader<BookSnapshot>, CatalogError> {
	lane_reader(catalog, LaneKey::BookSnapshots { exchange, symbol }, start, end)
}

/// A stored snapshot is a resync point, so both epochs are degenerate: `first == last`. `local` is
/// absent for historic ingest — we were not there, and copying the venue reading across would be
/// exactly the aliasing this schema exists to remove.
pub(crate) fn snapshot_shape(row: &BookSnapshot, prec: trading_data_core::PrecisionPriceQty) -> BookShape {
	BookShape {
		ts: Accumulator {
			venue: Span::at(row.ts_venue_exec),
			local: row.ts_local_recv.map(Span::at),
		},
		venue_send: row.ts_venue_send,
		prec,
		bids: row.bid_prices.iter().copied().zip(row.bid_qtys.iter().copied()).collect(),
		asks: row.ask_prices.iter().copied().zip(row.ask_qtys.iter().copied()).collect(),
	}
}

/// Price/qty precision stored in a book lane's files (deltas preferred, else snapshots). `None`
/// when neither lane has any file yet.
pub(crate) fn book_prec(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol) -> Result<Option<trading_data_core::PrecisionPriceQty>, CatalogError> {
	for key in [LaneKey::BookDeltas { exchange, symbol }, LaneKey::BookSnapshots { exchange, symbol }] {
		if let Some(file) = catalog.list(&key)?.first() {
			let (schema, _) = catalog.read(&file.path)?;
			assert_schema_version(schema.as_ref());
			return Ok(Some(prec_from_schema(schema.as_ref())));
		}
	}
	Ok(None)
}

pub(crate) fn pick_anchor(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>) -> Result<Option<BookShape>, CatalogError> {
	let key = LaneKey::BookSnapshots { exchange, symbol };
	let files = catalog.list(&key)?;
	let max_age = Exact::from(MAX_ANCHOR_AGE);

	let candidate = files.iter().rev().find(|f| f.ts_min <= start.as_nanos());

	if let Some(file) = candidate {
		let mut newest: Option<BookSnapshot> = None;
		let (schema, batches) = catalog.read(&file.path)?;
		assert_schema_version(schema.as_ref());
		let prec = prec_from_schema(schema.as_ref());
		for batch in batches {
			for row in BookSnapshot::decode(&batch, schema.as_ref()) {
				if row.ts_venue_exec > start {
					continue;
				}
				if newest.as_ref().is_none_or(|cur| row.ts_venue_exec > cur.ts_venue_exec) {
					newest = Some(row);
				}
			}
		}
		// Same-actor difference: both readings are on the venue clock, so no skew term.
		if let Some(row) = newest
			&& start - row.ts_venue_exec <= max_age
		{
			return Ok(Some(snapshot_shape(&row, prec)));
		}
	}

	let first_in_range = files.iter().find(|f| f.ts_max >= start.as_nanos());
	if let Some(f) = first_in_range {
		tracing::warn!(
			file = %f.path.display(),
			start = start.as_nanos(),
			max_anchor_age_secs = MAX_ANCHOR_AGE.as_secs(),
			"no recent anchor; book starts unseeded",
		);
	} else {
		tracing::warn!(start = start.as_nanos(), "no snapshot files at or after start; book starts unseeded");
	}
	Ok(None)
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use trading_data_core::{Instrument, PrecisionPriceQty, Side};

	use super::*;
	use crate::feather::{Feather, RotationPolicy};

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn prec() -> PrecisionPriceQty {
		PrecisionPriceQty { price: 2, qty: 5 }
	}

	const FOREVER: RotationPolicy = RotationPolicy { max_bytes: None, max_age: None };

	fn venue(ns: i64) -> Ts<Venue> {
		Ts::from_nanos(ns)
	}

	fn write_snapshot(cat: &Catalog, ts: i64) {
		let mut f = Feather::<BookSnapshot>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(BookSnapshot {
			ts_venue_exec: venue(ts),
			ts_venue_send: None,
			ts_local_recv: Some(Ts::from_nanos(ts)),
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
			ts_venue_exec: venue(ts),
			ts_venue_send: None,
			ts_local_recv: Some(Ts::from_nanos(ts)),
			monotonic_seq: mseq,
			gapped: false,
			side: Side::Buy,
			price: 1.0,
			qty: 0.0,
		});
		f.flush(cat).unwrap();
	}

	/// File interval bounds must live on the axis [`LaneReader`] filters on. When they tracked
	/// reception instead, a window below the reception window pruned files that do hold matching rows.
	#[test]
	fn arrival_lag_does_not_prune_matching_rows() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(Trade {
			ts_venue_exec: venue(900),
			ts_venue_send: None,
			ts_local_recv: Some(Ts::from_nanos(2000)),
			monotonic_seq: 1,
			trade_id: 1,
			side: Side::Buy,
			price: 1.0,
			qty: 1.0,
		});
		f.flush(&cat).unwrap();

		let got: Vec<Trade> = read_trades(&cat, ExchangeName::Binance, test_symbol(), venue(900), venue(950)).unwrap().collect();
		assert_eq!(got.len(), 1, "file pruned on the reception axis while rows filter on the execution axis");
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

		let (shape, deltas) = read_book(&cat, ExchangeName::Binance, test_symbol(), venue(100 * s), venue(200 * s)).unwrap();
		let shape = shape.expect("anchor within window");
		assert_eq!(shape.ts.venue.last, venue(50 * s));
		assert_eq!(shape.bids.get(&100), Some(&10));
		let deltas: Vec<BookDelta> = deltas.collect();
		assert_eq!(deltas.len(), 1);
		assert_eq!(deltas[0].ts_venue_exec, venue(110 * s));
	}

	#[test]
	fn anchor_out_of_window_skipped() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let s = 1_000_000_000_i64;
		write_snapshot(&cat, 0);
		write_snapshot(&cat, 120 * s);

		let (shape, deltas) = read_book(&cat, ExchangeName::Binance, test_symbol(), venue(60 * 60 * s), venue(2 * 60 * 60 * s)).unwrap();
		assert!(shape.is_none());
		assert_eq!(deltas.count(), 0);
	}
}
