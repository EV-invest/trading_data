use std::{cmp::Ordering, collections::BinaryHeap, time::Duration};

use arrow::array::RecordBatch;
use v_utils::trades::{ExchangeName, Symbol};

use crate::{
	catalog::{Catalog, CatalogError, FileEntry, Lane, LaneKey},
	schema::{BookDelta, BookSnapshot, Close, Data, Trade, UnixNanos, decode_closes, decode_deltas, decode_snapshots, decode_trades},
};

pub trait Row {
	fn ts_event(&self) -> UnixNanos;
	fn into_data(self) -> Data;
}
impl Row for BookSnapshot {
	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn into_data(self) -> Data {
		Data::Snapshot(self)
	}
}
impl Row for BookDelta {
	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn into_data(self) -> Data {
		Data::Delta(self)
	}
}
impl Row for Trade {
	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn into_data(self) -> Data {
		Data::Trade(self)
	}
}
impl Row for Close {
	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn into_data(self) -> Data {
		Data::Close(self)
	}
}

/// Streams one lane's rows in `[start, end]`, one parquet file at a time. No whole-lane
/// materialization; a mid-stream read failure of a catalog-owned file is unrecoverable and panics.
pub struct LaneReader<T> {
	catalog: Catalog,
	files: std::vec::IntoIter<FileEntry>,
	batches: std::vec::IntoIter<RecordBatch>,
	rows: std::vec::IntoIter<T>,
	decode: fn(&RecordBatch) -> Vec<T>,
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
				self.rows = (self.decode)(&batch).into_iter();
				continue;
			}
			let file = self.files.next()?;
			let batches = self.catalog.read(&file.path).expect("catalog file unreadable during replay");
			self.batches = batches.into_iter();
		}
	}
}

pub fn read_snapshots(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<BookSnapshot>, CatalogError> {
	lane_reader(catalog, LaneKey::book(Lane::Snapshots, exchange, symbol), start, end, decode_snapshots)
}
pub fn read_deltas(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<BookDelta>, CatalogError> {
	lane_reader(catalog, LaneKey::book(Lane::Deltas, exchange, symbol), start, end, decode_deltas)
}
pub fn read_trades(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<Trade>, CatalogError> {
	lane_reader(catalog, LaneKey::book(Lane::Trades, exchange, symbol), start, end, decode_trades)
}
pub fn read_closes(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos) -> Result<LaneReader<Close>, CatalogError> {
	lane_reader(catalog, LaneKey::book(Lane::Closes, exchange, symbol), start, end, decode_closes)
}
#[derive(Clone, Debug)]
pub struct ReplayConfig {
	pub start: UnixNanos,
	pub end: UnixNanos,
	pub max_anchor_age: Duration,
}
impl ReplayConfig {
	pub fn new(start: UnixNanos, end: UnixNanos) -> Self {
		Self {
			start,
			end,
			max_anchor_age: Duration::from_secs(15 * 60),
		}
	}
}

/// Lazy k-way merge of the typed lane readers into one time-ordered `Data` stream. The anchor
/// snapshot, when present, is emitted first to seed the book.
pub struct Replay {
	anchor: Option<Data>,
	sources: Vec<Box<dyn Iterator<Item = Data>>>,
	heap: BinaryHeap<HeapEntry>,
}
/// One symbol only. Cross-symbol ordering is the backtester's job.
pub fn replay(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, config: &ReplayConfig) -> Result<Replay, CatalogError> {
	let anchor = pick_anchor(catalog, exchange, symbol, config)?;
	let snap_start = anchor.as_ref().map(|a| (a.ts_event + 1).max(config.start)).unwrap_or(config.start);

	let mut sources: Vec<Box<dyn Iterator<Item = Data>>> = vec![
		Box::new(read_snapshots(catalog, exchange, symbol, snap_start, config.end)?.map(Row::into_data)),
		Box::new(read_deltas(catalog, exchange, symbol, config.start, config.end)?.map(Row::into_data)),
		Box::new(read_trades(catalog, exchange, symbol, config.start, config.end)?.map(Row::into_data)),
		Box::new(read_closes(catalog, exchange, symbol, config.start, config.end)?.map(Row::into_data)),
	];

	let mut heap = BinaryHeap::new();
	for (src, it) in sources.iter_mut().enumerate() {
		if let Some(data) = it.next() {
			heap.push(HeapEntry {
				key: (data.ts_event(), data.monotonic_seq()),
				src,
				data,
			});
		}
	}

	Ok(Replay {
		anchor: anchor.map(Data::Snapshot),
		sources,
		heap,
	})
}
fn lane_reader<T>(catalog: &Catalog, key: LaneKey, start: UnixNanos, end: UnixNanos, decode: fn(&RecordBatch) -> Vec<T>) -> Result<LaneReader<T>, CatalogError> {
	let files = catalog.list_range(&key, start, end)?;
	Ok(LaneReader {
		catalog: catalog.clone(),
		files: files.into_iter(),
		batches: Vec::new().into_iter(),
		rows: Vec::new().into_iter(),
		decode,
		start,
		end,
	})
}

struct HeapEntry {
	key: (UnixNanos, u64),
	src: usize,
	data: Data,
}
impl PartialEq for HeapEntry {
	fn eq(&self, other: &Self) -> bool {
		(self.key, self.src) == (other.key, other.src)
	}
}
impl Eq for HeapEntry {}
impl Ord for HeapEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		// Reversed: BinaryHeap is a max-heap, we want the smallest (ts_event, monotonic_seq) first.
		(other.key, other.src).cmp(&(self.key, self.src))
	}
}
impl PartialOrd for HeapEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Iterator for Replay {
	type Item = Data;

	fn next(&mut self) -> Option<Data> {
		if let Some(a) = self.anchor.take() {
			return Some(a);
		}
		let entry = self.heap.pop()?;
		if let Some(data) = self.sources[entry.src].next() {
			self.heap.push(HeapEntry {
				key: (data.ts_event(), data.monotonic_seq()),
				src: entry.src,
				data,
			});
		}
		Some(entry.data)
	}
}

fn pick_anchor(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, config: &ReplayConfig) -> Result<Option<BookSnapshot>, CatalogError> {
	let key = LaneKey::book(Lane::Snapshots, exchange, symbol);
	let files = catalog.list(&key)?;
	let max_age_ns = config.max_anchor_age.as_nanos() as i64;

	let candidate = files.iter().rev().find(|f| f.ts_min <= config.start);

	if let Some(file) = candidate {
		let mut newest: Option<BookSnapshot> = None;
		for batch in catalog.read(&file.path)? {
			for row in decode_snapshots(&batch) {
				if row.ts_event > config.start {
					continue;
				}
				let take = match &newest {
					None => true,
					Some(cur) => row.ts_event > cur.ts_event,
				};
				if take {
					newest = Some(row);
				}
			}
		}
		if let Some(row) = newest
			&& config.start - row.ts_event <= max_age_ns
		{
			return Ok(Some(row));
		}
	}

	let first_in_range = files.iter().find(|f| f.ts_max >= config.start);
	if let Some(f) = first_in_range {
		tracing::warn!(
			file = %f.path.display(),
			start = config.start,
			max_anchor_age_secs = config.max_anchor_age.as_secs(),
			"no recent anchor; replay will start from first snapshot >= start without book seed",
		);
	} else {
		tracing::warn!(start = config.start, "no snapshot files at or after start; replay will produce no anchor");
	}
	Ok(None)
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use v_utils::trades::Instrument;

	use super::*;
	use crate::{
		feather::{Feather, RotationPolicy},
		schema::FileMetadata,
	};

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn meta() -> FileMetadata {
		FileMetadata {
			exchange: "binance".into(),
			pair: "BTC-USDT".into(),
			price_precision: 2,
			qty_precision: 5,
		}
	}

	fn forever() -> RotationPolicy {
		RotationPolicy { max_bytes: None, max_age: None }
	}

	fn write_snapshot(cat: &Catalog, key: LaneKey, ts: i64) {
		let mut f = Feather::new_snapshots(key, meta(), forever());
		f.push_snapshot(ts, ts, ts as u64, [100].into_iter(), [10].into_iter(), [101].into_iter(), [10].into_iter());
		f.flush(cat).unwrap();
	}

	fn write_delta(cat: &Catalog, key: LaneKey, row: BookDelta) {
		let mut f = Feather::new_deltas(key, meta(), forever());
		f.push_delta(row);
		f.flush(cat).unwrap();
	}

	fn delta(ts: i64, mseq: u64) -> BookDelta {
		BookDelta {
			ts_event: ts,
			ts_init: ts,
			monotonic_seq: mseq,
			gapped: false,
			side: 0,
			price_raw: 0,
			qty_raw: 0,
		}
	}

	#[test]
	fn anchor_within_window_seeds_book() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let snaps_key = || LaneKey::book(Lane::Snapshots, ExchangeName::Binance, test_symbol());
		let deltas_key = || LaneKey::book(Lane::Deltas, ExchangeName::Binance, test_symbol());

		let s = 1_000_000_000_i64;
		write_snapshot(&cat, snaps_key(), 0);
		write_snapshot(&cat, snaps_key(), 50 * s);
		write_snapshot(&cat, snaps_key(), 120 * s);

		write_delta(&cat, deltas_key(), delta(110 * s, 1));

		let cfg = ReplayConfig::new(100 * s, 200 * s);
		let out: Vec<Data> = replay(&cat, ExchangeName::Binance, test_symbol(), &cfg).unwrap().collect();
		assert!(matches!(&out[0], Data::Snapshot(s) if s.ts_event == 50 * 1_000_000_000));
		assert!(matches!(&out[1], Data::Delta(d) if d.ts_event == 110 * 1_000_000_000));
		assert!(matches!(&out[2], Data::Snapshot(s) if s.ts_event == 120 * 1_000_000_000));
	}

	#[test]
	fn anchor_out_of_window_skipped() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let snaps_key = || LaneKey::book(Lane::Snapshots, ExchangeName::Binance, test_symbol());
		let s = 1_000_000_000_i64;
		write_snapshot(&cat, snaps_key(), 0);
		write_snapshot(&cat, snaps_key(), 120 * s);

		let cfg = ReplayConfig::new(60 * 60 * s, 2 * 60 * 60 * s);
		let out: Vec<Data> = replay(&cat, ExchangeName::Binance, test_symbol(), &cfg).unwrap().collect();
		assert!(out.is_empty(), "got {} rows", out.len());
	}
}
