use std::{
	fs,
	path::{Path, PathBuf},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use parquet::{
	arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
	basic::Compression,
	file::properties::WriterProperties,
};
use thiserror::Error;
use v_utils::trades::{Asset, ExchangeName, Symbol};

use crate::row::UnixNanos;

#[derive(Clone, Debug)]
pub struct Catalog {
	root: PathBuf,
}
impl Catalog {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into() }
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	pub(crate) fn lane_dir(&self, key: &LaneKey) -> PathBuf {
		let data = self.root.join("data");
		let sym = |exchange: &ExchangeName, symbol: &Symbol| {
			PathBuf::from(symbol.pair.base().to_string())
				.join(symbol.pair.quote().to_string())
				.join(format!("{exchange}{}", symbol.instrument))
		};
		match key {
			LaneKey::Trades { exchange, symbol } => data.join("trades").join(sym(exchange, symbol)),
			LaneKey::BookSnapshots { exchange, symbol } => data.join("book").join(sym(exchange, symbol)).join("snapshots"),
			LaneKey::BookDeltas { exchange, symbol } => data.join("book").join(sym(exchange, symbol)).join("deltas"),
			LaneKey::Oi { exchange, symbol } => data.join("oi").join(sym(exchange, symbol)),
			LaneKey::Mc { asset } => data.join("mc").join(asset.to_string()),
		}
	}

	pub(crate) fn write(&self, key: &LaneKey, batch: &RecordBatch, ts_min: UnixNanos, ts_max: UnixNanos) -> Result<PathBuf, CatalogError> {
		assert!(ts_min <= ts_max, "ts_min must be <= ts_max");

		let dir = self.lane_dir(key);
		fs::create_dir_all(&dir)?;

		let existing = self.list(key)?;
		for e in &existing {
			if intervals_overlap((e.ts_min, e.ts_max), (ts_min, ts_max)) {
				return Err(CatalogError::OverlappingInterval {
					existing: (e.ts_min, e.ts_max),
					new: (ts_min, ts_max),
				});
			}
		}

		let path = dir.join(format!("{ts_min}_{ts_max}.parquet"));
		let file = fs::File::create(&path)?;
		let props = WriterProperties::builder().set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default())).build();
		let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
		writer.write(batch)?;
		writer.close()?;
		Ok(path)
	}

	pub(crate) fn list(&self, key: &LaneKey) -> Result<Vec<FileEntry>, CatalogError> {
		let dir = self.lane_dir(key);
		if !dir.exists() {
			return Ok(Vec::new());
		}
		let mut entries = Vec::new();
		for ent in fs::read_dir(&dir)? {
			let ent = ent?;
			let path = ent.path();
			if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
				continue;
			}
			let stem = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| CatalogError::BadFilename(path.display().to_string()))?;
			let (lo, hi) = stem.split_once('_').ok_or_else(|| CatalogError::BadFilename(path.display().to_string()))?;
			let ts_min: i64 = lo.parse().map_err(|_| CatalogError::BadFilename(path.display().to_string()))?;
			let ts_max: i64 = hi.parse().map_err(|_| CatalogError::BadFilename(path.display().to_string()))?;
			entries.push(FileEntry { path, ts_min, ts_max });
		}
		entries.sort_by_key(|e| e.ts_min);
		Ok(entries)
	}

	pub(crate) fn list_range(&self, key: &LaneKey, start: UnixNanos, end: UnixNanos) -> Result<Vec<FileEntry>, CatalogError> {
		Ok(self.list(key)?.into_iter().filter(|e| e.ts_max >= start && e.ts_min <= end).collect())
	}

	/// Returns the file-level schema alongside the batches: per-batch schemas drop the
	/// key-value metadata (precisions) that decode needs.
	pub(crate) fn read(&self, path: &Path) -> Result<(SchemaRef, Vec<RecordBatch>), CatalogError> {
		let file = fs::File::open(path)?;
		let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
		let schema = builder.schema().clone();
		let reader = builder.build()?;
		let mut out = Vec::new();
		for batch in reader {
			out.push(batch?);
		}
		Ok((schema, out))
	}
}

#[derive(Debug, Error)]
pub enum CatalogError {
	#[error("io: {0}")]
	Io(#[from] std::io::Error),
	#[error("arrow: {0}")]
	Arrow(#[from] arrow::error::ArrowError),
	#[error("parquet: {0}")]
	Parquet(#[from] parquet::errors::ParquetError),
	#[error("write would create overlapping interval: existing {existing:?}, new {new:?}")]
	OverlappingInterval { existing: (UnixNanos, UnixNanos), new: (UnixNanos, UnixNanos) },
	#[error("malformed filename: {0}")]
	BadFilename(String),
}

/// Dir routing only; the row type carries everything else.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum LaneKey {
	Trades { exchange: ExchangeName, symbol: Symbol },
	BookSnapshots { exchange: ExchangeName, symbol: Symbol },
	BookDeltas { exchange: ExchangeName, symbol: Symbol },
	Oi { exchange: ExchangeName, symbol: Symbol },
	Mc { asset: Asset },
}

#[derive(Clone, Debug)]
pub(crate) struct FileEntry {
	pub path: PathBuf,
	pub ts_min: UnixNanos,
	pub ts_max: UnixNanos,
}

fn intervals_overlap(a: (UnixNanos, UnixNanos), b: (UnixNanos, UnixNanos)) -> bool {
	a.0 <= b.1 && b.0 <= a.1
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use v_utils::trades::{Instrument, PrecisionPriceQty, Side};

	use super::*;
	use crate::{
		feather::Feather,
		row::{Row as _, Trade},
	};

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn key() -> LaneKey {
		LaneKey::Trades {
			exchange: ExchangeName::Binance,
			symbol: test_symbol(),
		}
	}

	fn write_one(cat: &Catalog, ts: i64) {
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), PrecisionPriceQty { price: 2, qty: 5 }, Trade::POLICY);
		f.push(Trade {
			ts_event: ts,
			ts_init: Some(ts),
			monotonic_seq: 1,
			trade_id: 1,
			side: Side::Buy,
			price: 1.0,
			qty: 1.0,
		});
		f.flush(cat).unwrap();
	}

	#[test]
	fn write_list_read_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		write_one(&cat, 110);

		let listed = cat.list(&key()).unwrap();
		assert_eq!(listed.len(), 1);
		assert_eq!(listed[0].ts_min, 110);
		assert_eq!(listed[0].ts_max, 110);

		let (schema, read) = cat.read(&listed[0].path).unwrap();
		assert!(schema.metadata().contains_key("price_precision"));
		assert_eq!(read.len(), 1);
		assert_eq!(read[0].num_rows(), 1);
	}

	#[test]
	fn refuses_overlapping_write() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		write_one(&cat, 100);
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), PrecisionPriceQty { price: 2, qty: 5 }, Trade::POLICY);
		f.push(Trade {
			ts_event: 100,
			ts_init: Some(100),
			monotonic_seq: 2,
			trade_id: 2,
			side: Side::Sell,
			price: 1.0,
			qty: 1.0,
		});
		let err = f.flush(&cat).unwrap_err();
		assert!(matches!(err, CatalogError::OverlappingInterval { .. }));
	}

	#[test]
	fn list_range_prunes() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		write_one(&cat, 100);
		write_one(&cat, 300);
		write_one(&cat, 500);

		let pruned = cat.list_range(&key(), 250, 450).unwrap();
		assert_eq!(pruned.len(), 1);
		assert_eq!(pruned[0].ts_min, 300);
	}
}
