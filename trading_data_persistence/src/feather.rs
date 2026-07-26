use std::{
	path::PathBuf,
	time::{Duration, Instant},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use trading_data_core::{Asset, ExchangeName, PrecisionPriceQty, Symbol};

use crate::{
	catalog::{Catalog, CatalogError, LaneKey},
	row::{BookDelta, BookSnapshot, Mc, Oi, Row, Trade, UnixNanos},
};

#[derive(Clone, Copy, Debug)]
pub struct RotationPolicy {
	pub max_bytes: Option<usize>,
	pub max_age: Option<Duration>,
}

/// Typed lane writer: a key/schema mismatch is unrepresentable, push is monomorphic.
pub struct Feather<T: Row> {
	key: LaneKey,
	schema: SchemaRef,
	meta: T::Meta,
	policy: RotationPolicy,
	builders: T::Builders,
	rows: usize,
	approx_bytes: usize,
	oldest_ts: Option<UnixNanos>,
	newest_ts: Option<UnixNanos>,
	age_deadline: Option<Instant>,
	next_check_at_rows: usize,
}

impl Feather<Trade> {
	pub fn new(exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::Trades { exchange, symbol }, prec, policy)
	}
}

impl Feather<Oi> {
	pub fn new(exchange: ExchangeName, symbol: Symbol, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::Oi { exchange, symbol }, (), policy)
	}
}

impl Feather<Mc> {
	pub fn new(asset: Asset, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::Mc { asset }, (), policy)
	}
}

impl Feather<BookDelta> {
	pub(crate) fn new(exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::BookDeltas { exchange, symbol }, prec, policy)
	}
}

impl Feather<BookSnapshot> {
	pub(crate) fn new(exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::BookSnapshots { exchange, symbol }, prec, policy)
	}
}

impl<T: Row> Feather<T> {
	fn init(key: LaneKey, meta: T::Meta, policy: RotationPolicy) -> Self {
		let next_check_at_rows = policy.max_bytes.map(|m| (m / T::PER_ROW_MIN).max(64)).unwrap_or(usize::MAX);
		Self {
			key,
			schema: T::schema(meta),
			meta,
			policy,
			builders: T::Builders::default(),
			rows: 0,
			approx_bytes: 0,
			oldest_ts: None,
			newest_ts: None,
			age_deadline: None,
			next_check_at_rows,
		}
	}

	pub fn push(&mut self, row: T) {
		row.append(&mut self.builders, self.meta);
		self.rows += 1;
		self.approx_bytes += row.approx_bytes();
		// interval bounds track arrival time when real, else fall back to event time.
		let ts = row.ts_init().unwrap_or_else(|| row.ts_event());
		let was_empty = self.oldest_ts.is_none();
		self.oldest_ts = Some(self.oldest_ts.map_or(ts, |o| o.min(ts)));
		self.newest_ts = Some(self.newest_ts.map_or(ts, |n| n.max(ts)));
		if was_empty && let Some(age) = self.policy.max_age {
			self.age_deadline = Some(Instant::now() + age);
		}
	}

	pub fn len(&self) -> usize {
		self.rows
	}

	pub fn is_empty(&self) -> bool {
		self.rows == 0
	}

	pub fn should_flush(&self) -> bool {
		if self.rows == 0 {
			return false;
		}
		if let Some(max) = self.policy.max_bytes
			&& self.approx_bytes >= max
		{
			return true;
		}
		self.age_deadline_passed()
	}

	fn age_deadline_passed(&self) -> bool {
		self.age_deadline.is_some_and(|t| Instant::now() >= t)
	}

	pub fn flush(&mut self, catalog: &Catalog) -> Result<Option<PathBuf>, CatalogError> {
		if self.rows == 0 {
			return Ok(None);
		}
		let ts_min = self.oldest_ts.expect("set on first push");
		let ts_max = self.newest_ts.expect("set on first push");
		let arrays = T::finish(&mut self.builders);
		let batch = RecordBatch::try_new(self.schema.clone(), arrays).expect("valid schema/array shape");
		self.rows = 0;
		self.approx_bytes = 0;
		self.oldest_ts = None;
		self.newest_ts = None;
		self.age_deadline = None;
		let path = catalog.write(&self.key, &batch, ts_min, ts_max)?;
		Ok(Some(path))
	}

	pub fn maybe_flush(&mut self, catalog: &Catalog) -> Result<Option<PathBuf>, CatalogError> {
		if self.rows < self.next_check_at_rows && !self.age_deadline_passed() {
			return Ok(None);
		}
		if self.should_flush() { self.flush(catalog) } else { Ok(None) }
	}
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use trading_data_core::{Instrument, Side};

	use super::*;

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn prec() -> PrecisionPriceQty {
		PrecisionPriceQty { price: 2, qty: 5 }
	}

	const FOREVER: RotationPolicy = RotationPolicy { max_bytes: None, max_age: None };

	fn round_trip_batch<T: Row>(f: &mut Feather<T>, cat: &Catalog) -> Vec<T> {
		let path = f.flush(cat).unwrap().expect("flush wrote a file");
		let (schema, batches) = cat.read(&path).unwrap();
		assert_eq!(batches.len(), 1);
		T::decode(&batches[0], schema.as_ref())
	}

	#[test]
	fn flush_writes_parquet_on_bytes_policy() {
		let dir = tempdir().unwrap();
		let catalog = Catalog::new(dir.path());
		let mut feather = Feather::<BookDelta>::new(ExchangeName::Binance, test_symbol(), prec(), RotationPolicy { max_bytes: Some(1), max_age: None });
		feather.push(BookDelta {
			ts_event: 1,
			ts_init: Some(1),
			monotonic_seq: 1,
			gapped: false,
			side: Side::Buy,
			price: 0.01,
			qty: 0.00001,
		});
		assert!(feather.should_flush());
		let path = feather.flush(&catalog).unwrap().unwrap();
		assert!(path.exists());
		assert_eq!(feather.len(), 0);
	}

	#[test]
	fn trades_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		let row = Trade {
			ts_event: 1,
			ts_init: Some(2),
			monotonic_seq: 3,
			trade_id: 4,
			side: Side::Sell,
			price: 483.51,
			qty: 0.00042,
		};
		f.push(row);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![row]);
	}

	#[test]
	fn deltas_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<BookDelta>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		let row = BookDelta {
			ts_event: 1,
			ts_init: Some(2),
			monotonic_seq: 9,
			gapped: true,
			side: Side::Buy,
			price: 123.45,
			qty: 0.0,
		};
		f.push(row);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![row]);
	}

	#[test]
	fn snapshots_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<BookSnapshot>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(BookSnapshot {
			ts_event: 100,
			ts_init: Some(110),
			monotonic_seq: 1,
			bid_prices: vec![100, 99],
			bid_qtys: vec![10, 20],
			ask_prices: vec![101, 102],
			ask_qtys: vec![5, 7],
		});
		f.push(BookSnapshot {
			ts_event: 200,
			ts_init: Some(210),
			monotonic_seq: 2,
			bid_prices: vec![],
			bid_qtys: vec![],
			ask_prices: vec![103],
			ask_qtys: vec![1],
		});
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded.len(), 2);
		assert_eq!(decoded[0].bid_prices, vec![100, 99]);
		assert_eq!(decoded[1].ask_prices, vec![103]);
		assert_eq!(decoded[1].ts_event, 200);
	}

	#[test]
	fn oi_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Oi>::new(ExchangeName::Bybit, test_symbol(), FOREVER);
		let row = Oi {
			ts_event: 1,
			ts_init: Some(2),
			oi: 123_456.75,
		};
		f.push(row);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![row]);
	}

	#[test]
	fn mc_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Mc>::new(Asset::new("TAO"), FOREVER);
		let with_rank = Mc {
			ts_event: 1,
			ts_init: Some(2),
			market_cap: 2.5e9,
			rank: Some(42),
		};
		let without_rank = Mc {
			ts_event: 3,
			ts_init: Some(4),
			market_cap: 2.6e9,
			rank: None,
		};
		f.push(with_rank);
		f.push(without_rank);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![with_rank, without_rank]);
	}

	#[test]
	#[should_panic(expected = "does not round-trip")]
	fn precision_violation_panics() {
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(Trade {
			ts_event: 1,
			ts_init: Some(1),
			monotonic_seq: 1,
			trade_id: 1,
			side: Side::Buy,
			price: 0.001, // 3 decimals under price precision 2
			qty: 1.0,
		});
	}
}
