use std::{
	path::PathBuf,
	time::{Duration, Instant},
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use trading_data_core::{Asset, BookDelta, ExchangeName, PrecisionPriceQty, Symbol, TradeCols, Ts};

use crate::{
	catalog::{Catalog, CatalogError, LaneKey, Pending},
	row::{BookSnapshot, Mc, Oi, Row, Trade},
};

#[derive(Clone, Copy, Debug)]
pub struct RotationPolicy {
	pub max_bytes: Option<usize>,
	pub max_age: Option<Duration>,
	/// 1..=22. Archival lanes want the default 3; a live tee wants 1, where the encode is several
	/// times faster for a few percent of size — the trade a recording that must not stall makes.
	pub zstd_level: i32,
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
	oldest_ts: Option<Ts<T::Axis>>,
	newest_ts: Option<Ts<T::Axis>>,
	age_deadline: Option<Instant>,
	next_check_at_rows: Option<usize>,
}

impl Feather<Trade> {
	pub fn new(exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::Trades { exchange, symbol }, prec, policy)
	}

	/// Append a whole run. Raw columns go to disk as they came off the wire — the only conversion
	/// left anywhere on this path is the one at the CSV parse boundary.
	pub fn extend(&mut self, cols: TradeCols<'_>) {
		assert_eq!(self.meta, cols.prec, "trade run's precision differs from the lane's");
		self.builders.extend(cols);
		self.ran(cols.exec());
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

	pub(crate) fn extend(&mut self, levels: &[BookDelta]) {
		for &l in levels {
			self.push(l);
		}
	}
}

impl Feather<BookSnapshot> {
	pub(crate) fn new(exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, policy: RotationPolicy) -> Self {
		Self::init(LaneKey::BookSnapshots { exchange, symbol }, prec, policy)
	}
}

impl<T: Row> Feather<T> {
	fn init(key: LaneKey, meta: T::Meta, policy: RotationPolicy) -> Self {
		let rows_per_batch = policy.max_bytes.map(|m| (m / T::PER_ROW_MIN).max(64));
		Self {
			key,
			schema: T::schema(meta),
			meta,
			policy,
			// `None` is a lane bounded by age alone — `Oi` and `Mc`, hundreds of rows a day between
			// them — which is nothing to reserve for, and `with_capacity(0)` does not allocate.
			builders: T::builders(rows_per_batch.unwrap_or(0)),
			rows: 0,
			approx_bytes: 0,
			oldest_ts: None,
			newest_ts: None,
			age_deadline: None,
			next_check_at_rows: rows_per_batch,
		}
	}

	pub fn push(&mut self, row: T) {
		row.append(&mut self.builders, self.meta);
		self.rows += 1;
		self.approx_bytes += row.approx_bytes();
		// Interval bounds must be on the axis readers filter and prune on. Reception is always >=
		// execution, so bounding by reception prunes files that do hold matching rows.
		let ts = row.ts_axis();
		let was_empty = self.oldest_ts.is_none();
		self.oldest_ts = Some(self.oldest_ts.map_or(ts, |o| o.min(ts)));
		self.newest_ts = Some(self.newest_ts.map_or(ts, |n| n.max(ts)));
		if was_empty && let Some(age) = self.policy.max_age {
			self.age_deadline = Some(Instant::now() + age);
		}
	}

	/// [`Self::push`]'s bookkeeping for a whole run appended columnar. Only the trade lane has a
	/// columnar source, and it is fixed-width, so `PER_ROW_MIN` *is* the row size — the run's estimate
	/// here, not a floor.
	fn ran(&mut self, axis: &[Ts<T::Axis>]) {
		let Some((&lo, &hi)) = axis.iter().min().zip(axis.iter().max()) else {
			return;
		};
		self.rows += axis.len();
		self.approx_bytes += axis.len() * T::PER_ROW_MIN;
		let was_empty = self.oldest_ts.is_none();
		self.oldest_ts = Some(self.oldest_ts.map_or(lo, |o| o.min(lo)));
		self.newest_ts = Some(self.newest_ts.map_or(hi, |n| n.max(hi)));
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

	/// The batch, and the feather back to empty. Splitting this out of [`Self::flush`] is what lets
	/// a caller hand the encode to another thread instead of standing in it.
	pub(crate) fn take(&mut self) -> Option<Pending> {
		if self.rows == 0 {
			return None;
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
		Some(Pending {
			key: self.key,
			batch,
			ts_min: ts_min.as_nanos(),
			ts_max: ts_max.as_nanos(),
			zstd_level: self.policy.zstd_level,
		})
	}

	pub(crate) fn maybe_take(&mut self) -> Option<Pending> {
		if self.next_check_at_rows.is_none_or(|n| self.rows < n) && !self.age_deadline_passed() {
			return None;
		}
		self.should_flush().then(|| self.take()).flatten()
	}

	pub fn flush(&mut self, catalog: &Catalog) -> Result<Option<PathBuf>, CatalogError> {
		self.take().map(|p| catalog.write(p)).transpose()
	}

	pub fn maybe_flush(&mut self, catalog: &Catalog) -> Result<Option<PathBuf>, CatalogError> {
		self.maybe_take().map(|p| catalog.write(p)).transpose()
	}
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;
	use trading_data_core::{FrameKind, Instrument, Local, Precision, Side, Venue};

	use super::*;

	fn test_symbol() -> Symbol {
		Symbol::new("BTC-USDT".try_into().unwrap(), Instrument::Spot)
	}

	fn prec() -> PrecisionPriceQty {
		PrecisionPriceQty {
			price: Precision(2),
			qty: Precision(5),
		}
	}

	fn venue(ns: i64) -> Ts<Venue> {
		Ts::from_nanos(ns)
	}

	fn local(ns: i64) -> Ts<Local> {
		Ts::from_nanos(ns)
	}

	const FOREVER: RotationPolicy = RotationPolicy {
		max_bytes: None,
		max_age: None,
		zstd_level: 3,
	};

	fn round_trip_batch<T: Row>(f: &mut Feather<T>, cat: &Catalog) -> Vec<T> {
		let path = f.flush(cat).unwrap().expect("flush wrote a file");
		let (schema, batches) = crate::catalog::open(&path).unwrap();
		let batches: Vec<_> = batches.map(Result::unwrap).collect();
		assert_eq!(batches.len(), 1);
		T::decode(&batches[0], schema.as_ref())
	}

	#[test]
	fn flush_writes_parquet_on_bytes_policy() {
		let dir = tempdir().unwrap();
		let catalog = Catalog::new(dir.path());
		let mut feather = Feather::<BookDelta>::new(
			ExchangeName::Binance,
			test_symbol(),
			prec(),
			RotationPolicy {
				max_bytes: Some(1),
				max_age: None,
				zstd_level: 3,
			},
		);
		feather.push(BookDelta {
			prec: prec(),
			ts_venue_exec: venue(1),
			ts_local_recv: local(1),
			monotonic_seq: 1,
			kind: FrameKind::Update,
			side: Side::Buy,
			price: 1,
			qty: 1,
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
			ts_venue_exec: venue(1),
			ts_venue_send: Some(venue(2)),
			ts_local_recv: Some(local(3)),
			monotonic_seq: 3,
			side: Side::Sell,
			price: 48_351,
			qty: 42,
		};
		f.push(row);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![row]);
	}

	/// A null `ts_venue_send` must decode back to `None`, not to a neighbouring reading.
	#[test]
	fn absent_venue_send_stays_absent() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		f.push(Trade {
			ts_venue_exec: venue(1),
			ts_venue_send: None,
			ts_local_recv: None,
			monotonic_seq: 1,
			side: Side::Buy,
			price: 100,
			qty: 100_000,
		});
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded[0].ts_venue_send, None);
		assert_eq!(decoded[0].ts_local_recv, None);
	}

	#[test]
	fn deltas_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<BookDelta>::new(ExchangeName::Binance, test_symbol(), prec(), FOREVER);
		let row = BookDelta {
			prec: prec(),
			ts_venue_exec: venue(1),
			ts_local_recv: local(2),
			monotonic_seq: 9,
			kind: FrameKind::Correction,
			side: Side::Buy,
			price: 12_345,
			qty: 0,
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
			ts_venue_exec: venue(100),
			ts_local_recv: local(110),
			monotonic_seq: 1,
			bid_prices: vec![100, 99],
			bid_qtys: vec![10, 20],
			ask_prices: vec![101, 102],
			ask_qtys: vec![5, 7],
		});
		f.push(BookSnapshot {
			ts_venue_exec: venue(200),
			ts_local_recv: local(210),
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
		assert_eq!(decoded[1].ts_venue_exec, venue(200));
	}

	#[test]
	fn oi_round_trip() {
		let dir = tempdir().unwrap();
		let cat = Catalog::new(dir.path());
		let mut f = Feather::<Oi>::new(ExchangeName::Bybit, test_symbol(), FOREVER);
		let row = Oi {
			ts_venue_exec: venue(1),
			ts_venue_send: None,
			ts_local_recv: Some(local(2)),
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
			ts_local_exec: local(1),
			market_cap: 2.5e9,
			rank: Some(42),
		};
		let without_rank = Mc {
			ts_local_exec: local(3),
			market_cap: 2.6e9,
			rank: None,
		};
		f.push(with_rank);
		f.push(without_rank);
		let decoded = round_trip_batch(&mut f, &cat);
		assert_eq!(decoded, vec![with_rank, without_rank]);
	}
}
