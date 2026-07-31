use std::sync::Arc;

use arrow::{
	array::{Array, ArrayRef, Float64Array, Float64Builder, Int32Array, Int64Array, ListArray, RecordBatch, UInt8Array, UInt32Array, UInt64Array},
	datatypes::{DataType, Field, Schema, SchemaRef},
};
use trading_data_core::{FrameKind, Local, Precision, PrecisionPriceQty, Side, Ts, Venue};
use trading_data_dag::{Bump, Cell, Flat, Glance, Stamped, slice_nudge};

use crate::feather::RotationPolicy;

pub const SCHEMA_VERSION: &str = "7";
pub type UnixNanos = i64;

/// A lane's row type. Sealed: the set of lanes is this crate's contract with the disk.
pub trait Row: sealed::Sealed {
	/// Rotation default for this lane.
	const POLICY: RotationPolicy;
	/// The actor whose clock this lane's primary axis belongs to. Fixed per lane at compile time,
	/// which is what lets query bounds be typed without a per-row discriminant.
	type Axis;
	/// The reading every row of this lane has, and the one files are bounded and queried on.
	fn ts_axis(&self) -> Ts<Self::Axis>;
}

/// One stored trade, raw as the venue sent it: `price`/`qty` are meaningless without the lane's
/// precision. It is the *disk* row, never a graph view — nodes read [`TradeCols`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Trade {
	pub ts_venue_exec: Ts<Venue>,
	/// `Some` once the adapter reports an envelope time distinct from the execution time.
	pub ts_venue_send: Option<Ts<Venue>>,
	/// `Some` ⇔ we were there when it arrived (live-recorded); historic ingest writes `None`.
	pub ts_local_recv: Option<Ts<Local>>,
	pub monotonic_seq: u64,
	pub side: Side,
	pub price: i32,
	pub qty: u32,
}

/// One stored level of our own recollection — see `ShadowBook`: the persisted delta lane is
/// gapless and self-consistent, so `kind` (not the venue's `gapped` flag) is what a consumer reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BookDelta {
	pub ts_venue_exec: Ts<Venue>,
	/// Book lanes are only ever written by a live recording, and the adapter that took the frame
	/// off the wire always knows its own reception time — so unlike the trade lane there is no
	/// historic-ingest path that could leave this absent.
	pub ts_local_recv: Ts<Local>,
	pub monotonic_seq: u64,
	pub kind: FrameKind,
	/// Buy = bid, Sell = ask.
	pub side: Side,
	pub price: i32,
	/// `0` means delete this level.
	pub qty: u32,
}

/// Open interest, base units.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Oi {
	pub ts_venue_exec: Ts<Venue>,
	pub ts_venue_send: Option<Ts<Venue>>,
	/// `Some` ⇔ we were there when it arrived (live-recorded); historic ingest writes `None`.
	pub ts_local_recv: Option<Ts<Local>>,
	pub oi: f64,
}

/// Market cap. The source reports no event time, so the only reading is **ours** — this lane's
/// axis is `Local`, and venue-named columns would be a lie about whose clock produced it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Mc {
	pub ts_local_exec: Ts<Local>,
	pub market_cap: f64,
	/// Honest-None when the source lacks it.
	pub rank: Option<u32>,
}

/// Raw-keyed book snapshot; internal to the Book persistence model.
#[derive(Clone, Debug)]
pub(crate) struct BookSnapshot {
	pub ts_venue_exec: Ts<Venue>,
	/// Always present: see [`BookDelta::ts_local_recv`].
	pub ts_local_recv: Ts<Local>,
	pub monotonic_seq: u64,
	pub bid_prices: Vec<i32>,
	pub bid_qtys: Vec<u32>,
	pub ask_prices: Vec<i32>,
	pub ask_qtys: Vec<u32>,
}

impl Row for Trade {
	type Axis = Venue;

	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(50 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(24 * 3600)),
	};

	fn ts_axis(&self) -> Ts<Venue> {
		self.ts_venue_exec
	}
}
impl Row for BookDelta {
	type Axis = Venue;

	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(256 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(3600)),
	};

	fn ts_axis(&self) -> Ts<Venue> {
		self.ts_venue_exec
	}
}
impl Row for BookSnapshot {
	type Axis = Venue;

	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(64 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(6 * 3600)),
	};

	fn ts_axis(&self) -> Ts<Venue> {
		self.ts_venue_exec
	}
}
impl Row for Oi {
	type Axis = Venue;

	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: None,
		max_age: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
	};

	fn ts_axis(&self) -> Ts<Venue> {
		self.ts_venue_exec
	}
}
impl Row for Mc {
	type Axis = Local;

	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: None,
		max_age: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
	};

	fn ts_axis(&self) -> Ts<Local> {
		self.ts_local_exec
	}
}

pub(crate) mod sealed {
	use super::*;

	pub trait Sealed: Sized {
		type Builders: Default;
		/// Precision context for scaled-int lanes; `()` for f64-native lanes.
		type Meta: Copy;
		const PER_ROW_MIN: usize;
		fn schema(meta: Self::Meta) -> SchemaRef;
		fn append(&self, b: &mut Self::Builders, meta: Self::Meta);
		fn finish(b: &mut Self::Builders) -> Vec<ArrayRef>;
		fn decode(batch: &RecordBatch, file_schema: &Schema) -> Vec<Self>;
		/// Metadata that must agree across every file of a read range; `None` when the lane has none.
		fn file_sig(schema: &Schema) -> Option<String>;
		fn approx_bytes(&self) -> usize;
	}
}
use sealed::Sealed;

// storage encoding: 0 = Buy/bid, 1 = Sell/ask
fn side_u8(s: Side) -> u8 {
	match s {
		Side::Buy => 0,
		Side::Sell => 1,
	}
}

fn side_from(v: u8) -> Side {
	match v {
		0 => Side::Buy,
		1 => Side::Sell,
		x => panic!("invalid side byte {x} in stored lane"),
	}
}

// storage encoding, exactly parallel to `side_u8`: 0 = market activity, 1 = our reconciliation
fn kind_u8(k: FrameKind) -> u8 {
	match k {
		FrameKind::Update => 0,
		FrameKind::Correction => 1,
	}
}

fn kind_from(v: u8) -> FrameKind {
	match v {
		0 => FrameKind::Update,
		1 => FrameKind::Correction,
		x => panic!("invalid frame-kind byte {x} in stored lane"),
	}
}

pub(crate) fn prec_from_schema(schema: &Schema) -> PrecisionPriceQty {
	let get = |k: &str| {
		schema
			.metadata()
			.get(k)
			.unwrap_or_else(|| panic!("file metadata missing {k}"))
			.parse::<Precision>()
			.unwrap_or_else(|e| panic!("file metadata {k} not a precision: {e}"))
	};
	PrecisionPriceQty {
		price: get("price_precision"),
		qty: get("qty_precision"),
	}
}

fn schema_with(fields: Vec<Field>, pairs: &[(&str, String)]) -> SchemaRef {
	let mut meta: std::collections::HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
	meta.insert("schema_version".into(), SCHEMA_VERSION.into());
	Arc::new(Schema::new(fields).with_metadata(meta))
}

fn prec_pairs(prec: PrecisionPriceQty) -> [(&'static str, String); 2] {
	[("price_precision", prec.price.to_string()), ("qty_precision", prec.qty.to_string())]
}

fn prec_sig(schema: &Schema) -> Option<String> {
	let p = prec_from_schema(schema);
	Some(format!("{}/{}", p.price, p.qty))
}

/// A book lane's readings: an aggregate has an event window and a reception window, and nothing
/// else. Both are always known, hence non-nullable.
fn book_ts_fields() -> [Field; 2] {
	[Field::new("ts_venue_exec", DataType::Int64, false), Field::new("ts_local_recv", DataType::Int64, false)]
}

/// The wire columns a point-event venue lane carries, in schema order.
fn venue_ts_fields() -> [Field; 3] {
	[
		Field::new("ts_venue_exec", DataType::Int64, false),
		Field::new("ts_venue_send", DataType::Int64, true),
		Field::new("ts_local_recv", DataType::Int64, true),
	]
}

// Named lookup, not positional: inserting a column would otherwise shift every index in every
// decoder at once, and round-trip tests still pass when both sides shift together.
fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> &'a T {
	b.column_by_name(name)
		.unwrap_or_else(|| panic!("stored lane missing column {name}"))
		.as_any()
		.downcast_ref::<T>()
		.unwrap_or_else(|| panic!("column {name} has unexpected arrow type"))
}

fn opt_ts<A>(a: &Int64Array, i: usize) -> Option<Ts<A>> {
	(!a.is_null(i)).then(|| Ts::from_nanos(a.value(i)))
}

#[derive(Default)]
pub struct TradeBuilders {
	ts_venue_exec: arrow::array::Int64Builder,
	ts_venue_send: arrow::array::Int64Builder,
	ts_local_recv: arrow::array::Int64Builder,
	monotonic_seq: arrow::array::UInt64Builder,
	side: arrow::array::UInt8Builder,
	price_raw: arrow::array::Int32Builder,
	qty_raw: arrow::array::UInt32Builder,
}

impl Sealed for Trade {
	type Builders = TradeBuilders;
	type Meta = PrecisionPriceQty;

	const PER_ROW_MIN: usize = 48;

	fn schema(meta: PrecisionPriceQty) -> SchemaRef {
		let mut fields = venue_ts_fields().to_vec();
		fields.extend([
			Field::new("monotonic_seq", DataType::UInt64, false),
			Field::new("side", DataType::UInt8, false),
			Field::new("price_raw", DataType::Int32, false),
			Field::new("qty_raw", DataType::UInt32, false),
		]);
		schema_with(fields, &prec_pairs(meta))
	}

	fn append(&self, b: &mut TradeBuilders, _meta: PrecisionPriceQty) {
		b.ts_venue_exec.append_value(self.ts_venue_exec.as_nanos());
		b.ts_venue_send.append_option(self.ts_venue_send.map(Ts::as_nanos));
		b.ts_local_recv.append_option(self.ts_local_recv.map(Ts::as_nanos));
		b.monotonic_seq.append_value(self.monotonic_seq);
		b.side.append_value(side_u8(self.side));
		b.price_raw.append_value(self.price);
		b.qty_raw.append_value(self.qty);
	}

	fn finish(b: &mut TradeBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_venue_exec.finish()),
			Arc::new(b.ts_venue_send.finish()),
			Arc::new(b.ts_local_recv.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.side.finish()),
			Arc::new(b.price_raw.finish()),
			Arc::new(b.qty_raw.finish()),
		]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let exec = col::<Int64Array>(batch, "ts_venue_exec");
		let send = col::<Int64Array>(batch, "ts_venue_send");
		let recv = col::<Int64Array>(batch, "ts_local_recv");
		let monotonic = col::<UInt64Array>(batch, "monotonic_seq");
		let side = col::<UInt8Array>(batch, "side");
		let price = col::<Int32Array>(batch, "price_raw");
		let qty = col::<UInt32Array>(batch, "qty_raw");
		(0..batch.num_rows())
			.map(|i| Trade {
				ts_venue_exec: Ts::from_nanos(exec.value(i)),
				ts_venue_send: opt_ts(send, i),
				ts_local_recv: opt_ts(recv, i),
				monotonic_seq: monotonic.value(i),
				side: side_from(side.value(i)),
				price: price.value(i),
				qty: qty.value(i),
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<String> {
		prec_sig(schema)
	}

	fn approx_bytes(&self) -> usize {
		48
	}
}

#[derive(Default)]
pub struct BookDeltaBuilders {
	ts_venue_exec: arrow::array::Int64Builder,
	ts_local_recv: arrow::array::Int64Builder,
	monotonic_seq: arrow::array::UInt64Builder,
	kind: arrow::array::UInt8Builder,
	side: arrow::array::UInt8Builder,
	price_raw: arrow::array::Int32Builder,
	qty_raw: arrow::array::UInt32Builder,
}

impl Sealed for BookDelta {
	type Builders = BookDeltaBuilders;
	type Meta = PrecisionPriceQty;

	const PER_ROW_MIN: usize = 40;

	fn schema(meta: PrecisionPriceQty) -> SchemaRef {
		let mut fields = book_ts_fields().to_vec();
		fields.extend([
			Field::new("monotonic_seq", DataType::UInt64, false),
			Field::new("kind", DataType::UInt8, false),
			Field::new("side", DataType::UInt8, false),
			Field::new("price_raw", DataType::Int32, false),
			Field::new("qty_raw", DataType::UInt32, false),
		]);
		schema_with(fields, &prec_pairs(meta))
	}

	fn append(&self, b: &mut BookDeltaBuilders, _meta: PrecisionPriceQty) {
		b.ts_venue_exec.append_value(self.ts_venue_exec.as_nanos());
		b.ts_local_recv.append_value(self.ts_local_recv.as_nanos());
		b.monotonic_seq.append_value(self.monotonic_seq);
		b.kind.append_value(kind_u8(self.kind));
		b.side.append_value(side_u8(self.side));
		b.price_raw.append_value(self.price);
		b.qty_raw.append_value(self.qty);
	}

	fn finish(b: &mut BookDeltaBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_venue_exec.finish()),
			Arc::new(b.ts_local_recv.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.kind.finish()),
			Arc::new(b.side.finish()),
			Arc::new(b.price_raw.finish()),
			Arc::new(b.qty_raw.finish()),
		]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let exec = col::<Int64Array>(batch, "ts_venue_exec");
		let recv = col::<Int64Array>(batch, "ts_local_recv");
		let monotonic = col::<UInt64Array>(batch, "monotonic_seq");
		let kind = col::<UInt8Array>(batch, "kind");
		let side = col::<UInt8Array>(batch, "side");
		let price = col::<Int32Array>(batch, "price_raw");
		let qty = col::<UInt32Array>(batch, "qty_raw");
		(0..batch.num_rows())
			.map(|i| BookDelta {
				ts_venue_exec: Ts::from_nanos(exec.value(i)),
				ts_local_recv: Ts::from_nanos(recv.value(i)),
				monotonic_seq: monotonic.value(i),
				kind: kind_from(kind.value(i)),
				side: side_from(side.value(i)),
				price: price.value(i),
				qty: qty.value(i),
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<String> {
		prec_sig(schema)
	}

	fn approx_bytes(&self) -> usize {
		40
	}
}

#[derive(Default)]
pub struct BookSnapshotBuilders {
	ts_venue_exec: arrow::array::Int64Builder,
	ts_local_recv: arrow::array::Int64Builder,
	monotonic_seq: arrow::array::UInt64Builder,
	bid_prices: arrow::array::ListBuilder<arrow::array::Int32Builder>,
	bid_qtys: arrow::array::ListBuilder<arrow::array::UInt32Builder>,
	ask_prices: arrow::array::ListBuilder<arrow::array::Int32Builder>,
	ask_qtys: arrow::array::ListBuilder<arrow::array::UInt32Builder>,
}

impl Sealed for BookSnapshot {
	type Builders = BookSnapshotBuilders;
	type Meta = PrecisionPriceQty;

	const PER_ROW_MIN: usize = 32;

	fn schema(meta: PrecisionPriceQty) -> SchemaRef {
		let i32_list = || DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
		let u32_list = || DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
		let mut fields = book_ts_fields().to_vec();
		fields.extend([
			Field::new("monotonic_seq", DataType::UInt64, false),
			Field::new("bid_prices", i32_list(), false),
			Field::new("bid_qtys", u32_list(), false),
			Field::new("ask_prices", i32_list(), false),
			Field::new("ask_qtys", u32_list(), false),
		]);
		schema_with(fields, &prec_pairs(meta))
	}

	fn append(&self, b: &mut BookSnapshotBuilders, _meta: PrecisionPriceQty) {
		b.ts_venue_exec.append_value(self.ts_venue_exec.as_nanos());
		b.ts_local_recv.append_value(self.ts_local_recv.as_nanos());
		b.monotonic_seq.append_value(self.monotonic_seq);
		for &p in &self.bid_prices {
			b.bid_prices.values().append_value(p);
		}
		for &q in &self.bid_qtys {
			b.bid_qtys.values().append_value(q);
		}
		for &p in &self.ask_prices {
			b.ask_prices.values().append_value(p);
		}
		for &q in &self.ask_qtys {
			b.ask_qtys.values().append_value(q);
		}
		b.bid_prices.append(true);
		b.bid_qtys.append(true);
		b.ask_prices.append(true);
		b.ask_qtys.append(true);
	}

	fn finish(b: &mut BookSnapshotBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_venue_exec.finish()),
			Arc::new(b.ts_local_recv.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.bid_prices.finish()),
			Arc::new(b.bid_qtys.finish()),
			Arc::new(b.ask_prices.finish()),
			Arc::new(b.ask_qtys.finish()),
		]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let exec = col::<Int64Array>(batch, "ts_venue_exec");
		let recv = col::<Int64Array>(batch, "ts_local_recv");
		let monotonic = col::<UInt64Array>(batch, "monotonic_seq");
		let bid_prices = col_i32_list(batch, "bid_prices");
		let bid_qtys = col_u32_list(batch, "bid_qtys");
		let ask_prices = col_i32_list(batch, "ask_prices");
		let ask_qtys = col_u32_list(batch, "ask_qtys");
		(0..batch.num_rows())
			.map(|i| BookSnapshot {
				ts_venue_exec: Ts::from_nanos(exec.value(i)),
				ts_local_recv: Ts::from_nanos(recv.value(i)),
				monotonic_seq: monotonic.value(i),
				bid_prices: bid_prices[i].clone(),
				bid_qtys: bid_qtys[i].clone(),
				ask_prices: ask_prices[i].clone(),
				ask_qtys: ask_qtys[i].clone(),
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<String> {
		prec_sig(schema)
	}

	fn approx_bytes(&self) -> usize {
		32 + 8 * (self.bid_prices.len() + self.ask_prices.len())
	}
}

#[derive(Default)]
pub struct OiBuilders {
	ts_venue_exec: arrow::array::Int64Builder,
	ts_venue_send: arrow::array::Int64Builder,
	ts_local_recv: arrow::array::Int64Builder,
	oi: Float64Builder,
}

impl Sealed for Oi {
	type Builders = OiBuilders;
	type Meta = ();

	const PER_ROW_MIN: usize = 32;

	fn schema(_meta: ()) -> SchemaRef {
		let mut fields = venue_ts_fields().to_vec();
		fields.push(Field::new("oi", DataType::Float64, false));
		schema_with(fields, &[])
	}

	fn append(&self, b: &mut OiBuilders, _meta: ()) {
		b.ts_venue_exec.append_value(self.ts_venue_exec.as_nanos());
		b.ts_venue_send.append_option(self.ts_venue_send.map(Ts::as_nanos));
		b.ts_local_recv.append_option(self.ts_local_recv.map(Ts::as_nanos));
		b.oi.append_value(self.oi);
	}

	fn finish(b: &mut OiBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_venue_exec.finish()),
			Arc::new(b.ts_venue_send.finish()),
			Arc::new(b.ts_local_recv.finish()),
			Arc::new(b.oi.finish()),
		]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let exec = col::<Int64Array>(batch, "ts_venue_exec");
		let send = col::<Int64Array>(batch, "ts_venue_send");
		let recv = col::<Int64Array>(batch, "ts_local_recv");
		let oi = col::<Float64Array>(batch, "oi");
		(0..batch.num_rows())
			.map(|i| Oi {
				ts_venue_exec: Ts::from_nanos(exec.value(i)),
				ts_venue_send: opt_ts(send, i),
				ts_local_recv: opt_ts(recv, i),
				oi: oi.value(i),
			})
			.collect()
	}

	fn file_sig(_schema: &Schema) -> Option<String> {
		None
	}

	fn approx_bytes(&self) -> usize {
		32
	}
}

#[derive(Default)]
pub struct McBuilders {
	ts_local_exec: arrow::array::Int64Builder,
	market_cap: Float64Builder,
	rank: arrow::array::UInt32Builder,
}

impl Sealed for Mc {
	type Builders = McBuilders;
	type Meta = ();

	const PER_ROW_MIN: usize = 20;

	fn schema(_meta: ()) -> SchemaRef {
		schema_with(
			vec![
				Field::new("ts_local_exec", DataType::Int64, false),
				Field::new("market_cap", DataType::Float64, false),
				Field::new("rank", DataType::UInt32, true),
			],
			&[],
		)
	}

	fn append(&self, b: &mut McBuilders, _meta: ()) {
		b.ts_local_exec.append_value(self.ts_local_exec.as_nanos());
		b.market_cap.append_value(self.market_cap);
		b.rank.append_option(self.rank);
	}

	fn finish(b: &mut McBuilders) -> Vec<ArrayRef> {
		vec![Arc::new(b.ts_local_exec.finish()), Arc::new(b.market_cap.finish()), Arc::new(b.rank.finish())]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let exec = col::<Int64Array>(batch, "ts_local_exec");
		let market_cap = col::<Float64Array>(batch, "market_cap");
		let rank = col::<UInt32Array>(batch, "rank");
		(0..batch.num_rows())
			.map(|i| Mc {
				ts_local_exec: Ts::from_nanos(exec.value(i)),
				market_cap: market_cap.value(i),
				rank: (!rank.is_null(i)).then(|| rank.value(i)),
			})
			.collect()
	}

	fn file_sig(_schema: &Schema) -> Option<String> {
		None
	}

	fn approx_bytes(&self) -> usize {
		20
	}
}

fn col_i32_list(b: &RecordBatch, name: &str) -> Vec<Vec<i32>> {
	let list = col::<ListArray>(b, name);
	let values = list.values().as_any().downcast_ref::<Int32Array>().expect("i32 inner");
	let offsets = list.offsets();
	(0..list.len())
		.map(|i| {
			let start = offsets[i] as usize;
			let end = offsets[i + 1] as usize;
			(start..end).map(|j| values.value(j)).collect()
		})
		.collect()
}

fn col_u32_list(b: &RecordBatch, name: &str) -> Vec<Vec<u32>> {
	let list = col::<ListArray>(b, name);
	let values = list.values().as_any().downcast_ref::<UInt32Array>().expect("u32 inner");
	let offsets = list.offsets();
	(0..list.len())
		.map(|i| {
			let start = offsets[i] as usize;
			let end = offsets[i + 1] as usize;
			(start..end).map(|j| values.value(j)).collect()
		})
		.collect()
}

// DAG impls for this crate's own row types; the core out-types carry theirs in `trading_data_core`,
// where orphan rules put them.

impl Flat for Oi {
	const DIMS: &'static [usize] = &[1];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.oi;
		true
	}
}

impl Bump for Oi {
	fn bump(self, _slot: usize, h: f64) -> (Self, f64) {
		(Self { oi: self.oi + h, ..self }, h)
	}
}

impl Glance for Oi {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "oi {}", self.oi)
	}
}

impl Flat for Mc {
	const DIMS: &'static [usize] = &[1];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.market_cap;
		true
	}
}

impl Bump for Mc {
	fn bump(self, _slot: usize, h: f64) -> (Self, f64) {
		(
			Self {
				market_cap: self.market_cap + h,
				..self
			},
			h,
		)
	}
}

impl Glance for Mc {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "mc {:.3e}", self.market_cap)
	}
}

impl Stamped for Oi {
	fn ts_ns(&self) -> i64 {
		self.ts_venue_exec.as_nanos()
	}
}

impl Stamped for Mc {
	fn ts_ns(&self) -> i64 {
		self.ts_local_exec.as_nanos()
	}
}

pub struct OiRoot;
impl Cell for OiRoot {
	type Out<'t> = &'t [Oi];
}
slice_nudge!(OiRoot, Oi);

pub struct McRoot;
impl Cell for McRoot {
	type Out<'t> = &'t [Mc];
}
slice_nudge!(McRoot, Mc);
