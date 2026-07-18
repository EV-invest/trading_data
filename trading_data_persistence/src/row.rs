use std::sync::Arc;

use arrow::{
	array::{Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array, Int64Array, ListArray, RecordBatch, UInt8Array, UInt32Array, UInt64Array},
	datatypes::{DataType, Field, Schema, SchemaRef},
};
use trading_data_dag::{Flat, Glance, Stamped};
use v_utils::trades::{PrecisionPriceQty, Side};

use crate::feather::RotationPolicy;

pub const SCHEMA_VERSION: &str = "3";
pub type UnixNanos = i64;

/// A lane's row type. Sealed: the set of lanes is this crate's contract with the disk.
pub trait Row: sealed::Sealed {
	/// Rotation default for this lane.
	const POLICY: RotationPolicy;
}

/// Surfaces are f64 + typed [`Side`]; scaled ints are a storage detail.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Trade {
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
	pub monotonic_seq: u64,
	pub trade_id: u64,
	pub side: Side,
	pub price: f64,
	pub qty: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BookDelta {
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
	pub monotonic_seq: u64,
	pub gapped: bool,
	/// Buy = bid, Sell = ask.
	pub side: Side,
	pub price: f64,
	/// `0.0` means delete this level.
	pub qty: f64,
}

/// Open interest, base units.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Oi {
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
	pub oi: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Mc {
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
	pub market_cap: f64,
	/// Honest-None when the source lacks it.
	pub rank: Option<u32>,
}

/// Raw-keyed book snapshot; internal to the Book persistence model.
#[derive(Clone, Debug)]
pub(crate) struct BookSnapshot {
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
	pub monotonic_seq: u64,
	pub bid_prices: Vec<i32>,
	pub bid_qtys: Vec<u32>,
	pub ask_prices: Vec<i32>,
	pub ask_qtys: Vec<u32>,
}

impl Row for Trade {
	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(50 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(24 * 3600)),
	};
}
impl Row for BookDelta {
	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(256 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(3600)),
	};
}
impl Row for BookSnapshot {
	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: Some(64 * 1024 * 1024),
		max_age: Some(std::time::Duration::from_secs(6 * 3600)),
	};
}
impl Row for Oi {
	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: None,
		max_age: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
	};
}
impl Row for Mc {
	const POLICY: RotationPolicy = RotationPolicy {
		max_bytes: None,
		max_age: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
	};
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
		fn ts_event(&self) -> UnixNanos;
		fn ts_init(&self) -> UnixNanos;
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

/// f64 → scaled int with a bit-exact round-trip assert: no silent truncation.
fn raw_price(price: f64, prec: u8) -> i32 {
	let scale = 10f64.powi(prec as i32);
	let raw = (price * scale).round() as i32;
	assert!(raw as f64 / scale == price, "price {price} does not round-trip at precision {prec}");
	raw
}

fn raw_qty(qty: f64, prec: u8) -> u32 {
	let scale = 10f64.powi(prec as i32);
	let raw = (qty * scale).round() as u32;
	assert!(raw as f64 / scale == qty, "qty {qty} does not round-trip at precision {prec}");
	raw
}

pub(crate) fn prec_from_schema(schema: &Schema) -> PrecisionPriceQty {
	let get = |k: &str| {
		schema
			.metadata()
			.get(k)
			.unwrap_or_else(|| panic!("file metadata missing {k}"))
			.parse::<u8>()
			.unwrap_or_else(|e| panic!("file metadata {k} not a u8: {e}"))
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

fn col_i64(b: &RecordBatch, idx: usize) -> &Int64Array {
	b.column(idx).as_any().downcast_ref::<Int64Array>().expect("i64 column")
}

fn col_u64(b: &RecordBatch, idx: usize) -> &UInt64Array {
	b.column(idx).as_any().downcast_ref::<UInt64Array>().expect("u64 column")
}

fn col_f64(b: &RecordBatch, idx: usize) -> &Float64Array {
	b.column(idx).as_any().downcast_ref::<Float64Array>().expect("f64 column")
}

#[derive(Default)]
pub struct TradeBuilders {
	ts_event: arrow::array::Int64Builder,
	ts_init: arrow::array::Int64Builder,
	monotonic_seq: arrow::array::UInt64Builder,
	trade_id: arrow::array::UInt64Builder,
	side: arrow::array::UInt8Builder,
	price_raw: arrow::array::Int32Builder,
	qty_raw: arrow::array::UInt32Builder,
}

impl Sealed for Trade {
	type Builders = TradeBuilders;
	type Meta = PrecisionPriceQty;

	const PER_ROW_MIN: usize = 40;

	fn schema(meta: PrecisionPriceQty) -> SchemaRef {
		schema_with(
			vec![
				Field::new("ts_event", DataType::Int64, false),
				Field::new("ts_init", DataType::Int64, false),
				Field::new("monotonic_seq", DataType::UInt64, false),
				Field::new("trade_id", DataType::UInt64, false),
				Field::new("side", DataType::UInt8, false),
				Field::new("price_raw", DataType::Int32, false),
				Field::new("qty_raw", DataType::UInt32, false),
			],
			&prec_pairs(meta),
		)
	}

	fn append(&self, b: &mut TradeBuilders, meta: PrecisionPriceQty) {
		b.ts_event.append_value(self.ts_event);
		b.ts_init.append_value(self.ts_init);
		b.monotonic_seq.append_value(self.monotonic_seq);
		b.trade_id.append_value(self.trade_id);
		b.side.append_value(side_u8(self.side));
		b.price_raw.append_value(raw_price(self.price, meta.price));
		b.qty_raw.append_value(raw_qty(self.qty, meta.qty));
	}

	fn finish(b: &mut TradeBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_event.finish()),
			Arc::new(b.ts_init.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.trade_id.finish()),
			Arc::new(b.side.finish()),
			Arc::new(b.price_raw.finish()),
			Arc::new(b.qty_raw.finish()),
		]
	}

	fn decode(batch: &RecordBatch, file_schema: &Schema) -> Vec<Self> {
		let prec = prec_from_schema(file_schema);
		let (p_scale, q_scale) = (10f64.powi(prec.price as i32), 10f64.powi(prec.qty as i32));
		let ts_event = col_i64(batch, 0);
		let ts_init = col_i64(batch, 1);
		let monotonic = col_u64(batch, 2);
		let trade_id = col_u64(batch, 3);
		let side = batch.column(4).as_any().downcast_ref::<UInt8Array>().expect("u8 column");
		let price = batch.column(5).as_any().downcast_ref::<Int32Array>().expect("i32 column");
		let qty = batch.column(6).as_any().downcast_ref::<UInt32Array>().expect("u32 column");
		(0..batch.num_rows())
			.map(|i| Trade {
				ts_event: ts_event.value(i),
				ts_init: ts_init.value(i),
				monotonic_seq: monotonic.value(i),
				trade_id: trade_id.value(i),
				side: side_from(side.value(i)),
				price: price.value(i) as f64 / p_scale,
				qty: qty.value(i) as f64 / q_scale,
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<String> {
		prec_sig(schema)
	}

	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn ts_init(&self) -> UnixNanos {
		self.ts_init
	}

	fn approx_bytes(&self) -> usize {
		40
	}
}

#[derive(Default)]
pub struct BookDeltaBuilders {
	ts_event: arrow::array::Int64Builder,
	ts_init: arrow::array::Int64Builder,
	monotonic_seq: arrow::array::UInt64Builder,
	gapped: BooleanBuilder,
	side: arrow::array::UInt8Builder,
	price_raw: arrow::array::Int32Builder,
	qty_raw: arrow::array::UInt32Builder,
}

impl Sealed for BookDelta {
	type Builders = BookDeltaBuilders;
	type Meta = PrecisionPriceQty;

	const PER_ROW_MIN: usize = 40;

	fn schema(meta: PrecisionPriceQty) -> SchemaRef {
		schema_with(
			vec![
				Field::new("ts_event", DataType::Int64, false),
				Field::new("ts_init", DataType::Int64, false),
				Field::new("monotonic_seq", DataType::UInt64, false),
				Field::new("gapped", DataType::Boolean, false),
				Field::new("side", DataType::UInt8, false),
				Field::new("price_raw", DataType::Int32, false),
				Field::new("qty_raw", DataType::UInt32, false),
			],
			&prec_pairs(meta),
		)
	}

	fn append(&self, b: &mut BookDeltaBuilders, meta: PrecisionPriceQty) {
		b.ts_event.append_value(self.ts_event);
		b.ts_init.append_value(self.ts_init);
		b.monotonic_seq.append_value(self.monotonic_seq);
		b.gapped.append_value(self.gapped);
		b.side.append_value(side_u8(self.side));
		b.price_raw.append_value(raw_price(self.price, meta.price));
		b.qty_raw.append_value(raw_qty(self.qty, meta.qty));
	}

	fn finish(b: &mut BookDeltaBuilders) -> Vec<ArrayRef> {
		vec![
			Arc::new(b.ts_event.finish()),
			Arc::new(b.ts_init.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.gapped.finish()),
			Arc::new(b.side.finish()),
			Arc::new(b.price_raw.finish()),
			Arc::new(b.qty_raw.finish()),
		]
	}

	fn decode(batch: &RecordBatch, file_schema: &Schema) -> Vec<Self> {
		let prec = prec_from_schema(file_schema);
		let (p_scale, q_scale) = (10f64.powi(prec.price as i32), 10f64.powi(prec.qty as i32));
		let ts_event = col_i64(batch, 0);
		let ts_init = col_i64(batch, 1);
		let monotonic = col_u64(batch, 2);
		let gapped = batch.column(3).as_any().downcast_ref::<BooleanArray>().expect("bool column");
		let side = batch.column(4).as_any().downcast_ref::<UInt8Array>().expect("u8 column");
		let price = batch.column(5).as_any().downcast_ref::<Int32Array>().expect("i32 column");
		let qty = batch.column(6).as_any().downcast_ref::<UInt32Array>().expect("u32 column");
		(0..batch.num_rows())
			.map(|i| BookDelta {
				ts_event: ts_event.value(i),
				ts_init: ts_init.value(i),
				monotonic_seq: monotonic.value(i),
				gapped: gapped.value(i),
				side: side_from(side.value(i)),
				price: price.value(i) as f64 / p_scale,
				qty: qty.value(i) as f64 / q_scale,
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<String> {
		prec_sig(schema)
	}

	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn ts_init(&self) -> UnixNanos {
		self.ts_init
	}

	fn approx_bytes(&self) -> usize {
		40
	}
}

#[derive(Default)]
pub struct BookSnapshotBuilders {
	ts_event: arrow::array::Int64Builder,
	ts_init: arrow::array::Int64Builder,
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
		schema_with(
			vec![
				Field::new("ts_event", DataType::Int64, false),
				Field::new("ts_init", DataType::Int64, false),
				Field::new("monotonic_seq", DataType::UInt64, false),
				Field::new("bid_prices", i32_list(), false),
				Field::new("bid_qtys", u32_list(), false),
				Field::new("ask_prices", i32_list(), false),
				Field::new("ask_qtys", u32_list(), false),
			],
			&prec_pairs(meta),
		)
	}

	fn append(&self, b: &mut BookSnapshotBuilders, _meta: PrecisionPriceQty) {
		b.ts_event.append_value(self.ts_event);
		b.ts_init.append_value(self.ts_init);
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
			Arc::new(b.ts_event.finish()),
			Arc::new(b.ts_init.finish()),
			Arc::new(b.monotonic_seq.finish()),
			Arc::new(b.bid_prices.finish()),
			Arc::new(b.bid_qtys.finish()),
			Arc::new(b.ask_prices.finish()),
			Arc::new(b.ask_qtys.finish()),
		]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let ts_event = col_i64(batch, 0);
		let ts_init = col_i64(batch, 1);
		let monotonic = col_u64(batch, 2);
		let bid_prices = col_i32_list(batch, 3);
		let bid_qtys = col_u32_list(batch, 4);
		let ask_prices = col_i32_list(batch, 5);
		let ask_qtys = col_u32_list(batch, 6);
		(0..batch.num_rows())
			.map(|i| BookSnapshot {
				ts_event: ts_event.value(i),
				ts_init: ts_init.value(i),
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

	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn ts_init(&self) -> UnixNanos {
		self.ts_init
	}

	fn approx_bytes(&self) -> usize {
		32 + 8 * (self.bid_prices.len() + self.ask_prices.len())
	}
}

#[derive(Default)]
pub struct OiBuilders {
	ts_event: arrow::array::Int64Builder,
	ts_init: arrow::array::Int64Builder,
	oi: Float64Builder,
}

impl Sealed for Oi {
	type Builders = OiBuilders;
	type Meta = ();

	const PER_ROW_MIN: usize = 24;

	fn schema(_meta: ()) -> SchemaRef {
		schema_with(
			vec![
				Field::new("ts_event", DataType::Int64, false),
				Field::new("ts_init", DataType::Int64, false),
				Field::new("oi", DataType::Float64, false),
			],
			&[],
		)
	}

	fn append(&self, b: &mut OiBuilders, _meta: ()) {
		b.ts_event.append_value(self.ts_event);
		b.ts_init.append_value(self.ts_init);
		b.oi.append_value(self.oi);
	}

	fn finish(b: &mut OiBuilders) -> Vec<ArrayRef> {
		vec![Arc::new(b.ts_event.finish()), Arc::new(b.ts_init.finish()), Arc::new(b.oi.finish())]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let ts_event = col_i64(batch, 0);
		let ts_init = col_i64(batch, 1);
		let oi = col_f64(batch, 2);
		(0..batch.num_rows())
			.map(|i| Oi {
				ts_event: ts_event.value(i),
				ts_init: ts_init.value(i),
				oi: oi.value(i),
			})
			.collect()
	}

	fn file_sig(_schema: &Schema) -> Option<String> {
		None
	}

	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn ts_init(&self) -> UnixNanos {
		self.ts_init
	}

	fn approx_bytes(&self) -> usize {
		24
	}
}

#[derive(Default)]
pub struct McBuilders {
	ts_event: arrow::array::Int64Builder,
	ts_init: arrow::array::Int64Builder,
	market_cap: Float64Builder,
	rank: arrow::array::UInt32Builder,
}

impl Sealed for Mc {
	type Builders = McBuilders;
	type Meta = ();

	const PER_ROW_MIN: usize = 28;

	fn schema(_meta: ()) -> SchemaRef {
		schema_with(
			vec![
				Field::new("ts_event", DataType::Int64, false),
				Field::new("ts_init", DataType::Int64, false),
				Field::new("market_cap", DataType::Float64, false),
				Field::new("rank", DataType::UInt32, true),
			],
			&[],
		)
	}

	fn append(&self, b: &mut McBuilders, _meta: ()) {
		b.ts_event.append_value(self.ts_event);
		b.ts_init.append_value(self.ts_init);
		b.market_cap.append_value(self.market_cap);
		b.rank.append_option(self.rank);
	}

	fn finish(b: &mut McBuilders) -> Vec<ArrayRef> {
		vec![Arc::new(b.ts_event.finish()), Arc::new(b.ts_init.finish()), Arc::new(b.market_cap.finish()), Arc::new(b.rank.finish())]
	}

	fn decode(batch: &RecordBatch, _file_schema: &Schema) -> Vec<Self> {
		let ts_event = col_i64(batch, 0);
		let ts_init = col_i64(batch, 1);
		let market_cap = col_f64(batch, 2);
		let rank = batch.column(3).as_any().downcast_ref::<UInt32Array>().expect("u32 column");
		(0..batch.num_rows())
			.map(|i| Mc {
				ts_event: ts_event.value(i),
				ts_init: ts_init.value(i),
				market_cap: market_cap.value(i),
				rank: (!rank.is_null(i)).then(|| rank.value(i)),
			})
			.collect()
	}

	fn file_sig(_schema: &Schema) -> Option<String> {
		None
	}

	fn ts_event(&self) -> UnixNanos {
		self.ts_event
	}

	fn ts_init(&self) -> UnixNanos {
		self.ts_init
	}

	fn approx_bytes(&self) -> usize {
		28
	}
}

fn col_i32_list(b: &RecordBatch, idx: usize) -> Vec<Vec<i32>> {
	let list = b.column(idx).as_any().downcast_ref::<ListArray>().expect("list column");
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

fn col_u32_list(b: &RecordBatch, idx: usize) -> Vec<Vec<u32>> {
	let list = b.column(idx).as_any().downcast_ref::<ListArray>().expect("list column");
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

// DAG impls live here: `trading_data_dag` is dep-free, orphan rules force them into this crate.
// Nodes receive the full struct as deps; `Flat` is observation-only, so side/ts stay metadata.

impl Stamped for Trade {
	fn ts_ns(&self) -> i64 {
		self.ts_event
	}
}

impl Flat for Trade {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.price, self.qty]);
		true
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		let mut r = *self;
		*match slot {
			0 => &mut r.price,
			1 => &mut r.qty,
			_ => unreachable!("LEN = 2"),
		} += h;
		r
	}
}

impl Glance for Trade {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{} {}@{}", self.side, self.qty, self.price)
	}
}

impl Stamped for BookDelta {
	fn ts_ns(&self) -> i64 {
		self.ts_event
	}
}

impl Flat for BookDelta {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.price, self.qty]);
		true
	}

	fn nudge(&self, slot: usize, h: f64) -> Self {
		let mut r = *self;
		*match slot {
			0 => &mut r.price,
			1 => &mut r.qty,
			_ => unreachable!("LEN = 2"),
		} += h;
		r
	}
}

impl Glance for BookDelta {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{} {}@{}", self.side, self.qty, self.price)
	}
}

impl Stamped for Oi {
	fn ts_ns(&self) -> i64 {
		self.ts_event
	}
}

impl Flat for Oi {
	const DIMS: &'static [usize] = &[1];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.oi;
		true
	}

	fn nudge(&self, _slot: usize, h: f64) -> Self {
		Self { oi: self.oi + h, ..*self }
	}
}

impl Glance for Oi {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "oi {}", self.oi)
	}
}

impl Stamped for Mc {
	fn ts_ns(&self) -> i64 {
		self.ts_event
	}
}

impl Flat for Mc {
	const DIMS: &'static [usize] = &[1];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = self.market_cap;
		true
	}

	fn nudge(&self, _slot: usize, h: f64) -> Self {
		Self {
			market_cap: self.market_cap + h,
			..*self
		}
	}
}

impl Glance for Mc {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "mc {:.3e}", self.market_cap)
	}
}
