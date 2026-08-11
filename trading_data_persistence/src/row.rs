use std::sync::Arc;

use arrow::{
	array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, UInt8Array, UInt32Array, UInt64Array},
	datatypes::{DataType, Field, Schema, SchemaRef},
};
use trading_data_core::{BookDelta, FrameKind, Local, Precision, PrecisionPriceQty, Side, TradeCols, Ts, Venue};
use trading_data_dag::{Bump, Cell, Flat, Glance, Item, Stamped, always_present, slice_nudge};
use trading_data_macros::Lane;

use crate::feather::RotationPolicy;

pub const SCHEMA_VERSION: &str = "7";
pub type UnixNanos = i64;

/// Every signature is two `i8`s as `{}/{}`, so `"-128/-128"` (9 bytes) is the worst case.
pub(crate) type FileSig = arrayvec::ArrayString<16>;

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
#[derive(Clone, Copy, Debug, Lane, PartialEq)]
#[lane(per_row_min = 48, prec)]
pub struct Trade {
	#[col(ts)]
	pub ts_venue_exec: Ts<Venue>,
	/// `Some` once the adapter reports an envelope time distinct from the execution time.
	#[col(ts, null)]
	pub ts_venue_send: Option<Ts<Venue>>,
	/// `Some` ⇔ we were there when it arrived (live-recorded); historic ingest writes `None`.
	#[col(ts, null)]
	pub ts_local_recv: Option<Ts<Local>>,
	#[col(u64)]
	pub monotonic_seq: u64,
	#[col(u8, enc = side_u8, dec = side_from)]
	pub side: Side,
	#[col(i32, name = "price_raw")]
	pub price: i32,
	#[col(u32, name = "qty_raw")]
	pub qty: u32,
}

/// Open interest, base units.
#[derive(Clone, Copy, Debug, Item, Lane, PartialEq)]
#[lane(per_row_min = 32)]
pub struct Oi {
	#[stamp]
	#[col(ts)]
	pub ts_venue_exec: Ts<Venue>,
	#[col(ts, null)]
	pub ts_venue_send: Option<Ts<Venue>>,
	/// `Some` ⇔ we were there when it arrived (live-recorded); historic ingest writes `None`.
	#[col(ts, null)]
	pub ts_local_recv: Option<Ts<Local>>,
	#[slot]
	#[col(f64)]
	pub oi: f64,
}

/// Market cap. The source reports no event time, so the only reading is **ours** — this lane's
/// axis is `Local`, and venue-named columns would be a lie about whose clock produced it.
#[derive(Clone, Copy, Debug, Lane, PartialEq)]
#[lane(per_row_min = 20)]
pub struct Mc {
	#[col(ts)]
	pub ts_local_exec: Ts<Local>,
	#[col(f64)]
	pub market_cap: f64,
	/// Honest-None when the source lacks it.
	#[col(u32, null)]
	pub rank: Option<u32>,
}

/// Raw-keyed book snapshot; internal to the Book persistence model.
#[derive(Clone, Debug)]
pub(crate) struct BookSnapshot {
	pub ts_venue_exec: Ts<Venue>,
	/// Always present: see [`BookDelta`]'s own reception reading.
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
		zstd_level: 3,
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
		zstd_level: 3,
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
		zstd_level: 3,
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
		zstd_level: 3,
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
		zstd_level: 3,
	};

	fn ts_axis(&self) -> Ts<Local> {
		self.ts_local_exec
	}
}

pub(crate) mod sealed {
	use super::*;

	pub trait Sealed: Sized {
		type Builders;
		/// Precision context for scaled-int lanes; `()` for f64-native lanes.
		type Meta: Copy;
		const PER_ROW_MIN: usize;
		/// `rows` is what the rotation policy says the batch reaches before it is written, or 0 when
		/// the policy does not bound it. Half-measures are pointless here — a `Vec` growing to `n`
		/// copies ~`2n` bytes whatever it starts at, and the final doubling alone outweighs every one
		/// before it — so this is the whole count or nothing.
		fn builders(rows: usize) -> Self::Builders;
		fn schema(meta: Self::Meta) -> SchemaRef;
		fn append(&self, b: &mut Self::Builders, meta: Self::Meta);
		fn finish(b: &mut Self::Builders) -> Vec<ArrayRef>;
		fn decode(batch: &RecordBatch, file_schema: &Schema) -> Vec<Self>;
		/// Metadata that must agree across every file of a read range; `None` when the lane has none.
		fn file_sig(schema: &Schema) -> Option<FileSig>;
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

fn prec_sig(schema: &Schema) -> Option<FileSig> {
	use core::fmt::Write;
	let p = prec_from_schema(schema);
	let mut sig = FileSig::new();
	write!(sig, "{}/{}", p.price, p.qty).expect("two i8s and a slash fit");
	Some(sig)
}

/// A book lane's readings: an aggregate has an event window and a reception window, and nothing
/// else. Both are always known, hence non-nullable.
fn book_ts_fields() -> [Field; 2] {
	[Field::new("ts_venue_exec", DataType::Int64, false), Field::new("ts_local_recv", DataType::Int64, false)]
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

impl TradeBuilders {
	/// A run arrives as columns and lands as columns; routing it through a [`Trade`] per row put a
	/// capacity check and a validity-bit write on all seven fields of every one of them. The three
	/// columns whose element type already matches the builder's go across whole.
	pub(crate) fn extend(&mut self, cols: TradeCols<'_>) {
		self.monotonic_seq.append_slice(cols.monotonic_seq);
		self.price_raw.append_slice(cols.price);
		self.qty_raw.append_slice(cols.qty);
		// `Ts` and `Side` are newtypes over the stored representation, and reading them as one would
		// take the `unsafe` this pass is spending nothing on.
		for &t in cols.exec() {
			self.ts_venue_exec.append_value(t.as_nanos());
		}
		match cols.ts.send {
			Some(send) =>
				for &t in send {
					self.ts_venue_send.append_value(t.as_nanos());
				},
			None => self.ts_venue_send.append_nulls(cols.len()),
		}
		match cols.ts.recv {
			// One reception reading covers the whole run: the relay read its clock once.
			Some(r) => self.ts_local_recv.append_value_n(r.last.as_nanos(), cols.len()),
			None => self.ts_local_recv.append_nulls(cols.len()),
		}
		for &s in cols.side {
			self.side.append_value(side_u8(s));
		}
	}
}

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

	fn builders(rows: usize) -> BookDeltaBuilders {
		BookDeltaBuilders {
			ts_venue_exec: arrow::array::Int64Builder::with_capacity(rows),
			ts_local_recv: arrow::array::Int64Builder::with_capacity(rows),
			monotonic_seq: arrow::array::UInt64Builder::with_capacity(rows),
			kind: arrow::array::UInt8Builder::with_capacity(rows),
			side: arrow::array::UInt8Builder::with_capacity(rows),
			price_raw: arrow::array::Int32Builder::with_capacity(rows),
			qty_raw: arrow::array::UInt32Builder::with_capacity(rows),
		}
	}

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

	fn append(&self, b: &mut BookDeltaBuilders, meta: PrecisionPriceQty) {
		assert_eq!(self.prec, meta, "a level's precision differs from the lane's");
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

	fn decode(batch: &RecordBatch, file_schema: &Schema) -> Vec<Self> {
		let prec = prec_from_schema(file_schema);
		let exec = col::<Int64Array>(batch, "ts_venue_exec");
		let recv = col::<Int64Array>(batch, "ts_local_recv");
		let monotonic = col::<UInt64Array>(batch, "monotonic_seq");
		let kind = col::<UInt8Array>(batch, "kind");
		let side = col::<UInt8Array>(batch, "side");
		let price = col::<Int32Array>(batch, "price_raw");
		let qty = col::<UInt32Array>(batch, "qty_raw");
		(0..batch.num_rows())
			.map(|i| BookDelta {
				prec,
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

	fn file_sig(schema: &Schema) -> Option<FileSig> {
		prec_sig(schema)
	}

	fn approx_bytes(&self) -> usize {
		40
	}
}

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

	fn builders(rows: usize) -> BookSnapshotBuilders {
		BookSnapshotBuilders {
			ts_venue_exec: arrow::array::Int64Builder::with_capacity(rows),
			ts_local_recv: arrow::array::Int64Builder::with_capacity(rows),
			monotonic_seq: arrow::array::UInt64Builder::with_capacity(rows),
			bid_prices: arrow::array::ListBuilder::with_capacity(arrow::array::Int32Builder::new(), rows),
			bid_qtys: arrow::array::ListBuilder::with_capacity(arrow::array::UInt32Builder::new(), rows),
			ask_prices: arrow::array::ListBuilder::with_capacity(arrow::array::Int32Builder::new(), rows),
			ask_qtys: arrow::array::ListBuilder::with_capacity(arrow::array::UInt32Builder::new(), rows),
		}
	}

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
		let mut bid_prices = col_i32_list(batch, "bid_prices");
		let mut bid_qtys = col_u32_list(batch, "bid_qtys");
		let mut ask_prices = col_i32_list(batch, "ask_prices");
		let mut ask_qtys = col_u32_list(batch, "ask_qtys");
		// The columns are consumed row by row and never read again, so each level vector moves into
		// its snapshot; cloning built every one of them twice, at 200 levels a side.
		(0..batch.num_rows())
			.map(|i| BookSnapshot {
				ts_venue_exec: Ts::from_nanos(exec.value(i)),
				ts_local_recv: Ts::from_nanos(recv.value(i)),
				monotonic_seq: monotonic.value(i),
				bid_prices: std::mem::take(&mut bid_prices[i]),
				bid_qtys: std::mem::take(&mut bid_qtys[i]),
				ask_prices: std::mem::take(&mut ask_prices[i]),
				ask_qtys: std::mem::take(&mut ask_qtys[i]),
			})
			.collect()
	}

	fn file_sig(schema: &Schema) -> Option<FileSig> {
		prec_sig(schema)
	}

	fn approx_bytes(&self) -> usize {
		32 + 8 * (self.bid_prices.len() + self.ask_prices.len())
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
			values.values()[start..end].to_vec()
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
			values.values()[start..end].to_vec()
		})
		.collect()
}

// DAG impls for this crate's own row types; the core out-types carry theirs in `trading_data_core`,
// where orphan rules put them.

impl Glance for Oi {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "oi {}", v_utils::LargeNumber::new(self.oi))
	}
}

impl Flat for Mc {
	/// The rank slot: a coin the provider left unranked has a market cap all the same.
	const ABSENTABLE: bool = true;
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.market_cap, self.rank.map_or(f64::NAN, f64::from)]);
		true
	}
}

impl Bump for Mc {
	fn bump(self, slot: usize, h: f64) -> (Self, f64) {
		// a rank is a label, not a quantity: its column stays NaN rather than a fabricated zero.
		match slot {
			0 => (
				Self {
					market_cap: self.market_cap + h,
					..self
				},
				h,
			),
			1 => (self, 0.0),
			s => panic!("Mc has two slots, bumped {s}"),
		}
	}
}

impl Glance for Mc {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "mc {}", v_utils::LargeNumber::new(self.market_cap))
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

	/// What the lane *is*, for everything that reads the graph rather than writes it. The type keeps
	/// the `…Root` spelling it shares with [`OiRoot`].
	const NAME: &'static str = "MarketCap";
}
slice_nudge!(McRoot, Mc);

// a lane row is a reading that was published: absent only by not being in the batch.
always_present!(Oi, Mc);
