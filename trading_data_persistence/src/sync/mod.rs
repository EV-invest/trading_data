//! Central replay: one graph, one router, two feeds. A [`Feed`] weaves the required source lanes
//! into a single arrival-ordered stream of same-type [`Batch`]es. [`Replay`] is the backtest feed
//! over a catalog; [`Live`] is the live feed over push handles, teeing into the same Feather lanes
//! a backtest later reads — so a live recording replays into the *identical* event stream.
//!
//! Effective (arrival) time of every event is `ts_init`. For [`Replay`] that's the recorded value,
//! or a latency-simulation of `ts_event` for historic (`None`) rows. For [`Live`], `ts_init` is
//! stamped at **ingest** — a single point, on the consumer thread — so it is monotonic without
//! cross-thread races, and everything currently buffered is a complete prefix (every future event
//! gets a larger stamp). That is what lets [`Live`] weave-and-emit incrementally and drop consumed
//! rows: **bounded memory, no end-of-stream buffering, no quiet-lane stall.**

use std::sync::{
	Arc,
	mpsc::{Receiver, Sender, channel},
};

use jiff::Timestamp;
use trading_data_core::{Asset, ExchangeName, PrecisionPriceQty, Side, Symbol};
use v_utils::distributions::LatencyConfig;

use crate::{
	book::{BookShape, BookUpdate},
	catalog::Catalog,
	clock::Clock,
	feather::Feather,
	read::{book_prec, pick_anchor, read_book_deltas, read_book_snapshots, read_mc, read_oi, read_trades},
	row::{BookDelta, BookSnapshot, Mc, Oi, Row as _, Trade, UnixNanos},
};

/// The source lanes a graph may require. `Debug`/`Hash` back the deterministic per-lane latency
/// seed; the router reads only the lanes in the graph's dep tree.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LaneKind {
	Trades,
	Book,
	Oi,
	Mc,
}

/// One arrival-ordered run of same-type events, borrowed straight from a lane buffer (zero-copy).
/// A `BookAnchor` is a single snapshot resync; everything else is a contiguous slice.
#[derive(Debug)]
pub enum Batch<'a> {
	Trades(&'a [Trade]),
	Book(&'a [BookDelta]),
	BookAnchor(&'a BookShape),
	Oi(&'a [Oi]),
	Mc(&'a [Mc]),
}

/// A source of woven batches. `None` = exhausted (end-of-range for [`Replay`]; all senders dropped
/// and drained for [`Live`]).
pub trait Feed {
	fn next_batch(&mut self) -> Option<Batch<'_>>;
}

fn ts_ns(t: Timestamp) -> i64 {
	t.as_nanosecond() as i64
}

/// One time-sorted lane: effective timestamps in `ts`, rows in `rows`, consumed up to `cur`.
struct Lane<T> {
	ts: Vec<i64>,
	rows: Vec<T>,
	cur: usize,
}

impl<T> Default for Lane<T> {
	fn default() -> Self {
		Self {
			ts: Vec::new(),
			rows: Vec::new(),
			cur: 0,
		}
	}
}

impl<T> Lane<T> {
	fn head(&self) -> Option<i64> {
		self.ts.get(self.cur).copied()
	}

	fn is_empty(&self) -> bool {
		self.cur >= self.ts.len()
	}

	fn push(&mut self, ts: i64, row: T) {
		debug_assert!(self.ts.last().is_none_or(|&last| ts >= last), "lane timestamps must be non-decreasing");
		self.ts.push(ts);
		self.rows.push(row);
	}

	/// End index (exclusive) of the maximal run from `cur` whose timestamps stay `<= bound`.
	fn run_end(&self, bound: i64) -> usize {
		let mut e = self.cur + 1;
		while e < self.ts.len() && self.ts[e] <= bound {
			e += 1;
		}
		e
	}

	/// Reclaim the emitted prefix so a streaming lane stays bounded to its un-emitted window.
	/// Amortized O(1)/element: only compacts once the consumed prefix dominates.
	fn compact(&mut self) {
		if self.cur > 0 && self.cur * 2 >= self.ts.len() {
			self.ts.drain(..self.cur);
			self.rows.drain(..self.cur);
			self.cur = 0;
		}
	}
}

/// Merges the filled lanes into one arrival-ordered batch stream. The book lane is two sub-lanes
/// (anchors + deltas) that weave independently by effective time.
#[derive(Default)]
struct Weaver {
	trades: Lane<Trade>,
	deltas: Lane<BookDelta>,
	anchors: Lane<BookShape>,
	oi: Lane<Oi>,
	mc: Lane<Mc>,
	prev_emit: i64,
}

impl Weaver {
	fn is_empty(&self) -> bool {
		self.trades.is_empty() && self.deltas.is_empty() && self.anchors.is_empty() && self.oi.is_empty() && self.mc.is_empty()
	}

	fn compact(&mut self) {
		self.trades.compact();
		self.deltas.compact();
		self.anchors.compact();
		self.oi.compact();
		self.mc.compact();
	}

	fn next_batch(&mut self) -> Option<Batch<'_>> {
		let heads = [self.trades.head(), self.deltas.head(), self.anchors.head(), self.oi.head(), self.mc.head()];
		let winner = (0..heads.len()).filter(|&i| heads[i].is_some()).min_by_key(|&i| heads[i].expect("filtered to Some"))?;
		let win_ts = heads[winner].expect("winner has a head");
		let bound = (0..heads.len()).filter(|&i| i != winner).filter_map(|i| heads[i]).min().unwrap_or(i64::MAX);
		assert!(win_ts >= self.prev_emit, "weaver emitted out of arrival order: {win_ts} < {}", self.prev_emit);

		Some(match winner {
			0 => {
				let (a, e) = (self.trades.cur, self.trades.run_end(bound));
				self.trades.cur = e;
				self.prev_emit = self.trades.ts[e - 1];
				Batch::Trades(&self.trades.rows[a..e])
			}
			1 => {
				let (a, e) = (self.deltas.cur, self.deltas.run_end(bound));
				self.deltas.cur = e;
				self.prev_emit = self.deltas.ts[e - 1];
				Batch::Book(&self.deltas.rows[a..e])
			}
			2 => {
				let a = self.anchors.cur;
				self.anchors.cur += 1;
				self.prev_emit = self.anchors.ts[a];
				Batch::BookAnchor(&self.anchors.rows[a])
			}
			3 => {
				let (a, e) = (self.oi.cur, self.oi.run_end(bound));
				self.oi.cur = e;
				self.prev_emit = self.oi.ts[e - 1];
				Batch::Oi(&self.oi.rows[a..e])
			}
			_ => {
				let (a, e) = (self.mc.cur, self.mc.run_end(bound));
				self.mc.cur = e;
				self.prev_emit = self.mc.ts[e - 1];
				Batch::Mc(&self.mc.rows[a..e])
			}
		})
	}
}

fn effective(ts_init: Option<UnixNanos>, ts_event: UnixNanos, sampler: &mut Option<v_utils::distributions::LatencySampler>) -> i64 {
	match ts_init {
		Some(t) => t,
		None => sampler.as_mut().expect("historic lane needs a latency sampler").arrival(ts_event),
	}
}

fn snapshot_shape(row: &BookSnapshot, prec: PrecisionPriceQty) -> BookShape {
	let ts_event = Timestamp::from_nanosecond(row.ts_event as i128).expect("stored ts in range");
	let ts_init = Timestamp::from_nanosecond(row.ts_init.unwrap_or(row.ts_event) as i128).expect("stored ts in range");
	BookShape {
		ts_event,
		ts_init,
		ts_last: ts_init,
		prec,
		bids: row.bid_prices.iter().copied().zip(row.bid_qtys.iter().copied()).collect(),
		asks: row.ask_prices.iter().copied().zip(row.ask_qtys.iter().copied()).collect(),
	}
}

/// Backtest feed: fills the weaver from the catalog's lanes, then emits their arrival-ordered
/// merge. Historic rows (`ts_init = None`) get simulated arrival via `latency`.
///
// ponytail: eager-loads the whole [start,end] range into memory. Fine for day-scale ranges (a day
// of trades is a few MB) and it streams from disk one file at a time while filling. For very large
// ranges, chunk the range across successive `Replay`s, or make the fill lazy (hold the `LaneReader`s
// and refill each lane to a lookahead watermark) — same `Weaver` core, per-lane exhaustion is known.
pub struct Replay {
	weaver: Weaver,
}

impl Replay {
	pub fn new(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: UnixNanos, end: UnixNanos, lanes: &[LaneKind], latency: LatencyConfig) -> Self {
		let mut weaver = Weaver::default();
		let sampler = |lane: LaneKind| Some(latency.sampler(&format!("{lane:?}:{symbol}")));

		for &lane in lanes {
			match lane {
				LaneKind::Trades => {
					let mut s = sampler(lane);
					for t in read_trades(catalog, exchange, symbol, start, end).expect("open trades lane") {
						let ts = effective(t.ts_init, t.ts_event, &mut s);
						weaver.trades.push(ts, t);
					}
				}
				LaneKind::Oi => {
					let mut s = sampler(lane);
					for o in read_oi(catalog, exchange, symbol, start, end).expect("open oi lane") {
						let ts = effective(o.ts_init, o.ts_event, &mut s);
						weaver.oi.push(ts, o);
					}
				}
				LaneKind::Mc => {
					let mut s = sampler(lane);
					let asset = Asset::new(symbol.pair.base().to_string());
					for m in read_mc(catalog, asset, start, end).expect("open mc lane") {
						let ts = effective(m.ts_init, m.ts_event, &mut s);
						weaver.mc.push(ts, m);
					}
				}
				LaneKind::Book => {
					let prec = book_prec(catalog, exchange, symbol).expect("read book prec").expect("book lane has files");
					// pre-start anchor first (sorts before the range by construction).
					if let Some(shape) = pick_anchor(catalog, exchange, symbol, start).expect("pick anchor") {
						weaver.anchors.push(ts_ns(shape.ts_event), shape);
					}
					let mut s = sampler(lane);
					for snap in read_book_snapshots(catalog, exchange, symbol, start, end).expect("open snapshot lane") {
						let ts = effective(snap.ts_init, snap.ts_event, &mut s);
						weaver.anchors.push(ts, snapshot_shape(&snap, prec));
					}
					for d in read_book_deltas(catalog, exchange, symbol, start, end).expect("open delta lane") {
						let ts = effective(d.ts_init, d.ts_event, &mut s);
						weaver.deltas.push(ts, d);
					}
				}
			}
		}
		Self { weaver }
	}
}

impl Feed for Replay {
	fn next_batch(&mut self) -> Option<Batch<'_>> {
		self.weaver.next_batch()
	}
}

/// A live event pushed through a [`Live`] handle. Crate-private: per-event dispatch is acceptable
/// at network rates and preserves total arrival order across lanes, which per-lane channels can't.
/// Carries no arrival time — [`Live::ingest`] stamps `ts_init` on the consumer thread.
enum LiveEvt {
	Trade(Trade),
	Book(BookUpdate),
	Oi(Oi),
	Mc(Mc),
}

/// Cloneable push handle: just enqueues. Arrival time is stamped at ingest (single-threaded), so
/// the handle is clock-free and cheap to clone across pump tasks.
#[derive(Clone)]
pub struct Sink {
	tx: Sender<LiveEvt>,
}

impl Sink {
	pub fn trade(&self, t: Trade) {
		self.tx.send(LiveEvt::Trade(t)).ok();
	}

	pub fn oi(&self, o: Oi) {
		self.tx.send(LiveEvt::Oi(o)).ok();
	}

	pub fn mc(&self, m: Mc) {
		self.tx.send(LiveEvt::Mc(m)).ok();
	}

	pub fn book(&self, u: BookUpdate) {
		self.tx.send(LiveEvt::Book(u)).ok();
	}
}

/// Optional record tee: the same Feather lanes a backtest later reads.
struct Record {
	catalog: Catalog,
	trades: Feather<Trade>,
	snapshots: Feather<BookSnapshot>,
	deltas: Feather<BookDelta>,
	oi: Feather<Oi>,
	mc: Feather<Mc>,
}

/// Live feed. `next_batch` drains what's currently available (blocking for the first event),
/// stamps each with a monotonic arrival time, weaves one batch, and drops consumed rows — memory
/// stays bounded to the un-emitted window regardless of session length. Recording tees each event
/// to disk incrementally, so a live recording replays into the identical event stream (the
/// live≡backtest invariant). `None` once every sink is dropped and the buffer is drained.
pub struct Live {
	rx: Receiver<LiveEvt>,
	// dropped on first `next_batch` so the channel disconnects once external sinks drop; also gates
	// `sink()` to before-consume.
	tx: Option<Sender<LiveEvt>>,
	clock: Arc<dyn Clock>,
	/// Last stamp issued; arrival time is strictly increasing (`max(clock, last+1)`), so events are
	/// globally uniquely ordered and the weave is unambiguous.
	last_stamp: i64,
	prec: PrecisionPriceQty,
	monotonic: u64,
	record: Option<Record>,
	weaver: Weaver,
	disconnected: bool,
}

impl Live {
	pub fn new(catalog: Catalog, exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, record: bool, clock: Arc<dyn Clock>) -> Self {
		let (tx, rx) = channel();
		let record = record.then(|| {
			let asset = Asset::new(symbol.pair.base().to_string());
			Record {
				trades: Feather::<Trade>::new(exchange, symbol, prec, Trade::POLICY),
				snapshots: Feather::<BookSnapshot>::new(exchange, symbol, prec, BookSnapshot::POLICY),
				deltas: Feather::<BookDelta>::new(exchange, symbol, prec, BookDelta::POLICY),
				oi: Feather::<Oi>::new(exchange, symbol, Oi::POLICY),
				mc: Feather::<Mc>::new(asset, Mc::POLICY),
				catalog,
			}
		});
		Self {
			rx,
			tx: Some(tx),
			clock,
			last_stamp: i64::MIN,
			prec,
			monotonic: 0,
			record,
			weaver: Weaver::default(),
			disconnected: false,
		}
	}

	/// A cloneable push handle. Take all sinks before consuming; the first `next_batch` drops the
	/// feed's own sender so the channel can disconnect once external sinks are gone.
	pub fn sink(&self) -> Sink {
		Sink {
			tx: self.tx.as_ref().expect("sinks must be taken before the feed is consumed").clone(),
		}
	}

	/// Strictly-increasing arrival stamp (single-threaded, so no atomics): every buffered event is
	/// `<= last_stamp`, every future event `> last_stamp`.
	fn stamp(&mut self) -> i64 {
		let t = self.clock.now_ns().max(self.last_stamp + 1);
		self.last_stamp = t;
		t
	}

	fn ingest(&mut self, evt: LiveEvt) {
		let ts = self.stamp();
		match evt {
			LiveEvt::Trade(mut t) => {
				t.ts_init = Some(ts);
				if let Some(r) = &mut self.record {
					r.trades.push(t);
					r.trades.maybe_flush(&r.catalog).expect("trade feather flush");
				}
				self.weaver.trades.push(ts, t);
			}
			LiveEvt::Oi(mut o) => {
				o.ts_init = Some(ts);
				if let Some(r) = &mut self.record {
					r.oi.push(o);
					r.oi.maybe_flush(&r.catalog).expect("oi feather flush");
				}
				self.weaver.oi.push(ts, o);
			}
			LiveEvt::Mc(mut m) => {
				m.ts_init = Some(ts);
				if let Some(r) = &mut self.record {
					r.mc.push(m);
					r.mc.maybe_flush(&r.catalog).expect("mc feather flush");
				}
				self.weaver.mc.push(ts, m);
			}
			LiveEvt::Book(u) => self.ingest_book(u, ts),
		}
	}

	/// The single book flattener — shared by weave and record, so they can never drift. A snapshot
	/// resyncs (row + `BookAnchor`); a batch-delta flattens to `BookDelta` rows once, those very
	/// rows both weaving and teeing. Arrival `ts` (the ingest stamp) is the shape/rows' `ts_init`.
	fn ingest_book(&mut self, u: BookUpdate, ts: i64) {
		let mut shape = u.shape().clone();
		let ev = ts_ns(shape.ts_event);
		shape.ts_init = Timestamp::from_nanosecond(ts as i128).expect("stamp in range");
		match &u {
			BookUpdate::Snapshot(_) => {
				self.monotonic += 1;
				if let Some(r) = &mut self.record {
					r.snapshots.push(BookSnapshot {
						ts_event: ev,
						ts_init: Some(ts),
						monotonic_seq: self.monotonic,
						bid_prices: shape.bids.keys().copied().collect(),
						bid_qtys: shape.bids.values().copied().collect(),
						ask_prices: shape.asks.keys().copied().collect(),
						ask_qtys: shape.asks.values().copied().collect(),
					});
					r.snapshots.maybe_flush(&r.catalog).expect("snapshot feather flush");
				}
				self.weaver.anchors.push(ts, shape);
			}
			BookUpdate::BatchDelta { gapped, .. } => {
				let (p_scale, q_scale) = (10f64.powi(self.prec.price as i32), 10f64.powi(self.prec.qty as i32));
				let mut rows = Vec::new();
				let mut push = |side: Side, price: i32, qty: u32, seq: &mut u64| {
					*seq += 1;
					rows.push(BookDelta {
						ts_event: ev,
						ts_init: Some(ts),
						monotonic_seq: *seq,
						gapped: *gapped,
						side,
						price: price as f64 / p_scale,
						qty: qty as f64 / q_scale,
					});
				};
				for (&p, &q) in &shape.bids {
					push(Side::Buy, p, q, &mut self.monotonic);
				}
				for (&p, &q) in &shape.asks {
					push(Side::Sell, p, q, &mut self.monotonic);
				}
				if let Some(r) = &mut self.record {
					for d in &rows {
						r.deltas.push(*d);
					}
					r.deltas.maybe_flush(&r.catalog).expect("delta feather flush");
				}
				for d in rows {
					self.weaver.deltas.push(ts, d);
				}
			}
		}
	}

	fn final_flush(&mut self) {
		if let Some(r) = &mut self.record {
			r.trades.flush(&r.catalog).expect("trade flush");
			r.snapshots.flush(&r.catalog).expect("snapshot flush");
			r.deltas.flush(&r.catalog).expect("delta flush");
			r.oi.flush(&r.catalog).expect("oi flush");
			r.mc.flush(&r.catalog).expect("mc flush");
		}
	}
}

impl Feed for Live {
	fn next_batch(&mut self) -> Option<Batch<'_>> {
		self.tx = None; // drop our own sender so the channel disconnects once external sinks are gone
		self.weaver.compact(); // reclaim the previous batch's consumed rows (its borrow has ended)

		// Top up until there's something to weave. Everything ingested is a complete prefix (future
		// stamps are larger), so the whole current buffer is safe to emit — no watermark needed.
		while self.weaver.is_empty() {
			if self.disconnected {
				self.final_flush();
				return None;
			}
			match self.rx.recv() {
				Ok(e) => self.ingest(e),
				Err(_) => self.disconnected = true,
			}
			while let Ok(e) = self.rx.try_recv() {
				self.ingest(e);
			}
		}
		self.weaver.next_batch()
	}
}
