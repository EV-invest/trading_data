//! Central replay: one graph, one router, two feeds. [`Replay`] is the backtest feed over a
//! catalog; [`Live`] is the live feed over push handles, teeing into the same Feather lanes a
//! backtest later reads.
//!
//! They are not the same run and are not meant to be. [`Replay`] batches on a configured
//! [`ReadClock`] so a backtest outruns the market it replays, which means it hands the graph as one
//! step what arrived as several. [`Live`] has no such knob — it folds whatever is buffered when it
//! gets there, and never waits for more.
//!
//! Every event is keyed on an [`Arrival`]. For [`Replay`] that's the recorded reception, or a
//! latency-simulation of the venue axis for historic (`None`) rows. For [`Live`] it is stamped at
//! **ingest** — a single point, on the consumer thread — so it is monotonic without cross-thread
//! races, and everything currently buffered is a complete prefix (every future event gets a larger
//! stamp). That is what lets [`Live`] emit incrementally and drop consumed rows:
//! **bounded memory, no end-of-stream buffering, no quiet-lane stall.**
//!
//! The book lane is *our own recollection*, not the venue's story: [`Live`] runs every venue update
//! through a `ShadowBook`, which reconciles gaps and snapshot disagreements once, at ingest, and
//! emits checkpoints on our cadence. Venue snapshots are consumed, never stored.

use std::sync::{
	Arc,
	mpsc::{Receiver, Sender, channel},
};

use trading_data_core::{
	Arrival, BatchTrades, Book, BookDelta, BookShape, BookUpdate, Exact, ExchangeName, Local, PrecisionPriceQty, ReadClock, ShadowBook, Symbol, TradeBuf, TradeCols, Ts, Venue,
};
use trading_data_dag::Rewound;
use v_utils::distributions::LatencyConfig;

use crate::{
	catalog::{Catalog, LaneKey, Pending},
	clock::Clock,
	feather::{Feather, RotationPolicy},
	read::{lane_prec, pick_anchor, read_book_deltas, read_book_snapshots, read_mc, read_oi, read_trades, snapshot_shape},
	row::{BookSnapshot, Mc, Oi, Row as _, Trade},
};

/// How often we write a book checkpoint of our own. Bounds how far back a replay must read; drift
/// is not the concern, since the delta lane we write is gapless by construction.
const CHECKPOINT_CADENCE: Exact = Exact::from_nanos(60_000_000_000);

/// The source lanes a graph may require. `Debug`/`Hash` back the deterministic per-lane latency
/// seed; the router reads only the lanes in the graph's dep tree. Deltas and anchors are separate
/// kinds: naming one must not load *and accumulate* the other, since an un-drained lane in [`Live`]
/// grows without bound.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LaneKind {
	Trades,
	BookDeltas,
	BookAnchors,
	Oi,
	Mc,
}

pub struct Lanes<'a> {
	pub arrival: Arrival,
	pub ts_venue: Ts<Venue>,
	pub trades: TradeCols<'a>,
	pub deltas: &'a [BookDelta],
	pub anchor: Option<&'a BookShape>,
	pub oi: &'a [Oi],
	pub mc: &'a [Mc],
}

/// One emission, and the past it stands on. Two values rather than one because [`Feed::next`] ties
/// its [`Lanes`] to `&mut self`, so the feed cannot be read alongside the batch it just handed out.
pub struct Step<'a> {
	pub lanes: Lanes<'a>,
	pub past: Past<'a>,
}

/// Everything already delivered on the lanes something may be replayed out of, and nothing else. The
/// slices are truncated where the weave stands, so a row it has not emitted is not *representable*
/// here — which is the whole of what keeps a rewind from reading its own future.
#[derive(Clone, Copy, Default)]
pub struct Past<'a> {
	deltas: &'a [BookDelta],
	anchors: &'a [BookShape],
}

/// A source of woven lanes. `None` = exhausted (end-of-range for [`Replay`]; all senders dropped
/// and drained for [`Live`]).
pub trait Feed {
	fn next(&mut self) -> Option<Lanes<'_>>;

	/// The same emission, carrying the past an anchored node is replayed out of. Empty by default:
	/// a feed that has not recorded a past cannot seek one, and a driver holding such a feed drives
	/// [`tick`](trading_data_dag::Rewound) rather than `tick_rewind` — the node is then pinned awake
	/// and never asks.
	fn step(&mut self) -> Option<Step<'_>> {
		self.next().map(|lanes| Step { lanes, past: Past::default() })
	}
}

trait LaneBuf {
	fn len(&self) -> usize;
	fn drain_prefix(&mut self, n: usize);
}

impl<T> LaneBuf for Vec<T> {
	fn len(&self) -> usize {
		self.len()
	}

	fn drain_prefix(&mut self, n: usize) {
		self.drain(..n);
	}
}

impl LaneBuf for TradeBuf {
	fn len(&self) -> usize {
		self.len()
	}

	fn drain_prefix(&mut self, n: usize) {
		self.drain_prefix(n);
	}
}

/// One arrival-sorted lane: weave keys in `ts`, data in `buf`, consumed up to `cur`. The key lives
/// beside the datum, not in it — it is an ordering device, not a property of the datum.
struct Lane<C> {
	ts: Vec<Arrival>,
	buf: C,
	cur: usize,
}

impl<C: Default> Default for Lane<C> {
	fn default() -> Self {
		Self {
			ts: Vec::new(),
			buf: C::default(),
			cur: 0,
		}
	}
}

impl<C: LaneBuf> Lane<C> {
	fn head(&self) -> Option<Arrival> {
		self.ts.get(self.cur).copied()
	}

	fn is_empty(&self) -> bool {
		self.cur >= self.ts.len()
	}

	/// Claim `n` weave slots at `ts`; the caller has already appended `n` elements to `buf`.
	fn key(&mut self, ts: Arrival, n: usize) {
		debug_assert!(self.ts.last().is_none_or(|&last| ts >= last), "lane arrivals must be non-decreasing");
		self.ts.resize(self.ts.len() + n, ts);
		debug_assert_eq!(self.ts.len(), self.buf.len(), "lane keys and lane data must stay in step");
	}

	/// End index (exclusive) of the maximal run from `cur` whose keys stay `<= bound`.
	fn run_end(&self, bound: Arrival) -> usize {
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
			self.buf.drain_prefix(self.cur);
			self.cur = 0;
		}
	}
}

impl Lane<Vec<Oi>> {
	fn push(&mut self, ts: Arrival, row: Oi) {
		self.buf.push(row);
		self.key(ts, 1);
	}
}

impl Lane<Vec<Mc>> {
	fn push(&mut self, ts: Arrival, row: Mc) {
		self.buf.push(row);
		self.key(ts, 1);
	}
}

impl Lane<Vec<BookShape>> {
	fn push(&mut self, ts: Arrival, row: BookShape) {
		self.buf.push(row);
		self.key(ts, 1);
	}
}

impl Lane<TradeBuf> {
	fn extend(&mut self, ts: Arrival, cols: TradeCols<'_>) {
		self.buf.extend(cols);
		self.key(ts, cols.len());
	}
}

impl Lane<Vec<BookDelta>> {
	fn extend(&mut self, ts: Arrival, levels: &[BookDelta]) {
		self.buf.extend_from_slice(levels);
		self.key(ts, levels.len());
	}
}

#[derive(Default)]
struct Weaver {
	trades: Lane<TradeBuf>,
	deltas: Lane<Vec<BookDelta>>,
	anchors: Lane<Vec<BookShape>>,
	oi: Lane<Vec<Oi>>,
	mc: Lane<Vec<Mc>>,
	/// Not the epoch: a pre-start book checkpoint is keyed at [`Arrival::MIN`] on purpose (state
	/// carried into the range sorts before everything), so an epoch-defaulted watermark would read
	/// the very first emission as out-of-order.
	prev_emit: Arrival = Arrival::MIN,
	clock: ReadClock,
}

impl Weaver {
	fn new(clock: ReadClock) -> Self {
		Self { clock, ..Self::default() }
	}

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

	/// The pre-tick book cursors are read here and nowhere else: taking them after the winner advances
	/// would hand a rewind this tick's own batch to fold a second time, and a book fold being
	/// idempotent is precisely what would make that silent.
	fn step(&mut self) -> Option<Step<'_>> {
		let (past_d, past_a) = (self.deltas.cur, self.anchors.cur);
		let heads = [self.trades.head(), self.deltas.head(), self.anchors.head(), self.oi.head(), self.mc.head()];
		let winner = (0..heads.len()).filter(|&i| heads[i].is_some()).min_by_key(|&i| heads[i].expect("filtered to Some"))?;
		let win_ts = heads[winner].expect("winner has a head");
		let bound = (0..heads.len()).filter(|&i| i != winner).filter_map(|i| heads[i]).min().unwrap_or(Arrival::from_nanos(i64::MAX));
		assert!(win_ts >= self.prev_emit, "weaver emitted out of arrival order: {win_ts:?} < {:?}", self.prev_emit);

		// The cell is `win_ts`'s own, not one measured off the last emission: a run must group the same
		// way whether the feed was busy or idle before it.
		let bound = bound.min(self.clock.cell_end(win_ts));

		let (mut t, mut d, mut a, mut o, mut m) = ((0, 0), (0, 0), (0, 0), (0, 0), (0, 0));
		let ts_venue = match winner {
			0 => {
				t = (self.trades.cur, self.trades.run_end(bound));
				self.trades.cur = t.1;
				self.prev_emit = self.trades.ts[t.1 - 1];
				self.trades.buf.exec_at(t.1 - 1)
			}
			1 => {
				d = (self.deltas.cur, self.deltas.run_end(bound));
				self.deltas.cur = d.1;
				self.prev_emit = self.deltas.ts[d.1 - 1];
				self.deltas.buf[d.1 - 1].ts_venue_exec
			}
			2 => {
				a = (self.anchors.cur, self.anchors.run_end(bound));
				self.anchors.cur = a.1;
				self.prev_emit = self.anchors.ts[a.1 - 1];
				self.anchors.buf[a.1 - 1].ts.venue_exec.last
			}
			3 => {
				o = (self.oi.cur, self.oi.run_end(bound));
				self.oi.cur = o.1;
				self.prev_emit = self.oi.ts[o.1 - 1];
				self.oi.buf[o.1 - 1].ts_venue_exec
			}
			_ => {
				m = (self.mc.cur, self.mc.run_end(bound));
				self.mc.cur = m.1;
				self.prev_emit = self.mc.ts[m.1 - 1];
				mc_axis(self.mc.buf[m.1 - 1].ts_local_exec)
			}
		};

		Some(Step {
			lanes: Lanes {
				arrival: self.prev_emit,
				ts_venue,
				trades: self.trades.buf.cols(t.0..t.1),
				deltas: &self.deltas.buf[d.0..d.1],
				anchor: self.anchors.buf[a.0..a.1].last(),
				oi: &self.oi.buf[o.0..o.1],
				mc: &self.mc.buf[m.0..m.1],
			},
			past: Past {
				deltas: &self.deltas.buf[..past_d],
				anchors: &self.anchors.buf[..past_a],
			},
		})
	}

	fn next(&mut self) -> Option<Lanes<'_>> {
		self.step().map(|s| s.lanes)
	}
}

/// The weave key for a stored row: the recorded reception when we were there, else a simulated one.
fn effective<A>(recv: Option<Ts<Local>>, axis: Ts<A>, sampler: &mut Option<v_utils::distributions::LatencySampler>) -> Arrival {
	match recv {
		Some(t) => Arrival::from_nanos(t.as_nanos()),
		None => Arrival::from_nanos(sampler.as_mut().expect("historic lane needs a latency sampler").arrival(axis.as_nanos())),
	}
}

/// Market cap is daily-resolution, so venue/local skew is orders of magnitude below the sampling
/// interval and cannot change which rows a range selects, nor where the lane weaves. Named, not
/// implicit, so the one place this crate crosses actors on a time axis is greppable.
fn mc_axis<A, B>(t: Ts<A>) -> Ts<B> {
	Ts::from_nanos(t.as_nanos())
}

/// Backtest feed: fills the weaver from the catalog's lanes, then emits their arrival-ordered
/// merge. Historic rows (no recorded reception) get a simulated arrival via `latency`.
///
// ponytail: eager-loads the whole [start,end] range into memory. Fine for day-scale ranges (a day
// of trades is a few MB) and it streams from disk one file at a time while filling. For very large
// ranges, chunk the range across successive `Replay`s, or make the fill lazy (hold the `LaneReader`s
// and refill each lane to a lookahead watermark) — same `Weaver` core, per-lane exhaustion is known.
pub struct Replay {
	weaver: Weaver,
}

impl Replay {
	/// Each parameter is a distinct thing the caller has to state; a config struct would only rename
	/// the same eight.
	#[allow(clippy::too_many_arguments)]
	pub fn new(catalog: &Catalog, exchange: ExchangeName, symbol: Symbol, start: Ts<Venue>, end: Ts<Venue>, lanes: &[LaneKind], latency: LatencyConfig, read: ReadClock) -> Self {
		let mut weaver = Weaver::new(read);
		let sampler = |lane: LaneKind| Some(latency.sampler(&format!("{lane:?}:{symbol}")));
		// No file under any of `keys` leaves the lane's precision unknown, and a default would scale
		// every raw price and qty by 1 — wrong by whole orders of magnitude, on output that still
		// looks like data. It means the catalog is missing a lane the caller asked to replay.
		let prec = |keys: &[LaneKey]| {
			lane_prec(catalog, keys)
				.expect("read lane precision")
				.unwrap_or_else(|| panic!("replay of {symbol} on {exchange} needs {keys:?}, and the catalog holds no file for any of them"))
		};

		// A graph naming both book roots would otherwise load each lane twice.
		let mut seen = Vec::new();
		for &lane in lanes {
			if seen.contains(&lane) {
				continue;
			}
			seen.push(lane);
			match lane {
				LaneKind::Trades => {
					weaver.trades.buf.prec = prec(&[LaneKey::Trades { exchange, symbol }]);
					let mut s = sampler(lane);
					for t in read_trades(catalog, exchange, symbol, start, end).expect("open trades lane") {
						let ts = effective(t.ts_local_recv, t.ts_venue_exec, &mut s);
						weaver.trades.buf.push(t.ts_venue_exec, t.ts_venue_send, t.ts_local_recv, t.monotonic_seq, t.side, t.price, t.qty);
						weaver.trades.key(ts, 1);
					}
				}
				LaneKind::Oi => {
					let mut s = sampler(lane);
					for o in read_oi(catalog, exchange, symbol, start, end).expect("open oi lane") {
						let ts = effective(o.ts_local_recv, o.ts_venue_exec, &mut s);
						weaver.oi.push(ts, o);
					}
				}
				LaneKind::Mc => {
					let mut s = sampler(lane);
					for m in read_mc(catalog, symbol.pair.base(), mc_axis(start), mc_axis(end)).expect("open mc lane") {
						// This lane's axis is already ours, so it is its own reception reading.
						let ts = effective(Some(m.ts_local_exec), m.ts_local_exec, &mut s);
						weaver.mc.push(ts, m);
					}
				}
				LaneKind::BookDeltas => {
					// named for the assertion it carries: no file under either key means the catalog is
					// missing a lane the caller asked to replay, and the rows themselves carry their own.
					let _ = prec(&[LaneKey::BookDeltas { exchange, symbol }, LaneKey::BookSnapshots { exchange, symbol }]);
					let mut s = sampler(lane);
					for d in read_book_deltas(catalog, exchange, symbol, start, end).expect("open delta lane") {
						let ts = effective(Some(d.ts_local_recv), d.ts_venue_exec, &mut s);
						weaver.deltas.buf.push(d);
						weaver.deltas.key(ts, 1);
					}
				}
				LaneKind::BookAnchors => {
					let p = prec(&[LaneKey::BookSnapshots { exchange, symbol }, LaneKey::BookDeltas { exchange, symbol }]);
					// The pre-start checkpoint is state carried into the range, not an event that
					// arrived in it — it has no arrival of its own, and must sort before everything.
					if let Some(shape) = pick_anchor(catalog, exchange, symbol, start).expect("pick anchor") {
						weaver.anchors.push(Arrival::MIN, shape);
					}
					let mut s = sampler(lane);
					for snap in read_book_snapshots(catalog, exchange, symbol, start, end).expect("open snapshot lane") {
						let ts = effective(Some(snap.ts_local_recv), snap.ts_venue_exec, &mut s);
						weaver.anchors.push(ts, snapshot_shape(&snap, p));
					}
				}
			}
		}
		Self { weaver }
	}
}

impl Feed for Replay {
	fn next(&mut self) -> Option<Lanes<'_>> {
		self.weaver.next()
	}

	fn step(&mut self) -> Option<Step<'_>> {
		self.weaver.step()
	}
}

/// The seek a backtest has and a live feed does not: the whole recorded past is already in memory,
/// so a book that slept is fetched back rather than waited for.
///
/// Which resume is taken is a cost question and nothing more — both land on the same book, because a
/// level is absolute in `(side, price)` and the newer row always wins.
impl Rewound<Book> for Past<'_> {
	fn rewind(&mut self, b: &mut Book) {
		let Some(need) = self.deltas.last() else { return };
		if b.seq() == Some(need.monotonic_seq) {
			return; // awake last tick: one integer compare, which is the steady state
		}
		// where our own cursor resumes
		let here = b.seq().map(|s| self.deltas.partition_point(|d| d.monotonic_seq <= s));
		// and where the newest delivered checkpoint does. Strictly-before, so a tie re-folds a row the
		// checkpoint already holds — free — where skipping one is not; and clamped into the stream, so
		// either resume ends on `need` and leaves the same cursor behind it.
		let seek = self
			.anchors
			.last()
			.map(|a| (a, self.deltas.partition_point(|d| d.ts_local_recv < a.ts.local_recv.last).min(self.deltas.len() - 1)));

		let rows = |from: usize| self.deltas.len() - from;
		match (here, seek) {
			(Some(h), Some((a, k))) if a.bids.len() + a.asks.len() + rows(k) < rows(h) => b.rewind(Some(a), &self.deltas[k..]),
			(Some(h), _) => b.rewind(None, &self.deltas[h..]),
			(None, Some((a, k))) => b.rewind(Some(a), &self.deltas[k..]),
			// no place in the chain and no checkpoint delivered yet: there is no past to be replayed to,
			// which is the same thing an unseeded book already says for itself.
			(None, None) => return,
		}
		// the tripwire for everything above: a book left anywhere but on the last delivered row makes
		// the next `Book::step` read `Reach::Gone`, and a `Gone` in a rewound backtest means the past
		// lied about where it ended.
		debug_assert_eq!(b.seq(), Some(need.monotonic_seq), "a rewind must stand the book on the last row the past holds");
	}
}

/// A live event pushed through a [`Live`] handle. Crate-private: per-*message* dispatch is
/// acceptable at network rates and preserves total arrival order across lanes, which per-lane
/// channels can't. Carries no arrival time — [`Live::ingest`] stamps it on the consumer thread.
enum LiveEvt {
	Trades(BatchTrades),
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
	/// A whole venue message, whole: one clock read, one flush check and one send for the batch,
	/// instead of one of each per trade.
	pub fn trades(&self, b: BatchTrades) {
		self.push(LiveEvt::Trades(b));
	}

	pub fn oi(&self, o: Oi) {
		self.push(LiveEvt::Oi(o));
	}

	pub fn mc(&self, m: Mc) {
		self.push(LiveEvt::Mc(m));
	}

	pub fn book(&self, u: BookUpdate) {
		self.push(LiveEvt::Book(u));
	}

	/// The receiver is gone only if [`Live`] was dropped while a sink is still held: the session is
	/// over and every further event is being taken off the wire into nothing. A pump that keeps
	/// running there costs bandwidth and silently records nothing, so it comes down with the feed.
	/// Orderly shutdown is to drop the sinks first — that is what disconnects the channel.
	fn push(&self, e: LiveEvt) {
		assert!(self.tx.send(e).is_ok(), "Live was dropped while a Sink is still live; drop the sinks first");
	}
}

/// Optional record tee: the same Feather lanes a backtest later reads. `writer` is declared last so
/// it outlives the feathers that feed it.
struct Record {
	trades: Feather<Trade>,
	snapshots: Feather<BookSnapshot>,
	deltas: Feather<BookDelta>,
	oi: Feather<Oi>,
	mc: Feather<Mc>,
	writer: Writer,
}

/// The parquet encode, moved off the thread holding the wire. A full `BookDelta` batch is 256 MiB
/// by its own rotation policy, which is the better part of a second of ZSTD — spent on the ingest
/// thread that is a gap in the recording, not merely a slow write.
///
/// Not `thread::scope`: a scope would have to wrap every caller's session, and `Live` is handed out
/// as a value. Owning the handle and joining it in `Drop` bounds the thread just as tightly — it
/// cannot outlive the recording, and nothing here is detached.
struct Writer {
	tx: Option<Sender<Pending>>,
	thread: Option<std::thread::JoinHandle<()>>,
}

impl Writer {
	fn new(catalog: Catalog) -> Self {
		let (tx, rx) = channel::<Pending>();
		let thread = std::thread::Builder::new()
			.name("td-catalog-writer".into())
			.spawn(move || {
				for p in rx {
					catalog.write(p).expect("catalog write");
				}
			})
			.expect("spawn the catalog writer");
		Self { tx: Some(tx), thread: Some(thread) }
	}

	fn send(&self, pending: Option<Pending>) {
		if let Some(p) = pending {
			self.tx
				.as_ref()
				.expect("a lane is only taken while the writer runs")
				.send(p)
				.expect("the writer thread outlives every send");
		}
	}

	/// Blocks until everything sent is on disk. Only ever called once the session is over, where
	/// there is nothing left to keep off this thread — and a reader opening the catalog right after
	/// must not race the tail.
	fn finish(&mut self) {
		drop(self.tx.take());
		if let Some(t) = self.thread.take()
			&& t.join().is_err()
			&& !std::thread::panicking()
		{
			panic!("the catalog writer died with a lane in flight");
		}
	}
}

impl Drop for Writer {
	fn drop(&mut self) {
		self.finish();
	}
}

/// Live feed. [`Feed::next`] drains what's currently available (blocking for the first event),
/// stamps each message with a monotonic arrival time, weaves one step, and drops consumed rows —
/// memory stays bounded to the un-emitted window regardless of session length. Recording tees
/// incrementally. `None` once every sink is dropped and the buffer is drained.
///
/// **No configured [`ReadClock`]** — [`ReadClock::ALL`], which is not a rate. Whatever is buffered
/// by the time we are ready to fold is folded together, as aggressively as any backtest would; what
/// live never does is *wait* to collect more, and an absolute cell of any finite size is a wait. So
/// how these steps group is a function of how busy the consumer was, not of a knob, and a replay of
/// this recording will not reproduce them — it will contain the same events, grouped however its
/// own read clock says.
pub struct Live {
	rx: Receiver<LiveEvt>,
	// dropped on first `next` so the channel disconnects once external sinks drop; also gates
	// `sink()` to before-consume.
	tx: Option<Sender<LiveEvt>>,
	clock: Arc<dyn Clock>,
	/// Last key issued; strictly increasing (`max(clock, last+1)`), so events are globally uniquely
	/// ordered and the weave is unambiguous.
	last_stamp: Arrival,
	prec: PrecisionPriceQty,
	trade_seq: u64,
	/// Scratch for one message's trade run — extended into the weaver and the tee in one pass.
	staging: TradeBuf,
	shadow: ShadowBook,
	record: Option<Record>,
	weaver: Weaver,
	disconnected: bool,
}

impl Live {
	pub fn new(catalog: Catalog, exchange: ExchangeName, symbol: Symbol, prec: PrecisionPriceQty, record: bool, clock: Arc<dyn Clock>) -> Self {
		let (tx, rx) = channel();
		let record = record.then(|| {
			// A recording is written once and read many times, but not at the price of stalling the
			// thread it is recorded from: level 1 encodes several times faster for a few percent of
			// size, and the writer thread hides even that.
			let tee = |p: RotationPolicy| RotationPolicy { zstd_level: 1, ..p };
			Record {
				trades: Feather::<Trade>::new(exchange, symbol, prec, tee(Trade::POLICY)),
				snapshots: Feather::<BookSnapshot>::new(exchange, symbol, prec, tee(BookSnapshot::POLICY)),
				deltas: Feather::<BookDelta>::new(exchange, symbol, prec, tee(BookDelta::POLICY)),
				oi: Feather::<Oi>::new(exchange, symbol, tee(Oi::POLICY)),
				mc: Feather::<Mc>::new(symbol.pair.base(), tee(Mc::POLICY)),
				writer: Writer::new(catalog),
			}
		});
		let mut weaver = Weaver::new(ReadClock::ALL);
		weaver.trades.buf.prec = prec;
		Self {
			rx,
			tx: Some(tx),
			clock,
			last_stamp: Arrival::MIN,
			prec,
			trade_seq: 0,
			staging: TradeBuf::new(prec),
			shadow: ShadowBook::new(prec, CHECKPOINT_CADENCE),
			record,
			weaver,
			disconnected: false,
		}
	}

	/// A cloneable push handle. Take all sinks before consuming; the first [`Feed::next`] drops the
	/// feed's own sender so the channel can disconnect once external sinks are gone.
	pub fn sink(&self) -> Sink {
		Sink {
			tx: self.tx.as_ref().expect("sinks must be taken before the feed is consumed").clone(),
		}
	}

	/// Strictly-increasing weave key (single-threaded, so no atomics): every buffered event is
	/// `<= last_stamp`, every future event `> last_stamp`.
	fn stamp(&mut self) -> Arrival {
		let t = Arrival::from_nanos(self.clock.now_ns().max(self.last_stamp.as_nanos() + 1));
		self.last_stamp = t;
		t
	}

	/// The recorded reception reading. It is the weave key rather than a second, raw clock read:
	/// replay must reconstruct the same order from disk, and a raw read can tie where the key
	/// cannot. The `+1` that monotonisation can introduce is far below wire-latency resolution.
	fn recv_of(a: Arrival) -> Ts<Local> {
		Ts::from_nanos(a.as_nanos())
	}

	fn ingest(&mut self, evt: LiveEvt) {
		let ts = self.stamp();
		match evt {
			LiveEvt::Trades(b) => {
				assert_eq!(b.prec(), self.prec, "trade batch precision differs from the lane's");
				let recv = Self::recv_of(ts);
				self.staging.clear();
				for t in b.iter() {
					self.trade_seq += 1;
					self.staging.push(t.time, t.sent, Some(recv), self.trade_seq, t.side, t.price, t.qty);
				}
				let cols = self.staging.cols(0..self.staging.len());
				if let Some(r) = &mut self.record {
					r.trades.extend(cols);
					r.writer.send(r.trades.maybe_take());
				}
				self.weaver.trades.extend(ts, cols);
			}
			LiveEvt::Oi(mut o) => {
				o.ts_local_recv = Some(Self::recv_of(ts));
				if let Some(r) = &mut self.record {
					r.oi.push(o);
					r.writer.send(r.oi.maybe_take());
				}
				self.weaver.oi.push(ts, o);
			}
			LiveEvt::Mc(mut m) => {
				m.ts_local_exec = Self::recv_of(ts);
				if let Some(r) = &mut self.record {
					r.mc.push(m);
					r.writer.send(r.mc.maybe_take());
				}
				self.weaver.mc.push(ts, m);
			}
			LiveEvt::Book(u) => self.ingest_book(u, ts),
		}
	}

	/// The single book path — shared by weave and record, so they can never drift. The venue update
	/// goes through the shadow book; what leaves here is *our* frame, and then *our* checkpoint if
	/// the cadence has elapsed (after the fold, so the checkpoint includes it).
	fn ingest_book(&mut self, u: BookUpdate, ts: Arrival) {
		let recv = Self::recv_of(ts);
		if let Some(levels) = self.shadow.ingest(&u, recv) {
			if let Some(r) = &mut self.record {
				r.deltas.extend(levels);
				r.writer.send(r.deltas.maybe_take());
			}
			self.weaver.deltas.extend(ts, levels);
		}
		if let Some(shape) = self.shadow.checkpoint(recv) {
			if let Some(r) = &mut self.record {
				r.snapshots.push(BookSnapshot {
					ts_venue_exec: shape.ts.venue_exec.last,
					ts_local_recv: recv,
					monotonic_seq: 0,
					bid_prices: shape.bids.keys().copied().collect(),
					bid_qtys: shape.bids.values().copied().collect(),
					ask_prices: shape.asks.keys().copied().collect(),
					ask_qtys: shape.asks.values().copied().collect(),
				});
				r.writer.send(r.snapshots.maybe_take());
			}
			self.weaver.anchors.push(ts, shape);
		}
	}

	fn final_flush(&mut self) {
		if let Some(r) = &mut self.record {
			r.writer.send(r.trades.take());
			r.writer.send(r.snapshots.take());
			r.writer.send(r.deltas.take());
			r.writer.send(r.oi.take());
			r.writer.send(r.mc.take());
			// A drained feed is a catalog someone is about to read.
			r.writer.finish();
		}
	}
}

#[cfg(test)]
mod tests {
	use trading_data_core::{FrameKind, Precision, Side};

	use super::*;

	const PREC: PrecisionPriceQty = PrecisionPriceQty {
		price: Precision(2),
		qty: Precision(2),
	};

	fn delta(seq: u64, ns: i64) -> BookDelta {
		BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(ns),
			ts_local_recv: Ts::from_nanos(ns),
			monotonic_seq: seq,
			kind: FrameKind::Update,
			side: Side::Buy,
			price: 100,
			qty: seq as u32,
		}
	}

	/// I3, structurally. A rewind reading its own future is not a bug a [`Past`] can have: its slices
	/// are cut where the weave stands, so a row the feed has not emitted is not a value it can build.
	/// This is the one line of the design that a plausible-looking `len()` would silently break —
	/// `Replay` holds the whole day, so reading past the cursor still produces a well-formed book.
	#[test]
	fn a_past_stops_where_the_weave_stands() {
		let mut w = Weaver::new(ReadClock::EVENT);
		for (i, seq) in [1u64, 2, 3].into_iter().enumerate() {
			w.deltas.extend(Arrival::from_nanos(2 * i as i64 + 1), &[delta(seq, seq as i64)]);
		}
		for i in 0..2i64 {
			w.anchors.push(Arrival::from_nanos(2 * i + 2), BookShape { prec: PREC, ..Default::default() });
		}

		// what the past holds as each of the five interleaved arrivals is emitted
		let want: [(&[u64], usize); 5] = [(&[], 0), (&[1], 0), (&[1], 1), (&[1, 2], 1), (&[1, 2], 2)];
		for (n, (deltas, anchors)) in want.into_iter().enumerate() {
			let s = w.step().unwrap_or_else(|| panic!("five keyed rows are five steps, and step {n} is not one"));
			assert_eq!(s.past.deltas.iter().map(|d| d.monotonic_seq).collect::<Vec<_>>(), deltas, "step {n}");
			assert_eq!(s.past.anchors.len(), anchors, "step {n}");
		}
		assert!(w.step().is_none(), "the weave is drained");
	}
}

/// Draining to `None` is one way a session ends; every other one — an early break, a `?`, a panic
/// upstream — goes through here, and without it the un-rotated tail of each lane is on the floor.
/// A no-op after the drained path, since a `Feather` holding no rows writes nothing.
impl Drop for Live {
	fn drop(&mut self) {
		self.final_flush();
	}
}

impl Feed for Live {
	fn next(&mut self) -> Option<Lanes<'_>> {
		self.tx = None; // drop our own sender so the channel disconnects once external sinks are gone
		self.weaver.compact(); // reclaim the previous step's consumed rows (its borrow has ended)

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
		self.weaver.next()
	}
}
