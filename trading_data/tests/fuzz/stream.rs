//! Generation of one live session: the venue messages, the arrival stamps they are keyed at, and
//! the read clock a replay of the recording will group under.
//!
//! Two things are aimed rather than left to luck. **Boundaries**: a stamp is drawn at the exact
//! `cell_end` of the read clock's cell as often as it is drawn at random, so the transition between
//! two cells is hit rather than stumbled onto. **Ties**: `Live` keys every message at a distinct,
//! strictly increasing stamp, so the one tie the weaver can ever see is a single book message that
//! emits a delta run *and* our own checkpoint at the same stamp (`weaver.typ` §1.5). That needs the
//! message to land in a new checkpoint period, so a stamp gap of a whole cadence is one of the draws.

use trading_data::{
	Aggregate, BatchTrades, BookShape, BookUpdate, Clock, Exact, InnerTrade, Instrument, Local, Mc, Oi, Precision, PrecisionPriceQty, ReadClock, Side, Sink, Span, Symbol, Ts, Venue,
};

use crate::frng::Frng;

pub const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
/// `ShadowBook`'s cadence, mirrored so a generated stamp can be aimed a whole period ahead. Private
/// to persistence and rightly so — a fuzzer that needs the number states it as its own assumption,
/// and the tie-coverage check is what catches the two drifting apart.
const CHECKPOINT_CADENCE: i64 = 60_000_000_000;
/// Where a session's arrivals start. Deliberately not a multiple of any cell size below, so the very
/// first stamp already sits mid-cell and boundary aiming has somewhere to aim.
const EPOCH: i64 = 1_000_000_003;
/// Book prices live in this window either side of the mid, so a run keeps touching levels it already
/// holds — a book that only ever drains never exercises a delete against a live level.
const DEPTH: i32 = 8;
const MID: i32 = 10_000;

pub fn symbol() -> Symbol {
	Symbol::new("BTC-USDT".try_into().expect("static pair"), Instrument::Perp)
}

/// One venue message, held as the payload rather than as the built batch — a `BatchTrades` is not
/// `Clone`, and the counts an oracle wants are the payload's anyway.
pub enum Msg {
	Trades(Vec<InnerTrade>),
	Book(BookUpdate),
	Oi(f64),
	Mc(f64),
}

impl Msg {
	pub fn push(self, sink: &Sink) {
		match self {
			Msg::Trades(t) => sink.trades(BatchTrades::new(PREC, t, Ts::from_nanos(0))),
			Msg::Book(u) => sink.book(u),
			Msg::Oi(oi) => sink.oi(Oi {
				ts_venue_exec: Ts::from_nanos(1_000),
				ts_venue_send: None,
				ts_local_recv: None,
				oi,
			}),
			// `Live` overwrites the exec axis with its own ingest stamp: this lane's axis is already
			// ours, so its reception reading *is* its execution one.
			Msg::Mc(market_cap) => sink.mc(Mc {
				ts_local_exec: Ts::from_nanos(0),
				market_cap,
				rank: None,
			}),
		}
	}
}

pub struct Session {
	pub msgs: Vec<Msg>,
	/// One per message, strictly increasing — what the clock hands `Live` on ingest, and therefore
	/// the weave key and the recorded reception in one.
	pub stamps: Vec<i64>,
	/// What a replay of the recording groups under.
	pub read: ReadClock,
	/// The read clock's cell in nanoseconds, for the oracle's own arithmetic. `0` is
	/// [`ReadClock::EVENT`], `i64::MAX` is [`ReadClock::ALL`].
	pub cell: i64,
}

/// Hands out the generated stamps in order, then keeps stepping past the last one. `Live` reads its
/// clock exactly once per ingested message, so index and message stay in step — and the run past the
/// end only matters if that ever stops being true.
pub struct StampClock {
	stamps: Vec<i64>,
	next: std::sync::atomic::AtomicUsize,
}

impl StampClock {
	pub fn new(stamps: &[i64]) -> Self {
		Self {
			stamps: stamps.to_vec(),
			next: std::sync::atomic::AtomicUsize::new(0),
		}
	}
}

impl Clock for StampClock {
	fn now_ns(&self) -> i64 {
		let i = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		match self.stamps.get(i) {
			Some(&t) => t,
			None => self.stamps.last().copied().unwrap_or(EPOCH) + 1 + i as i64,
		}
	}
}

/// The read clocks a replay may be asked for. `EVENT` and `ALL` are the two degenerate ends and are
/// in the draw rather than special-cased; the finite ones straddle the stamp gaps below, so a cell
/// holds anywhere between one message and all of them.
const CELLS: [i64; 6] = [0, 1, 1_000, 1_000_000, 5_000_000_000, i64::MAX];

pub fn session(f: &mut Frng) -> Session {
	// Swarm testing: the mix is drawn per run rather than tuned once, so a run that is all book and a
	// run that is all trades both happen instead of every run being the average of them.
	let weights = [1 + f.below(8), 1 + f.below(8), f.below(4), f.below(4)];
	let cell = CELLS[f.below(CELLS.len() as u32) as usize];
	let read = match cell {
		0 => ReadClock::EVENT,
		i64::MAX => ReadClock::ALL,
		n => ReadClock::from(Exact::from_nanos(n)),
	};

	let (mut msgs, mut stamps) = (Vec::new(), Vec::new());
	let mut at = EPOCH;
	let mut seeded = false;
	// Two bytes per message at the outside, so `size` maps to trace length at roughly that rate.
	while f.remaining() > 3 {
		let msg = match f.weighted(&weights) {
			0 => trades(f),
			1 => {
				let m = book(f, seeded);
				seeded = true;
				m
			}
			2 => Msg::Oi(f.span(0.0, 1_000.0)),
			_ => Msg::Mc(f.span(1e6, 1e9)),
		};
		at = next_stamp(f, at, cell);
		msgs.push(msg);
		stamps.push(at);
	}
	Session { msgs, stamps, read, cell }
}

/// The next arrival, strictly after `prev`. Half the draws aim at the cell the read clock will cut,
/// and one aims a whole checkpoint period ahead so a book message can carry a delta run and an
/// anchor at the same stamp.
fn next_stamp(f: &mut Frng, prev: i64, cell: i64) -> i64 {
	let end = |a: i64| match cell {
		0 | i64::MAX => a,
		n => a.saturating_sub(a.rem_euclid(n)).saturating_add(n - 1),
	};
	let want = match f.below(6) {
		0 => prev + 1,                                   // the tightest possible gap
		1 => end(prev),                                  // exactly this cell's last arrival
		2 => end(prev) + 1,                              // exactly the next cell's first
		3 => end(prev + CHECKPOINT_CADENCE),             // a checkpoint period ahead, on a boundary
		4 => prev + CHECKPOINT_CADENCE,                  // a checkpoint period ahead, off one
		_ => prev + 1 + f.span(0.0, 4_000_000.0) as i64, // a plain gap
	};
	want.max(prev + 1)
}

fn trades(f: &mut Frng) -> Msg {
	// One at the low end, not zero: `BatchTrades::new` asserts non-empty by construction, so an empty
	// trade message is not a case the weaver can be asked about — it is not representable.
	let n = 1 + f.below(3) as usize;
	Msg::Trades(
		(0..n)
			.map(|_| InnerTrade {
				time: Ts::from_nanos(1_000),
				sent: None,
				price: MID + f.below(2 * DEPTH as u32) as i32 - DEPTH,
				qty: 1 + f.below(200),
				side: if f.byte() % 2 == 0 { Side::Buy } else { Side::Sell },
			})
			.collect(),
	)
}

fn book(f: &mut Frng, seeded: bool) -> Msg {
	let levels = |f: &mut Frng, base: i32| -> Vec<(i32, u32)> {
		let n = 1 + f.below(4) as usize;
		(0..n)
			.map(|_| {
				// a quarter of the qtys are 0: a delete against a level the book holds is the case a
				// generator that only ever adds never reaches.
				let q = f.below(4);
				(base + f.below(DEPTH as u32) as i32, if q == 0 { 0 } else { 1 + f.below(500) })
			})
			.collect()
	};
	let bids = levels(f, MID - DEPTH);
	let asks = levels(f, MID + 1);
	let shape = shape(&bids, &asks);
	// An unseeded book takes whichever arrives first as its epoch, so the first message is only ever
	// a seed; past that a snapshot is a correction and a delta is the steady state.
	match (seeded, f.below(4)) {
		(false, _) | (true, 0) => Msg::Book(BookUpdate::Snapshot(shape)),
		(true, k) => Msg::Book(BookUpdate::BatchDelta { shape, gapped: k == 1 }),
	}
}

/// A venue shape as an adapter hands it over. `Live` overwrites the reception reading with its own
/// ingest stamp, so only the exec axis here is ours to state.
fn shape(bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	BookShape {
		ts: Aggregate {
			venue_exec: Span::at(Ts::<Venue>::from_nanos(1_000)),
			local_recv: Span::at(Ts::<Local>::from_nanos(1_000)),
		},
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
	}
}
