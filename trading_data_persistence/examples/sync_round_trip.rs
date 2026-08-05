//! **Storage round-trip: what `Live` recorded is what `Replay` reads back — every event, in order,
//! folding to the same book.** Drive `Live` (record=true) with a synthetic multi-lane stream, then
//! `Replay` the just-recorded catalog and compare.
//!
//! What is *not* asserted is that the two produce the same steps. They cannot and should not: a
//! replay batches on its [`ReadClock`] to outrun the market it replays, live has no such knob, and
//! demanding the two agree step for step would be demanding that a backtest stop being a lossy
//! model of a live run. So the comparison is over the *flattened* event stream, which is the thing
//! the parquet layer is actually responsible for: nothing dropped, nothing reordered, nothing
//! invented.
//!
//! The book comparison is the one that pays for the shadow layer: the persisted delta lane is our
//! own reconciliation, so replaying it must land on bit-identical `(epoch, len, best_bid,
//! best_ask)` — not merely "a snapshot appeared in both streams".
//!
//! Real `ts_local_recv` is recorded, so replay short-circuits latency simulation and equality is exact.

use std::sync::{
	Arc,
	atomic::{AtomicI64, Ordering},
};

use tempfile::tempdir;
use trading_data_core::{Aggregate, ExchangeName, InnerTrade, Instrument, Local, Precision, PrecisionPriceQty, Price, Qty, Side, Span, Symbol, Ts, Venue};
use trading_data_persistence::{BatchTrades, Book, BookShape, BookUpdate, Catalog, Clock, DeltaFrame, Exact, Feed, LaneKind, LatencyConfig, Live, Oi, ReadClock, Replay};

/// Monotonic synthetic clock: each read advances 1ms, so live arrival stamps are strictly
/// increasing across lanes and the recording replays deterministically.
struct EventClock(AtomicI64);
impl Clock for EventClock {
	fn now_ns(&self) -> i64 {
		self.0.fetch_add(1_000_000, Ordering::Relaxed)
	}
}

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
const LANES: &[LaneKind] = &[LaneKind::Trades, LaneKind::BookDeltas, LaneKind::BookAnchors, LaneKind::Oi];
const CLOCK: ReadClock = ReadClock::from(Exact::from_nanos(5_000_000));

fn symbol() -> Symbol {
	Symbol::new("BTC-USDT".try_into().expect("static pair"), Instrument::Perp)
}

/// Owned, comparable projection of one woven step: every lane's contents, plus what a consumer's
/// book reads after folding it. Enough to catch any content or reconciliation drift.
/// What a consumer reads off the folded book: epoch, level count, best bid, best ask.
type BookRead = (u64, usize, Option<(Price, Qty)>, Option<(Price, Qty)>);

#[derive(Debug, PartialEq)]
struct Step {
	trades: Vec<(i64, u64, i32, u32)>,
	deltas: (bool, Vec<(i64, u64, i32, u32)>),
	anchor: Option<(i64, usize, usize)>,
	oi: Vec<i64>,
	book: BookRead,
}

fn collect(feed: &mut impl Feed) -> Vec<Step> {
	let mut out = Vec::new();
	let mut book = Book::default();
	while let Some(l) = feed.next() {
		let t = l.trades;
		let d = l.deltas.cols();
		let synced = book.step(l.anchor, l.deltas);
		out.push(Step {
			trades: (0..t.len())
				.map(|i| (t.ts.recv.expect("recorded").last.as_nanos(), t.monotonic_seq[i], t.price[i], t.qty[i]))
				.collect(),
			deltas: (
				matches!(l.deltas, DeltaFrame::Correction(_)),
				(0..d.len())
					.map(|i| (d.ts.recv.expect("recorded").last.as_nanos(), d.monotonic_seq[i], d.price[i], d.qty[i]))
					.collect(),
			),
			anchor: l.anchor.map(|s| (s.ts.local_recv.last.as_nanos(), s.bids.len(), s.asks.len())),
			oi: l.oi.iter().map(|o| o.ts_local_recv.expect("recorded").as_nanos()).collect(),
			book: match synced {
				true => (book.epoch(), book.len(), book.best_bid(), book.best_ask()),
				false => (0, 0, None, None),
			},
		});
	}
	out
}

// A real adapter stamps its own reception; `Live` then overwrites it with the ingest stamp so the
// woven rows and the shape agree.
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

fn trades(seqs: &[u64]) -> BatchTrades {
	let inner = seqs
		.iter()
		.map(|&seq| InnerTrade {
			time: Ts::from_nanos(1_000),
			sent: None,
			price: 10_000 + seq as i32,
			qty: 5_000,
			side: if seq % 2 == 0 { Side::Buy } else { Side::Sell },
		})
		.collect();
	BatchTrades::new(PREC, inner, Ts::from_nanos(0))
}

fn oi(v: f64) -> Oi {
	Oi {
		ts_venue_exec: Ts::from_nanos(1_000),
		ts_venue_send: None,
		ts_local_recv: None,
		oi: v,
	}
}

fn main() {
	let dir = tempdir().expect("temp dir");
	let catalog = Catalog::new(dir.path());
	let clock = Arc::new(EventClock(AtomicI64::new(1_000_000_000)));

	// --- record a live session ---
	let live_summary = {
		let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), PREC, true, clock.clone());
		let sink = live.sink();

		// interleave lanes so the weaver actually merges non-trivially.
		sink.trades(trades(&[1]));
		sink.oi(oi(42.0));
		sink.trades(trades(&[2, 3]));
		sink.book(BookUpdate::Snapshot(shape(&[(10_000, 5), (9_999, 3)], &[(10_001, 4), (10_002, 6)])));
		sink.book(BookUpdate::BatchDelta {
			shape: shape(&[(9_998, 2)], &[(10_003, 1)]),
			gapped: false,
		});
		sink.trades(trades(&[4]));
		sink.oi(oi(43.0));
		// a dropped websocket packet: our own frame says so, and the venue's next snapshot corrects it
		sink.book(BookUpdate::BatchDelta {
			shape: shape(&[(9_997, 7)], &[]),
			gapped: true,
		});
		sink.book(BookUpdate::Snapshot(shape(&[(10_000, 5), (9_999, 3), (9_998, 2)], &[(10_001, 4), (10_002, 6), (10_003, 1)])));

		drop(sink);
		collect(&mut live)
	};

	// --- replay the recording over the same range ---
	let latency = LatencyConfig {
		p68: std::time::Duration::from_millis(10),
		p95: std::time::Duration::from_millis(30),
		p997: std::time::Duration::from_millis(90),
		seed: 1,
	};
	let mut replay = Replay::new(&catalog, ExchangeName::Bybit, symbol(), Ts::from_nanos(0), Ts::from_nanos(i64::MAX), LANES, latency, CLOCK);
	let replay_summary = collect(&mut replay);

	assert_eq!(events(&live_summary), events(&replay_summary), "replay lost, reordered or invented events");
	let final_book = live_summary.last().expect("non-empty run").book;
	assert_eq!(
		final_book,
		replay_summary.last().expect("non-empty run").book,
		"the recorded delta lane folds to a different book"
	);
	assert!(final_book.0 > 0 && final_book.2.is_some(), "the folded book never synced: {final_book:?}");
	assert!(
		live_summary.iter().any(|s| s.deltas.0),
		"the gapped delta must reach the consumer as a Correction, in both streams"
	);
	assert!(live_summary.iter().any(|s| s.anchor.is_some()), "our own checkpoint must weave into both streams");
	assert!(live_summary.iter().filter(|s| !s.trades.is_empty()).count() >= 1, "trades must weave");
	assert!(live_summary.iter().filter(|s| !s.oi.is_empty()).count() >= 1, "oi must weave");

	println!("sync_round_trip: {} events round-tripped incl. book state. ok", events(&live_summary).len());

	concurrent_streaming_matches_replay();
}

/// The paramount path: consume a `Live` feed *concurrently* with a producer thread (bounded
/// streaming, blocking-recv drain), then replay the recording — the flattened event streams must
/// match. Deterministic: one producer sends in a fixed order, ingest stamps in receive order.
///
/// Flattened, not step-wise, for the reason in the module doc: how the two group is theirs to
/// decide, what they contain is not.
fn concurrent_streaming_matches_replay() {
	use std::{thread, time::Duration};

	let dir = tempdir().expect("temp dir");
	let catalog = Catalog::new(dir.path());
	let clock = Arc::new(EventClock(AtomicI64::new(1_000_000_000)));

	let mut live = Live::new(catalog.clone(), ExchangeName::Bybit, symbol(), PREC, true, clock);
	let sink = live.sink();
	let producer = thread::spawn(move || {
		for i in 0..40u64 {
			sink.trades(trades(&[i]));
			if i % 4 == 0 {
				sink.oi(oi(i as f64));
			}
			thread::sleep(Duration::from_micros(100)); // let the consumer find the channel empty and block
		}
		// sink dropped here → channel disconnects once the feed's own tx is gone
	});

	let live_flat = flat(&mut live);
	producer.join().expect("producer panicked");

	let latency = LatencyConfig {
		p68: Duration::from_millis(10),
		p95: Duration::from_millis(30),
		p997: Duration::from_millis(90),
		seed: 2,
	};
	let mut replay = Replay::new(
		&catalog,
		ExchangeName::Bybit,
		symbol(),
		Ts::from_nanos(0),
		Ts::from_nanos(i64::MAX),
		&[LaneKind::Trades, LaneKind::Oi],
		latency,
		CLOCK,
	);
	let replay_flat = flat(&mut replay);

	assert_eq!(live_flat, replay_flat, "replay of a concurrently-recorded session lost or reordered events");
	assert_eq!(live_flat.iter().filter(|(lane, _)| *lane == b't').count(), 40, "all 40 trades must weave");
	println!("concurrent streaming: {} events round-tripped. ok", live_flat.len());
}

/// Flatten collected steps to one entry per event, in emission order. `monotonic_seq` is globally
/// unique and per-event, whereas a run's reception reading is per-*run* — so the timestamp is
/// deliberately not in here: it says how the feed grouped, which is the feed's business.
fn events(steps: &[Step]) -> Vec<(u8, u64, i32, u32)> {
	let mut out = Vec::new();
	for s in steps {
		out.extend(s.trades.iter().map(|&(_, seq, p, q)| (b't', seq, p, q)));
		out.extend(s.deltas.1.iter().map(|&(_, seq, p, q)| (b'd', seq, p, q)));
		out.extend(s.anchor.map(|(_, b, a)| (b'a', 0, b as i32, a as u32)));
		out.extend(s.oi.iter().map(|_| (b'o', 0, 0, 0)));
	}
	out
}

/// Flatten a feed to `(lane_tag, seq)` per event, in emission order — robust to batch chunking.
fn flat(feed: &mut impl Feed) -> Vec<(u8, u64)> {
	let mut out = Vec::new();
	while let Some(l) = feed.next() {
		out.extend(l.trades.monotonic_seq.iter().map(|&s| (b't', s)));
		out.extend(l.oi.iter().map(|o| (b'o', o.ts_local_recv.expect("recorded").as_nanos() as u64)));
		assert!(l.mc.is_empty() && l.anchor.is_none() && l.deltas.cols().is_empty(), "only the trade and oi lanes are driven");
	}
	out
}
