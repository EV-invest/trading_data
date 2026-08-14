//! **Phase 1 — the weaver.** `weaver.typ` §1.9's five promises, over a generated multi-lane session
//! instead of a hand-written one, and the storage round-trip `sync_round_trip.rs` fixes.
//!
//! Two targets over one generated trace:
//!
//! - `weave` drives `Live` with recording off and checks the emission itself — order, grouping,
//!   exactly-once, and that nothing is held back.
//! - `round_trip` records the same session and replays the catalog twice, under a generated
//!   `ReadClock` and under `EVENT`: what came back must be what went in, under either grouping. The
//!   §1.5 tie-break rides on the `EVENT` leg, which is the only grouping under which a tie is one —
//!   see [`tie_oracle`].
//!
//! What is deliberately *not* asserted is that `Live` and `Replay` produce the same steps. They
//! group differently by nature (`weaver.typ` §2, ARCHITECTURE.md), and demanding agreement there
//! buys a green check that means nothing. The comparison is over the flattened element stream and
//! the folded book — the two things the parquet layer is actually responsible for.

use std::sync::Arc;

use tempfile::tempdir;
use trading_data::{Batch as _, Book, BookChunk, Catalog, ExchangeName, Feed, FrameKind, Horizon, LaneKind, LatencyConfig, Live, ReadClock, Replay, Ts};
use v_utils::{TF_15MIN, fuzz::Frng};

use crate::stream::{self, Msg, PREC, Session, StampClock, symbol};

pub const VERSION: u32 = v_utils::fuzz::fnv(&[v_utils::fuzz::FRNG_SRC, include_str!("stream.rs"), include_str!("weaver.rs")]);

/// The lanes, in the order the weaver's own tie-break ranks them — a book message feeds 1 and 2, and
/// that pair at one stamp is the tie `weaver.typ` §1.5 pins.
const LANES: [&str; 5] = ["trades", "deltas", "anchors", "oi", "mc"];

/// Both sides as raw levels, which is the whole of what "the same book" means. Deliberately not the
/// epoch: an epoch counts resyncs, and how many a run needs is a function of how the feed grouped
/// its anchors, which is the feed's business and not the book's.
type Levels = (Vec<(i32, u32)>, Vec<(i32, u32)>);

/// One emission, projected to everything an oracle below reads. Owned, because a `Lanes` borrows the
/// feed it came from and the checks are over the whole run.
struct StepView {
	arrival: i64,
	/// Index into [`LANES`] of the one lane this step carries.
	lane: usize,
	trades: Vec<(u64, i32, u32)>,
	/// `(monotonic_seq, recv, price, qty, is_correction)` — `recv` groups a venue message's run.
	deltas: Vec<(u64, i64, i32, u32, bool)>,
	anchor: Option<(usize, usize)>,
	oi: usize,
	mc: usize,
}

fn collect(feed: &mut impl Feed) -> (Vec<StepView>, Levels) {
	let (mut out, mut book, mut chunk) = (Vec::new(), Book::default(), BookChunk::default());
	while let Some(l) = feed.next() {
		// the frame's own retention driven by hand: a `Feed` hands lanes, and folding a book off one
		// means standing in for the `Buffer<BookDeltas, 15m>` a graph would have grown.
		chunk.advance(l.deltas, Horizon::Over(TF_15MIN));
		book.step(l.anchor, &chunk);
		let t = l.trades;
		let lanes = [!t.is_empty(), !l.deltas.is_empty(), l.anchor.is_some(), !l.oi.is_empty(), !l.mc.is_empty()];
		out.push(StepView {
			arrival: l.arrival.as_nanos(),
			lane: lanes.iter().position(|&p| p).unwrap_or(LANES.len()),
			trades: (0..t.len()).map(|i| (t.monotonic_seq[i], t.price[i], t.qty[i])).collect(),
			deltas: l
				.deltas
				.iter()
				.map(|d| (d.monotonic_seq, d.ts_local_recv.as_nanos(), d.price, d.qty, d.kind != FrameKind::Update))
				.collect(),
			anchor: l.anchor.map(|s| (s.bids.len(), s.asks.len())),
			oi: l.oi.len(),
			mc: l.mc.len(),
		});
	}
	(out, (book.bids().to_vec(), book.asks().to_vec()))
}

/// What the sinks pushed, so "nothing held back" has something to be measured against.
struct Pushed {
	trades: usize,
	oi: usize,
	mc: usize,
}

impl Pushed {
	fn of(msgs: &[Msg]) -> Self {
		Self {
			trades: msgs.iter().map(|m| if let Msg::Trades(t) = m { t.len() } else { 0 }).sum(),
			oi: msgs.iter().filter(|m| matches!(m, Msg::Oi(_))).count(),
			mc: msgs.iter().filter(|m| matches!(m, Msg::Mc(_))).count(),
		}
	}
}

/// Drive one generated session through `Live`. `record` is what the round-trip target needs and the
/// weave target does not — a run that never opens a catalog never pays for one.
fn live(msgs: Vec<Msg>, stamps: &[i64], catalog: Catalog, record: bool) -> (Vec<StepView>, Levels) {
	let clock = Arc::new(StampClock::new(stamps));
	let mut feed = Live::new(catalog, ExchangeName::Bybit, symbol(), PREC, record, clock);
	let sink = feed.sink();
	for m in msgs {
		m.push(&sink);
	}
	drop(sink);
	collect(&mut feed)
}

/// The §1.9 promises, over whichever feed produced `steps`.
fn weave_oracles(who: &str, steps: &[StepView]) -> Result<(), String> {
	let mut prev = i64::MIN;
	for (i, s) in steps.iter().enumerate() {
		// 1. emission order is non-decreasing in `Arrival`.
		check!(s.arrival >= prev, "{who}: step {i} emitted out of arrival order ({} after {prev})", s.arrival);
		prev = s.arrival;
		// 2. exactly one lane is populated per step.
		let n = [!s.trades.is_empty(), !s.deltas.is_empty(), s.anchor.is_some(), s.oi > 0, s.mc > 0]
			.iter()
			.filter(|p| **p)
			.count();
		check!(n == 1, "{who}: step {i} populates {n} lanes, not one");
	}

	// 3. a venue message never splits across ticks. A book message's delta run shares one reception
	//    stamp, so the run is recoverable from the stream itself rather than from what generated it.
	let mut seen: Vec<(i64, usize)> = Vec::new();
	for (i, s) in steps.iter().enumerate() {
		for &(_, recv, ..) in &s.deltas {
			match seen.iter_mut().find(|(r, _)| *r == recv) {
				Some((_, at)) => check!(*at == i, "{who}: the delta run at recv {recv} is split across steps {at} and {i}"),
				None => seen.push((recv, i)),
			}
		}
	}
	Ok(())
}

/// The §1.5 tie-break, which is only *observable* under [`ReadClock::EVENT`].
///
/// A step reports `arrival` as the **last** key of the run it emitted, not the first. Under any
/// coarser clock a lane's run spans keys, so two steps can report the same `arrival` without the
/// weaver ever having faced a tie — an anchors run whose head sits a period back swallows the
/// checkpoint that shares a stamp with this step's deltas, and comes out ahead of them on the
/// strength of its head. Under `EVENT` a run cannot span keys at all, so head, last and `arrival`
/// are one number and equal `arrival` *is* the tie. Then the order must be the lane order, which is
/// what makes a replayed book message put its delta frame ahead of the checkpoint it wrote, exactly
/// as live did.
fn tie_oracle(steps: &[StepView]) -> Result<(), String> {
	for w in steps.windows(2) {
		check!(
			w[0].arrival < w[1].arrival || w[0].lane < w[1].lane,
			"event-replay: at arrival {} lane {} follows lane {} — the tie-break must order {LANES:?}",
			w[1].arrival,
			LANES[w[1].lane],
			LANES[w[0].lane]
		);
	}
	Ok(())
}

/// Everything the weave carried, in emission order and stripped of how it grouped — the projection
/// both feeds must agree on, and the one the anchors deliberately stay out of (a run of checkpoints
/// inside one cell is delivered as its last, so anchors are not an exactly-once lane; the folded
/// book is what stands for them).
fn flat(steps: &[StepView]) -> (Vec<(u64, i32, u32)>, Vec<(u64, i32, u32, bool)>, usize, usize) {
	let mut t = Vec::new();
	let mut d = Vec::new();
	let (mut oi, mut mc) = (0, 0);
	for s in steps {
		t.extend(s.trades.iter().copied());
		d.extend(s.deltas.iter().map(|&(seq, _, p, q, c)| (seq, p, q, c)));
		oi += s.oi;
		mc += s.mc;
	}
	(t, d, oi, mc)
}

/// The lanes the catalog actually holds. Asking `Replay` for one nothing was written to is not a
/// finding, it is a caller error the engine rightly panics on — so this reads what the live pass
/// emitted, which is exactly what the tee wrote.
fn recorded(steps: &[StepView]) -> Vec<LaneKind> {
	let (t, d, oi, mc) = flat(steps);
	let mut out = Vec::new();
	if !t.is_empty() {
		out.push(LaneKind::Trades);
	}
	if !d.is_empty() {
		out.push(LaneKind::BookDeltas);
	}
	if steps.iter().any(|s| s.anchor.is_some()) {
		out.push(LaneKind::BookAnchors);
	}
	if oi > 0 {
		out.push(LaneKind::Oi);
	}
	if mc > 0 {
		out.push(LaneKind::Mc);
	}
	out
}

fn report(who: &str, steps: &[StepView]) {
	let (t, d, oi, mc) = flat(steps);
	eprintln!(
		"{who}: {} steps, {} trades, {} deltas, {oi} oi, {mc} mc, {} anchors",
		steps.len(),
		t.len(),
		d.len(),
		steps.iter().filter(|s| s.anchor.is_some()).count()
	);
	for (i, s) in steps.iter().enumerate() {
		eprintln!(
			"  [{i:>3}] @{} {}  t={} d={} a={:?} oi={} mc={}",
			s.arrival,
			LANES.get(s.lane).copied().unwrap_or("none"),
			s.trades.len(),
			s.deltas.len(),
			s.anchor,
			s.oi,
			s.mc
		);
	}
}

pub fn run(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let Session { msgs, stamps, cell, .. } = stream::session(f);
	if msgs.is_empty() {
		return Ok(());
	}
	let want = Pushed::of(&msgs);
	let n = msgs.len();
	let dir = tempdir().expect("temp dir");
	let (steps, _) = live(msgs, &stamps, Catalog::new(dir.path()), false);
	if verbose {
		eprintln!("session: {n} msgs, cell={cell}ns");
		report("live", &steps);
	}
	weave_oracles("live", &steps)?;

	// 5. under `Live`, nothing is held back: what the sinks pushed is what a drained feed emitted.
	let (t, d, oi, mc) = flat(&steps);
	check!(t.len() == want.trades, "live held back trades: {} of {} emitted", t.len(), want.trades);
	check!(oi == want.oi, "live held back oi: {oi} of {} emitted", want.oi);
	check!(mc == want.mc, "live held back mc: {mc} of {} emitted", want.mc);

	// 6. every element exactly once, in key order. The trade lane numbers its own elements from 1 on
	//    ingest, so the flattened stream must *be* that sequence.
	let seqs: Vec<u64> = t.iter().map(|&(s, ..)| s).collect();
	check!(seqs == (1..=want.trades as u64).collect::<Vec<_>>(), "the trade lane is not its own sequence: {seqs:?}");
	let dseqs: Vec<u64> = d.iter().map(|&(s, ..)| s).collect();
	check!(
		dseqs == (1..=dseqs.len() as u64).collect::<Vec<_>>(),
		"the delta lane dropped, repeated or reordered an element: {dseqs:?}"
	);
	Ok(())
}

pub fn run_round_trip(f: &mut Frng, verbose: bool) -> Result<(), String> {
	let Session { msgs, stamps, read, cell } = stream::session(f);
	if msgs.is_empty() {
		return Ok(());
	}
	let n = msgs.len();
	let dir = tempdir().expect("temp dir");
	let catalog = Catalog::new(dir.path());
	let (live_steps, live_book) = live(msgs, &stamps, catalog.clone(), true);
	let lanes = recorded(&live_steps);
	if lanes.is_empty() {
		return Ok(()); // nothing was written, so there is nothing to read back
	}

	// Real receptions are recorded, so a replay short-circuits latency simulation and the weave key
	// it reconstructs is the one live issued — equality is exact rather than distributional.
	let latency = LatencyConfig {
		p68: std::time::Duration::from_millis(10),
		p95: std::time::Duration::from_millis(30),
		p997: std::time::Duration::from_millis(90),
		seed: 1,
	};
	// Two replays of one recording: the generated clock, and `EVENT`, which is the finest a replay can
	// be read at and the only grouping under which a tie is a tie. Same events out of both, whatever
	// the read clock did to how they arrived — which is the persistence layer's half of
	// `rates.folds.exactly-once`, one tier below the graph's.
	let at = |read| {
		let mut r = Replay::new(&catalog, ExchangeName::Bybit, symbol(), Ts::from_nanos(0), Ts::from_nanos(i64::MAX), &lanes, latency, read);
		collect(&mut r)
	};
	let (replay_steps, replay_book) = at(read);
	let (event_steps, event_book) = at(ReadClock::EVENT);
	if verbose {
		eprintln!("session: {n} msgs, cell={cell}ns, lanes={lanes:?}");
		report("live", &live_steps);
		report("replay", &replay_steps);
		report("event", &event_steps);
	}

	// the weave promises hold of a replay exactly as they hold of live — same weaver, other source.
	weave_oracles("replay", &replay_steps)?;
	weave_oracles("event", &event_steps)?;
	tie_oracle(&event_steps)?;

	let (lt, ld, loi, lmc) = flat(&live_steps);
	for (who, steps, book) in [("replay", &replay_steps, &replay_book), ("event", &event_steps, &event_book)] {
		let (rt, rd, roi, rmc) = flat(steps);
		check!(lt == rt, "{who} lost, reordered or invented trades:\n  live   {lt:?}\n  {who} {rt:?}");
		check!(ld == rd, "{who} lost, reordered or invented deltas:\n  live   {ld:?}\n  {who} {rd:?}");
		check!(loi == roi, "{who} read back {roi} oi rows, live emitted {loi}");
		check!(lmc == rmc, "{who} read back {rmc} mc rows, live emitted {lmc}");
		check!(
			live_book == *book,
			"the recorded delta lane folds to a different book under {who}:\n  live  {live_book:?}\n  {who} {book:?}"
		);
	}
	Ok(())
}
