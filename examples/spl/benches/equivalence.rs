//! The correctness gate the other three benches rest on: the strategy's nodes, called directly from
//! outside any `Graph`, run the same *strategy* as `Graph::tick` over the same tape.
//!
//! Same strategy, not the same tick stream. What the NT comparison this guards actually asks is
//! whether both paths traded alike, and two runs that open the same episodes, on the same sides, at
//! the same committed size and holding the same exposure did — whatever a slot's worth of timing
//! did in between. Demanding the readings line up tick for tick is a stronger claim than that, and
//! a stronger one than a hand-wired chain against an engine-driven frame can be held to.
//!
//! The NT benches run exactly this — our node types, invoked by hand — with the bars and the book
//! coming from NautilusTrader instead of from `Bars`/`Book`. If the direct-call path diverges from
//! the graph, every number the comparison prints is measuring two different strategies.
//!
//! Three pieces of `graph!` are reproduced by hand below, and each is a thing an NT actor has to get
//! right too: the per-`Emit` buffer cleared every tick; a closed [`StdScreener`] gate meaning the
//! node is *not called* and reads its `Latent` out, which is not the same as calling it with empty
//! deps; and the [`Armed`] latch's *deferred* commutation, applied at the start of the next tick.
//!
//! A third path runs beside them: the same graph driven through `tick_rewind`, where [`Book`] sleeps
//! while nothing demands it and is replayed forward out of the feed's [`Past`] on the tick something
//! does. That one is held to **exact** equality — the tolerance below exists because `Direct`
//! reproduces the sweep by hand, and two `Graph`s over one tape have no such excuse. `wakes` is what
//! keeps the claim from being vacuous: a graph that never slept is the same graph.

use std::path::{Path, PathBuf};

use trading_data::{
	Armed, Bar, Batch as _, Blind as _, Book, BookChunk, BookDelta, BookShape, Buffering, Close, Elems, Episode, Exact, ExchangeName, Feed as _, Fold, Horizon, Latch as _, LatencyConfig,
	Mc, McRoot, Ohlc, Ohlcs, Oi, OiRoot, Over, Past, Predicate, ReadClock, Reading, Replay, Rewound, Runs as _, Scan, Side, Step, TradeCols, Volume, Volumes, bench::ring::Ring,
	required_lanes,
};
use trading_data_spl::{
	config::Config,
	day_bounds, ensure_lanes,
	nodes::{Atr, Batches, BookTop, BookTopSnap, Change, ChangeBack, Classify, Decision, Deprecator, Graph, Imbalance, Intent, Momentum, Spread, StdScreener, VolUsd},
	symbol, trading_days,
};
use v_utils::*;

/// What a run *did*. Counts are exact — an episode either happened or it did not, and that is the
/// thing worth failing over. The quantities are relative, because they are sums over per-tick
/// readings and one tick's worth of drift is not a different strategy.
const TOLERANCE: f64 = 1e-3;
#[tokio::main]
async fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	// Every node asserts the config names what it wires as it is built, so both sides are constructed
	// before a byte of archive is touched.
	let mut graph = Graph::default();
	let mut slept = Graph::default();
	let mut direct = Direct::default();

	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	let catalog = ensure_lanes(&cache, situation).await;
	let kinds = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));

	// The situation's whole window, per-day like `main`: the first day of it screens nothing —
	// momentum wants its whole window of 5m closes before the screener can fire at all — so a truncated
	// window would assert that two silent paths are equally silent. The `fired` check at the end is
	// what makes that a failure rather than a pass.
	let days = trading_days(situation);
	let mut ticks = 0u64;
	let mut wakes = 0u64;
	let (mut want_ran, mut got_ran, mut slept_ran) = (Outcome::default(), Outcome::default(), Outcome::default());
	for d in &days {
		let (start, end) = day_bounds(*d);
		let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &kinds, latency, read_clock);
		while let Some(Step { lanes: l, past }) = feed.step() {
			let (trades, deltas, anchor, oi, mc) = (l.trades, l.deltas, l.anchor, l.oi, l.mc);
			let batches = || Batches {
				trades,
				deltas,
				anchors: anchor,
				oi,
				mc,
			};
			let want: Vec<Intent> = graph.tick(l.ts_venue.as_nanos(), batches()).deprecator.to_vec();
			want_ran.absorb(&want);
			got_ran.absorb(direct.tick(trades, deltas, anchor, oi, mc));

			let mut counted = Counted(past, 0);
			let woke: Vec<Intent> = slept.tick_rewind(l.ts_venue.as_nanos(), batches(), &mut counted).deprecator.to_vec();
			slept_ran.absorb(&woke);
			wakes += counted.1;
			ticks += 1;
		}
	}
	assert!(
		want_ran.episodes > 0,
		"{ticks} ticks over {} days closed no episode at all — the gate compared two silent paths",
		days.len()
	);
	assert!(
		wakes > 0,
		"{ticks} ticks and the anchored path never slept — the exact equality below compares one graph with itself"
	);
	assert_eq!(want_ran, slept_ran, "a book that slept and was replayed back is not the same strategy as one that never slept");
	want_ran.agrees_with(&got_ran);
	println!("equivalence: {ticks} ticks, {wakes} wakes, {want_ran:?}, graph == direct == rewound");
}

/// [`Past`], with the one thing the comparison cannot see off the outcomes: whether the second graph
/// slept at all. A wake is a rewind that actually moved the book's cursor — the steady-state call
/// finds it already where the past ends and does nothing.
struct Counted<'a>(Past<'a>, u64);

impl Rewound<Book> for Counted<'_> {
	fn rewind(&mut self, b: &mut Book) {
		let before = b.seq();
		self.0.rewind(b);
		self.1 += u64::from(b.seq() != before);
	}
}

#[derive(Debug, Default, PartialEq)]
struct Outcome {
	/// one per closed episode
	episodes: usize,
	long: usize,
	short: usize,
	/// Σ over episodes of the size it committed
	committed: f64,
	/// Σ over every published change of `target_q` — a digest of the stream, not a held-size
	/// integral now that a tick moving nothing publishes nothing
	exposure: f64,
}

impl Outcome {
	fn absorb(&mut self, intents: &[Intent]) {
		for i in intents {
			self.exposure += i.target_q;
			if i.terminal {
				self.episodes += 1;
				self.committed += i.base_q;
				match i.side {
					Side::Buy => self.long += 1,
					Side::Sell => self.short += 1,
				}
			}
		}
	}

	fn agrees_with(&self, other: &Self) {
		assert_eq!(
			(self.episodes, self.long, self.short),
			(other.episodes, other.long, other.short),
			"the two paths closed different episodes:\n  graph  {self:?}\n  direct {other:?}"
		);
		for (what, a, b) in [("committed", self.committed, other.committed), ("exposure", self.exposure, other.exposure)] {
			let off = (a - b).abs() / a.abs().max(b.abs()).max(f64::MIN_POSITIVE);
			assert!(off <= TOLERANCE, "{what} differs by {:.4}% (> {:.4}%): graph {a}, direct {b}", off * 100.0, TOLERANCE * 100.0);
		}
	}
}

/// The whole reachable closure of `Graph`'s `deprecator` output, by hand: the nodes, the histories
/// their `Buffering` deps read, and the buffers the engine would own. `Rsi` is the one node of the
/// real graph missing here — it is a second output that nothing on this path reads.
#[derive(Default)]
struct Direct {
	ohlc_1m: Ohlcs<{ TF_1MIN }>,
	ohlc_5m: Ohlcs<{ TF_5MIN }>,
	ohlc_1h: Ohlcs<{ TF_1H }>,
	vol_1m: Volumes<{ TF_1MIN }>,
	vol_5m: Volumes<{ TF_5MIN }>,
	vol_1h: Volumes<{ TF_1H }>,
	bars_1m: trading_data::Bars<{ TF_1MIN }>,
	bars_5m: trading_data::Bars<{ TF_5MIN }>,
	bars_1h: trading_data::Bars<{ TF_1H }>,
	book: Book,
	/// The frame's `Buffer<BookDeltas, 15m>`, by hand — the direct path owns every node the graph has.
	chunk: BookChunk,
	book_top: BookTop,

	m1: Ring<Bar>,
	m5: Ring<Bar>,
	h1: Ring<Bar>,
	oi: Ring<Oi>,
	mc: Ring<Mc>,

	atr: Atr<{ TF_1MIN }>,
	momentum: Momentum<{ TF_5MIN }, 181>,
	screener: StdScreener,
	change_1d: ChangeBack<{ TF_1MIN }, { TF_1H }, { TF_1D }, { Timeframe(TF_1D.0 + TF_1H.0) }>,
	change_3m: Change<{ TF_1MIN }, { TF_3MIN }>,
	vol_usd_1m: VolUsd<{ TF_1MIN }>,
	vol_usd_1h: VolUsd<{ TF_1H }>,
	imbalance: Imbalance,
	spread: Spread,
	classify: Classify,
	decision: Decision,
	deprecator: Deprecator,
	armed: Armed<Deprecator>,

	b_ohlc: [Vec<Ohlc>; 3],
	b_vol: [Vec<Volume>; 3],
	b_bars: [Vec<Bar>; 3],
	b_top: Vec<Option<BookTopSnap>>,
	b_atr: Vec<Reading>,
	b_mom: Vec<Option<f64>>,
	b_c1d: Vec<Reading>,
	b_c3m: Vec<Reading>,
	b_vol_usd_1m: Vec<f64>,
	b_vol_usd_1h: Vec<f64>,
	b_imb: Vec<Reading>,
	b_spr: Vec<Reading>,
	b_dep: Vec<Intent>,

	/// The frame's carries over `Atr` / `Momentum`. Engine storage, so a replica of the graph owns
	/// them too — ungated, and so untouched by the commutation reset below.
	l_atr: Option<f64>,
	l_mom: Option<f64>,
	l_vol_usd_1h: Option<f64>,

	/// `graph!`'s `__pending`: a terminal out commutates at the *next* tick's start, because the
	/// frame still borrows this one's batches at the end of it.
	pending: bool,
}

impl Direct {
	fn tick(&mut self, trades: TradeCols<'_>, deltas: &[BookDelta], anchor: Option<&BookShape>, oi: &[Oi], mc: &[Mc]) -> &[Intent] {
		if self.pending {
			self.pending = false;
			self.armed.commutate();
			// `gates_on(Armed<Deprecator>)` picks out exactly one node of this graph.
			self.deprecator = Deprecator::default();
			self.b_dep.clear();
		}

		for b in &mut self.b_ohlc {
			b.clear();
		}
		for b in &mut self.b_vol {
			b.clear();
		}
		for b in &mut self.b_bars {
			b.clear();
		}
		Close::emit(&mut self.ohlc_1m, (trades,), &mut self.b_ohlc[0]);
		Close::emit(&mut self.ohlc_5m, (trades,), &mut self.b_ohlc[1]);
		Close::emit(&mut self.ohlc_1h, (trades,), &mut self.b_ohlc[2]);
		Close::emit(&mut self.vol_1m, (trades,), &mut self.b_vol[0]);
		Close::emit(&mut self.vol_5m, (trades,), &mut self.b_vol[1]);
		Close::emit(&mut self.vol_1h, (trades,), &mut self.b_vol[2]);
		let [o1, o5, oh] = &self.b_ohlc;
		let [v1, v5, vh] = &self.b_vol;
		let [b1, b5, bh] = &mut self.b_bars;
		Scan::emit(&mut self.bars_1m, (o1, v1), b1);
		Scan::emit(&mut self.bars_5m, (o5, v5), b5);
		Scan::emit(&mut self.bars_1h, (oh, vh), bh);

		self.b_top.clear();
		self.chunk.advance(deltas, Horizon::Over(v_utils::TF_15MIN));
		let folded = self.book.advance((anchor, &self.chunk));
		self.book_top.emit((folded,), &mut self.b_top);

		self.m1.push(&self.b_bars[0]);
		self.m5.push(&self.b_bars[1]);
		self.h1.push(&self.b_bars[2]);
		self.oi.push(oi);
		self.mc.push(mc);

		self.b_atr.clear();
		Fold::emit(&mut self.atr, (&self.b_bars[0],), &mut self.b_atr);
		self.b_mom.clear();
		self.momentum.emit((self.m5.hist::<Buffering<trading_data::Bars<{ TF_5MIN }>, Elems<181>>>(),), &mut self.b_mom);
		if let Some(v) = self.b_atr.iter().rev().find_map(|x| x.get()) {
			self.l_atr = Some(v);
		}
		if let Some(v) = self.b_mom.iter().rev().find_map(|x| *x) {
			self.l_mom = Some(v);
		}

		let hit = <Predicate as trading_data::Kernel<StdScreener>>::advance(&mut self.screener, (&self.b_bars[0], self.l_mom));

		self.b_vol_usd_1h.clear();
		Scan::emit(&mut self.vol_usd_1h, (&self.b_bars[2],), &mut self.b_vol_usd_1h);
		if let Some(v) = self.b_vol_usd_1h.last() {
			self.l_vol_usd_1h = Some(*v);
		}

		self.b_c1d.clear();
		self.b_c3m.clear();
		self.b_vol_usd_1m.clear();
		self.b_imb.clear();
		self.b_spr.clear();
		// A closed gate is not "advance with nothing": the node is never called, and its out is the
		// `Latent` reading — an empty run, or `None`.
		let decision = if hit {
			Scan::emit(
				&mut self.change_1d,
				(
					&self.b_bars[0],
					self.h1.hist::<Buffering<trading_data::Bars<{ TF_1H }>, Over<{ Timeframe(TF_1D.0 + TF_1H.0) }>>>(),
				),
				&mut self.b_c1d,
			);
			Scan::emit(
				&mut self.change_3m,
				(self.m1.hist::<Buffering<trading_data::Bars<{ TF_1MIN }>, Over<TF_3MIN>>>(),),
				&mut self.b_c3m,
			);
			Scan::emit(&mut self.vol_usd_1m, (&self.b_bars[0],), &mut self.b_vol_usd_1m);
			Scan::emit(&mut self.imbalance, (&self.b_top,), &mut self.b_imb);
			Scan::emit(&mut self.spread, (&self.b_top,), &mut self.b_spr);
			let classified = self.classify.advance((
				true,
				&self.b_bars[0],
				self.l_mom,
				&self.b_c1d,
				&self.b_c3m,
				&self.b_vol_usd_1m,
				self.l_vol_usd_1h,
				&self.b_imb,
				&self.b_spr,
				// `Ring` bridges into `Hist` alone, and an `Mc` is never an absence — so the newest row it
				// ever held is what the frame's carry over `McRoot` holds.
				self.mc.hist::<Buffering<McRoot, Elems<1>>>().all().last().copied(),
				self.oi.hist::<Buffering<OiRoot, Over<{ Timeframe(4 * TF_5MIN.0) }>>>(),
			));
			self.decision.advance((classified,))
		} else {
			None
		};

		self.b_dep.clear();
		if self.armed.advance((decision,)) {
			self.deprecator.emit((true, decision, self.l_atr, &self.b_top), &mut self.b_dep);
		}
		if Episode::terminal(&self.b_dep.as_slice()) {
			self.pending = true;
		}
		&self.b_dep
	}
}
