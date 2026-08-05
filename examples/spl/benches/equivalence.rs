//! The correctness gate the other three benches rest on: the strategy's nodes, called directly from
//! outside any `Graph`, produce the same `Intent` stream as `Graph::tick` over the same tape.
//!
//! The NT benches run exactly this — our node types, invoked by hand — with the bars and the book
//! coming from NautilusTrader instead of from `Bars`/`Book`. If the direct-call path diverges from
//! the graph, every number the comparison prints is measuring two different strategies.
//!
//! Three pieces of `graph!` are reproduced by hand below, and each is a thing an NT actor has to get
//! right too: the per-`Emit` buffer cleared every tick; a closed [`StdScreener`] gate meaning the
//! node is *not called* and reads its `Latent` out, which is not the same as calling it with empty
//! deps; and the [`Armed`] latch's *deferred* commutation, applied at the start of the next tick.

use std::path::{Path, PathBuf};

use trading_data::{
	Armed, Bar, Book, BookShape, Buffering, DeltaFrame, Emit as _, Episode, Exact, ExchangeName, Feed as _, Horizon, Latch as _, LatencyConfig, Mc, McRoot, Node as _, Ohlc, Ohlcs, Oi,
	OiRoot, ReadClock, Replay, TradeCols, Volume, Volumes, bench::ring::Ring, required_lanes,
};
use trading_data_spl::{
	config::Config,
	day_bounds, ensure_lanes,
	nodes::{Atr, Batches, BookTop, BookTopSnap, Change1d, Change3m, Classify, Decision, Deprecator, Graph, Imbalance, Intent, Momentum, OI_REACH, Spread, StdScreener, Volume1h, Volume1m},
	symbol, trading_days,
};
use v_utils::*;

fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	// Every node asserts the config names what it wires as it is built, so both sides are constructed
	// before a byte of archive is touched.
	let mut graph = Graph::default();
	let mut direct = Direct::default();

	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	// Criterion's harness is sync; acquisition is not, and it happens once before any timing starts.
	let catalog = tokio::runtime::Runtime::new().expect("build the acquisition runtime").block_on(ensure_lanes(&cache, situation));
	let kinds = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));

	// The situation's whole window, per-day like `main`: the first day of it screens nothing —
	// momentum wants its whole window of 5m closes before the screener can fire at all — so a truncated
	// window would assert that two silent paths are equally silent. The `fired` check at the end is
	// what makes that a failure rather than a pass.
	let days = trading_days(situation);
	let (mut ticks, mut fired) = (0u64, 0u64);
	for d in &days {
		let (start, end) = day_bounds(*d);
		let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &kinds, latency, read_clock);
		while let Some(l) = feed.next() {
			let (trades, deltas, anchor, oi, mc) = (l.trades, l.deltas, l.anchor, l.oi, l.mc);
			let want: Vec<Option<Intent>> = graph
				.tick(
					l.ts_venue.as_nanos(),
					Batches {
						trades,
						deltas,
						anchors: anchor,
						oi,
						mc,
					},
				)
				.deprecator
				.to_vec();
			let got = direct.tick(trades, deltas, anchor, oi, mc);
			assert_eq!(want.len(), got.len(), "tick {ticks}: graph emitted {} slots, the direct chain {}", want.len(), got.len());
			for (i, (w, g)) in want.iter().zip(got).enumerate() {
				assert!(same(w, g), "tick {ticks} slot {i}:\n  graph  {w:?}\n  direct {g:?}");
			}
			fired += want.iter().flatten().count() as u64;
			ticks += 1;
		}
	}
	assert!(fired > 0, "{ticks} ticks over {} days produced no intent at all — the gate compared two silent paths", days.len());
	println!("equivalence: {ticks} ticks, {fired} intents, graph == direct");
}

/// Bit-exact, since both sides run the same arithmetic on the same inputs: anything looser would be
/// hiding a real divergence behind a tolerance.
fn same(a: &Option<Intent>, b: &Option<Intent>) -> bool {
	let (Some(a), Some(b)) = (a, b) else { return a.is_none() && b.is_none() };
	let bits = |i: &Intent| {
		[
			i.base_q.to_bits(),
			i.target_q.to_bits(),
			i.eval.to_bits(),
			i.lambda_atr.to_bits(),
			i.trail_fraction.to_bits(),
			i.sl.to_bits(),
			i.tp.to_bits(),
		]
	};
	a.ts_ns == b.ts_ns && a.side == b.side && bits(a) == bits(b) && a.trail_stop.map(f64::to_bits) == b.trail_stop.map(f64::to_bits) && a.draining == b.draining && a.terminal == b.terminal
}

/// The whole reachable closure of `Graph`'s `deprecator` output, by hand: the nodes, the histories
/// their `Buffering` deps read, and the buffers the engine would own. `Rsi` is the one node of the
/// real graph missing here — it is a second output that nothing on this path reads.
#[derive(Default)]
struct Direct {
	ohlc_1m: Ohlcs<{ TF_1MIN }>,
	ohlc_5m: Ohlcs<{ TF_5MIN }>,
	ohlc_1h: Ohlcs<{ TF_1H }>,
	ohlc_4h: Ohlcs<{ TF_4H }>,
	vol_1m: Volumes<{ TF_1MIN }>,
	vol_5m: Volumes<{ TF_5MIN }>,
	vol_1h: Volumes<{ TF_1H }>,
	vol_4h: Volumes<{ TF_4H }>,
	bars_1m: trading_data::Bars<{ TF_1MIN }>,
	bars_5m: trading_data::Bars<{ TF_5MIN }>,
	bars_1h: trading_data::Bars<{ TF_1H }>,
	bars_4h: trading_data::Bars<{ TF_4H }>,
	book: Book,
	book_top: BookTop,

	m1: Ring<Bar>,
	m5: Ring<Bar>,
	h1: Ring<Bar>,
	h4: Ring<Bar>,
	oi: Ring<Oi>,
	mc: Ring<Mc>,

	atr: Atr,
	momentum: Momentum,
	screener: StdScreener,
	change_1d: Change1d,
	change_3m: Change3m,
	volume_1m: Volume1m,
	volume_1h: Volume1h,
	imbalance: Imbalance,
	spread: Spread,
	classify: Classify,
	decision: Decision,
	deprecator: Deprecator,
	armed: Armed<Deprecator>,

	b_ohlc: [Vec<Ohlc>; 4],
	b_vol: [Vec<Volume>; 4],
	b_bars: [Vec<Bar>; 4],
	b_top: Vec<Option<BookTopSnap>>,
	b_atr: Vec<Option<f64>>,
	b_mom: Vec<Option<f64>>,
	b_c1d: Vec<Option<f64>>,
	b_c3m: Vec<Option<f64>>,
	b_v1m: Vec<f64>,
	b_v1h: Vec<Option<f64>>,
	b_imb: Vec<Option<f64>>,
	b_spr: Vec<Option<f64>>,
	b_dep: Vec<Option<Intent>>,

	/// `graph!`'s `__pending`: a terminal out commutates at the *next* tick's start, because the
	/// frame still borrows this one's batches at the end of it.
	pending: bool,
}

impl Direct {
	fn tick(&mut self, trades: TradeCols<'_>, deltas: DeltaFrame<'_>, anchor: Option<&BookShape>, oi: &[Oi], mc: &[Mc]) -> &[Option<Intent>] {
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
		self.ohlc_1m.emit((trades,), &mut self.b_ohlc[0]);
		self.ohlc_5m.emit((trades,), &mut self.b_ohlc[1]);
		self.ohlc_1h.emit((trades,), &mut self.b_ohlc[2]);
		self.ohlc_4h.emit((trades,), &mut self.b_ohlc[3]);
		self.vol_1m.emit((trades,), &mut self.b_vol[0]);
		self.vol_5m.emit((trades,), &mut self.b_vol[1]);
		self.vol_1h.emit((trades,), &mut self.b_vol[2]);
		self.vol_4h.emit((trades,), &mut self.b_vol[3]);
		let [o1, o5, oh, o4] = &self.b_ohlc;
		let [v1, v5, vh, v4] = &self.b_vol;
		let [b1, b5, bh, b4] = &mut self.b_bars;
		self.bars_1m.emit((o1, v1), b1);
		self.bars_5m.emit((o5, v5), b5);
		self.bars_1h.emit((oh, vh), bh);
		self.bars_4h.emit((o4, v4), b4);

		self.b_top.clear();
		let folded = self.book.advance((anchor, deltas));
		self.book_top.emit((folded, deltas), &mut self.b_top);

		self.m1.push(&self.b_bars[0]);
		self.m5.push(&self.b_bars[1]);
		self.h1.push(&self.b_bars[2]);
		self.h4.push(&self.b_bars[3]);
		self.oi.push(oi);
		self.mc.push(mc);

		self.b_atr.clear();
		self.atr.emit((&self.b_bars[0],), &mut self.b_atr);
		self.b_mom.clear();
		self.momentum.emit(
			(
				self.m5.hist::<Buffering<trading_data::Bars<{ TF_5MIN }>, { Horizon::Elems(181) }>>(),
				self.h4.hist::<Buffering<trading_data::Bars<{ TF_4H }>, { Horizon::Elems(181) }>>(),
			),
			&mut self.b_mom,
		);

		let hit = self.screener.advance((&self.b_bars[0], &self.b_mom));

		self.b_c1d.clear();
		self.b_c3m.clear();
		self.b_v1m.clear();
		self.b_v1h.clear();
		self.b_imb.clear();
		self.b_spr.clear();
		// A closed gate is not "advance with nothing": the node is never called, and its out is the
		// `Latent` reading — an empty run, or `None`.
		let decision = if hit {
			self.change_1d.emit(
				(
					&self.b_bars[0],
					self.h1.hist::<Buffering<trading_data::Bars<{ TF_1H }>, { Horizon::Span(Timeframe(TF_1D.0 + TF_1H.0)) }>>(),
				),
				&mut self.b_c1d,
			);
			self.change_3m
				.emit((self.m1.hist::<Buffering<trading_data::Bars<{ TF_1MIN }>, { Horizon::Span(TF_3MIN) }>>(),), &mut self.b_c3m);
			self.volume_1m.emit((&self.b_bars[0],), &mut self.b_v1m);
			self.volume_1h.emit(
				(&self.b_bars[0], self.h1.hist::<Buffering<trading_data::Bars<{ TF_1H }>, { Horizon::Elems(1) }>>()),
				&mut self.b_v1h,
			);
			self.imbalance.emit((&self.b_top,), &mut self.b_imb);
			self.spread.emit((&self.b_top,), &mut self.b_spr);
			let classified = self.classify.advance((
				true,
				&self.b_bars[0],
				self.m5.hist::<Buffering<trading_data::Bars<{ TF_5MIN }>, { Horizon::Elems(181) }>>(),
				self.h4.hist::<Buffering<trading_data::Bars<{ TF_4H }>, { Horizon::Elems(181) }>>(),
				&self.b_c1d,
				&self.b_c3m,
				&self.b_v1m,
				&self.b_v1h,
				&self.b_imb,
				&self.b_spr,
				// `Ring` bridges into `Hist` alone, and an `Mc` is never an absence — so the newest row it
				// ever held is what the frame's `Latest<McRoot>` holds.
				self.mc.hist::<Buffering<McRoot, { Horizon::Elems(1) }>>().all().last().copied(),
				self.oi.hist::<Buffering<OiRoot, OI_REACH>>(),
			));
			self.decision.advance((classified,))
		} else {
			None
		};

		self.b_dep.clear();
		if self.armed.advance((decision,)) {
			self.deprecator.emit((true, decision, &self.b_atr, &self.b_top), &mut self.b_dep);
		}
		if Episode::terminal(&self.b_dep.as_slice()) {
			self.pending = true;
		}
		&self.b_dep
	}
}
