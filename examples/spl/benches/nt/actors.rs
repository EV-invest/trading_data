//! The strategy as fourteen NautilusTrader actors: our exact node types, called by hand, with every
//! hop between them a JSON `Signal` on the msgbus — the shape scam_pump_liqs runs.
//!
//! The DAG has ticks; an actor system has events, and that is the whole of the difference being
//! measured. Each node here fires on the event its `Deps` are clocked by — [`Atr`] on a 1m close,
//! [`Momentum`] on a 5m close, [`Imbalance`]/[`Spread`] on a book delta, [`Deprecator`] on a book
//! read — which is the same dependency order the graph steps in, minus the tick that synchronises
//! them. Where that costs fidelity it is listed in the bench's `notes`, not hidden.
//!
//! Registration order is load-bearing: the msgbus dispatches a topic's handlers in subscription
//! order, and `on_start` runs in the order actors were added. The indies are therefore registered
//! before [`Screen`], and [`Classifier`] after both, so a nested publish always finds its inputs
//! already written.

use std::{cell::RefCell, fmt::Debug, rc::Rc, sync::atomic::Ordering};

use nautilus_common::{
	actor::{DataActor, DataActorConfig, DataActorCore},
	nautilus_actor,
	signal::Signal,
};
use nautilus_model::{
	data::{Bar as NtBar, BarSpecification, BarType, CustomData, OrderBookDeltas},
	enums::{AggregationSource, BarAggregation, BookType, PriceType},
	identifiers::{ActorId, InstrumentId},
	orderbook::OrderBook,
};
use serde::{Deserialize, Serialize};
use trading_data::{
	Armed, Bar, Blind as _, Buffering, Direction, Elems, Episode, Fold, Latch as _, Level as _, Local, Mc, McRoot, Oi, OiRoot, Over, Predicate, Run as _, Runs as _, Scan, Ts, Usd,
	bench::{COUNTERS, Digest, ring::Ring},
};
use trading_data_spl::{
	DEPTH,
	nodes::{Atr, BookTopSnap, Change1d, Change3m, Classified, Classify, Decided, Decision, Deprecator, Imbalance, Intent, Momentum, OiReach, Spread, StdScreener, VolUsd},
};
use v_utils::*;

use crate::nt::{McPoint, OiPoint};

const BAR1M: &str = "bar1m";
const BAR5M: &str = "bar5m";
const BAR1H: &str = "bar1h";
const BAR4H: &str = "bar4h";
const BOOKTOP: &str = "booktop";
const MOMENTUM: &str = "momentum";
const SCREENER: &str = "screener";
const ATR: &str = "atr";
const CHANGE1D: &str = "change1d";
const CHANGE3M: &str = "change3m";
const VOLUSD1M: &str = "volusd1m";
const VOLUSD1H: &str = "volusd1h";
const IMBALANCE: &str = "imbalance";
const SPREAD: &str = "spread";
const CLASSIFIED: &str = "classified";
const DECISION: &str = "decision";
/// Whether a position is open — the only thing the Shallow/Deep tier switches on.
const SITUATION: &str = "situation";

/// The four aggregations the strategy reads, in minutes. NT builds each internally off the trade
/// stream, so the bench pays for the aggregation the graph's `Ohlcs`/`Volumes` also pay for.
const STEPS: [(usize, BarAggregation, &str); 4] = [
	(4, BarAggregation::Hour, BAR4H),
	(1, BarAggregation::Hour, BAR1H),
	(5, BarAggregation::Minute, BAR5M),
	(1, BarAggregation::Minute, BAR1M),
];

// --- wire shapes -------------------------------------------------------------------------------
// None of `Bar`, `BookTopSnap`, `Classified` or `Decided` is serialisable, and making them so would
// be a source change for a bench. Mirroring them here is not overhead smuggled into the NT row: a
// bus that moves JSON has to encode *something*, and this is that something.

/// The gate's payload: the verdict plus the 1m bar it was drawn from, so a subscriber needs no
/// second subscription to `bar1m` and no assumption about dispatch order.
type Gate = (bool, BarDto);
#[derive(Deserialize, Serialize)]
struct BarDto {
	ts_close: i64,
	open: f64,
	high: f64,
	low: f64,
	close: f64,
	vol_base: f64,
}

impl From<&Bar> for BarDto {
	fn from(b: &Bar) -> Self {
		Self {
			ts_close: b.ts_close.as_nanos(),
			open: b.open,
			high: b.high,
			low: b.low,
			close: b.close,
			vol_base: b.vol_base,
		}
	}
}

impl From<BarDto> for Bar {
	fn from(d: BarDto) -> Self {
		Self {
			ts_close: Ts::from_nanos(d.ts_close),
			open: d.open,
			high: d.high,
			low: d.low,
			close: d.close,
			vol_base: d.vol_base,
		}
	}
}

#[derive(Deserialize, Serialize)]
struct TopDto {
	ts_ns: i64,
	best_bid: f64,
	best_ask: f64,
	top20_bid_depth_usd: f64,
	top20_ask_depth_usd: f64,
}

impl From<&BookTopSnap> for TopDto {
	fn from(s: &BookTopSnap) -> Self {
		Self {
			ts_ns: s.ts_ns,
			best_bid: s.best_bid,
			best_ask: s.best_ask,
			top20_bid_depth_usd: s.top20_bid_depth_usd,
			top20_ask_depth_usd: s.top20_ask_depth_usd,
		}
	}
}

impl From<TopDto> for BookTopSnap {
	fn from(d: TopDto) -> Self {
		Self {
			ts_ns: d.ts_ns,
			best_bid: d.best_bid,
			best_ask: d.best_ask,
			top20_bid_depth_usd: d.top20_bid_depth_usd,
			top20_ask_depth_usd: d.top20_ask_depth_usd,
		}
	}
}

#[derive(Deserialize, Serialize)]
struct DecidedDto {
	long: bool,
	flat: bool,
	size: f64,
}

impl From<&Decided> for DecidedDto {
	fn from(d: &Decided) -> Self {
		Self {
			long: d.direction == Direction::Long,
			flat: d.direction == Direction::Flat,
			size: d.size.0,
		}
	}
}

impl From<DecidedDto> for Decided {
	fn from(d: DecidedDto) -> Self {
		Self {
			direction: match (d.flat, d.long) {
				(true, _) => Direction::Flat,
				(false, true) => Direction::Long,
				(false, false) => Direction::Short,
			},
			size: Usd(d.size),
		}
	}
}

fn encode<T: Serialize>(v: &T) -> String {
	serde_json::to_string(v).expect("every wire shape here is a plain struct of numbers")
}

fn decode<T: for<'de> Deserialize<'de>>(s: &Signal) -> T {
	serde_json::from_str(&s.value).unwrap_or_else(|e| panic!("signal `{}` carried {}: {e}", s.name, s.value))
}

/// Struct, native-core wiring, `Debug` and a constructor. Fields in the optional paren group are
/// constructor arguments; the braced ones are state and start at `Default`.
macro_rules! actor {
	($(#[$doc:meta])* $name:ident $(($($arg:ident : $at:ty),* $(,)?))? { $($field:ident : $ty:ty),* $(,)? }) => {
		$(#[$doc])*
		pub struct $name {
			core: DataActorCore,
			$($($arg: $at,)*)?
			$($field: $ty,)*
		}

		nautilus_actor!($name);

		impl Debug for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				f.write_str(stringify!($name))
			}
		}

		impl $name {
			pub fn new($($($arg: $at),*)?) -> Self {
				Self {
					core: DataActorCore::new(DataActorConfig {
						actor_id: Some(ActorId::from(stringify!($name))),
						..Default::default()
					}),
					$($($arg,)*)?
					$($field: Default::default(),)*
				}
			}
		}
	};
}

// --- roots -------------------------------------------------------------------------------------

actor!(Bars(id: InstrumentId) {});

impl DataActor for Bars {
	fn on_start(&mut self) -> anyhow::Result<()> {
		let id = self.id;
		for (step, aggregation, _) in STEPS {
			let spec = BarSpecification::new(step, aggregation, PriceType::Last);
			self.subscribe_bars(BarType::new(id, spec, AggregationSource::Internal), None, None);
		}
		Ok(())
	}

	fn on_bar(&mut self, bar: &NtBar) -> anyhow::Result<()> {
		COUNTERS.bars_closed.fetch_add(1, Ordering::Relaxed);
		let spec = bar.bar_type.spec();
		let (step, aggregation) = (spec.step.get(), spec.aggregation);
		let (.., name) = STEPS
			.into_iter()
			.find(|&(s, a, _)| s == step && a == aggregation)
			.unwrap_or_else(|| panic!("subscribed only to {STEPS:?}, got {step} {aggregation:?}"));
		let bar = Bar {
			ts_close: Ts::from_nanos(bar.ts_event.as_u64() as i64),
			open: bar.open.as_f64(),
			high: bar.high.as_f64(),
			low: bar.low.as_f64(),
			close: bar.close.as_f64(),
			vol_base: bar.volume.as_f64(),
		};
		self.publish_signal(name, encode(&BarDto::from(&bar)), (bar.ts_close.as_nanos() as u64).into());
		Ok(())
	}
}

actor!(
	/// NT's `OrderBook` standing in for our `Book`, read as our [`BookTopSnap`]. The snap's fields are
	/// public, so `BookTop` itself is not wired here — the read is the node.
	Book(id: InstrumentId, deep: bool) { book: Option<OrderBook>, open: bool });

impl Book {
	/// The Shallow/Deep edge: `optimized_nt` opens the delta feed only while there is a situation to
	/// read a book for, which is the whole of what the tiering buys.
	///
	/// Reopening starts from an empty ladder. A book that skipped deltas is stale, not shallow, and
	/// the honest reading of one is none — [`BookTopSnap`] is `None` until both sides refill, exactly
	/// as our own `Book` reads while unseeded.
	fn tier(&mut self, want: bool) {
		if self.open == want {
			return;
		}
		self.open = want;
		let id = self.id;
		match want {
			true => {
				self.book = Some(OrderBook::new(id, BookType::L2_MBP));
				self.subscribe_book_deltas(id, BookType::L2_MBP, None, None, false, None);
			}
			false => self.unsubscribe_book_deltas(id, None, None),
		}
	}
}

impl DataActor for Book {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.book = Some(OrderBook::new(self.id, BookType::L2_MBP));
		match self.deep {
			true => {
				self.subscribe_signal(SCREENER, None);
				self.subscribe_signal(SITUATION, None);
			}
			false => self.tier(true),
		}
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let want = match signal.name.as_str() {
			SCREENER => decode::<Gate>(signal).0 || self.open,
			_ => decode::<bool>(signal),
		};
		self.tier(want);
		Ok(())
	}

	fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
		let book = self.book.as_mut().expect("built in on_start");
		book.apply_deltas(deltas)?;
		let snap = book.best_bid_price().zip(book.best_ask_price()).map(|(bid, ask)| BookTopSnap {
			ts_ns: deltas.ts_event.as_u64() as i64,
			best_bid: bid.as_f64(),
			best_ask: ask.as_f64(),
			top20_bid_depth_usd: book.bids(Some(DEPTH)).map(|l| l.exposure()).sum(),
			top20_ask_depth_usd: book.asks(Some(DEPTH)).map(|l| l.exposure()).sum(),
		});
		self.publish_signal(BOOKTOP, encode(&snap.as_ref().map(TopDto::from)), deltas.ts_event);
		Ok(())
	}
}

// --- ungated derivations -----------------------------------------------------------------------

actor!(Atrs { node: Atr, out: Vec<Option<f64>> });

impl DataActor for Atrs {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR1M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let bar = [Bar::from(decode::<BarDto>(signal))];
		self.out.clear();
		Fold::emit(&mut self.node, (&bar,), &mut self.out);
		self.publish_signal(ATR, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(Momenta { node: Momentum, m5: Ring<Bar>, h4: Ring<Bar>, pending: Vec<Bar>, out: Vec<Option<f64>> });

impl DataActor for Momenta {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR4H, None);
		self.subscribe_signal(BAR5M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let bar = Bar::from(decode::<BarDto>(signal));
		if signal.name == BAR4H {
			self.pending.push(bar);
			return Ok(());
		}
		self.h4.push(&self.pending);
		self.pending.clear();
		self.m5.push(&[bar]);
		self.out.clear();
		self.node.emit(
			(
				self.m5.hist::<Buffering<trading_data::Bars<{ TF_5MIN }>, Elems<181>>>(),
				self.h4.hist::<Buffering<trading_data::Bars<{ TF_4H }>, Elems<181>>>(),
			),
			&mut self.out,
		);
		self.publish_signal(MOMENTUM, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(
	/// The gate. Everything downstream of it subscribes to `screener` rather than to `bar1m`, so the
	/// verdict and the bar it was computed from cannot arrive out of order.
	Screen { node: StdScreener, mom: Option<f64> });

impl DataActor for Screen {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(MOMENTUM, None);
		self.subscribe_signal(BAR1M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		if signal.name == MOMENTUM {
			// the frame's `Latest<Momentum>`: a level, so it is never cleared after a read.
			if let Some(v) = decode::<Vec<Option<f64>>>(signal).iter().rev().find_map(|x| *x) {
				self.mom = Some(v);
			}
			return Ok(());
		}
		let bar = [Bar::from(decode::<BarDto>(signal))];
		COUNTERS.screener.fetch_add(1, Ordering::Relaxed);
		let hit = Predicate::advance(&mut self.node, (&bar, self.mom));
		self.publish_signal(SCREENER, encode(&(hit, BarDto::from(&bar[0]))), signal.ts_event);
		Ok(())
	}
}

// --- the indies ----------------------------------------------------------------------------------
// Each fires on the event its `Deps` are clocked by, and none of them knows the screener exists: an
// actor graph has no gate to propagate, so the work is done and then thrown away on a miss. That is
// the cost the tiering in `optimized_nt` claws back, and the only reason it exists.

actor!(C1d { node: Change1d, h1: Ring<Bar>, pending: Vec<Bar>, out: Vec<Option<f64>> });

impl DataActor for C1d {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR1H, None);
		self.subscribe_signal(BAR1M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		if signal.name == BAR1H {
			self.pending.push(decode::<BarDto>(signal).into());
			return Ok(());
		}
		let bar = [Bar::from(decode::<BarDto>(signal))];
		self.h1.push(&self.pending);
		self.pending.clear();
		self.out.clear();
		Scan::emit(
			&mut self.node,
			(&bar, self.h1.hist::<Buffering<trading_data::Bars<{ TF_1H }>, Over<{ Timeframe(TF_1D.0 + TF_1H.0) }>>>()),
			&mut self.out,
		);
		self.publish_signal(CHANGE1D, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(C3m { node: Change3m, m1: Ring<Bar>, out: Vec<Option<f64>> });

impl DataActor for C3m {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR1M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		self.m1.push(&[Bar::from(decode::<BarDto>(signal))]);
		self.out.clear();
		Scan::emit(&mut self.node, (self.m1.hist::<Buffering<trading_data::Bars<{ TF_1MIN }>, Over<TF_3MIN>>>(),), &mut self.out);
		self.publish_signal(CHANGE3M, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(VolUsd1m { node: VolUsd<{ TF_1MIN }>, out: Vec<f64> });

impl DataActor for VolUsd1m {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR1M, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let bar = [Bar::from(decode::<BarDto>(signal))];
		self.out.clear();
		Scan::emit(&mut self.node, (&bar,), &mut self.out);
		self.publish_signal(VOLUSD1M, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(VolUsd1h { node: VolUsd<{ TF_1H }>, out: Vec<f64> });

impl DataActor for VolUsd1h {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BAR1H, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let bar = [Bar::from(decode::<BarDto>(signal))];
		self.out.clear();
		Scan::emit(&mut self.node, (&bar,), &mut self.out);
		self.publish_signal(VOLUSD1H, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(Imb { node: Imbalance, top: Vec<Option<BookTopSnap>>, out: Vec<Option<f64>> });

impl DataActor for Imb {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BOOKTOP, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		self.top.clear();
		self.top.push(decode::<Option<TopDto>>(signal).map(BookTopSnap::from));
		self.out.clear();
		Scan::emit(&mut self.node, (&self.top,), &mut self.out);
		self.publish_signal(IMBALANCE, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

actor!(Spr { node: Spread, top: Vec<Option<BookTopSnap>>, out: Vec<Option<f64>> });

impl DataActor for Spr {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(BOOKTOP, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		self.top.clear();
		self.top.push(decode::<Option<TopDto>>(signal).map(BookTopSnap::from));
		self.out.clear();
		Scan::emit(&mut self.node, (&self.top,), &mut self.out);
		self.publish_signal(SPREAD, encode(&self.out), signal.ts_event);
		Ok(())
	}
}

// --- the decision chain ------------------------------------------------------------------------

actor!(Classifier {
	node: Classify,
	oi: Ring<Oi>,
	mc: Ring<Mc>,
	mom: Option<f64>,
	pending_oi: Vec<Oi>,
	pending_mc: Vec<Mc>,
	c1d: Vec<Option<f64>>,
	c3m: Vec<Option<f64>>,
	vol_usd_1m: Vec<f64>,
	vol_usd_1h: Option<f64>,
	imb: Vec<Option<f64>>,
	spr: Vec<Option<f64>>,
});

impl DataActor for Classifier {
	fn on_start(&mut self) -> anyhow::Result<()> {
		for name in [MOMENTUM, CHANGE1D, CHANGE3M, VOLUSD1M, VOLUSD1H, IMBALANCE, SPREAD, SCREENER] {
			self.subscribe_signal(name, None);
		}
		self.subscribe_data(crate::nt::oi_type(), None, None);
		self.subscribe_data(crate::nt::mc_type(), None, None);
		Ok(())
	}

	fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
		if let Some(p) = data.data.as_any().downcast_ref::<OiPoint>() {
			self.pending_oi.push(Oi {
				ts_venue_exec: Ts::from_nanos(p.ts_event.as_u64() as i64),
				ts_venue_send: None,
				ts_local_recv: None,
				oi: p.oi,
			});
		} else if let Some(p) = data.data.as_any().downcast_ref::<McPoint>() {
			self.pending_mc.push(Mc {
				ts_local_exec: Ts::<Local>::from_nanos(p.ts_event.as_u64() as i64),
				market_cap: p.market_cap,
				rank: None,
			});
		}
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		match signal.name.as_str() {
			// the frame's `Latest<Momentum>`: a level, so it is never cleared after a read.
			MOMENTUM => {
				if let Some(v) = decode::<Vec<Option<f64>>>(signal).iter().rev().find_map(|x| *x) {
					self.mom = Some(v);
				}
				return Ok(());
			}
			CHANGE1D => return Ok(self.c1d = decode(signal)),
			CHANGE3M => return Ok(self.c3m = decode(signal)),
			VOLUSD1M => return Ok(self.vol_usd_1m = decode(signal)),
			// the frame's `Latest<VolUsd<1h>>`: a level, so it is never cleared after a read.
			VOLUSD1H => {
				if let Some(v) = decode::<Vec<f64>>(signal).last() {
					self.vol_usd_1h = Some(*v);
				}
				return Ok(());
			}
			IMBALANCE => return Ok(self.imb = decode(signal)),
			SPREAD => return Ok(self.spr = decode(signal)),
			_ => (),
		}
		let (hit, bar) = decode::<Gate>(signal);
		self.oi.push(&self.pending_oi);
		self.pending_oi.clear();
		self.mc.push(&self.pending_mc);
		self.pending_mc.clear();
		if !hit {
			return Ok(());
		}
		COUNTERS.classify.fetch_add(1, Ordering::Relaxed);
		let bar = [Bar::from(bar)];
		let out = self.node.advance((
			true,
			&bar,
			self.mom,
			&self.c1d,
			&self.c3m,
			&self.vol_usd_1m,
			self.vol_usd_1h,
			&self.imb,
			&self.spr,
			// `Ring` bridges into `Hist` alone, and an `Mc` is never an absence — so the newest row it
			// ever held is what the frame's `Latest<McRoot>` holds.
			self.mc.hist::<Buffering<McRoot, Elems<1>>>().all().last().copied(),
			self.oi.hist::<Buffering<OiRoot, OiReach>>(),
		));
		self.publish_signal(CLASSIFIED, encode(&out.map(|c| c.0.to_vec())), signal.ts_event);
		Ok(())
	}
}

actor!(Decide { node: Decision });

impl DataActor for Decide {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(CLASSIFIED, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		let c = decode::<Option<Vec<f64>>>(signal).map(|v| Classified(v.try_into().expect("the classifier published its own outcome vector")));
		COUNTERS.decision.fetch_add(1, Ordering::Relaxed);
		let out = self.node.advance((c,));
		self.publish_signal(DECISION, encode(&out.as_ref().map(DecidedDto::from)), signal.ts_event);
		Ok(())
	}
}

actor!(
	/// Terminal sink. `Armed`'s deferred commutation is the one piece of `graph!` that has no NT
	/// counterpart at all, so it is spelled out: a terminal intent re-arms nothing until the next book
	/// read, and the node is rebuilt when it does.
	Deprecate(digest: Rc<RefCell<Digest>>) {
	node: Deprecator,
	armed: Armed<Deprecator>,
	decision: Option<Decided>,
	atr: Option<f64>,
	out: Vec<Option<Intent>>,
	pending: bool,
	live: bool,
});

impl DataActor for Deprecate {
	fn on_start(&mut self) -> anyhow::Result<()> {
		self.subscribe_signal(ATR, None);
		self.subscribe_signal(DECISION, None);
		self.subscribe_signal(BOOKTOP, None);
		Ok(())
	}

	fn on_signal(&mut self, signal: &Signal) -> anyhow::Result<()> {
		match signal.name.as_str() {
			// the frame's `Latest<Atr>`: a level, and ungated, so neither a read nor the commutation
			// below clears it.
			ATR => {
				if let Some(v) = decode::<Vec<Option<f64>>>(signal).iter().rev().find_map(|x| *x) {
					self.atr = Some(v);
				}
				return Ok(());
			}
			DECISION => return Ok(self.decision = decode::<Option<DecidedDto>>(signal).map(Decided::from)),
			_ => (),
		}
		if self.pending {
			self.pending = false;
			self.armed.commutate();
			self.node = Deprecator::default();
		}
		let top = [decode::<Option<TopDto>>(signal).map(BookTopSnap::from)];
		self.out.clear();
		COUNTERS.deprecator.fetch_add(1, Ordering::Relaxed);
		if self.armed.advance((self.decision,)) {
			self.node.emit((true, self.decision, self.atr, &top), &mut self.out);
		}
		self.decision = None;
		if Episode::terminal(&self.out.as_slice()) {
			self.pending = true;
		}
		let live = self.out.iter().flatten().next().is_some_and(|i| !i.terminal);
		if live != self.live {
			self.live = live;
			self.publish_signal(SITUATION, encode(&live), signal.ts_event);
		}
		let mut digest = self.digest.borrow_mut();
		for intent in self.out.iter().flatten() {
			digest.feed(intent);
		}
		Ok(())
	}
}
