//! The NT rows: one `BacktestEngine`, the tape from the same archive the graph reads, and the
//! strategy as fourteen actors on the msgbus.
//!
//! Building the tape walks `Replay` exactly as `td_graph` does, so both rows see the same trades and
//! the same book. That walk is setup and sits outside the probe — NT wants its whole stream up front
//! while the graph decodes per day, and timing one against the other would be timing parquet.

mod actors;

use std::{
	cell::RefCell,
	path::{Path, PathBuf},
	rc::Rc,
	sync::{Arc, atomic::Ordering},
};

use nautilus_backtest::{
	config::{BacktestEngineConfig, SimulatedVenueConfig},
	engine::BacktestEngine,
};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
	currencies::CURRENCY_MAP,
	data::{Data, OrderBookDelta, OrderBookDeltas, OrderBookDeltas_API, TradeTick, custom::CustomData, order::BookOrder},
	enums::{AccountType, AggressorSide, BookAction, BookType, CurrencyType, OmsType, OrderSide},
	identifiers::{InstrumentId, Symbol, TradeId, Venue},
	instruments::{CryptoPerpetual, Instrument as _, InstrumentAny},
	types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence_macros::custom_data;
use trading_data::{Book, DeltaCols, Exact, ExchangeName, Feed as _, LatencyConfig, ReadClock, Replay, Side, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};

use crate::harness::{COUNTERS, Digest, Probe, Row, publish};

#[custom_data(no_arrow)]
pub struct OiPoint {
	pub oi: f64,
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
}

#[custom_data(no_arrow)]
pub struct McPoint {
	pub market_cap: f64,
	pub ts_event: UnixNanos,
	pub ts_init: UnixNanos,
}

pub fn oi_type() -> nautilus_model::data::DataType {
	nautilus_model::data::DataType::new(stringify!(OiPoint), None, None)
}

pub fn mc_type() -> nautilus_model::data::DataType {
	nautilus_model::data::DataType::new(stringify!(McPoint), None, None)
}

/// `ts_event = ts_init = venue execution time`. The archive carries no per-element local stamp — the
/// receive column is a span over the whole batch — and the exec clock is the one NT's bar aggregator
/// must bucket on for its boundaries to be our `Ohlcs`' boundaries.
fn stamp(ns: i64) -> UnixNanos {
	UnixNanos::from(u64::try_from(ns).expect("the archive's window is after the epoch"))
}

/// The whole window as NT `Data`, plus the instrument the graph's precision implies.
///
/// The book is folded here by our own [`Book`] for one reason: it owns when a resync happens, and NT
/// has to be told. Every epoch bump becomes a `Clear` plus the full ladder, so the two books hold the
/// same levels at the same instants rather than merely starting the same.
fn tape() -> (Vec<Data>, InstrumentAny) {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);
	let kinds = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));

	let id = InstrumentId::new(Symbol::new(situation.bybit_symbol.replace('-', "")), Venue::new("BYBIT"));
	let mut out = Vec::new();
	let mut book = Book::default();
	let mut prec = None;

	for d in trading_days(situation) {
		let (start, end) = day_bounds(d);
		let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &kinds, latency, read_clock);
		while let Some(l) = feed.next() {
			let t = l.trades;
			if !t.is_empty() {
				prec.get_or_insert(t.prec);
				let (ps, qs) = (t.prec.price.scale(), t.prec.qty.scale());
				let (pd, qd) = (t.prec.price.0 as u8, t.prec.qty.0 as u8);
				for i in 0..t.len() {
					let ts = stamp(t.exec()[i].as_nanos());
					out.push(Data::Trade(TradeTick::new(
						id,
						Price::new(t.price[i] as f64 / ps, pd),
						Quantity::new(t.qty[i] as f64 / qs, qd),
						match t.side[i] {
							Side::Buy => AggressorSide::Buyer,
							Side::Sell => AggressorSide::Seller,
						},
						TradeId::new(&t.monotonic_seq[i].to_string()),
						ts,
						ts,
					)));
				}
			}

			let cols = l.deltas.cols();
			let epoch = book.epoch();
			// `step` is the only entry point that knows whether this frame folds at all; a frame that
			// left our book desynced left it unreadable, and NT is told nothing rather than told a lie.
			if book.step(l.anchor, l.deltas) && !cols.is_empty() {
				prec.get_or_insert(cols.prec);
				let ts = stamp(cols.exec()[cols.len() - 1].as_nanos());
				let deltas = match book.epoch() == epoch {
					true => incremental(id, cols),
					false => resync(id, &book, ts),
				};
				out.push(Data::Deltas(OrderBookDeltas_API::new(OrderBookDeltas::new(id, deltas))));
			}

			for o in l.oi {
				let ts = stamp(o.ts_venue_exec.as_nanos());
				out.push(Data::Custom(CustomData::from_arc(Arc::new(OiPoint::new(o.oi, ts, ts)))));
			}
			for m in l.mc {
				let ts = stamp(m.ts_local_exec.as_nanos());
				out.push(Data::Custom(CustomData::from_arc(Arc::new(McPoint::new(m.market_cap, ts, ts)))));
			}
		}
	}

	let prec = prec.expect("the window carried neither a trade nor a delta");
	let (pd, qd) = (prec.price.0 as u8, prec.qty.0 as u8);
	let quote = Currency::from("USDT");
	let base = Symbol::new(situation.bybit_symbol.replace('-', "").trim_end_matches("USDT").to_string());
	let base = *CURRENCY_MAP
		.lock()
		.expect("currency map is not poisoned")
		.entry(base.to_string())
		.or_insert_with(|| Currency::new(base.as_str(), 8, 0, base.as_str(), CurrencyType::Crypto));
	let instrument = InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
		id,
		id.symbol,
		base,
		quote,
		quote,
		false,
		pd,
		qd,
		Price::new(1.0 / prec.price.scale(), pd),
		Quantity::new(1.0 / prec.qty.scale(), qd),
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		None,
		UnixNanos::default(),
		UnixNanos::default(),
	));
	(out, instrument)
}

/// qty 0 is a level deletion in our lane and a `Delete` in NT's; everything else is the level's new
/// absolute size. `order_id` is left at 0 because `L2_MBP` overwrites it with a price-derived one.
fn incremental(id: InstrumentId, cols: DeltaCols<'_>) -> Vec<OrderBookDelta> {
	let (ps, qs) = (cols.prec.price.scale(), cols.prec.qty.scale());
	let (pd, qd) = (cols.prec.price.0 as u8, cols.prec.qty.0 as u8);
	(0..cols.len())
		.map(|i| {
			let ts = stamp(cols.exec()[i].as_nanos());
			let side = match cols.side[i] {
				Side::Buy => OrderSide::Buy,
				Side::Sell => OrderSide::Sell,
			};
			let action = match cols.qty[i] {
				0 => BookAction::Delete,
				_ => BookAction::Update,
			};
			let order = BookOrder::new(side, Price::new(cols.price[i] as f64 / ps, pd), Quantity::new(cols.qty[i] as f64 / qs, qd), 0);
			OrderBookDelta::new(id, action, order, 0, cols.monotonic_seq[i], ts, ts)
		})
		.collect()
}

fn resync(id: InstrumentId, book: &Book, ts: UnixNanos) -> Vec<OrderBookDelta> {
	let (ps, qs) = (book.prec().price.scale(), book.prec().qty.scale());
	let (pd, qd) = (book.prec().price.0 as u8, book.prec().qty.0 as u8);
	let clear = OrderBookDelta::new(
		id,
		BookAction::Clear,
		BookOrder::new(OrderSide::NoOrderSide, Price::new(0.0, pd), Quantity::new(0.0, qd), 0),
		0,
		0,
		ts,
		ts,
	);
	let level = |side, &(p, q): &(i32, u32)| {
		let order = BookOrder::new(side, Price::new(p as f64 / ps, pd), Quantity::new(q as f64 / qs, qd), 0);
		OrderBookDelta::new(id, BookAction::Add, order, 0, 0, ts, ts)
	};
	std::iter::once(clear)
		.chain(book.bids().iter().map(|l| level(OrderSide::Buy, l)))
		.chain(book.asks().iter().map(|l| level(OrderSide::Sell, l)))
		.collect()
}

fn engine(data: Vec<Data>, instrument: &InstrumentAny) -> BacktestEngine {
	let config = BacktestEngineConfig {
		logging: LoggerConfig::from_spec("bypass_logging").expect("bypass_logging is a valid spec"),
		bypass_logging: true,
		run_analysis: false,
		..Default::default()
	};
	let mut engine = BacktestEngine::new(config).expect("engine config is valid");
	engine
		.add_venue(
			SimulatedVenueConfig::builder()
				.venue(Venue::new("BYBIT"))
				.oms_type(OmsType::Netting)
				.account_type(AccountType::Margin)
				.book_type(BookType::L2_MBP)
				.starting_balances(vec![Money::from("1_000_000 USDT")])
				.build()
				.expect("venue config is valid"),
		)
		.expect("venue is addable");
	engine.add_instrument(instrument).expect("instrument matches its venue");
	engine.add_data(data, None, false, true).expect("the tape is non-empty");
	engine
}

/// Registration order is the msgbus' dispatch order within a topic, and the chain below depends on
/// it: everything that fills a ring off `bar1m` is registered before the screener that reads those
/// rings, and the classifier after the six indies whose outputs it latches.
pub fn run(name: &str, shallow_deep: bool, mut notes: Vec<String>) {
	let (data, instrument) = tape();
	let (trades, deltas) = data.iter().fold((0u64, 0u64), |(t, d), x| match x {
		Data::Trade(_) => (t + 1, d),
		Data::Deltas(x) => (t, d + x.deltas.len() as u64),
		_ => (t, d),
	});
	let events = data.len() as u64;
	let id = instrument.id();

	let digest = Rc::new(RefCell::new(Digest::default()));
	let mut e = engine(data.clone(), &instrument);
	e.add_actor(actors::Bars::new(id)).expect("actor ids are distinct");
	e.add_actor(actors::Book::new(id, shallow_deep)).expect("actor ids are distinct");
	e.add_actor(actors::Momenta::new()).expect("actor ids are distinct");
	e.add_actor(actors::Atrs::new()).expect("actor ids are distinct");
	e.add_actor(actors::C1d::new()).expect("actor ids are distinct");
	e.add_actor(actors::C3m::new()).expect("actor ids are distinct");
	e.add_actor(actors::V1m::new()).expect("actor ids are distinct");
	e.add_actor(actors::V1h::new()).expect("actor ids are distinct");
	e.add_actor(actors::Imb::new()).expect("actor ids are distinct");
	e.add_actor(actors::Spr::new()).expect("actor ids are distinct");
	e.add_actor(actors::Classifier::new()).expect("actor ids are distinct");
	e.add_actor(actors::Screen::new()).expect("actor ids are distinct");
	e.add_actor(actors::Decide::new()).expect("actor ids are distinct");
	e.add_actor(actors::Deprecate::new(digest.clone())).expect("actor ids are distinct");

	COUNTERS.trades.fetch_add(trades, Ordering::Relaxed);
	COUNTERS.deltas.fetch_add(deltas, Ordering::Relaxed);
	let probe = Probe::start();
	e.run(None, None, None, false).expect("the tape is sorted and the venue is registered");
	let total = probe.stop();
	e.dispose();

	// The same engine over the same tape with nothing subscribed: the DataEngine still routes every
	// element, so what this subtracts is the strategy and the bus traffic it causes, not the replay.
	let mut bare = engine(data, &instrument);
	let began = std::time::Instant::now();
	bare.run(None, None, None, false).expect("the tape is sorted and the venue is registered");
	let feed_s = began.elapsed().as_secs_f64();
	bare.dispose();

	notes.push("bars are NT's internal TimeBarAggregator, which closes on its clock; ours closes when the next out-of-bucket trade lands".into());
	notes.push("no tick: each node fires on the event its deps are clocked by, so imbalance/spread read the latest book rather than this tick's".into());
	notes.push("4h and 1h closes reach their rings on the next 5m/1m close, since NT does not order aggregators against each other".into());
	// NT's actor registry is a thread-local that outlives `dispose`, so the sink is read through its
	// handle rather than unwrapped out of it.
	publish(Row::new(name, events, &digest.borrow(), total, feed_s, notes));
}
