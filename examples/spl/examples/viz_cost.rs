//! Where the `main.rs` replay's wall clock actually goes: one day, walked three ways.
//!
//! `td_graph`'s bench row already times the graph without an observer; what it cannot say is how
//! much of the 3-minute replay is the `Viz` recorder attached to every fire. This walks the same
//! feed with the observer off and on, so the two numbers are read off one process on one day.

use std::{
	path::{Path, PathBuf},
	time::Instant,
};

use exec_viz::Viz;
use trading_data::{Cell, Exact, ExchangeName, Feed as _, LatencyConfig, ReadClock, Replay, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};
use v_utils::*;

const SCROLLBACK: usize = 20_000;

fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);
	let lanes = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	let day = trading_days(situation)[0];
	let feed = || {
		let (start, end) = day_bounds(day);
		Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock)
	};

	let mut ticks = 0u64;
	let began = Instant::now();
	let mut f = feed();
	while let Some(l) = f.next() {
		ticks += 1;
		std::hint::black_box(&l);
	}
	let feed_s = began.elapsed().as_secs_f64();

	let began = Instant::now();
	let mut graph = Graph::default();
	let mut f = feed();
	while let Some(l) = f.next() {
		std::hint::black_box(graph.tick(l.ts_venue.as_nanos(), l.into()));
	}
	let plain_s = began.elapsed().as_secs_f64();

	let began = Instant::now();
	let mut graph = Graph::default();
	let mut recorder = Viz::new(Some(<trading_data::Bars<{ TF_1MIN }> as Cell>::NAME), SCROLLBACK, 60_000);
	let mut f = feed();
	while let Some(l) = f.next() {
		let ts_ns = l.ts_venue.as_nanos();
		std::hint::black_box(graph.tick_obs(ts_ns, l.into(), recorder.at(ts_ns)));
	}
	let obs_s = began.elapsed().as_secs_f64();

	println!("{ticks} ticks over {day}");
	println!("feed only          {feed_s:>8.2}s");
	println!("+ graph.tick       {plain_s:>8.2}s  (graph {:.2}s)", plain_s - feed_s);
	println!("+ graph.tick_obs   {obs_s:>8.2}s  (Viz {:.2}s)", obs_s - plain_s);
}
