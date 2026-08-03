//! The baseline row: `Replay` → `Graph::tick`, the loop `main` runs minus the `Viz` recorder.
//!
//! `tick` rather than `tick_obs`: an inactive observer costs nothing but a bench that carried one
//! would be measuring a facility the NT rows have no counterpart for.

#[path = "harness/mod.rs"]
mod harness;

use std::{
	hint::black_box,
	path::{Path, PathBuf},
	sync::atomic::Ordering,
};

use harness::{COUNTERS, Digest, Probe, Row, publish};
use trading_data::{Exact, ExchangeName, Feed as _, LatencyConfig, ReadClock, Replay, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};

fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let mut graph = Graph::default();

	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);
	let kinds = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	let days = trading_days(situation);
	let feed = |d| {
		let (start, end) = day_bounds(d);
		Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &kinds, latency, read_clock)
	};

	let probe = Probe::start();
	let mut digest = Digest::default();
	let mut ticks = 0u64;
	for d in &days {
		let mut f = feed(*d);
		while let Some(l) = f.next() {
			COUNTERS.trades.fetch_add(l.trades.len() as u64, Ordering::Relaxed);
			COUNTERS.deltas.fetch_add(l.deltas.cols().len() as u64, Ordering::Relaxed);
			ticks += 1;
			for intent in graph.tick(l.ts_venue.as_nanos(), l.into()).deprecator.iter().flatten() {
				digest.feed(intent);
			}
		}
	}
	let total = probe.stop();

	// The same decode and the same tick cadence with the strategy removed. `Replay` is deterministic
	// given the config's latency seed, so this walks exactly the batches the run above did.
	let began = std::time::Instant::now();
	for d in &days {
		let mut f = feed(*d);
		while let Some(l) = f.next() {
			black_box(&l);
		}
	}
	let feed_s = began.elapsed().as_secs_f64();

	// Every wired node runs at most once per tick, so the tick count *is* each shared site's pass
	// count — but the graph does not report how often the screener gate was shut, and the note
	// below is there so the zeroes are not read as "never ran".
	for c in [&COUNTERS.screener, &COUNTERS.classify, &COUNTERS.decision, &COUNTERS.deprecator] {
		c.store(ticks, Ordering::Relaxed);
	}
	publish(Row::new(
		"td_graph",
		ticks,
		&digest,
		total,
		feed_s,
		vec![
			"pass counts are tick counts: the gated subtree's skips are not observable from outside the graph".into(),
			"bars_closed is not an output of this graph, so it reads 0 here".into(),
			"also computes Rsi, which the NT rows have no counterpart for".into(),
			"total includes the per-day parquet decode; NT decodes its whole tape at setup, so only the compute column compares".into(),
		],
	));
}
