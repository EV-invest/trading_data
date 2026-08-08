//! Per-node fire census over one replayed day, split by what the screener said on the tick.
//! Answers "is a node that never lights actually being skipped, or is the viz stale" without a
//! browser: the engine's own `Observer` sees exactly what the recorder would.

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};

use trading_data::{Exact, ExchangeName, Feed as _, Fire, LatencyConfig, Observer, ReadClock, Replay, Want, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};

const GATE: &str = "StdScreener";

#[tokio::main]
async fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	let catalog = ensure_lanes(&cache, situation).await;
	let lanes = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	let (start, end) = day_bounds(trading_days(situation)[0]);

	let mut graph = Graph::default();
	let mut census = Census::default();
	let mut f = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock);
	while let Some(l) = f.next() {
		census.ticks += 1;
		graph.tick_obs(l.ts_venue.as_nanos(), l.into(), &mut census);
	}

	println!("{} ticks, {} of them with `{GATE}` true\n", census.ticks, census.open_ticks);
	println!("  {:<44} {:>10} {:>10}", "node (step order)", "fires", "of-which-open");
	for n in &census.order {
		let (all, open) = census.hits[n];
		println!("  {n:<44} {all:>10} {open:>10}");
	}
}
#[derive(Default)]
struct Census {
	/// insertion order is step order, which is topo order
	order: Vec<&'static str>,
	/// per node: (fires, fires on a tick the gate read true)
	hits: HashMap<&'static str, (u64, u64)>,
	gate_open: bool,
	open_ticks: u64,
	ticks: u64,
}

impl Observer for Census {
	fn want(&self, _: &'static str) -> Want {
		Want::Vals
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		if node == GATE {
			self.gate_open = fire.vals.is_some_and(|v| v[0] != 0.0);
			self.open_ticks += self.gate_open as u64;
		}
		let e = self.hits.entry(node).or_insert_with(|| {
			self.order.push(node);
			(0, 0)
		});
		if fire.vals.is_some() {
			e.0 += 1;
			e.1 += self.gate_open as u64;
		}
	}
}
