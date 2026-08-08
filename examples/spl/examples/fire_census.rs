//! Per-node fire census over one replayed day, split by what the screener said on the tick.
//! Answers "is a node that never lights actually being skipped, or is the viz stale" without a
//! browser: the engine's own `Observer` sees exactly what the recorder would.
//!
//! Two graphs over the one tape, because a sleep is only worth what the driver lets it be worth:
//! `runs` is the live reading, where an anchored node is pinned awake because nothing would fetch
//! back what it skipped, and `runs-rw` is the backtest's, where the feed's `Past` is there to.

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
};

use trading_data::{Exact, ExchangeName, Feed as _, Fire, LatencyConfig, Observer, ReadClock, Replay, Step, Want, required_lanes};
use trading_data_spl::{
	config::Config,
	day_bounds, ensure_lanes,
	nodes::{Batches, Graph},
	symbol, trading_days,
};

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

	let (mut graph, mut slept) = (Graph::default(), Graph::default());
	let (mut census, mut rewound) = (Census::default(), Census::default());
	let mut f = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock);
	while let Some(Step { lanes: l, mut past }) = f.step() {
		census.ticks += 1;
		let ts = l.ts_venue.as_nanos();
		let (trades, deltas, anchors, oi, mc) = (l.trades, l.deltas, l.anchor, l.oi, l.mc);
		let b = || Batches { trades, deltas, anchors, oi, mc };
		graph.tick_obs(ts, b(), &mut census);
		slept.tick_rewind_obs(ts, b(), &mut past, &mut rewound);
	}

	println!("{} ticks, {} of them with `{GATE}` true\n", census.ticks, census.open_ticks);
	println!("  {:<44} {:>10} {:>10} {:>10} {:>10}", "node (step order)", "runs", "runs-rw", "fires", "of-which-open");
	for n in &census.order {
		let (runs, all, open) = census.hits[n];
		println!("  {n:<44} {runs:>10} {:>10} {all:>10} {open:>10}", rewound.hits[n].0);
	}
}
#[derive(Default)]
struct Census {
	/// insertion order is step order, which is topo order
	order: Vec<&'static str>,
	/// per node: (runs, fires, fires on a tick the gate read true). A run that did not fire is a
	/// clocked node between publications; a fire without a run is not a thing.
	hits: HashMap<&'static str, (u64, u64, u64)>,
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
			(0, 0, 0)
		});
		e.0 += fire.ran as u64;
		if fire.vals.is_some() {
			e.1 += 1;
			e.2 += self.gate_open as u64;
		}
	}
}
