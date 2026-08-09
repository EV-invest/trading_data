//! M1: what an anchored book's sleeps are actually *shaped* like, on the real graph and the real
//! feed.
//!
//! A mean occupancy figure is worse than useless here, because the gate is bursty: one long sleep is
//! nearly free — a single wake pays for all of it — while a flipping stretch pays a wake per flip.
//! So what is reported is the distribution of sleep run-lengths, in ticks and in the delta rows a
//! sleep skipped, and the one number the trade turns on: **delta rows skipped per wake**.
//!
//! The demand is not re-derived from the gates. `Rewound::rewind` is called at the node's sweep
//! position exactly when the node is going to advance, so a `Past` that records being asked *is* the
//! demand, read off the sweep that computes it.

use std::path::{Path, PathBuf};

use trading_data::{Book, Exact, ExchangeName, Feed as _, LatencyConfig, Past, ReadClock, Replay, Rewound, Step, required_lanes};
use trading_data_spl::{
	config::Config,
	day_bounds, ensure_lanes,
	nodes::{Batches, Graph},
	symbol, trading_days,
};

fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	let catalog = tokio::runtime::Runtime::new().expect("a runtime for the archive fetch").block_on(ensure_lanes(&cache, situation));
	let lanes = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	// the whole window, not one day: the first day screens nothing at all — momentum wants its whole
	// span of 5m closes before the gate can open once — so a truncated range reports one long sleep
	// and calls it a distribution.
	let days = trading_days(situation);

	let mut graph = Graph::default();
	let (mut ticks, mut rows, mut wakes) = (0u64, 0u64, 0u64);
	// one entry per sleep: (ticks dark, delta rows that went by while dark)
	let mut sleeps: Vec<(u64, u64)> = Vec::new();
	let mut dark = (0u64, 0u64);
	// venue timestamps of the wakes, for the flip-storm window below
	let mut woke_at: Vec<i64> = Vec::new();

	for d in &days {
		let (start, end) = day_bounds(*d);
		let mut f = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock);
		while let Some(Step { lanes: l, past }) = f.step() {
			let (ts, n) = (l.ts_venue.as_nanos(), l.deltas.len() as u64);
			ticks += 1;
			rows += n;

			let mut w = Watch { past, ran: false, woke: false };
			let b: Batches<'_> = l.into();
			std::hint::black_box(graph.tick_rewind(ts, b, &mut w));

			match w.ran {
				true => {
					if dark.0 > 0 {
						sleeps.push(dark);
						dark = (0, 0);
					}
					if w.woke {
						wakes += 1;
						woke_at.push(ts);
					}
				}
				false => dark = (dark.0 + 1, dark.1 + n),
			}
		}
	}
	if dark.0 > 0 {
		sleeps.push(dark);
	}

	println!("{} days, {ticks} ticks, {rows} delta rows, {} sleeps, {wakes} wakes", days.len(), sleeps.len());

	// the histogram, not the mean: a few long sleeps and a flip-storm are the same average and
	// nothing like the same cost.
	println!("\n  sleep length (ticks)   sleeps   rows skipped");
	const BUCKETS: [u64; 7] = [1, 2, 8, 64, 512, 4096, u64::MAX];
	let mut lo = 0;
	for hi in BUCKETS {
		let hit: Vec<&(u64, u64)> = sleeps.iter().filter(|s| s.0 > lo && s.0 <= hi).collect();
		if !hit.is_empty() {
			let label = if hi == u64::MAX { format!("{}+", lo + 1) } else { format!("{}..{hi}", lo + 1) };
			println!("  {label:>20}   {:>6}   {:>12}", hit.len(), hit.iter().map(|s| s.1).sum::<u64>());
		}
		lo = hi;
	}

	let mut lens: Vec<f64> = sleeps.iter().map(|s| s.1 as f64).collect();
	let pct = |v: &mut Vec<f64>, p: f64| {
		v.sort_by(f64::total_cmp);
		v[((v.len() - 1) as f64 * p) as usize]
	};
	if !lens.is_empty() {
		println!(
			"\nrows skipped per sleep:  p50 {:.0}  p90 {:.0}  p99 {:.0}  max {:.0}",
			pct(&mut lens, 0.5),
			pct(&mut lens, 0.9),
			pct(&mut lens, 0.99),
			pct(&mut lens, 1.0)
		);
	}

	// the one number the trade turns on, against what a wake pays: depth plus the rows since the
	// checkpoint the seek lands on.
	let skipped: u64 = sleeps.iter().map(|s| s.1).sum();
	println!("rows skipped per wake:   {:.0}  (of {rows} folded per day)", skipped as f64 / wakes.max(1) as f64);

	// the flip-storm: not how often it sleeps on average, but the worst stretch, where every wake
	// pays its own tail.
	let (mut i, mut storm) = (0usize, 0usize);
	for j in 0..woke_at.len() {
		while woke_at[j] - woke_at[i] > 60_000_000_000 {
			i += 1;
		}
		storm = storm.max(j - i + 1);
	}
	println!("worst flip-storm:        {storm} wakes inside one 60s stretch");
}

/// The feed's own past, plus the one bit the sweep does not otherwise report: whether it asked.
struct Watch<'a> {
	past: Past<'a>,
	ran: bool,
	woke: bool,
}

impl Rewound<Book> for Watch<'_> {
	fn rewind(&mut self, b: &mut Book) {
		self.ran = true;
		let before = b.seq();
		self.past.rewind(b);
		// a wake is a rewind that moved the cursor; the steady-state call finds it already there
		self.woke = b.seq() != before;
	}
}
