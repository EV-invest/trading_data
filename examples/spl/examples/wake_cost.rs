//! What a dark book actually pays to wake — measured off the feed, not argued from the consts.
//!
//! Recoverability is a property of the delta lane alone: `Book::step` takes `Reach::Net` iff the
//! book's last seq still sits inside the chunk's period, and otherwise needs a `BookAnchors` row on
//! the very tick it woke. So one pass recording, per tick, the chunk's period base and whether an
//! anchor landed answers the question for *any* sleep, without running a strategy.
//!
//! The second half is the trade the tumble period actually is: a longer period means fewer wakes
//! that need an anchor at all, paid for in retained levels. `Span(1d)` over a one-day replay is the
//! never-tumbling net, priced.

use std::path::{Path, PathBuf};

use trading_data::{Batch as _, BookChunk, Exact, ExchangeName, Feed as _, Horizon, LatencyConfig, ReadClock, Replay, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};
use v_utils::{TF_1D, TF_1H, TF_4H, TF_15MIN, Timeframe};

#[tokio::main]
async fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	let catalog = ensure_lanes(&cache, situation).await;
	let lanes = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	let day = trading_days(situation)[0];
	let (start, end) = day_bounds(day);

	const PERIODS: [Timeframe; 4] = [TF_15MIN, TF_1H, TF_4H, TF_1D];
	let mut chunks = PERIODS.map(|_| BookChunk::default());
	let mut peak = [0usize; 4];
	let mut tumbles = [0usize; 4];
	let mut base = [i64::MIN; 4];
	let mut broke = [false; 4];

	let (mut ts, mut anchor_ts) = (Vec::new(), Vec::new());
	let mut with_deltas = 0u64;
	let mut f = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock);
	while let Some(l) = f.next() {
		for i in 0..PERIODS.len() {
			chunks[i].advance(l.deltas, Horizon::Over(PERIODS[i]));
			peak[i] = peak[i].max(chunks[i].levels());
			broke[i] |= !chunks[i].contiguous();
			let period = Horizon::ns(PERIODS[i]);
			if let Some(d) = l.deltas.last() {
				let b = d.ts_venue_exec.as_nanos().div_euclid(period) * period;
				if base[i] != i64::MIN && b != base[i] {
					tumbles[i] += 1;
				}
				base[i] = b;
			}
		}
		with_deltas += !l.deltas.is_empty() as u64;
		if l.anchor.is_some() {
			anchor_ts.push(l.ts_venue.as_nanos());
		}
		ts.push(l.ts_venue.as_nanos());
	}

	let pct = |v: &mut Vec<f64>, p: f64| {
		v.sort_by(f64::total_cmp);
		v[((v.len() - 1) as f64 * p) as usize]
	};
	println!("{} ticks, {with_deltas} carrying deltas, {} carrying an anchor", ts.len(), anchor_ts.len());
	let mut gaps: Vec<f64> = anchor_ts.windows(2).map(|w| (w[1] - w[0]) as f64 / 1e9).collect();
	println!(
		"anchor arrival gap, s:  p50 {:.1}  p90 {:.1}  p99 {:.1}  max {:.1}",
		pct(&mut gaps, 0.5),
		pct(&mut gaps, 0.9),
		pct(&mut gaps, 0.99),
		pct(&mut gaps, 1.0)
	);

	// A wake past a tumble is dark until the next tick carrying an anchor. Read at every tick: the
	// worst case, and the one a screener firing after a quiet stretch actually hits.
	let mut nxt = None;
	let mut blind: Vec<f64> = Vec::new();
	for t in ts.iter().rev() {
		if anchor_ts.binary_search(t).is_ok() {
			nxt = Some(*t);
		}
		if let Some(a) = nxt {
			blind.push((a - t) as f64 / 1e9);
		}
	}
	println!(
		"blind s after a wake past a tumble:  p50 {:.1}  p90 {:.1}  p99 {:.1}  max {:.1}",
		pct(&mut blind, 0.5),
		pct(&mut blind, 0.9),
		pct(&mut blind, 0.99),
		pct(&mut blind, 1.0)
	);

	println!("\n  tumble period   tumbles/day   net peak (levels)   recording gapless");
	for i in 0..PERIODS.len() {
		println!("  {:>13}   {:>11}   {:>17}   {}", PERIODS[i].to_string(), tumbles[i], peak[i], !broke[i]);
	}
	println!("\n  rows the lane would have to retain instead: {with_deltas} ticks' worth of deltas");
}
