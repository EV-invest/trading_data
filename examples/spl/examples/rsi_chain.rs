//! Scratch driver for the decomposed RSI chain: six hours of the situation's first day, no viz, no
//! checks — just the publish counts and the last value at each stage, so `RsiDelta → AvgGain/AvgLoss
//! → Rsi` can be eyeballed against the single-node version it replaced.

use std::path::PathBuf;

use trading_data::{BatchWindow, Exact, Feed as _, LatencyConfig, Replay, Ts, required_lanes};
use trading_data_core::ExchangeName;
use trading_data_spl::{
	config::Config,
	day_bounds, ensure_lanes,
	nodes::{Graph, RsiValues},
	symbol, trading_days,
};

fn main() {
	let cfg = Config::load(&PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);

	let (start, _) = day_bounds(trading_days(situation)[0]);
	let end = Ts::from_nanos(start.as_nanos() + 6 * 3600 * 1_000_000_000);
	let lanes = required_lanes::<Graph>();
	let mut feed = Replay::new(
		&catalog,
		ExchangeName::Bybit,
		symbol(situation),
		start,
		end,
		&lanes,
		LatencyConfig::from(cfg.backtest.arrival_latency),
		BatchWindow::from(Exact::from_nanos(100_000_000)),
	);

	let mut graph = Graph::default();
	let (mut bars, mut deltas, mut gains, mut losses, mut rsis) = (0u64, 0u64, 0u64, 0u64, 0u64);
	let mut last: Option<(f64, f64, RsiValues)> = None;
	while let Some(l) = feed.next() {
		let out = graph.tick(l.into());
		bars += out.bar_5m.len() as u64;
		deltas += out.rsi_delta.len() as u64;
		gains += out.avg_gain.iter().flatten().count() as u64;
		losses += out.avg_loss.iter().flatten().count() as u64;
		rsis += out.rsi.iter().flatten().count() as u64;
		for i in 0..out.rsi.len() {
			if let (Some(g), Some(l), Some(r)) = (out.avg_gain[i], out.avg_loss[i], out.rsi[i]) {
				last = Some((g, l, r));
			}
		}
	}
	println!("5m bars {bars}, deltas {deltas}, avg_gain {gains}, avg_loss {losses}, rsi {rsis}");
	match last {
		Some((g, l, r)) => println!("last: gain {g:.6} loss {l:.6} → actual {:.4} smooth {:.4}", r.actual, r.smooth),
		None => println!("rsi never warmed"),
	}
	assert_eq!(deltas, bars.saturating_sub(1), "one delta per bar past the first");
	assert_eq!(gains, losses, "the two Wilder legs warm together");
}
