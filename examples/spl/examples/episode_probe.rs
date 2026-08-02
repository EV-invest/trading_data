//! Scratch driver for the arming path: the full window, no viz and no FD observer, counting only
//! what stands between a screener hit and an intent — `Screener → Classify → Armed → Deprecator`.

use std::path::PathBuf;

use trading_data::{BatchWindow, Exact, ExchangeName, Feed as _, LatencyConfig, Replay, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};

fn main() {
	let cfg = Config::load(&PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);
	let lanes = required_lanes::<Graph>();

	let mut graph = Graph::default();
	let (mut ticks, mut hits, mut classifications, mut armed_ticks, mut ran, mut intents) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
	let (mut atr_while_armed, mut top_while_armed) = (0u64, 0u64);
	let (mut first_hit, mut first_armed) = (None, None);
	// the entry condition: `Deprecator` enters only inside `for d in top`, so a classification landing
	// on a tick that carried no book snapshot is one nothing can act on.
	let (mut class_with_book, mut atr_warm) = (0u64, false);
	let mut first_actionable = None;
	// is the miss about the screener's timing, or do a bar close and a book read simply never share a
	// tick? `Bar1m` is clocked by trades and `BookTop` by deltas — one weave, two lanes.
	let (mut bar_ticks, mut book_ticks, mut both) = (0u64, 0u64, 0u64);
	for d in &trading_days(situation) {
		let (start, end) = day_bounds(*d);
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
		while let Some(l) = feed.next() {
			ticks += 1;
			let out = graph.tick(l.into());
			hits += out.std_screener as u64;
			classifications += out.classify.is_some() as u64;
			if out.std_screener {
				first_hit.get_or_insert(ticks);
			}
			if out.armed {
				armed_ticks += 1;
				first_armed.get_or_insert(ticks);
				atr_while_armed += out.atr.iter().flatten().count() as u64;
				top_while_armed += out.book_top.len() as u64;
			}
			let (bar, book) = (!out.bar_1m.is_empty(), out.book_top.iter().flatten().next().is_some());
			bar_ticks += bar as u64;
			book_ticks += book as u64;
			both += (bar && book) as u64;
			atr_warm |= out.atr.iter().flatten().next().is_some();
			if out.classify.is_some() && out.book_top.iter().flatten().next().is_some() {
				class_with_book += 1;
				if atr_warm {
					first_actionable.get_or_insert(ticks);
				}
			}
			ran += u64::from(!out.deprecator.is_empty());
			intents += out.deprecator.iter().flatten().count() as u64;
		}
	}
	println!("{ticks} ticks: hits {hits} (first at {first_hit:?}), classifications {classifications}");
	println!("armed on {armed_ticks} ticks (first at {first_armed:?}); while armed: {atr_while_armed} atr publishes, {top_while_armed} book reads");
	println!("deprecator advanced on {ran} ticks, emitted {intents} intents");
	println!("classifications landing on a tick that also carried a book snapshot: {class_with_book} (first actionable at {first_actionable:?})");
	println!("ticks carrying a 1m bar: {bar_ticks}; a book read: {book_ticks}; both: {both}");
}
