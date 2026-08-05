//! What replaying a recorded catalog costs: rows woven, and the high-water resident set it took.
//!
//! Peak RSS is the number this exists for. A reader that collects a whole parquet file before
//! yielding its first row and one that streams it decode the same bytes and run the same
//! instructions; they differ only in how many of those bytes are live at once. Wall clock on this
//! box is meaningless (a dozen agents share it), so it is printed but not to be believed.
//!
//! Usage: `read_cost <catalog-root> [PAIR]` — e.g. `read_cost tmp/spl_cache/PROVEUSDT/catalog`.

use std::{
	env, fs,
	time::{Duration, Instant},
};

use trading_data_core::{ExchangeName, Instrument, Symbol, Ts};
use trading_data_persistence::{Catalog, Exact, Feed, LaneKind, LatencyConfig, ReadClock, Replay};

const LANES: &[LaneKind] = &[LaneKind::Trades, LaneKind::BookDeltas, LaneKind::BookAnchors, LaneKind::Oi];

/// The kernel's own high-water mark: the peak is reached and released inside `Replay::new`, so
/// sampling from this process after the fact would read a trough.
fn peak_rss_kb() -> u64 {
	let status = fs::read_to_string("/proc/self/status").expect("procfs is mounted");
	status
		.lines()
		.find_map(|l| l.strip_prefix("VmHWM:"))
		.expect("VmHWM is in every /proc/self/status")
		.trim_end_matches(" kB")
		.trim()
		.parse()
		.expect("VmHWM is a decimal count of kB")
}

fn main() {
	let mut args = env::args().skip(1);
	let root = args.next().expect("catalog root is the first argument");
	let pair = args.next().unwrap_or_else(|| "PROVE-USDT".into());

	let catalog = Catalog::new(root);
	let symbol = Symbol::new(pair.as_str().try_into().expect("PAIR is BASE-QUOTE"), Instrument::Perp);
	let latency = LatencyConfig {
		p68: Duration::from_millis(10),
		p95: Duration::from_millis(30),
		p997: Duration::from_millis(90),
		seed: 1,
	};

	let began = Instant::now();
	let mut replay = Replay::new(
		&catalog,
		ExchangeName::Bybit,
		symbol,
		Ts::from_nanos(0),
		Ts::from_nanos(i64::MAX),
		LANES,
		latency,
		ReadClock::from(Exact::from(Duration::from_millis(100))),
	);
	let load_s = began.elapsed().as_secs_f64();
	let load_peak = peak_rss_kb();

	let mut steps = 0u64;
	while let Some(l) = replay.next() {
		steps += 1;
		std::hint::black_box(&l);
	}

	println!("steps            {steps}");
	println!("load             {load_s:.3}s");
	println!("peak rss (load)  {:.1} MiB", load_peak as f64 / 1024.0);
	println!("peak rss (total) {:.1} MiB", peak_rss_kb() as f64 / 1024.0);
}
