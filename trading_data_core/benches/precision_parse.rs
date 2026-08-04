use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use trading_data_core::Precision;

/// One ob200 line's worth of levels: 400 per side, a price and a qty each.
const LEVELS: usize = 400;

/// Binance-shaped: prices at the venue tick, quantities zero-padded to eight decimals.
fn levels() -> Vec<(String, String)> {
	(0..LEVELS)
		.map(|i| (format!("{}.{:02}", 42000 + i, i % 100), format!("{}.{:08}", i % 7, (i * 137) % 1000 * 100_000)))
		.collect()
}

#[library_benchmark]
#[bench::ob200(setup = levels)]
fn parse_levels(levels: Vec<(String, String)>) -> i64 {
	let (price, qty) = (Precision(2), Precision(8));
	// Summed rather than discarded: a parse whose result nothing reads is a parse LLVM may delete.
	let mut acc = 0i64;
	for (p, q) in &levels {
		acc += black_box(price.parse_i32(p)) as i64 + black_box(qty.parse_u32(q)) as i64;
	}
	acc
}

library_benchmark_group!(name = precision_parse; benchmarks = parse_levels);
main!(library_benchmark_groups = precision_parse);
