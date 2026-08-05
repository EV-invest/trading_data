//! What a lane rotation costs the thread that calls it: the ZSTD encode, at the archival level and
//! at the level a live tee uses.
//!
//! This is the stall item 10 is about. A `BookDelta` feather rotates at 256 MiB by its own policy,
//! and on a live feed the thread standing in that encode is the one holding the wire — a gap in the
//! recording, not merely a slow write. The delta lane's constructor is crate-private, so the trade
//! lane stands in for it here; the encode is the same code either way.
//!
//! Usage: `write_cost <catalog-root> [PAIR]` — reads a real trade lane, re-writes it, times it.

use std::{env, time::Instant};

use tempfile::tempdir;
use trading_data_core::{ExchangeName, Instrument, Precision, PrecisionPriceQty, Symbol, Ts};
use trading_data_persistence::{Catalog, Feather, RotationPolicy, Row as _, Trade, read_trades};

fn main() {
	let mut args = env::args().skip(1);
	let root = args.next().expect("catalog root is the first argument");
	let pair = args.next().unwrap_or_else(|| "PROVE-USDT".into());

	let src = Catalog::new(root);
	let symbol = Symbol::new(pair.as_str().try_into().expect("PAIR is BASE-QUOTE"), Instrument::Perp);
	let rows: Vec<Trade> = read_trades(&src, ExchangeName::Bybit, symbol, Ts::from_nanos(0), Ts::from_nanos(i64::MAX))
		.expect("open trades lane")
		.collect();
	assert!(!rows.is_empty(), "the catalog holds no trades for {symbol}");
	println!("rows  {}", rows.len());

	for level in [3, 1] {
		let dir = tempdir().expect("temp dir");
		let out = Catalog::new(dir.path());
		let prec = PrecisionPriceQty::new(Precision(2), Precision(3));
		let mut f = Feather::<Trade>::new(ExchangeName::Binance, symbol, prec, RotationPolicy { zstd_level: level, ..Trade::POLICY });
		for &r in &rows {
			f.push(r);
		}
		let began = Instant::now();
		f.flush(&out).expect("flush").expect("non-empty batch");
		println!("zstd {level}  {:.3}s", began.elapsed().as_secs_f64());
	}
}
