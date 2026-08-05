//! The invariant the whole retention rests on: a period's *net* is its rows.
//!
//! [`BookChunk`] throws away the rows and keeps the last qty per `(side, price)`. That is only
//! sound if a book seeded at the period base and handed the net lands exactly where a book that
//! folded every row lands — for every run, deletes and corrections included. If it ever does not,
//! a woken book is quietly wrong and nothing downstream can tell.

use trading_data_core::{Aggregate, Book, BookChunk, BookDelta, BookShape, FrameKind, Local, Precision, PrecisionPriceQty, Side, Span, Ts, Venue};
use trading_data_dag::{Batch as _, Horizon};
use v_utils::TF_15MIN;

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
const REACH: Horizon = Horizon::Span(TF_15MIN);
const DEPTH: i32 = 40;

fn anchor() -> BookShape {
	BookShape {
		ts: Aggregate {
			venue_exec: Span::at(Ts::<Venue>::from_nanos(0)),
			local_recv: Span::at(Ts::<Local>::from_nanos(0)),
		},
		prec: PREC,
		bids: (0..DEPTH).map(|p| (p, p as u32 + 1)).collect(),
		asks: (DEPTH..2 * DEPTH).map(|p| (p, p as u32 + 1)).collect(),
	}
}

/// A run inside one period: a quarter deletes, a tenth corrections, prices inside the seeded window
/// so the book hovers at its depth rather than draining.
fn run(seed: u64, n: usize) -> Vec<BookDelta> {
	let mut rng = seed;
	(0..n as u64)
		.map(|i| {
			rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			let r = rng >> 33;
			let (side, base) = if r & 1 == 0 { (Side::Buy, 0) } else { (Side::Sell, DEPTH) };
			BookDelta {
				prec: PREC,
				ts_venue_exec: Ts::from_nanos(i as i64 * 1_000),
				ts_local_recv: Ts::from_nanos(i as i64 * 1_000),
				monotonic_seq: i + 1,
				kind: if r % 10 == 0 { FrameKind::Correction } else { FrameKind::Update },
				side,
				price: base + (r >> 1) as i32 % DEPTH,
				qty: if r % 4 == 0 { 0 } else { (r >> 8) as u32 % 1000 + 1 },
			}
		})
		.collect()
}

fn levels(b: &Book) -> (Vec<(i32, u32)>, Vec<(i32, u32)>) {
	(b.bids().to_vec(), b.asks().to_vec())
}

#[test]
fn a_period_s_net_is_that_period_s_rows() {
	for (seed, n, tick) in [(1u64, 400usize, 1usize), (7, 400, 13), (99, 1_000, 7), (12345, 40, 40)] {
		let rows = run(seed, n);
		let anchor = anchor();

		// row by row, as an awake book folds: every step continuous, nothing ever rebuilt.
		let (mut folded, mut chunk) = (Book::default(), BookChunk::default());
		for batch in rows.chunks(tick) {
			chunk.advance(batch, REACH);
			assert!(folded.step(Some(&anchor), &chunk), "the reference fold must stay synced");
		}

		// the whole run as one net, taken by a book that has no place in the chain at all
		let (mut woken, mut whole) = (Book::default(), BookChunk::default());
		whole.advance(&rows, REACH);
		assert!(woken.step(Some(&anchor), &whole), "a seeded book must take the net");

		assert_eq!(levels(&folded), levels(&woken), "seed {seed}, {n} rows in batches of {tick}");
		assert!(whole.levels() <= 2 * DEPTH as usize, "the net is bounded by depth, not by the {n} rows it folded");
	}
}

/// The reason the wake path is bounded: what the chunk holds tracks the book's depth, not the
/// stream's length. A regression here is the design's failure mode, and it is silent.
#[test]
fn the_net_costs_depth_and_not_row_count() {
	const ROWS: usize = 100_000;
	let mut chunk = BookChunk::default();
	chunk.advance(&run(4, ROWS), REACH);
	// one entry per price *touched*, and the run only ever touches the seeded window
	assert_eq!(chunk.levels(), 2 * DEPTH as usize, "{ROWS} rows collapse onto the levels they touch");
}
