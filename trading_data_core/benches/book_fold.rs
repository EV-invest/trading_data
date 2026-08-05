use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use trading_data_core::{Aggregate, Book, BookChunk, BookDelta, BookShape, FrameKind, Local, Precision, PrecisionPriceQty, Side, Span, Ts, Venue};
use trading_data_dag::{Batch as _, Horizon};
use v_utils::TF_15MIN;

const FRAMES: usize = 50_000;
const PER_FRAME: usize = 4;

const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(5),
};

/// The lane's shape: a book of `levels` per side, then frames touching a few levels each, a quarter
/// of them deletes. Prices stay inside the seeded window, so the book hovers at its depth rather
/// than draining.
fn stream(levels: i32) -> (BookShape, Vec<BookDelta>) {
	let anchor = BookShape {
		ts: Aggregate {
			venue_exec: Span::at(Ts::<Venue>::from_nanos(0)),
			local_recv: Span::at(Ts::<Local>::from_nanos(0)),
		},
		prec: PREC,
		bids: (0..levels).map(|p| (p, p as u32 + 1)).collect(),
		asks: (levels..2 * levels).map(|p| (p, p as u32 + 1)).collect(),
	};

	let mut buf = Vec::new();
	let mut rng = 0x2545_f491_4f6c_dd1d_u64;
	for i in 0..(FRAMES * PER_FRAME) as u64 {
		rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		let r = rng >> 33;
		let (side, base) = if r & 1 == 0 { (Side::Buy, 0) } else { (Side::Sell, levels) };
		let price = base + (r >> 1) as i32 % levels;
		let qty = if r % 4 == 0 { 0 } else { (r >> 8) as u32 % 1000 + 1 };
		buf.push(BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(i as i64),
			ts_local_recv: Ts::from_nanos(i as i64),
			monotonic_seq: i + 1,
			kind: FrameKind::Update,
			side,
			price,
			qty,
		});
	}
	(anchor, buf)
}

#[library_benchmark]
#[bench::top20(args = (20), setup = stream)]
#[bench::depth200(args = (200), setup = stream)]
fn fold_deltas((anchor, buf): (BookShape, Vec<BookDelta>)) {
	let mut b = Book::default();
	let mut chunk = BookChunk::default();
	for f in 0..FRAMES {
		let seed = (f == 0).then_some(&anchor);
		chunk.advance(&buf[f * PER_FRAME..(f + 1) * PER_FRAME], Horizon::Span(TF_15MIN));
		black_box(b.step(seed, &chunk));
	}
	black_box(&b);
}

library_benchmark_group!(name = book_fold; benchmarks = fold_deltas);
main!(library_benchmark_groups = book_fold);
