//! The tape `book_gating.rs` and `book_rewind.rs` are both driven off — levels in, levels out, and
//! not one assertion. The graphs differ (one is `#[node(anchored)]`, the other is not) and so do the
//! drivers; what does not differ is what a level *is*, and that is all this holds.

use trading_data::{Book, BookDelta, BookShape, FrameKind, Precision, PrecisionPriceQty, Side, Ts};

pub const PREC: PrecisionPriceQty = PrecisionPriceQty {
	price: Precision(2),
	qty: Precision(4),
};
/// One tumble of the retained net, in nanoseconds — the same grid the checkpoint is written on.
pub const PERIOD: i64 = 900_000_000_000;

/// Both sides as raw levels, which is the whole of what "the same book" means here.
pub type Shape = (Vec<(i32, u32)>, Vec<(i32, u32)>);
pub type Read = Option<(u64, Shape)>;

pub fn anchor(bids: &[(i32, u32)], asks: &[(i32, u32)]) -> BookShape {
	BookShape {
		prec: PREC,
		bids: bids.iter().copied().collect(),
		asks: asks.iter().copied().collect(),
		..Default::default()
	}
}

/// One run of levels at the given sequence numbers — the gapless chain the shadow book writes.
pub fn run(ts_ns: i64, levels: &[(u64, Side, i32, u32)]) -> Vec<BookDelta> {
	levels
		.iter()
		.map(|&(seq, side, price, qty)| BookDelta {
			prec: PREC,
			ts_venue_exec: Ts::from_nanos(ts_ns),
			ts_local_recv: Ts::from_nanos(ts_ns),
			monotonic_seq: seq,
			kind: FrameKind::Update,
			side,
			price,
			qty,
		})
		.collect()
}

pub fn read(b: Option<&Book>) -> Read {
	b.map(|b| (b.epoch(), (b.bids().to_vec(), b.asks().to_vec())))
}
