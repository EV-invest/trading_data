//! Minimal live graph: trades + book-delta roots feed Cvd (running signed notional per trade) and
//! BookFlow (running signed delta qty per book delta). 1m bars won't close in 15s, so the proof
//! rides on these per-event outputs, not bars.

use trading_data::{BookDelta, Cell, DepOuts, Flat, Node, Nudge, Trade};
use v_utils::trades::Side;

macro_rules! slice_nudge {
	($C:ty, $E:ty) => {
		impl Nudge for $C {
			type Scratch = Vec<$E>;

			fn stage<'t>(out: &'t [$E], s: &mut Vec<$E>, bump: Option<usize>, h: f64) {
				s.clear();
				s.extend_from_slice(out);
				if let Some(slot) = bump
					&& let Some(last) = s.last_mut()
				{
					*last = Flat::nudge(&*last, slot, h);
				}
			}

			fn view<'l>(s: &'l Vec<$E>) -> &'l [$E] {
				s
			}
		}
	};
}

pub struct Trades;
pub struct BookRoot;
/// Cumulative volume delta: running Σ signed notional, one element per trade.
#[derive(Clone, Default)]
pub struct Cvd {
	sum: f64,
	buf: Vec<f64>,
}
/// Running Σ signed book-delta qty (bid +, ask −), one element per delta. A `qty == 0.0` delta is a
/// level delete and contributes nothing.
#[derive(Clone, Default)]
pub struct BookFlow {
	sum: f64,
	buf: Vec<f64>,
}
fn signed_notional(t: &Trade) -> f64 {
	let n = t.price * t.qty;
	match t.side {
		Side::Buy => n,
		Side::Sell => -n,
	}
}

impl Cell for Trades {
	type Out<'t> = &'t [Trade];
}
slice_nudge!(Trades, Trade);

impl Cell for BookRoot {
	type Out<'t> = &'t [BookDelta];
}
slice_nudge!(BookRoot, BookDelta);

impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
impl Node for Cvd {
	type Deps = (Trades,);

	fn advance<'t>(&'t mut self, (trades,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for t in trades {
			self.sum += signed_notional(t);
			self.buf.push(self.sum);
		}
		&self.buf
	}
}
slice_nudge!(Cvd, f64);

impl Cell for BookFlow {
	type Out<'t> = &'t [f64];
}
impl Node for BookFlow {
	type Deps = (BookRoot,);

	fn advance<'t>(&'t mut self, (deltas,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.buf.clear();
		for d in deltas {
			self.sum += match d.side {
				Side::Buy => d.qty,
				Side::Sell => -d.qty,
			};
			self.buf.push(self.sum);
		}
		&self.buf
	}
}
slice_nudge!(BookFlow, f64);

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[Trade], book: BookRoot[BookDelta] };
	out TickOut;
	cvd: Cvd,
	book_flow: BookFlow,
}
