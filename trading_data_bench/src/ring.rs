//! The one bridge the benches need into the DAG's private history type: a `Hist<'_, T>` an actor
//! can hand a node, built outside a `Graph`.
//!
//! `Hist`'s fields are private and only [`trading_data::Buffering`] constructs one — but
//! `Buffering<C, H>: Nudge` is public, and `Nudge::view` is exactly "read this scratch back as the
//! dep's out". So the scratch *is* the buffer's [`Rows`], and this owns one.
//!
//! Retention is unbounded where the frame's `Buffer<C, K>` trims to `K`, and the two are
//! nonetheless the same reading. `Hist::all` re-cuts to the consumer's declared horizon, so extra
//! history behind it is invisible; `trailing_at` at `Horizon::Elems` never consults the watermark;
//! and at `Horizon::Span` the window is complete iff `cut >= watermark`, which holds here exactly
//! when it holds there — a trimming buffer's watermark is the last *dropped* stamp, always at or
//! behind `cut`, and before its first drop it is the first stamp ever seen, which is what this
//! holds forever.
//!
//! ponytail: unbounded, because every series a bench buffers is small — a day of 1m bars is 1440
//! rows, and OI publishes 288 times. Trim to the join of the declared reaches if a window ever runs
//! long enough for that to matter.

use trading_data::{Batch as _, Horizon, Nudge, Rows, Stamped};

pub struct Ring<T> {
	rows: Rows<T>,
}

// hand-written: `derive` would demand `T: Default`, which no retained row is.
impl<T> Default for Ring<T> {
	fn default() -> Self {
		Self { rows: Rows::default() }
	}
}
impl<T: Copy + Stamped> Ring<T> {
	/// One tick's batch, empty batches included — `fresh` is what the reading is rate-preserving
	/// against, so a tick that published nothing has to say so.
	pub fn push(&mut self, fresh: &[T]) {
		self.rows.advance(fresh, Horizon::Elems(usize::MAX));
	}

	/// The history as the reader's own `Buffering<C, H>` sees it. `B` is that dep, written out at
	/// the call site — the horizon lives in the type, and nothing else can supply it.
	pub fn hist<B: Nudge<Scratch = Rows<T>>>(&self) -> B::Out<'_> {
		B::view(&self.rows)
	}
}
