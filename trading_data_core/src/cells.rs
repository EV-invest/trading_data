//! The root cells over this crate's lane holders, and [`Book`] as an ordinary graph node.
//!
//! `Cell`/`Node` are the dag's and these types are ours, so orphan rules make this the only crate
//! that may write these impls. Nothing else here knows the dag exists.

use trading_data_dag::{Blind, Buffering, Cell, DepOuts, Nudge, Over, node, slice_nudge};
use v_utils::TF_15MIN;

use crate::{Book, BookChunk, BookDelta, BookShape, TradeBuf, TradeCols};

pub struct Trades;
impl Cell for Trades {
	type Out<'t> = TradeCols<'t>;
}

impl Nudge for Trades {
	type Scratch = TradeBuf;

	fn stage<'t>(out: TradeCols<'t>, s: &mut TradeBuf, bump: Option<usize>, h: f64) -> f64 {
		s.clear();
		s.prec = out.prec;
		s.extend(out);
		bump.map_or(0.0, |slot| s.bump_last(slot, h))
	}

	fn view<'l>(s: &'l TradeBuf) -> TradeCols<'l> {
		s.cols(0..s.len())
	}
}

/// Anchors are [`Book`]'s input, not the graph's — nothing else should name this.
pub struct BookAnchors;
impl Cell for BookAnchors {
	type Out<'t> = Option<&'t BookShape>;
}

impl Nudge for BookAnchors {
	type Scratch = Option<BookShape>;

	fn stage(out: Option<&BookShape>, s: &mut Self::Scratch, _: Option<usize>, _: f64) -> f64 {
		*s = out.cloned();
		0.0 // structural: perturbing a level makes it a different book, not a nearby one
	}

	fn view(s: &Self::Scratch) -> Option<&BookShape> {
		s.as_ref()
	}
}

pub struct BookDeltas;
impl Cell for BookDeltas {
	type Out<'t> = &'t [BookDelta];
}

slice_nudge!(BookDeltas, BookDelta, BookChunk);

/// `Option<&Book>` is `Latent`, so the book **can be gated** — and a closed gate returns `None`
/// without pulling deps, so no checkpoint and no frame is even read.
impl Cell for Book {
	type Out<'t> = Option<&'t Book>;
}

/// `rewarms`: what makes the tick a latch costs recoverable is not the retention — a sleep past two
/// of the chunk's boundaries outruns that — but the *seek*, which `anchored` is the whole of. A feed
/// with no past to seek pins this awake instead ([`Awake`](trading_data_dag::Awake)), where it costs
/// nothing and says nothing.
#[node(anchored, rewarms)]
impl Blind for Book {
	/// Both deps are roots, which is what anchoring currently asks: a rewind reads them back out of
	/// the past, and a past is read per lane.
	///
	/// The reach back over the deltas is the *engine's* — a `Buffering` dep, so it accumulates whether
	/// or not this node is dark. `trading_data_persistence` reads its anchor-age bound off this.
	///
	/// `BookAnchors` is a dep and not merely a lane: `required_lanes` walks `DepSet::NAMES`, so
	/// dropping it here would stop the checkpoint lane loading and leave a replay nothing to seek.
	type Deps = (BookAnchors, Buffering<BookDeltas, Over<TF_15MIN>>);

	const WHY: &'static str = "an order book fold is not a scalar function of its deltas";

	fn advance<'t>(&'t mut self, (anchor, chunk): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.step(anchor, chunk).then_some(&*self)
	}
}

impl Nudge for Book {
	type Scratch = Option<Book>;

	fn stage(out: Option<&Book>, s: &mut Self::Scratch, _: Option<usize>, _: f64) -> f64 {
		match (out, s.as_mut()) {
			// `clone_from` so that a hand-written `Clone for Book` would keep the two level vectors
			// across ticks; `Book` derives its own, and a derived `clone_from` is `*self =
			// source.clone()`, so today this reuses nothing. The observer pays two book clones per
			// fired node and again per dep slot, which is what makes that worth closing.
			(Some(b), Some(dst)) => dst.clone_from(b),
			(Some(b), None) => *s = Some(b.clone()),
			(None, _) => *s = None,
		}
		0.0 // best bid/ask are levels, not a continuum: there is no nearby book
	}

	fn view(s: &Self::Scratch) -> Option<&Book> {
		s.as_ref()
	}
}
