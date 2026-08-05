//! The root cells over this crate's lane holders, and [`Book`] as an ordinary graph node.
//!
//! `Cell`/`Node` are the dag's and these types are ours, so orphan rules make this the only crate
//! that may write these impls. Nothing else here knows the dag exists.

use trading_data_dag::{Buffering, Cell, DepOuts, Horizon, Node, Nudge, node, slice_nudge};
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

#[node]
impl Node for Book {
	/// The reach back over the deltas is exactly one checkpoint interval, and it is the *engine's* —
	/// a `Buffering` dep, so it accumulates whether or not this node is dark, which is the whole of
	/// what makes the book gateable. `trading_data_persistence` reads its anchor-age bound off this.
	///
	/// The `BookChunk` behind it tumbles on the same absolute boundary the checkpoint is written on,
	/// so a wake is one resync plus one net — depth, not the length of the sleep.
	type Deps = (BookAnchors, Buffering<BookDeltas, { Horizon::Span(TF_15MIN) }>);

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
