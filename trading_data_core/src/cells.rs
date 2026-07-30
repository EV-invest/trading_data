//! The root cells over this crate's lane holders, and [`Book`] as an ordinary graph node.
//!
//! `Cell`/`Node` are the dag's and these types are ours, so orphan rules make this the only crate
//! that may write these impls. Nothing else here knows the dag exists.

use trading_data_dag::{Cell, DepOuts, Node, Nudge};

use crate::{Book, BookShape, DeltaBuf, DeltaFrame, TradeBuf, TradeCols};

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
	type Out<'t> = DeltaFrame<'t>;
}

impl Nudge for BookDeltas {
	type Scratch = DeltaBuf;

	fn stage<'t>(out: DeltaFrame<'t>, s: &mut DeltaBuf, bump: Option<usize>, h: f64) -> f64 {
		s.clear();
		s.prec = out.cols().prec;
		s.extend(out);
		bump.map_or(0.0, |slot| s.bump_last(slot, h))
	}

	fn view<'l>(s: &'l DeltaBuf) -> DeltaFrame<'l> {
		s.frame(0..s.len())
	}
}

/// `Option<&Book>` is `Latent`, so the book **can be gated** — and a closed gate returns `None`
/// without pulling deps, so no checkpoint and no frame is even read. `HISTORIC = false` is sound
/// precisely because, unlike a recurrence, a book re-warms from a checkpoint: gating it off and
/// back on costs one desync and one resync, not a warmup it can never recover.
impl Cell for Book {
	type Out<'t> = Option<&'t Book>;
}

impl Node for Book {
	type Deps = (BookAnchors, BookDeltas);

	const HISTORIC: bool = false;

	fn advance<'t>(&'t mut self, (anchor, deltas): DepOuts<'t, Self>) -> Option<&'t Book> {
		self.step(anchor, deltas).then_some(&*self)
	}
}

impl Nudge for Book {
	type Scratch = Option<Book>;

	fn stage(out: Option<&Book>, s: &mut Self::Scratch, _: Option<usize>, _: f64) -> f64 {
		match (out, s.as_mut()) {
			// `clone_from` keeps the two `BTreeMap` allocations across ticks — the observer already
			// pays two book clones per fired node, and again per dep slot.
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
