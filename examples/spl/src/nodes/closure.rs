//! A second graph over the same node library, asking for one indie. It exists to be measured from
//! [`crate::nodes`]'s tests: what `outputs` does not reach is not instantiated, so the book chain,
//! the screener and the whole `Deprecator` subtree are absent — and with them four of the five roots.
//!
//! It lives in the crate rather than in `tests/` because a dep shim is textually scoped: only here
//! are the names `Rsi` is written in resolvable.

use trading_data::{BookAnchors, BookDeltas, BookShape, DeltaFrame, Mc, McRoot, Oi, OiRoot, TradeCols, Trades};

// an alias is spelled where it is declared and pasted where it is named, so `Rsi`'s own arguments
// have to resolve here too.
use crate::nodes::{Rsi, RsiSeries, rsi::Knobs};

trading_data::graph! {
	pub struct SmallGraph;
	batches SmallBatches;
	// every root the app can feed, so that what `required_lanes` reports is the closure's doing and
	// not the declaration's.
	roots { trades: Trades[TradeCols], deltas: BookDeltas[DeltaFrame], anchors: BookAnchors[BookShape], oi: OiRoot[Oi], mc: McRoot[Mc] };
	out SmallOut;
	outputs { rsi: Rsi }
}
