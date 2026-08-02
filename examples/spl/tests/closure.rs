//! The user-visible promise of a derived graph: unneeded work is not merely unused, it does not
//! exist. [`crate::nodes::closure`] asks the same node library for one indie; everything the full
//! strategy needs and this does not is measured absent here.

use trading_data::{LaneKind, required_lanes};
use trading_data_spl::nodes::closure::SmallGraph;

#[test]
fn one_output_pulls_only_its_own_chain() {
	// the whole graph, in the order it steps: the book chain, the market cap, the screener and the
	// episode under it are not upstream of an RSI, so they are not here.
	assert_eq!(
		SmallGraph::NODES,
		["Bar5m", "RsiDelta<RsiSeries>", "AvgGain<RsiSeries,Knobs>", "AvgLoss<RsiSeries,Knobs>", "Rsi<RsiSeries,Knobs>"]
	);

	// and the four roots nothing reaches are declared, present in `SmallBatches`, and never loaded.
	assert_eq!(required_lanes::<SmallGraph>(), [LaneKind::Trades]);
}
