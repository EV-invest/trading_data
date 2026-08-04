//! The same actors with scam_pump_liqs' Shallow/Deep tier: the delta feed is opened on a screener
//! hit and closed when the situation ends, so the book is maintained only while something reads it.
//!
//! This is NT's answer to what `Gating<Screener>` is in the graph — a gate the framework propagates
//! for free, spelled out by hand as a subscription the strategy opens and closes. The difference the
//! table shows is what that hand-rolling costs, and how much of the gate's reach it recovers: the
//! book stops being maintained, but the bar-clocked indies still run.

#[path = "nt/mod.rs"]
mod nt;

fn main() {
	nt::run(
		"optimized_nt",
		true,
		vec![
			"book deltas subscribed only between a screener hit and the situation's terminal intent".into(),
			"a reopened book starts empty, so imbalance/spread read None until both sides refill".into(),
		],
	);
}
