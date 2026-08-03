//! The same actors with scam_pump_liqs' Shallow/Deep tier: the delta feed is opened on a screener
//! hit and closed when the situation ends, so the book is maintained only while something reads it.

#[path = "harness/mod.rs"]
mod harness;
#[path = "nt/mod.rs"]
mod nt;
#[path = "harness/ring.rs"]
mod ring;

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
