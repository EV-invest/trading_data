//! The actor row with every feed always on: NT owns the trades, the book and the bar aggregation,
//! our node types own everything above them, and the msgbus carries each hop as JSON.

#[path = "harness/mod.rs"]
mod harness;
#[path = "nt/mod.rs"]
mod nt;
#[path = "harness/ring.rs"]
mod ring;

fn main() {
	nt::run("naive_nt", false, vec!["every feed subscribed for the whole run".into()]);
}
