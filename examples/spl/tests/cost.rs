//! `cost.typ` quotes hard numbers, and a document that quotes numbers is only as good as its claim
//! to still be describing the thing it measured. Re-measuring here is not on the table — a leg is
//! ~3.5 minutes and wants the whole parquet cache — so what is checked is the one thing that goes
//! stale silently and cheaply: the graph the numbers were taken over.
//!
//! Regenerate with `cargo r --release -p trading_data_spl --example viz_cost`.

use trading_data_spl::nodes::Graph;

#[test]
fn the_document_still_describes_this_graph() {
	let raw = include_str!("../cost.json");
	let art: serde_json::Value = serde_json::from_str(raw).expect("cost.json parses");
	let measured: Vec<&str> = art["derived"]
		.as_array()
		.expect("cost.json carries a `derived` list")
		.iter()
		.map(|n| n.as_str().expect("a node name is a string"))
		.collect();

	assert_eq!(
		measured,
		Graph::NODES,
		"cost.typ's numbers were taken over a different derived closure than the one that compiles \
		 today — re-run `--example viz_cost`"
	);
}
