//! r[verify boundaries.examples.facade-only]
//!
//! The manifest is the chokepoint: cargo has no private workspace member, but without the dep
//! neither `use trading_data_core::…` nor a macro-emitted dag path resolves at all.

use std::{fs, path::Path};

#[test]
fn an_example_names_the_facade_only() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
	let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("the facade sits in the workspace it is the facade of");

	// off `members`, not a directory walk: a renamed example must fall out of the guard loudly.
	let examples: Vec<&str> = workspace
		.lines()
		.filter_map(|l| l.trim().trim_end_matches(',').trim_matches('"').strip_prefix("examples/"))
		.collect();
	assert!(!examples.is_empty(), "no `examples/` workspace member — the guard is reading the wrong manifest");

	for example in examples {
		let manifest = root.join("examples").join(example).join("Cargo.toml");
		let text = fs::read_to_string(&manifest).expect("a workspace member has a manifest");
		for line in text.lines() {
			let key = line.split(['=', '.', ' ']).next().expect("split yields one part");
			assert!(
				!key.starts_with("trading_data") || key == "trading_data",
				"{}: `{key}` — an example depends on the facade only, see docs/spec/boundaries.md",
				manifest.display()
			);
		}
	}
}
