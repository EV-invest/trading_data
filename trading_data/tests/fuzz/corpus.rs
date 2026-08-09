//! The growing set of previously-failing `(target, seed, size)` cases, kept as a plain git-tracked
//! data file (`CORPUS.txt`) rather than a source `const` so `fuzz` can append a freshly minimized
//! repro on its own: find → fix → the new line is already recorded, just commit it.
//!
//! Every entry is stamped with the version of the generator that recorded it, because a
//! `(seed, size)` names a *trace* only under that generator. Reshape the generator and the same pair
//! replays some unrelated trace — a random run wearing a regression test's name, which is worse than
//! no test at all, since it reports green for a case it never exercises. `regressions` replays only
//! entries at the current version; older lines stay in the file as the record of what once broke and
//! why, and are never run again. Nothing is ever deleted — a superseded line stops being a test
//! without ceasing to be documentation, and that costs nothing to keep.
//!
//! The version is *per target*: one fingerprint over every generator in the binary would let an edit
//! to the weaver's stream shape retire the gate corpus, which is a silent loss of coverage dressed
//! up as housekeeping. So each target names the sources its own `(seed, size)` means something
//! under, and `include_str!` stays at that call site — it cannot cross a module boundary as data.

use std::{fs::OpenOptions, io::Write, path::PathBuf};

/// FNV-1a over the sources that decide what a `(seed, size)` means. Derived rather than hand-bumped:
/// a version someone has to remember to raise is a version that silently goes stale, and the failure
/// mode there is a corpus quietly passing without testing anything. Over-sensitive on purpose (a
/// comment edit invalidates too): the cost of a false invalidation is a few free runs, the cost of a
/// false match is a lie.
pub const fn fnv(sources: &[&str]) -> u32 {
	let mut h: u32 = 0x811c_9dc5;
	let mut i = 0;
	while i < sources.len() {
		let bytes = sources[i].as_bytes();
		let mut j = 0;
		while j < bytes.len() {
			h = (h ^ bytes[j] as u32).wrapping_mul(0x0100_0193);
			j += 1;
		}
		i += 1;
	}
	h
}

pub struct Entry {
	pub target: String,
	pub seed: u64,
	pub size: usize,
	pub version: u32,
}

/// Parse the corpus: one `target seed size version` per line; `#` starts a comment; blanks ignored.
pub fn load() -> Vec<Entry> {
	let text = std::fs::read_to_string(path()).unwrap_or_default();
	let mut out = Vec::new();
	for line in text.lines() {
		let data = line.split('#').next().unwrap_or("").trim();
		if data.is_empty() {
			continue;
		}
		let mut it = data.split_whitespace();
		let target = it.next().expect("corpus line has a target").to_owned();
		let seed = it.next().expect("corpus line has a seed").parse().expect("corpus seed is a u64");
		let size = it.next().expect("corpus line has a size").parse().expect("corpus size is a usize");
		let version = it.next().expect("corpus line has a version").parse().expect("corpus version is a u32");
		out.push(Entry { target, seed, size, version });
	}
	out
}

/// Append a newly-found minimal repro unless it's already recorded at this generator version.
/// `reason` is written as a trailing comment so a reviewer of the diff sees what broke.
pub fn record(target: &str, seed: u64, size: usize, version: u32, reason: &str) {
	if load().iter().any(|e| e.target == target && e.seed == seed && e.size == size && e.version == version) {
		return;
	}
	let line = format!("{target} {seed} {size} {version}  # {}\n", reason.replace('\n', " "));
	// One short append on the rare failure path; the file is only ever appended to.
	let mut f = OpenOptions::new().create(true).append(true).open(path()).expect("open CORPUS.txt for append");
	f.write_all(line.as_bytes()).expect("append to CORPUS.txt");
}

/// `CARGO_MANIFEST_DIR` is fixed at compile time to the crate dir, so the path resolves regardless
/// of the test runner's working directory.
fn path() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fuzz/CORPUS.txt")
}
