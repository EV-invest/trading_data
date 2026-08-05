//! What a measured unit measures with, and the table they all print.
//!
//! Each writes its row to `tmp/bench/<name>.json` and then prints every row on disk, so whichever
//! unit ran last shows the full comparison — `cargo bench -p trading_data_spl` ends with it.
//!
//! Timing excludes setup: the archives are decoded and the NT tape is built before the clock
//! starts. `feed` is the same run with the strategy removed, so `compute = total - feed` is the part
//! that is actually the thing under test, and td's per-day parquet decode stops flattering NT's
//! (which happened at setup).
//!
//! `publish = false` and a dev-dependency everywhere: nothing measured here can reach a shipped
//! build, which is why [`Census`] lives in this crate rather than next to the trait it implements.

pub mod ring;

use std::{
	collections::BTreeMap,
	fs,
	hash::{DefaultHasher, Hash, Hasher},
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use trading_data::{Fire, Observer, Want};

/// `USER_HZ`, the unit `/proc/<pid>/stat` reports CPU time in. Fixed at 100 on Linux regardless of
/// `CONFIG_HZ` — it is ABI, not a kernel tunable.
const CLK_TCK: f64 = 100.0;
/// Coarse enough that the sampler is not in the measurement, fine enough that a resident set held
/// for under a second still shows up in the p95.
const SAMPLE: Duration = Duration::from_millis(100);

/// Wall clock, CPU time and a resident-set trace over one timed section.
pub struct Probe {
	began: Instant,
	cpu0: u64,
	stop: Arc<AtomicBool>,
	sampler: JoinHandle<Vec<f64>>,
}
impl Probe {
	pub fn start() -> Self {
		let stop = Arc::new(AtomicBool::new(false));
		let flag = stop.clone();
		// Joined in `stop` — nothing outlives the section it measures.
		let sampler = thread::spawn(move || {
			let mut out = Vec::new();
			while !flag.load(Ordering::Relaxed) {
				thread::sleep(SAMPLE);
				out.push(rss_mb());
			}
			out
		});
		Self {
			began: Instant::now(),
			cpu0: cpu_ticks(),
			stop,
			sampler,
		}
	}

	/// (wall seconds, CPU seconds, resident MB at p68 and p95). Mean cores busy is CPU over wall, so it
	/// needs no trace of its own — only the memory does, a peak being one allocation rather than a
	/// footprint. The two quantiles read as what the run holds and what it spikes to; far apart means
	/// the footprint is a transient, and the p95 alone would be read as steady state.
	pub fn stop(self) -> (f64, f64, [f64; 2]) {
		let wall = self.began.elapsed().as_secs_f64();
		let cpu = (cpu_ticks() - self.cpu0) as f64 / CLK_TCK;
		self.stop.store(true, Ordering::Relaxed);
		let mut rss = self.sampler.join().expect("the sampler only reads procfs");
		rss.sort_by(f64::total_cmp);
		assert!(!rss.is_empty(), "a timed section shorter than one {SAMPLE:?} window has no footprint to report");
		let at = |q: f64| rss[((rss.len() as f64 * q) as usize).min(rss.len() - 1)];
		(wall, cpu, [at(0.68), at(0.95)])
	}
}

/// Call sites shared by all three implementations, so "how many times did the strategy actually
/// run" is comparable across them. Inputs over passes is the batch factor.
pub struct Counters {
	pub trades: AtomicU64,
	pub deltas: AtomicU64,
	pub bars_closed: AtomicU64,
	pub screener: AtomicU64,
	pub classify: AtomicU64,
	pub decision: AtomicU64,
	pub deprecator: AtomicU64,
}
impl Counters {
	/// A zero here means "this implementation cannot see that site", not "it never ran" — which is
	/// why the row carries `notes`.
	fn snapshot(&self) -> BTreeMap<String, u64> {
		[
			("trades", &self.trades),
			("deltas", &self.deltas),
			("bars_closed", &self.bars_closed),
			("screener", &self.screener),
			("classify", &self.classify),
			("decision", &self.decision),
			("deprecator", &self.deprecator),
		]
		.into_iter()
		.map(|(k, v)| (k.to_string(), v.load(Ordering::Relaxed)))
		.collect()
	}
}

pub static COUNTERS: Counters = Counters {
	trades: AtomicU64::new(0),
	deltas: AtomicU64::new(0),
	bars_closed: AtomicU64::new(0),
	screener: AtomicU64::new(0),
	classify: AtomicU64::new(0),
	decision: AtomicU64::new(0),
	deprecator: AtomicU64::new(0),
};
/// Order-sensitive digest of the compared output stream. Two implementations that agree on this ran
/// the same strategy; two that don't are being timed on different work, and the table says so.
///
/// The item's `Hash` *is* the comparison: what a run is identical over is a statement about that
/// item's type, so it belongs with the type and not here.
#[derive(Default)]
pub struct Digest {
	hasher: DefaultHasher,
	pub count: u64,
}
impl Digest {
	pub fn feed(&mut self, item: impl Hash) {
		self.count += 1;
		item.hash(&mut self.hasher);
	}
}

#[derive(Deserialize, Serialize)]
pub struct Row {
	pub name: String,
	pub ticks: u64,
	pub intents: u64,
	pub digest: u64,
	pub total_s: f64,
	pub feed_s: f64,
	pub cpu_s: f64,
	pub cores_avail: usize,
	/// Which CPUs the run was allowed on. `cores_avail` is a count and a count cannot say whether two
	/// of them were SMT siblings of one physical core.
	pub cpus: String,
	/// Resident MB at p68 and p95 of the timed section.
	pub rss_mb: [f64; 2],
	pub counters: BTreeMap<String, u64>,
	/// Where this row is not comparable to the others on its own terms.
	pub notes: Vec<String>,
}
impl Row {
	pub fn new(name: &str, ticks: u64, digest: &Digest, total: (f64, f64, [f64; 2]), feed_s: f64, notes: Vec<String>) -> Self {
		Self {
			name: name.to_string(),
			ticks,
			intents: digest.count,
			digest: digest.hasher.finish(),
			total_s: total.0,
			feed_s,
			cpu_s: total.1,
			cores_avail: thread::available_parallelism().expect("a thread count is always known on linux").get(),
			cpus: status_field("Cpus_allowed_list:"),
			rss_mb: total.2,
			counters: COUNTERS.snapshot(),
			notes,
		}
	}
}

/// Persist this run's row, then print every row on disk. Rows outlive their run so the last bench
/// to finish prints the whole comparison without the three having to share a process.
pub fn publish(row: Row) {
	let dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../tmp/bench"));
	fs::create_dir_all(&dir).expect("tmp/ is writable");
	fs::write(dir.join(format!("{}.json", row.name)), serde_json::to_vec_pretty(&row).expect("a Row serialises")).expect("tmp/bench is writable");

	let mut rows: Vec<Row> = fs::read_dir(&dir)
		.expect("just created")
		.map(|e| e.expect("a readable dir entry").path())
		.filter(|p| p.extension().is_some_and(|e| e == "json"))
		.map(|p| serde_json::from_slice(&fs::read(&p).expect("just listed")).unwrap_or_else(|e| panic!("stale row at {}: {e} — delete tmp/bench and rerun", p.display())))
		.collect();
	rows.sort_by(|a, b| a.name.cmp(&b.name));

	println!(
		"\n{:<14} {:>9} {:>9} {:>9} {:>9} {:>7} {:>11} {:>11} {:>9} {:>18}",
		"bench", "total s", "feed s", "compute", "cpu s", "cores", "rss p68 MB", "rss p95 MB", "intents", "digest"
	);
	for r in &rows {
		println!(
			"{:<14} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>7.2} {:>11.0} {:>11.0} {:>9} {:>18x}",
			r.name,
			r.total_s,
			r.feed_s,
			r.total_s - r.feed_s,
			r.cpu_s,
			r.cpu_s / r.total_s,
			r.rss_mb[0],
			r.rss_mb[1],
			r.intents,
			r.digest
		);
	}
	for r in &rows {
		println!("{}: cpus {} ({} of them)", r.name, r.cpus, r.cores_avail);
	}

	let keys: Vec<&String> = rows.first().map(|r| r.counters.keys().collect()).unwrap_or_default();
	if !keys.is_empty() {
		print!("\n{:<14}", "passes");
		for k in &keys {
			print!(" {k:>12}");
		}
		println!();
		for r in &rows {
			print!("{:<14}", r.name);
			for k in &keys {
				print!(" {:>12}", r.counters[*k]);
			}
			println!();
		}
	}

	match rows.windows(2).all(|w| w[0].digest == w[1].digest) {
		true if rows.len() > 1 => println!("\nintent streams identical across all {} rows", rows.len()),
		true => (),
		false => println!("\nDIGESTS DIFFER — the rows above are timing different work, see each row's notes"),
	}
	for r in &rows {
		for n in &r.notes {
			println!("  {}: {n}", r.name);
		}
	}
}
/// Fires and skips per node, in step order — which `trading_data_dag/model.typ` states *is* topo
/// order, so the row order doubles as the observed topology and a dep name never seen as a stepped
/// node is a root.
///
/// [`Want::Vals`], not [`Want::Nothing`]: the sweep returns before `on` below that level, so the
/// cheapest observation that exists at all is one flatten per stepped node per tick. The clone and
/// the re-advance per dep slot — [`Want::Jac`]'s alone — stay unbought.
#[derive(Default)]
pub struct Census {
	seen: Vec<Seen>,
}

struct Seen {
	name: &'static str,
	deps: &'static [&'static str],
	fired: u64,
	skipped: u64,
}

impl Observer for Census {
	fn want(&self, _: &'static str) -> Want {
		Want::Vals
	}

	fn on(&mut self, node: &'static str, deps: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		// ponytail: linear over the node count, so quadratic per tick. A graph large enough for that
		// to show would want the cursor-with-wrap the tape uses; at twenty nodes it is under the
		// flatten this already pays for.
		let i = self.seen.iter().position(|s| s.name == node).unwrap_or_else(|| {
			self.seen.push(Seen {
				name: node,
				deps,
				fired: 0,
				skipped: 0,
			});
			self.seen.len() - 1
		});
		match fire.vals.is_some() {
			true => self.seen[i].fired += 1,
			false => self.seen[i].skipped += 1,
		}
	}
}

impl core::fmt::Display for Census {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		writeln!(f, "{:<44} {:>10} {:>10} {:>7}  deps", "node (step = topo order)", "fired", "skipped", "hit")?;
		for s in &self.seen {
			let steps = s.fired + s.skipped;
			writeln!(
				f,
				"{:<44} {:>10} {:>10} {:>6.1}%  {}",
				s.name,
				s.fired,
				s.skipped,
				100.0 * s.fired as f64 / steps as f64,
				s.deps.join(", ")
			)?;
		}
		let mut roots: Vec<&str> = self.seen.iter().flat_map(|s| s.deps).filter(|d| !self.seen.iter().any(|s| s.name == **d)).copied().collect();
		roots.sort_unstable();
		roots.dedup();
		write!(f, "roots (dep names never stepped): {}", roots.join(", "))
	}
}

fn cpu_ticks() -> u64 {
	let stat = fs::read_to_string("/proc/self/stat").expect("procfs is mounted");
	// The comm field is parenthesised and may itself contain spaces, so the fields are counted from
	// the last ')', not from the start.
	let rest = &stat[stat.rfind(')').expect("stat has a comm field") + 1..];
	let mut f = rest.split_whitespace().skip(11);
	let utime: u64 = f.next().expect("utime").parse().expect("utime is a number");
	let stime: u64 = f.next().expect("stime").parse().expect("stime is a number");
	utime + stime
}

fn status_field(key: &str) -> String {
	let status = fs::read_to_string("/proc/self/status").expect("procfs is mounted");
	let line = status
		.lines()
		.find(|l| l.starts_with(key))
		.unwrap_or_else(|| panic!("/proc/self/status reports {key} for any live process"));
	line[key.len()..].trim().to_string()
}

fn rss_mb() -> f64 {
	status_field("VmRSS:")
		.split_whitespace()
		.next()
		.expect("VmRSS has a value")
		.parse::<f64>()
		.expect("VmRSS is a number")
		/ 1024.0
}
