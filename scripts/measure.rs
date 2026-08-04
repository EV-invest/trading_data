//! `measure <bench|stat|flame|cost>` — one CPU reservation, four readings of it. `bench` is the
//! deterministic tier (iai-callgrind, instruction counts); the rest are the wall-clock tier and need
//! a unit that *returns*, which is what `--headless` on the examples is for.
//!
//! `BENCH_CPUS` places the run on a fixed CPU set. Placement alone stops migration, not contention:
//! a core is only ours if everything else is confined to the complement, which is privileged (a user
//! slice is delegated `cpu io memory pids`, never `cpuset`). Cheapest is to have PID 1 hold the
//! reservation — `systemd.settings.Manager.CPUAffinity`, inherited by every unit and session — and
//! that is what [`reserved`] looks for first. Failing that we take the reservation for the run if
//! sudo answers, and say so loudly if it cannot.
//!
//! SMT means a set has to name both siblings of every physical core it claims (`lscpu -e` pairs
//! them), or what it reserved is threads sharing a contended core.

use std::{
	collections::BTreeSet,
	env, fs,
	os::unix::process::ExitStatusExt as _,
	process::{Command, ExitCode},
};

/// Everything a cpuset can be narrowed on. `user.slice` alone would leave the kernel's own workers
/// and every system unit free to land on the set we just claimed.
const SLICES: [&str; 3] = ["init.scope", "system.slice", "user.slice"];

const HELP: &str = "\
measure bench [cargo-bench args]       instruction counts, iai-callgrind (`-p PKG` to narrow)
measure stat  <simple|spl> [app args]  perf stat -r 5: wall, IPC, cache and branch misses
measure flame <simple|spl> [app args]  cargo flamegraph, one frame per DAG node
measure cost                           the replay's wall clock, itemized — rewrites examples/spl/cost.json

`flame` reads which node, never how much: `--features profile` denies the sweep its
inlining, so each node's box also holds a `Cons` copy the shipped build elides — and the
frame grows as the sweep goes, so the later a node sits the more it is overstated. Widths
rank nodes; `stat` and `bench` are the builds a number comes from.

BENCH_CPUS=<list> confines the run to those CPUs and everything else to the complement;
without it the wall clock reads the scheduler as much as the code. Name both SMT siblings
of every core you claim (`lscpu -e` pairs them).

This box reports perf_event_paranoid=2 and kptr_restrict=1: user-space frames resolve,
kernel ones do not. That is the parquet decode and IO half of a replay — attributing it
needs paranoid=1, which is a sysctl and deliberately not something this asks sudo for.
";

fn main() -> ExitCode {
	let repo = repo();
	let manifest = format!("{repo}/Cargo.toml");
	let mut args = env::args().skip(1);
	let verb = args.next().unwrap_or_default();
	let rest: Vec<String> = args.collect();

	match verb.as_str() {
		"bench" => reserved(argv(["cargo", "bench", "--manifest-path", &manifest], rest), &[]),
		"stat" => {
			let (pkg, rest) = unit(rest, "stat");
			// Built outside the reservation: a compile is not the thing being timed, and holding the
			// complement confined for it would starve the machine for minutes.
			build(["cargo", "build", "--profile", "profiling", "--manifest-path", &manifest, "-p", pkg]);
			let unit = format!("{repo}/target/profiling/{pkg}");
			let perf = [
				"perf",
				"stat",
				"-r",
				"5",
				"-e",
				"task-clock,cycles,instructions,cache-misses,branch-misses,page-faults",
				"--",
				&unit,
			];
			reserved(argv(perf, rest), &[("TD_HEADLESS", "1")])
		}
		"flame" => {
			let (pkg, rest) = unit(rest, "flame");
			fs::create_dir_all(format!("{repo}/tmp")).expect("the repo is writable");
			let out = format!("{repo}/tmp/flame-{pkg}.svg");
			// `--features profile` is what makes this readable: without it LLVM folds the whole sweep
			// into one frame and the SVG says `Graph::tick` and nothing else. `dwarf`, because the
			// profile carries line tables but no frame pointer (see `[profile.profiling]`).
			let flame = [
				"cargo",
				"flamegraph",
				"--profile",
				"profiling",
				"--features",
				"profile",
				"--manifest-path",
				&manifest,
				"-p",
				pkg,
				"-c",
				"record -F 997 --call-graph dwarf,16384 -g",
				"-o",
				&out,
				"--",
			];
			let code = reserved(argv(flame, rest), &[("TD_HEADLESS", "1")]);
			eprintln!("measure: tmp/flame-{pkg}.svg");
			code
		}
		"cost" => {
			// `examples/spl/cost.typ` reads what this writes, so it is the one reading whose noise
			// outlives the run — an unreserved leg lands in a committed document. Release rather than
			// `profiling`: the legs are differences of wall clock, and the profile that carries frame
			// information is not the one the app ships.
			build(["cargo", "build", "--release", "--manifest-path", &manifest, "-p", "trading_data_spl", "--example", "viz_cost"]);
			reserved(argv([format!("{repo}/target/release/examples/viz_cost").as_str()], rest), &[])
		}
		_ => {
			eprint!("{HELP}");
			ExitCode::FAILURE
		}
	}
}

/// Runs `argv` on `BENCH_CPUS` with everything else pushed off it, or plainly if that variable is
/// unset. Reservation is a property of the run and not of what is being run, so every verb is here.
fn reserved(argv: Vec<String>, envs: &[(&str, &str)]) -> ExitCode {
	let Some(cpus) = env::var("BENCH_CPUS").ok().filter(|s| !s.is_empty()) else {
		return code(wait(command(&argv, envs)));
	};
	let want = expand(&cpus);

	if want.is_disjoint(&expand(&pid1_affinity())) {
		return code(wait(taskset(&cpus, &argv, envs)));
	}

	let every = expand(&fs::read_to_string("/sys/devices/system/cpu/possible").expect("/sys is mounted"));
	let complement = join(every.difference(&want));
	if !sudo(["true"]) {
		eprintln!(
			"measure: WARNING — {cpus} is not reserved and sudo declined, so anything else on this machine shares those cores. Wall clock will read the scheduler as much as the code."
		);
		return code(wait(taskset(&cpus, &argv, envs)));
	}

	let confined = Confined::take(&complement, join(every.iter()));
	eprintln!("measure: took {cpus} for this run; set systemd.settings.Manager.CPUAffinity={complement} to hold them");

	// cgroup cpusets only narrow, so the run cannot live under a slice it just confined — it gets a
	// top-level one instead. The environment is spelled out rather than inherited: `sudo` has already
	// reset HOME and PATH by the time systemd-run reads them, and a bench that builds into /root is
	// not the same bench.
	let mut cmd = Command::new("sudo");
	cmd.args(["-n", "systemd-run", "--scope", "--quiet", "--collect", "--slice=measure"])
		.args(["-p", &format!("AllowedCPUs={cpus}")])
		// SAFETY: `getuid`/`getgid` read a field of our own credentials and cannot fail.
		.args([format!("--uid={}", unsafe { getuid() }), format!("--gid={}", unsafe { getgid() })])
		.arg("--same-dir");
	for key in ["PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME"] {
		if let Ok(val) = env::var(key) {
			cmd.args(["-E", &format!("{key}={val}")]);
		}
	}
	for (key, val) in envs {
		cmd.args(["-E", &format!("{key}={val}")]);
	}
	cmd.args(&argv);
	let out = code(wait(cmd));
	drop(confined);
	out
}

/// The slices narrowed for the duration of one run, and the CPU list they go back onto on the way
/// out. Restored by naming every CPU rather than by clearing the property: an empty value unsets it
/// in systemd but leaves the unit's `cpuset.cpus` wherever it last wrote it, and the machine stays
/// confined until reboot. `--runtime` throughout, so a reboot is the backstop either way.
struct Confined {
	every: String,
}

impl Confined {
	fn take(complement: &str, every: String) -> Self {
		// Constructed before the narrowing so a slice that refuses it still unwinds through `Drop`,
		// rather than leaving whichever slices did take it confined.
		let held = Self { every };
		for slice in SLICES {
			assert!(sudo(["systemctl", "set-property", "--runtime", slice, &format!("AllowedCPUs={complement}")]), "confining {slice}");
		}
		// A ctrl-c reaches the whole foreground group, so the scope dies on its own; what this stops
		// is the same signal killing *us* first and leaving the machine confined. It has to be a
		// handler rather than `SIG_IGN`, which `exec` would pass on to the run itself — the run must
		// stay interruptible. The handler does nothing: `wait` returns on its own once the scope is
		// gone, and the restore is this value's `Drop`.
		for sig in [SIGINT, SIGTERM] {
			// SAFETY: `survive` touches nothing, which is the only bar a signal handler has to clear.
			unsafe { signal(sig, survive) };
		}
		held
	}
}

impl Drop for Confined {
	fn drop(&mut self) {
		for slice in SLICES {
			sudo(["systemctl", "set-property", "--runtime", slice, &format!("AllowedCPUs={}", self.every)]);
		}
	}
}

const SIGINT: i32 = 2;
const SIGTERM: i32 = 15;

unsafe extern "C" {
	fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
	fn getuid() -> u32;
	fn getgid() -> u32;
}

extern "C" fn survive(_: i32) {}

/// A `0-3,8` CPU list, as a set. Panics on anything else — a mistyped `BENCH_CPUS` that quietly
/// reserved a different set than it was asked for still produces a number, and that number is read.
fn expand(list: &str) -> BTreeSet<u32> {
	let mut out = BTreeSet::new();
	for part in list.split([',', ' ', '\n']).filter(|p| !p.is_empty()) {
		let cpu = |s: &str| s.parse::<u32>().unwrap_or_else(|_| panic!("`{part}` is not a CPU or a CPU range"));
		match part.split_once('-') {
			Some((lo, hi)) => out.extend(cpu(lo)..=cpu(hi)),
			None => {
				out.insert(cpu(part));
			}
		}
	}
	assert!(!out.is_empty(), "`{list}` names no CPUs");
	out
}

/// The cheapest reservation there is: PID 1's own affinity is inherited by every unit and every
/// session, so a set outside it is a set nothing on this machine can be scheduled onto.
fn pid1_affinity() -> String {
	fs::read_to_string("/proc/1/status")
		.expect("/proc is mounted")
		.lines()
		.find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
		.expect("a process status reports its affinity")
		.trim()
		.to_owned()
}

fn join<'a>(cpus: impl Iterator<Item = &'a u32>) -> String {
	cpus.map(u32::to_string).collect::<Vec<_>>().join(",")
}

fn argv<'a>(head: impl IntoIterator<Item = &'a str>, tail: Vec<String>) -> Vec<String> {
	head.into_iter().map(String::from).chain(tail).collect()
}

fn unit(args: Vec<String>, verb: &str) -> (&'static str, Vec<String>) {
	let Some((named, rest)) = args.split_first() else {
		eprintln!("measure: {verb} <simple|spl>");
		std::process::exit(1);
	};
	let pkg = match named.as_str() {
		"simple" => "trading_data_simple",
		"spl" => "trading_data_spl",
		other => {
			eprintln!("measure: unknown unit '{other}' (simple|spl)");
			std::process::exit(1);
		}
	};
	(pkg, rest.to_vec())
}

fn build<const N: usize>(args: [&str; N]) {
	let code = wait(command(&args.map(String::from), &[]));
	if code != 0 {
		std::process::exit(code);
	}
}

fn command(argv: &[String], envs: &[(&str, &str)]) -> Command {
	let mut cmd = Command::new(&argv[0]);
	cmd.args(&argv[1..]).envs(envs.iter().copied());
	cmd
}

fn taskset(cpus: &str, argv: &[String], envs: &[(&str, &str)]) -> Command {
	let mut cmd = Command::new("taskset");
	cmd.args(["-c", cpus]).args(argv).envs(envs.iter().copied());
	cmd
}

fn wait(mut cmd: Command) -> i32 {
	let status = cmd.status().unwrap_or_else(|e| panic!("`{:?}` failed to start: {e}", cmd.get_program()));
	// 128+n for a signalled child, as a shell reports it: `measure` is read from a terminal, and a
	// ctrl-c'd bench must not read as a bench that returned 0.
	status.code().unwrap_or_else(|| 128 + status.signal().expect("a status is a code or a signal"))
}

fn code(status: i32) -> ExitCode {
	ExitCode::from(u8::try_from(status).expect("a unix exit status is a byte"))
}

fn sudo<const N: usize>(args: [&str; N]) -> bool {
	Command::new("sudo").arg("-n").args(args).status().expect("sudo is on PATH").success()
}

fn repo() -> String {
	let out = Command::new("git").args(["rev-parse", "--show-toplevel"]).output().expect("git is on PATH");
	assert!(out.status.success(), "measure runs inside the repository it measures");
	String::from_utf8(out.stdout).expect("a path from git is utf-8").trim().to_owned()
}
