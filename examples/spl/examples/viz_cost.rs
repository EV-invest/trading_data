//! Where the `main.rs` replay's wall clock actually goes: one day, walked three ways.
//!
//! `td_graph`'s bench row already times the graph without an observer; what it cannot say is how
//! much of the 3-minute replay is the `Viz` recorder attached to every fire. This walks the same
//! feed with the observer off and on, so the two numbers are read off one process on one day.

use std::{
	fmt::Write as _,
	path::{Path, PathBuf},
	time::Instant,
};

use exec_viz::{Backpressure, Viz};
use trading_data::{Cell, Exact, ExchangeName, Feed as _, Fire, LatencyConfig, Observer, ReadClock, Replay, Want, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};
use v_utils::*;

const SCROLLBACK: usize = 20_000;

#[tokio::main]
async fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(situation.pair.replace("-", ""));
	let catalog = ensure_lanes(&cache, situation).await;
	let lanes = required_lanes::<Graph>();
	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));
	let day = trading_days(situation)[0];
	let feed = || {
		let (start, end) = day_bounds(day);
		Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock)
	};

	// `Replay::new` eager-loads the whole range, so the decode and the merge are two different
	// timers on one leg rather than one number the later deltas would have to be read against.
	let mut ticks = 0u64;
	let began = Instant::now();
	let mut f = feed();
	let load_s = began.elapsed().as_secs_f64();
	while let Some(l) = f.next() {
		ticks += 1;
		std::hint::black_box(&l);
	}
	let feed_s = began.elapsed().as_secs_f64();

	let began = Instant::now();
	let mut graph = Graph::default();
	let mut f = feed();
	while let Some(l) = f.next() {
		std::hint::black_box(graph.tick(l.ts_venue.as_nanos(), l.into()));
	}
	let plain_s = began.elapsed().as_secs_f64();

	let leg = |want| {
		let began = Instant::now();
		let mut graph = Graph::default();
		let mut probe = Probe { want, vals: 0.0, jac: 0 };
		let mut f = feed();
		while let Some(l) = f.next() {
			std::hint::black_box(graph.tick_obs(l.ts_venue.as_nanos(), l.into(), &mut probe));
		}
		std::hint::black_box(&probe);
		began.elapsed().as_secs_f64()
	};
	let vals_s = leg(Want::Vals);
	let fd_s = leg(Want::Jac);

	// Two legs over the same recording: what the graph thread waits for when the tape must keep up,
	// and what it pays when it is allowed to outrun it — the producer side alone.
	let tape = |mode| {
		let began = Instant::now();
		let mut graph = Graph::default();
		let (_viz, mut recorder) = Viz::new(Some(<trading_data::Bars<{ TF_1MIN }> as Cell>::NAME), SCROLLBACK, 60_000, mode);
		let mut f = feed();
		while let Some(l) = f.next() {
			let ts_ns = l.ts_venue.as_nanos();
			std::hint::black_box(graph.tick_obs(ts_ns, l.into(), &mut recorder.at(ts_ns)));
		}
		let elapsed = began.elapsed().as_secs_f64();
		recorder.seal();
		elapsed
	};
	let obs_s = tape(Backpressure::Block);
	let free_s = tape(Backpressure::Drop);

	// The tape's `on` rebuilt in the pieces it is made of, each level doing everything the level below
	// it does. Everything but the handoff, so the last delta against `free_s` is the channel and the
	// absorbing thread with nothing else folded into it.
	let piece = |upto| {
		let began = Instant::now();
		let mut graph = Graph::default();
		let mut rec = Bill::new(upto);
		let mut f = feed();
		while let Some(l) = f.next() {
			rec.open();
			std::hint::black_box(graph.tick_obs(l.ts_venue.as_nanos(), l.into(), &mut rec));
		}
		let elapsed = began.elapsed().as_secs_f64();
		(elapsed, rec)
	};
	let (bare_s, _) = piece(Piece::Bare);
	let (glance_s, _) = piece(Piece::Glance);
	let (cols_s, bill) = piece(Piece::Columns);

	// One list, printed and written, so the terminal and `cost.typ` can never disagree about what a
	// line is called or which stage it belongs to.
	let stages = [
		("the feed", "parquet decode + latency sample", load_s),
		("the feed", "the weave (arrival merge)", feed_s - load_s),
		("the graph", "every derived node", plain_s - feed_s),
		("reaching the observer", "flatten (Want::Vals)", vals_s - plain_s),
		("reaching the observer", "finite-diff Jacobian (Want::Jac)", fd_s - vals_s),
		("the tape's `on`", "bookkeeping", bare_s - fd_s),
		("the tape's `on`", "glance (the node's own one-liner)", glance_s - bare_s),
		("the tape's `on`", "vals+jac columns", cols_s - glance_s),
		("the tape's `on`", "handoff (channel + absorb)", free_s - cols_s),
		("the tape's `on`", "backpressure wait", obs_s - free_s),
	];

	println!("{ticks} ticks over {day}\n");
	let mut group = "";
	for (g, label, secs) in stages {
		if g != group {
			group = g;
			println!("── {g} {}", "─".repeat(45 - g.chars().count()));
		}
		println!("  {label:<34} {secs:>6.2}s {:>5.1}%", 100.0 * secs / obs_s);
	}
	println!("{}", "─".repeat(49));
	// The one line the recorder's own work is read off: everything past `Want::Jac` is the tape's.
	println!("  {:<34} {:>6.2}s", "the tape", obs_s - fd_s);
	println!("  {:<34} {obs_s:>6.2}s", "total");

	// Which nodes the rendering bill is actually run up by — the only thing that says where
	// de-stringing pays first. Bytes rather than a per-fire timer: at ~27 ns a read the clock would
	// be a third of what it measures, and rendered length is what the cost is proportional to.
	let mut rows: Vec<_> = bill.names.iter().zip(&bill.bytes).map(|(n, &g)| (*n, g)).collect();
	rows.sort_unstable_by_key(|&(_, g)| core::cmp::Reverse(g));
	let tg: u64 = rows.iter().map(|&(_, g)| g).sum();
	let per = |b: u64| b as f64 / ticks as f64;
	println!("\n── rendered bytes per tick, by node ─────────────");
	println!("  {:<38} {:>8} {:>6}", "node", "glance", "share");
	for &(n, g) in rows.iter().take(10) {
		let n = if n.len() > 38 { &n[n.len() - 38..] } else { n };
		println!("  {n:<38} {:>8.0} {:>5.1}%", per(g), 100.0 * g as f64 / tg as f64);
	}
	println!("  {:<38} {:>8.0} {:>5.1}%", format!("[all {} nodes]", rows.len()), per(tg), 100.0);
	println!("  {:<38} {:>8.1}", "MB over the day", tg as f64 / 1e6);

	// `cost.typ` renders these; nothing regenerates them but this example, and `tests/cost.rs` is
	// what notices when the graph has moved on since it last ran. `derived` is the graph's own
	// closure rather than what the observer saw, which also counts roots — it is the list the test
	// can check without a feed.
	let art = serde_json::json!({
		"day": day.to_string(),
		"ticks": ticks,
		"total_s": obs_s,
		"tape_s": obs_s - fd_s,
		"derived": Graph::NODES,
		"stages": stages.map(|(g, label, secs)| serde_json::json!({ "group": g, "label": label, "secs": secs })),
		"render": rows.iter().map(|&(n, g)| serde_json::json!({ "node": n, "glance": g })).collect::<Vec<_>>(),
	});
	let at = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/cost.json"));
	std::fs::write(at, serde_json::to_string_pretty(&art).expect("a json! literal serializes")).expect("write cost.json");
	println!("\nwrote {}", at.display());
}

/// How far down the tape's `on` a [`Bill`] leg goes; each level does everything below it.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum Piece {
	Bare,
	Glance,
	Columns,
}

/// `exec_viz`'s `Rec::on`, itemized — the same columns reused across ticks, the same fire-gated
/// render, the same appends, and nothing else. What it does *not* do is hand the tick off, which is
/// what leaves the channel priceable as a delta against the real recorder. Kept in step by hand;
/// see `tmp/ongoing_dev/logic_duplication.md`.
struct Bill {
	upto: Piece,
	idx: usize,
	names: Vec<&'static str>,
	/// One tick, as the tape holds it — see `exec_viz`'s `Acts`.
	outs: String,
	vals: Vec<f64>,
	jac: Vec<f64>,
	ends: Vec<[u32; 3]>,
	/// Per node, glance bytes rendered across the whole day.
	bytes: Vec<u64>,
}
impl Bill {
	fn new(upto: Piece) -> Self {
		Self {
			upto,
			idx: 0,
			names: Vec::new(),
			outs: String::new(),
			vals: Vec::new(),
			jac: Vec::new(),
			ends: Vec::new(),
			bytes: Vec::new(),
		}
	}

	/// `Rec::at`, minus the recycling — there is one buffer here and it is never handed away.
	fn open(&mut self) {
		self.idx = 0;
		self.outs.clear();
		self.vals.clear();
		self.jac.clear();
		self.ends.clear();
	}
}

impl Observer for Bill {
	fn want(&self) -> Want {
		Want::Jac
	}

	fn on(&mut self, node: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		let i = self.idx;
		self.idx += 1;
		if self.names.len() == i {
			self.names.push(node);
			self.bytes.push(0);
		} else {
			assert_eq!(self.names[i], node, "step order shifted between ticks");
		}
		if self.upto == Piece::Bare {
			return;
		}

		if fire.vals.is_some() {
			let was = self.outs.len();
			write!(self.outs, "{}", fire.glance).expect("`String`'s `Write` is infallible");
			self.bytes[i] += (self.outs.len() - was) as u64;
		}
		if self.upto == Piece::Glance {
			return;
		}

		if let Some(vals) = fire.vals {
			self.vals.extend_from_slice(vals);
		}
		if let Some(jac) = fire.jac {
			self.jac.extend_from_slice(jac);
		}
		self.ends.push([self.outs.len() as u32, self.vals.len() as u32, self.jac.len() as u32]);
	}
}

/// An observer that records nothing, so what it costs is what `step_seen` spends *reaching* it —
/// which is what `want` dials: the pre-advance clone and one `clone_from`+`advance` per dep slot of
/// `fd_jac` are `Want::Jac`'s alone. The sums are what keep that work from being eliminated as dead
/// — an empty `on` would inline to nothing and time the graph twice.
struct Probe {
	want: Want,
	vals: f64,
	jac: usize,
}

impl Observer for Probe {
	fn want(&self) -> Want {
		self.want
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		self.vals += fire.vals.map_or(0.0, |v| v.iter().sum());
		self.jac += fire.jac.map_or(0, <[f64]>::len);
	}
}
