//! Where the `main.rs` replay's wall clock actually goes: one day, walked three ways.
//!
//! `td_graph`'s bench row already times the graph without an observer; what it cannot say is how
//! much of the 3-minute replay is the `Viz` recorder attached to every fire. This walks the same
//! feed with the observer off and on, so the two numbers are read off one process on one day.

use std::{
	fmt::{self, Write as _},
	path::{Path, PathBuf},
	time::Instant,
};

use exec_viz::{Backpressure, Viz};
use trading_data::{Cell, Exact, ExchangeName, Feed as _, Fire, LatencyConfig, Observer, ReadClock, Replay, Want, required_lanes};
use trading_data_spl::{config::Config, day_bounds, ensure_lanes, nodes::Graph, symbol, trading_days};
use v_utils::*;

const SCROLLBACK: usize = 20_000;

fn main() {
	let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix")));
	let situation = &cfg.situation;
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);
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

	let began = Instant::now();
	let mut graph = Graph::default();
	let mut fmt = Fmt::default();
	let mut f = feed();
	while let Some(l) = f.next() {
		std::hint::black_box(graph.tick_obs(l.ts_venue.as_nanos(), l.into(), &mut fmt));
	}
	let fmt_s = began.elapsed().as_secs_f64();
	std::hint::black_box(&fmt);

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

	// The tape's `on` rebuilt in the four pieces it is made of, each level doing everything the level
	// below it does. Everything but the handoff, so the last delta against `free_s` is the channel
	// and the absorbing thread with nothing else folded into it.
	let piece = |upto| {
		let began = Instant::now();
		let mut graph = Graph::default();
		let mut rec = Bill::new(upto);
		let mut f = feed();
		while let Some(l) = f.next() {
			rec.idx = 0;
			std::hint::black_box(graph.tick_obs(l.ts_venue.as_nanos(), l.into(), &mut rec));
		}
		let elapsed = began.elapsed().as_secs_f64();
		(elapsed, rec)
	};
	let (bare_s, _) = piece(Piece::Bare);
	let (glance_s, _) = piece(Piece::Glance);
	let (debug_s, _) = piece(Piece::Debug);
	let (refill_s, bill) = piece(Piece::Refill);

	// One list, printed and written, so the terminal and `cost.typ` can never disagree about what a
	// line is called or which stage it belongs to.
	let stages = [
		("the feed", "parquet decode + latency sample", load_s),
		("the feed", "the weave (arrival merge)", feed_s - load_s),
		("the graph", "every derived node", plain_s - feed_s),
		("reaching the observer", "flatten (Want::Vals)", vals_s - plain_s),
		("reaching the observer", "finite-diff Jacobian (Want::Jac)", fd_s - vals_s),
		("the tape's `on`", "bookkeeping", bare_s - fd_s),
		("the tape's `on`", "glance (Display, unclipped)", glance_s - bare_s),
		("the tape's `on`", "detail (Debug, clipped at 256)", debug_s - glance_s),
		("the tape's `on`", "vals+jac memcpy", refill_s - debug_s),
		("the tape's `on`", "handoff (channel + absorb)", free_s - refill_s),
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
	println!("  {:<34} {obs_s:>6.2}s", "total");
	println!("  {:<34} {fmt_s:>6.2}s", "unclipped Debug would have been");

	// Which nodes the rendering bill is actually run up by — the only thing that says where
	// de-stringing pays first. Bytes rather than a per-fire timer: at ~27 ns a read the clock would
	// be a third of what it measures, and rendered length is what the cost is proportional to.
	let mut rows: Vec<_> = bill.names.iter().zip(&bill.bytes).map(|(n, &(g, d))| (*n, g, d)).collect();
	rows.sort_unstable_by_key(|&(_, g, d)| core::cmp::Reverse(g + d));
	let (tg, td) = rows.iter().fold((0u64, 0u64), |(a, b), &(_, g, d)| (a + g, b + d));
	let per = |b: u64| b as f64 / ticks as f64;
	println!("\n── rendered bytes per tick, by node ─────────────");
	println!("  {:<38} {:>8} {:>8} {:>6}", "node", "glance", "detail", "share");
	for &(n, g, d) in rows.iter().take(10) {
		let n = if n.len() > 38 { &n[n.len() - 38..] } else { n };
		println!("  {n:<38} {:>8.0} {:>8.0} {:>5.1}%", per(g), per(d), 100.0 * (g + d) as f64 / (tg + td) as f64);
	}
	println!("  {:<38} {:>8.0} {:>8.0} {:>5.1}%", format!("[all {} nodes]", rows.len()), per(tg), per(td), 100.0);
	println!("  {:<38} {:>8.1} {:>8.1}", "MB over the day", tg as f64 / 1e6, td as f64 / 1e6);

	// `cost.typ` renders these; nothing regenerates them but this example, and `tests/cost.rs` is
	// what notices when the graph has moved on since it last ran. `derived` is the graph's own
	// closure rather than what the observer saw, which also counts roots — it is the list the test
	// can check without a feed.
	let art = serde_json::json!({
		"day": day.to_string(),
		"ticks": ticks,
		"total_s": obs_s,
		"unclipped_s": fmt_s,
		"derived": Graph::NODES,
		"stages": stages.map(|(g, label, secs)| serde_json::json!({ "group": g, "label": label, "secs": secs })),
		"render": rows.iter().map(|&(n, g, d)| serde_json::json!({ "node": n, "glance": g, "detail": d })).collect::<Vec<_>>(),
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
	Debug,
	Refill,
}

/// `exec_viz`'s `Rec::on`, itemized — the same per-node slots reused across ticks, the same clip at
/// 256, the same two `refill`s, and nothing else. What it does *not* do is hand the tick off, which
/// is what leaves the channel priceable as a delta against the real recorder.
struct Bill {
	upto: Piece,
	idx: usize,
	names: Vec<&'static str>,
	slots: Vec<Slot>,
	/// Per node, `(glance, detail)` bytes rendered across the whole day.
	bytes: Vec<(u64, u64)>,
}
impl Bill {
	fn new(upto: Piece) -> Self {
		Self {
			upto,
			idx: 0,
			names: Vec::new(),
			slots: Vec::new(),
			bytes: Vec::new(),
		}
	}
}

#[derive(Default)]
struct Slot {
	out: String,
	detail: String,
	vals: Vec<f64>,
	jac: Vec<f64>,
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
			self.slots.push(Slot::default());
			self.bytes.push((0, 0));
		} else {
			assert_eq!(self.names[i], node, "step order shifted between ticks");
		}
		if self.upto == Piece::Bare {
			return;
		}

		let slot = &mut self.slots[i];
		slot.out.clear();
		write!(slot.out, "{}", fire.glance).expect("`String`'s `Write` is infallible");
		self.bytes[i].0 += slot.out.len() as u64;
		if self.upto == Piece::Glance {
			return;
		}

		slot.detail.clear();
		// The `Err` is the clip reporting it has seen enough; stopping the render early is the point.
		let _ = write!(
			Clip {
				out: &mut slot.detail,
				left: Some(256)
			},
			"{:?}",
			fire.debug
		);
		self.bytes[i].1 += slot.detail.len() as u64;
		if self.upto == Piece::Debug {
			return;
		}

		refill(&mut slot.vals, fire.vals);
		refill(&mut slot.jac, fire.jac);
	}
}

fn refill(dst: &mut Vec<f64>, src: Option<&[f64]>) {
	dst.clear();
	if let Some(src) = src {
		dst.extend_from_slice(src);
	}
}

/// `exec_viz`'s `Clip`, copied rather than shared: it is private there, and a leg that priced a
/// different truncation would price the wrong thing.
struct Clip<'a> {
	out: &'a mut String,
	left: Option<usize>,
}

impl fmt::Write for Clip<'_> {
	fn write_str(&mut self, s: &str) -> fmt::Result {
		let Some(left) = self.left else { return Err(fmt::Error) };
		if s.is_empty() {
			return Ok(());
		}
		match s.char_indices().nth(left) {
			Some((i, _)) => {
				self.out.push_str(&s[..i]);
				self.out.push('…');
				self.left = None;
				Err(fmt::Error)
			}
			None => {
				self.left = Some(left - s.chars().count());
				self.out.push_str(s);
				Ok(())
			}
		}
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

/// The counterfactual the tape is measured against: the same two renderings per fire, with the
/// `Debug` one taken in full. The tape stops it at 256 chars, and stopping is not truncating — a
/// batch root's `Debug` walks its whole arrival otherwise, which is what this leg prices. It reads
/// neither vals nor jac, but asks at the tape's level so the two legs differ only by the clip.
#[derive(Default)]
struct Fmt(usize);

impl Observer for Fmt {
	fn want(&self) -> Want {
		Want::Jac
	}

	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [bool], fire: Fire<'_>) {
		self.0 += format!("{}", fire.glance).len() + format!("{:?}", fire.debug).len();
	}
}
