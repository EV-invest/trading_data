//! Check-driver over the SPL port. One long-lived `Graph`, one `Replay` per day over the
//! situation's window, every lane. `Replay` eager-loads its range, so chunking it across successive
//! feeds is the sanctioned way to cover a window; graph state carries across, only the per-lane
//! latency seed resets, which is deterministic. SPL's `request_bars(warmup)` + `Screener::trade_start`
//! have no counterpart: warmth is each node's own `None`, so there is no phase to stage.
//!
//! Which instrument, which window and every strategy knob come from `config.nix` — the same file
//! scam_pump_liqs configures itself with, pointed at its `examples/situations.nix` MOCK target.
//!
//! The checks below are the integration test — the deprecator's five load-bearing invariants, plus
//! SPL's own `trailing_stop_partial_degradation` replayed
//! through the ported `TrailingStop`. Everything the replay observes is a *finding about market
//! data*, not tainted internal state, so it is counted and logged rather than panicked: killing an
//! eight-minute run destroys the very tape you wanted to look at it with. Only the trail check
//! panics — it reads no data, and so runs first, before a byte is loaded.
//!
//! The run is recorded by an attached [`Viz`] whose server runs *alongside* it, so the degradation
//! is eyeballable against price while it is still being produced. `nix run .#spl` builds the
//! `exec_viz_web` bundle and runs this against it.

use std::path::PathBuf;

use clap::Parser;
use exec_viz::Viz;
use trading_data::{Exact, ExchangeName, Feed, LatencyConfig, ReadClock, Replay, Side, Ts, read_mc, read_oi, required_lanes};
use trading_data_spl::{
	asset,
	config::{self, Config},
	day_bounds, ensure_lanes,
	nodes::{Graph, Intent, TrailingStop},
	symbol, trading_days, ui,
};
use v_utils::utils::tracing::{LogDestination, init_subscriber};

/// A failed observation about the replayed data: counted and logged, never fatal. The run's whole
/// value is the tape it leaves behind, and a panic mid-replay throws that away.
macro_rules! flag {
	($self:ident, $cond:expr, $($arg:tt)*) => {
		if !$cond {
			$self.violations += 1;
			tracing::error!($($arg)*);
		}
	};
}

/// This app's slot in the port range. The base is the devShell's and the flake's; the port is
/// derived, not chosen, so several apps can be up at once and the URL says which is which.
const ORDINAL: u16 = 3;
/// Same number as `flake.nix`'s `port_range_base`, so a bare `cargo r` outside the devShell serves
/// on the slot the flake would have given it.
const PORT_BASE: u16 = 59990;

/// Retained ticks. A trading day weaves into ~50× this, so most of the run is only reachable at a
/// stride — the tape thins with age rather than forgetting its front, which keeps the degradation
/// at the end tick-exact and the morning still walkable.
const SCROLLBACK: usize = 20_000;
const HOUR_NS: i64 = 3600 * 1_000_000_000;
const DAY_NS: i64 = 24 * HOUR_NS;
#[derive(Parser)]
#[command(about = "scam_pump_liqs as a trading_data graph", long_about = None)]
struct Cli {
	/// Strategy + situation config; the flag wins over the env. Mirrors scam_pump_liqs' `--config`.
	#[arg(long, env = "SPL_CONFIG", default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix"))]
	config: PathBuf,
}

#[tokio::main]
async fn main() {
	// `stderr_errors(false)`: the progress bar owns the terminal for the whole run, and a violation
	// smeared across it would read worse than the JSON line. The final summary is what points here.
	init_subscriber(LogDestination::file(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl.log")).stderr_errors(false));

	// Data-independent, so it costs nothing to fail in the first millisecond rather than the last.
	trailing_stop_partial_degradation();

	let cli = Cli::parse();
	let cfg = Config::load(&cli.config);
	let situation = &cfg.situation;
	let days = trading_days(situation);
	println!(
		"{} {} .. {} ({} days), screen {:?}, {}",
		situation.pair,
		situation.start,
		situation.end,
		days.len(),
		cfg.strategy.screen,
		cfg.strategy.indies.rsi
	);

	// Before the archives, not after: every node asserts the config names something it wires as it is
	// constructed, and a cold cache would otherwise spend gigabytes earning the right to that panic.
	let mut graph = Graph::default();

	// Per-symbol so switching the situation's pair cannot read another one's catalog.
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);

	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let read_clock = ReadClock::from(Exact::from(cfg.backtest.read_clock.duration()));

	// Every lane, every day, recorded. There is no warmup phase and no horizon to size: a node that
	// has not seen enough history emits `None`, the same channel it declines on when warm, so nothing
	// downstream can read an unwarmed value in the first place.
	let lanes = required_lanes::<Graph>();
	println!("required lanes: {lanes:?}");
	// no candles: the 1m bar is no graph output, so nothing feeds `Viz::bar` and the series draws as
	// an indicator pane like any other node.
	let viz = Viz::new(None, SCROLLBACK, 60_000);
	let mut recorder = viz.clone();
	let mut day = Day::default();
	let began = std::time::Instant::now();
	let (run_start, _) = day_bounds(days[0]);

	// Bound before a byte is read, and printed before the bar starts redrawing over it: the point of
	// serving concurrently is that the URL works from the first second.
	let port = std::env::var("PORT").map_or(PORT_BASE, |p| p.parse().expect("PORT is a u16")) + ORDINAL;
	let server = viz.clone().serve_on(Viz::bind(port).await);
	println!("serving on http://localhost:{port}");

	let pb = ui::run("replay", days.len() as i64 * DAY_NS);
	tokio::join!(server, async {
		for d in &days {
			let (start, end) = day_bounds(*d);
			// ponytail: `Replay::new` eager-decodes the day's parquet (~40MB, seconds) with no await, so
			// new connections stall for that long once per day. Chunking the window across successive
			// `Replay`s (see the module doc) fixes it, but resets the per-lane latency seed at each
			// boundary, which moves the run's numbers.
			let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, read_clock);
			while let Some(lanes) = feed.next() {
				day.ticks += 1;
				let ts_ns = lanes.ts_venue.as_nanos();
				if day.ticks % 256 == 0 {
					// `join!` polls both branches on this one task: without a yield the accept loop never
					// runs, and the bound port stays deaf for the whole replay.
					tokio::task::yield_now().await;
					// Both calls take the draw lock. Every 1024th tick is still twice a second.
					if day.ticks % 1024 == 0 {
						pb.set_position((ts_ns - run_start.as_nanos()) as u64);
						pb.set_message(format!("{} episodes, {} intents{}", day.episodes, day.intents, day.violations_msg()));
					}
				}
				let out = graph.tick_obs(lanes.into(), recorder.at(ts_ns));

				for intent in out.deprecator {
					day.check_intent(*intent);
				}
			}
		}
		ui::finish_run(&pb, format!("{} ticks, {} episodes{}", day.ticks, day.episodes, day.violations_msg()));

		println!("traded: episodes={} intents={}", day.episodes, day.intents);
		println!("{} ticks in {:.1}s on a {} read clock", day.ticks, began.elapsed().as_secs_f64(), cfg.backtest.read_clock);

		let oi: Vec<_> = read_oi(&catalog, ExchangeName::Bybit, symbol(situation), Ts::MIN, Ts::MAX).expect("open oi lane").collect();
		flag!(day, oi.windows(2).all(|w| w[0].ts_venue_exec <= w[1].ts_venue_exec), "oi timestamps unordered");
		let mc: Vec<_> = read_mc(&catalog, asset(situation), Ts::MIN, Ts::MAX).expect("open mc lane").collect();
		println!("oi rows={} mc rows={} (mc={:.3e})", oi.len(), mc.len(), mc[0].market_cap);

		match day.violations {
			0 => println!("spl: ok"),
			n => println!("spl: {n} INVARIANT VIOLATIONS — see tmp/spl.log"),
		}
		recorder.seal();
	});
}

/// Per-day accumulator and the running check block over the deprecator stream.
#[derive(Default)]
struct Day {
	ticks: u64,
	intents: u64,
	episodes: u64,
	/// Observations that failed; each one is in `tmp/spl.log` with its detail.
	violations: u64,
	last_intent_ns: i64,
	/// Dropped rather than closed at end of day: an episode still open then never had to drain.
	open: Option<Open>,
}
impl Day {
	/// Empty while the run is clean, so the bar reads exactly as it always did and only a violated
	/// run shouts.
	fn violations_msg(&self) -> String {
		match self.violations {
			0 => String::new(),
			n => format!(", {n} VIOLATIONS"),
		}
	}

	fn check_intent(&mut self, intent: Option<Intent>) {
		// An idle slot, or a book tick that declined to publish mid-episode — neither ends an episode.
		// Only `Intent::terminal` does, below.
		let Some(i) = intent else { return };
		self.intents += 1;
		flag!(self, i.ts_ns > self.last_intent_ns, "intents not strictly increasing: {} <= {}", i.ts_ns, self.last_intent_ns);
		self.last_intent_ns = i.ts_ns;
		flag!(
			self,
			(0.0..=i.base_q).contains(&i.target_q),
			"target_q {} outside [0, base_q={}] at {}",
			i.target_q,
			i.base_q,
			i.ts_ns
		);
		let open = self.open.unwrap_or_else(|| {
			self.episodes += 1;
			Open {
				trail_fraction: 1.0,
				first_zero_ns: None,
				prev_ns: i.ts_ns,
				last_ns: i.ts_ns,
			}
		});
		flag!(
			self,
			i.trail_fraction <= open.trail_fraction,
			"trail_fraction rose within an episode ({} > {}) — certainty must ratchet",
			i.trail_fraction,
			open.trail_fraction
		);
		let first_zero_ns = open.first_zero_ns.or((i.eval == 0.0).then_some(i.ts_ns));
		flag!(
			self,
			i.draining == first_zero_ns.is_some(),
			"draining ({}) must latch on the first zero eval, at {}",
			i.draining,
			i.ts_ns
		);
		if i.draining {
			flag!(self, i.target_q == 0.0, "draining tick still carries size {} at {}", i.target_q, i.ts_ns);
		}
		self.open = Some(Open {
			trail_fraction: i.trail_fraction,
			first_zero_ns,
			prev_ns: open.last_ns,
			last_ns: i.ts_ns,
		});
		if i.terminal {
			self.close_episode();
		}
	}

	fn close_episode(&mut self) {
		let Some(o) = self.open.take() else { return };
		let Some(first_zero_ns) = o.first_zero_ns else {
			flag!(
				self,
				false,
				"episode ended at {} without ever reaching zero eval — only the drain deadline may end one",
				o.last_ns
			);
			return;
		};
		// The invariant is that the episode ended on the *first* tick at or after the deadline, not
		// within any slack of it.
		let deadline = first_zero_ns + config::strategy().classification.drain_grace.duration().as_nanos() as i64;
		flag!(self, o.last_ns >= deadline, "episode ended {}ns before its drain deadline", deadline - o.last_ns);
		flag!(self, o.prev_ns < deadline, "episode outlived its drain deadline by a whole tick, ending at {}", o.last_ns);
	}
}

/// The episode currently being checked.
#[derive(Clone, Copy)]
struct Open {
	trail_fraction: f64,
	/// When 100% deprecation first latched the drain clock.
	first_zero_ns: Option<i64>,
	prev_ns: i64,
	last_ns: i64,
}

/// SPL's own unit test of the trail (`classification/lib.rs:353`), replayed through the port. The
/// one test carried over: it is a persisted useful payload, not a retrofit. Reads no market data,
/// so it stays a panic — a broken trail means the run would be measuring nothing.
fn trailing_stop_partial_degradation() {
	let check = |side: Side, seq: &[(f64, f64)]| {
		let mut trail = TrailingStop::new(side, 10.0, 0.33);
		for (i, &(price, expected)) in seq.iter().enumerate() {
			let (multiplier, _, _) = trail.step(price);
			assert_eq!(multiplier, expected, "{side:?} step {i} at price {price}");
		}
	};
	let full = 1.0 - 0.33;
	check(
		Side::Buy,
		&[
			(100.0, 1.0),              // extreme seeds here, no retrace
			(95.0, 1.0 - 0.33 * 0.5),  // halfway to cut-off ⇒ half certainty
			(100.0, 1.0 - 0.33 * 0.5), // recovery to the extreme re-adds nothing
			(110.0, 1.0 - 0.33 * 0.5), // new extreme ratchets up, certainty holds
			(100.0, full),             // full `distance` retrace ⇒ capped at severity
			(50.0, full),              // beyond cut-off stays capped
		],
	);
	check(
		Side::Sell,
		&[
			(100.0, 1.0),
			(105.0, 1.0 - 0.33 * 0.5),
			(100.0, 1.0 - 0.33 * 0.5),
			(90.0, 1.0 - 0.33 * 0.5),
			(100.0, full),
			(150.0, full),
		],
	);
}
