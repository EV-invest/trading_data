//! Assert-driver over the SPL port. One long-lived `Graph`, one `Replay` per day over the
//! situation's window, every lane. `Replay` eager-loads its range, so chunking it across successive
//! feeds is the sanctioned way to cover a window; graph state carries across, only the per-lane
//! latency seed resets, which is deterministic. SPL's `request_bars(warmup)` + `Screener::trade_start`
//! have no counterpart: warmth is each node's own `None`, so there is no phase to stage.
//!
//! Which instrument, which window and every strategy knob come from `config.nix` — the same file
//! scam_pump_liqs configures itself with, pointed at its `examples/situations.nix` MOCK target.
//!
//! The asserts below are the integration test — indie warmth, book shape, and the deprecator's
//! five load-bearing invariants — plus SPL's own `trailing_stop_partial_degradation`
//! replayed through the ported `TrailingStop`. The run is recorded by an attached [`Viz`] and served
//! afterwards, so the degradation is eyeballable against price. `nix run .#spl` builds the
//! `exec_viz_web` bundle and runs this against it.

use std::path::PathBuf;

use clap::Parser;
use exec_viz::{Viz, api_types::BarOut};
use trading_data::{BatchWindow, Exact, Feed, LatencyConfig, Replay, Ts, read_mc, read_oi, required_lanes};
use trading_data_core::{ExchangeName, Side, Venue};
use trading_data_spl::{
	asset,
	config::{self, Config, Screen},
	day_bounds, ensure_lanes,
	nodes::{BookTopSnap, Graph, Intent, TrailingStop},
	symbol, trading_days, ui,
};

/// This app's slot in the port range. The base is the devShell's and the flake's; the port is
/// derived, not chosen, so several apps can be up at once and the URL says which is which.
const ORDINAL: u16 = 3;
/// Same number as `flake.nix`'s `port_range_base`, so a bare `cargo r` outside the devShell serves
/// on the slot the flake would have given it.
const PORT_BASE: u16 = 59990;

/// Retained ticks. A trading day weaves into far more than this; the ring keeps the tail, which is
/// what a scrub of the degradation wants.
const SCROLLBACK: usize = 20_000;
const HOUR_NS: i64 = 3600 * 1_000_000_000;
const DAY_NS: i64 = 24 * HOUR_NS;
/// The knob the whole measurement turns on: `TrailingStop`'s extremum and the `lambda_atr` crossing
/// are running readings over *evaluated* states, so how much book one tick may swallow is exactly
/// how much of the degradation goes unseen.
const MEASURED_WINDOW_MS: i64 = 100;
const MEASURED_WINDOW: BatchWindow = BatchWindow::from(Exact::from_nanos(MEASURED_WINDOW_MS * 1_000_000));
#[derive(Parser)]
#[command(about = "scam_pump_liqs as a trading_data graph", long_about = None)]
struct Cli {
	/// Strategy + situation config; the flag wins over the env. Mirrors scam_pump_liqs' `--config`.
	#[arg(long, env = "SPL_CONFIG", default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/config.nix"))]
	config: PathBuf,
}

#[tokio::main]
async fn main() {
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

	// Per-symbol so switching the situation's pair cannot read another one's catalog.
	let cache = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tmp/spl_cache")).join(&situation.bybit_symbol);
	let catalog = ensure_lanes(&cache, situation);

	let latency: LatencyConfig = cfg.backtest.arrival_latency.into();
	let mut graph = Graph::default();

	// Every lane, every day, recorded. There is no warmup phase and no horizon to size: a node that
	// has not seen enough history emits `None`, the same channel it declines on when warm, so nothing
	// downstream can read an unwarmed value in the first place.
	let lanes = required_lanes::<Graph>();
	println!("required lanes: {lanes:?}");
	let viz = Viz::new(Some("Bars<1>"), SCROLLBACK, 60_000);
	let mut recorder = viz.clone();
	let mut day = Day::default();
	let began = std::time::Instant::now();
	let (run_start, _) = day_bounds(days[0]);
	let pb = ui::run("replay", days.len() as i64 * DAY_NS);

	for d in &days {
		let (start, end) = day_bounds(*d);
		let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(situation), start, end, &lanes, latency, MEASURED_WINDOW);
		while let Some(lanes) = feed.next() {
			day.ticks += 1;
			let ts_ns = lanes.ts_venue.as_nanos();
			// Ticks land ~2k/s, and both calls take the draw lock. Every 1024th is still twice a second.
			if day.ticks % 1024 == 0 {
				pb.set_position((ts_ns - run_start.as_nanos()) as u64);
				pb.set_message(format!("{} book reads, {} episodes, {} intents", day.top, day.episodes, day.intents));
			}
			// The tape is the book's only independent witness: nothing downstream ever compares them.
			if let Some(&raw) = lanes.trades.price.last() {
				day.last_trade_px = Some(raw as f64 / lanes.trades.prec.price.scale());
			}
			let out = graph.tick_obs(lanes.into(), recorder.at(ts_ns));

			for b in out.bar_1m {
				viz.bar(BarOut {
					ts_ms: b.ts_open / 1_000_000,
					open: b.open,
					high: b.high,
					low: b.low,
					close: b.close,
					volume: b.vol_base * b.close,
				});
			}
			// Each indie is read on its own, at its own rate — there is no snapshot to gate them together.
			for r in out.rsi.iter().flatten() {
				day.rsi_snaps += 1;
				top(&mut day.max_rsi, r.actual);
			}
			for p in out.price.iter().flatten() {
				day.price_snaps += 1;
				top(&mut day.max_change_1d, p.change_1d_pct);
			}
			for m in out.momentum.iter().flatten() {
				day.momentum_snaps += 1;
				day.first_momentum_ns.get_or_insert(ts_ns);
				top(&mut day.max_sharpe_fast, m.fast);
				if let Some(x) = m.slow {
					top(&mut day.max_sharpe_slow, x);
				}
			}
			day.rsi_hits += out.rsi_screener.iter().flatten().count() as u64;
			day.std_hits += out.std_screener.iter().flatten().count() as u64;
			day.classifications += out.classify.is_some() as u64;

			day.top_reads += out.book_top.len() as u64;
			for x in out.book_top.iter().flatten() {
				day.check_top(x);
			}
			assert_eq!(out.deprecator.len(), out.book_top.len(), "Deprecator/BookTop rate mismatch");
			for i in 0..out.deprecator.len() {
				day.check_intent(out.deprecator[i], out.book_top[i]);
			}
		}
	}
	ui::finish_run(&pb, format!("{} ticks, {} book reads, {} episodes", day.ticks, day.top, day.episodes));

	println!(
		"traded: rsi={} price={} momentum={} top={}/{} rsi_hits={} std_hits={} classifications={} episodes={} intents={}",
		day.rsi_snaps, day.price_snaps, day.momentum_snaps, day.top, day.top_reads, day.rsi_hits, day.std_hits, day.classifications, day.episodes, day.intents
	);
	// What the window bought, and what it cost: the FD observer runs per node per tick, so wall-clock
	// is the binding constraint on how fine `MEASURED_WINDOW` can go.
	println!("{} ticks in {:.1}s at a {MEASURED_WINDOW_MS}ms batch window", day.ticks, began.elapsed().as_secs_f64());
	println!(
		"tape vs book: max |last_trade - mid| / mid = {}",
		day.max_tape_divergence.map_or_else(|| "n/a".to_string(), |v| format!("{:.3}%", v * 100.0))
	);
	// Ingest persists every change to the top-20 view, so the ceiling is the book itself; what the
	// batch window then decides is how many of those changes a tick swallows without anyone looking.
	// SPL's 1Hz timer is the reference, not the target — these gaps should sit well under it.
	let spl_samples = days.len() as u64 * 24 * 3600;
	println!(
		"book cadence: {} reads vs SPL's {spl_samples} 1s samples ({:.1}×); blind gaps: mean {:.0}ms, max {}ms (ending {:?}), {} over 2s, {} over 10s",
		day.top,
		day.top as f64 / spl_samples as f64,
		day.blind_total_ms as f64 / day.top as f64,
		day.max_blind_ms,
		Ts::<Venue>::from_nanos(day.max_blind_at_ns),
		day.blind_over_2s,
		day.blind_over_10s
	);
	let momentum = cfg.strategy.indies.momentum;
	let seen = |x: Option<f64>| x.map_or_else(|| "n/a".to_string(), |v| format!("{v:.3}"));
	println!(
		"window maxima: rsi={} change_1d={}% sharpe {}={}{}",
		seen(day.max_rsi),
		seen(day.max_change_1d),
		momentum.fast,
		seen(day.max_sharpe_fast),
		momentum.slow.map_or_else(String::new, |tf| format!(" {tf}={}", seen(day.max_sharpe_slow)))
	);

	// Only the configured screener's own inputs have to warm. Asserting on all three would re-impose
	// the union the shallow tier used to demand — under `Screen::Std` nothing reads `Rsi` at all.
	let (needed, warm) = match cfg.strategy.screen {
		Screen::Rsi(_) => ("price+rsi", day.price_snaps > 0 && day.rsi_snaps > 0),
		Screen::Std(_) => ("momentum", day.momentum_snaps > 0),
	};
	assert!(
		warm,
		"the configured screener's inputs ({needed}) never warmed in the trading window: rsi={} price={} momentum={}",
		day.rsi_snaps, day.price_snaps, day.momentum_snaps
	);
	// Bars are aggregated from trades, so a bucket with no trade emits none: on a thin instrument the
	// `lookback + 1`-th *emitted* bar lands strictly later than `lookback + 1` bar-widths in, which is
	// why this is a floor and never the expected value.
	let widest = [Some(momentum.fast), momentum.slow].into_iter().flatten().max().expect("a fast leg is always configured");
	let earliest = run_start.as_nanos() + (momentum.lookback as i64 + 1) * widest.duration().as_nanos() as i64;
	if let Some(warm_at) = day.first_momentum_ns {
		assert!(
			warm_at >= earliest,
			"momentum published at {:?}, before its window could possibly have filled ({:?})",
			Ts::<Venue>::from_nanos(warm_at),
			Ts::<Venue>::from_nanos(earliest)
		);
	}
	assert_eq!(day.top, day.top_reads, "the book was unsynced on some read — our own checkpoints failed to seed it");
	trailing_stop_partial_degradation();

	let oi: Vec<_> = read_oi(&catalog, ExchangeName::Bybit, symbol(situation), Ts::MIN, Ts::MAX).expect("open oi lane").collect();
	assert!(oi.windows(2).all(|w| w[0].ts_venue_exec <= w[1].ts_venue_exec), "oi timestamps unordered");
	let mc: Vec<_> = read_mc(&catalog, asset(situation), Ts::MIN, Ts::MAX).expect("open mc lane").collect();
	println!("oi rows={} mc rows={} (mc={:.3e})", oi.len(), mc.len(), mc[0].market_cap);

	println!("spl: ok");
	let port = std::env::var("PORT").map_or(PORT_BASE, |p| p.parse().expect("PORT is a u16")) + ORDINAL;
	println!("serving on http://localhost:{port}");
	viz.serve(port).await;
}

/// Per-day accumulator and the running assert block over the book/deprecator streams.
#[derive(Default)]
struct Day {
	rsi_snaps: u64,
	price_snaps: u64,
	momentum_snaps: u64,
	/// Book reads that produced a snapshot, and every read attempted — equal unless the book desyncs.
	top: u64,
	top_reads: u64,
	last_top_ns: i64,
	last_trade_px: Option<f64>,
	/// Milliseconds the graph spent between two book evaluations — SPL's timer would have looked
	/// every second regardless.
	max_blind_ms: i64,
	max_blind_at_ns: i64,
	blind_over_2s: u64,
	blind_over_10s: u64,
	blind_total_ms: i64,
	max_tape_divergence: Option<f64>,
	rsi_hits: u64,
	std_hits: u64,
	classifications: u64,
	ticks: u64,
	intents: u64,
	episodes: u64,
	/// First tick a Sharpe landed on, for the floor assert.
	first_momentum_ns: Option<i64>,
	max_rsi: Option<f64>,
	max_change_1d: Option<f64>,
	max_sharpe_fast: Option<f64>,
	max_sharpe_slow: Option<f64>,
	last_intent_ns: i64,
	/// Dropped rather than closed at end of day: an episode still open then never had to drain.
	open: Option<Open>,
}
impl Day {
	fn check_top(&mut self, d: &BookTopSnap) {
		self.top += 1;
		// A fold that lost its precision, or that dropped deletes and froze a phantom top, drifts away
		// from the traded price and nothing else here would notice. A tape print can still land between
		// two book messages, so the bound is for corruption, not for staleness — the observed max is
		// printed so the two stay distinguishable.
		if let Some(px) = self.last_trade_px {
			let divergence = (px - d.mid()).abs() / d.mid();
			top(&mut self.max_tape_divergence, divergence);
			assert!(
				divergence < 0.1,
				"book mid {} is {:.1}% away from the last trade at {px} — the fold has lost the market, at {}",
				d.mid(),
				divergence * 100.0,
				d.ts_ns
			);
		}
		// A read is stamped with the last delta of its woven run and the weaver never reorders, so reads
		// walk the lane forwards. Equality is legal — two runs can end on messages sharing the archive's
		// millisecond resolution.
		assert!(d.ts_ns >= self.last_top_ns, "book reads went backwards at {}, after {}", d.ts_ns, self.last_top_ns);
		if self.last_top_ns != 0 {
			let gap = (d.ts_ns - self.last_top_ns) / 1_000_000;
			if gap > self.max_blind_ms {
				(self.max_blind_ms, self.max_blind_at_ns) = (gap, d.ts_ns);
			}
			self.blind_over_2s += u64::from(gap > 2_000);
			self.blind_over_10s += u64::from(gap > 10_000);
			self.blind_total_ms += gap;
		}
		self.last_top_ns = d.ts_ns;
		assert!(d.best_bid < d.best_ask, "crossed book at {}: {} >= {}", d.ts_ns, d.best_bid, d.best_ask);
		assert!(d.spread_pct.is_finite() && d.spread_pct > 0.0, "degenerate spread at {}: {}", d.ts_ns, d.spread_pct);
		assert!(d.top20_bid_depth_usd > 0.0 && d.top20_ask_depth_usd > 0.0, "empty top-20 depth at {}", d.ts_ns);
		assert!((-1.0..=1.0).contains(&d.imbalance), "imbalance out of range at {}: {}", d.ts_ns, d.imbalance);
	}

	fn check_intent(&mut self, intent: Option<Intent>, top: Option<BookTopSnap>) {
		let Some(i) = intent else {
			self.close_episode();
			return;
		};
		let d = top.expect("an intent is only emitted on a book tick");
		self.intents += 1;
		assert!(i.ts_ns > self.last_intent_ns, "intents not strictly increasing: {} <= {}", i.ts_ns, self.last_intent_ns);
		self.last_intent_ns = i.ts_ns;
		assert!(i.side == Side::Buy, "the ported classification stub hardcodes Buy");
		assert!((0.0..=i.base_q).contains(&i.target_q), "target_q {} outside [0, base_q={}] at {}", i.target_q, i.base_q, i.ts_ns);
		let inside = d.mid() > i.sl && d.mid() < i.tp;
		assert_eq!(i.lambda_atr == 0.0, !inside, "lambda_atr disagrees with the ATR envelope at {}", i.ts_ns);

		// Two episodes can be adjacent in the stream, so the id is what separates them, not a gap.
		if self.open.is_some_and(|o| o.episode != i.episode) {
			self.close_episode();
		}
		let open = self.open.unwrap_or_else(|| {
			self.episodes += 1;
			Open {
				episode: i.episode,
				trail_fraction: 1.0,
				first_zero_ns: None,
				prev_ns: i.ts_ns,
				last_ns: i.ts_ns,
			}
		});
		assert!(
			i.trail_fraction <= open.trail_fraction,
			"trail_fraction rose within an episode ({} > {}) — certainty must ratchet",
			i.trail_fraction,
			open.trail_fraction
		);
		let first_zero_ns = open.first_zero_ns.or((i.eval == 0.0).then_some(i.ts_ns));
		assert_eq!(i.draining, first_zero_ns.is_some(), "draining must latch on the first zero eval, at {}", i.ts_ns);
		if i.draining {
			assert_eq!(i.target_q, 0.0, "draining tick still carries size at {}", i.ts_ns);
		}
		self.open = Some(Open {
			episode: i.episode,
			trail_fraction: i.trail_fraction,
			first_zero_ns,
			prev_ns: open.last_ns,
			last_ns: i.ts_ns,
		});
	}

	fn close_episode(&mut self) {
		let Some(o) = self.open.take() else { return };
		let Some(first_zero_ns) = o.first_zero_ns else {
			panic!("episode ended at {} without ever reaching zero eval — only the drain deadline may end one", o.last_ns);
		};
		// Book ticks land on the last message of a woven run, at no fixed spacing: the invariant is that
		// the episode ended on the *first* tick at or after the deadline, not within any slack of it.
		let deadline = first_zero_ns + config::strategy().classification.drain_grace.duration().as_nanos() as i64;
		assert!(o.last_ns >= deadline, "episode ended {}ns before its drain deadline", deadline - o.last_ns);
		assert!(o.prev_ns < deadline, "episode outlived its drain deadline by a whole tick, ending at {}", o.last_ns);
	}
}

/// The episode currently being checked.
#[derive(Clone, Copy)]
struct Open {
	episode: u64,
	trail_fraction: f64,
	/// When 100% deprecation first latched the drain clock.
	first_zero_ns: Option<i64>,
	prev_ns: i64,
	last_ns: i64,
}

fn top(slot: &mut Option<f64>, v: f64) {
	*slot = Some(slot.map_or(v, |m| m.max(v)));
}

/// SPL's own unit test of the trail (`classification/lib.rs:353`), replayed through the port. The
/// one test carried over: it is a persisted useful payload, not a retrofit.
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
