//! Animates a real fuzz trace into one self-contained SMIL SVG — the asset `docs/.readme_assets/`
//! carries — and prints the coverage tally the film exists to make checkable.
//!
//! `FUZZ_FILM=docs/.readme_assets/fuzz.svg cargo t -p trading_data --features bench --test fuzz`
//!
//! It films [`schedule`], because that target's claim is the one a film can *show*: one element
//! sequence, [`schedule::K`] groupings of it into ticks, and the folded element streams identical
//! across every one of them while the ticks underneath are nothing alike. What it draws is
//! [`Census`] — what this repo already prints when it wants to know what a sweep did — so there is
//! no second graph renderer here to keep in step with the first, and no dependency pointed at
//! `exec_viz`, which depends on this crate's engine.
//!
//! It lives in the fuzz binary rather than in `examples/` because the generator it films is these
//! modules': a film with its own copy of `elements`/`grouping` would drift from the fuzzer the
//! moment either moved, and would then be animating something nothing tests.
//!
//! Nothing is tweened: rows are text, so a keyframe is a `<g>` revealed for its own slot by a
//! discrete `opacity` animation. No browser, no rasterizer, no ffmpeg, and it embeds with a plain
//! `![](…)`.

use std::{fmt::Write as _, path::Path};

use trading_data::bench::Census;

use crate::{
	corpus,
	fixture::{self, G, Outs, Step, Tick, WARM},
	frng::Frng,
	schedule,
};

/// Wall-clock per tick. One keyframe is one tick, not one grouping, so this is what makes a
/// coarse grouping read as few long steps next to a fine one's many short ones.
const STEP_S: f64 = 0.32;
/// Freeze on the last frame before looping, so the end reads as an end.
const HOLD_S: f64 = 1.5;
const PAD: f64 = 12.0;
const LINE_H: f64 = 17.0;
const FONT: f64 = 13.0;
/// Advance width of the monospace stack at [`FONT`]. An estimate — which font of the stack the
/// reader has is the reader's — so the canvas is sized with [`SLACK`] columns of headroom rather
/// than to the character.
const CHAR_W: f64 = FONT * 0.6;
/// Columns of headroom on the canvas, against a monospace advance a little wider than [`CHAR_W`].
/// The deps column is last, so underestimating clips a dependency name off the right edge.
const SLACK: f64 = 3.0;
const C_BG: &str = "#141414";
const C_DIM: &str = "#5c5c5c";
const C_FG: &str = "#ddd";
const C_ACCENT: &str = "#63e9cd";

/// What a filmed trace is checked to reach. Tallied over *every* scanned seed and not only the
/// filmed one: a tally of the winner would hide exactly the gaps this exists to expose.
const BUCKETS: [&str; 7] = [
	"grouping: whole trace in one tick",
	"grouping: one element per tick",
	"grouping: chunked",
	"tick carrying no element",
	"batch straddling a period close",
	"warm-up boundary crossed",
	"peak differs across groupings",
];

/// One tick, drawn.
struct Frame {
	/// [`Census`]'s own `Display`, split. Row `i` is the `i`th node in step order, which
	/// `trading_data_dag/model.typ` states *is* topological order.
	rows: Vec<String>,
	caption: String,
}

pub fn film(out: &Path) {
	// Its own default, a ninth of the fuzzer's: a keyframe is a tick, and every element the trace
	// draws is another tick under the one-element-per-tick grouping. At `DEFAULT_SIZE` the four
	// groupings came to 567 of them — three minutes of film and 1.5 MB of asset. This lands at ~62
	// ticks and ~180 KB, and still reaches every bucket below.
	let size = crate::env_usize("FUZZ_SIZE", 28);
	let seeds: u64 = crate::env_usize("FUZZ_FILM_SEEDS", 128) as u64;
	let mut counts = [0u64; BUCKETS.len()];
	let mut best = (0u64, 0u32);
	for seed in 0..seeds {
		let hits = scan(seed, size);
		for (i, c) in counts.iter_mut().enumerate() {
			*c += u64::from((hits >> i) & 1);
		}
		if hits.count_ones() > best.1 {
			best = (seed, hits.count_ones());
		}
	}
	eprintln!("coverage over seeds 0..{seeds} (size {size}), counted per seed:");
	for (label, n) in BUCKETS.iter().zip(counts) {
		eprintln!("  {label:<38} {n:>6}{}", if n == 0 { "   <-- NEVER REACHED" } else { "" });
	}

	let seed = std::env::var("FUZZ_SEED").ok().map_or(best.0, |s| s.parse().expect("FUZZ_SEED must be a u64"));
	// The film assumes a green fuzzer: a trace that violates the claim is a bug report, and drawing
	// it as though it were the invariant holding would be the asset lying.
	if let Err(what) = schedule::run(&mut Frng::new(seed, size), false) {
		panic!("seed {seed} at size {size} fails the schedule oracle, so there is no invariant to film: {what}");
	}

	let mut f = Frng::new(seed, size);
	let els = schedule::elements(&mut f);
	assert!(!els.is_empty(), "seed {seed} drew an empty element sequence; raise FUZZ_SIZE");
	let mut frames = Vec::new();
	let mut shapes = Vec::new();
	let mut folded = Vec::new();
	for k in 0..schedule::K {
		let steps = schedule::grouping(&mut f, &els, k);
		let mut g = G::default();
		// One census per grouping, not one for the run: what the film is about is how differently the
		// same element sequence is *stepped*, and a shared tally would sum that difference away.
		let mut census = Census::default();
		let mut outs: Vec<Outs> = Vec::new();
		for (i, s) in steps.iter().enumerate() {
			outs.push(fixture::tick_obs(&mut g, s, &mut census));
			frames.push(Frame {
				rows: census.to_string().lines().map(str::to_string).collect(),
				caption: format!(
					// `total` and not `bucket`: it folds a `Folding` dep and so publishes once per element,
					// which is the count `rates.folds.exactly-once` makes invariant — so this number climbs
					// to the same place under every grouping while the ticks beside it do not.
					"seed {seed}  ·  grouping {k} of {}  ·  tick {:>3}/{:<3}  ·  batch {:>2} elements  ·  {:>2} of {} elements folded",
					schedule::K - 1,
					i + 1,
					steps.len(),
					s.src.len(),
					outs.iter().map(|o| o.total.len()).sum::<usize>(),
					els.len(),
				),
			});
		}
		shapes.push(steps.len());
		folded.push(schedule::fold(&outs));
	}

	// Asserted rather than assumed: the footer states the streams agree, and a footer is a claim.
	assert!(
		folded.iter().all(|x| *x == folded[0]),
		"the filmed groupings disagree, which `schedule::run` just said they do not"
	);
	let digest = corpus::fnv(&[&format!("{:?}", folded[0])]);
	let footer = format!(
		"{} elements · {} ticks under {} groupings · folded stream fnv {digest:08x}, the same under every one of them",
		els.len(),
		shapes.iter().map(usize::to_string).collect::<Vec<_>>().join("/"),
		schedule::K,
	);

	// A backstop, not the sizing knob — `FUZZ_SIZE` is. Cutting here cuts whole groupings off the
	// end, and a film missing a grouping is a film of a weaker claim, so it says so loudly.
	let max_s: f64 = std::env::var("FUZZ_FILM_MAX_S").ok().and_then(|s| s.parse().ok()).unwrap_or(30.0);
	let full = frames.len();
	frames.truncate(full.min((max_s / STEP_S).floor() as usize).max(2));
	assert_eq!(
		frames.len(),
		full,
		"{full} ticks is past the {max_s}s ceiling, and the groupings past it would go undrawn — lower FUZZ_SIZE (now {size}) or raise FUZZ_FILM_MAX_S"
	);

	let svg = render(&frames, &footer);
	// A relative path is the workspace root's, not the test runner's cwd — same reason `corpus::path`
	// leans on `CARGO_MANIFEST_DIR`, and the asset this writes lives one level above that.
	let out = match out.is_absolute() {
		true => out.to_path_buf(),
		false => Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(out),
	};
	std::fs::write(&out, &svg).unwrap_or_else(|e| panic!("write {}: {e}", out.display()));
	eprintln!(
		"\nfilmed seed {seed} ({} of {} buckets): {} ticks, {:.1}s, {} KiB -> {}",
		best.1,
		BUCKETS.len(),
		frames.len(),
		duration(frames.len()),
		svg.len() / 1024,
		out.display()
	);
}

/// The bucket mask one seed's trace lands in — read off the generated trace rather than off the
/// oracle, so it says what was *reached* and not merely what passed.
fn scan(seed: u64, size: usize) -> u32 {
	let mut f = Frng::new(seed, size);
	let els = schedule::elements(&mut f);
	if els.is_empty() {
		return 0;
	}
	let mut m = 0u32;
	m |= u32::from(els.len() >= WARM) << 5;
	let mut peaks = Vec::new();
	for k in 0..schedule::K {
		let steps = schedule::grouping(&mut f, &els, k);
		m |= 1
			<< match k {
				0 => 0,
				1 => 1,
				_ => 2,
			};
		if steps.iter().any(|s| s.src.is_empty()) {
			m |= 1 << 3;
		}
		if steps.iter().any(straddles) {
			m |= 1 << 4;
		}
		let mut g = G::default();
		peaks.push(steps.iter().map(|s| fixture::tick(&mut g, s)).last().and_then(|o| o.peak));
	}
	// The sanctioned counter-example: `Peak` reads a running extremum over *evaluated* states, so a
	// coarser grouping legitimately sees a different one — `rates.deps.tick-opaque` says so, and no
	// oracle compares it. A film that never reached the divergence would be showing a weaker claim
	// than the one the target makes.
	m |= u32::from(peaks.iter().any(|p| *p != peaks[0])) << 6;
	m
}

/// Whether a batch carries a `Bucket` period close inside it, rather than between it and the next —
/// the case `schedule::elements` aims its timestamp draw at, and the one a fold on the close is
/// interesting at.
fn straddles(s: &Step) -> bool {
	const PERIOD: i64 = 60_000_000_000;
	let bucket = |t: &Tick| t.ts.div_euclid(PERIOD);
	s.src.windows(2).any(|w| bucket(&w[0]) != bucket(&w[1]))
}

fn duration(n: usize) -> f64 {
	(n.saturating_sub(1)) as f64 * STEP_S + HOLD_S
}

fn render(frames: &[Frame], footer: &str) -> String {
	let cols = frames
		.iter()
		.flat_map(|f| f.rows.iter().chain(std::iter::once(&f.caption)))
		.chain(std::iter::once(&footer.to_string()))
		.map(|l| l.chars().count())
		.max()
		.expect("a film has at least one line");
	let table = frames.iter().map(|f| f.rows.len()).max().expect("a film has at least one frame");
	let w = (cols as f64 + SLACK) * CHAR_W + 2.0 * PAD;
	// caption, blank, the table at its widest, blank, footer.
	let h = 2.0 * PAD + (table as f64 + 4.0) * LINE_H;
	let dur = duration(frames.len());

	let mut body = String::new();
	for (i, f) in frames.iter().enumerate() {
		let start = i as f64 * STEP_S / dur;
		let end = match i + 1 == frames.len() {
			true => 1.0,
			false => (i + 1) as f64 * STEP_S / dur,
		};
		let (values, times) = match i {
			0 => ("1;0".to_string(), format!("0;{end:.6}")),
			_ => ("0;1;0".to_string(), format!("0;{start:.6};{end:.6}")),
		};
		let mut rows = String::new();
		let mut y = PAD + LINE_H;
		let _ = write!(rows, "{}", text(PAD, y, C_FG, &f.caption));
		y += 2.0 * LINE_H;
		for (r, line) in f.rows.iter().enumerate() {
			// A row that moved is a node the sweep reached this tick: every column but the name is a
			// counter, so "changed" and "stepped" are the same reading, and it costs no second observer.
			// Under this target no gate ever shuts, so what stays dim is the header and the roots line —
			// point the film at a target that darkens nodes and their rows fall to dim on their own.
			let moved = i > 0 && frames[i - 1].rows.get(r) != Some(line);
			let _ = write!(rows, "{}", text(PAD, y, if moved || i == 0 { C_ACCENT } else { C_DIM }, line));
			y += LINE_H;
		}
		let _ = write!(
			body,
			r#"<g opacity="0"><animate attributeName="opacity" values="{values}" keyTimes="{times}" calcMode="discrete" dur="{dur:.3}s" repeatCount="indefinite"/>{rows}</g>"#
		);
	}

	format!(
		r#"<svg xmlns="http://www.w3.org/2000/svg" width="{w:.0}" height="{h:.0}" viewBox="0 0 {w:.0} {h:.0}" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="{FONT}">
<rect width="100%" height="100%" fill="{C_BG}"/>
{body}
{}
</svg>
"#,
		text(PAD, h - PAD - LINE_H * 0.4, C_FG, footer)
	)
}

fn text(x: f64, y: f64, fill: &str, line: &str) -> String {
	format!(r#"<text x="{x:.0}" y="{y:.0}" xml:space="preserve" fill="{fill}">{}</text>"#, escape(line))
}

fn escape(s: &str) -> String {
	s.replace('&', "&amp;").replace('<', "&lt;")
}
