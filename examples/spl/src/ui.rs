//! The acquisition lanes' and the replay phases' progress bars, in scam_pump_liqs' `fetch::ui`
//! style: one steady-tick bar each, finished green so the completed bars are the run's summary.

use std::{borrow::Cow, time::Duration};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

const TICK: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// A replay phase's bar. Venue time is the only axis whose total is known before the phase runs —
/// tick counts are not — so that is what it measures. Standalone rather than in the lanes'
/// `MultiProgress`: the phases are strictly sequential, and each is finished before the summary
/// line it belongs to prints.
pub fn run(prefix: &'static str, span_ns: i64) -> ProgressBar {
	let pb = ProgressBar::new(span_ns as u64);
	pb.set_style(
		ProgressStyle::with_template(" {spinner:.cyan} {prefix:.bold} [{elapsed_precise}] {bar:30.cyan/238} {percent:>3}% ({eta}) {msg:.dim}")
			.expect("static template")
			.tick_strings(TICK),
	);
	pb.set_prefix(prefix);
	pb.enable_steady_tick(Duration::from_millis(80));
	pb
}
pub fn finish_run(pb: &ProgressBar, msg: impl Into<Cow<'static, str>>) {
	pb.set_position(pb.length().expect("run bars are created with a length"));
	pb.set_style(ProgressStyle::with_template(" ✓ {prefix:.bold.green} [{elapsed_precise}] {bar:30.green/238} {percent:>3}% {msg:.green}").expect("static template"));
	pb.finish_with_message(msg);
}
/// `prefix` is padded by the caller so the done style, which has its own colour, still lines up.
pub(crate) fn lane(mp: &MultiProgress, prefix: &'static str, days: usize) -> ProgressBar {
	let pb = mp.add(ProgressBar::new(days as u64));
	pb.set_style(
		ProgressStyle::with_template(" {spinner:.cyan} {prefix:.bold} [{elapsed_precise}] {bar:30.cyan/238} {pos:>3}/{len} days ({eta}) {msg:.dim}")
			.expect("static template")
			.tick_strings(TICK),
	);
	pb.set_prefix(prefix);
	pb.set_message("waiting");
	pb.enable_steady_tick(Duration::from_millis(80));
	pb
}

pub(crate) fn finish(pb: &ProgressBar, msg: impl Into<Cow<'static, str>>) {
	pb.set_position(pb.length().expect("lane bars are created with a length"));
	pb.set_style(ProgressStyle::with_template(" ✓ {prefix:.bold.green} [{elapsed_precise}] {bar:30.green/238} {pos:>3}/{len} days {msg:.green}").expect("static template"));
	pb.finish_with_message(msg);
}
