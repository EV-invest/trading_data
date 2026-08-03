//! Points in, probabilities out: what the evidence failed to score is ignorance, and ignorance is
//! the default outcome's share.

use trading_data_dag::{Flat, ProbabilisticDistribution};

#[derive(Clone, Copy, Default, PartialEq)]
enum Read {
	#[default]
	Unknown,
	Yes,
	No,
}

#[derive(Clone, Copy)]
struct Verdict([f64; 3]);
impl Flat for Verdict {
	const DIMS: &'static [usize] = &[3];

	fn flat(&self, out: &mut [f64]) -> bool {
		self.0.flat(out)
	}
}
impl ProbabilisticDistribution for Verdict {
	type Outcome = Read;

	const OUTCOMES: &'static [Read] = &[Read::Unknown, Read::Yes, Read::No];
	const POINTS: f64 = 16.0;
}

#[track_caller]
fn check(points: [f64; 3], expect: [f64; 3]) {
	let mut got = points;
	Verdict::certainty(&mut got);
	assert_eq!(got.iter().sum::<f64>(), 1.0, "{points:?} left the space unallocated: {got:?}");
	assert_eq!(got, expect, "{points:?}");
}

#[test]
fn unscored_points_are_the_default_outcome() {
	check([0.0, 0.0, 0.0], [1.0, 0.0, 0.0]);
	// one point out of the root's four: a whisper, and the rest of the space stays unsure.
	check([0.0, 1.0, 0.0], [0.75, 0.25, 0.0]);
	check([1.0, 1.0, 0.0], [0.75, 0.25, 0.0]);
	check([0.0, 3.0, 1.0], [0.0, 0.75, 0.25]);
	// past the root the evidence is overwhelming, and certainty is the split between the outcomes.
	check([0.0, 5.0, 3.0], [0.0, 0.625, 0.375]);
}
