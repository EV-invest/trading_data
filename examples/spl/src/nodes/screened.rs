use core::fmt;

use trading_data::{Flat, Glance};

/// A screener hit. A miss emits nothing at all — SPL's `Screened` contract to the classifier.
#[derive(Clone, Copy, Debug)]
pub struct Screened {
	pub ts_ns: i64,
}

impl Flat for Screened {
	const DIMS: &'static [usize] = &[];

	fn flat(&self, out: &mut [f64]) -> bool {
		out[0] = 1.0;
		true
	}
}
structural_bump!(Screened);

impl Glance for Screened {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("hit")
	}
}
