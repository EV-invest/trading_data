//! Fixed-point money. A raw integer is meaningless without the [`Precision`] it was scaled by, so
//! the two travel together everywhere except the columnar buffers, where the run's precision is
//! hoisted out of the loop.

use serde::{Deserialize, Serialize};

/// Decimal exponent: `raw = value × 10^precision`. Signed, so a million-dollar index tick and a
/// satoshi tick both fit the same 32-bit raw column.
#[derive(Clone, Copy, Debug, Default, Deserialize, derive_more::Display, Eq, derive_more::From, derive_more::FromStr, Hash, derive_more::Into, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Precision(pub i8);

impl Precision {
	/// Raw ticks per unit. Hoist it once per run and divide inside the loop — the raw column is
	/// what the venue sent and what the disk holds, so this is the only decode there is.
	pub fn scale(self) -> f64 {
		10f64.powi(self.0 as i32)
	}

	pub fn parse_i32(self, s: &str) -> i32 {
		digits(s, self).parse().expect("realigned digits fit i32")
	}

	pub fn parse_u32(self, s: &str) -> u32 {
		digits(s, self).parse().expect("realigned digits fit u32")
	}
}

/// Re-align a decimal string onto `precision`'s tick and return it as a raw-integer string.
/// Trailing zeros are insignificant (Binance pads `.24` to `.24000000`); a significant digit the
/// tick cannot hold is a feed/config mismatch and panics.
fn digits(s: &str, precision: Precision) -> String {
	let (int, frac) = s.split_once('.').unwrap_or((s, ""));
	let frac = frac.trim_end_matches('0');
	let mut significant = String::with_capacity(int.len() + frac.len());
	significant.push_str(int);
	significant.push_str(frac);

	match precision.0 as isize - frac.len() as isize {
		0 => significant,
		pad if pad > 0 => {
			significant.extend(std::iter::repeat_n('0', pad as usize));
			significant
		}
		cut => {
			let keep = significant.len().saturating_sub(-cut as usize);
			let (head, tail) = significant.split_at(keep);
			assert!(tail.bytes().all(|b| b == b'0'), "{s:?} carries digits below the 10^{} tick", precision.0);
			match head {
				"" | "-" => "0".to_owned(),
				h => h.to_owned(),
			}
		}
	}
}

/// Per-batch precision shared across all levels / trades in a book / trade batch.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, derive_new::new)]
pub struct PrecisionPriceQty {
	pub price: Precision,
	pub qty: Precision,
}

/// Fixed-point quantity. Non-negative.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, derive_new::new)]
pub struct Qty {
	pub raw: u32,
	pub precision: Precision,
}

impl Qty {
	pub fn from_f64(value: f64, precision: Precision) -> Self {
		Self {
			raw: (value * precision.scale()).round() as u32,
			precision,
		}
	}

	pub fn as_f64(self) -> f64 {
		self.raw as f64 / self.precision.scale()
	}

	pub fn is_zero(self) -> bool {
		self.raw == 0
	}
}

/// Fixed-point price. Signed to support spreads and options.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, derive_new::new)]
pub struct Price {
	pub raw: i32,
	pub precision: Precision,
}

impl Price {
	pub fn from_f64(value: f64, precision: Precision) -> Self {
		Self {
			raw: (value * precision.scale()).round() as i32,
			precision,
		}
	}

	pub fn as_f64(self) -> f64 {
		self.raw as f64 / self.precision.scale()
	}

	pub fn is_zero(self) -> bool {
		self.raw == 0
	}

	pub fn max(precision: Precision) -> Self {
		Self { raw: i32::MAX, precision }
	}

	pub fn min(precision: Precision) -> Self {
		Self { raw: i32::MIN, precision }
	}
}

impl From<Price> for f64 {
	fn from(p: Price) -> f64 {
		p.as_f64()
	}
}

impl From<Qty> for f64 {
	fn from(q: Qty) -> f64 {
		q.as_f64()
	}
}

/// Precision is inferred from the literal's own decimals — a venue-supplied precision goes through
/// [`Precision::parse_i32`] instead.
impl std::str::FromStr for Price {
	type Err = std::num::ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (precision, raw_str) = split_decimal(s);
		Ok(Self { raw: raw_str.parse()?, precision })
	}
}

impl std::str::FromStr for Qty {
	type Err = std::num::ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (precision, raw_str) = split_decimal(s);
		Ok(Self { raw: raw_str.parse()?, precision })
	}
}

fn split_decimal(s: &str) -> (Precision, String) {
	match s.split_once('.') {
		Some((int, frac)) => (Precision(frac.len() as i8), format!("{int}{frac}")),
		None => (Precision(0), s.to_owned()),
	}
}

impl std::fmt::Display for Price {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{:.prec$}", self.as_f64(), prec = self.precision.0.max(0) as usize)
	}
}

impl std::fmt::Display for Qty {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{:.prec$}", self.as_f64(), prec = self.precision.0.max(0) as usize)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn round_trips() {
		assert_eq!(Price::from_f64(42000.50, Precision(2)).as_f64(), 42000.50);
		assert_eq!(Qty::from_f64(1.234, Precision(3)).as_f64(), 1.234);
		assert!(Qty::from_f64(0.0, Precision(2)).is_zero());
		// raw integer addition is exact where f64 fails for 0.1 + 0.2
		assert_eq!(
			Price::from_f64(0.1, Precision(1)).raw + Price::from_f64(0.2, Precision(1)).raw,
			Price::from_f64(0.3, Precision(1)).raw
		);
	}

	#[test]
	fn from_str_infers_precision() {
		assert_eq!("42000.50".parse::<Price>().unwrap(), Price::new(4200050, Precision(2)));
		assert_eq!("-1.25".parse::<Price>().unwrap(), Price::new(-125, Precision(2)));
		assert_eq!("100".parse::<Price>().unwrap(), Price::new(100, Precision(0)));
		assert_eq!("1.234".parse::<Qty>().unwrap(), Qty::new(1234, Precision(3)));
		assert_eq!("50".parse::<Qty>().unwrap(), Qty::new(50, Precision(0)));
	}

	#[test]
	fn realign_to_tick() {
		assert_eq!(Precision(8).parse_i32("0.24"), 24_000_000);
		assert_eq!(Precision(2).parse_i32("42000.50"), 4_200_050);
		assert_eq!(Precision(0).parse_u32("0"), 0);
		// large-tick instruments: raw stays in i32 range
		assert_eq!(Precision(-3).parse_i32("12000000"), 12_000);
		assert_eq!(Precision(-3).parse_i32("-12000000"), -12_000);
		assert_eq!(Precision(-3).parse_u32("0"), 0);
	}

	#[test]
	#[should_panic(expected = "carries digits below")]
	fn tick_too_coarse() {
		Precision(-3).parse_i32("12000500");
	}
}
