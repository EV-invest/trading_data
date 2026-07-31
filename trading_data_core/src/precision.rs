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

/// Re-scale `raw` from one tick onto another. Upscaling is exact or overflows; downscaling is only
/// defined when the digits it drops are zero — anything else is the same feed/config mismatch
/// [`digits`] rejects.
fn realign(raw: i64, from: Precision, to: Precision) -> i64 {
	let gap = to.0 as i32 - from.0 as i32;
	let pow = 10i64.checked_pow(gap.unsigned_abs()).expect("precision gap fits i64");
	match gap >= 0 {
		true => raw.checked_mul(pow).expect("upscaled raw fits i64"),
		false => {
			assert_eq!(raw % pow, 0, "{raw}@10^{} carries digits below the 10^{} tick", from.0, to.0);
			raw / pow
		}
	}
}

/// A raw integer plus the tick it was scaled by. Arithmetic realigns the RHS onto the LHS and keeps
/// the LHS's precision; a realignment or range failure is corrupt state, so it panics.
macro_rules! precise {
	($name:ident, $raw:ty) => {
		#[allow(non_camel_case_types)]
		#[derive(Clone, Copy, Debug, Default, Eq, derive_new::new)]
		pub struct $name {
			raw: $raw,
			prec: Precision,
		}

		impl $name {
			pub fn as_f64(self) -> f64 {
				self.raw as f64 / self.prec.scale()
			}

			fn combine(self, rhs: Self, op: fn(i64, i64) -> Option<i64>) -> Self {
				let rhs = realign(rhs.raw as i64, rhs.prec, self.prec);
				let raw = op(self.raw as i64, rhs).expect("no i64 overflow between two 32-bit raws");
				Self {
					raw: <$raw>::try_from(raw).expect(concat!("result fits ", stringify!($raw))),
					prec: self.prec,
				}
			}
		}

		impl std::ops::Add for $name {
			type Output = Self;

			fn add(self, rhs: Self) -> Self {
				self.combine(rhs, i64::checked_add)
			}
		}

		impl std::ops::Sub for $name {
			type Output = Self;

			fn sub(self, rhs: Self) -> Self {
				self.combine(rhs, i64::checked_sub)
			}
		}

		impl std::ops::AddAssign for $name {
			fn add_assign(&mut self, rhs: Self) {
				*self = *self + rhs;
			}
		}

		impl std::ops::SubAssign for $name {
			fn sub_assign(&mut self, rhs: Self) {
				*self = *self - rhs;
			}
		}

		/// Over the *value*, not the `(raw, precision)` tuple: `1.00` and `1` are one number.
		impl Ord for $name {
			fn cmp(&self, other: &Self) -> std::cmp::Ordering {
				let to = Precision(self.prec.0.max(other.prec.0));
				realign(self.raw as i64, self.prec, to).cmp(&realign(other.raw as i64, other.prec, to))
			}
		}

		impl PartialOrd for $name {
			fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
				Some(self.cmp(other))
			}
		}

		impl PartialEq for $name {
			fn eq(&self, other: &Self) -> bool {
				self.cmp(other).is_eq()
			}
		}

		impl std::fmt::Display for $name {
			fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				write!(f, "{:.prec$}", self.as_f64(), prec = self.prec.0.max(0) as usize)
			}
		}
	};
}

precise!(pi32, i32);
precise!(pu32, u32);

impl std::ops::Neg for pi32 {
	type Output = Self;

	fn neg(self) -> Self {
		Self { raw: -self.raw, prec: self.prec }
	}
}

/// The shared half of a money type: construction, decode, and text. What differs is which ops it
/// admits — a price is a point on a scale, a quantity is a magnitude.
macro_rules! money {
	($(#[$doc:meta])* $name:ident, $inner:ident, $raw:ty) => {
		$(#[$doc])*
		#[derive(Clone, Copy, Debug, Default, derive_more::Display, Eq, Ord, PartialEq, PartialOrd)]
		pub struct $name($inner);

		impl $name {
			pub fn new(raw: $raw, precision: Precision) -> Self {
				Self($inner::new(raw, precision))
			}

			pub fn as_f64(self) -> f64 {
				self.0.as_f64()
			}
		}

		impl From<$name> for f64 {
			fn from(v: $name) -> f64 {
				v.as_f64()
			}
		}

		/// Precision is inferred from the literal's own decimals — a venue-supplied precision goes
		/// through [`Precision::parse_i32`] instead.
		impl std::str::FromStr for $name {
			type Err = std::num::ParseIntError;

			fn from_str(s: &str) -> Result<Self, Self::Err> {
				let (precision, raw_str) = split_decimal(s);
				Ok(Self::new(raw_str.parse()?, precision))
			}
		}
	};
}

money!(
	/// Fixed-point price. Signed to support options and index ticks below zero.
	Price,
	pi32,
	i32
);
money!(
	/// Fixed-point quantity. Non-negative.
	Qty,
	pu32,
	u32
);

/// A price plus a price is not a price — only the difference of two is a value, and it is a
/// [`pi32`] delta, not a `Price`.
impl std::ops::Sub for Price {
	type Output = pi32;

	fn sub(self, rhs: Self) -> pi32 {
		self.0 - rhs.0
	}
}

impl std::ops::Add for Qty {
	type Output = Self;

	fn add(self, rhs: Self) -> Self {
		Self(self.0 + rhs.0)
	}
}

impl std::ops::Sub for Qty {
	type Output = Self;

	fn sub(self, rhs: Self) -> Self {
		Self(self.0 - rhs.0)
	}
}

impl std::ops::AddAssign for Qty {
	fn add_assign(&mut self, rhs: Self) {
		self.0 += rhs.0;
	}
}

impl std::ops::SubAssign for Qty {
	fn sub_assign(&mut self, rhs: Self) {
		self.0 -= rhs.0;
	}
}

fn split_decimal(s: &str) -> (Precision, String) {
	match s.split_once('.') {
		Some((int, frac)) => (Precision(frac.len() as i8), format!("{int}{frac}")),
		None => (Precision(0), s.to_owned()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn compares_by_value_not_by_representation() {
		assert_eq!(Price::new(100, Precision(2)), Price::new(1, Precision(0)));
		assert!(Price::new(100, Precision(2)) < Price::new(5, Precision(0)));
		assert!(Qty::new(1234, Precision(3)) > Qty::new(1, Precision(0)));
	}

	#[test]
	fn arithmetic_lands_on_the_lhs_precision() {
		let spread = Price::new(4200075, Precision(2)) - Price::new(420005, Precision(1));
		assert_eq!(spread, pi32::new(25, Precision(2)));
		assert_eq!(spread.as_f64(), 0.25);

		let mut q = Qty::new(1500, Precision(3));
		q += Qty::new(2, Precision(0));
		assert_eq!(q, Qty::new(3500, Precision(3)));
		// exact where f64 fails for 0.1 + 0.2
		assert_eq!(Qty::new(1, Precision(1)) + Qty::new(2, Precision(1)), Qty::new(3, Precision(1)));
	}

	#[test]
	#[should_panic(expected = "carries digits below")]
	fn lossy_downscale_panics() {
		let _ = Price::new(1, Precision(0)) - Price::new(125, Precision(2));
	}

	#[test]
	#[should_panic(expected = "fits i32")]
	fn price_overflow_panics() {
		let _ = Price::new(i32::MIN, Precision(0)) - Price::new(1, Precision(0));
	}

	#[test]
	#[should_panic(expected = "fits u32")]
	fn qty_underflow_panics() {
		let _ = Qty::new(1, Precision(0)) - Qty::new(2, Precision(0));
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
