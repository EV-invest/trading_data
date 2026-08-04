//! Fixed-point money. A raw integer is meaningless without the [`Precision`] it was scaled by, so
//! the two travel together everywhere except the columnar buffers, where the run's precision is
//! hoisted out of the loop.

use serde::{Deserialize, Serialize};

/// Decimal exponent: `raw = value × 10^precision`. Signed, so a million-dollar index tick and a
/// satoshi tick both fit the same 32-bit raw column.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, derive_more::Display, derive_more::From, derive_more::FromStr, derive_more::Into)]
#[serde(transparent)]
pub struct Precision(pub i8);

/// The only exponents an i64 raw can hold, and the only ones [`realigned`] admits. Tabulated
/// because `checked_pow` is a multiply-and-check loop run once per parsed number.
static POW10: [i64; 19] = [
	1,
	10,
	100,
	1_000,
	10_000,
	100_000,
	1_000_000,
	10_000_000,
	100_000_000,
	1_000_000_000,
	10_000_000_000,
	100_000_000_000,
	1_000_000_000_000,
	10_000_000_000_000,
	100_000_000_000_000,
	1_000_000_000_000_000,
	10_000_000_000_000_000,
	100_000_000_000_000_000,
	1_000_000_000_000_000_000,
];

/// [`POW10`] mirrored around zero, indexed by `exponent + 18`. Bit-identical to `10f64.powi` across
/// the range (asserted below), so this is a table lookup and nothing else.
static POW10F: [f64; 37] = [
	1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12,
	1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
];

fn pow10(exp: u32) -> i64 {
	*POW10.get(exp as usize).expect("precision gap fits i64")
}

impl Precision {
	/// Raw ticks per unit. Hoist it once per run and divide inside the loop — the raw column is
	/// what the venue sent and what the disk holds, so this is the only decode there is.
	pub fn scale(self) -> f64 {
		*POW10F.get((self.0 as isize + 18) as usize).expect("precision within the i64 raw's decimal range")
	}

	pub fn parse_i32(self, s: &str) -> i32 {
		i32::try_from(realigned(s, self)).expect("realigned digits fit i32")
	}

	pub fn parse_u32(self, s: &str) -> u32 {
		u32::try_from(realigned(s, self)).expect("realigned digits fit u32")
	}
}

/// Re-align a decimal string onto `precision`'s tick. Trailing zeros are insignificant (Binance
/// pads `.24` to `.24000000`); a significant digit the tick cannot hold is a feed/config mismatch
/// and panics. Runs once per level of every book message, so it walks the bytes in place rather
/// than materializing the realigned digits.
fn realigned(s: &str, precision: Precision) -> i64 {
	let (sign, body) = match s.strip_prefix('-') {
		Some(rest) => (-1, rest),
		None => (1, s.strip_prefix('+').unwrap_or(s)),
	};
	let (int, frac) = body.split_once('.').unwrap_or((body, ""));
	let frac = frac.trim_end_matches('0');
	assert!((1..=18).contains(&(int.len() + frac.len())), "{s:?} is not a decimal number i64 holds");

	let mut acc: i64 = 0;
	for &b in int.as_bytes().iter().chain(frac.as_bytes()) {
		let d = b.wrapping_sub(b'0');
		assert!(d < 10, "{s:?} is not a decimal number");
		acc = acc * 10 + d as i64;
	}

	let gap = precision.0 as i32 - frac.len() as i32;
	let pow = pow10(gap.unsigned_abs());
	sign * match gap >= 0 {
		true => acc.checked_mul(pow).expect("realigned raw fits i64"),
		false => {
			assert_eq!(acc % pow, 0, "{s:?} carries digits below the 10^{} tick", precision.0);
			acc / pow
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
	let pow = pow10(gap.unsigned_abs());
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
		#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, derive_more::Display)]
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

	/// The tables replaced `checked_pow`/`powi` on the promise of being the same number, and an f64
	/// off by an ULP would move every decoded price without failing anything else.
	#[test]
	fn the_tables_are_what_they_replaced() {
		for e in 0..POW10.len() as u32 {
			assert_eq!(pow10(e), 10i64.checked_pow(e).unwrap());
		}
		for e in -18i8..=18 {
			assert_eq!(Precision(e).scale().to_bits(), 10f64.powi(e as i32).to_bits(), "10^{e}");
		}
	}

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
