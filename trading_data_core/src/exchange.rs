use std::str::FromStr;

use crate::Pair;

#[derive(Clone, Copy, Debug, strum::Display, strum::EnumString, Eq, Hash, PartialEq)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum ExchangeName {
	Binance,
	Bybit,
	Kucoin,
	Mexc,
	BitFlyer,
	Coincheck,
	Yahoo,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, strum::Display, strum::EnumString, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize)]
#[non_exhaustive]
pub enum Instrument {
	#[default]
	#[strum(serialize = "")]
	Spot,
	#[strum(serialize = ".P")]
	Perp,
	#[strum(serialize = ".M")]
	Margin, //Q: do we care for being able to parse spot/margin diff from ticker defs?
	#[strum(serialize = ".PERP_INVERSE")]
	PerpInverse,
	#[strum(serialize = ".OPTIONS")]
	Options,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Eq, Hash, PartialEq, serde::Serialize, derive_new::new)]
pub struct Symbol {
	pub pair: Pair,
	pub instrument: Instrument,
}

impl std::fmt::Display for Symbol {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}{}", self.pair, self.instrument)
	}
}

impl FromStr for Symbol {
	type Err = eyre::Report;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (pair_str, instrument_ticker_str) = s.split_once('.').map(|(p, i)| (p, format!(".{}", i.to_uppercase()))).unwrap_or((s, "".to_owned()));
		let pair = Pair::from_str(pair_str)?;
		let instrument = Instrument::from_str(&instrument_ticker_str)?;

		Ok(Symbol { pair, instrument })
	}
}
impl From<&str> for Symbol {
	fn from(s: &str) -> Self {
		Self::from_str(s).unwrap()
	}
}

/// Per-batch precision shared across all levels / trades in a book / trade batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrecisionPriceQty {
	pub price: u8,
	pub qty: u8,
}

impl PrecisionPriceQty {
	/// Strip the decimal point from a string and right-pad to `expected_precision` decimals.
	/// Trailing zeros beyond `expected_precision` are ignored (Binance pads `.24` to `.24000000`);
	/// any non-zero digit beyond `expected_precision` is a bug and panics.
	fn digits(s: &str, expected_precision: u8) -> String {
		match s.find('.') {
			Some(dot) => {
				let int_part = &s[..dot];
				let frac_part = &s[dot + 1..];
				let frac_significant = frac_part.trim_end_matches('0');
				let significant_decimals = frac_significant.len() as u8;
				assert!(
					significant_decimals <= expected_precision,
					"string {s:?} has {significant_decimals} significant decimal places, expected at most {expected_precision}"
				);
				let pad = expected_precision as usize - frac_significant.len();
				let mut out = String::with_capacity(int_part.len() + expected_precision as usize);
				out.push_str(int_part);
				out.push_str(frac_significant);
				for _ in 0..pad {
					out.push('0');
				}
				out
			}
			None => {
				let mut out = String::with_capacity(s.len() + expected_precision as usize);
				out.push_str(s);
				for _ in 0..expected_precision {
					out.push('0');
				}
				out
			}
		}
	}

	pub fn parse_price(&self, s: &str) -> i32 {
		Self::digits(s, self.price).parse().expect("price digits are valid i32")
	}

	pub fn parse_qty(&self, s: &str) -> u32 {
		Self::digits(s, self.qty).parse().expect("qty digits are valid u32")
	}
}
