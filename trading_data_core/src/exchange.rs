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
