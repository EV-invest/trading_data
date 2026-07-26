use derive_more::{Deref, DerefMut, Display};
use eyre::Report;
use serde::{Deserializer, Serialize, Serializer};

//HACK: should implement `pad`, but rust is broken (or skill issue (upd: definitely broken)). Whatever the case, doing `f.pad(s)` on the same output breaks things downstream (no clue why).
#[derive(Clone, Copy, derive_more::Debug, Default, Deref, DerefMut, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[debug("{}", self.as_str())]
#[display("{}", self.as_str())]
pub struct Asset(pub [u8; 16]);
impl Asset {
	pub fn new<S: AsRef<str>>(s: S) -> Self {
		let s = s.as_ref().to_uppercase();
		let mut bytes = [0; 16];
		bytes[..s.len()].copy_from_slice(s.as_bytes());
		Self(bytes)
	}

	pub fn as_str(&self) -> &str {
		std::str::from_utf8(&self.0).unwrap().trim_end_matches('\0')
	}

	/// Construct the standard USD-quoted [Pair] for this asset.
	/// Linear contracts quote against USDT; inverse contracts quote against USD.
	pub fn usd_pair(self, is_inverse: bool) -> Pair {
		match is_inverse {
			false => Pair::new(self, "USDT".into()),
			true => Pair::new(self, "USD".into()),
		}
	}
}
impl From<&str> for Asset {
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}
impl From<String> for Asset {
	fn from(s: String) -> Self {
		Self::new(s)
	}
}
impl AsRef<str> for Asset {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}
impl PartialEq<str> for Asset {
	fn eq(&self, other: &str) -> bool {
		self.as_str() == other
	}
}
impl PartialEq<&str> for Asset {
	fn eq(&self, other: &&str) -> bool {
		&self.as_str() == other
	}
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pair {
	base: Asset,
	quote: Asset,
}
impl Pair {
	pub fn new<S: Into<Asset>>(base: S, quote: S) -> Self {
		Self {
			base: base.into(),
			quote: quote.into(),
		}
	}

	pub fn is_usdt(&self) -> bool {
		self.quote == "USDT" && self.base != "BTCST" /*Binance thing*/
	}

	pub fn base(&self) -> &Asset {
		&self.base
	}

	pub fn quote(&self) -> &Asset {
		&self.quote
	}

	// Exchange-specific {{{
	pub fn fmt_binance(&self) -> String {
		format!("{}{}", self.base, self.quote)
	}

	pub fn fmt_bybit(&self) -> String {
		format!("{}{}", self.base, self.quote)
	}

	pub fn fmt_mexc(&self) -> String {
		format!("{}_{}", self.base, self.quote)
	}
	//,}}}
}
impl<A: Into<Asset>> From<(A, A)> for Pair {
	fn from((base, quote): (A, A)) -> Self {
		Self::new(base, quote)
	}
}
//HACK: should implement `pad`, but rust is broken (or skill issue). Whatever the case, doing `f.pad(s)` on the same output breaks things downstream (no clue why).
impl std::fmt::Display for Pair {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(f, "{}-{}", self.base, self.quote)
	}
}

impl<'de> serde::Deserialize<'de> for Pair {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>, {
		let s = String::deserialize(deserializer)?;
		s.parse().map_err(serde::de::Error::custom)
	}
}
impl Serialize for Pair {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer, {
		self.to_string().serialize(serializer)
	}
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid pair format '{provided_str}'. Expected two assets separated by one of: [{}]", allowed_delimiters.join(" "))]
pub struct InvalidPairError {
	provided_str: String,
	allowed_delimiters: Vec<String>,
}
impl InvalidPairError {
	pub fn new<S: Into<String>>(provided_str: &str, allowed_delimiters: impl IntoIterator<Item = S>) -> Self {
		Self {
			provided_str: provided_str.to_owned(),
			allowed_delimiters: allowed_delimiters.into_iter().map(Into::into).collect(),
		}
	}
}

/// function to prevent human error in the order of the prefixes, because I know sooner or later I'll mess it up. Will return false if say "WETH" is found _after_ "ETH"
///HACK: couldn't figure out how to do this at compile time
#[doc(hidden)]
fn check_prefix_order<const N: usize>(arr: [&str; N]) -> eyre::Result<()> {
	for i in 0..N {
		for j in (i + 1)..N {
			if arr[i].len() < arr[j].len() && arr[j].ends_with(arr[i]) {
				eyre::bail!("{} is a suffix of {}", arr[i], arr[j]);
			}
		}
	}
	Ok(())
}

impl std::str::FromStr for Pair {
	type Err = Report;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let delimiters = [',', '-', '_', '/'];
		let currencies = [
			"EURI", "EUR", "FDUSD", "BFUSD", "RWUSD", "USD1", "BNFCR", "USD", "GBP", "USDP", "USDS", "PLN", "RON", "CZK", "TRY", "JPY", "BRL", "RUB", "AUD", "NGN", "MXN", "COP", "ARS",
			"BKRW", "IDRT", "UAH", "BIDR", "BVND", "ZAR",
		];
		let crypto = ["USDT", "USDC", "UST", "BTC", "WETH", "ETH", "BNB", "SOL", "XRP", "PAX", "DAI", "VAI", "DOGE", "DOT", "TRX"];
		if let Err(e) = check_prefix_order(currencies) {
			unreachable!("Invalid prefix order, I messed up bad: {e}");
		}
		if let Err(e) = check_prefix_order(crypto) {
			unreachable!("Invalid prefix order, I messed up bad: {e}");
		}
		let recognized_quotes = [currencies.as_slice(), crypto.as_slice()].concat();

		for delimiter in delimiters {
			if s.contains(delimiter) {
				let parts: Vec<_> = s.split(delimiter).map(str::trim).filter(|s| !s.is_empty()).collect();
				if parts.len() == 2 {
					return Ok(Self::new(parts[0], parts[1]));
				}
				return Err(InvalidPairError::new(s, delimiters.iter().map(|c| c.to_string())).into());
			}
		}

		if let Some(quote) = recognized_quotes.iter().find(|q| s.ends_with(*q)) {
			let base_len = s.len() - quote.len();
			if base_len > 0 {
				let base = &s[..base_len];
				return Ok(Self::new(base, *quote));
			}
		}

		Err(InvalidPairError::new(s, delimiters.iter().map(|c| c.to_string())).into())
	}
}
impl TryFrom<&str> for Pair {
	type Error = Report;

	fn try_from(s: &str) -> Result<Self, Self::Error> {
		s.parse()
	}
}
impl TryFrom<String> for Pair {
	type Error = Report;

	fn try_from(s: String) -> Result<Self, Self::Error> {
		s.parse()
	}
}
impl From<Pair> for String {
	fn from(pair: Pair) -> Self {
		pair.to_string()
	}
}

impl PartialEq<Pair> for &str {
	fn eq(&self, other: &Pair) -> bool {
		Pair::try_from(*self).expect("provided string can't be converted to `Pair` automatically") == *other // important not to cast `to_string` on `Pair` instance here as it will break (nightmare to debug) if I change `Display` impl
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_pairs() {
		assert_eq!("BTC-USD".parse::<Pair>().unwrap(), Pair::new("BTC", "USD"));
		assert_eq!("ETH,USD".parse::<Pair>().unwrap(), Pair::new("ETH", "USD"));
		assert_eq!("SOL_USDT".parse::<Pair>().unwrap(), Pair::new("SOL", "USDT"));
		assert_eq!("XRP/USDC".parse::<Pair>().unwrap(), Pair::new("XRP", "USDC"));
		assert_eq!("btc - usd".parse::<Pair>().unwrap(), Pair::new("BTC", "USD"));
		assert_eq!("DOGEUSDT".parse::<Pair>().unwrap(), Pair::new("DOGE", "USDT"));
		assert_eq!(Pair::from(("ADA", "USDT")), Pair::new("ADA", "USDT"));

		assert!("something".parse::<Pair>().is_err());
		assert!("".parse::<Pair>().is_err());
		assert!("BTC".parse::<Pair>().is_err());
		assert!("BTC-".parse::<Pair>().is_err());
		assert!("-USD".parse::<Pair>().is_err());
	}

	#[test]
	fn display_pairs() {
		assert_eq!(Pair::new("BTC", "USDT").to_string(), "BTC-USDT");
	}
}
