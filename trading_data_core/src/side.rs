use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize)]
pub enum Side {
	#[default] // not to be used almost ever, only here to allow for propagation of the trait
	Buy,
	Sell,
}
impl Side {
	pub fn to_str(&self) -> &'static str {
		match self {
			Side::Buy => "BUY",
			Side::Sell => "SELL",
		}
	}
}

impl FromStr for Side {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.to_uppercase().as_str() {
			"BUY" => Ok(Side::Buy),
			"SELL" => Ok(Side::Sell),
			_ => Err(format!("Invalid side: {s}")),
		}
	}
}
impl std::fmt::Display for Side {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.to_str().fmt(f)
	}
}

impl<'de> Deserialize<'de> for Side {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>, {
		struct SideVisitor;

		impl serde::de::Visitor<'_> for SideVisitor {
			type Value = Side;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("`BUY` or `SELL`")
			}

			fn visit_str<E>(self, value: &str) -> Result<Side, E>
			where
				E: serde::de::Error, {
				Side::from_str(value).map_err(serde::de::Error::custom)
			}
		}

		deserializer.deserialize_str(SideVisitor)
	}
}

impl std::ops::Not for Side {
	type Output = Side;

	fn not(self) -> Self::Output {
		match self {
			Side::Buy => Side::Sell,
			Side::Sell => Side::Buy,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_side_from_str() {
		assert_eq!(Side::from_str("BUY").unwrap(), Side::Buy);
		assert_eq!(Side::from_str("Sell").unwrap(), Side::Sell);
		assert!(Side::from_str("foo").is_err());
	}

	#[test]
	fn test_side_to_str() {
		assert_eq!(Side::Buy.to_str(), "BUY");
		assert_eq!(Side::Sell.to_str(), "SELL");
	}
}
