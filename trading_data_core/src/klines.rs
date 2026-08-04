use std::time::Duration;

use eyre::Result;
use jiff::Timestamp;
use v_utils::Timeframe;

#[derive(Clone, Copy, Debug, Default, PartialEq, derive_new::new)]
pub struct Ohlc {
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
}

pub fn p_to_ohlc(p: &[(f64, Timestamp)], timeframe: &Timeframe) -> Result<Vec<Ohlc>> {
	if p.is_empty() {
		return Ok(Vec::new());
	}

	let duration = timeframe.duration();
	let mut ohlc_data = Vec::new();
	let mut current_ohlc = Ohlc::new(p[0].0, p[0].0, p[0].0, p[0].0);
	let mut current_start = p[0].1;

	for &(price, timestamp) in p.iter() {
		if timestamp >= current_start + duration {
			ohlc_data.push(current_ohlc);
			current_start = timestamp - Duration::from_nanos((timestamp.as_nanosecond() % duration.as_nanos() as i128).try_into().unwrap());
			current_ohlc = Ohlc::new(price, price, price, price);
		} else {
			current_ohlc.high = current_ohlc.high.max(price);
			current_ohlc.low = current_ohlc.low.min(price);
			current_ohlc.close = price;
		}
	}

	if !ohlc_data.is_empty() && current_ohlc.open != ohlc_data.last().unwrap().open {
		ohlc_data.push(current_ohlc);
	}

	Ok(ohlc_data)
}

/// take a price-series, and imagine that entries are constantly spaced
pub fn mock_p_to_ohlc(p: &[f64], step: usize) -> Vec<Ohlc> {
	let mut ohlc_data = Vec::new();

	for chunk in p.chunks(step) {
		if chunk.is_empty() {
			continue;
		}

		let ohlc = Ohlc {
			open: chunk[0],
			high: *chunk.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
			low: *chunk.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
			close: *chunk.last().unwrap(),
		};

		ohlc_data.push(ohlc);
	}

	ohlc_data
}

/// Standard candlestick data unit. Can only ever be full, - if an exchange returns partial data for an ongoing candle, or if trading/exchange is down leading to the associated data being cut, the [Kline] object is NOT created.
#[derive(Clone, Copy, Debug, Default, PartialEq, derive_more::Deref, derive_more::DerefMut, derive_new::new)]
pub struct Kline {
	pub open_time: Timestamp,
	#[deref_mut]
	#[deref]
	pub ohlc: Ohlc,
	/// later on I'm likely to graduate to having everything normalized to USDT, or, even better, to actual inflation-adjusted USD dollars, but for now mark this as explicitly `quote`-denominated
	pub volume_quote: f64,
	pub trades: Option<usize>,
	pub taker_buy_volume_quote: Option<f64>,
}

/// Unlike `Kline`, timestamp signifies the end of the period, not the start. As another difference - `Vec<Close>` can have uneven spacing between measured points.
#[derive(Clone, Copy, Debug, Default, PartialEq, derive_more::Deref, derive_more::DerefMut, serde::Deserialize, serde::Serialize, derive_new::new)]
pub struct Close {
	#[deref_mut]
	#[deref]
	pub close: f64,
	pub timestamp: Timestamp,
}
