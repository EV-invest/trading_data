//! Plain embeddable indicator state machines. Deliberately not `Node` impls — callers embed
//! them inside their own nodes, so this crate never learns about the derivation engine.
//!
//! Wilder semantics throughout: warmup = SMA over the first `period` samples, then
//! `(prev*(n-1)+x)/n`; `None` until warm.

#[derive(Clone)]
struct Wilder {
	period: usize,
	value: f64,
	warmed: usize,
}

impl Wilder {
	fn new(period: usize) -> Self {
		assert!(period > 0);
		Self { period, value: 0.0, warmed: 0 }
	}

	fn update(&mut self, x: f64) -> Option<f64> {
		let n = self.period as f64;
		if self.warmed < self.period {
			self.value += x;
			self.warmed += 1;
			if self.warmed < self.period {
				return None;
			}
			self.value /= n;
		} else {
			self.value = (self.value * (n - 1.0) + x) / n;
		}
		Some(self.value)
	}
}

/// The two averages RSI is a ratio of; [`rsi`] is that ratio.
#[derive(Clone)]
pub struct WilderAvgGainLoss {
	prev_close: Option<f64>,
	gain: Wilder,
	loss: Wilder,
}

impl WilderAvgGainLoss {
	pub fn new(period: usize) -> Self {
		Self {
			prev_close: None,
			gain: Wilder::new(period),
			loss: Wilder::new(period),
		}
	}

	pub fn update(&mut self, close: f64) -> Option<(f64, f64)> {
		let prev = self.prev_close.replace(close)?;
		let delta = close - prev;
		let (gain, loss) = if delta >= 0.0 { (delta, 0.0) } else { (0.0, -delta) };
		self.gain.update(gain).zip(self.loss.update(loss))
	}
}

pub fn rsi(avg_gain: f64, avg_loss: f64) -> f64 {
	if avg_loss == 0.0 { 100.0 } else { 100.0 - 100.0 / (1.0 + avg_gain / avg_loss) }
}

#[derive(Clone)]
pub struct WilderAtr {
	prev_close: Option<f64>,
	atr: Wilder,
}

impl WilderAtr {
	pub fn new(period: usize) -> Self {
		Self {
			prev_close: None,
			atr: Wilder::new(period),
		}
	}

	pub fn update(&mut self, high: f64, low: f64, close: f64) -> Option<f64> {
		let tr = match self.prev_close.replace(close) {
			Some(pc) => (high - low).max((high - pc).abs()).max((low - pc).abs()),
			None => high - low,
		};
		self.atr.update(tr)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// StockCharts' classic Wilder RSI-14 worked example; goldens recomputed independently and
	// they match the published 70.53 / 66.32 / … sequence.
	#[test]
	fn rsi14_golden_sequence() {
		let closes = [
			44.3389, 44.0902, 44.1497, 43.6124, 44.3278, 44.8264, 45.0955, 45.4245, 45.8433, 46.0826, 45.8931, 46.0328, 45.6140, 46.2820, 46.2820, 46.0028, 46.0328, 46.4116, 46.2222,
			45.6439, 46.2122, 46.2521, 45.7137, 46.4515, 45.7835, 45.3548, 44.0288, 44.1783, 44.2181, 44.5672, 43.4205, 42.6628, 43.1314,
		];
		let golden = [
			70.5328, 66.3186, 66.5498, 69.4063, 66.3552, 57.9749, 62.9296, 63.2571, 56.0593, 62.3771, 54.7076, 50.4228, 39.9898, 41.4605, 41.8689, 45.4632, 37.3040, 33.0795, 37.7730,
		];
		let mut avgs = WilderAvgGainLoss::new(14);
		let outs: Vec<_> = closes.iter().map(|&c| avgs.update(c).map(|(g, l)| rsi(g, l))).collect();
		assert!(outs[..14].iter().all(Option::is_none), "warm only after period deltas");
		for (got, want) in outs[14..].iter().zip(golden) {
			let got = got.expect("warm from the 15th close on");
			assert!((got - want).abs() < 5e-4, "{got} vs {want}");
			assert!((0.0..=100.0).contains(&got));
		}
	}

	#[test]
	fn atr_warmup_and_smoothing() {
		let mut atr = WilderAtr::new(3);
		// bars: (high, low, close); TRs: 2, then max(3, |4-2|, |1-2|)=3, then 4 → SMA warm = 3
		assert_eq!(atr.update(3.0, 1.0, 2.0), None);
		assert_eq!(atr.update(4.0, 1.0, 3.0), None);
		assert_eq!(atr.update(6.0, 2.0, 5.0), Some(3.0));
		// next TR = max(1, |6-5|, |5-5|) = 1 → (3*2 + 1)/3
		assert_eq!(atr.update(6.0, 5.0, 5.5), Some(7.0 / 3.0));
	}
}
