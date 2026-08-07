//! The indicator tier: the plain state machines, and the graph nodes that are nothing but those
//! machines wired to a series. A strategy crate names these rather than re-deriving them; what it
//! still owns is the wiring — which series, which lengths, which thresholds.
//!
//! Wilder semantics throughout: warmup = SMA over the first `period` samples, then
//! `(prev*(n-1)+x)/n`; `None` until warm.
#![feature(adt_const_params)]

mod bar;
mod rsi_node;

pub use bar::{Bar, Bars, Ohlc, Ohlcs, Volume, Volumes, closed_by};
pub use rsi_node::{AvgGain, AvgLoss, Rsi, RsiDelta, RsiSpec, RsiValues};
use trading_data_dag::{Ex, Expr, constant, lt, min, select};

/// Wilder's running mean, as the two expressions a [`trading_data_dag::Folds`] body steps its state
/// by: what the mean becomes, and how many samples have gone into it. Warmup is an SMA over the
/// first `n`, which is the whole reason the count is carried — and `warmed < n` is equally how a
/// reader of the state knows it is still cold.
pub fn wilder<V: Expr, W: Expr, X: Expr>(value: Ex<V>, warmed: Ex<W>, x: Ex<X>, n: f64) -> (Ex<impl Expr>, Ex<impl Expr>) {
	let cold = value + x;
	(
		select(
			lt(warmed, constant(n)),
			select(lt(warmed, constant(n - 1.0)), cold, cold / constant(n)),
			(value * constant(n - 1.0) + x) / constant(n),
		),
		min(warmed + constant(1.0), constant(n)),
	)
}

pub fn rsi(avg_gain: f64, avg_loss: f64) -> f64 {
	if avg_loss == 0.0 { 100.0 } else { 100.0 - 100.0 / (1.0 + avg_gain / avg_loss) }
}

#[cfg(test)]
mod tests {
	use trading_data_dag::{Vars, max};

	use super::*;

	/// One `AvgGain`/`AvgLoss` state step, driven the way the node drives it: the same two expressions
	/// [`wilder`] returns, evaluated over `[delta, value, warmed]` — which is the env `Rsi`'s halves
	/// read into, `sign` and all.
	fn step(sign: f64, state: (f64, f64), delta: f64, n: f64) -> (f64, f64) {
		let v = Vars;
		let half = max(constant(sign) * v.get::<0>(), constant(0.0));
		let (value, warmed) = wilder(v.get::<1>(), v.get::<2>(), half, n);
		let env = [delta, state.0, state.1];
		(value.lower().eval(&env), warmed.lower().eval(&env))
	}

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
		let (mut gain, mut loss) = ((0.0, 0.0), (0.0, 0.0));
		let outs: Vec<Option<f64>> = closes
			.windows(2)
			.map(|w| {
				let delta = w[1] - w[0];
				gain = step(1.0, gain, delta, 14.0);
				loss = step(-1.0, loss, delta, 14.0);
				// `warmed < base_len` is how the node's own `value` reads cold, so it is the same cut here.
				(gain.1 >= 14.0).then(|| rsi(gain.0, loss.0))
			})
			.collect();

		assert!(outs[..13].iter().all(Option::is_none), "warm only after period deltas");
		for (got, want) in outs[13..].iter().zip(golden) {
			let got = got.expect("warm from the 14th delta on");
			assert!((got - want).abs() < 5e-4, "{got} vs {want}");
			assert!((0.0..=100.0).contains(&got));
		}
	}
}
