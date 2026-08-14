//! One root and one RSI chain, plus the three flow readings and the one differentiable node that
//! between them exercise the machinery an indicator alone does not: per-trade rate, buffered
//! windows, and the symbolic/finite-difference agreement. Cheap on purpose — a question about the
//! engine is answered here rather than by a 32-day spl sweep.

use core::fmt;

use trading_data::{
	Buffering, Bump, Carried, Cell, Closes, DepOuts, DepReads, Elems, Env, Expr, Flat, Folding, Folds, Glance, Horizon, Lanes, Over, Pending, Reading, RsiSpec, Runs, Sampling, Side, Slots,
	Stamped, Symbolic, Tag, Timeframe, TradeCols, Trades, Unflat, Vars, Witness, always_present, constant, node, slice_nudge,
};
use v_utils::*;

const _: () = {
	let (partial, opaque) = trading_data::Fidelity::hatches(Graph::FIDELITY);
	assert!(
		partial <= 7,
		"a kernel here covers less of what its body read than the graph pins for: print `Graph::FIDELITY`, then raise this number in a diff that says which node lost its reach and why"
	);
	assert!(
		opaque <= 2,
		"a kernel here computes with no algebra at all where the graph pins fewer: print `Graph::FIDELITY`, then raise this number in a diff carrying the node's `WHY`"
	);
};
/// Wilder's own lengths, and the whole of what this graph configures.
pub struct Len14;
impl RsiSpec for Len14 {
	const NAME: &'static str = "Len14";

	fn base_len() -> usize {
		14
	}

	fn smooth_len() -> usize {
		14
	}
}

/// Cumulative volume delta: running Σ signed notional, one element per trade — the one reading here
/// clocked by the tape rather than by a bar.
#[derive(Clone, Default)]
pub struct Cvd(Carried);
/// One period of signed order flow, and the close it left the price at. Its own series rather than a
/// field of [`trading_data::Bar`]: signed flow is not something a shared bar carries, and λ is its
/// only reader.
#[derive(Clone, Copy, Debug)]
pub struct Flow {
	pub ts_close: i64,
	pub close: f64,
	/// Σ signed `price*qty` (Buy = +, Sell = −) over the period.
	pub quote: f64,
}
/// [`Flow`] per closed `TF`, as [`trading_data::Bars`] is [`trading_data::Bar`] per closed period.
#[derive(Clone, Default)]
pub struct Flows<const TF: Timeframe>(Pending);
impl<const TF: Timeframe> Flows<TF> {
	const TAG: Tag = Tag::new("Flow:", TF);
}

/// Kyle's λ: through-origin OLS of per-period Δclose on signed flow, `λ = Σ(Δp·f) / Σ(f²)`, over the
/// `WIN - 1` deltas a `WIN`-element window spans.
#[derive(Clone, Default)]
pub struct Lambda<const TF: Timeframe, const WIN: usize>;
impl<const TF: Timeframe, const WIN: usize> Lambda<TF, WIN> {
	const TAG: Tag = Tag::new("Lambda:", TF).count(WIN);
}

/// Rolling `WIN`-bar quote volume. `None` until the window is whole — a partial sum compared against
/// a threshold is a lie, not a warmup.
#[derive(Clone, Default)]
pub struct RollingVolUsd<const TF: Timeframe, const WIN: usize>;
impl<const TF: Timeframe, const WIN: usize> RollingVolUsd<TF, WIN> {
	const TAG: Tag = Tag::new("RollingVolUsd:", TF).count(WIN);
}

/// A pure blend of the current levels — the one genuinely differentiable node here (every other
/// kernel is stateful or batch). Its value *is* an [`Expr`] of its deps' standing levels, so it
/// differentiates and documents itself exactly; `main` asserts the exact Jacobian against the
/// retained finite-difference one, tick for tick.
#[derive(Clone, Copy, Default)]
pub struct Signal;
fn signed(side: Side, notional: f64) -> f64 {
	match side {
		Side::Buy => notional,
		Side::Sell => -notional,
	}
}

impl Cell for Cvd {
	type Out<'t> = &'t [f64];
}
#[node]
impl Folds for Cvd {
	type Deps = (Trades,);

	const STATE: usize = 1;

	fn read<W: Witness>((trades,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let t = trades.at(i)?;
		env.put(t);
		env.attr(signed(t.side, 1.0));
		Some(t.exec.as_nanos())
	}

	/// The side is an attribute — `Flat` leaves it out because a side has no slope — so it sits past
	/// the state the recurrence carries.
	fn step(&self, v: Vars) -> impl Slots {
		let (price, qty, sum, side) = (v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>());
		sum + side * (price * qty)
	}

	fn value(&self, v: Vars) -> impl Slots {
		v.get::<2>()
	}

	fn carried(&self) -> &Carried {
		&self.0
	}

	fn carried_mut(&mut self) -> &mut Carried {
		&mut self.0
	}
}
slice_nudge!(Cvd, f64);

impl Flat for Flow {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		out.copy_from_slice(&[self.close, self.quote]);
		true
	}
}
impl Bump for Flow {
	fn bump(mut self, slot: usize, h: f64) -> (Self, f64) {
		*[&mut self.close, &mut self.quote][slot] += h;
		(self, h)
	}
}
impl Unflat for Flow {
	fn unflat(ts_ns: i64, slots: &[f64]) -> Self {
		Self {
			ts_close: ts_ns,
			close: slots[0],
			quote: slots[1],
		}
	}
}
impl Glance for Flow {
	fn glance(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "flow {:.0}", self.quote)
	}
}
impl Stamped for Flow {
	fn ts_ns(&self) -> i64 {
		self.ts_close
	}
}
always_present!(Flow);
impl<const TF: Timeframe> Cell for Flows<TF> {
	type Out<'t> = &'t [Flow];

	const CLOCK: Option<Timeframe> = Some(TF);
	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe> Closes for Flows<TF> {
	/// The partial period is the whole of the state, so the trades it holds reach back exactly one.
	type Deps = (Folding<Trades, Over<TF>>,);

	const PERIOD: Timeframe = TF;

	fn read<W: Witness>((trades,): DepReads<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
		let t = trades.at(i)?;
		env.put(t);
		env.attr(signed(t.side, 1.0));
		Some(t.exec.as_nanos())
	}

	/// The side is an attribute — `Flat` leaves it out because a side has no slope, exactly as [`Cvd`]
	/// reads it — so it sits past the accumulator.
	fn open(&self, v: Vars) -> impl Slots {
		let (price, qty, side) = (v.get::<0>(), v.get::<1>(), v.get::<4>());
		(price, side * (price * qty))
	}

	fn fold(&self, v: Vars) -> impl Slots {
		let (price, qty, quote, side) = (v.get::<0>(), v.get::<1>(), v.get::<3>(), v.get::<4>());
		(price, quote + side * (price * qty))
	}

	fn pending(&self) -> &Pending {
		&self.0
	}

	fn pending_mut(&mut self) -> &mut Pending {
		&mut self.0
	}
}
slice_nudge!([const TF: Timeframe] Flows<TF>, Flow);
impl<const TF: Timeframe, const WIN: usize> Cell for Lambda<TF, WIN> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe, const WIN: usize> Runs for Lambda<TF, WIN> {
	type Deps = (Buffering<Flows<TF>, Elems<WIN>>,);

	const WHY: &'static str = "a through-origin fit over a whole retained window, and no kernel indexes a window: `Scan` reads a point, `Close` a period, `Fold` all history through \
	                          state. A window body would also want one `Var` per retained element, against a `MAX_VARS` of 16";

	fn emit(&mut self, (hist,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.narrowed(Horizon::Elems(WIN)).trailing().map(|w| w.map(kyle_lambda)));
	}
}
fn kyle_lambda(flows: &[Flow]) -> f64 {
	let denom: f64 = flows[1..].iter().map(|f| f.quote * f.quote).sum();
	if denom == 0.0 {
		return 0.0;
	}
	flows.windows(2).map(|w| (w[1].close - w[0].close) * w[1].quote).sum::<f64>() / denom
}
slice_nudge!([const TF: Timeframe, const WIN: usize] Lambda<TF, WIN>, Option<f64>);
impl<const TF: Timeframe, const WIN: usize> Cell for RollingVolUsd<TF, WIN> {
	type Out<'t> = &'t [Option<f64>];

	const NAME: &'static str = Self::TAG.as_str();
}
#[node]
impl<const TF: Timeframe, const WIN: usize> Runs for RollingVolUsd<TF, WIN> {
	type Deps = (Buffering<trading_data::Bars<TF>, Elems<WIN>>,);

	const WHY: &'static str = "a sum over a whole retained window, and no kernel indexes a window: `Scan` reads a point, `Close` a period, `Fold` all history through state. A window \
	                          body would also want one `Var` per retained element, against a `MAX_VARS` of 16";

	fn emit(&mut self, (hist,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.narrowed(Horizon::Elems(WIN)).trailing().map(|w| w.map(|w| w.iter().map(|b| b.vol_base * b.close).sum())));
	}
}
slice_nudge!([const TF: Timeframe, const WIN: usize] RollingVolUsd<TF, WIN>, Option<f64>);

impl Cell for Signal {
	/// Its three deps warm on three different schedules, and the blend is a number only once all of
	/// them stand — which is what the kernel publishes through, and what a bare `f64` had no channel
	/// for.
	type Out<'t> = Reading;
}
#[node]
impl Symbolic for Signal {
	/// [`Sampling`] on all three: a level node reads levels, and the last element of a run is a
	/// reading of how the feed grouped its messages rather than of the market
	/// (`r[rates.deps.tick-opaque]`). The carry is monotone, so what this blends is the standing value
	/// of each series however many ticks ago it published.
	type Deps = (Sampling<Lambda<{ TF_1MIN }, 61>>, Sampling<RollingVolUsd<{ TF_1MIN }, 60>>, Sampling<Cvd>);

	fn body(&self, v: Vars) -> impl Expr {
		let (lambda, vol, cvd) = (v.get::<0>(), v.get::<1>(), v.get::<2>());
		constant(1e6) * lambda + constant(1e-6) * (cvd - vol)
	}
}

trading_data::graph! {
	pub struct Graph;
	batches Batches;
	roots { trades: Trades[TradeCols] };
	out TickOut;
	// `main.rs` reads every one of these: the day-end levels, and `Signal` through the exact/FD witness.
	outputs {
		rsi: trading_data::Rsi<trading_data::Bars<{ TF_1MIN }>, Len14>,
		bar: trading_data::Bars<{ TF_1MIN }>,
		cvd: Cvd,
		lambda: Lambda<{ TF_1MIN }, 61>,
		vol_usd: RollingVolUsd<{ TF_1MIN }, 60>,
		signal: Signal
	}
}

// r[impl kernels.opaque.stated]
// r[impl kernels.fidelity.stated]
// `<=`, not `==`: a count that falls is the direction of travel, and only a rise owes a diff.

/// The whole of the routing an app needs: every lane is present, and the graph names the ones it
/// takes. No discriminant to re-dispatch, no `Default` fill.
impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self { trades: l.trades }
	}
}
