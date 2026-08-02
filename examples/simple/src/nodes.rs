//! One root and one RSI chain, plus the three flow readings and the one differentiable node that
//! between them exercise the machinery an indicator alone does not: per-trade rate, buffered
//! windows, and the symbolic/finite-difference agreement. Cheap on purpose — a question about the
//! engine is answered here rather than by a 32-day spl sweep.

use core::fmt;

use trading_data::{
	Buffering, Bump, Cell, Emit, EmitOuts, Exact, Expr, Flat, Folding, Glance, Horizon, Lanes, RsiSpec, Side, Stamped, Symbolic, Timeframe, TradeCols, Trades, Vars, constant, node,
	slice_nudge,
};

/// This graph's clock.
pub const M1: Timeframe = Timeframe::from_str_const("1m");
// the graph reaches every cell through a shim keyed on its name, and a bare `use` leaves no shim
// behind — so the two series this example imports are aliased rather than imported.
trading_data::node_alias! { pub Bar1m = trading_data::Bars<M1>; }
trading_data::node_alias! { pub Rsi14 = trading_data::Rsi<Bar1m, Len14>; }

/// λ's window, in minutes.
pub const LAMBDA_WINDOW: usize = 60;
/// The reach both windowed readings are served from: λ's `window + 1`, which the hour of volume
/// rides along inside.
const REACH: Horizon = Horizon::Elems(LAMBDA_WINDOW + 1);
/// Wilder's own lengths, and the whole of what this graph configures.
pub struct Len14;
impl RsiSpec for Len14 {
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
pub struct Cvd {
	sum: f64,
}
/// A minute of signed order flow, and the close it left the price at. Its own series rather than a
/// field of [`trading_data::Bar`]: signed flow is not something a shared bar carries, and λ is its
/// only reader.
#[derive(Clone, Copy, Debug)]
pub struct Flow {
	pub ts_close: i64,
	pub close: f64,
	/// Σ signed `price*qty` (Buy = +, Sell = −) over the minute.
	pub quote: f64,
}
#[derive(Clone, Default)]
pub struct Flow1m(Option<Flow>);
/// Kyle's λ: through-origin OLS of per-minute Δclose on signed flow, `λ = Σ(Δp·f) / Σ(f²)`, over the
/// [`LAMBDA_WINDOW`] deltas a `LAMBDA_WINDOW + 1` minute window spans.
#[derive(Clone, Default)]
pub struct Lambda1m;
/// Rolling 60-bar quote volume. `None` until the hour is whole — a partial sum compared against a
/// threshold is a lie, not a warmup.
#[derive(Clone, Default)]
pub struct VolUsd1h;
/// A pure blend of the current levels — the one genuinely differentiable node here (every other
/// kernel is stateful or batch). Its value *is* an [`Expr`] of the scalar (`.last()`) views of its
/// deps, so it differentiates and documents itself exactly; `main` asserts the exact Jacobian
/// against the retained finite-difference one, tick for tick.
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
impl Emit for Cvd {
	type Deps = (Trades,);

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<f64>) {
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		for i in 0..trades.len() {
			self.sum += signed(trades.side[i], (trades.price[i] as f64 / ps) * (trades.qty[i] as f64 / qs));
			out.push(self.sum);
		}
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

impl Cell for Flow1m {
	type Out<'t> = &'t [Flow];
}
#[node]
impl Emit for Flow1m {
	/// The partial minute is the whole of the state, so the trades it holds reach back exactly one.
	type Deps = (Folding<Trades, { Horizon::Span(M1) }>,);

	fn emit(&mut self, (trades,): EmitOuts<'_, Self>, out: &mut Vec<Flow>) {
		let (ps, qs) = (trades.prec.price.scale(), trades.prec.qty.scale());
		let step = Exact::from_nanos(M1.duration().as_nanos() as i64);
		for (i, exec) in trades.exec().iter().enumerate() {
			let (price, qty) = (trades.price[i] as f64 / ps, trades.qty[i] as f64 / qs);
			let ts_close = exec.floor(step).as_nanos() + step.as_nanos();
			let flow = signed(trades.side[i], price * qty);
			match &mut self.0 {
				Some(f) if f.ts_close == ts_close => {
					f.close = price;
					f.quote += flow;
				}
				acc => {
					if let Some(done) = acc.take() {
						out.push(done);
					}
					*acc = Some(Flow {
						ts_close,
						close: price,
						quote: flow,
					});
				}
			}
		}
	}
}
slice_nudge!(Flow1m, Flow);

impl Cell for Lambda1m {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for Lambda1m {
	type Deps = (Buffering<Flow1m, REACH>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.narrowed(Horizon::Elems(LAMBDA_WINDOW + 1)).trailing().map(|w| w.map(kyle_lambda)));
	}
}
fn kyle_lambda(flows: &[Flow]) -> f64 {
	let denom: f64 = flows[1..].iter().map(|f| f.quote * f.quote).sum();
	if denom == 0.0 {
		return 0.0;
	}
	flows.windows(2).map(|w| (w[1].close - w[0].close) * w[1].quote).sum::<f64>() / denom
}
slice_nudge!(Lambda1m, Option<f64>);

impl Cell for VolUsd1h {
	type Out<'t> = &'t [Option<f64>];
}
#[node]
impl Emit for VolUsd1h {
	type Deps = (Buffering<Bar1m, REACH>,);

	fn emit(&mut self, (hist,): EmitOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
		out.extend(hist.narrowed(Horizon::Elems(60)).trailing().map(|w| w.map(|w| w.iter().map(|b| b.vol_base * b.close).sum())));
	}
}
slice_nudge!(VolUsd1h, Option<f64>);

impl Cell for Signal {
	type Out<'t> = f64;
}
#[node]
impl Symbolic for Signal {
	type Deps = (Lambda1m, VolUsd1h, Cvd);

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
	outputs { rsi: Rsi14, bar: Bar1m, cvd: Cvd, lambda: Lambda1m, vol_usd_1h: VolUsd1h, signal: Signal }
}

/// The whole of the routing an app needs: every lane is present, and the graph names the ones it
/// takes. No discriminant to re-dispatch, no `Default` fill.
impl<'t> From<Lanes<'t>> for Batches<'t> {
	fn from(l: Lanes<'t>) -> Self {
		Self { trades: l.trades }
	}
}
