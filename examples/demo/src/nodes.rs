//! SPL replica graph: Prints → Bar1m → (Rsi14, Atr14, Momentum, VolUsd1h) → Screener → Classify.
//! All outs are value types; the hand-wired chain in [`Graph::tick_obs`] is the topo order.

use trading_data::{Cell, Cons, DepOuts, Nil, Node, Observer, WilderAtr, WilderRsi, step_obs};

pub const MOM_WINDOW: usize = 60;
// Tuned to TAO-USDT 2025-01-03: the goal is the mechanism firing, not signal quality.
const MOM_TH: f64 = 1.0;
const RSI_HI: f64 = 65.0;
const RSI_LO: f64 = 35.0;
const VOL_TH: f64 = 100_000.0;
const STREAK_N: u32 = 2;
const MOM_HIGH_BAND: f64 = 3.0;
const MOM_MID_BAND: f64 = 2.0;

#[derive(Clone, Copy, Debug)]
pub struct Print {
	pub ts: i64,
	pub price: f64,
	pub qty: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Bar {
	pub ts_open: i64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub vol_quote: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
	None,
	Liquidations,
	MmClosing,
	Manipulation,
}

#[derive(Clone, Copy, Debug)]
pub struct Classified {
	pub category: Category,
	pub dist: [(Category, f64); 4],
}

pub struct Prints;
impl Cell for Prints {
	type Out<'t> = Option<Print>;
}

#[derive(Default)]
pub struct Bar1m {
	acc: Option<Bar>,
}
impl Cell for Bar1m {
	type Out<'t> = Option<Bar>;
}
impl Node for Bar1m {
	type Deps = (Prints,);

	fn advance<'t>(&mut self, (p,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let p = p?;
		let ts_open = p.ts - p.ts.rem_euclid(60_000_000_000);
		match &mut self.acc {
			Some(b) if b.ts_open == ts_open => {
				b.high = b.high.max(p.price);
				b.low = b.low.min(p.price);
				b.close = p.price;
				b.vol_quote += p.price * p.qty;
				None
			}
			acc => {
				let done = *acc;
				*acc = Some(Bar {
					ts_open,
					open: p.price,
					high: p.price,
					low: p.price,
					close: p.price,
					vol_quote: p.price * p.qty,
				});
				done
			}
		}
	}
}

pub struct Rsi14(pub WilderRsi);
impl Cell for Rsi14 {
	type Out<'t> = Option<f64>;
}
impl Node for Rsi14 {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		self.0.update(bar?.close)
	}
}

pub struct Atr14(pub WilderAtr);
impl Cell for Atr14 {
	type Out<'t> = Option<f64>;
}
impl Node for Atr14 {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let b = bar?;
		self.0.update(b.high, b.low, b.close)
	}
}

pub struct Momentum {
	prev_close: Option<f64>,
	returns: [f64; MOM_WINDOW],
	idx: usize,
	filled: usize,
}
impl Default for Momentum {
	fn default() -> Self {
		Self {
			prev_close: None,
			returns: [0.0; MOM_WINDOW],
			idx: 0,
			filled: 0,
		}
	}
}
impl Cell for Momentum {
	type Out<'t> = Option<f64>;
}
impl Node for Momentum {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		let close = bar?.close;
		let prev = self.prev_close.replace(close)?;
		self.returns[self.idx] = close / prev - 1.0;
		self.idx = (self.idx + 1) % MOM_WINDOW;
		self.filled = (self.filled + 1).min(MOM_WINDOW);
		if self.filled < MOM_WINDOW {
			return None;
		}
		let n = MOM_WINDOW as f64;
		let mean = self.returns.iter().sum::<f64>() / n;
		let var = self.returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
		if var == 0.0 {
			return Some(0.0);
		}
		Some(mean / var.sqrt() * n.sqrt())
	}
}

pub struct VolUsd1h {
	ring: [f64; 60],
	idx: usize,
}
impl Default for VolUsd1h {
	fn default() -> Self {
		Self { ring: [0.0; 60], idx: 0 }
	}
}
impl Cell for VolUsd1h {
	type Out<'t> = f64;
}
impl Node for VolUsd1h {
	type Deps = (Bar1m,);

	fn advance<'t>(&mut self, (bar,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if let Some(b) = bar {
			self.ring[self.idx] = b.vol_quote;
			self.idx = (self.idx + 1) % self.ring.len();
		}
		self.ring.iter().sum()
	}
}

/// Stateful hysteresis streak — deliberately non-vectorizable, like the SPL RsiScreener.
#[derive(Default)]
pub struct Screener {
	streak: u32,
}
impl Cell for Screener {
	type Out<'t> = Option<bool>;
}
impl Node for Screener {
	type Deps = (Momentum, Rsi14, VolUsd1h);

	fn advance<'t>(&mut self, (mom, rsi, vol): DepOuts<'t, Self>) -> Self::Out<'t> {
		let (mom, rsi) = mom.zip(rsi)?;
		let hit = mom.abs() > MOM_TH && !(RSI_LO..=RSI_HI).contains(&rsi) && vol > VOL_TH;
		self.streak = if hit { self.streak + 1 } else { 0 };
		Some(self.streak >= STREAK_N)
	}
}

pub struct Classify;
impl Cell for Classify {
	type Out<'t> = Option<Classified>;
}
impl Node for Classify {
	type Deps = (Screener, Momentum);

	fn advance<'t>(&mut self, (hit, mom): DepOuts<'t, Self>) -> Self::Out<'t> {
		if !hit? {
			return None;
		}
		let mom = mom.expect("Screener only emits once Momentum is warm");
		let category = if mom.abs() > MOM_HIGH_BAND {
			Category::Manipulation
		} else if mom.abs() > MOM_MID_BAND {
			Category::Liquidations
		} else {
			Category::MmClosing
		};
		let rest = (1.0 - 0.6) / 3.0;
		let dist = [Category::None, Category::Liquidations, Category::MmClosing, Category::Manipulation].map(|c| (c, if c == category { 0.6 } else { rest }));
		Some(Classified { category, dist })
	}
}

#[derive(Clone, Copy, Debug)]
pub struct TickOut {
	pub bar: Option<Bar>,
	pub rsi: Option<f64>,
	pub atr: Option<f64>,
	pub momentum: Option<f64>,
	pub vol_usd_1h: f64,
	pub screener: Option<bool>,
	pub classified: Option<Classified>,
}

pub struct Graph {
	bar: Bar1m,
	rsi: Rsi14,
	atr: Atr14,
	momentum: Momentum,
	vol: VolUsd1h,
	screener: Screener,
	classify: Classify,
}

impl Default for Graph {
	fn default() -> Self {
		Self {
			bar: Bar1m::default(),
			rsi: Rsi14(WilderRsi::new(14)),
			atr: Atr14(WilderAtr::new(14)),
			momentum: Momentum::default(),
			vol: VolUsd1h::default(),
			screener: Screener::default(),
			classify: Classify,
		}
	}
}

impl Graph {
	pub fn tick(&mut self, print: Option<Print>) -> TickOut {
		self.tick_obs(print, &mut ())
	}

	pub fn tick_obs<O: Observer>(&mut self, print: Option<Print>, obs: &mut O) -> TickOut {
		obs.on(core::any::type_name::<Prints>(), &[], &print);
		let f = Cons::<Prints, Nil> { out: print, tail: Nil };
		let f = step_obs(f, &mut self.bar, obs);
		let f = step_obs(f, &mut self.rsi, obs);
		let f = step_obs(f, &mut self.atr, obs);
		let f = step_obs(f, &mut self.momentum, obs);
		let f = step_obs(f, &mut self.vol, obs);
		let f = step_obs(f, &mut self.screener, obs);
		let f = step_obs(f, &mut self.classify, obs);
		TickOut {
			classified: f.head(),
			screener: f.tail.head(),
			vol_usd_1h: f.tail.tail.head(),
			momentum: f.tail.tail.tail.head(),
			atr: f.tail.tail.tail.tail.head(),
			rsi: f.tail.tail.tail.tail.tail.head(),
			bar: f.tail.tail.tail.tail.tail.tail.head(),
		}
	}
}
