//! Diamond over two multi-rate roots: `None` propagation through the DAG, plus an
//! inference-stress graph (chain depth 10 + one arity-8 node, zero call-site annotations).

use trading_data_dag::{Cell, Cons, DepOuts, Fire, Nil, Node, Observer, step, step_obs};

struct Trades;
impl Cell for Trades {
	type Out<'t> = Option<f64>;
}
struct Quotes;
impl Cell for Quotes {
	type Out<'t> = Option<f64>;
}

#[derive(Clone)]
struct A;
impl Cell for A {
	type Out<'t> = Option<f64>;
}
impl Node for A {
	type Deps = (Trades,);

	fn advance<'t>(&mut self, (t,): DepOuts<'t, Self>) -> Self::Out<'t> {
		t.map(|x| x * 2.0)
	}
}

#[derive(Clone)]
struct B;
impl Cell for B {
	type Out<'t> = Option<f64>;
}
impl Node for B {
	type Deps = (A,);

	fn advance<'t>(&mut self, (a,): DepOuts<'t, Self>) -> Self::Out<'t> {
		a.map(|x| x + 1.0)
	}
}

#[derive(Clone)]
struct C;
impl Cell for C {
	type Out<'t> = Option<f64>;
}
impl Node for C {
	type Deps = (A,);

	fn advance<'t>(&mut self, (a,): DepOuts<'t, Self>) -> Self::Out<'t> {
		a.map(|x| x * 3.0)
	}
}

#[derive(Clone)]
struct D;
impl Cell for D {
	type Out<'t> = Option<f64>;
}
impl Node for D {
	type Deps = (B, C);

	fn advance<'t>(&mut self, (b, c): DepOuts<'t, Self>) -> Self::Out<'t> {
		b.zip(c).map(|(b, c)| b + c)
	}
}

#[derive(Clone)]
struct Cross;
impl Cell for Cross {
	type Out<'t> = Option<f64>;
}
impl Node for Cross {
	type Deps = (Trades, Quotes);

	fn advance<'t>(&mut self, (t, q): DepOuts<'t, Self>) -> Self::Out<'t> {
		t.zip(q).map(|(t, q)| t - q)
	}
}

fn tick(trades: Option<f64>, quotes: Option<f64>) -> (Option<f64>, Option<f64>) {
	let f = Cons::<Trades, Nil> { out: trades, tail: Nil };
	let f = Cons::<Quotes, _> { out: quotes, tail: f };
	let f = step(f, &mut A);
	let f = step(f, &mut B);
	let f = step(f, &mut C);
	let f = step(f, &mut D);
	let f = step(f, &mut Cross);
	(f.tail.head(), f.head())
}

#[test]
fn diamond_option_propagation() {
	assert_eq!(tick(Some(2.0), Some(1.0)), (Some(17.0), Some(1.0)));
	assert_eq!(tick(Some(3.0), None), (Some(25.0), None));
	assert_eq!(tick(None, Some(5.0)), (None, None));
	assert_eq!(tick(None, None), (None, None));
}

#[derive(Default)]
struct Rec(Vec<(&'static str, &'static [&'static str], String)>);
impl Observer for Rec {
	fn on(&mut self, node: &'static str, deps: &'static [&'static str], _: &'static [&'static str], fire: Fire<'_>) {
		self.0.push((node, deps, format!("{:?}", fire.debug)));
	}
}

// type_name output is compiler-unstable; compare on the trimmed last segment only.
fn trim(name: &str) -> &str {
	name.rsplit("::").next().expect("rsplit yields at least one segment")
}

#[test]
fn observer_sees_topo_order_deps_and_values() {
	let mut rec = Rec::default();
	let (mut a, mut b, mut c, mut d, mut cross) = (A, B, C, D, Cross);
	let mut tick = |trades: Option<f64>, quotes: Option<f64>, rec: &mut Rec| {
		let f = Cons::<Trades, Nil> { out: trades, tail: Nil };
		let f = Cons::<Quotes, _> { out: quotes, tail: f };
		let f = step_obs(f, &mut a, rec);
		let f = step_obs(f, &mut b, rec);
		let f = step_obs(f, &mut c, rec);
		let f = step_obs(f, &mut d, rec);
		let f = step_obs(f, &mut cross, rec);
		(f.tail.head(), f.head())
	};

	assert_eq!(tick(Some(2.0), Some(1.0), &mut rec), (Some(17.0), Some(1.0)));
	assert_eq!(tick(None, Some(5.0), &mut rec), (None, None));

	let seen: Vec<(&str, Vec<&str>, &str)> = rec.0.iter().map(|(n, d, o)| (trim(n), d.iter().map(|d| trim(d)).collect(), o.as_str())).collect();
	assert_eq!(
		seen,
		vec![
			("A", vec!["Trades"], "Some(4.0)"),
			("B", vec!["A"], "Some(5.0)"),
			("C", vec!["A"], "Some(12.0)"),
			("D", vec!["B", "C"], "Some(17.0)"),
			("Cross", vec!["Trades", "Quotes"], "Some(1.0)"),
			("A", vec!["Trades"], "None"),
			("B", vec!["A"], "None"),
			("C", vec!["A"], "None"),
			("D", vec!["B", "C"], "None"),
			("Cross", vec!["Trades", "Quotes"], "None"),
		]
	);
}

struct R;
impl Cell for R {
	type Out<'t> = f64;
}
macro_rules! chain {
	($name:ident, $dep:ty) => {
		struct $name;
		impl Cell for $name {
			type Out<'t> = f64;
		}
		impl Node for $name {
			type Deps = ($dep,);

			fn advance<'t>(&mut self, (x,): DepOuts<'t, Self>) -> Self::Out<'t> {
				x + 1.0
			}
		}
	};
}
chain!(S1, R);
chain!(S2, S1);
chain!(S3, S2);
chain!(S4, S3);
chain!(S5, S4);
chain!(S6, S5);
chain!(S7, S6);
chain!(S8, S7);
chain!(S9, S8);
chain!(S10, S9);

struct Wide;
impl Cell for Wide {
	type Out<'t> = f64;
}
impl Node for Wide {
	type Deps = (S3, S4, S5, S6, S7, S8, S9, S10);

	fn advance<'t>(&mut self, (a, b, c, d, e, g, h, j): DepOuts<'t, Self>) -> Self::Out<'t> {
		a + b + c + d + e + g + h + j
	}
}

#[test]
fn inference_stress_depth_10_arity_8() {
	let f = Cons::<R, Nil> { out: 0.0, tail: Nil };
	let f = step(f, &mut S1);
	let f = step(f, &mut S2);
	let f = step(f, &mut S3);
	let f = step(f, &mut S4);
	let f = step(f, &mut S5);
	let f = step(f, &mut S6);
	let f = step(f, &mut S7);
	let f = step(f, &mut S8);
	let f = step(f, &mut S9);
	let f = step(f, &mut S10);
	let f = step(f, &mut Wide);
	assert_eq!(f.head(), (3..=10).sum::<i32>() as f64);
}

#[derive(Default)]
struct JacRec(Vec<Option<Vec<f64>>>);
impl Observer for JacRec {
	fn on(&mut self, _: &'static str, _: &'static [&'static str], _: &'static [&'static str], fire: Fire<'_>) {
		self.0.push(fire.jac.map(<[f64]>::to_vec));
	}
}

#[test]
fn fd_linear_dep() {
	let mut rec = JacRec::default();
	let f = Cons::<Trades, Nil> { out: Some(2.0), tail: Nil };
	step_obs(f, &mut A, &mut rec);
	let jac = rec.0[0].as_ref().expect("A fired");
	assert!((jac[0] - 2.0).abs() < 1e-3, "{jac:?}");
}

struct Level;
impl Cell for Level {
	type Out<'t> = f64;
}
impl Clone for Level {
	fn clone(&self) -> Self {
		Level
	}
}
impl Node for Level {
	type Deps = (Trades, Quotes);

	fn advance<'t>(&mut self, (t, q): DepOuts<'t, Self>) -> Self::Out<'t> {
		// multi-rate leaf: an unfired dep contributes nothing this tick
		t.unwrap_or(0.0) + 3.0 * q.unwrap_or(0.0)
	}
}

#[test]
fn fd_unfired_dep_nan_column() {
	let mut rec = JacRec::default();
	let f = Cons::<Trades, Nil> { out: None, tail: Nil };
	let f = Cons::<Quotes, _> { out: Some(5.0), tail: f };
	step_obs(f, &mut Level, &mut rec);
	let jac = rec.0[0].as_ref().expect("Level always fires");
	assert!(jac[0].is_nan(), "{jac:?}");
	assert!((jac[1] - 3.0).abs() < 1e-3, "{jac:?}");
}

struct Gate;
impl Cell for Gate {
	type Out<'t> = bool;
}
#[derive(Clone)]
struct OnOff;
impl Cell for OnOff {
	type Out<'t> = f64;
}
impl Node for OnOff {
	type Deps = (Gate,);

	fn advance<'t>(&mut self, (g,): DepOuts<'t, Self>) -> Self::Out<'t> {
		if g { 5.0 } else { 0.0 }
	}
}

#[test]
fn fd_bool_dep_zero_column() {
	let mut rec = JacRec::default();
	let f = Cons::<Gate, Nil> { out: true, tail: Nil };
	step_obs(f, &mut OnOff, &mut rec);
	assert_eq!(rec.0[0].as_deref(), Some(&[0.0][..]));
}

#[derive(Clone)]
struct Under;
impl Cell for Under {
	type Out<'t> = Option<f64>;
}
impl Node for Under {
	type Deps = (Trades,);

	fn advance<'t>(&mut self, (t,): DepOuts<'t, Self>) -> Self::Out<'t> {
		t.filter(|x| *x <= 1.0)
	}
}

#[test]
fn fd_bump_unfired_nan_column() {
	let mut rec = JacRec::default();
	let f = Cons::<Trades, Nil> { out: Some(1.0), tail: Nil };
	step_obs(f, &mut Under, &mut rec);
	let jac = rec.0[0].as_ref().expect("fires at the boundary");
	assert!(jac[0].is_nan(), "{jac:?}");
}
