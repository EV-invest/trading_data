//@error-in-other-file: nothing compares a reading that may not be there
//@error-in-other-file: nothing compares a reading that may not be there
//! A comparison has no answer over a reading that may not be there, and `Expr::MAYBE` is what says
//! which trees can reach one — so the refusal lands where the comparison is *written* rather than on
//! the tick some warmup finally produces a NaN (`r[outs.absence.typed]`).
use trading_data_dag::{Expr, Vars, absent, constant, gt, lt};

fn compared() -> impl Expr {
	lt(absent(), constant(1.0))
}

fn compared_the_other_way() -> impl Expr {
	let v = Vars;
	gt(v.get::<0>() + absent(), constant(1.0))
}

fn main() {
	let _ = (compared(), compared_the_other_way());
}
