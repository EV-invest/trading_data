//@error-in-other-file: a `Select` condition that may not be there decides nothing
//! A `Select` is where a body *writes* a declination, so its branches stay permissive — but a
//! condition that may not be there decides nothing, and `present(x)` is the test that reads one.
use trading_data_dag::{Expr, absent, constant, select};

fn undecidable() -> impl Expr {
	select(absent(), constant(1.0), constant(2.0))
}

/// The sanctioned shape, kept beside it: the *branch* is where an absence belongs.
fn declines() -> impl Expr {
	select(constant(1.0), absent(), constant(2.0))
}

fn main() {
	let _ = (undecidable(), declines());
}
