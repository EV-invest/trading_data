//! The GAT headline: a heavy root enters the frame as `&'t Book`, and a node hands
//! root-borrowed data (`Option<&'t _>`) downstream.

use trading_data_dag::{Cell, Cons, DepOuts, Nil, Node, step};

struct Book {
	bids: Vec<(i64, u64)>,
	asks: Vec<(i64, u64)>,
}

struct BookC;
impl Cell for BookC {
	type Out<'t> = &'t Book;
}

struct BestBid;
impl Cell for BestBid {
	type Out<'t> = Option<&'t (i64, u64)>;
}
impl Node for BestBid {
	type Deps = (BookC,);

	fn advance<'t>(&'t mut self, (book,): DepOuts<'t, Self>) -> Self::Out<'t> {
		book.bids.first()
	}
}

struct Mid;
impl Cell for Mid {
	type Out<'t> = Option<f64>;
}
impl Node for Mid {
	type Deps = (BookC, BestBid);

	fn advance<'t>(&'t mut self, (book, best_bid): DepOuts<'t, Self>) -> Self::Out<'t> {
		let bid = best_bid?.0;
		let ask = book.asks.first()?.0;
		Some((bid + ask) as f64 / 2.0)
	}
}

// self-borrowing `advance` caps the lent ref by the node borrow, so it can't escape the frame —
// assert in place while the (stateless) nodes are alive.
fn check(book: &Book, expect: (Option<&(i64, u64)>, Option<f64>)) {
	let (mut best_bid, mut mid) = (BestBid, Mid);
	let f = Cons::<BookC, Nil> { out: book, tail: Nil };
	let f = step(f, &mut best_bid);
	let f = step(f, &mut mid);
	assert_eq!((f.tail.head(), f.head()), expect);
}

#[test]
fn borrowed_root_flows_through_frame() {
	let mut book = Book {
		bids: vec![(100, 5)],
		asks: vec![(102, 7)],
	};
	check(&book, (Some(&(100, 5)), Some(101.0)));

	book.bids.clear();
	check(&book, (None, None));

	book.bids.push((99, 3));
	book.asks[0] = (103, 1);
	check(&book, (Some(&(99, 3)), Some(101.0)));
}
