//! Which gates a node's *whole* demand sits behind — the reading of `Deps` that says a node's runs
//! are read by nobody while some gate is false, and so need not be taken. See `docs/ARCHITECTURE.md`
//! § Demand.

use std::collections::BTreeSet;

use proc_macro2::{Span, TokenStream};

use crate::{
	diag::{Diag, Result},
	state::{NodeInfo, State},
	ty::{self, Wrap},
};

/// Where a dep spelling lands. Two readings of one answer: the demand rule walks the sweep alone,
/// while a rendered graph has to place a root edge too.
#[derive(Clone, Copy)]
enum At {
	Root(usize),
	Node(usize),
}

/// One edge, as the demand pass reads it: what it points at, and which of the three dep kinds it
/// carries that the rule turns on.
struct Edge {
	at: At,
	gate: bool,
	fold: bool,
}

impl Edge {
	/// `None` where the dep names a root — the only reading the demand rule takes.
	fn to(&self) -> Option<usize> {
		match self.at {
			At::Node(i) => Some(i),
			At::Root(_) => None,
		}
	}
}

fn key_of(ts: &TokenStream) -> Result<String> {
	Ok(ty::norm(&ty::parse_type(&ty::flatten(ts.clone()))?))
}

/// The cell a dep spelling names, wrapper stripped and aliases resolved — the reason `visit`
/// canonicalizes too: an alias is a second spelling of one series rather than a second series.
pub fn cell(st: &State, ts: &TokenStream) -> Result<(String, Wrap)> {
	let (cell, wrap) = ty::unwrap_dep(&ty::parse_type(&ty::flatten(ts.clone()))?)?;
	let named = ty::norm(&cell);
	Ok((st.aliases.iter().find(|(a, _)| *a == named).map_or(named, |(_, answered)| answered.clone()), wrap))
}

/// The key a dep or output spelling was recorded under — `visit`'s reading of it, wrapper and all.
fn spelling(st: &State, ts: &TokenStream) -> Result<(String, Wrap)> {
	let (cell, wrap) = cell(st, ts)?;
	let key = match wrap {
		Wrap::Buf { .. } => format!("Buffer<{cell}>"),
		Wrap::Sample => format!("Latest<{cell}>"),
		_ => cell,
	};
	Ok((key, wrap))
}

/// The node a dep spelling reads — the same three readings `resolve::visit` walks, in that order,
/// plus the alias table, since a spelling need not be a key.
fn target(st: &State, order: &[String], key: &str) -> Result<At> {
	if let Some(i) = order.iter().position(|k| k == key) {
		return Ok(At::Node(i));
	}
	for (r, root) in st.cfg.roots.iter().enumerate() {
		if key_of(&root.ty)? == key {
			return Ok(At::Root(r));
		}
	}
	let Some((_, answered)) = st.aliases.iter().find(|(a, _)| a == key) else {
		return Err(Diag::new(
			Span::call_site(),
			format!("`{key}` is no root, node, alias or buffer of this graph — the walk resolved it, so a demand pass that cannot is a driver bug"),
		));
	};
	match order.iter().position(|k| k == answered) {
		Some(i) => Ok(At::Node(i)),
		None => Err(Diag::new(Span::call_site(), format!("`{key}` aliases `{answered}`, which the walk never stepped"))),
	}
}

fn edges(st: &State, order: &[String], n: &NodeInfo) -> Result<Vec<Edge>> {
	n.deps
		.iter()
		.map(|d| {
			let (key, wrap) = spelling(st, &d.ty)?;
			Ok(Edge {
				at: target(st, order, &key)?,
				gate: matches!(wrap, Wrap::Gate),
				fold: matches!(wrap, Wrap::Fold),
			})
		})
		.collect()
}

/// A node's demand, as the graph can state it: `⋁ over consumers c of (demand(c) ∧ ⋀ gates(c))`, in
/// disjunctive normal form over gate node indices. One empty conjunct is `true` — the answer for an
/// output and for anything pinned.
///
/// A set could only have meant AND, and two consumers behind *different* gates intersect to nothing,
/// which reads as "always demanded" — sound, but the degenerate answer.
pub type Dnf = Vec<Vec<usize>>;

/// Why the rule below never suppresses a node. Read in the order the variants are written, so a node
/// answering to more than one reason names the first — which is also the order the rule's `||` chain
/// evaluates them in.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Pin {
	None,
	Fold,
	Retention,
	Latch,
	Gate,
	Output,
}

impl Pin {
	/// Whether this reason makes the node run unconditionally *upstream* — an output is demanded, but
	/// only on the ticks its own gates let it run, so it is not one of these.
	fn hard(self) -> bool {
		!matches!(self, Pin::None | Pin::Output)
	}
}

/// What one pass over the graph knows about demand. The sweep reads [`live`](Demand::live) and
/// [`rewinders`](Demand::rewinders); the rendered shape reads the rest, which the rule computed
/// either way and used to throw away.
pub struct Demand {
	pub live: Vec<Dnf>,
	pub rewinders: Vec<Vec<usize>>,
	pub is_gate: Vec<bool>,
	pub pinned: Vec<Pin>,
	/// Per node, per dep: where it points, indexed over roots-then-sweep — the layout a rendered
	/// graph places edges in, and the one thing about an edge only the driver can say.
	pub deps: Vec<Vec<usize>>,
	/// Per `outputs` entry, the node it names — `None` where it names a root, which is stepped by
	/// nobody and so has no sweep index.
	pub outputs: Vec<Option<usize>>,
}

fn truth() -> Vec<BTreeSet<usize>> {
	vec![BTreeSet::new()]
}

/// `a ∨ b`, kept from growing by absorption — `A ∨ (A∧B) = A`, which is also what collapses the whole
/// formula the moment one disjunct is the empty conjunct.
fn or(mut a: Vec<BTreeSet<usize>>, b: Vec<BTreeSet<usize>>) -> Vec<BTreeSet<usize>> {
	a.extend(b);
	a.sort();
	a.dedup();
	a.iter().filter(|c| !a.iter().any(|o| o.len() < c.len() && o.is_subset(c))).cloned().collect()
}

/// Per node in `State::order`: the condition under which its out is read by anybody, and which
/// anchored nodes' pasts have to rewind before it may go dark at all.
///
/// The second list is what makes sleeping conditional on the *driver*: a node may only be suppressed
/// where something will fetch back what it skipped, and whether anything will is the feed's answer,
/// not the graph's. An anchored node names itself; a retention read only by anchored nodes names all
/// of them.
pub fn suppressors(st: &State) -> Result<Demand> {
	let order = &st.order;
	let n = order.len();
	let info: Vec<&NodeInfo> = order.iter().map(|k| st.known.iter().find(|x| x.key == *k).expect("an ordered node is known")).collect();
	let edges: Vec<Vec<Edge>> = info.iter().map(|x| edges(st, order, x)).collect::<Result<_>>()?;

	let mut consumers: Vec<Vec<usize>> = vec![Vec::new(); n];
	// the gates that dominate a node's own run. A latch is among them, but only ever suppresses a node
	// that declares `Cell::REWARMS` — that carve-out is the sweep's to emit, since a `const` is not
	// something this pass can read.
	let mut hard: Vec<Vec<usize>> = vec![Vec::new(); n];
	let mut is_gate = vec![false; n];
	for c in 0..n {
		for e in &edges[c] {
			let Some(i) = e.to() else { continue };
			assert!(i < c, "post-order: `{}` is stepped after its consumer `{}`", order[i], order[c]);
			consumers[i].push(c);
			if e.gate {
				is_gate[i] = true;
				hard[c].push(i);
			}
		}
	}

	// A retention read *only* by anchored nodes may go dark with them. It is the one relaxation of the
	// hole-free rule, and it is the whole win: a book that sleeps still costs every delta its buffer
	// folds for it, and here nothing folds them at all. The hole is not a hole — the rows are on disk,
	// and the rewind is what fetches them back.
	let held: Vec<bool> = (0..n)
		.map(|i| st.bufs.iter().any(|b| b.key == order[i]) && !consumers[i].is_empty() && consumers[i].iter().all(|&c| info[c].anchored))
		.collect();

	// never suppressed: node-held state cannot re-warm through a skip (the same reason `Pull::open`
	// forbids `Gating` + `Folding`), frame retention must be hole-free unless the above says
	// otherwise, a latch is momentary, and a gate is what *decides* demand rather than something
	// conditioned on it.
	let mut pinned: Vec<Pin> = (0..n)
		.map(|i| match () {
			_ if edges[i].iter().any(|e| e.fold) => Pin::Fold,
			_ if st.bufs.iter().any(|b| b.key == order[i]) && !held[i] => Pin::Retention,
			_ if info[i].latch => Pin::Latch,
			_ if is_gate[i] => Pin::Gate,
			_ => Pin::None,
		})
		.collect();

	// whose past has to be a real one before each node may sleep
	let rewinders: Vec<Vec<usize>> = (0..n)
		.map(|i| match (info[i].anchored, held[i]) {
			(true, _) => vec![i],
			(false, true) => consumers[i].clone(),
			(false, false) => Vec::new(),
		})
		.collect();

	let mut outputs = BTreeSet::new();
	let mut named_at: Vec<Option<usize>> = Vec::new();
	for named in &st.cfg.named {
		named_at.push(match target(st, order, &spelling(st, &named.ty)?.0)? {
			At::Node(i) => {
				outputs.insert(i);
				Some(i)
			}
			At::Root(_) => None,
		});
	}

	let mut live: Vec<Vec<BTreeSet<usize>>> = vec![Vec::new(); n];
	for i in (0..n).rev() {
		if pinned[i].hard() || outputs.contains(&i) {
			live[i] = truth();
			continue;
		}
		assert!(!consumers[i].is_empty(), "`{}` is neither an output nor read by anything, yet the walk reached it", order[i]);
		let mut acc: Vec<BTreeSet<usize>> = Vec::new();
		for c in consumers[i].iter().copied() {
			// a pinned consumer runs unconditionally, so what it reads is unconditionally demanded —
			// which is how retention and folds carry demand upstream without a second closure pass.
			let term = match pinned[c].hard() {
				true => truth(),
				false => live[c].iter().map(|conj| conj.iter().chain(&hard[c]).copied().collect()).collect(),
			};
			acc = or(acc, term);
			if acc[0].is_empty() {
				break;
			}
		}
		// a gate stepped after the node it would suppress has not resolved when the sweep asks, so that
		// disjunct degrades to standing demand rather than failing the build — which is exactly the
		// answer an intersection gave. A latch is exempt: its bit is read before the sweep starts.
		if acc.iter().flatten().any(|g| *g >= i && !info[*g].latch) {
			acc = truth();
		}
		live[i] = acc;
	}

	// an output the rule left unpinned still runs for somebody, and that is the one thing a reader of
	// the shape cannot recover from the formula: its demand is `⊤` for a reason nothing upstream says.
	for i in outputs {
		if pinned[i] == Pin::None {
			pinned[i] = Pin::Output;
		}
	}

	let roots = st.cfg.roots.len();
	let deps = edges
		.iter()
		.map(|es| {
			es.iter()
				.map(|e| match e.at {
					At::Root(r) => r,
					At::Node(i) => roots + i,
				})
				.collect()
		})
		.collect();

	Ok(Demand {
		live: live.into_iter().map(|d| d.into_iter().map(|c| c.into_iter().collect()).collect()).collect(),
		rewinders,
		is_gate,
		pinned,
		deps,
		outputs: named_at,
	})
}
