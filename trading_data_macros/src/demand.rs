//! Which gates a node's *whole* demand sits behind — the reading of `Deps` that says a node's runs
//! are read by nobody while some gate is false, and so need not be taken. See `docs/ARCHITECTURE.md`
//! § Demand.

use std::collections::BTreeSet;

use proc_macro2::{Span, TokenStream};

use crate::{
	state::{NodeInfo, State},
	ty::{self, Wrap},
};

/// One edge, as the demand pass reads it: what it points at, and which of the three dep kinds it
/// carries that the rule turns on.
struct Edge {
	/// `None` where the dep names a root.
	to: Option<usize>,
	gate: bool,
	fold: bool,
}

fn key_of(ts: &TokenStream) -> syn::Result<String> {
	Ok(ty::norm(&ty::parse_type(&ty::flatten(ts.clone()))?))
}

/// The key a dep or output spelling was recorded under — `visit`'s reading of it, wrapper and all.
/// The cell is canonicalized first, for the reason `visit` canonicalizes it: a retention is keyed on
/// the series it holds, and an alias is a second spelling of one series rather than a second series.
fn spelling(st: &State, ts: &TokenStream) -> syn::Result<(String, Wrap)> {
	let (cell, wrap) = ty::unwrap_dep(&ty::parse_type(&ty::flatten(ts.clone()))?)?;
	let named = ty::norm(&cell);
	let cell = st.aliases.iter().find(|(a, _)| *a == named).map_or(named, |(_, answered)| answered.clone());
	let key = match wrap {
		Wrap::Buf { .. } => format!("Buffer<{cell}>"),
		Wrap::Sample => format!("Latest<{cell}>"),
		_ => cell,
	};
	Ok((key, wrap))
}

/// The node a dep spelling reads — the same three readings `resolve::visit` walks, in that order,
/// plus the alias table, since a spelling need not be a key.
fn target(st: &State, order: &[String], key: &str) -> syn::Result<Option<usize>> {
	if let Some(i) = order.iter().position(|k| k == key) {
		return Ok(Some(i));
	}
	for r in &st.cfg.roots {
		if key_of(&r.ty)? == key {
			return Ok(None);
		}
	}
	let Some((_, answered)) = st.aliases.iter().find(|(a, _)| a == key) else {
		return Err(syn::Error::new(
			Span::call_site(),
			format!("`{key}` is no root, node, alias or buffer of this graph — the walk resolved it, so a demand pass that cannot is a driver bug"),
		));
	};
	match order.iter().position(|k| k == answered) {
		Some(i) => Ok(Some(i)),
		None => Err(syn::Error::new(Span::call_site(), format!("`{key}` aliases `{answered}`, which the walk never stepped"))),
	}
}

fn edges(st: &State, order: &[String], n: &NodeInfo) -> syn::Result<Vec<Edge>> {
	n.deps
		.iter()
		.map(|d| {
			let (key, wrap) = spelling(st, &d.ty)?;
			Ok(Edge {
				to: target(st, order, &key)?,
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

/// Per node in `State::order`, the condition under which its out is read by anybody.
pub fn suppressors(st: &State) -> syn::Result<Vec<Dnf>> {
	let order = &st.order;
	let n = order.len();
	let info: Vec<&NodeInfo> = order.iter().map(|k| st.known.iter().find(|x| x.key == *k).expect("an ordered node is known")).collect();
	let edges: Vec<Vec<Edge>> = info.iter().map(|x| edges(st, order, x)).collect::<syn::Result<_>>()?;

	let mut consumers: Vec<Vec<usize>> = vec![Vec::new(); n];
	// the gates that dominate a node's own run. A latch is among them, but only ever suppresses a node
	// that declares `Cell::REWARMS` — that carve-out is the sweep's to emit, since a `const` is not
	// something this pass can read.
	let mut hard: Vec<Vec<usize>> = vec![Vec::new(); n];
	let mut is_gate = vec![false; n];
	for c in 0..n {
		for e in &edges[c] {
			let Some(i) = e.to else { continue };
			assert!(i < c, "post-order: `{}` is stepped after its consumer `{}`", order[i], order[c]);
			consumers[i].push(c);
			if e.gate {
				is_gate[i] = true;
				hard[c].push(i);
			}
		}
	}

	// never suppressed: node-held state cannot re-warm through a skip (the same reason `Pull::open`
	// forbids `Gating` + `Folding`), frame retention must be hole-free, a latch is momentary, and a
	// gate is what *decides* demand rather than something conditioned on it.
	let pinned: Vec<bool> = (0..n)
		.map(|i| edges[i].iter().any(|e| e.fold) || st.bufs.iter().any(|b| b.key == order[i]) || info[i].latch || is_gate[i])
		.collect();

	let mut outputs = BTreeSet::new();
	for named in &st.cfg.named {
		if let Some(i) = target(st, order, &spelling(st, &named.ty)?.0)? {
			outputs.insert(i);
		}
	}

	let mut live: Vec<Vec<BTreeSet<usize>>> = vec![Vec::new(); n];
	for i in (0..n).rev() {
		if pinned[i] || outputs.contains(&i) {
			live[i] = truth();
			continue;
		}
		assert!(!consumers[i].is_empty(), "`{}` is neither an output nor read by anything, yet the walk reached it", order[i]);
		let mut acc: Vec<BTreeSet<usize>> = Vec::new();
		for c in consumers[i].iter().copied() {
			// a pinned consumer runs unconditionally, so what it reads is unconditionally demanded —
			// which is how retention and folds carry demand upstream without a second closure pass.
			let term = match pinned[c] {
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

	Ok(live.into_iter().map(|d| d.into_iter().map(|c| c.into_iter().collect()).collect()).collect())
}
