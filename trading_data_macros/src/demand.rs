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
fn spelling(ts: &TokenStream) -> syn::Result<(String, Wrap)> {
	let (cell, wrap) = ty::unwrap_dep(&ty::parse_type(&ty::flatten(ts.clone()))?);
	let key = match wrap {
		Wrap::Buf(_) => format!("Buffer<{}>", ty::norm(&cell)),
		Wrap::Sample => format!("Latest<{}>", ty::norm(&cell)),
		_ => ty::norm(&cell),
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
			let (key, wrap) = spelling(&d.ty)?;
			Ok(Edge {
				to: target(st, order, &key)?,
				gate: matches!(wrap, Wrap::Gate),
				fold: matches!(wrap, Wrap::Fold),
			})
		})
		.collect()
}

/// Per node in `State::order`, the gate nodes whose reading false makes its out unread — indices
/// into the same order, empty where the node runs unconditionally.
pub fn suppressors(st: &State) -> syn::Result<Vec<Vec<usize>>> {
	let order = &st.order;
	let n = order.len();
	let info: Vec<&NodeInfo> = order.iter().map(|k| st.known.iter().find(|x| x.key == *k).expect("an ordered node is known")).collect();
	let edges: Vec<Vec<Edge>> = info.iter().map(|x| edges(st, order, x)).collect::<syn::Result<_>>()?;

	let mut consumers: Vec<Vec<usize>> = vec![Vec::new(); n];
	// the gates that dominate a node's own run, latches excluded: a latch is momentary, so everything
	// upstream of a latch-gated node is standing demand — it must be warm before the episode arms.
	let mut hard: Vec<Vec<usize>> = vec![Vec::new(); n];
	let mut is_gate = vec![false; n];
	for c in 0..n {
		for e in &edges[c] {
			let Some(i) = e.to else { continue };
			assert!(i < c, "post-order: `{}` is stepped after its consumer `{}`", order[i], order[c]);
			consumers[i].push(c);
			if e.gate {
				is_gate[i] = true;
				if !info[i].latch {
					hard[c].push(i);
				}
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
		if let Some(i) = target(st, order, &spelling(&named.ty)?.0)? {
			outputs.insert(i);
		}
	}

	let mut live: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
	for i in (0..n).rev() {
		if pinned[i] || outputs.contains(&i) {
			continue;
		}
		assert!(!consumers[i].is_empty(), "`{}` is neither an output nor read by anything, yet the walk reached it", order[i]);
		let mut acc: Option<BTreeSet<usize>> = None;
		for c in consumers[i].iter().copied() {
			// a pinned consumer runs unconditionally, so what it reads is unconditionally demanded —
			// which is how retention and folds carry demand upstream without a second closure pass.
			let mut term = BTreeSet::new();
			if !pinned[c] {
				term.extend(live[c].iter().copied());
				term.extend(hard[c].iter().copied());
			}
			acc = Some(match acc {
				None => term,
				Some(a) => a.intersection(&term).copied().collect(),
			});
			if acc.as_ref().expect("just set").is_empty() {
				break;
			}
		}
		live[i] = acc.unwrap_or_default();
	}

	// `gating_leads` plus a post-order walk already put a dominating gate ahead of what it dominates;
	// here that stops being an argument and starts being checked.
	for i in 0..n {
		if let Some(g) = live[i].iter().find(|g| **g >= i) {
			return Err(syn::Error::new(
				Span::call_site(),
				format!(
					"`{}` is stepped before `{}`, which its demand reads — the sweep cannot ask a gate that has not resolved yet",
					order[i], order[*g]
				),
			));
		}
	}

	Ok(live.into_iter().map(|s| s.into_iter().collect()).collect())
}
