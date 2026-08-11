//! `Graph::SHAPE` — the walk's own answers written out as a value, so what the driver knows about a
//! graph survives past the `tick` it is spent writing.
//!
//! Nothing per-dep is emitted here: every such fact is a one-token projection of `DepSet`, which is
//! the same constant the sweep dispatches on and so cannot drift from what compiles. What this file
//! writes is only what `DepSet` cannot say — the sweep position each dep resolves to, the demand
//! formula, and the pin that explains a node the rule never suppresses.

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::{
	demand::{Demand, Pin},
	state::{NodeInfo, State},
};

fn pin(p: Pin) -> TokenStream {
	match p {
		Pin::None => quote!(None),
		Pin::Fold => quote!(Fold),
		Pin::Retention => quote!(Retention),
		Pin::Latch => quote!(Latch),
		Pin::Gate => quote!(Gate),
		Pin::Flow => quote!(Flow),
		Pin::Output => quote!(Output),
	}
}

fn indices(xs: &[usize]) -> impl Iterator<Item = Literal> + '_ {
	xs.iter().copied().map(Literal::usize_unsuffixed)
}

/// `node_tys` is `emit`'s, not this file's: a retention's type is the join of every read taken of it,
/// and computing that a second time is exactly the drift the shape exists to rule out.
pub fn shape_const(st: &State, d: &Demand, node_tys: &[TokenStream]) -> TokenStream {
	let c = &st.cfg;
	let dag = &c.dag;
	let roots = c.roots.len();

	let root_nodes = c.roots.iter().map(|r| {
		let (ty, field) = (&r.ty, r.field.to_string());
		// a root is fed rather than computed: it reads nothing, nothing conditions it, and there is no
		// kernel to owe a fidelity for.
		quote! {
			#dag::NodeShape {
				name: #dag::node_name::<#ty>(),
				kind: #dag::Kind::Root,
				clock: <#ty as #dag::Cell>::CLOCK,
				fidelity: #dag::Fidelity::Exact,
				anchored: false,
				pin: #dag::Pin::None,
				rewinds: false,
				field: ::core::option::Option::Some(#field),
				deps: &[],
				dep_gates: &[],
				dep_folds: &[],
				dep_retains: &[],
				dep_reach: &[],
				demand: &[&[]],
			}
		}
	});

	let info: Vec<&NodeInfo> = st.order.iter().map(|k| st.known.iter().find(|n| n.key == *k).expect("an ordered node is known")).collect();
	let stepped = (0..info.len()).map(|i| {
		let (n, ty) = (info[i], &node_tys[i]);
		let deps_ty = quote!(<#ty as #dag::Wired>::Deps);
		let fidelity = match n.emit {
			true => quote!(<<#ty as #dag::Emit>::Kernel as #dag::Run<#ty>>::FIDELITY),
			false => quote!(<<#ty as #dag::Node>::Kernel as #dag::Level<#ty>>::FIDELITY),
		};

		// the `Buffer` key is the driver's own spelling, so a node is one exactly where the driver wrote
		// it as one — not where its name happens to read like one.
		let buffer = st.bufs.iter().any(|b| b.key == n.key);
		let kind = match () {
			_ if buffer => quote!(Buffer),
			_ if n.latch => quote!(Latch),
			_ if d.is_gate[i] => quote!(Gate),
			_ if n.emit => quote!(Emit),
			_ => quote!(Node),
		};

		let anchored = n.anchored;
		let rewinds = !d.rewinders[i].is_empty();
		let pin = pin(d.pinned[i]);
		let field = match c.named.iter().enumerate().find(|(k, _)| d.outputs[*k] == Some(i)) {
			Some((_, named)) => {
				let f = named.field.to_string();
				quote!(::core::option::Option::Some(#f))
			}
			None => quote!(::core::option::Option::None),
		};
		let deps = indices(&d.deps[i]);
		// the formula is over the sweep, and `nodes` puts the roots first — a gate is never a root, so
		// no shift here can collide with one.
		let demand = d.live[i].iter().map(|conj| {
			let gs = conj.iter().map(|g| Literal::usize_unsuffixed(g + roots));
			quote!(&[#(#gs),*])
		});

		quote! {
			#dag::NodeShape {
				name: #dag::node_name::<#ty>(),
				kind: #dag::Kind::#kind,
				clock: <#ty as #dag::Cell>::CLOCK,
				fidelity: #fidelity,
				anchored: #anchored,
				pin: #dag::Pin::#pin,
				rewinds: #rewinds,
				field: #field,
				deps: &[#(#deps),*],
				dep_gates: <#deps_ty as #dag::DepSet>::GATES,
				dep_folds: <#deps_ty as #dag::DepSet>::FOLDS,
				dep_retains: <#deps_ty as #dag::DepSet>::RETAINS,
				dep_reach: <#deps_ty as #dag::DepSet>::REACH,
				demand: &[#(#demand),*],
			}
		}
	});

	let graph = &c.graph;
	quote! {
		#dag::Shape {
			graph: stringify!(#graph),
			nodes: &[#(#root_nodes,)* #(#stepped,)*],
		}
	}
}
