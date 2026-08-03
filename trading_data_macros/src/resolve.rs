//! `__graph_resolve!` — the driver. It walks the dep tree one node per expansion, calling back into
//! each cell's shim for edges it cannot see, and emits the graph once the closure is complete.

use proc_macro2::{Ident, Span, TokenStream};
use quote::{ToTokens, quote};
use syn::Type;

use crate::{
	state::{Awaiting, Buf, Dep, NodeInfo, Reader, State},
	ty::{self, Wrap},
};

enum Answer {
	Node {
		emit: bool,
		diff: bool,
		latch: bool,
		self_ty: TokenStream,
		deps: Vec<Dep>,
	},
	Trigger(Dep),
}

fn read_answer(r: &mut Reader) -> Option<Answer> {
	if r.peek_at("kind") {
		r.at("kind");
		let emit = r.ident() == "emit";
		r.at("diff");
		let diff = r.flag();
		r.at("latch");
		let latch = r.flag();
		r.at("self");
		let self_ty = r.brace();
		r.at("deps");
		let mut dr = r.list();
		let mut deps = Vec::new();
		while !dr.eof() {
			deps.push(Dep { shim: dr.brace(), ty: dr.brace() });
		}
		return Some(Answer::Node { emit, diff, latch, self_ty, deps });
	}
	if r.peek_at("trigger") {
		r.at("trigger");
		return Some(Answer::Trigger(Dep { shim: r.brace(), ty: r.brace() }));
	}
	None
}

fn key_of(ts: &TokenStream) -> syn::Result<(String, Type)> {
	let t: Type = ty::parse_type(&ty::flatten(ts.clone()))?;
	Ok((ty::norm(&t), t))
}

fn on_stack(st: &State, key: &str) -> bool {
	st.stack.iter().any(|(k, _)| k == key)
}

fn cycle(st: &State, key: &str) -> syn::Error {
	let mut path: Vec<&str> = st.stack.iter().map(|(k, _)| k.as_str()).collect();
	path.push(key);
	syn::Error::new(
		Span::call_site(),
		format!("dep cycle: {} — a graph is a dag, and a node cannot read what it feeds", path.join(" → ")),
	)
}

/// Records a resolved node, unless the answer names one already walked — which is what an alias
/// looks like from here: asked about under one spelling, answering under its own.
fn record(st: &mut State, info: NodeInfo) -> syn::Result<()> {
	if st.known.iter().any(|n| n.key == info.key) {
		return match on_stack(st, &info.key) {
			true => Err(cycle(st, &info.key)),
			false => Ok(()),
		};
	}
	st.stack.push((info.key.clone(), 0));
	st.known.push(info);
	Ok(())
}

fn accept(st: &mut State, answer: Option<Answer>) -> syn::Result<()> {
	let awaiting = core::mem::replace(&mut st.awaiting, Awaiting::Nothing);
	match (awaiting, answer) {
		(Awaiting::Nothing, None) => Ok(()),
		(Awaiting::Node(asked, req), Some(Answer::Node { emit, diff, latch, self_ty, deps })) => {
			let (key, _) = key_of(&self_ty)?;
			// an alias answers under the aliased cell's own key, and every consumer edge naming it by
			// the alias has to find its way back here.
			if asked != key && !st.aliases.iter().any(|(a, _)| *a == asked) {
				st.aliases.push((asked, key.clone()));
			}
			record(
				st,
				NodeInfo {
					key,
					ty: req,
					emit,
					diff,
					latch,
					deps,
				},
			)
		}
		(Awaiting::Trigger(key, req), Some(Answer::Trigger(arm))) => record(
			st,
			NodeInfo {
				key,
				ty: req,
				emit: false,
				diff: false,
				latch: true,
				deps: vec![arm],
			},
		),
		_ => panic!("__graph_resolve: a shim answered a question the driver did not ask"),
	}
}

/// The shim call that answers for `dep`, with the state to hand back.
fn ask(st: &State, shim: &TokenStream, cell: &Type) -> TokenStream {
	let args = ty::type_args(cell).into_iter().map(|a| {
		let s = match &a {
			syn::GenericArgument::Type(t) => ty::shim_path(t, "__td_node_").unwrap_or_else(|_| quote!(crate::__td_not_a_cell)),
			_ => quote!(crate::__td_not_a_cell),
		};
		quote!({ #s } { #a })
	});
	let dag = &st.cfg.dag;
	quote! { #shim ! { #dag::__graph_resolve, [ #(#args),* ], @state #st } }
}

/// Walks one edge. `Ok(Some(..))` is a question only the compiler can answer: emit it and pause.
fn visit(st: &mut State, dep: Dep) -> syn::Result<Option<TokenStream>> {
	let (_, full) = key_of(&dep.ty)?;
	let (cell, wrap) = ty::unwrap_dep(&full);
	let key = ty::norm(&cell);

	if let Wrap::Buf(reach) = wrap {
		let dag = &st.cfg.dag;
		let reach = reach.unwrap_or_else(|| quote!(#dag::Horizon::Unit));
		let bkey = format!("Buffer<{key}>");
		match st.bufs.iter_mut().find(|b| b.key == bkey) {
			Some(b) => b.reaches.push(reach),
			None => st.bufs.push(Buf {
				key: bkey.clone(),
				ty: cell.to_token_stream(),
				reaches: vec![reach],
			}),
		}
		// the buffer is a node of the frame in its own right, reading the series it retains.
		return record(
			st,
			NodeInfo {
				key: bkey,
				ty: TokenStream::new(),
				emit: false,
				diff: false,
				latch: false,
				deps: vec![Dep {
					shim: dep.shim,
					ty: cell.to_token_stream(),
				}],
			},
		)
		.map(|()| None);
	}

	if let Wrap::Sample = wrap {
		let dag = &st.cfg.dag;
		// no reach to join: unlike a buffer, every reader of a level asks for the same one thing, so
		// the frame type is settled here rather than in `emit`.
		return record(
			st,
			NodeInfo {
				key: format!("Latest<{key}>"),
				ty: quote!(#dag::Latest<#cell>),
				emit: false,
				diff: false,
				latch: false,
				deps: vec![Dep {
					shim: dep.shim,
					ty: quote!(#dag::Folding<#cell, { #dag::Horizon::Unbounded }>),
				}],
			},
		)
		.map(|()| None);
	}

	if st.cfg.roots.iter().any(|r| key_of(&r.ty).map(|(k, _)| k == key).unwrap_or(false)) {
		return Ok(None);
	}
	if st.known.iter().any(|n| n.key == key) {
		return match on_stack(st, &key) {
			true => Err(cycle(st, &key)),
			false => Ok(None),
		};
	}

	// `Armed<N>`'s dep is `N::Trigger` — an associated type, so the arm is asked for by name.
	if ty::head_is(&cell, "Armed") {
		let inner = ty::sole_arg(&cell).ok_or_else(|| syn::Error::new(Span::call_site(), "`Armed` without an episode"))?;
		let shim = ty::shim_path(&inner, "__td_trigger_")?;
		st.awaiting = Awaiting::Trigger(key, cell.to_token_stream());
		let dag = &st.cfg.dag;
		return Ok(Some(quote! { #shim ! { #dag::__graph_resolve, [], @state #st } }));
	}

	st.awaiting = Awaiting::Node(key, cell.to_token_stream());
	Ok(Some(ask(st, &dep.shim, &cell)))
}

pub fn resolve(input: TokenStream) -> syn::Result<TokenStream> {
	let mut r = Reader::new(input);
	let answer = read_answer(&mut r);
	r.at("state");
	let mut st = State::parse(&mut r);
	accept(&mut st, answer)?;

	loop {
		if let Some((key, i)) = st.stack.last().cloned() {
			let deps = st.known.iter().find(|n| n.key == key).expect("a stacked node is known").deps.clone();
			let Some(dep) = deps.get(i).cloned() else {
				st.stack.pop();
				st.order.push(key);
				continue;
			};
			st.stack.last_mut().expect("just read").1 += 1;
			match visit(&mut st, dep)? {
				Some(call) => return Ok(call),
				None => continue,
			}
		}
		if !st.queue.is_empty() {
			let dep = st.queue.remove(0);
			match visit(&mut st, dep)? {
				Some(call) => return Ok(call),
				None => continue,
			}
		}
		return emit(st);
	}
}

/// A private field name that still reads as the node it holds — the error messages are written in
/// these, and the key's own spelling says more than an index would.
fn field_of(key: &str) -> Ident {
	let s: String = key.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect();
	Ident::new(&format!("__n_{s}"), Span::call_site())
}

fn emit(st: State) -> syn::Result<TokenStream> {
	let c = &st.cfg;
	let dag = &c.dag;
	let (vis, graph, batches, out) = (&c.vis, &c.graph, &c.batches, &c.out);

	let rfields: Vec<&Ident> = c.roots.iter().map(|r| &r.field).collect();
	let root_tys: Vec<&TokenStream> = c.roots.iter().map(|r| &r.ty).collect();
	let event_tys: Vec<&TokenStream> = c.roots.iter().map(|r| &r.event).collect();

	let nodes: Vec<&NodeInfo> = st.order.iter().map(|k| st.known.iter().find(|n| n.key == *k).expect("an ordered node is known")).collect();
	// a buffer's reach is the join of every read taken of it, which is only known now that they all are.
	let node_tys: Vec<TokenStream> = nodes
		.iter()
		.map(|n| match st.bufs.iter().find(|b| b.key == n.key) {
			Some(b) => {
				let cell = &b.ty;
				let k = b.reaches.iter().skip(1).fold(b.reaches[0].clone(), |acc, h| quote!(#dag::Horizon::join(#acc, #h)));
				quote!(#dag::Buffer<#cell, { #k }>)
			}
			None => n.ty.clone(),
		})
		.collect();
	let fields: Vec<Ident> = nodes.iter().map(|n| field_of(&n.key)).collect();
	let names: Vec<String> = nodes.iter().map(|n| n.key.clone()).collect();
	let sup = crate::demand::suppressors(&st)?;

	let latched: Vec<usize> = (0..nodes.len()).filter(|i| nodes[*i].latch).collect();
	let lfields: Vec<&Ident> = latched.iter().map(|i| &fields[*i]).collect();
	let latch_tys: Vec<&TokenStream> = latched.iter().map(|i| &node_tys[*i]).collect();

	let decls: Vec<TokenStream> = nodes.iter().map(|n| if n.emit { quote!(#dag::Emit) } else { quote!(#dag::Node) }).collect();
	let node_deps: Vec<TokenStream> = node_tys.iter().zip(&decls).map(|(t, d)| quote!(<#t as #d>::Deps)).collect();
	// an `emit` node's out is a run the engine buffers; the frame is still keyed on the node itself.
	let node_fields: Vec<TokenStream> = nodes
		.iter()
		.zip(&fields)
		.zip(&node_tys)
		.map(|((n, f), t)| if n.emit { quote!(#f: #dag::Emitter<#t>) } else { quote!(#f: #t) })
		.collect();

	// the demand is hoisted into its own binding: as an argument it would sit after `f`, which the
	// call moves before the gate reads could borrow it.
	let steps = nodes.iter().zip(&fields).zip(&sup).map(|((n, f), s)| {
		let gates: Vec<&TokenStream> = s.iter().map(|i| &node_tys[*i]).collect();
		// a `Gate`'s out *is* the `bool`, and `Gating::opens` is the identity — nothing to unwrap.
		let demand = quote!(let d = #(#dag::Has::<#gates, _>::get(&f))&&*;);
		match (n.emit, n.diff, s.is_empty()) {
			(true, _, true) => quote!(let f = #dag::step_emit_obs(f, #f, true, ts, obs);),
			(true, _, false) => quote!(#demand let f = #dag::step_emit_obs(f, #f, d, ts, obs);),
			(false, true, true) => quote!(let f = #dag::step_exact(f, #f, obs);),
			(false, true, false) => quote!(#demand let f = #dag::step_exact_when(f, #f, d, obs);),
			(false, false, true) => quote!(let f = #dag::step_obs(f, #f, obs);),
			(false, false, false) => quote!(#demand let f = #dag::step_when_obs(f, #f, d, obs);),
		}
	});

	// an `Emitter` resets through its own method: `Default::default()` on the wrapper would drop the
	// buffer's capacity, which the next episode only has to earn back.
	let resets: Vec<TokenStream> = nodes
		.iter()
		.zip(&fields)
		.map(|(n, f)| {
			if n.emit {
				quote!(self.#f.reset())
			} else {
				quote!(self.#f = ::core::default::Default::default())
			}
		})
		.collect();

	let pending = Ident::new(&format!("__{graph}Pending"), Span::call_site());
	let apply_pending = latched.iter().map(|i| {
		let (lf, lty) = (&fields[*i], &node_tys[*i]);
		let (resets, node_deps) = (&resets, &node_deps);
		quote! {
			if self.__pending.#lf {
				self.__pending.#lf = false;
				<#lty as #dag::Latch>::commutate(&mut self.#lf);
				#(
					if const {
						#dag::gates_on(
							<#node_deps as #dag::DepSet>::NAMES,
							<#node_deps as #dag::DepSet>::GATES,
							#dag::node_name::<#lty>(),
						)
					} {
						#resets;
					}
				)*
			}
		}
	});

	let (ofields, otys): (Vec<&Ident>, Vec<&TokenStream>) = c.named.iter().map(|n| (&n.field, &n.ty)).unzip();

	Ok(quote! {
		#vis struct #batches<'t> {
			#(pub #rfields: <#root_tys as #dag::Cell>::Out<'t>,)*
		}

		#[derive(Default)]
		#[doc(hidden)]
		struct #pending {
			#(#lfields: bool,)*
		}

		#[derive(Default)]
		#vis struct #graph {
			#(#node_fields,)*
			__pending: #pending,
		}

		const _: () = {
			const LATCHES: &[&str] = &[#(#dag::node_name::<#latch_tys>()),*];
			const NAMES: &[&str] = &[#(#dag::node_name::<#node_tys>()),*];
			const METAS: &[#dag::NodeMeta] = &[#(
				#dag::NodeMeta {
					name: #dag::node_name::<#node_tys>(),
					deps: <#node_deps as #dag::DepSet>::NAMES,
					gates: <#node_deps as #dag::DepSet>::GATES,
					latch: #dag::contains(LATCHES, #dag::node_name::<#node_tys>()),
				},
			)*];

			// the derived node set is deduped on how the deps spell each cell; two spellings that are
			// one type but normalise apart would otherwise surface as an inscrutable `Has` ambiguity.
			assert!(#dag::distinct(NAMES), "two nodes of this graph share a `Cell::NAME`");

			// a latch is cut from within: what it gates must be what cuts it. The *field*, not the type:
			// a const-generic type name carries braces, and `assert!` reads its message as a format string.
			#(assert!(
				#dag::cut_gated(#dag::node_name::<<#latch_tys as #dag::Latch>::Cut>(), #dag::node_name::<#latch_tys>(), METAS),
				concat!(stringify!(#lfields), "'s `Cut` is no field of this graph naming it in a `Gating` dep — a latch cut by something it does not gate never commutates")
			);)*

			// an internal latch must not gate its own arm.
			#(assert!(
				!#dag::deadlocked(#dag::node_name::<#latch_tys>(), <<#latch_tys as #dag::Node>::Deps as #dag::DepSet>::NAMES, METAS),
				concat!(stringify!(#lfields), " gates its own arm: the arm is dark exactly while the latch is down, so it can never re-arm")
			);)*

			// a declared rate its inputs cannot deliver — and, where a node names its period in its own
			// type and reads producers named for the same one, the pin that keeps the two from drifting.
			#(assert!(
				#dag::clock_divides(<#node_tys as #dag::Cell>::CLOCK, <#node_deps as #dag::DepSet>::CLOCKS),
				concat!(stringify!(#fields), " declares a `Cell::CLOCK` no whole number of its inputs' periods tiles")
			);)*
		};

		#[derive(Clone, Copy, Debug)]
		#vis struct #out<'t> {
			#(pub #ofields: <#otys as #dag::Cell>::Out<'t>,)*
			#[doc(hidden)]
			pub __lt: ::core::marker::PhantomData<&'t ()>,
		}

		impl #dag::Roots for #graph {
			/// `TypeId`s of the events whose root is consumed by some node (the dep tree).
			fn required_events() -> #dag::MacroVec<::core::any::TypeId> {
				const DEPS: &[&[&str]] = &[#(<#node_deps as #dag::DepSet>::NAMES),*];
				let mut out = #dag::MacroVec::new();
				#(
					if DEPS.iter().any(|ns| #dag::contains(ns, #dag::node_name::<#root_tys>())) {
						out.push(#dag::event_id::<#event_tys>());
					}
				)*
				out
			}
		}

		impl #graph {
			/// The derived closure, in sweep order — what this graph's outputs actually cost.
			#vis const NODES: &'static [&'static str] = &[#(#names),*];

			/// `ts` is the tick's event time in nanoseconds — what a node's declared `Emit::CLOCK` is
			/// read against, and the only thing the sweep needs from a tick besides its batches.
			#vis fn tick<'t>(&'t mut self, ts: i64, b: #batches<'t>) -> #out<'t> {
				self.tick_obs(ts, b, &mut ())
			}

			#vis fn tick_obs<'t>(&'t mut self, ts: i64, b: #batches<'t>, obs: &mut impl #dag::Observer) -> #out<'t> {
				// deferred commutation: apply last tick's terminals before anything borrows self.
				#(#apply_pending)*

				let #batches { #(#rfields,)* } = b;
				let Self { #(#fields,)* __pending } = self;

				#(#dag::observe_root::<#root_tys, _>(#rfields, obs);)*
				let f = #dag::Nil;
				#(let f = #dag::Cons::<#root_tys, _> { out: #rfields, tail: f };)*
				#(#steps)*

				#(
					if #dag::Episode::terminal(&#dag::Has::<<#latch_tys as #dag::Latch>::Cut, _>::get(&f)) {
						__pending.#lfields = true;
					}
				)*

				#out {
					#(#ofields: #dag::Has::<#otys, _>::get(&f),)*
					__lt: ::core::marker::PhantomData,
				}
			}
		}
	})
}
