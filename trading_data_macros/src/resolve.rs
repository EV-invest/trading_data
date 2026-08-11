//! `__graph_resolve!` — the driver. It walks the dep tree one node per expansion, calling back into
//! each cell's shim for edges it cannot see, and emits the graph once the closure is complete.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::{ToTokens, quote};
use syn::Type;

use crate::{
	diag::{Diag, Result},
	state::{Awaiting, Buf, Dep, NodeInfo, Reader, Sample, State},
	ty::{self, Wrap},
};

enum Answer {
	Node {
		emit: bool,
		latch: bool,
		anchored: bool,
		self_ty: TokenStream,
		deps: Vec<Dep>,
	},
	Trigger(Dep),
}

fn read_answer(r: &mut Reader) -> Option<Answer> {
	if r.peek_at("kind") {
		r.at("kind");
		let emit = r.ident() == "emit";
		r.at("latch");
		let latch = r.flag();
		r.at("anchored");
		let anchored = r.flag();
		r.at("self");
		let self_ty = r.brace();
		r.at("deps");
		let mut dr = r.list();
		let mut deps = Vec::new();
		while !dr.eof() {
			deps.push(Dep { shim: dr.brace(), ty: dr.brace() });
		}
		return Some(Answer::Node {
			emit,
			latch,
			anchored,
			self_ty,
			deps,
		});
	}
	if r.peek_at("trigger") {
		r.at("trigger");
		return Some(Answer::Trigger(Dep { shim: r.brace(), ty: r.brace() }));
	}
	None
}

fn key_of(ts: &TokenStream) -> Result<(String, Type)> {
	let t: Type = ty::parse_type(&ty::flatten(ts.clone()))?;
	Ok((ty::norm(&t), t))
}

fn on_stack(st: &State, key: &str) -> bool {
	st.stack.iter().any(|(k, _)| k == key)
}

fn cycle(st: &State, key: &str) -> Diag {
	let mut path: Vec<&str> = st.stack.iter().map(|(k, _)| k.as_str()).collect();
	path.push(key);
	Diag::new(Span::call_site(), format!("dep cycle: {}", path.join(" → ")))
		.note("a graph is a dag: a node cannot read what it feeds")
		.help("if the back edge is meant to read the *previous* tick, that is state the node keeps itself, not a dep")
}

/// Records a resolved node, unless the answer names one already walked — which is what an alias
/// looks like from here: asked about under one spelling, answering under its own.
fn record(st: &mut State, info: NodeInfo) -> Result<()> {
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

fn accept(st: &mut State, answer: Option<Answer>) -> Result<()> {
	let awaiting = core::mem::replace(&mut st.awaiting, Awaiting::Nothing);
	match (awaiting, answer) {
		(Awaiting::Nothing, None) => Ok(()),
		(
			Awaiting::Node(asked, req),
			Some(Answer::Node {
				emit,
				latch,
				anchored,
				self_ty,
				deps,
			}),
		) => {
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
					generated: false,
					latch,
					anchored,
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
				generated: false,
				latch: true,
				anchored: false,
				deps: vec![arm],
			},
		),
		// `#[node]` names a body-trait shim `__td_node_` and an `Episodic` one `__td_trigger_`, so a cell
		// asked for under the wrong reading fails to resolve the macro rather than reaching here. What
		// does reach here is a shim nothing in this crate wrote.
		_ => Err(Diag::new(Span::call_site(), "a shim answered a question `graph!` did not ask").note(
			"`__td_node_*` and `__td_trigger_*` are written by `#[node]` and read by the driver alone: one hand-written, or left behind by another version of this crate, is what gets here",
		)),
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

fn is_root(st: &State, key: &str) -> bool {
	st.cfg.roots.iter().any(|r| key_of(&r.ty).map(|(k, _)| k == key).unwrap_or(false))
}

/// Step `dep` back onto the walk: the driver has to see it a second time, once the question it asked
/// first has been answered. The stack is the walk while there is one, and the queue is what seeds it.
fn re_walk(st: &mut State, dep: &Dep) {
	match st.stack.last_mut() {
		Some((_, walked)) => *walked -= 1,
		None => st.queue.insert(0, dep.clone()),
	}
}

/// Walks one edge. `Ok(Some(..))` is a question only the compiler can answer: emit it and pause.
fn visit(st: &mut State, dep: Dep) -> Result<Option<TokenStream>> {
	let (_, full) = key_of(&dep.ty)?;
	let (cell, wrap) = ty::unwrap_dep(&full)?;
	let key = ty::norm(&cell);

	if let Wrap::Buf { reach } = wrap {
		// A cell reached through a `node_alias!` answers under its own key. Keying the retention on the
		// spelling instead would put a second `Buffer` over one series in the frame, which makes every
		// `Buffering` of it ambiguous — so the cell is resolved first and the key taken from its answer.
		let settled = is_root(st, &key) || st.known.iter().any(|n| n.key == key);
		let key = match st.aliases.iter().find(|(a, _)| *a == key) {
			Some((_, answered)) => answered.clone(),
			None if !settled => {
				re_walk(st, &dep);
				st.awaiting = Awaiting::Node(key, cell.to_token_stream());
				return Ok(Some(ask(st, &dep.shim, &cell)));
			}
			None => key,
		};
		let dag = &st.cfg.dag;
		// the join is over `Horizon` values, so the [`Reach`] type the dep stated is projected here.
		let reach = quote!(<#reach as #dag::Reach>::HORIZON);
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
				generated: true,
				latch: false,
				anchored: false,
				deps: vec![Dep {
					shim: dep.shim,
					ty: cell.to_token_stream(),
				}],
			},
		)
		.map(|()| None);
	}

	if let Wrap::Sample = wrap {
		// keyed on the cell's own answer, for the reason a `Buffer` is: two spellings of one series
		// would put two carries in the frame, which makes every `Sampling` of it ambiguous.
		let settled = is_root(st, &key) || st.known.iter().any(|n| n.key == key);
		let key = match st.aliases.iter().find(|(a, _)| *a == key) {
			Some((_, answered)) => answered.clone(),
			None if !settled => {
				re_walk(st, &dep);
				st.awaiting = Awaiting::Node(key, cell.to_token_stream());
				return Ok(Some(ask(st, &dep.shim, &cell)));
			}
			None => key,
		};
		// no node, and so no demand formula, no pin and no card: the carry is filled in the same
		// generated line as the series it follows, which is what leaves it nothing to sleep through.
		if !st.samples.iter().any(|s| s.key == key) {
			st.samples.push(Sample { key, ty: cell.to_token_stream() });
		}
		return Ok(None);
	}

	if is_root(st, &key) {
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
		let inner = ty::sole_arg(&cell).ok_or_else(|| Diag::spanned(&cell, "`Armed` names the episode it latches").help("write `Armed<C>`"))?;
		let shim = ty::shim_path(&inner, "__td_trigger_")?;
		st.awaiting = Awaiting::Trigger(key, cell.to_token_stream());
		let dag = &st.cfg.dag;
		return Ok(Some(quote! { #shim ! { #dag::__graph_resolve, [], @state #st } }));
	}

	st.awaiting = Awaiting::Node(key, cell.to_token_stream());
	Ok(Some(ask(st, &dep.shim, &cell)))
}

pub fn resolve(input: TokenStream) -> Result<TokenStream> {
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

/// Where the carry over a sampled series stands. Not a node field: nothing steps it, nothing
/// resets it, and `Sampling<C>` is what the frame slot beside it is keyed on.
fn held_of(key: &str) -> Ident {
	let s: String = key.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect();
	Ident::new(&format!("__held_{s}"), Span::call_site())
}

/// What `#[node(anchored)]` may be written on. Here rather than in a `const` assert because the
/// message has to name the node *and* the dep that broke it, and an `assert!` message is a literal.
///
/// A rewind re-reads a node's inputs out of the past, and only a root maps one-to-one onto a lane a
/// past can be read from. A derived dep would want its own producer replayed first, recursively;
/// that is a playback architecture this does not have yet, and the dominant use of anchoring is
/// volume-bottlenecked nodes, which are input-shaped and so covered by roots.
fn anchoring(st: &State) -> Result<()> {
	let at = |m: String| Diag::new(Span::call_site(), m);
	for n in st.known.iter().filter(|n| n.anchored) {
		if n.emit {
			return Err(at(format!("`{}` is `#[node(anchored)]` on a run-shaped node", n.key))
				.note("a rewind restores what a node *holds*, and an emitter's out is the engine's buffer, which is this tick's and no other's"));
		}
		for d in &n.deps {
			let (dep, wrap) = crate::demand::cell(st, &d.ty)?;
			// a gating dep is permission, not data: it is read at the sweep position like any other
			// tick's, and no rewind re-reads it.
			if matches!(wrap, Wrap::Gate) {
				continue;
			}
			if matches!(wrap, Wrap::Fold) {
				return Err(at(format!("`{}` is `#[node(anchored)]` and folds `{dep}` itself", n.key))
					.help(format!("state it as `Buffering<{dep}, R>`, and the reach becomes the frame's"))
					.note("a `Folding` reach is the node's own state, so a rewind that re-read it would fold it twice"));
			}
			if !is_root(st, &dep) {
				return Err(at(format!("`{}` is `#[node(anchored)]` and reads `{dep}`, which is no root of this graph", n.key))
					.note("a rewind reads a node's inputs back out of the past, and only a root is a lane a past can be read from")
					.note("replaying an arbitrary producer is a thing the playback side may grow; it is not needed yet"));
			}
		}
	}
	Ok(())
}

fn emit(st: State) -> Result<TokenStream> {
	anchoring(&st)?;
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
	let indices: Vec<Literal> = (0..nodes.len()).map(Literal::usize_unsuffixed).collect();
	let names: Vec<String> = nodes.iter().map(|n| n.key.clone()).collect();
	let demand = crate::demand::suppressors(&st)?;
	let (sup, rewinders) = (&demand.live, &demand.rewinders);

	let latched: Vec<usize> = (0..nodes.len()).filter(|i| nodes[*i].latch).collect();
	let lfields: Vec<&Ident> = latched.iter().map(|i| &fields[*i]).collect();
	let latch_tys: Vec<&TokenStream> = latched.iter().map(|i| &node_tys[*i]).collect();

	// a latch that some demand formula reads gets a local for its standing bit — the one gate whose
	// value does not depend on where in the sweep it is asked, hoisted so it is taken exactly once.
	let standing: Vec<Option<Ident>> = (0..nodes.len())
		.map(|i| (nodes[i].latch && sup.iter().flatten().any(|conj| conj.contains(&i))).then(|| Ident::new(&format!("__standing{}", fields[i]), Span::call_site())))
		.collect();
	let standing_decls: Vec<TokenStream> = (0..nodes.len())
		.filter_map(|i| {
			let (id, f, t) = (standing[i].as_ref()?, &fields[i], &node_tys[i]);
			Some(quote!(let #id = <#t as #dag::Latch>::standing(&self.#f);))
		})
		.collect();

	let decls: Vec<TokenStream> = nodes.iter().map(|n| if n.emit { quote!(#dag::Emit) } else { quote!(#dag::Node) }).collect();

	// one carry per sampled series: a graph field for the value that stands, and a frame slot pushed
	// in the series' own line, so a `Sampling` read is a plain `Has` off `Sampling<C>`.
	let held_fields: Vec<Ident> = st.samples.iter().map(|s| held_of(&s.key)).collect();
	let held_decls: Vec<TokenStream> = st
		.samples
		.iter()
		.zip(&held_fields)
		.map(|(s, f)| {
			let cell = &s.ty;
			quote!(#f: ::core::option::Option<<<#cell as #dag::Series>::Item as #dag::Present>::Val>)
		})
		.collect();
	let carries = |key: &str| -> TokenStream {
		st.samples
			.iter()
			.filter(|s| s.key == key)
			.map(|s| {
				let (cell, f) = (&s.ty, held_of(&s.key));
				quote!(let f = #dag::Cons::<#dag::Sampling<#cell>, _> { out: #dag::carry::<#cell>(#f, #dag::Has::<#cell, _>::get(&f)), tail: f };)
			})
			.collect()
	};
	// a carry over a root is filled the same way a carry over a node is, in the line that puts its
	// source on the frame — which for a root is the seeding, not a step.
	let root_pushes: Vec<TokenStream> = c
		.roots
		.iter()
		.map(|r| {
			let (ty, field) = (&r.ty, &r.field);
			let carry = carries(&key_of(&r.ty)?.0);
			Ok(quote!(let f = #dag::Cons::<#ty, _> { out: #field, tail: f }; #carry))
		})
		.collect::<Result<_>>()?;

	// the fidelity census. Both sides read it off the kernel the node named, so neither is tracked
	// through the driver and neither can drift from what actually computes.
	let (hatch_names, fidelities): (Vec<&String>, Vec<TokenStream>) = nodes
		.iter()
		.zip(&node_tys)
		.filter(|(n, _)| !n.generated)
		.map(|(n, t)| {
			let fid = match n.emit {
				true => quote!(<<#t as #dag::Emit>::Kernel as #dag::Run<#t>>::FIDELITY),
				false => quote!(<<#t as #dag::Node>::Kernel as #dag::Level<#t>>::FIDELITY),
			};
			(&n.key, fid)
		})
		.unzip();
	let node_deps: Vec<TokenStream> = node_tys.iter().zip(&decls).map(|(t, d)| quote!(<#t as #d>::Deps)).collect();
	// an `emit` node's out is a run the engine buffers; the frame is still keyed on the node itself.
	let node_fields: Vec<TokenStream> = nodes
		.iter()
		.zip(&fields)
		.zip(&node_tys)
		.map(|((n, f), t)| if n.emit { quote!(#f: #dag::Emitter<#t>) } else { quote!(#f: #t) })
		.collect();

	// the anchored set: what `P` has to be a past for, and — since nothing else names `P` — the whole
	// of what a driver's feed owes the sweep.
	let rewound: Vec<TokenStream> = (0..nodes.len())
		.filter(|i| nodes[*i].anchored)
		.map(|i| {
			let t = &node_tys[i];
			quote!(#dag::Rewound<#t>)
		})
		.collect();
	// `!REWINDS` per node, as a demand disjunct: a past that does not rewind is one nothing may sleep
	// behind. Same erasure as the latch carve-out above, and the whole of what `Awake` (and so a live
	// feed) costs.
	let pinned_awake: Vec<Vec<TokenStream>> = rewinders
		.iter()
		.map(|rs| {
			rs.iter()
				.map(|r| {
					let t = &node_tys[*r];
					quote!(!const { <P as #dag::Rewound<#t>>::REWINDS })
				})
				.collect()
		})
		.collect();
	let past_bound = match rewound.is_empty() {
		true => TokenStream::new(),
		false => quote!(where P: #(#rewound)+*),
	};
	// `P` is then a parameter of the shape alone — the entry point is the same one either way, so a
	// driver need not know whether the graph it holds happens to anchor anything.
	let past_unread = rewound.is_empty().then(|| quote!(let _ = past;));

	// the demand is hoisted into its own binding: as an argument it would sit after `f`, which the
	// call moves before the gate reads could borrow it.
	let steps = nodes.iter().zip(&fields).zip(sup).zip(&node_tys).zip(&pinned_awake).map(|((((n, f), s), ty), awake)| {
		// a `Gate`'s out *is* the `bool`, and `Gating::opens` is the identity — nothing to unwrap.
		let mut disj: Vec<TokenStream> = s
			.iter()
			.map(|conj| {
				let terms = conj.iter().map(|g| match &standing[*g] {
					Some(id) => quote!(#id),
					None => {
						let gt = &node_tys[*g];
						quote!(#dag::Has::<#gt, _>::get(&f))
					}
				});
				quote!((#(#terms)&&*))
			})
			.collect();
		disj.extend(awake.iter().cloned());
		let demand = quote!(let d = #(#disj)||*;);
		let uncond = s.len() == 1 && s[0].is_empty();
		// at the node's own sweep position: its retention has stepped and its consumer has not read it
		// yet, so this is the one point where the tick it is demanded on is still the tick it answers
		// on. Conditioned on the whole of `advances`, not on the demand alone — an anchored node that
		// nothing suppresses still goes dark behind its own gate, and rewinding one that will not run
		// would be the fold the sleep was taken to avoid.
		let rewind = n.anchored.then(|| {
			let demanded = if uncond { quote!(true) } else { quote!(d) };
			quote! {
				if const { <P as #dag::Rewound<#ty>>::REWINDS } && #dag::advances::<#ty, _, _>(&f, #demanded) {
					<P as #dag::Rewound<#ty>>::rewind(past, &mut *#f);
				}
			}
		});
		let step = match (n.emit, uncond) {
			(true, true) => quote!(let f = #dag::step_emit_obs(f, #f, true, ts, __sweep, obs);),
			(true, false) => quote!(#demand let f = #dag::step_emit_obs(f, #f, d, ts, __sweep, obs);),
			(false, true) => quote!(#rewind let f = #dag::step_obs(f, #f, __sweep, obs);),
			(false, false) => quote!(#demand #rewind let f = #dag::step_when_obs(f, #f, d, __sweep, obs);),
		};
		let carry = carries(&n.key);
		quote!(#step #carry)
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

	// r[impl outs.moved.outputs-plane]
	// a level output carries the outputs-plane `Moved` reading; a run's edge is its element count,
	// and a root is a run by nature.
	let olevel: Vec<bool> = c
		.named
		.iter()
		.map(|n| {
			let key = key_of(&n.ty)?.0;
			let key = st.aliases.iter().find(|(a, _)| *a == key).map(|(_, k)| k.clone()).unwrap_or(key);
			Ok(st.known.iter().find(|kn| kn.key == key).is_some_and(|kn| !kn.emit))
		})
		.collect::<Result<_>>()?;
	let moved_fields: Vec<Ident> = c
		.named
		.iter()
		.zip(&olevel)
		.filter(|(_, l)| **l)
		.map(|(n, _)| Ident::new(&format!("__moved_{}", n.field), Span::call_site()))
		.collect();
	let odecls: Vec<TokenStream> = c
		.named
		.iter()
		.zip(&olevel)
		.map(|(n, l)| {
			let (f, t) = (&n.field, &n.ty);
			match l {
				true => quote!(pub #f: #dag::Moved<<#t as #dag::Cell>::Out<'t>>),
				false => quote!(pub #f: <#t as #dag::Cell>::Out<'t>),
			}
		})
		.collect();
	let oinits: Vec<TokenStream> = {
		let mut moved = moved_fields.iter();
		c.named
			.iter()
			.zip(&olevel)
			.map(|(n, l)| {
				let (f, t) = (&n.field, &n.ty);
				match l {
					true => {
						let mf = moved.next().expect("one storage field per level output");
						quote!(#f: { let __o = #dag::Has::<#t, _>::get(&f); #dag::Moved { moved: #mf.moved(&__o), out: __o } })
					}
					false => quote!(#f: #dag::Has::<#t, _>::get(&f)),
				}
			})
			.collect()
	};

	let shape = crate::shape::shape_const(&st, &demand, &node_tys);

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
			#(#held_decls,)*
			#(#moved_fields: #dag::PrevOut,)*
			__pending: #pending,
			__sweep: #dag::Sweep,
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
			// Per node rather than over the set, so the message can name a field: `assert!` reads a
			// non-literal message as a format string, and const panic formats nothing.
			#(assert!(
				!#dag::contains(NAMES.split_at(#indices).0, #dag::node_name::<#node_tys>()),
				concat!(stringify!(#fields), " shares its `Cell::NAME` with a node stepped before it — one cell reached under two spellings the driver read apart")
			);)*

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
			#(#odecls,)*
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

			/// How this graph *compiles*: the sweep order, every edge and its kind, and the demand
			/// formula each node's dormancy is decided by. `Display` draws it, and `Shape::under`
			/// answers the question a running graph cannot be asked — who goes dark under a gate
			/// assignment that has not happened.
			#vis const SHAPE: #dag::Shape = #shape;

			/// Every node this graph *declares*, and how much of what it read its Jacobian covers
			/// (`r[kernels.fidelity.stated]`). The engine's own `Buffer` fields are left out: nobody
			/// wrote them, so nobody owes a reason for them.
			///
			/// Count `Partial` and `Opaque` separately and pin both — they are different admissions. Either
			/// may fall silently; either may only rise in a diff that says why.
			#vis const FIDELITY: &'static [(&'static str, #dag::Fidelity)] = &[#((#hatch_names, #fidelities)),*];

			/// `ts` is the tick's event time in nanoseconds — what a node's declared `Emit::CLOCK` is
			/// read against, and the only thing the sweep needs from a tick besides its batches.
			///
			/// Every anchored node is pinned awake here: `Awake` is a past that never rewinds, so nothing
			/// sleeps behind a replay that is not going to happen.
			#vis fn tick<'t>(&'t mut self, ts: i64, b: #batches<'t>) -> #out<'t> {
				self.tick_rewind_obs(ts, b, &mut #dag::Awake, &mut ())
			}

			#vis fn tick_obs<'t>(&'t mut self, ts: i64, b: #batches<'t>, obs: &mut impl #dag::Observer) -> #out<'t> {
				self.tick_rewind_obs(ts, b, &mut #dag::Awake, obs)
			}

			/// [`tick`](Self::tick) with a past the anchored nodes may sleep behind: each is replayed
			/// forward on the tick it is demanded, so a sleep is invisible to whoever reads it.
			#vis fn tick_rewind<'t, P>(&'t mut self, ts: i64, b: #batches<'t>, past: &mut P) -> #out<'t> #past_bound {
				self.tick_rewind_obs(ts, b, past, &mut ())
			}

			#vis fn tick_rewind_obs<'t, P>(&'t mut self, ts: i64, b: #batches<'t>, past: &mut P, obs: &mut impl #dag::Observer) -> #out<'t> #past_bound {
				#past_unread
				// deferred commutation: apply last tick's terminals before anything borrows self.
				#(#apply_pending)*
				// after the commutation, so a spent episode reads open here and nothing behind it stays
				// dark a tick longer than the episode did.
				#(#standing_decls)*

				let #batches { #(#rfields,)* } = b;
				let Self { #(#fields,)* #(#held_fields,)* #(#moved_fields,)* __pending, __sweep } = self;
				__sweep.restart();

				#(#dag::observe_root::<#root_tys, _>(#rfields, __sweep, obs);)*
				let f = #dag::Nil;
				#(#root_pushes)*
				#(#steps)*

				#(
					if #dag::Episode::terminal(&#dag::Has::<<#latch_tys as #dag::Latch>::Cut, _>::get(&f)) {
						__pending.#lfields = true;
					}
				)*

				#out {
					#(#oinits,)*
					__lt: ::core::marker::PhantomData,
				}
			}
		}
	})
}
