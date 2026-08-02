//! The driver's whole state, as tokens.
//!
//! Each round of the trampoline hands it to a `macro_rules!` shim, which passes it back verbatim, so
//! it must survive as an opaque `tt` run — and it carries type tokens (`$crate::AvgGain<$B, $S>`)
//! whose hygiene a string round-trip would destroy. Hence a token grammar rather than a serialized
//! one: every field is a brace group of raw tokens or a bracket group of records.

use proc_macro2::{Delimiter, Group, Ident, Literal, TokenStream, TokenTree};
use quote::{ToTokens, TokenStreamExt, quote};

/// Positional token reader. Every shape here is written by [`State::to_tokens`] and read straight
/// back, so a mismatch is a bug in this file and panicking says so at the one place that can fix it.
pub struct Reader {
	t: Vec<TokenTree>,
	i: usize,
}

impl Reader {
	pub fn new(ts: TokenStream) -> Self {
		Reader { t: ts.into_iter().collect(), i: 0 }
	}

	pub fn eof(&self) -> bool {
		self.i >= self.t.len()
	}

	fn pop(&mut self) -> TokenTree {
		let t = self.t.get(self.i).unwrap_or_else(|| panic!("__graph_resolve state ran out at {}", self.i)).clone();
		self.i += 1;
		t
	}

	fn group(&mut self, d: Delimiter) -> TokenStream {
		match self.pop() {
			TokenTree::Group(g) if g.delimiter() == d => g.stream(),
			other => panic!("__graph_resolve state: expected {d:?}, got `{other}`"),
		}
	}

	pub fn brace(&mut self) -> TokenStream {
		self.group(Delimiter::Brace)
	}

	/// A bracketed run of records. Separating commas are dropped here — `quote!` writes them and the
	/// shims' `@deps` lists carry them, but no record's own grammar has one.
	pub fn list(&mut self) -> Reader {
		let ts = self.group(Delimiter::Bracket);
		Reader {
			t: ts.into_iter().filter(|t| !matches!(t, TokenTree::Punct(p) if p.as_char() == ',')).collect(),
			i: 0,
		}
	}

	pub fn ident(&mut self) -> Ident {
		match self.pop() {
			TokenTree::Ident(i) => i,
			other => panic!("__graph_resolve state: expected an ident, got `{other}`"),
		}
	}

	pub fn text(&mut self) -> String {
		let lit = self.pop().to_string();
		lit.trim_matches('"').to_string()
	}

	pub fn int(&mut self) -> usize {
		self.pop().to_string().parse().expect("__graph_resolve state: an index is an integer literal")
	}

	pub fn flag(&mut self) -> bool {
		self.ident() == "true"
	}

	/// Skips the `@name` marker the shims write for legibility.
	pub fn at(&mut self, name: &str) {
		let TokenTree::Punct(p) = self.pop() else { panic!("expected `@{name}`") };
		assert_eq!(p.as_char(), '@');
		let got = self.ident();
		assert_eq!(got, name, "expected `@{name}`");
	}

	pub fn peek_at(&self, name: &str) -> bool {
		matches!((self.t.get(self.i), self.t.get(self.i + 1)), (Some(TokenTree::Punct(p)), Some(TokenTree::Ident(i))) if p.as_char() == '@' && i == name)
	}
}

fn brace(ts: &TokenStream) -> TokenTree {
	TokenTree::Group(Group::new(Delimiter::Brace, ts.clone()))
}

fn list<T>(items: &[T], f: impl Fn(&T) -> TokenStream) -> TokenTree {
	TokenTree::Group(Group::new(Delimiter::Bracket, items.iter().map(f).collect()))
}

fn text(s: &str) -> TokenTree {
	TokenTree::Literal(Literal::string(s))
}

fn flag(b: bool) -> TokenTree {
	TokenTree::Ident(Ident::new(if b { "true" } else { "false" }, proc_macro2::Span::call_site()))
}

/// A root cell and the event type its slice carries.
pub struct Root {
	pub field: Ident,
	pub ty: TokenStream,
	pub event: TokenStream,
}

/// A `outputs`/`observe` entry: the name the app reads it under, and the cell.
pub struct Named {
	pub field: Ident,
	pub ty: TokenStream,
}

/// An edge, as a shim resolves it: where to ask about the cell, and the dep expression naming it.
#[derive(Clone)]
pub struct Dep {
	pub shim: TokenStream,
	pub ty: TokenStream,
}

#[derive(Clone)]
pub struct NodeInfo {
	pub key: String,
	pub ty: TokenStream,
	pub emit: bool,
	pub diff: bool,
	pub latch: bool,
	pub deps: Vec<Dep>,
}

/// One series the frame must retain, and every reach asked of it.
pub struct Buf {
	pub key: String,
	pub ty: TokenStream,
	pub reaches: Vec<TokenStream>,
}

/// Which shim's answer the next driver round is reading.
pub enum Awaiting {
	Nothing,
	Node(String, TokenStream),
	Trigger(String, TokenStream),
}

pub struct Cfg {
	/// The path the invoking crate reaches the dag under, resolved once by `graph!`.
	pub dag: TokenStream,
	pub vis: TokenStream,
	pub graph: Ident,
	pub batches: Ident,
	pub out: Ident,
	pub roots: Vec<Root>,
	pub named: Vec<Named>,
}

pub struct State {
	pub cfg: Cfg,
	pub awaiting: Awaiting,
	pub known: Vec<NodeInfo>,
	/// DFS frames: a node and how many of its deps have been walked.
	pub stack: Vec<(String, usize)>,
	/// Post-order — the topological order the sweep is emitted in.
	pub order: Vec<String>,
	pub bufs: Vec<Buf>,
	/// The `outputs`/`observe` cells not yet seeded into the walk.
	pub queue: Vec<Dep>,
}

impl State {
	pub fn parse(r: &mut Reader) -> State {
		let dag = r.brace();
		let vis = r.brace();
		let graph = r.ident();
		let batches = r.ident();
		let out = r.ident();

		let mut rr = r.list();
		let mut roots = Vec::new();
		while !rr.eof() {
			roots.push(Root {
				field: rr.ident(),
				ty: rr.brace(),
				event: rr.brace(),
			});
		}

		let mut nr = r.list();
		let mut named = Vec::new();
		while !nr.eof() {
			named.push(Named { field: nr.ident(), ty: nr.brace() });
		}

		let mut ar = Reader::new(r.brace());
		let awaiting = match ar.eof() {
			true => Awaiting::Nothing,
			false => {
				let tag = ar.ident();
				let (key, ty) = (ar.text(), ar.brace());
				match tag == "node" {
					true => Awaiting::Node(key, ty),
					false => Awaiting::Trigger(key, ty),
				}
			}
		};

		let mut kr = r.list();
		let mut known = Vec::new();
		while !kr.eof() {
			let (key, ty, emit, diff, latch) = (kr.text(), kr.brace(), kr.flag(), kr.flag(), kr.flag());
			let mut dr = kr.list();
			let mut deps = Vec::new();
			while !dr.eof() {
				deps.push(Dep { shim: dr.brace(), ty: dr.brace() });
			}
			known.push(NodeInfo { key, ty, emit, diff, latch, deps });
		}

		let mut sr = r.list();
		let mut stack = Vec::new();
		while !sr.eof() {
			stack.push((sr.text(), sr.int()));
		}

		let mut or = r.list();
		let mut order = Vec::new();
		while !or.eof() {
			order.push(or.text());
		}

		let mut br = r.list();
		let mut bufs = Vec::new();
		while !br.eof() {
			let (key, ty) = (br.text(), br.brace());
			let mut hr = br.list();
			let mut reaches = Vec::new();
			while !hr.eof() {
				reaches.push(hr.brace());
			}
			bufs.push(Buf { key, ty, reaches });
		}

		let mut qr = r.list();
		let mut queue = Vec::new();
		while !qr.eof() {
			queue.push(Dep { shim: qr.brace(), ty: qr.brace() });
		}

		State {
			cfg: Cfg {
				dag,
				vis,
				graph,
				batches,
				out,
				roots,
				named,
			},
			awaiting,
			known,
			stack,
			order,
			bufs,
			queue,
		}
	}
}

impl ToTokens for State {
	fn to_tokens(&self, ts: &mut TokenStream) {
		let c = &self.cfg;
		ts.append(brace(&c.dag));
		ts.append(brace(&c.vis));
		ts.append(c.graph.clone());
		ts.append(c.batches.clone());
		ts.append(c.out.clone());
		ts.append(list(&c.roots, |r| {
			let (f, t, e) = (&r.field, brace(&r.ty), brace(&r.event));
			quote!(#f #t #e)
		}));
		ts.append(list(&c.named, |n| {
			let (f, t) = (&n.field, brace(&n.ty));
			quote!(#f #t)
		}));
		ts.append(brace(&match &self.awaiting {
			Awaiting::Nothing => TokenStream::new(),
			Awaiting::Node(k, t) => {
				let (k, t) = (text(k), brace(t));
				quote!(node #k #t)
			}
			Awaiting::Trigger(k, t) => {
				let (k, t) = (text(k), brace(t));
				quote!(trigger #k #t)
			}
		}));
		ts.append(list(&self.known, |n| {
			let (k, t) = (text(&n.key), brace(&n.ty));
			let (e, d, l) = (flag(n.emit), flag(n.diff), flag(n.latch));
			let deps = list(&n.deps, |d| {
				let (s, t) = (brace(&d.shim), brace(&d.ty));
				quote!(#s #t)
			});
			quote!(#k #t #e #d #l #deps)
		}));
		ts.append(list(&self.stack, |(k, i)| {
			let (k, i) = (text(k), Literal::usize_unsuffixed(*i));
			quote!(#k #i)
		}));
		ts.append(list(&self.order, |k| text(k).into_token_stream()));
		ts.append(list(&self.bufs, |b| {
			let (k, t) = (text(&b.key), brace(&b.ty));
			let hs = list(&b.reaches, |h| brace(h).into_token_stream());
			quote!(#k #t #hs)
		}));
		ts.append(list(&self.queue, |d| {
			let (s, t) = (brace(&d.shim), brace(&d.ty));
			quote!(#s #t)
		}));
	}
}
