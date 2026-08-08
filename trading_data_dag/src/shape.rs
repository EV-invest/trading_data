//! The graph [`graph!`](crate::graph) built, as a value it can be asked about.
//!
//! The driver computes a sweep order, a kind per edge, and a demand formula per node, and then spends
//! all three writing one straight-line `tick`. Nothing survives into the generated code in a form
//! anything can read back — which is why "how does this graph compile" has only ever been answerable
//! by expanding it. [`Shape`] is that answer as data, and its [`Display`] is the answer as a picture.
//!
//! [`under`](Shape::under) is the half a running graph cannot give: a gate assignment is a
//! hypothetical, so who goes dark under one is the graph's own answer rather than a feed's.

use alloc::{borrow::ToOwned, format, string::String, vec, vec::Vec};
use core::fmt;

use v_utils::Timeframe;

use crate::{Fidelity, Horizon};

/// What a node *is* to the sweep. A root is no node of it at all — it is prepended so that every
/// edge has somewhere to point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
	Root,
	Node,
	Emit,
	Gate,
	Latch,
	/// The retention `graph!` writes beside a series some consumer reads through [`Buffering`](crate::Buffering).
	Buffer,
	/// The level it writes beside one read through [`Sampling`](crate::Sampling).
	Latest,
}

impl Kind {
	const fn word(self) -> &'static str {
		match self {
			Kind::Root => "root",
			Kind::Node => "node",
			Kind::Emit => "emit",
			Kind::Gate => "gate",
			Kind::Latch => "latch",
			Kind::Buffer => "buffer",
			Kind::Latest => "latest",
		}
	}
}

/// Why `graph!` never suppresses a node — kept apart rather than collapsed to a flag, so a render
/// says which reason held rather than only that one did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pin {
	None,
	/// It folds a reach of its own, which a skipped tick cannot re-warm.
	Fold,
	/// It is retention the frame must keep hole-free.
	Retention,
	Latch,
	/// It is what *decides* demand, so nothing conditions it.
	Gate,
	/// Somebody reads it out of the graph.
	Output,
}

impl Pin {
	const fn reason(self) -> Option<&'static str> {
		match self {
			Pin::None => None,
			Pin::Fold => Some("fold"),
			Pin::Retention => Some("retention"),
			Pin::Latch => Some("latch"),
			Pin::Gate => Some("gate"),
			Pin::Output => Some("output"),
		}
	}
}

/// One node of [`Shape::nodes`]. Every per-dep field is positional with
/// [`DepSet::NAMES`](crate::DepSet::NAMES) and is that trait's own constant, projected — so none of
/// it can drift from what compiles.
pub struct NodeShape {
	pub name: &'static str,
	pub kind: Kind,
	pub clock: Option<Timeframe>,
	pub fidelity: Fidelity,
	pub anchored: bool,
	pub rewarms: bool,
	pub pin: Pin,
	/// Whether a sleep of this node is one only a rewinding past makes sound — the driver's own
	/// rewinder set, as the single bit a render needs of it.
	pub rewinds: bool,
	/// The `roots`/`outputs` field name, where this node is one.
	pub field: Option<&'static str>,
	/// The index in [`Shape::nodes`] each dep resolves against. The one thing here only the driver
	/// knows: a dep spelling reaches its target through aliases, buffers and levels.
	pub deps: &'static [usize],
	pub dep_gates: &'static [bool],
	pub dep_folds: &'static [bool],
	pub dep_retains: &'static [bool],
	pub dep_reach: &'static [Horizon],
	/// `⋁` of conjuncts over [`Shape::nodes`] indices — the condition under which anything reads this
	/// node. An empty conjunct is `⊤`.
	pub demand: &'static [&'static [usize]],
}

pub struct Shape {
	pub graph: &'static str,
	/// Roots first, then sweep order — so every [`NodeShape::deps`] index is total and every edge
	/// points backwards.
	pub nodes: &'static [NodeShape],
}

impl Shape {
	/// Every gate and latch of this graph, spelled as the picture spells it — which is the whole of
	/// what [`under`](Shape::under) wants an assignment for.
	pub fn gates(&self) -> impl Iterator<Item = String> + '_ {
		self.nodes.iter().filter(|n| matches!(n.kind, Kind::Gate | Kind::Latch)).map(|n| trim(n.name))
	}

	/// The same picture, with each node read as live or dark under `state`.
	///
	/// Panics unless `state` names exactly the gate set: a graph that grows a gate has to break the
	/// snapshot that pinned its shape, which is the whole reason to pin one.
	pub fn under<'a>(&'a self, state: &'a [(&'static str, bool)]) -> Under<'a> {
		let mut missing: Vec<String> = self.gates().collect();
		for (i, (name, _)) in state.iter().enumerate() {
			assert!(
				!state[..i].iter().any(|(o, _)| o == name),
				"`{name}` is named twice in the state of `{}`{}",
				self.graph,
				self.gate_set()
			);
			match missing.iter().position(|g| g == name) {
				Some(k) => drop(missing.remove(k)),
				None => panic!("`{name}` is no gate of `{}`{}", self.graph, self.gate_set()),
			}
		}
		assert!(missing.is_empty(), "{} left unstated in the state of `{}`{}", missing.join(", "), self.graph, self.gate_set());
		Under { shape: self, state }
	}

	fn gate_set(&self) -> String {
		match self.gates().next() {
			None => " — it gates nothing".to_owned(),
			Some(_) => format!(" — its gates are {}", self.gates().collect::<Vec<_>>().join(", ")),
		}
	}
}

impl fmt::Display for Shape {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		render(self, None, f)
	}
}

/// A [`Shape`] read under one assignment of its gates — see [`Shape::under`].
pub struct Under<'a> {
	shape: &'a Shape,
	state: &'a [(&'static str, bool)],
}

impl Under<'_> {
	fn open(&self, name: &str) -> bool {
		let name = trim(name);
		self.state.iter().find(|(g, _)| *g == name).expect("`under` accepted the state, so every gate is in it").1
	}

	/// Read the way `resolve.rs` writes the sweep, so this is a reading of the emitted code rather
	/// than a second model of it: a node runs where something demands it *and* its own gates are open.
	///
	/// An anchored node's `!REWINDS` disjunct is taken as false throughout — a rewinding past is the
	/// graph's own answer, where `Awake` is a particular driver's.
	fn live(&self) -> Vec<bool> {
		let ns = self.shape.nodes;
		ns.iter()
			.map(|n| {
				if n.kind == Kind::Root {
					return true;
				}
				let own_open = n.deps.iter().zip(n.dep_gates).all(|(&t, &g)| !g || self.open(ns[t].name));
				// a latch is read a tick ahead of the consumer it arms, so it only darkens what says a
				// lost tick is one a later tick rebuilds.
				let latched = !n.rewarms && n.demand.iter().copied().flatten().any(|&g| ns[g].kind == Kind::Latch);
				let demanded = latched || n.demand.iter().any(|conj| conj.iter().all(|&g| self.open(ns[g].name)));
				demanded && own_open
			})
			.collect()
	}
}

impl fmt::Display for Under<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		render(self.shape, Some(self), f)
	}
}

/// [`Cell::NAME`](crate::Cell::NAME) defaults to [`core::any::type_name`], which spells a
/// hand-written node by its whole module path. Only the last segment names it, and a generic
/// argument is a name in its own right — a [`Tag`](crate::Tag)-built name has no path and falls
/// through unchanged.
fn trim(name: &str) -> String {
	let (mut out, mut seg) = (String::new(), 0usize);
	let mut it = name.char_indices().peekable();
	while let Some((i, c)) = it.next() {
		if c == ':' && matches!(it.peek(), Some((_, ':'))) {
			it.next();
			seg = i + 2;
			continue;
		}
		if matches!(c, '<' | '>' | ',' | '(' | ')') {
			out.push_str(&name[seg..i]);
			out.push(c);
			seg = i + c.len_utf8();
		}
	}
	out.push_str(&name[seg..]);
	out
}

fn horizon(h: Horizon) -> String {
	let t = h.tag();
	t.as_str().to_owned()
}

/// The columns to the right of the name: what the node declares, what pins it, and every edge whose
/// kind the lane alone cannot show.
fn annotate(shape: &Shape, i: usize) -> String {
	let n = &shape.nodes[i];
	let mut parts: Vec<String> = Vec::new();
	if let Some(c) = n.clock {
		parts.push(format!("@{c}"));
	}
	parts.push(match (n.kind, n.field) {
		(Kind::Root, Some(field)) => format!("root {field}"),
		(k, _) => k.word().to_owned(),
	});
	if let Some(r) = n.pin.reason() {
		parts.push(format!("pin·{r}"));
	}

	let mut edges: Vec<String> = Vec::new();
	for k in 0..n.deps.len() {
		let to = &shape.nodes[n.deps[k]];
		edges.push(match () {
			_ if n.dep_gates[k] => format!("⊣{}", trim(to.name)),
			_ if n.dep_folds[k] => format!("⟳{}@{}", trim(to.name), horizon(n.dep_reach[k])),
			// the retention's own name already carries the joined reach, so the *declared* one is what
			// this site has to say; a level has no reach to state at all.
			_ if n.dep_retains[k] && to.kind == Kind::Buffer => format!("⌸@{}", horizon(n.dep_reach[k])),
			_ if n.dep_retains[k] => format!("·{}", trim(to.name)),
			// a bare dep is the lane, and the lane is already drawn.
			_ => continue,
		});
	}
	if !edges.is_empty() {
		parts.push(edges.join(" "));
	}

	if n.kind != Kind::Root
		&& let Some(field) = n.field
	{
		parts.push(format!("→out {field}"));
	}
	if n.fidelity != Fidelity::Exact {
		parts.push(format!("{:?}", n.fidelity));
	}
	parts.join("  ")
}

/// One drawn line: the lanes and the node, and — where there is a node on it — the columns that
/// annotate it. Measured together, so the annotations line up down the page.
struct Row {
	left: String,
	right: Option<String>,
}

/// git-log-style lanes: a slot per producer with consumers still ahead of it, held open from the
/// tick it is stepped to the tick its last reader takes it.
fn render(shape: &Shape, under: Option<&Under<'_>>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
	let ns = shape.nodes;
	let state = under.map(|u| u.live());
	let mut remaining = vec![0usize; ns.len()];
	for n in ns {
		let mut seen: Vec<usize> = Vec::new();
		for &t in n.deps {
			if !seen.contains(&t) {
				seen.push(t);
				remaining[t] += 1;
			}
		}
	}

	let mut rows: Vec<Row> = Vec::new();
	let mut lanes: Vec<Option<usize>> = Vec::new();
	for i in 0..ns.len() {
		let n = &ns[i];
		// one lane per producer, however many dep positions name it.
		let mut uniq: Vec<usize> = Vec::new();
		for &t in n.deps {
			if !uniq.contains(&t) {
				uniq.push(t);
			}
		}
		let gating: Vec<bool> = uniq.iter().map(|&t| n.deps.iter().zip(n.dep_gates).any(|(&d, &g)| d == t && g)).collect();
		let lane_of: Vec<usize> = uniq
			.iter()
			.map(|&t| {
				lanes
					.iter()
					.position(|s| *s == Some(t))
					.expect("a producer holds its lane until its last consumer, and this is one")
			})
			.collect();
		let last: Vec<bool> = uniq.iter().map(|&t| remaining[t] == 1).collect();

		let pre = lanes.clone();
		for (k, &t) in uniq.iter().enumerate() {
			remaining[t] -= 1;
			if last[k] {
				lanes[lane_of[k]] = None;
			}
		}
		while lanes.last() == Some(&None) {
			lanes.pop();
		}
		let target = lanes.iter().position(|s| s.is_none()).unwrap_or(lanes.len());

		if !uniq.is_empty() {
			let (lo, hi) = (
				lane_of.iter().chain(core::iter::once(&target)).copied().min().expect("deps are not empty"),
				lane_of.iter().chain(core::iter::once(&target)).copied().max().expect("deps are not empty"),
			);
			let width = pre.len().max(target + 1);
			let mut line = String::new();
			for c in 0..width {
				line.push(match lane_of.iter().position(|&l| l == c) {
					// a control read is drawn dashed in the lane it leaves, so permission reads apart
					// from data without the shared horizontal run having to carry two kinds at once.
					// The corners mirror at the right end of the run, where the line turns the other way.
					Some(k) => match (gating[k], last[k], c == hi && hi > lo) {
						(true, true, _) => '╌',
						(true, false, _) => '┽',
						(false, true, false) => '╰',
						(false, true, true) => '╯',
						(false, false, false) => '├',
						(false, false, true) => '┤',
					},
					None if c == target => match (c == lo, c == hi) {
						(true, _) => '╭',
						(_, true) => '╮',
						_ => '┬',
					},
					None if c > lo && c < hi => match pre.get(c) {
						Some(Some(_)) => '┼',
						_ => '─',
					},
					None => match pre.get(c) {
						Some(Some(_)) => '│',
						_ => ' ',
					},
				});
				if c + 1 < width {
					line.push(if c >= lo && c < hi { '─' } else { ' ' });
				}
			}
			rows.push(Row {
				left: line.trim_end().to_owned(),
				right: None,
			});
		}

		let mut line = String::new();
		for c in 0..lanes.len().max(target + 1) {
			if c > 0 {
				line.push(' ');
			}
			line.push(match () {
				_ if c == target => ' ', // filled in below, so a two-glyph mark does not shift the lanes
				_ => match lanes.get(c) {
					Some(Some(_)) => '│',
					_ => ' ',
				},
			});
		}
		let mark = match (n.kind, state.as_ref().map(|live| live[i])) {
			(Kind::Root, _) => "╷".to_owned(),
			(_, None) | (_, Some(true)) => "●".to_owned(),
			// the sleep the graph permits, and the sleep only a replay pays for.
			(_, Some(false)) => match n.rewinds {
				true => "░⟲".to_owned(),
				false => "░".to_owned(),
			},
		};
		let at = line.char_indices().nth(target * 2).expect("the target lane was drawn").0;
		line.replace_range(at..at + 1, &mark);
		rows.push(Row {
			left: format!("{line} {}", trim(n.name)),
			right: Some(annotate(shape, i)),
		});

		if remaining[i] > 0 {
			match target == lanes.len() {
				true => lanes.push(Some(i)),
				false => lanes[target] = Some(i),
			}
		}
	}

	match under {
		None => writeln!(f, "graph {}", shape.graph)?,
		Some(u) => {
			let mut head = format!("graph {}  ·", shape.graph);
			for (g, open) in u.state {
				head.push_str(&format!("  {g}={}", if *open { "open" } else { "closed" }));
			}
			writeln!(f, "{head}")?;
		}
	}
	writeln!(f)?;

	let width = rows.iter().filter(|r| r.right.is_some()).map(|r| r.left.chars().count()).max().unwrap_or(0);
	for r in &rows {
		match &r.right {
			None => writeln!(f, "{}", r.left)?,
			Some(a) if a.is_empty() => writeln!(f, "{}", r.left)?,
			Some(a) => writeln!(f, "{:pad$}  {a}", r.left, pad = width + r.left.len() - r.left.chars().count())?,
		}
	}

	writeln!(f)?;
	write!(f, "legend  ╷root ●live ░dark ⟲needs-a-rewinding-past  ⊣gating ⟳folding ⌸buffering ·sampling")?;
	if let Some(live) = &state {
		let dark = live.iter().filter(|l| !**l).count();
		write!(f, "\n{dark} of {} dark", live.len())?;
	}
	Ok(())
}
