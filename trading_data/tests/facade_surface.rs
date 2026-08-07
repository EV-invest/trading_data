//! The facade's `pub use` lists are the client's whole vocabulary, so they are read here as data:
//! once for the two pairing invariants that hold today only by hand, and once as a snapshot, so
//! adding or dropping a name is a reviewed diff rather than a side effect.

use std::{
	collections::BTreeSet,
	fs,
	path::{Path, PathBuf},
};

use syn::{ImplItem, Item, Type, UseTree, Visibility};
// the compile-time half of the shim pairing: an unpaired name here is an unresolved import.
#[expect(unused_imports, reason = "naming both halves of each pair is what this asserts")]
use trading_data::{
	__td_node_AvgGain, __td_node_AvgLoss, __td_node_Bars, __td_node_Book, __td_node_Ohlcs, __td_node_Rsi, __td_node_RsiDelta, __td_node_Volumes, AvgGain, AvgLoss, Bars, Book, Ohlcs, Rsi,
	RsiDelta, Volumes,
};

/// The same pairs as data, checked against the facade below — so a pair added there cannot skip the
/// import that proves it has both halves.
const PAIRED: [&str; 8] = ["AvgGain", "AvgLoss", "Bars", "Book", "Ohlcs", "Rsi", "RsiDelta", "Volumes"];

fn root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn parse(path: &Path) -> syn::File {
	syn::parse_file(&fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every name the facade re-exports, as `(crate, name)` — a name that moved crates is a different
/// vocabulary entry even though it reads the same.
fn exports() -> BTreeSet<(String, String)> {
	fn walk(tree: &UseTree, from: &str, out: &mut BTreeSet<(String, String)>) {
		match tree {
			// the first segment is the crate the name is reached through; deeper ones are its modules.
			UseTree::Path(p) => {
				let head = p.ident.to_string();
				walk(&p.tree, if from.is_empty() { &head } else { from }, out);
			}
			UseTree::Group(g) => g.items.iter().for_each(|t| walk(t, from, out)),
			// a leaf reached with no path before it *is* the crate — `pub use trading_data_dag as __dag`.
			UseTree::Name(n) => {
				out.insert((if from.is_empty() { n.ident.to_string() } else { from.to_owned() }, n.ident.to_string()));
			}
			UseTree::Rename(r) => {
				out.insert((if from.is_empty() { r.ident.to_string() } else { from.to_owned() }, r.rename.to_string()));
			}
			UseTree::Glob(_) => panic!("a glob re-export is not a vocabulary: name what the facade exports"),
		}
	}

	let mut out = BTreeSet::new();
	for item in parse(&root().join("trading_data/src/lib.rs")).items {
		let Item::Use(u) = item else { continue };
		if matches!(u.vis, Visibility::Public(_)) {
			walk(&u.tree, "", &mut out);
		}
	}
	assert!(!out.is_empty(), "no `pub use` in the facade — this is reading the wrong file");
	out
}

/// r[verify boundaries.facade.shim-pairing]
///
/// A cell is named in `outputs`/`Deps` by its type, and `graph!` answers by invoking the shim under
/// the same path. Export one without the other and the graph fails on a macro nobody wrote.
#[test]
fn a_cell_and_its_shim_are_exported_together() {
	let names: BTreeSet<String> = exports().into_iter().map(|(_, n)| n).collect();

	let shimmed: BTreeSet<String> = names.iter().filter_map(|n| n.strip_prefix("__td_node_")).map(str::to_owned).collect();
	for cell in &shimmed {
		assert!(
			names.contains(cell),
			"`__td_node_{cell}` is exported but `{cell}` is not: a graph could not name the cell its shim answers for"
		);
	}
	assert_eq!(
		shimmed,
		PAIRED.iter().map(|s| (*s).to_owned()).collect(),
		"the facade's cell/shim pairs and the `use` above have drifted; the `use` is what checks each pair has both halves"
	);
}

/// Every type this workspace declares `pub`, wherever it is declared — the facade re-exports across
/// crates, so what counts as "one of ours" spans them.
fn declared(dirs: &[&str]) -> BTreeSet<String> {
	fn named(items: &[Item], out: &mut BTreeSet<String>) {
		for item in items {
			let (vis, name) = match item {
				Item::Struct(i) => (&i.vis, i.ident.to_string()),
				Item::Enum(i) => (&i.vis, i.ident.to_string()),
				Item::Trait(i) => (&i.vis, i.ident.to_string()),
				Item::Type(i) => (&i.vis, i.ident.to_string()),
				Item::Union(i) => (&i.vis, i.ident.to_string()),
				Item::Mod(m) => {
					if let Some((_, inner)) = &m.content {
						named(inner, out);
					}
					continue;
				}
				_ => continue,
			};
			if matches!(vis, Visibility::Public(_)) {
				out.insert(name);
			}
		}
	}

	let mut out = BTreeSet::new();
	for f in sources(dirs) {
		named(&parse(&f).items, &mut out);
	}
	out
}

fn sources(dirs: &[&str]) -> Vec<PathBuf> {
	fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
		for e in fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
			let p = e.expect("a readable entry").path();
			match p.is_dir() {
				true => walk(&p, out),
				false if p.extension().is_some_and(|x| x == "rs") => out.push(p),
				false => (),
			}
		}
	}

	let mut out = Vec::new();
	for d in dirs {
		let p = root().join(d);
		match p.is_dir() {
			true => walk(&p, &mut out),
			false => out.push(p),
		}
	}
	assert!(!out.is_empty(), "no sources under {dirs:?}");
	out
}

/// Every type named anywhere inside `ty`, head and arguments alike.
fn mentions(ty: &Type, out: &mut Vec<String>) {
	struct V<'a>(&'a mut Vec<String>);
	impl syn::visit::Visit<'_> for V<'_> {
		fn visit_path(&mut self, p: &syn::Path) {
			self.0.extend(p.segments.iter().map(|s| s.ident.to_string()));
			syn::visit::visit_path(self, p);
		}
	}
	syn::visit::Visit::visit_type(&mut V(out), ty);
}

/// r[verify boundaries.facade.signature-closure]
///
/// What a client has to *spell* to call an exported item: an argument it constructs and a field it
/// fills. `private_interfaces` says this within a crate; across the facade's re-export boundary
/// nothing does, which is what holds `BatchTrades` in without an example naming it.
///
/// Returns are deliberately out of scope — an algebra combinator returns `Ex<Abs<_>>` and the body
/// that called it returns `impl Slots`, so the concrete type is threaded and never written.
#[test]
fn a_type_a_client_must_spell_is_exported() {
	// `trading_data/src/lib.rs`, not the crate: `bench` is reached by module path rather than through
	// the flat vocabulary, so its own types are nameable without being re-exported.
	const CRATES: [&str; 6] = [
		"trading_data/src/lib.rs",
		"trading_data_core/src",
		"trading_data_dag/src",
		"trading_data_derivatives/src",
		"trading_data_expr/src",
		"trading_data_persistence/src",
	];
	let exported: BTreeSet<String> = exports().into_iter().map(|(_, n)| n).collect();
	let ours = declared(&CRATES);

	let mut reached: Vec<(String, String)> = Vec::new();
	let note = |owner: &str, ty: &Type, reached: &mut Vec<(String, String)>| {
		let mut names = Vec::new();
		mentions(ty, &mut names);
		reached.extend(names.into_iter().map(|n| (owner.to_owned(), n)));
	};
	for f in sources(&CRATES) {
		for item in parse(&f).items {
			match item {
				Item::Impl(i) if i.trait_.is_none() => {
					let mut head = Vec::new();
					mentions(&i.self_ty, &mut head);
					let Some(owner) = head.first().filter(|h| exported.contains(*h)).cloned() else { continue };
					for m in &i.items {
						let ImplItem::Fn(m) = m else { continue };
						if matches!(m.vis, Visibility::Public(_)) {
							m.sig.inputs.iter().for_each(|a| {
								if let syn::FnArg::Typed(t) = a {
									note(&owner, &t.ty, &mut reached)
								}
							});
						}
					}
				}
				Item::Struct(s) if matches!(s.vis, Visibility::Public(_)) && exported.contains(&s.ident.to_string()) => {
					let owner = s.ident.to_string();
					for fl in &s.fields {
						if matches!(fl.vis, Visibility::Public(_)) {
							note(&owner, &fl.ty, &mut reached);
						}
					}
				}
				Item::Fn(fun) if matches!(fun.vis, Visibility::Public(_)) && exported.contains(&fun.sig.ident.to_string()) => {
					let owner = fun.sig.ident.to_string();
					fun.sig.inputs.iter().for_each(|a| {
						if let syn::FnArg::Typed(a) = a {
							note(&owner, &a.ty, &mut reached)
						}
					});
				}
				_ => (),
			}
		}
	}

	let missing: BTreeSet<(String, String)> = reached.into_iter().filter(|(_, n)| ours.contains(n) && !exported.contains(n)).collect();
	assert!(
		missing.is_empty(),
		"a client reaching only through the facade cannot name these, yet must to call what does:\n{}",
		missing.iter().map(|(o, n)| format!("  {o} names {n}")).collect::<Vec<_>>().join("\n")
	);
}

/// The vocabulary itself. A name is added or dropped by review, not by a `pub use` that happened to
/// compile — which is the whole point of having pinned it.
#[test]
fn the_facade_exports_exactly_this() {
	insta::assert_snapshot!(exports().iter().map(|(c, n)| format!("{c}::{n}")).collect::<Vec<_>>().join("\n"));
}
