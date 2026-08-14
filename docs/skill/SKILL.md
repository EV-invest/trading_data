---
name: trading-data
description: Author and wire derivation graphs with the `trading_data` Rust framework — writing `#[node]` cells, spelling `type Deps`, declaring `graph!`, and driving it off `Replay` or `Live`. Use whenever a crate depends on `trading_data`, or a task mentions its primitives — `#[node]`, `graph!`, `Cell`, `Buffering`/`Folding`/`Sampling`/`Gating`, `Scans`/`Closes`/`Folds`/`Runs`/`Symbolic`/`Decides`/`Blind`, `Lanes`, `ReadClock`, `Replay`/`Live`.
---

# trading_data

A compile-time derivation DAG over market data. `type Deps` **is** the graph: a `graph!` names its
roots and its outputs, and the node set, the topological order, every buffer's size, every gate and
every demand are derived by walking `Deps` backwards. The whole sweep monomorphizes to one
straight-line function.

You depend on `trading_data` and nothing else. Naming a `trading_data_*` sub-crate is a boundary
violation (`r[boundaries.examples.facade-only]`); whatever you need is re-exported from the facade or
does not belong in your crate.

## Resolve the sources first

Every path below is repo-relative. Resolve them against, in order:

1. a local checkout of the workspace — `cargo metadata --format-version 1 | jq -r '.packages[]|select(.name=="trading_data").manifest_path'`, then the directory above `trading_data/`;
2. `https://github.com/EV-invest/trading_data/blob/main/<path>` (raw: `raw.githubusercontent.com/EV-invest/trading_data/main/<path>`).

| path | what it is | read it |
|---|---|---|
| `docs/ARCHITECTURE.md` | the two sentences everything rests on: reception order, and one graph over two feeds | **always, first** |
| `trading_data_dag/model.typ` | the framework itself — pipeline, step family, dep vocabulary, `Horizon`/`CLOCK`, every node kind, the out plane, every enforcement point | **always, before writing a node** |
| `docs/spec/` (`boundaries` `feeds` `kernels` `outs` `params` `rates`) | the normative requirements; `r[...]` ids are cited from code | before trading anything off |
| `trading_data/src/lib.rs` | the client's entire vocabulary — if a name is not in these `pub use` lists, you may not spell it | when an import will not resolve |
| `trading_data_persistence/weaver.typ` | the `Arrival` key, the read clock, the lanes, what the storage round-trip does and does not prove | when touching feeds or persistence |
| `examples/simple/src/nodes.rs` | one root, one RSI chain, plus `Folds`/`Runs`/`Symbolic` in ~250 lines | the fastest complete reading |
| `examples/spl/src/nodes/` | a whole strategy: `Scans`, `Decides` + `Gate`, `Blind`, `Runs`, `Episodic`, `Plot` | for a realistic node of each kind |
| `examples/live/src/main.rs` | a `Live` session: sinks, blocking consumer, ctrl-c | when wiring a live run |

Nothing in this skill restates those. It says which one answers your question, and gives the
mechanical shape the prose leaves implicit.

## What you actually author

Five lines of declaration, plus one impl block per node.

```rust
trading_data::graph! {
    pub struct Graph;
    batches Batches;                                    // generated root-slices struct
    roots { trades: Trades[TradeCols], oi: OiRoot[Oi] }; // cell : Root[Event]
    out TickOut;
    outputs { bar: Bars<{ TF_1MIN }>, sig: MySignal }    // what the graph is for
}
impl<'t> From<Lanes<'t>> for Batches<'t> {              // the whole routing layer
    fn from(l: Lanes<'t>) -> Self { Self { trades: l.trades, oi: l.oi } }
}
```

No node list, no order, no buffer size, no gate badge — all derived. A node no output reaches is not
instantiated, and neither is the source lane that would have fed it.

## The loop

1. **Read** `ARCHITECTURE.md` and `model.typ` §1.3–§1.5. Skipping this produces code that compiles
   into the wrong semantics.
2. **Pick the kernel** — the body trait *is* the choice, there is no attribute for it.
   See [references/nodes.md](references/nodes.md).
3. **Spell the deps** — five wrappers, no sixth; the axis they partition is *who holds the history*.
   See [references/deps.md](references/deps.md).
4. **Pay the out plane** — `Flat`, `Bump`, the `slice_nudge!`/`value_nudge!` witness, and whatever
   else the readings you want demand. [references/nodes.md](references/nodes.md#the-out-plane).
5. **Wire it** — `graph!`, `From<Lanes>`, a feed. [references/wiring.md](references/wiring.md).
6. **Build.** Most of this framework's rules are const-asserts and `#[node]` diagnostics; a build is
   the check. When one fires, [references/errors.md](references/errors.md) maps it to the fix.

## Rules that get broken

Each is enforced, but knowing it before you write saves a rewrite.

- **A dep read never says whether that dep produced this tick.** `None` means *nothing stands* and
  carries nothing else — never published, stopped publishing, and a shut gate are the same state,
  deliberately and permanently (`r[rates.deps.tick-opaque]`).
- **Absence is one thing.** `None`, the empty batch and an all-NaN flattening are one reading, and
  the meaning is taken: never attach a second one to it. Whether a node fired is a different axis
  entirely, the engine's own, and no dep read exposes it (`r[outs.absence.one-reading]`).
- **A level publishes only when it changes.** Observation only: same flattening as last tick ⇒
  `fires: 0` and `vals: None`, while the out a consumer reads off the frame stands as it always did
  (`r[outs.fired.on-change]`).
- **A node owns its rate.** `Cell::CLOCK` is declared on the cell, never derived from or overridable
  through `Deps`, and a clocked node sees only *closed* elements (`r[rates.node.declared]`,
  `r[rates.node.whole-elements]`).
- **`Gating` + `Folding` on one node is a compile error.** A closed gate pulls no deps, so a reach
  the node holds can never re-warm. Move it into the frame and read it as `Buffering`.
- **No fixed-generic aliases.** `type Change3m = Change<TF_1MIN, TF_3MIN>` is banned: the parameter
  is half the type's identity (`r[params.newtype.no-fixed-generics]`). `node_alias!` is for swapping
  *which cell* is wired, not for supplying a constant.
- **Drawing never motivates a node.** A step that computes nothing and differentiates to nothing
  stays out of the topology; name slot groups with `Plot` on the node that already exists.
- **A backtest is not a rerun of a live run.** Never assert the two agree event for event, and never
  tune until they do — the read clock's batching is a deliberate approximation.

## References

| file | covers |
|---|---|
| [references/deps.md](references/deps.md) | the five dep spellings, `Horizon`/`Reach`, `CLOCK`, gating · demand · latches · anchoring |
| [references/nodes.md](references/nodes.md) | the seven body traits with skeletons, `#[node]` flags, and the out-plane impls each out shape owes |
| [references/wiring.md](references/wiring.md) | `graph!` grammar, the generated API, `required_lanes`, `Replay`/`Live`, observers |
| [references/errors.md](references/errors.md) | enforcement point → what the message means → the fix |

## Verify

From the workspace root (`nix develop` first if there is a `flake.nix`):

```bash
cargo b                      # most rules are const-asserts; the build is the check
cargo t -p trading_data      # facade surface snapshot: a name enters or leaves by review
cargo r -p trading_data_simple -- --headless
```

`trading_data` is a **framework**. Judge a design by how directly it encodes the problem space in the
type system, not by how much it changes. Never treat an example's current output as correct — when
the engine changes, read the diff for regressions, not for reassurance.
