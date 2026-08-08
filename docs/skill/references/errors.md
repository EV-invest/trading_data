# Errors — enforcement point → fix

Most of this framework's rules are const-asserts, `#[diagnostic::on_unimplemented]` notes and
`#[node]`/`graph!` diagnostics. **A build is the check.** Nearly every message below already tells you
the fix; this table adds *why* it exists, so you don't work around it.

`model.typ` §1.7 lists the enforcement points in full.

## `#[node]` / `graph!` expansion

| message | why | fix |
|---|---|---|
| `cannot find macro __td_node_Foo` | `graph!` reads `type Deps` off the shim `#[node]` writes; the type system cannot be asked | put `#[node]` on `Foo`'s body impl, and export the cell and its shim together |
| ``impl Node`/`impl Emit` is written by `#[node]`, not by hand`` | the kernel set is sealed — there must be no body the engine cannot also read | write a body trait: `Symbolic` `Decides` `Blind` / `Scans` `Closes` `Folds` `Runs` |
| ``Buffer<C, H>`/`Latest<C>` is the frame cell `graph!` grows for you, not a dep spelling`` | `K` is the join over every read of `C` in the graph — no one dep site holds it | write `Buffering<C, R>` / `Sampling<C>` |
| ``a `Buffering` dep names the reach the engine retains for it`` | | `Over<TF>` · `Elems<N>` — the reach **this** node reads |
| ``an associated type names no cell `graph!` can ask about`` | expansion is textual: it walks tokens, not the type system | write the concrete cell |
| ``a `node_alias!` names a cell, not a dep-position wrapper`` | | alias the cell; each `type Deps` says how far back *it* reads |
| `dep cycle: A → B → A` | a graph is a dag | if the back edge means the *previous* tick, that is state the node keeps itself, not a dep |
| `<field> gates its own arm` (`deadlocked`) | the arm is dark exactly while the latch is down, so it can never re-arm | drop the `Gating<Armed<Self>>` from the arm's deps |
| `cut_gated` | the `Cut` must gate on the latch, or the episode cannot end | gate the `Cut` node on the latch |
| ``<field> declares a `Cell::CLOCK` no whole number of its inputs' periods tiles`` | a node observing fractions of its inputs' elements is neither rate | fix `CLOCK`, or the producer's |
| ``<node> is `#[node(anchored)]` and folds <dep> itself`` | a `Folding` reach is the node's own state; a rewind re-reading it would fold it twice | `Buffering<dep, R>` — the reach becomes the frame's |
| `outputs` empty / `roots` empty | a graph that produces nothing is built out of nothing | name at least one of each |
| ``add `trading_data` to this crate's `[dependencies]``` | generated code reaches the dag through the invoking crate's own path | depend on the facade — and **only** the facade |

## Trait bounds you have not paid

| message | fix |
|---|---|
| ``{Self}` is not a cell`` | `impl Cell` with `type Out<'t> = &'t [Item]` for a run, or an owned `Copy` value for a level |
| ``{Self}` is no dep set`` | `type Deps` is a tuple, wrappers and all. A single dep still needs its comma: `(Trades,)` |
| ``{Self}` names no reach`` | `Over<TF>` · `Elems<N>` · `Unbounded` |
| ``{Self}` has no finite-difference witness`` | `slice_nudge!(C, Item)` (run) or `value_nudge!(C)` (level); generics in leading brackets |
| ``{Self}` cannot be read as a flat element`` | `impl Flat`: state `DIMS`, write every slot, return whether it fired |
| ``{Self}` cannot be perturbed`` | `impl Bump`. A slot that cannot move returns `(self, 0.0)` — its column stays NaN, never a fabricated zero |
| ``{Self}` cannot be rebuilt from the slots a kernel computed`` | `impl Unflat` — a per-element kernel writes `f64` slots and the run is made of items |
| ``{Self}` carries no event time`` | `impl Stamped`: the default `Rows` retention trims by `ts_ns` every tick |
| ``can't compare `X` with `X` ``, required by a bound in `Latest<C>` | derive `PartialEq` on the sampled item's `Val` — a level publishes on change, so it has to compare |
| ``{Self}` has no one-line reading`` | `impl Glance` — every stepped node is drawable |
| ``{Self}` is not a gate`` | `impl Gate for {Self} {}` — and its out must be `bool` |
| ``{Self}` says nothing about an episode ending`` | `impl Episode` — a latch commutates on its `Cut`'s terminal out |
| ``{Self}` is not declared a run of items`` | `slice_nudge!` writes the `Series` impl; a non-`Rows` retention names its `Batch` as the third argument |

### ``{Self}` has no unfired reading`` (no `Latent` impl)

The one bound the demand pass puts back on you. A node every reader of which sits behind one gate is
skipped while that gate is false, so it must be able to decline. Make its out `Option`-valued (or a
slice — the empty batch is the latent reading), or give it a consumer that is not gated.

This is deliberate: a skip costs a **type**, checked at compile time, rather than a runtime badge.

## Const-asserts

| assert | means |
|---|---|
| `gating_leads(GATES)` | every `Gating` dep must **precede** every plain one in the tuple — `open` reads left to right, so a closed gate short-circuits before a plain dep is pulled |
| `!any(GATES) \|\| !any(FOLDS)` | a gated node cannot hold its own reach: a closed gate pulls no deps, so a `Folding` dep never re-warms. Retain it in the frame instead |
| `K.serves(H)` | the buffer's joined reach must cover this read, and `H` must be neither `Unit` nor `Unbounded` |
| `O::LEN > 0` | a zero-slot out would fire and leave the buffer byte-identical to an unfired one — the one way absence could mean two things |
| `Plot::coherent` | a multi-plot node must name each plot's slots |
| `MAX_VARS` / scalar deps | `Symbolic` takes ≤16 deps and every one must be scalar — a vector dep desyncs `Var<I>` |
| `no frame cell answers for {N}` | the frame holds what the walk derived from `outputs`; a cell nothing reaches is never stepped |

## Ambiguity

Two instances of one node type in a frame make every read of it ambiguous — `Has<'t, N, I>` resolves a
cell **by type**, and `I` is inferred and never named. Same failure for two `Buffer<C, _>`: every
`Buffering<C, _>` becomes ambiguous.

If you wanted two, they are two *types*: write the parameter out (`Bars<TF_1MIN>` and `Bars<TF_1H>` are
two series, not one series configured twice).

## What is *not* checked

- That the numbers are right. Drive `examples/simple` and read its counts.
- That a backtest reproduces a live run. It does not, on purpose, and nothing should assert it does.
- That a comment is current. `ARCHITECTURE.md` and the specs are normative; a comment is not.
