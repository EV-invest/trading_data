# Deps — the edges

`model.typ` §1.3–§1.4 is the source. This is the mechanical residue: what to write, and what each
spelling costs.

## The five spellings, no sixth

```rust
type Deps = (Gating<Screener>, Buffering<Bars<TF>, Over<OVER>>, Sampling<Momentum>, Trades);
```

| spelling | what the body receives | resolves against | reach | who holds history | gates |
|---|---|---|---|---|---|
| `C` | `C::Out<'t>` — this tick's batch, nothing more | `C` | `Unit` | the producer, for one tick | no |
| `Gating<G: Gate>` | `bool` — permission, not data | `G` | `Unit` | — | **yes** |
| `Buffering<C: Series, R: Reach>` | `Hist<'t, Item>` (the series' own `Batch::View`) | `Buffer<C, K>` | `R::HORIZON` | **the engine** | no |
| `Sampling<C: Series>` | `Option<Item::Val>` — the last value whenever it came | `Latest<C>` | `Unit` | **the engine** (one) | no |
| `Folding<C, R: Reach>` | `C::Out<'t>` — plus a *claim* to fold the reach yourself | `C` | `R::HORIZON` | **the node** | no |

`Buffer<C, H>` and `Latest<C>` are off the facade — naming one in dep position is a `#[node]` error
telling you the wrapper to write instead. `Armed<E>` is the one engine-owned node you do name, because
it is the gate.

### Reach is a type in dep position, a value everywhere else

`Over<TF>` · `Elems<N>` · `Unbounded` are `Reach` impls carrying one `HORIZON` const. That is what
lets a node parameterised by a timeframe write `Folding<Trades, Over<TF>>` — a braced const argument
may not mention a generic parameter, but an associated const reads its impl's generics freely.

`Horizon` (`Unit` < `Elems(n)` < `Over(tf)` < `Unbounded`) is the value vocabulary the engine joins
and compares in. Totally ordered, so a graph can never ask for two reaches that cannot both be met.

- `Over(_) ⊒ Elems(_)` always; `Elems(_) ⊒ Over(_)` never — a count cannot promise a span.
- A `Buffer` is const-asserted bounded: `Unit` and `Unbounded` inside `Buffering` are compile errors.
- The buffer's capacity `K` is the join over *every* read of `C` in the graph. Nobody declares it, and
  no one dep site could.

### `Hist<'t, T>` — what a `Buffering` dep hands you

Past ++ fresh, cut to **your** declared horizon, so shortening some unrelated consumer's window
cannot change your results.

| call | view |
|---|---|
| `.fresh()` | byte-identical to the unbuffered series out |
| `.past()` / `.all()` | the cross-rate view, for a consumer clocked by a faster series |
| `.trailing()` | one window per fresh element — rate preservation for free |
| `.trailing_at(i)` / `.lagged_at(i, n)` | per-element, inside a `Scans`/`Closes` `read` |
| `.narrowed(h)` | a shallower view; asserts the retained reach serves it |

`Rows<Item>` — every row — is the default `Series::Batch`. It **slides**: trimmed by `ts_ns` each
tick, so `Over(tf)` is the last `tf` of wall clock ending now. A batch that *folds* (a book's levels
keep only the last qty per price) cannot un-fold, so it can only **tumble** — reset on the absolute
boundary, floored from the epoch. Same declaration, different window; the views are different types,
so no consumer can read one as the other.

## `Cell::CLOCK` — how often, declared on the node

```rust
impl<const TF: Timeframe> Cell for Volumes<TF> {
    type Out<'t> = &'t [Volume];
    const CLOCK: Option<Timeframe> = Some(TF);
    const NAME: &'static str = Self::TAG.as_str();
}
```

`None` (default) — publishes whenever its inputs do. `Some(tf)` — over elements whose `tf` period has
**closed**, never re-entered while one is in progress.

Declared by the node, enforced by whoever can: `Emitter::opens` withholds a tick only from a node
every one of whose deps is `RETAINED`, because a withheld tick on a pass-through dep is a batch
delivered to nobody.

`graph!` const-asserts `clock_divides` — every feeding rate must tile a declared one. That also pins a
period spelled twice: `Bars<TF>` names `TF` in its type and reads `Ohlcs<TF>`, so any `CLOCK` but
`Some(TF)` fails to build.

## Gating, demand, latches

A `Gating` dep is permission. A closed gate short-circuits before any plain dep is read — which is why
`Pull::open` const-asserts that every `Gating` dep **precedes** every plain one in the tuple.

Demand is the same edge read backwards, and it is a **formula**, not a set:

```
demand(i) = ⋁ over consumers c of ( demand(c) ∧ ⋀ gates(c) )
demand(i) = true   where i is an output, or pinned (own fold · a frame buffer · a latch · a gate)
```

You never write it. What a skip costs you is a *type*: a suppressed node is bounded by `Latent`, so an
out with no unfired reading cannot be skipped and says so at compile time (`Option<T> ⇒ None`,
`&[T] ⇒ &[]`).

You do not state the one exemption either. A **latch** is read from its standing bit at tick start, so
it darkens producers one tick *ahead* of the consumer arming, and only a node that survives that tick
may be darkened by it — read off `Deps` again, since a root's batch and an `Emit`'s buffer are this
tick's and no other's where a level node's out is state it keeps. A gate stepped earlier needs no
exemption: it is read off the frame on the same tick, co-extensively.

Consequence, not hidden: on the tick a latch arms, a node darkened by it is still dark and wakes the
tick after.

### `Episodic` — the closed form

A hand-written `Latch` leaves the loop open: nothing checks that the gate you armed is the gate your
episode cuts. `Episodic` closes it in the type — `Armed<Self>` is the only gate it can be.

```rust
#[node]
impl Episodic for Deprecator {
    type Trigger = Decision;
    fn arms<'t>(d: TriggerOut<'t, Self>) -> bool { d.is_some_and(|d| d.direction != Direction::Flat) }
}
// and the node itself gates on its own arm:
type Deps = (Gating<Armed<Deprecator>>, Decision, Sampling<Atr<{ TF_1MIN }>>, BookTop);
```

The episode ends when the `Cut` out reports `Episode::terminal`; `graph!` commutates and resets every
node gated on it to `Default` at the **next** tick's start. One episode at a time; triggers during one
are absorbed.

Two checks fire at build: `cut_gated` (the `Cut` must gate on the latch) and `deadlocked` (the arm must
not — a latch whose arm it darkens never re-arms).

## Anchoring — when a sleep outruns every bound

A node whose sleep outruns its deps' retention is `#[node(anchored)]`: it is replayed forward out of
the driver's recorded past on the tick it is demanded, so there is no window in which a wired node
reports absence.

Recoverability becomes the *feed's* claim rather than the graph's — the dag names the past by trait
(`Rewound`) and never by type, and a feed with none to seek (any live one) pins every anchored node
awake for free. Drive it with `tick_rewind`/`tick_rewind_obs`; plain `tick` passes `Awake`.

The win is upstream: a retention read only by anchored nodes sleeps with them, so an undemanded book
folds no delta anywhere.
