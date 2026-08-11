# Nodes — the seven bodies

`model.typ` §1.5 says what each kernel *is* and §1.6 what an out must be able to say about itself.
This is the shape you type.

The kernel set is sealed (`r[kernels.closed]`): **the body trait you implement is the choice of
kernel**, and there is no attribute spelling one you have no body for. That seal is what makes the
value, the formula, the exact Jacobian and the annotated trace all fall out of one declaration.

## Choosing

| your out | trait | kernel | it is |
|---|---|---|---|
| `f64` | `Symbolic` | `Pure` | a scalar expression over ≤16 **scalar** deps; exact gradient in one pass |
| `bool` | `Decides` | `Predicate` | a gate's verdict — the **leading dep drives**: one that fired nothing has decided nothing |
| any `Copy` | `Blind` | `Opaque` | the level hatch. `const WHY` required, counted into `FIDELITY` |
| `&'t [Item]` | `Scans` | `Scan` | one out element per **leading dep element**, carrying nothing between them. Exact unconditionally |
| `&'t [Item]` | `Closes` | `Close` | elements are whole `PERIOD`s, closed by the first driving element past a boundary. Rate-changing by construction |
| `&'t [Item]` | `Folds` | `Fold` | a recurrence: `step` says what the state becomes, `value` what the element then is |
| `&'t [Item]` | `Runs` | `Raw` | the run hatch. `const WHY` required |
| — | `Episodic` | — | not a body: it makes `Armed<Self>` the only gate this node can have |

Reach for a hatch last. `WHY` is the price, and it is a claim someone can disagree with — `Book`'s
"an order book fold is not a scalar function of its deltas" is one; a bare escape is not
(`r[kernels.opaque.stated]`). `graph!` exposes `Graph::FIDELITY`, and a graph may pin the `Partial`
and `Opaque` counts so neither rises without a diff that says why.

## Every cell owes this much

```rust
#[derive(Clone, Default)]                       // Default: graph! builds the frame; Clone: the FD witness
pub struct MyNode<const TF: Timeframe>;
impl<const TF: Timeframe> MyNode<TF> {
    const TAG: Tag = Tag::new("MyNode:", TF);   // .then(other_tf) / .count(n) compose
}
impl<const TF: Timeframe> Cell for MyNode<TF> {
    type Out<'t> = &'t [Option<f64>];
    const NAME: &'static str = Self::TAG.as_str();   // override only where the type carries parameters
    const CLOCK: Option<Timeframe> = Some(TF);       // omit unless the node declares a rate
}
```

`Cell::NAME` defaults to the Rust path, which is right for a type whose identity *is* its name. A
parameterised cell overrides it through `Tag`, because `type_name` renders parameters the compiler's
way and the rest of the graph spells them its own.

## Skeletons

### `Symbolic` — a scalar expression

```rust
impl Cell for Signal { type Out<'t> = f64; }
#[node]
impl Symbolic for Signal {
    type Deps = (Lambda<{ TF_1MIN }, 61>, RollingVolUsd<{ TF_1MIN }, 60>, Cvd);
    fn body(&self, v: Vars) -> impl Expr {
        let (lambda, vol, cvd) = (v.get::<0>(), v.get::<1>(), v.get::<2>());
        constant(1e6) * lambda + constant(1e-6) * (cvd - vol)
    }
}
value_nudge!(Signal);
```

Every dep must be scalar (const-asserted — a vector dep desyncs `Var<I>`), so `v.get::<I>()` is dep
`I`. The algebra is load-bearing: there is no other way to state the value, and that is what buys the
formula, the derivatives and the trace for free. `abs` `exp` `sqrt` `square` `powi_of` `min` `max`
`gt` `lt` `select` `sum` `constant` come off the facade.

### `Decides` — a gate

```rust
impl Cell for StdScreener { type Out<'t> = bool; }
#[node]
impl Decides for StdScreener {
    type Deps = (Bars<{ TF_1MIN }>, Sampling<Momentum<{ TF_5MIN }, 181>>);
    fn body(&self, v: Vars) -> impl Expr { gt(v.get::<5>(), constant(threshold)) }
}
impl Gate for StdScreener {}
value_nudge!(StdScreener);
```

`Vars` indexes the deps' **flattened slots** concatenated in `Deps` order — `Bars` occupies 0–4, so
the sampled momentum is 5. The leading dep drives: a tick that closed no bar has screened nothing,
and that is the kernel's reading of an empty run rather than a test inside the body. An unpublished
`Sampling` reads NaN, which compares false either way. Jacobian all-zero and exact: a predicate's
slope is zero off the boundary, and at it the step is not a slope any reading may claim.

### `Scans` — one out element per driving element

```rust
#[node]
impl<const TF: Timeframe, const OVER: Timeframe> Scans for Change<TF, OVER> {
    type Deps = (Buffering<Bars<TF>, Over<OVER>>,);

    fn read<W: Witness>((bars,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
        let (b, lag) = bars.lagged_at(i, 0)?;
        env.dep(0).lag(lag).put(b);                          // slots 0..=4 — Bar::DIMS
        match bars.trailing_at(i) {
            Some((w, lag)) => env.dep(0).lag(lag).put(&w[0].open),   // slot 5
            None => env.opaque().put(&f64::NAN),                     // slot 5, standing for no element
        }
        Some(b.ts_ns())
    }
    fn body(&self, v: Vars) -> impl Slots {
        let (close, base_open) = (v.get::<3>(), v.get::<5>());
        select(gt(base_open, constant(0.0)), (close - base_open) / base_open * constant(100.0), constant(f64::NAN))
    }
}
slice_nudge!([const TF: Timeframe, const OVER: Timeframe] Change<TF, OVER>, Option<f64>);
```

Slots are **appended** in `read` order. Each names the dep and the lag it was copied off
(`env.dep(0).lag(n)`) or stands for no element at all (`env.opaque()`). Leading with the deps' own
elements at lag 0 lines the gradient up with the Jacobian's columns; the lags are what let `exact`
scatter that same gradient over the whole reach in one pass.

- `read -> None` is absence **arriving** — answered without evaluating anything.
- `NaN` out of the body is the body **declining**.
- Which elements you read is Rust, and must not depend on anything being differentiated: index by
  count or by timestamp, never by a value (`r[kernels.selection.index-is-not-a-variable]`).

### `Closes` — elements are whole periods

```rust
#[node]
impl<const TF: Timeframe> Closes for Ohlcs<TF> {
    type Deps = (Folding<Trades, Over<TF>>,);
    const PERIOD: Timeframe = TF;
    fn read<W: Witness>((trades,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> { .. }
    fn open(&self, v: Vars) -> impl Slots { let p = v.get::<0>(); (p, p, p, p) }
    fn fold(&self, v: Vars) -> impl Slots {
        let (price, open, high, low) = (v.get::<0>(), v.get::<2>(), v.get::<3>(), v.get::<4>());
        (open, max(high, price), min(low, price), price)
    }
    fn pending(&self) -> &Pending { &self.0 }
    fn pending_mut(&mut self) -> &mut Pending { &mut self.0 }
}
```

The kernel owns the walk, the floor-to-period and the close time — a timestamp is not a slot, so it
never could be the body's. The body owns the numbers: what a first element opens with, what a further
one folds in. The accumulator's slots follow the element's, so both read the element at one set of
indices. Permanently `Partial`: the rest of the period reached the reported element only through the
accumulator, and those elements live in the dep's *declaration* (`Folding<Trades, Over<TF>>`), not in
its out, so there is no lag to index them at.

### `Folds` — a recurrence

```rust
#[derive(Clone, Default)] pub struct Cvd(Carried);
impl Cell for Cvd { type Out<'t> = &'t [f64]; }
#[node]
impl Folds for Cvd {
    type Deps = (Trades,);
    const EXTRA: usize = 1;      // slots put by env.opaque()
    const STATE: usize = 1;      // slots the recurrence carries

    fn read<W: Witness>((trades,): &DepOuts<'_, Self>, i: usize, env: &mut Env<'_, W>) -> Option<i64> {
        let (exec, lag) = trades.exec().at(i)?;
        env.dep(0).lag(lag).put(&[price, qty]);      // slots 0,1
        env.opaque().put(&signed(trades.side[i], 1.0));  // slot 2 — the EXTRA
        Some(exec.as_nanos())
    }
    fn step(&self, v: Vars) -> impl Slots {           // slot 3 is the state, after the element's
        let (price, qty, side, sum) = (v.get::<0>(), v.get::<1>(), v.get::<2>(), v.get::<3>());
        sum + side * (price * qty)
    }
    fn value(&self, v: Vars) -> impl Slots { v.get::<3>() }
    fn carried(&self) -> &Carried { &self.0 }
    fn carried_mut(&mut self) -> &mut Carried { &mut self.0 }
}
slice_nudge!(Cvd, f64);
```

Both bodies read one env whose state slots are the state **after** the element. Declining leaves the
state where it stood — an average is not advanced by an absence. Permanently
`Partial("state history, by design")`: the state is no dep, has no reach to be indexed at, and a
derivative carrying accumulated state sensitivity is a different quantity.

A fold or recurrence sees every element of what it folds **exactly once, in order**
(`r[rates.folds.exactly-once]`). That is the one thing permitted to depend on what arrived, and it is
a statement about the element sequence, identical under every grouping.

### `Runs` — the run hatch

```rust
#[node]
impl Runs for BookTop {
    type Deps = (Book,);
    const WHY: &'static str = "reading the top of a book is a lookup into a fold, not an expression over it";
    fn emit(&mut self, (book,): DepOuts<'_, Self>, out: &mut Vec<Option<BookTopSnap>>) { .. }
}
slice_nudge!(BookTop, Option<BookTopSnap>);
```

The **engine** owns the run: your struct holds only what it remembers between ticks, and `emit`
cannot read what it wrote last tick. `&mut self`, not `&'t mut self` — only the buffer is lent.

### `Blind` — the level hatch

```rust
#[node]
impl Blind for Decision {
    type Deps = (Classify,);
    const WHY: &'static str = "a direction is a sign and a size is a bucket — neither varies smoothly in what produced it";
    fn advance<'t>(&'t mut self, (c,): DepOuts<'t, Self>) -> Self::Out<'t> { c.map(Decided::from) }
}
value_nudge!(Decision);
```

Self-borrows, so a node lends its own buffer for the whole tick.

## `#[node]` flags

`#[node]` publishes the impl's `type Deps` to `graph!`, which cannot ask the type system for it, and
writes the `Node`/`Emit` impl mapping the body trait to its kernel. It also writes a `__td_node_<Cell>`
shim at your crate root — a graph naming the cell reaches it under the same path, so a cell and its
shim are exported together.

| flag | means |
|---|---|
| `#[node]` | the default |
| `#[node(latch)]` | this cell has a hand-written `impl Latch` (a separate impl) |
| `#[node(anchored)]` | replay this node forward out of the driver's past on the tick it is demanded |

One shim per cell. Put `#[node]` on the body impl (`Blind`/`Runs`/`Symbolic`/`Decides`/`Scans`/
`Closes`/`Folds`) and on `impl Episodic` (which publishes `Armed<Self>`'s dep instead).

## The out plane

What your out type owes, by what you want out of it.

| trait | owed when | note |
|---|---|---|
| `Nudge` | **always**, via `slice_nudge!(C, Item)` or `value_nudge!(C)` | `slice_nudge!` also writes the `Series` impl; a third argument names a `Batch` other than `Rows` |
| `Flat` | the item is ever observed | `DIMS` must occupy **≥1 slot** (`r[outs.flat.nonempty]`); NaN per slot = no value there |
| `Bump` | same | return the step *actually* taken; a discrete slot returns `0.0` and leaves its column NaN rather than fabricating a zero. An item no slot of which can move is a two-line macro of your own — `structural_bump!` in `examples/spl/src/nodes.rs` is the pattern, not a facade export |
| `Unflat` | the item comes out of `Scans`/`Closes`/`Folds` | rebuilt from computed slots + the event time. Blanket for `f64`, `[f64; N]`, `Option<T>` |
| `Stamped` | the series is ever `Buffering`-ed | a history you cannot index by time is one you can only read at an assumed cadence |
| `Present` | the series is ever `Sampling`-ed | `always_present!(Item)` where every element carries something. Blanket for `Option<T>` |
| `Episode` | the item is a latch's `Cut` out | `&[T]` is terminal if **any** element is — the boundary was crossed somewhere in the run |
| `Glance` | you want it on a card | the one compact line; display-dual of `Flat` |
| `Plot` | you want it drawn | `const PLOTS` on the node. Slot groups with their own scale and guides — axes partition by **unit** |

Generic cells write their parameters in leading brackets:
`slice_nudge!([B: Series<Item = Bar>] RsiDelta<B>, f64)`.

`Flat` fires by having slots, not by a flag — which is why `LEN ≥ 1` is const-asserted at every
observed node. `None`, the empty batch and an all-NaN flattening are one fact: **did not fire**.

## Two Jacobians, never conflated

`Fire::jac` is the **one-step** reading: each dep's *last* element perturbed, prior state held fixed.
Differentiating and finite-differencing land on the same number, so `Fire::exact` says only how it was
reached. A node reading `.trailing()` over 181 bars has one column describing bar 180 and silence
about 0–179.

`Fire::exact_block` (`Want::Exact`) is the second quantity, asked for separately because it costs
separately: one column group per **lag** of a dep's reach, oldest first. A kernel that cannot fill it
says so rather than filling a block it cannot stand behind (`r[kernels.jac.two-quantities]`).

Observing is non-invasive by rule: a tick's outs are bit-identical at `Want::Jac` and `Want::Nothing`
(`r[observe.noninvasive]`). The FD witness re-advances a *clone* restored from the pre-advance state,
never the node.
