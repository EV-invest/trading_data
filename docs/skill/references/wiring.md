# Wiring — declaring, routing, driving

## `graph!`

```rust
trading_data::graph! {
    pub struct Graph;
    batches Batches;
    roots { trades: Trades[TradeCols], deltas: BookDeltas[BookDelta], anchors: BookAnchors[BookShape],
            oi: OiRoot[Oi], mc: McRoot[Mc] };
    out TickOut;
    outputs { bar: Bars<{ TF_1MIN }>, deprecator: Deprecator, rsi: Rsi<Bars<{ TF_5MIN }>, Knobs> }
}
```

Grammar, in order and all required: the struct, `batches <Ident>`, `roots { field: Cell[Event], .. }`
(≥1), `out <Ident>`, `outputs { field: Cell, .. }` (≥1 — a graph that produces nothing is built out of
nothing).

Every cell named here, and every cell they reach, must carry `#[node]` on its body impl. Otherwise
expansion fails with `cannot find macro __td_node_Foo`, naming the cell that is missing it.

`node_alias!` declares a `type` alias `graph!` can follow. Swapping the right-hand side reroutes every
graph that names it, and both spellings resolve to one field:

```rust
trading_data::node_alias! { pub Screener = StdScreener; }
```

It is for switching *which cell* is wired. Supplying a constant through it is banned
(`r[params.newtype.no-fixed-generics]`).

### What it generates

| item | |
|---|---|
| `struct Batches<'t>` | one field per root, of that root cell's `Out<'t>`. Deliberately **not** `Default` — every field is filled explicitly, and a silently-empty root is a footgun |
| `struct TickOut<'t>` | one field per output |
| `Graph::tick(&mut self, ts: i64, b: Batches) -> TickOut` | the sweep. `ts` is the tick's event time in ns — what a declared `CLOCK` reads against |
| `Graph::tick_obs(.., obs: &mut impl Observer)` | the same sweep, observed |
| `Graph::tick_rewind(.., past: &mut P)` / `tick_rewind_obs` | with a past anchored nodes may sleep behind; plain `tick` passes `Awake` |
| `Graph::NODES: &[&str]` | the derived closure, in sweep order — what the outputs actually cost |
| `Graph::FIDELITY: &[(&str, Fidelity)]` | every node the graph *declares* and how much of what it read its Jacobian covers. Count `Partial` and `Opaque` separately and pin both |
| `<Graph as Roots>::required_events()` | `TypeId`s of the events some node's dep tree actually reached — a declared root nothing reaches is never loaded |

Const-asserted at expansion: distinct node names · `cut_gated` · `deadlocked` ·
`clock_divides(CLOCK, CLOCKS)`.

## Routing — the whole layer

```rust
impl<'t> From<Lanes<'t>> for Batches<'t> {
    fn from(l: Lanes<'t>) -> Self {
        Self { trades: l.trades, deltas: l.deltas, anchors: l.anchor, oi: l.oi, mc: l.mc }
    }
}
```

There is no routing discriminant. Batch-ness needs no tag — it iterates. Every lane is present on
`Lanes`; ones that did not arrive are empty; the graph names the ones it takes.

`required_lanes::<Graph>()` maps the graph's `required_events()` to `LaneKind`s. It lives on the facade
because it is the one point that knows both an event's `TypeId` and its lane — the dag stays
storage-free and persistence cannot see graph types. An unknown event panics.

## Feeds

`persistence::sync` weaves the required lanes into one arrival-ordered stream of `Lanes`. The
backtest/live seam is **only** where events come from; node code is identical across the two.

### `Replay`

```rust
let lanes = required_lanes::<Graph>();
let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(), day_start, day_end,
                           &lanes, latency, ReadClock::from(Exact::from_nanos(60_000_000_000)));
let mut graph = Graph::default();
while let Some(l) = feed.next() {
    let ts_ns = l.ts_venue.as_nanos();
    let out = graph.tick(ts_ns, l.into());
}
```

`ReadClock` cuts arrival time into cells and hands the graph everything in one cell as a single step.
Cells are **absolute** — floored from the epoch, not from the last emission — so a replay of a range
groups identically every time. A venue message never splits. `ReadClock::EVENT` is a zero-length cell,
so "no batching" is the degenerate setting rather than a separate path.

Size the read clock for the question being asked, and read the answer as an estimate. Events inside
one cell reach the graph together, so the strategy never acts between two that arrived apart — that is
the approximation, it is deliberate, and **nothing should assert it away**.

`Replay` eager-loads its range; a window wider than memory is *chained* — successive `Replay`s over
one long-lived graph, which carries node state across.

### `Live`

```rust
let mut live = Live::new(catalog, ExchangeName::Bybit, symbol(), prec, /* record */ false, Arc::new(LiveClock));
let ts_sink = live.sink();
let bk_sink = live.sink();
let consumer = tokio::task::spawn_blocking(move || {
    let mut graph = Graph::default();
    while let Some(l) = live.next() {
        let ts_ns = l.ts_venue.as_nanos();
        graph.tick_obs(ts_ns, l.into(), &mut recorder.at(ts_ns));
    }
});
```

`Live` states no rate and **cannot be given one**: it weaves `ReadClock::ALL`, folding whatever is
buffered when it gets there. Same aggressive batching a backtest does, with the *waiting* removed — an
event is folded into a tick before the feed blocks on the next one (`r[feeds.live.on-arrival]`). No
clock boundary, no timer, no fill level, no waiting on a second lane.

`record: true` tees every event into the same Feather lanes a `Replay` later reads. An indefinite
session should record `false` — the catalog would grow without bound.

Consume on a blocking thread while async pumps feed the sinks. Memory stays bounded to the un-emitted
window however long the session runs.

## Observing

```rust
impl Observer for MyObs {
    fn want(&self, node: &'static str) -> Want { Want::Jac }
    fn on(&mut self, node: &'static str, deps: &'static [&'static str], gates: &'static [bool], fire: Fire<'_>) { .. }
}
```

`Want` is `Nothing < Vals < Jac < Exact`, asked **once per step, of the node about to run** — so only
the node being inspected pays, and there is no default. `()` is the erasing observer: `want()` returns
`Nothing` and the whole thing compiles away, so `tick_obs` over `()` *is* `tick`.

`deps` is `DepSet::NAMES` — the cell each dep names, wrapper stripped, which is also the frame slot it
is wired to. `gates` is positional with it, marking control edges. Roots report empty.

Step order **is** topo order, so the observed sequence doubles as the static topology: a dep name never
seen as a stepped node is a root.

`Fire` carries `ran` · `fires` · `vals` · `jac` · `exact` · `formula` · `deriv` · `trace` · `glance` ·
`dims` · `plots` · `clock`. A **level publishes only where its value moved** — same value as last tick
⇒ `fires: 0`, `vals: None`, no Jacobian (`r[outs.fired.on-change]`). `ran` is what separates that from
a skip. A run is untouched: `fires` is its element count, identical elements and all. Two observers compose as a tuple — an app's own assertions next to a viz
recorder are two readings of one sweep.

## Cost and parallelism

- One graph per unit. Parallelism is across **symbols** (live) or **episodes** (backtest), rayon
  across — never intra-tick.
- Universe/cross-sectional work is graph *composition*, not an execution tier: per-symbol graphs are
  values, and a universe-level graph ticks at bar cadence with its roots seeded from theirs.
- No polars execution tier in the engine, ever. Polars is offline research over the parquet and a
  property-test oracle (dev-dep only).
