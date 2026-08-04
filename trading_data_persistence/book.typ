#set page(width: 297mm, height: auto, margin: 14mm)
#set text(size: 9.5pt)
#set par(justify: false)
#show raw.where(block: true): it => block(
  fill: luma(248),
  inset: 8pt,
  radius: 3pt,
  width: 100%,
  text(size: 7.6pt, it),
)
#import "@preview/fletcher:0.5.8" as fletcher: diagram, edge, node

= The book — primitives and data flow

Read off `trading_data_core/src/{book.rs, cols.rs, cells.rs, precision.rs, ts.rs}`,
`trading_data_persistence/src/{sync/mod.rs, feather.rs, row.rs, read.rs}`, and the consumers in
`examples/spl/src/nodes/book_top.rs` and `examples/live/src/nodes.rs`.

Companion to `trading_data_dag/model.typ` (what happens to a tick) and `weaver.typ` (where a tick
comes from). This one covers the book lanes.

== 1. Overview

```
  Three types carry the book, and they split along write / read:

     ShadowBook   consumes venue updates, emits our delta frames, mints checkpoints.  Write side.
     Book         folds a checkpoint and a stream of delta frames into levels.        Both sides.
     BookShape    the level map. Used as the venue's message, as a checkpoint, and as
                  the shape a stored snapshot row decodes to.

  ShadowBook owns a Book and calls `apply` on it with each frame it emits, so the levels the
  recorder holds are the levels a replay reconstructs from the same frames.

  Venue snapshots are consumed; they do not reach disk. What is written is the frame stream ShadowBook
  produced, whose `monotonic_seq` it assigns by increment, plus checkpoints on a 60 s cadence.
```

=== 1.1 Shapes

```rust
// ── venue side ───────────────────────────────────────────────────────────────────────────────
enum BookUpdate {                            // what an adapter hands us
    Snapshot(BookShape),
    BatchDelta { shape: BookShape, gapped: bool },   // gapped: the venue's seq chain broke
}

struct BookShape {                           // venue message, checkpoint, decoded snapshot row
    ts:   Aggregate,                         // both `first`s: start of the accumulation epoch
    prec: PrecisionPriceQty,                 // one value for every level in the map
    asks: BTreeMap<i32, u32>,                // ascending
    bids: BTreeMap<i32, u32>,                // ascending
}

struct Aggregate { venue_exec: Span<Venue>, local_recv: Span<Local> }
struct Span<A>   { first: Ts<A>, last: Ts<A> }
struct PrecisionPriceQty { price: Precision, qty: Precision }    // Precision(i8): raw = v × 10^p

// ── write side ───────────────────────────────────────────────────────────────────────────────
struct ShadowBook {
    book:  Book,                   // folded with every frame this instance emits
    out:   DeltaBuf,               // the emitted frame borrows from it
    seq:   u64,                    // our own chain, incremented once per emitted level
    cadence: Exact,                // CHECKPOINT_CADENCE = 60 s (sync/mod.rs)
    epoch_start:     Option<Ts<Local>>,     // set on seed; `Some` while the book is synced
    last_checkpoint: Option<Ts<Local>>,     // cleared on seed
}
fn ingest(&mut self, u: &BookUpdate, recv: Ts<Local>) -> Option<DeltaFrame<'_>>;
fn checkpoint(&mut self, recv: Ts<Local>) -> Option<BookShape>;

// ── the fold ─────────────────────────────────────────────────────────────────────────────────
struct Book {
    prec:  PrecisionPriceQty,
    bids:  Vec<(i32, u32)>,        // descending  ] both best-first, index 0 = top of book
    asks:  Vec<(i32, u32)>,        // ascending   ]
    epoch: u64,                    // incremented by every resync
    synced: bool,
    seq:   Option<u64>,            // last folded monotonic_seq; `None` after a resync
    span:  Span<Venue>,
}
fn step(&mut self, anchor: Option<&BookShape>, frame: DeltaFrame<'_>) -> bool;   // the only pub verb

// ── the delta carrier ────────────────────────────────────────────────────────────────────────
enum DeltaFrame<'a> { Update(DeltaCols<'a>), Correction(DeltaCols<'a>) }
enum FrameKind      { Update, Correction }        // the stored u8 discriminant

struct DeltaCols<'a> {                            // borrowed columnar view; what a node reads
    prec: PrecisionPriceQty,
    ts:   RelayCols<'a, Venue, Local>,            // { exec: Option<&[Ts]>, send, recv: Option<Span> }
    monotonic_seq: &'a [u64],
    side: &'a [Side], price: &'a [i32], qty: &'a [u32],       // qty == 0 deletes the level
}
struct DeltaBuf {                                 // owned twin; serves as lane buffer, Nudge
    prec: PrecisionPriceQty,                      // scratch, and ShadowBook's emit accumulator
    exec: Vec<Ts<Venue>>, recv: Vec<Option<Ts<Local>>>,
    monotonic_seq: Vec<u64>, kind: Vec<FrameKind>,
    side: Vec<Side>, price: Vec<i32>, qty: Vec<u32>,
}

// ── graph cells (cells.rs; orphan rules put these impls in that crate) ───────────────────────
struct BookAnchors;   // Cell::Out = Option<&BookShape>   Nudge::Scratch = Option<BookShape>
struct BookDeltas;    // Cell::Out = DeltaFrame<'_>       Nudge::Scratch = DeltaBuf
impl  Cell for Book { type Out<'t> = Option<&'t Book>; }   // Option ⇒ Latent
impl  Node for Book { type Deps = (BookAnchors, Folding<BookDeltas, {Horizon::Span(TF_15MIN)}>); }

// ── disk rows ────────────────────────────────────────────────────────────────────────────────
pub struct BookDelta {                       // one row per level   ·   256 MB / 1 h rotation
    ts_venue_exec: Ts<Venue>,
    ts_local_recv: Ts<Local>,                // book lanes are written only by a live recording
    monotonic_seq: u64,
    kind: FrameKind,                         // ours; the venue's `gapped` is folded into it
    side: Side, price: i32, qty: u32,
}
pub(crate) struct BookSnapshot {             // one row per checkpoint  ·  64 MB / 6 h rotation
    ts_venue_exec: Ts<Venue>, ts_local_recv: Ts<Local>,
    monotonic_seq: u64,                      // written 0; a checkpoint carries no element sequence
    bid_prices: Vec<i32>, bid_qtys: Vec<u32>,
    ask_prices: Vec<i32>, ask_qtys: Vec<u32>,
}
```

=== 1.2 Ownership and conversions

```
  ShadowBook ─owns─▶ Book        folded with each emitted frame
             ─owns─▶ DeltaBuf    the emitted frame borrows it; cleared at the top of each ingest

  BookShape  ◀─shape()── Book               used to emit a checkpoint
  BookShape  ──resync()─▶ Book              used to seed the fold
  BookShape  ◀─snapshot_shape()── BookSnapshot        precision comes from the file's metadata

  DeltaFrame ◀─frame()── DeltaBuf           borrowed view over the owned twin
  DeltaFrame ──extend()─▶ Feather<BookDelta>          one row per level
  DeltaFrame ──apply()──▶ Book              both kinds fold the same way
```

== 2. Write path

```
  venue WS
     │  Sink::push   (clock-free, one send per message)
     ▼
 LiveEvt::Book(BookUpdate) ─────▶ Live::ingest_book(u, ts)          single-threaded
                                        │  recv = recv_of(ts)   ← the arrival key, reinterpreted
                                        ▼
                                 ShadowBook::ingest(&u, recv)
     ┌──────────────────┬────────────────────┴──────────────┬──────────────────────────┐
     │ Snapshot         │ Snapshot                          │ BatchDelta               │ BatchDelta
     │   & !synced      │   & synced                        │   & !synced              │   & synced
     ▼                  ▼                                   ▼                          ▼
   seed(s)         book.diff(&shape)                     seed(shape)            levels verbatim
   ─────────       ─────────────────                     ──────────             ──────────────
   book.resync     levels where the two differ:           same seed path as     kind = gapped
   epoch += 1        theirs, where ours disagrees          the snapshot case          ? Correction
   epoch_start=recv  qty 0, where theirs holds                                        : Update
   last_ckpt = None    no such level                      ⇒ None
   ⇒ None          kind = Correction
                   empty diff ⇒ None
     └──────────────────┴───────────────────────────────────┴──────────────────────────┘
                                        ▼
                            for each level:  self.seq += 1
                                             out.push(exec, Some(recv), seq, kind, side, price, qty)
                                        ▼
                            self.book.apply(frame)
                                        ▼
                                 Some(DeltaFrame)
                                        │
        ┌───────────────────────────────┴──────────────────────────────┐
        ▼                                                              ▼
  Feather<BookDelta>::extend(frame)                        weaver.deltas.extend(ts, frame)
  one BookDelta row per level                              one arrival key per element
        │                                                              │
        ▼                                                              ▼
  data/book_deltas/<sym>/{ts_min}_{ts_max}.parquet             Lanes::deltas ⇒ Graph::tick

                                        ▼   then, at the same `recv`
                            ShadowBook::checkpoint(recv)
                                  synced, and recv - last_checkpoint >= 60 s
                                        ▼
                                  BookShape { ts: Aggregate {
                                       venue_exec: self.span,                  ← epoch's venue window
                                       local_recv: Span(epoch_start, recv) } } ← time since resync
                                        │
        ┌───────────────────────────────┴──────────────────────────────┐
        ▼                                                              ▼
  Feather<BookSnapshot> (monotonic_seq: 0)                  weaver.anchors.push(ts, shape)
```

```
  `checkpoint` runs after `apply`, so the shape it returns includes the frame emitted alongside it.
  Both carry the same `Arrival`; the weaver breaks that tie on lane index, and deltas (1) precede
  anchors (2). A replay seeded from such a checkpoint therefore starts folding at the following
  frame.
```

#align(center, diagram(
  spacing: (13mm, 9mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node((0, 0), align(center)[`BookUpdate`], fill: luma(235), name: <upd>),

  node((-2.2, 1.2), align(center)[`Snapshot` \ #text(7pt)[unsynced]], name: <s0>),
  node((-0.75, 1.2), align(center)[`Snapshot` \ #text(7pt)[synced]], name: <s1>),
  node((0.75, 1.2), align(center)[`BatchDelta` \ #text(7pt)[unsynced]], name: <d0>),
  node((2.2, 1.2), align(center)[`BatchDelta` \ #text(7pt)[synced]], name: <d1>),

  edge(<upd>, <s0>, "->"),
  edge(<upd>, <s1>, "->"),
  edge(<upd>, <d0>, "->"),
  edge(<upd>, <d1>, "->"),

  node(
    (-1.5, 2.5),
    align(center)[`seed` \ #text(7pt)[resync · `epoch += 1`]],
    fill: rgb("#f2ecdc"),
    name: <seed>,
  ),
  node((-0.75, 3.6), align(center)[`None`], fill: luma(240), name: <none>),
  node((0.2, 2.5), align(center)[`diff(shape)`], fill: rgb("#f6e6e6"), name: <diff>),
  node((2.2, 2.5), align(center)[levels verbatim], fill: rgb("#e9f1e4"), name: <verb>),

  edge(<s0>, <seed>, "->"),
  edge(<d0>, <seed>, "->"),
  edge(<seed>, <none>, "->"),
  edge(<s1>, <diff>, "->"),
  edge(<d1>, <verb>, "->"),
  edge(<diff>, <none>, "->", label: [empty], label-size: 7pt, label-side: right),

  node(
    (1.2, 3.7),
    align(center)[`DeltaFrame` \ #text(7pt)[`Correction` | `Update`] \ #text(7pt)[`self.seq += 1` per level]],
    fill: luma(235),
    name: <fr>,
  ),
  edge(<diff>, <fr>, "->", label: [`Correction`], label-size: 7pt),
  edge(<verb>, <fr>, "->", label: [`gapped ? Correction : Update`], label-size: 7pt, label-side: right),

  node((1.2, 4.9), align(center)[`book.apply(frame)`], fill: rgb("#e4edf5"), name: <ap>),
  edge(<fr>, <ap>, "->"),

  node((-0.4, 6.1), align(center)[`Feather<BookDelta>` \ #text(7pt)[one row per level]], name: <fd>),
  node((1.2, 6.1), align(center)[`weaver.deltas`], name: <wd>),
  node(
    (2.9, 6.1),
    align(center)[`checkpoint(recv)` \ #text(7pt)[after the fold · 60 s]],
    fill: rgb("#e9f1e4"),
    name: <ck>,
  ),

  edge(<ap>, <fd>, "->"),
  edge(<ap>, <wd>, "->"),
  edge(<ap>, <ck>, "->"),
))

=== 2.1 What each venue input produces

#table(
  columns: (auto, auto, auto, 1fr),
  stroke: 0.4pt + luma(180),
  align: (left, left, left, left),
  table.header([*venue input*], [*emitted*], [*persisted*], [*notes*]),

  [delta, chain intact], [`DeltaFrame::Update`], [the levels], [],
  [delta, `gapped`], [`DeltaFrame::Correction`], [the levels, `kind = Correction`], [the venue's flag is read once, here],
  [delta, book unsynced], [nothing], [nothing], [`seed`: the shape becomes the epoch's starting state],
  [snapshot, book unsynced], [nothing], [nothing], [`seed`, same path],
  [snapshot, equal to ours], [nothing], [nothing], [`diff` returns an empty vector],
  [snapshot, differing], [`DeltaFrame::Correction`], [the diff levels], [levels present in theirs and differing in ours, plus `qty 0` for levels only ours holds],
  [cadence elapsed], [`BookShape`], [a `BookSnapshot` row], [emitted after the frame at the same `recv`],
)

== 3. Read path

```
 BookSnapshots lane                            BookDeltas lane
        │  pick_anchor(start)                         │  lane_reader(start, end)
        │    newest row with ts_min <= start,         │    streams one parquet file at a time,
        │    within MAX_ANCHOR_AGE                    │    filters ts_axis ∈ [start, end]
        │  snapshot_shape(row, prec)  ← precision     │
        │    from the file's schema metadata          │
        │    Span::at(..) for both epochs             │
        ▼                                             ▼
   BookAnchors                                   BookDeltas
   Out = Option<&BookShape>                      Out = DeltaFrame<'t>
   a run of checkpoints collapses to             the weaver breaks a run where `FrameKind`
   `.last()`; a pre-range one gets               changes, since a frame wraps a run of levels
   `Arrival::MIN`
        └──────────────────┬──────────────────────────┘
                           ▼
              Book::advance ⇒ Book::step(anchor, frame)


   ┌─────────────────────────────── Book::step, in order ───────────────────────────────┐
   │                                                                                     │
   │  1.  missed(frame.seq)                                                              │
   │        seq.first() against self.seq: `first != last + 1`  ⇒  synced = false          │
   │        the same condition covers an unseeded start, a gap in the recording, and      │
   │        frames that went by while a gate was shut                                     │
   │                                                                                     │
   │  2.  !synced && anchor.is_some()  ⇒  resync(s)                                      │
   │        prec = s.prec  ·  bids ← s.bids.rev()  ·  asks ← s.asks  ·  span = s.ts       │
   │        epoch += 1  ·  synced = true  ·  seq = None                                  │
   │        skipped while synced: the checkpoint holds what the fold already holds, and   │
   │        taking it would clone both maps and increment `epoch`                         │
   │                                                                                     │
   │  3.  synced  ⇒  apply(frame)                                                        │
   │        assert_eq!(self.prec, cols.prec)                                              │
   │        per level:  seek(levels, side, price)  — binary search over the best-first     │
   │                    ordering                                                          │
   │            (Ok(j),  0) ⇒ remove(j)          the level went away                       │
   │            (Ok(j),  q) ⇒ levels[j].1 = q    the level's qty changed                   │
   │            (Err(_), 0) ⇒ noop               a delete below the window we hold         │
   │            (Err(j), q) ⇒ insert(j, ..)      a level appeared                          │
   │        span = Span(min(span.first, exec[0]), exec.last())                            │
   │        seq  = cols.monotonic_seq.last()                                              │
   │                                                                                     │
   │  4.  returns `synced`  ⇒  `Some(&Book)` / `None`                                    │
   └─────────────────────────────────────────────────────────────────────────────────────┘
```

#align(center, diagram(
  spacing: (30mm, 13mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node((0, 0), align(center)[`!synced` \ #text(7pt)[publishes `None`]], fill: rgb("#f6e6e6"), name: <un>),
  node((1.6, 0), align(center)[`synced` \ #text(7pt)[publishes `Some(&Book)`]], fill: rgb("#e9f1e4"), name: <sy>),

  edge(
    <un>,
    <sy>,
    "->",
    bend: -34deg,
    label: align(center)[#text(7pt)[an anchor is present \ `resync` · `epoch += 1` · `seq = None`]],
    label-side: left,
  ),
  edge(
    <sy>,
    <un>,
    "->",
    bend: -34deg,
    label: align(center)[#text(7pt)[`missed(seq)`: `first != last + 1` \ a gap, or a gate that was shut]],
    label-side: left,
  ),

  node(
    (0, 1.6),
    align(center)[#text(7pt)[no anchor yet: the frame \ is dropped]],
    stroke: none,
    name: <drop>,
  ),
  node(
    (1.6, 1.6),
    align(center)[#text(7pt)[`apply(frame)` — `Update` and \ `Correction` fold the same way]],
    stroke: none,
    name: <app>,
  ),
  edge(<un>, <drop>, "->", bend: 40deg),
  edge(<drop>, <un>, "->", bend: 40deg),
  edge(<sy>, <app>, "->", bend: 40deg),
  edge(<app>, <sy>, "->", bend: 40deg),
))

=== 3.1 Level ordering and precision

```
  BookShape (venue message · checkpoint · row)   Book (the fold)
  ────────────────────────────────────────────   ────────────────────────────────────────────
  bids: BTreeMap<i32,u32>  ascending    ──rev──▶ bids: Vec<(i32,u32)>  descending  ] best-first
  asks: BTreeMap<i32,u32>  ascending    ──────▶ asks: Vec<(i32,u32)>  ascending    ] index 0 = top
                                        ◀─shape()── .iter().copied().collect()

  ponytail: sorted Vec, on the reasoning that at the depth a lane carries the memmove of an insert
  costs less than a B-tree descent. `debug_assert!(levels.len() <= 1024)` marks where that stops
  being the assumption; a full-depth feed would want a map.

  Precision is held once per shape, per cols view, per Book, and in the parquet schema metadata.
  Levels themselves carry raw i32 price and u32 qty as the venue sent them; the decode to f64 happens
  at the reading end. `apply` asserts the frame's precision equals the book's, `Feather::extend`
  asserts the run's equals the lane's, and a lane with no file panics — there is no default, and
  `Precision(0)` would scale every price by 1.
```

== 4. Disk lanes

#table(
  columns: (auto, auto, auto, auto, auto, 1fr),
  stroke: 0.4pt + luma(180),
  align: (left, left, left, left, left, left),
  table.header([*lane*], [*row*], [*axis*], [*rotation*], [*weave shape*], [*notes*]),

  [`BookDeltas`],
  [`BookDelta`],
  [`Venue`],
  [256 MB / 1 h],
  [`DeltaFrame<'t>`],
  [one row per level. `ts_local_recv` is a plain `Ts<Local>`, since the adapter that took the frame off the wire knew its reception time and there is no historic-ingest path into this lane.],

  [`BookSnapshots`],
  [`BookSnapshot`],
  [`Venue`],
  [64 MB / 6 h],
  [`Option<&BookShape>`],
  [our checkpoints, at `CHECKPOINT_CADENCE`. `monotonic_seq` is written 0, and `resync` sets the fold's `seq` to `None`, so the following frame's first seq re-seeds the chain. `pub(crate)`.],
)

```
  The two are separate `LaneKind`s, so a graph naming one root does not open and accumulate the
  other. Under `Live` an un-drained lane grows for the length of the session.

  MAX_ANCHOR_AGE is read off the graph:

      <<Book as Node>::Deps as DepSet>::REACH[1]   must be a `Horizon::Span(tf)`, else compile error
                                                   ⇒ TF_15MIN, today

  So the reader looks back as far as the folding node declares it reaches. A range with no
  checkpoint in that window yields `None`, and the book stays unsynced until one arrives.
```

== 5. Graph cells

```
  BookAnchors      Cell::Out = Option<&BookShape>   Nudge::Scratch = Option<BookShape>
                   stage() returns 0.0 — there is no finite-difference step over a level map
                   its doc comment marks it as `Book`'s input

  BookDeltas       Cell::Out = DeltaFrame<'t>       Nudge::Scratch = DeltaBuf
                   the owned twin serves as lane buffer, FD scratch and emit accumulator;
                   `bump_last(slot, h)` is the part only the scratch role uses

  Book             Cell::Out = Option<&Book>        Nudge::Scratch = Option<Book>
                   Deps = (BookAnchors, Folding<BookDeltas, {Horizon::Span(TF_15MIN)}>)
                   advance = `self.step(anchor, deltas).then_some(&*self)`

     The dep is `Folding` because `DeltaFrame` does not implement `Series`, so there is nothing for
     the engine to retain. `Pull::open` const-asserts `!any(GATES) || !any(FOLDS)`, so this node as
     declared cannot sit behind a gate. Retaining the deltas as stamped level rows would change
     that.

     `stage` calls `clone_from` so that a hand-written `Clone for Book` could reuse both level
     vectors across ticks. `Book` derives `Clone`, and a derived `clone_from` is
     `*self = source.clone()`, so at present the observer clones the book twice per fired node and
     again per dep slot.
```

```
  Readers of the folded book

  Flat for &Book        [best_bid, best_ask]     via Price::as_f64
  Flat for &BookShape   [bids.keys().next_back(), asks.keys().next()] / price.scale()
                        the same two numbers, read off the map ordering
  Glance for &Book      "{bid}/{ask} ({n} lvls)"  ·  "empty ({n} lvls)"
  Glance for &BookShape "checkpoint {n}b/{n}a"

  examples/spl  BookTop : Emit
      Deps = (Folding<Book, {Horizon::Unbounded}>, BookDeltas)
      ⇒ BookTopSnap { ts_ns, best_bid, best_ask, top20_bid_depth_usd, top20_ask_depth_usd }
      The reach is Unbounded: the book accumulates every delta since its anchor.
      While one side is still empty the out is `None` for that tick.
      The `BookDeltas` dep supplies the run's last `exec` stamp; the batch collapses to a single
      read at its end.

  examples/live BookFlow : Emit
      Deps = (Folding<BookDeltas, {Horizon::Unbounded}>,)  — reads the level stream directly
```

=== 5.1 Gating

```
  gate open    ─▶ deps pulled ─▶ anchor + frame ─▶ fold ─▶ Some(&Book)
  gate closed  ─▶ deps not pulled, so neither lane is read. `Latent` for `Option<T>` is `None`
  gate reopen  ─▶ the frames that went by left a seq hole
                    └▶ missed() ⇒ desync ⇒ the next checkpoint resyncs, `epoch += 1`, and the
                       levels held from before are cleared

  An episode spent shut therefore costs one desync and one resync, and the levels come back from the
  next checkpoint.

  `GatedBook` (trading_data/tests/book_gating.rs) is a test-local wrapper over `Book::step` with
  `Folding<BookDeltas, _>` replaced by a bare `BookDeltas` behind `Gating<Hot>`. It exists to hold
  the three claims above under test; no library names it.
```

== 6. enforcement points

```
  Book::apply         assert: frame precision == book precision
  Book::apply         debug: levels.len() <= 1024 — the sorted-Vec assumption
  Book::step          a `monotonic_seq` discontinuity desyncs; a checkpoint re-arms, and the fold
                      restarts from the checkpoint's levels
  Book::step          the single public verb; `resync`, `apply`, `missed` and `diff` are private, so
                      a caller cannot apply a frame without the `missed` check ahead of it
  ShadowBook::ckpt    expect: `epoch_start` is set whenever the book is synced
  Feather::extend     assert: the run's precision equals the lane's
  Feather::extend     expect: `ts.recv` is present, which is what lets `BookDelta::ts_local_recv` be
                      a plain `Ts<Local>`
  read.rs             assert: `schema_version` matches exactly · file metadata consistent across
                      the read range
  read.rs             const:  MAX_ANCHOR_AGE is a `Horizon::Span` on `Book`'s declared reach, or it
                      does not compile
  Replay::new         panic:  no file under any candidate key, so no precision to hoist
  Pull::open          const:  `Gating` + `Folding` on one node does not compile
  Weaver::next        deltas break a run where `FrameKind` changes, so one frame carries one kind
```
