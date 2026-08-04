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

= The book — every primitive, and what touches what

Read off `trading_data_core/src/{book.rs, cols.rs, cells.rs, precision.rs, ts.rs}`,
`trading_data_persistence/src/{sync/mod.rs, feather.rs, row.rs, read.rs}`, and the two consumers in
`examples/spl/src/nodes/book_top.rs` and `examples/live/src/nodes.rs`.

Third of three: `trading_data_dag/model.typ` is what happens to a tick, `weaver.typ` is where a tick
comes from, this one is what *one lane* is made of. §1.8 of the weaver states the rule; this is the
one lane that pays the most for it.

== 1. The axiom, and the seven primitives that fall out of it

```
  A GAP IS A FACT ABOUT OUR CONNECTION, NOT ABOUT THE MARKET.

  Stored raw, every replay would re-derive the reconciliation from whichever venue snapshots
  happened to land — a cadence we neither control nor can reproduce. So the reconciliation happens
  ONCE, at record time, and what reaches disk is our own recollection: gapless by construction,
  self-consistent, and folded blind on the way back.

  That splits the book into a WRITE side and a READ side that share no code except the fold itself:

     ShadowBook  ── decides.   Owns a `Book`, emits frames, mints checkpoints.        write-only
     Book        ── folds.     Knows nothing of venues, gaps, or disk.                both sides
     BookShape   ── carries.   The wire, the checkpoint and the disk row are one shape.

  and the same instance of `Book` sits inside `ShadowBook`, so what we recorded and what a replay
  folds cannot drift. That is the whole trick; everything below is bookkeeping around it.
```

=== 1.1 The shapes, verbatim

```rust
// ── venue side ───────────────────────────────────────────────────────────────────────────────
enum BookUpdate {                            // what an adapter hands us; consumed, never stored
    Snapshot(BookShape),
    BatchDelta { shape: BookShape, gapped: bool },   // gapped = the VENUE's seq chain broke
}

struct BookShape {                           // wire + checkpoint + disk, one shape
    ts:   Aggregate,                         // both `first`s = start of the accumulation EPOCH
    prec: PrecisionPriceQty,                 // shared across every level; hoisted, never per-level
    asks: BTreeMap<i32, u32>,                // ascending
    bids: BTreeMap<i32, u32>,                // ascending — `Book` re-orders on resync
}

struct Aggregate { venue_exec: Span<Venue>, local_recv: Span<Local> }
struct Span<A>   { first: Ts<A>, last: Ts<A> }
struct PrecisionPriceQty { price: Precision, qty: Precision }    // Precision(i8): raw = v × 10^p

// ── the reconciler (write side only) ─────────────────────────────────────────────────────────
struct ShadowBook {
    book:  Book,                   // our recollection — kept in lockstep with what we emit
    out:   DeltaBuf,               // scratch the emitted frame borrows from
    seq:   u64,                    // OUR chain. Never the venue's. Gapless by construction
    cadence: Exact,                // CHECKPOINT_CADENCE = 60s (sync/mod.rs)
    epoch_start:     Option<Ts<Local>>,     // set on seed; `Some` ⇔ book is synced
    last_checkpoint: Option<Ts<Local>>,     // cleared on seed: a new epoch owes a fresh checkpoint
}
fn ingest(&mut self, u: &BookUpdate, recv: Ts<Local>) -> Option<DeltaFrame<'_>>;
fn checkpoint(&mut self, recv: Ts<Local>) -> Option<BookShape>;

// ── the fold (both sides) ────────────────────────────────────────────────────────────────────
struct Book {
    prec:  PrecisionPriceQty,
    bids:  Vec<(i32, u32)>,        // DESCENDING  ] best-first, contiguous,
    asks:  Vec<(i32, u32)>,        // ascending   ] index 0 = top of book
    epoch: u64,                    // bumped by every resync: "same book, deeper" ≠ "a other book"
    synced: bool,
    seq:   Option<u64>,            // last folded monotonic_seq; `None` right after a resync
    span:  Span<Venue>,
}
fn step(&mut self, anchor: Option<&BookShape>, frame: DeltaFrame<'_>) -> bool;   // the ONLY verb

// ── the delta carrier ────────────────────────────────────────────────────────────────────────
enum DeltaFrame<'a> { Update(DeltaCols<'a>), Correction(DeltaCols<'a>) }
enum FrameKind      { Update, Correction }        // the stored u8, parallel to the side byte

struct DeltaCols<'a> {                            // borrowed SoA view — what a node reads
    prec: PrecisionPriceQty,
    ts:   RelayCols<'a, Venue, Local>,            // { exec: Option<&[Ts]>, send, recv: Option<Span> }
    monotonic_seq: &'a [u64],
    side: &'a [Side], price: &'a [i32], qty: &'a [u32],       // qty == 0 ⇒ DELETE this level
}
struct DeltaBuf {                                 // owned twin: lane buffer + Nudge scratch +
    prec: PrecisionPriceQty,                      //             ShadowBook's emit accumulator
    exec: Vec<Ts<Venue>>, recv: Vec<Option<Ts<Local>>>,
    monotonic_seq: Vec<u64>, kind: Vec<FrameKind>,
    side: Vec<Side>, price: Vec<i32>, qty: Vec<u32>,
}

// ── graph roots (cells.rs — the only crate that may write these impls) ───────────────────────
struct BookAnchors;   // Cell::Out = Option<&BookShape>   Nudge::Scratch = Option<BookShape>
struct BookDeltas;    // Cell::Out = DeltaFrame<'_>       Nudge::Scratch = DeltaBuf
impl  Cell for Book { type Out<'t> = Option<&'t Book>; }   // Option ⇒ Latent ⇒ gateable
impl  Node for Book { type Deps = (BookAnchors, Folding<BookDeltas, {Horizon::Span(TF_15MIN)}>); }

// ── disk rows ────────────────────────────────────────────────────────────────────────────────
pub struct BookDelta {                       // one row PER LEVEL   ·   256 MB / 1 h rotation
    ts_venue_exec: Ts<Venue>,
    ts_local_recv: Ts<Local>,                // NOT Option: book lanes are only ever live-recorded
    monotonic_seq: u64,
    kind: FrameKind,                         // ours. Not the venue's `gapped`
    side: Side, price: i32, qty: u32,
}
pub(crate) struct BookSnapshot {             // one row PER CHECKPOINT  ·  64 MB / 6 h rotation
    ts_venue_exec: Ts<Venue>, ts_local_recv: Ts<Local>,
    monotonic_seq: u64,                      // written 0 — a checkpoint has no element sequence
    bid_prices: Vec<i32>, bid_qtys: Vec<u32>,
    ask_prices: Vec<i32>, ask_qtys: Vec<u32>,
}
```

=== 1.2 Who holds whom

```
  ShadowBook ─owns─▶ Book        the recorded book and the replayed book are the same fold
             ─owns─▶ DeltaBuf    the emitted frame borrows it; cleared at the top of every ingest

  BookShape  ◀─shape()── Book              fold  ⇒ wire   (checkpoint out)
  BookShape  ──resync()─▶ Book             wire  ⇒ fold   (checkpoint in)
  BookShape  ◀─snapshot_shape()── BookSnapshot          disk ⇒ wire, precision from the FILE

  DeltaFrame ◀─frame()── DeltaBuf          borrowed view over the owned twin
  DeltaFrame ──extend()─▶ Feather<BookDelta>            one row per level
  DeltaFrame ──apply()──▶ Book                          the fold, both kinds identically
```

== 2. The write path — `ShadowBook` is the only thing that ever sees a venue

```
  venue WS
     │  Sink::push   (clock-free, one send per MESSAGE)
     ▼
 LiveEvt::Book(BookUpdate) ─────▶ Live::ingest_book(u, ts)          ONE thread, ONE point
                                        │  recv = recv_of(ts)   ← the ARRIVAL KEY reinterpreted,
                                        │                          never a second clock read
                                        ▼
                                 ShadowBook::ingest(&u, recv)
     ┌──────────────────┬────────────────────┴──────────────┬───────────────────────────┐
     │ Snapshot         │ Snapshot                          │ BatchDelta                │ BatchDelta
     │   & !synced      │   & synced                        │   & !synced               │   & synced
     ▼                  ▼                                   ▼                           ▼
   seed(s)         book.diff(&shape)                     seed(shape)              levels verbatim
   ─────────       ─────────────────                     ──────────               ──────────────
   book.resync     the levels that carry US onto THEM:    a delta before any       kind = gapped
   epoch += 1        theirs, where ours disagrees          snapshot IS the start          ? Correction
   epoch_start=recv  ours, as qty 0, where theirs          of the epoch — not a           : Update
   last_ckpt = None    has no such level                   fallback path
   ⇒ None          kind = Correction                      ⇒ None
   (a seed is      empty diff ⇒ None
    not an event)  (an agreeing snapshot is not an event)
     └──────────────────┴───────────────────────────────────┴───────────────────────────┘
                                        ▼
                            for each level:  self.seq += 1
                                             out.push(exec, Some(recv), seq, kind, side, price, qty)
                                        ▼
                            self.book.apply(frame)      ◀── we fold exactly what we emit
                                        ▼
                                 Some(DeltaFrame)
                                        │
        ┌───────────────────────────────┴──────────────────────────────┐
        ▼                                                              ▼
  Feather<BookDelta>::extend(frame)                        weaver.deltas.extend(ts, frame)
  one BookDelta row per level                              lane key = the arrival, per element
        │                                                              │
        ▼                                                              ▼
  data/book_deltas/<sym>/{ts_min}_{ts_max}.parquet             Lanes::deltas ⇒ Graph::tick

                                        ▼   then, after the fold, at the same `recv`
                            ShadowBook::checkpoint(recv)
                                  synced?  and  recv - last_checkpoint >= 60s ?
                                        ▼
                                  BookShape { ts: Aggregate {
                                       venue_exec: self.span,                  ← epoch's venue window
                                       local_recv: Span(epoch_start, recv) } } ← time SINCE RESYNC,
                                        │                                        i.e. folded drift
        ┌───────────────────────────────┴──────────────────────────────┐
        ▼                                                              ▼
  Feather<BookSnapshot> (monotonic_seq: 0)                  weaver.anchors.push(ts, shape)
```

```
  THE CHECKPOINT IS MINTED AFTER THE FOLD, so it INCLUDES the frame it was emitted alongside.
  Both land on the same `Arrival`, and the weaver's tie-break is lane INDEX — deltas(1) before
  anchors(2) — so the replayed order matches the live one: the frame arrives, then the checkpoint
  that already contains it. A replay seeded from that checkpoint reads a gapless chain starting at
  the NEXT frame, which is exactly what `Book::step` wants.
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

  node((-1.5, 2.5), align(center)[`seed` \ #text(7pt)[resync · `epoch += 1`]], fill: rgb("#f2ecdc"), name: <seed>),
  node((-0.75, 3.6), align(center)[`None` \ #text(7pt)[not an event]], fill: luma(240), name: <none>),
  node((0.2, 2.5), align(center)[`diff(shape)`], fill: rgb("#f6e6e6"), name: <diff>),
  node((2.2, 2.5), align(center)[levels verbatim], fill: rgb("#e9f1e4"), name: <verb>),

  edge(<s0>, <seed>, "->"),
  edge(<d0>, <seed>, "->"),
  edge(<seed>, <none>, "->"),
  edge(<s1>, <diff>, "->"),
  edge(<d1>, <verb>, "->"),
  edge(<diff>, <none>, "->", label: [empty], label-size: 7pt, label-side: right),

  node((1.2, 3.7), align(center)[`DeltaFrame` \ #text(7pt)[`Correction` | `Update`] \ #text(7pt)[`self.seq += 1` per level]], fill: luma(235), name: <fr>),
  edge(<diff>, <fr>, "->", label: [`Correction`], label-size: 7pt),
  edge(<verb>, <fr>, "->", label: [`gapped ? Correction : Update`], label-size: 7pt, label-side: right),

  node((1.2, 4.9), align(center)[`book.apply(frame)` \ #text(7pt)[we fold what we emit]], fill: rgb("#e4edf5"), name: <ap>),
  edge(<fr>, <ap>, "->"),

  node((-0.4, 6.1), align(center)[`Feather<BookDelta>` \ #text(7pt)[one row per level]], name: <fd>),
  node((1.2, 6.1), align(center)[`weaver.deltas`], name: <wd>),
  node((2.9, 6.1), align(center)[`checkpoint(recv)` \ #text(7pt)[after the fold · 60 s]], fill: rgb("#e9f1e4"), name: <ck>),

  edge(<ap>, <fd>, "->"),
  edge(<ap>, <wd>, "->"),
  edge(<ap>, <ck>, "->"),
))

=== 2.1 The reconciliation table

#table(
  columns: (auto, auto, auto, auto),
  stroke: 0.4pt + luma(180),
  align: (left, left, left, left),
  table.header([*venue input*], [*emitted*], [*persisted*], [*why*]),

  [delta, chain intact], [`DeltaFrame::Update`], [levels, verbatim], [market activity],
  [delta, `gapped`], [`DeltaFrame::Correction`], [levels + `kind`], [the hole is ours, so the repair is ours],
  [snapshot, we are unsynced], [nothing (`seed`)], [nothing], [no chain to reconcile against yet],
  [snapshot, agreeing], [nothing], [nothing], [an agreeing snapshot is not an event],
  [snapshot, disagreeing], [`DeltaFrame::Correction`], [exactly the diffs], [the minimal carry from ours onto theirs],
  [cadence elapsed], [`BookShape`], [a `BookSnapshot` row], [our checkpoint, on our cadence],
  [any venue snapshot], [—], [*never stored*], [their story is not our recollection],
)

== 3. The read path — the same fold, run blind

```
 BookSnapshots lane                            BookDeltas lane
        │  pick_anchor(start)                         │  lane_reader(start, end)
        │    newest row with ts_min <= start,         │    streams one parquet file at a time,
        │    within MAX_ANCHOR_AGE                    │    filters ts_axis ∈ [start, end]
        │  snapshot_shape(row, prec)  ← precision     │
        │    off the FILE's schema metadata           │
        │    Span::at(..) both epochs: a stored       │
        │    snapshot IS a resync point               │
        ▼                                             ▼
   BookAnchors                                   BookDeltas
   Out = Option<&BookShape>                      Out = DeltaFrame<'t>
   a LEVEL: a run of checkpoints collapses       a FLOW WITH IDENTITY: the weaver breaks a
   to `.last()` — older ones are superseded,     run where `FrameKind` changes, because the
   not skipped. Pre-range ⇒ `Arrival::MIN`       frame wraps the RUN, not the row
        └──────────────────┬──────────────────────────┘
                           ▼
              Book::advance ⇒ Book::step(anchor, frame)


   ┌─────────────────────────────── Book::step, in order ───────────────────────────────┐
   │                                                                                     │
   │  1.  missed(frame.seq)                                                              │
   │        seq.first() vs self.seq: `first != last + 1`  ⇒  synced = false               │
   │        one path covers three things — an unseeded start, a gap in our own recording, │
   │        and an episode that went by while a gate was shut                             │
   │                                                                                     │
   │  2.  !synced && anchor.is_some()  ⇒  resync(s)                                      │
   │        prec = s.prec  ·  bids ← s.bids.rev()  ·  asks ← s.asks  ·  span = s.ts       │
   │        epoch += 1  ·  synced = true  ·  seq = None                                  │
   │        NOT taken while synced: our chain is gapless, so the checkpoint is the state  │
   │        we already hold — taking it would clone both maps and bump `epoch` for free   │
   │                                                                                     │
   │  3.  synced  ⇒  apply(frame)                                                        │
   │        assert_eq!(self.prec, cols.prec)                                              │
   │        per level:  seek(levels, side, price)  — binary search, best-first ordering    │
   │            (Ok(j),  0) ⇒ remove(j)          a level that went away                    │
   │            (Ok(j),  q) ⇒ levels[j].1 = q    a level that moved                        │
   │            (Err(_), 0) ⇒ noop               a delete BELOW our window                 │
   │            (Err(j), q) ⇒ insert(j, ..)      a level that appeared                     │
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
    align(center)[#text(7pt)[no anchor yet: frames are dropped, \ never folded onto stale state]],
    stroke: none,
    name: <drop>,
  ),
  node(
    (1.6, 1.6),
    align(center)[#text(7pt)[`apply(frame)` — `Update` and \ `Correction` fold identically]],
    stroke: none,
    name: <app>,
  ),
  edge(<un>, <drop>, "->", bend: 40deg),
  edge(<drop>, <un>, "->", bend: 40deg),
  edge(<sy>, <app>, "->", bend: 40deg),
  edge(<app>, <sy>, "->", bend: 40deg),
))

=== 3.1 Two orderings, one book

```
  BookShape (wire · checkpoint · disk)          Book (the fold)
  ────────────────────────────────────          ────────────────────────────────────────────
  bids: BTreeMap<i32,u32>  ASCENDING    ──rev──▶ bids: Vec<(i32,u32)>  DESCENDING  ] best-first
  asks: BTreeMap<i32,u32>  ascending    ──────▶ asks: Vec<(i32,u32)>  ascending    ] index 0 = top
                                        ◀─shape()── .iter().copied().collect()

  ponytail: a sorted Vec beats a B-tree to ~1k levels — at the depth a lane carries, the memmove of
  an insert costs less than a descent. `debug_assert!(levels.len() <= 1024)` marks the ceiling; a
  full-depth feed is where it flips back to a map.

  PRECISION IS THE HOLDER'S, never the level's. `PrecisionPriceQty` sits on the shape, on the cols,
  on the `Book`, and in the parquet schema metadata — one value hoisted out of every loop, so the
  columns stay exactly what the venue sent and what the disk holds, with no f64 round trip between.
  `apply` asserts the frame's precision equals the book's; `Feather::extend` asserts the run's
  equals the lane's; a lane with no file at all PANICS rather than defaulting, because a default
  precision scales every price by 1 and produces plausible-looking numbers that are wrong by orders
  of magnitude.
```

== 4. The two lanes on disk

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
  [one row per LEVEL. `ts_local_recv` is not `Option`: the adapter that took the frame off the wire always knew its own reception time, so there is no historic-ingest path here. Carries `kind`, never the venue's `gapped`.],

  [`BookSnapshots`],
  [`BookSnapshot`],
  [`Venue`],
  [64 MB / 6 h],
  [`Option<&BookShape>`],
  [our checkpoints, our cadence. `monotonic_seq` written 0 — a checkpoint carries no element sequence, and `resync` sets `seq = None` so the next frame's first seq re-seeds the chain. `pub(crate)`: it is internal to the persistence model, and nothing outside may name it.],
)

```
  Separate `LaneKind`s, deliberately: naming one root must not LOAD AND ACCUMULATE the other. A
  graph reading only `BookDeltas` never opens the snapshot lane, and in `Live` an un-drained
  duplicate lane grows without bound.

  MAX_ANCHOR_AGE is not a constant of this crate. It is read off the graph:

      <<Book as Node>::Deps as DepSet>::REACH[1]   must be a `Horizon::Span(tf)`, else compile error
                                                   ⇒ TF_15MIN, today

  The reader looks back exactly as far as the folding node DECLARED it reaches. It bounds a READ,
  not drift — our delta lane is gapless by construction, so a miss here means a hole in our own
  recording, and the honest answer is an unsynced book rather than a seeded one.
```

== 5. In the graph

```
  BookAnchors      Cell::Out = Option<&BookShape>   Nudge::Scratch = Option<BookShape>
                   stage() ⇒ 0.0: perturbing a level makes it a DIFFERENT book, not a nearby one
                   "Anchors are `Book`'s input, not the graph's" — nothing else should name it

  BookDeltas       Cell::Out = DeltaFrame<'t>       Nudge::Scratch = DeltaBuf
                   the owned twin serves three roles at once: lane buffer, FD scratch, emit
                   accumulator. `bump_last(slot, h)` is the only thing scratch does that a lane
                   buffer does not

  Book             Cell::Out = Option<&Book>        Nudge::Scratch = Option<Book>
                   Deps = (BookAnchors, Folding<BookDeltas, {Horizon::Span(TF_15MIN)}>)
                   advance = `self.step(anchor, deltas).then_some(&*self)`

     `Folding` and not `Buffering` only because `DeltaFrame` is no `Series` — there is nothing the
     engine could retain for it. Which is also what still makes a gated `Book` a compile error
     (`Pull::open`: `!any(GATES) || !any(FOLDS)`), checkpoint or no. Retaining the deltas as stamped
     level rows is the one change that would lift it.

     The stage() note in cells.rs is a live cost: `clone_from` is written so that a hand-rolled
     `Clone for Book` would keep both level vectors across ticks, but `Book` derives its own, and a
     derived `clone_from` is `*self = source.clone()`. So today the observer pays two book clones
     per fired node and again per dep slot.
```

```
  READERS OF THE FOLDED BOOK

  Flat for &Book        [best_bid, best_ask]     via Price::as_f64
  Flat for &BookShape   [bids.keys().next_back(), asks.keys().next()] / price.scale()
                        the same two numbers, read off the other ordering — a checkpoint reads like
                        the book it seeds
  Glance for &Book      "{bid}/{ask} ({n} lvls)"  ·  "empty ({n} lvls)"
  Glance for &BookShape "checkpoint {n}b/{n}a"

  examples/spl  BookTop : Emit
      Deps = (Folding<Book, {Horizon::Unbounded}>, BookDeltas)
      ⇒ BookTopSnap { ts_ns, best_bid, best_ask, top20_bid_depth_usd, top20_ask_depth_usd }
      Unbounded and not Span: the book is the accumulation of every delta since its anchor, so a
      gate closing over it would lose a state no later tick rebuilds. The reach says so.
      One side still empty is WARMUP, not corruption — the tick declines and consumers don't enter.
      The `BookDeltas` dep is there for the run's last `exec` stamp; the batch collapses to the one
      read at its end, so the out is never longer than a tick.

  examples/live BookFlow : Emit
      Deps = (Folding<BookDeltas, {Horizon::Unbounded}>,)  — reads the LEVELS, not the book
```

=== 5.1 Gating

```
  gate OPEN    ─▶ deps pulled ─▶ anchor + frame ─▶ fold ─▶ Some(&Book)
  gate CLOSED  ─▶ deps NOT pulled — no checkpoint read, no level read. `Latent` for `Option<T>`
                  is `None`, and that is the whole dark branch
  gate REOPEN  ─▶ the frames that went by left a seq hole
                    └▶ missed() ⇒ desync ⇒ the next checkpoint resyncs, `epoch += 1`, and the
                       stale levels are CLEARED rather than folded onto

  This is why a book may be gated where a recurrence may not: it re-warms from a checkpoint, out of
  band, so the graph owes it no history. Cost of an episode off and on is one desync and one
  resync — not a warmup it can never recover.

  `GatedBook` (trading_data/tests/book_gating.rs) is a TEST-LOCAL eight-line wrapper over the same
  public `Book::step`, swapping `Folding<BookDeltas, _>` for a bare `BookDeltas` behind
  `Gating<Hot>`. It is not shipped and nothing in any library names it — its only job is to make the
  three claims above fail loudly if they stop holding.
```

== 6. The enforcement points, in full

```
  Book::apply         assert: frame precision == book precision — the one mismatch that silently
                      rescales every level
  Book::apply         debug: levels.len() <= 1024 — the sorted-Vec ceiling, named where it flips
  Book::step          a `monotonic_seq` discontinuity desyncs; ONLY a checkpoint re-arms, and from
                      the checkpoint's state rather than the stale one
  Book::step          one entry point, not four verbs: a caller that could `apply` without first
                      checking `missed` is a caller that can fold onto stale state. `resync`,
                      `apply`, `missed` and `diff` are all private
  ShadowBook::ckpt    expect: `epoch_start` is set whenever the book is synced
  Feather::extend     assert: the run's precision equals the lane's
  Feather::extend     expect: `ts.recv` is present — "a book lane is only ever written by a live
                      recording", which is also why `BookDelta::ts_local_recv` is not `Option`
  read.rs             assert: `schema_version` matches exactly · file metadata consistent across
                      the read range
  read.rs             const:  MAX_ANCHOR_AGE is a `Horizon::Span` on `Book`'s declared reach, or it
                      does not compile
  Replay::new         panic:  no file under any candidate key ⇒ no default precision, ever
  Pull::open          const:  `Gating` + `Folding` on one node is a compile error — hence the
                      shipped `Book` node cannot be gated as written
  Weaver::next        deltas break a run where `FrameKind` changes: an `Update` and a `Correction`
                      are not the same kind of thing and must not reach a consumer as one frame
```

== 7. What is not modelled

```
  · NO PRICE BUCKETING. Levels are keyed by the venue's own raw tick, at the lane's shared
    precision, and nothing anywhere coarsens them. `bucket` in this repo only ever means TIME
    (bars, five-minute OI publishes).
  · NO PER-PERIOD DELTA BATCHING. `CHECKPOINT_CADENCE` (60 s) bounds how far a replay reads back;
    it does not group deltas. The only grouping a run gets is the weaver's `ReadClock` cell, which
    is a cost knob and unobservable to a node.
  · `Book` carries no venue sequence of its own after a resync — `seq = None` until the next frame
    re-seeds it, so the very first frame after a checkpoint can never be judged for a gap. That is
    intentional: the checkpoint is a seed, and a seed has no predecessor.
  · `Book::len()` is `bids.len() + asks.len()`, both sides at once. There is no per-side depth
    reading, because no consumer has needed one.
```

```
  The chain of custody, one sentence per link:

    the venue said it          ── ts_venue_exec, and both the file bounds and the query bounds are on it
    we received it             ── Live::stamp, one thread, one point, monotone by construction
    we wrote down that we did  ── recv_of ⇒ ts_local_recv, the key itself and not a second reading
    we reconciled it ONCE      ── ShadowBook: gaps and disagreements became OUR frames and OUR kinds
    we checkpointed ourselves  ── after the fold, on our cadence, never the venue's snapshot
    a replay folds it blind    ── Book::step knows nothing of venues, gaps, or disk
    and lands on the same book ── which is what `examples/sync_round_trip.rs` asserts, epoch and all
```
