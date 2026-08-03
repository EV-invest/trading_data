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

= `trading_data_persistence::sync` — the weaver

Read off `sync/mod.rs`, `read.rs`, `catalog.rs`, `feather.rs`, `row.rs`, and — because the key and
the reconciliation live a tier down — `trading_data_core/src/{ts.rs, cols.rs, book.rs}`. The
evidence in §2 is the round-trip in `examples/sync_round_trip.rs`.

Companion to `trading_data_dag/model.typ`: that one is what happens to a tick, this one is where a
tick comes from.

== 1. The problem

=== 1.1 One axiom

```
  A strategy may read only what it had already received.

  So the stream's order is RECEPTION order, and reception is a reading only WE can make. The venue's
  execution axis says what happened; it does not say when we could have acted on it. Ordering a
  replay on that axis is lookahead with extra steps — two lanes whose wire latencies differ would
  come out in an order no live session could ever have seen, and the strategy would trade on it.

  Every decision below is that sentence made mechanical. Three follow immediately:

    (1) reception must be a KEY, not a measurement        ──▶  `Arrival`          §1.3
    (2) where it was not recorded it must be MANUFACTURED,
        and manufactured the same way twice               ──▶  `effective`        §1.3
    (3) a live session must record its own KEY, never a
        second clock read that could tie where the key did not  ──▶  `recv_of`    §1.8

  and one that is the whole crate's shape:

    (4) whatever a replay would otherwise have to RE-DERIVE from circumstance must instead be a
        fact on disk — decided once, at ingest.                                   §1.8
```

=== 1.2 The whole pipeline, wire and disk to tick

```
LIVE ── the recording direction                          REPLAY ── the reading direction
┌────────────────────────────────────┐                   ┌────────────────────────────────────────┐
│ BatchTrades · BookUpdate · Oi · Mc │                   │ data/<lane>/<sym>/{ts_min}_{ts_max}.pq  │
│   one whole VENUE MESSAGE          │                   │   files partition time on the ROW's own │
└────────────────────────────────────┘                   │   axis (Venue for four lanes, Local     │
   │ Sink::push — clock-free, cloneable, one send        │   for Mc); overlap is a write error     │
   ▼         per message, not per element                └────────────────────────────────────────┘
 mpsc (per-MESSAGE, one channel — per-lane channels                │ list_range(key, start, end) — prune by interval
       could not preserve cross-lane order)                        ▼
   │                                                       LaneReader<T>  streams one parquet file at a
   ▼  Live::ingest — ONE thread, ONE point                    time; filters ts_axis ∈ [start, end];
 Arrival = max(clock.now_ns(), last+1)   ◀── THE DECISION      asserts schema_version and metadata
   │                                                          consistency across the whole range
   ├─ recv_of(a) ⇒ Ts<Local> written onto every row ─────┐            │
   ├─ ShadowBook::ingest  ⇒ DeltaFrame{Update|Correction}│            ▼  effective(recv, axis, sampler)
   │  ShadowBook::checkpoint ⇒ BookShape (our cadence)   │      Some(recv) ⇒ that IS the live key, read back
   ├─ tee ⇒ Feather<T> builders ⇒ RecordBatch ⇒ parquet ─┘      None       ⇒ seeded latency draw off the
   └─ Lane<C>: buf.extend(cols) ; ts.resize(+n, a)                          venue axis (historic ingest)
                    │                                                       │
                    └──────────────────┬────────────────────────────────────┘
                                       ▼
                        ┌──────────────────────────────────┐
                        │ Weaver — five Lane<C>, one key   │   §1.4 the lane
                        │ space, one merge                 │   §1.5 the step
                        └──────────────────────────────────┘
                                       │  next() ⇒ ONE lane's run
                                       ▼
                        Lanes<'t> { arrival, ts_venue, trades, deltas, anchor, oi, mc }
                                       │  impl From<Lanes<'t>> for Batches<'t>  — the app's WHOLE
                                       ▼  routing layer. No discriminant: batch-ness iterates.
                        Graph::tick(lanes.ts_venue.as_nanos(), batches)
```

The two directions are one code path from the `Lane` down. That is the entire reason the round-trip
is evidence of anything: `Live` and `Replay` differ only in how a `Lane` gets filled.

=== 1.3 `Arrival` — the one key, and the only thing it may do

```
 provenance                              feed     when
 ─────────────────────────────────────────────────────────────────────────────────────────────
 Live::stamp   max(clock.now_ns, last+1) Live     always. Single-threaded, so no atomics and no
                                                  cross-thread race — and STRICTLY increasing, so
                                                  every event is globally uniquely ordered.
 recorded  Ts<Local> ts_local_recv       Replay   we were there. It is not a reading of the same
                                                  event; it IS the live key, written down.
 sampler.arrival(ts_venue_exec)          Replay   we were not (historic ingest, recv = None).
                                                  Seeded on "{lane:?}:{symbol}" ⇒ two runs of the
                                                  same range weave identically.

 `Arrival` is deliberately NOT subtractable. Two arrivals differ by no duration: one of them may be
 `last + 1` because the clock had not moved between two messages. `ReadClock::cell_end` is the ONLY
 arithmetic an arrival admits, and `ReadClock` is a newtype over `Exact` so that it stays the only
 one. Monotone BY CONSTRUCTION, not by measurement — which is what lets `Live` skip the watermark
 entirely (§1.7).

 `Arrival::MIN` is a real key, not a sentinel for "unset". A book checkpoint picked from before the
 replay range is state CARRIED INTO the range — it has no arrival of its own and must sort before
 everything. Hence `Weaver::prev_emit` starts at `MIN` rather than at the epoch: an epoch-defaulted
 watermark would read the very first emission as out of order. Hence too `cell_end` saturates.
```

`ReadClock` is the second axis, and it is a cost knob rather than a correctness one:

```
   ReadClock(Exact)      arrival time cut into cells; one tick carries AT MOST one cell.
   ReadClock::EVENT      Exact(0) — every distinct arrival is its own cell. The degenerate case,
                         reached through the same `cell_end`, not through a special path.

   cell_end(a)  =  floor(a, step) + step - 1        the last arrival still inside a's cell

   ABSOLUTE — floored from the epoch, never measured off the last emission. Which cell an event
   falls in is a property of THE EVENT ALONE: identical across runs, and independent of what the
   feed did before it. A relative window would make a run's contents a function of the feed's
   idleness, and `Live` is idle in places `Replay` is not — the round-trip would break on grouping
   before it ever reached a value.

   It never WAITS for a cell to fill. A cell that already holds data is handed over as it stands.
   That is `feeds.live.on-arrival` satisfied by construction rather than by a `Live`-only branch,
   and it is what `feeds.replay.grid-optional` demands of any grouping rule: exist only if it
   degenerates to on-arrival under `Live`. This one caps a run; it does not create one.
```

=== 1.4 The lane — the key beside the datum

```
  struct Lane<C> { ts: Vec<Arrival>, buf: C, cur: usize }

  The key lives BESIDE the datum, not in it: ordering is a device we impose, not a property the
  venue told us. `buf` is the columnar holder the rest of the system already speaks (`TradeBuf`,
  `DeltaBuf`, `Vec<Oi>`, `Vec<Mc>`, `Vec<BookShape>`), reached through one `LaneBuf` trait whose
  entire surface is `len` and `drain_prefix`.

  ONE KEY PER ELEMENT, and a whole message's elements share one value. Two consequences, both
  wanted:
    · the merge walks a single index space — no nested "which message, which element" cursor
    · a venue message can never SPLIT across ticks. `run_end` takes keys `<= bound` and `bound >=
      win_ts`, so a message is all-in or all-out. Atomicity falls out; nothing enforces it.

  `key(ts, n)` debug-asserts arrivals are non-decreasing and that keys and data stayed in step —
  the two ways a lane can silently stop meaning anything.

  compact()   drops the emitted prefix once `cur * 2 >= ts.len()`. Amortized O(1) per element, and
              the reason a `Live` session's memory is bounded to its UN-EMITTED window rather than
              to its length. Called at the top of the next `next()`, not at the end of this one:
              the `Lanes<'_>` handed out still borrows the buffer at end-of-tick.
```

=== 1.5 The step — one winner, two bounds, one run

```
  1.  heads    = [trades, deltas, anchors, oi, mc].map(head)      lane order is the tie-break
  2.  winner   = argmin over Some(head)                            `min_by_key` ⇒ FIRST minimum
                 ⇒ on an exact tie the lower lane index wins, deterministically. Load-bearing:
                   one book message emits a frame AND a checkpoint at the SAME stamp, and deltas(1)
                   before anchors(2) is what makes the replayed order match the live one.
  3.  bound    = min( min other head , clock.cell_end(win_ts) )
                      └── preserves order ACROSS lanes    └── caps grouping WITHIN one lane
                          this one is CORRECTNESS              this one is COST
                 (no other lane has a head ⇒ i64::MAX: nothing left to interleave against)
  4.  assert     win_ts >= prev_emit                       the whole weave, in one line
  5.  run      = buf[cur .. run_end(bound)]                maximal prefix whose keys stay <= bound
                 deltas use `run_end_of_kind`: a run also breaks where `FrameKind` changes, because
                 a `DeltaFrame` wraps the RUN, not the row — an Update and a Correction are not the
                 same kind of thing and must not reach a consumer as one frame.
  6.  prev_emit = the run's LAST key; that is also `Lanes::arrival`.
      ts_venue  = the run's LAST element's venue reading — the tick's event time.
```

```
  A STEP CARRIES EXACTLY ONE LANE'S RUN. The other four are empty.

  Not a limitation — it is the carrier of order. Two lanes in one step would reach the graph as
  simultaneous, and be folded in `Deps` order instead of arrival order. `rates.folds.exactly-once`
  is a statement about the ELEMENT SEQUENCE, and the element sequence is only a sequence while one
  lane holds the step. `ReadClock` therefore coarsens ticks strictly WITHIN a lane; it never merges
  across.

  And a run's shape is that lane's nature:
     TradeCols<'t> / &[Oi] / &[Mc]        a FLOW      — every element is an event
     DeltaFrame<'t>                       a flow WITH IDENTITY — hence the kind break
     Option<&BookShape>                   a LEVEL     — `buf[a.0..a.1].last()`: a run of
                                                        checkpoints collapses to the newest, because
                                                        the older ones are superseded, not skipped
```

#align(center, diagram(
  spacing: (13mm, 10mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node(
    (0, 0),
    align(center)[`heads[5]` \ #text(7pt)[trades · deltas · anchors · oi · mc]],
    fill: luma(235),
    name: <heads>,
  ),

  node(
    (-1.4, 1.2),
    align(center)[`winner` \ #text(7pt)[min; ties ⇒ lowest index]],
    name: <win>,
  ),
  node((1.4, 1.2), align(center)[`bound`], fill: rgb("#f2ecdc"), name: <bound>),

  edge(<heads>, <win>, "->"),
  edge(<heads>, <bound>, "->"),

  node(
    (0.5, 2.5),
    align(center)[next other head \ #text(7pt)[order ACROSS lanes]],
    fill: rgb("#e4edf5"),
    name: <other>,
  ),
  node(
    (2.4, 2.5),
    align(center)[`cell_end(win_ts)` \ #text(7pt)[grouping WITHIN a lane]],
    fill: rgb("#e9f1e4"),
    name: <cell>,
  ),

  edge(
    <bound>,
    <other>,
    "->",
    label: [correctness],
    label-size: 7pt,
    label-side: left,
  ),
  edge(<bound>, <cell>, "->", label: [cost], label-size: 7pt),

  node(
    (-1.4, 2.5),
    align(center)[`assert win_ts >= prev_emit`],
    fill: rgb("#f6e6e6"),
    name: <mono>,
  ),
  edge(<win>, <mono>, "->"),

  node(
    (0, 3.9),
    align(
      center,
    )[`run = buf[cur .. run_end(bound)]` \ #text(7pt)[deltas also break on `FrameKind`]],
    fill: luma(235),
    name: <run>,
  ),
  edge(<mono>, <run>, "->"),
  edge(<other>, <run>, "->"),
  edge(<cell>, <run>, "->"),

  node(
    (0, 5.1),
    align(
      center,
    )[`Lanes<'t>` \ #text(7pt)[ONE lane populated · `arrival` = run's last key · `ts_venue` = run's last element]],
    fill: luma(235),
    name: <lanes>,
  ),
  edge(<run>, <lanes>, "->"),
))

=== 1.6 The five lanes

#table(
  columns: (auto, auto, auto, auto, auto, 1fr),
  stroke: 0.4pt + luma(180),
  align: (left, left, left, left, left, left),
  table.header([*lane*], [*row*], [*axis*], [*weave shape*], [*key comes from*], [*notes*]),
  [`Trades`], [`Trade`], [`Venue`], [`TradeCols<'t>`], [recorded or sampled], [`ts_local_recv: Option` — absent means historic ingest, we were not there.],

  [`BookDeltas`], [`BookDelta`], [`Venue`], [`DeltaFrame<'t>`], [recorded], [written ONLY by a live recording, so `ts_local_recv` is not optional. Carries `kind`, not the venue's `gapped`.],

  [`BookAnchors`], [`BookSnapshot`], [`Venue`], [`Option<&BookShape>`], [recorded, or `Arrival::MIN`], [our checkpoints, on our cadence (60 s). The pre-range one is state carried in.],

  [`Oi`], [`Oi`], [`Venue`], [`&[Oi]`], [recorded or sampled], [plain slice; genuinely `f64`.],

  [`Mc`], [`Mc`], [`Local`], [`&[Mc]`], [its own reading], [no venue event time exists, so this lane's axis is our clock. `mc_axis` is the one named crossing.],
)

```
  Which lanes are opened is not the caller's taste: `required_lanes::<G>()` maps the graph's
  `Roots::required_events()` TypeIds to `LaneKind`s. A lane no output reaches is never read — and
  because that walk already deduped nothing, `Replay::new` dedupes again by hand: a graph naming
  both book roots would otherwise load AND ACCUMULATE the delta lane twice, and in `Live` an
  un-drained duplicate grows without bound. Deltas and anchors are separate `LaneKind`s for exactly
  that reason — naming one must not load the other.

  Precision is the holder's, hoisted once per run from the file's schema metadata. No file under any
  candidate key ⇒ PANIC, never a default: a default would scale every raw price and qty by 1, wrong
  by whole orders of magnitude, on output that still looks like data.

  MAX_ANCHOR_AGE is not a constant in this crate. It is read off the graph:
      <<Book as Node>::Deps as DepSet>::REACH[1]   must be a `Horizon::Span(tf)`, else compile error
  The reader looks back exactly as far as the folding node DECLARED it reaches. Bounding a read, not
  bounding drift — our own delta lane is gapless by construction, so a miss here means a gap in our
  own recording.
```

=== 1.7 The two feeds — fill, then drain

```
  Replay ── FILL, then merge
    eager: the whole [start, end] range of every required lane is loaded before the first `next()`.
    Streams from disk one parquet file at a time while filling, so the peak is the lane vectors,
    not the file set. Per-lane exhaustion is known from the start, so `is_empty` is the end of the
    stream and there is no watermark question to ask.
      ponytail: day-scale ranges only (a day of trades is a few MB). A larger window is CHAINED —
      successive `Replay`s over one long-lived graph, which carries its node state across; only the
      per-lane latency seed resets, deterministically.

  Live ── DRAIN, then merge, then reclaim
    next():  drop our own `tx`  ── so the channel can disconnect once external sinks are gone
             compact()          ── the previous step's borrow has ended
             while weaver.is_empty():
                 rx.recv()      ── BLOCK for the first event only
                 while rx.try_recv(): ingest    ── then take everything already queued
             weaver.next()

    The whole current buffer is safe to emit with NO WATERMARK, because the stamp is monotone by
    construction: everything ingested is `<= last_stamp` and every future event will be `>`. So what
    is buffered is always a COMPLETE PREFIX of the stream. That single property buys all three of
    the things `feeds.live.*` demands:
       · bounded memory        (nothing is held back "in case something older shows up")
       · no end-of-stream buffering
       · no quiet-lane stall   (an `Mc` lane that speaks once a day holds nothing back)

    A per-lane channel would be faster to write and would lose exactly the thing that makes this
    work: the total order across lanes. So dispatch is per MESSAGE — acceptable at network rates,
    and one clock read, one flush check and one send per venue message rather than per trade.

    `Sink::push` asserts the receive end is alive. A pump that outlives its feed costs bandwidth and
    records nothing; orderly shutdown is to drop the sinks first, which is what disconnects it.
    `Drop for Live` runs `final_flush` — draining to `None` is only ONE way a session ends, and every
    other one (an early break, a `?`, a panic upstream) would otherwise leave each lane's un-rotated
    tail on the floor.
```

=== 1.8 Record the decision, not the evidence

```
  This is the crate's shape, and it is one rule applied four times: anything a replay would have had
  to RE-DERIVE from circumstance is decided once at ingest and written down.

    what                     decided at ingest by       persisted as              replay does
    ───────────────────────────────────────────────────────────────────────────────────────────────
    arrival ORDER            Live::stamp                ts_local_recv on the row   reads it back
    book RECONCILIATION      ShadowBook                 FrameKind + a gapless      folds it, blind
                                                        monotonic_seq + OUR
                                                        checkpoints
    lane PRECISION           Feather (schema metadata)  file key/value metadata    hoists it
    file EXTENT              Feather rotation           {ts_min}_{ts_max}.parquet  prunes on it

  `recv_of` is the sharpest case and reads like a triviality: the recorded reception is the ARRIVAL
  KEY reinterpreted, not a second `clock.now_ns()`. A second read could TIE where the key could not,
  and a tie is an order the replay is free to resolve differently. The `+1` monotonisation may
  introduce is far below wire-latency resolution, so nothing is lost by using the key as the reading.

  `ShadowBook` is the largest case. A gap or a resync is a fact about OUR CONNECTION, not about the
  market. Stored raw, every replay would re-derive the reconciliation from whichever venue snapshots
  happened to land — a cadence we neither control nor can reproduce.

     venue input                    emitted                       persisted
     ─────────────────────────────────────────────────────────────────────────────────────
     delta, chain intact            DeltaFrame::Update            the levels, verbatim
     delta, gapped                  DeltaFrame::Correction        levels + kind
     snapshot agreeing with us      nothing                       nothing
     snapshot disagreeing           DeltaFrame::Correction        exactly the diffs
     our cadence elapsed            BookShape checkpoint          a snapshot row
     venue snapshot                 —                             NEVER stored

  So the delta lane on disk is our own recollection, gapless and self-consistent; `Book::step` folds
  both kinds identically, and the kind survives as the frame's IDENTITY so a flow or imbalance node
  must say which it means and cannot fabricate signal out of a dropped websocket packet. The one
  step that was nondeterministic has been moved off the replay path entirely.

  The exception proves the rule: a row whose `ts_local_recv` is `None` had no decision recorded
  because we were not there. That one is re-derived — and made deterministic by seeding the sampler
  on the lane and symbol, so the manufacture is at least reproducible.
```

#align(center, diagram(
  spacing: (15mm, 9mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node(
    (0, 0),
    align(center)[venue stream \ #text(7pt)[trades · book · oi]],
    fill: luma(235),
    name: <venue>,
  ),
  node(
    (0, 1.2),
    align(center)[`Live::ingest` \ #text(7pt)[one thread, one point]],
    fill: rgb("#f6e6e6"),
    name: <ing>,
  ),
  edge(<venue>, <ing>, "->", label: [`Sink`], label-size: 7pt),

  node((-1.6, 2.4), align(center)[`Arrival` stamp], name: <st>),
  node((0, 2.4), align(center)[`ShadowBook`], name: <sb>),
  node((1.6, 2.4), align(center)[precision], name: <pr>),

  edge(<ing>, <st>, "->"),
  edge(<ing>, <sb>, "->"),
  edge(<ing>, <pr>, "->"),

  node(
    (0, 3.7),
    align(
      center,
    )[parquet lanes, teed incrementally \ #text(7pt)[`ts_local_recv` · `FrameKind` + gapless `seq` · schema metadata]],
    fill: rgb("#e4edf5"),
    name: <disk>,
  ),
  edge(<st>, <disk>, "->"),
  edge(<sb>, <disk>, "->"),
  edge(<pr>, <disk>, "->"),

  node((0, 5.2), align(center)[`Replay`], fill: luma(235), name: <rep>),
  edge(
    <disk>,
    <rep>,
    "->",
    label: [reads the DECISION, re-derives nothing],
    label-size: 7pt,
    label-side: left,
  ),

  node(
    (2.6, 5.2),
    align(center)[`ts_local_recv = None` \ #text(7pt)[historic ingest]],
    fill: rgb("#f2ecdc"),
    name: <hist>,
  ),
  edge(
    <hist>,
    <rep>,
    "->",
    label: [seeded latency draw],
    label-size: 7pt,
    label-side: right,
  ),
))

=== 1.9 What the weave promises, and what it does not

```
  PROMISED
    · the emission order is non-decreasing in `Arrival`, and asserted at every step (not debug-only)
    · a venue message never splits across ticks
    · a step carries exactly one lane
    · under `Live`, memory is bounded to the un-emitted window and no lane is held for another
    · given the same catalog, the same `[start, end]`, the same `ReadClock` and the same latency
      seed, `Replay` is bit-identical run to run

  NOT PROMISED — and worth naming, since they are the edges a caller can walk off
    · `Lanes::ts_venue` — the timestamp handed to `Graph::tick` — is NOT monotone. It is the venue
      reading of the run's last element, and two lanes with different wire latencies deliver
      decreasing venue times in increasing arrival order. Nothing asserts otherwise. The only engine
      consumer of that number is `Emitter::opens`, which compares `ts / period != last_period`
      rather than ordering it, so a step backwards re-opens a period rather than corrupting one.
      Per-element `Stamped::ts_ns` — what a `Buffer` actually trims and partitions on — is sorted
      within a lane, which is where sortedness is needed.
    · how a run was CHUNKED is not a fact a node may read (`rates.deps.tick-opaque`), and the weaver
      does not try to make chunking stable across feeds. §2 is what that costs and what it buys.
    · `Replay` is not memory-bounded. That is a stated ceiling, not an oversight.
```

=== 1.10 The enforcement points, in full

```
  Lane::key            debug: arrivals non-decreasing · keys and data in step — the two ways a lane
                       stops meaning anything without failing
  Weaver::next         assert: win_ts >= prev_emit. Release-mode, because it is the invariant
  ReadClock::cell_end  assert: step >= 0 — a read clock cannot run backwards
  Live::stamp          max(clock, last+1): monotonicity is CONSTRUCTED, so there is nothing to check
  Sink::push           assert: the receiver is alive — a pump outliving its feed records nothing
  Live::sink           expect: sinks are taken before the feed is consumed (the first `next` drops
                       the feed's own sender)
  Live::ingest         assert: a trade batch's precision equals the lane's
  Replay::new          panic: the catalog holds no file for a required lane. A default precision is
                       the one failure that would produce plausible-looking wrong numbers
  effective            expect: a historic lane needs a latency sampler
  read.rs              assert: `schema_version` matches exactly (no silent cross-version reads) ·
                       file metadata consistent across the read range
  read.rs              const:  `MAX_ANCHOR_AGE` is a `Horizon::Span` on `Book`'s declared reach, or
                       it does not compile
  Catalog::write       error:  OverlappingInterval — a lane's files partition time, they do not
                       overlap it
  Feather::push        interval bounds are on the axis readers FILTER on. When they tracked reception
                       instead, a window narrower than the reception lag pruned files that do hold
                       matching rows (regression test in `read.rs`)
  Book::step           a `monotonic_seq` discontinuity desyncs; only a checkpoint re-arms, and from
                       the checkpoint's state rather than the stale one
  Drop for Live        `final_flush` on every exit path, not only the drained one
```

== 2. What the round-trip actually proves

`examples/sync_round_trip.rs` records a `Live` session and replays the catalog it just wrote. It
asserts two different things, at two different strengths, and the difference is the model.

```
  main()  — every push happens BEFORE the first `next()`, so `Live` has the whole stream buffered
            and groups by cell exactly as `Replay` does.
            ⇒ STEP-FOR-STEP equality of runs: trade contents, delta contents, `Correction` flag,
              anchor shape, oi — AND the `(epoch, len, best_bid, best_ask)` a consumer's `Book`
              folds out of them at every step.
            The book assertion is the one that pays for the shadow layer (§1.8): replaying our own
            delta lane must land on a bit-identical book, not merely "a snapshot appeared in both".

  concurrent_streaming_matches_replay()
          — a producer thread sleeps 100 µs between messages, so the consumer finds the channel
            empty and BLOCKS. `Live` groups one message per tick; `Replay` still groups by cell.
            ⇒ only the ELEMENT stream is comparable: `flat()` to `(lane_tag, seq)` per event,
              "robust to batch chunking".

  That asymmetry is not a weakness of the test. It IS `rates.deps.tick-opaque`, observed:

      the STRONG form holds where the two feeds happen to see the same buffering
      the WEAK form is what the framework actually promises
      and a node is only allowed to depend on the weak one

  Which is also why the `ReadClock` may exist at all. It changes chunking; chunking is unobservable;
  therefore it is a free knob on the CPU bill — `spl` prints "N ticks in Ts on a <read clock> read
  clock" and tunes it from config. A framework where a node could see the grouping would have no
  such knob, and no round-trip either: the two feeds group differently BY NATURE, so any node that
  could see the grouping would break the equivalence the round-trip exists to demonstrate.
```

```
  The chain of custody, end to end, as one sentence per link:

    the venue said it            ── ts_venue_exec, and the file bounds and query bounds are on it
    we received it               ── Live::stamp, ONE thread, ONE point, monotone by construction
    we wrote down that we did    ── recv_of ⇒ ts_local_recv, the key itself and not a second reading
    we reconciled it once        ── ShadowBook: gaps and disagreements become OUR frames and kinds
    we ordered it by receipt     ── Weaver: min head, bounded by the next head and by the cell
    we handed it over whole      ── one lane, one run, a venue message never split
    and the graph never learned  ── how any of it was chunked
```
