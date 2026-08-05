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

= `trading_data_dag` — the utilization framework

Read off `trading_data_dag/src/lib.rs` and `trading_data_macros/`; the census in §2 is counted, at
compile time, off the crate trees under `examples/`.

== 1. The framework

=== 1.1 The whole pipeline, declaration to observation

```
DECLARATION ── the entire authored surface of a graph is five lines
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  graph! {  pub struct G;  batches Batches;  roots { f: C[Event], .. };                │
│            out TickOut;   outputs { name: Cell, .. }  }                               │
└──────────────────────────────────────────────────────────────────────────────────────┘
        │   nothing else is stated: no node list, no order, no buffer size, no gate badge
        ▼
DERIVATION ── proc-macro walk of `type Deps`, backwards from `outputs`
   ├─ node set     reachable(outputs). A node no output reaches is not instantiated —
   │               and neither is the root lane that would have fed it.
   ├─ topo order   post-order of that walk. `Pull`'s `Has` bound re-proves it in the
   │               type system, so a mis-ordered sweep is a compile error, not a bug.
   ├─ buffers      one `Buffer<C, K>` per series, K = ⋁ { J : some consumer said
   │               `Buffering<C, J>` }. K is nobody's to declare.
   ├─ demand       per node, a formula in DNF: ⋁ over consumers c of (demand(c) ∧ ⋀ gates(c)).
   │               `true` ⇒ somebody reads it always.
   │                 · a latch dominates only what declares `Cell::REWARMS` (it is read one
   │                   tick ahead — §1.3), and a gate the sweep has not reached yet not at all
   │                 · anything holding history is pinned  (Folding/Spanning dep, Buffer,
   │                   latch, gate itself — state cannot re-warm through a skip)
   ├─ latches      `Cut` must gate on the latch (`cut_gated`); the arm must not be gated
   │               by it (`deadlocked`) — a latch whose arm it darkens never re-arms.
   └─ roots        `required_events()` — the TypeIds the walk actually reached, which the
                   facade maps to source lanes.
        │
        ▼
SWEEP ── `tick(ts, Batches)`, one straight-line monomorphized fn; no dispatch, no runtime graph
   the tick's event time and its batches are the whole input — `ts` is what a declared `CLOCK` reads
   Batches ──seed──▶ Cons<'t, R₀, Nil> ──step──▶ Cons<'t, N₁, ·> ──step──▶ … ──▶ TickOut
                     └────────────────── the frame: a type-indexed HList ──────────────┘
                        `Has<'t, N, I>` resolves a cell by TYPE; `I` is the inferred index
                        path, never named. Two instances of one node type ⇒ ambiguous ⇒
                        compile error.
        │
        ▼
OBSERVATION ── the same sweep, read. `()` observer ⇒ `want() = Nothing` ⇒ erased entirely.
   Observer::on(name, dep names, dep gate flags, Fire { vals, jac, exact, formula, … })
   Step order IS topo order, so the observed sequence doubles as the static topology; a dep
   name never seen as a stepped node is a root.
```

#align(center, diagram(
  spacing: (11mm, 10mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node((0, 0), align(center)[`graph!` \ #text(7pt)[roots · out · outputs]], fill: luma(235), name: <decl>),

  node((-2.0, 1.3), [node set], name: <set>),
  node((-0.9, 1.3), [topo order], name: <topo>),
  node((0.3, 1.3), [buffers `K = ⋁J`], name: <bufs>),
  node((1.5, 1.3), [demand], name: <dem>),
  node((2.6, 1.3), [latch checks], name: <latch>),

  edge(<decl>, <set>, "->", label: [walk `Deps` backwards], label-pos: 0.72, label-side: right),
  edge(<decl>, <topo>, "->"),
  edge(<decl>, <bufs>, "->"),
  edge(<decl>, <dem>, "->"),
  edge(<decl>, <latch>, "->"),

  node((0.3, 2.7), align(center)[`fn tick(ts, Batches) -> TickOut` \ #text(7pt)[one straight-line monomorphized fn]], fill: luma(235), name: <sweep>),
  edge(<set>, <sweep>, "->"),
  edge(<topo>, <sweep>, "->"),
  edge(<bufs>, <sweep>, "->"),
  edge(<dem>, <sweep>, "->", label: [suppress], label-size: 7pt),
  edge(<latch>, <sweep>, "->", label: [commutate], label-size: 7pt),

  node((-1.4, 4.0), align(center)[`Cons<'t, N, T>` \ #text(7pt)[type-indexed HList]], name: <frame>),
  node((0.3, 4.0), align(center)[`step` family], name: <step>),
  node((2.1, 4.0), align(center)[`Observer::on` \ #text(7pt)[`Fire { vals, jac, formula, … }`]], name: <obs>),

  edge(<sweep>, <step>, "->"),
  edge(<step>, <frame>, "->", bend: -25deg, label: [prepends the out], label-size: 7pt),
  edge(<frame>, <step>, "->", bend: -25deg, label: [`Pull` / `Has`], label-size: 7pt),
  edge(<step>, <obs>, "->", label: [`want()`], label-size: 7pt),
))

=== 1.2 The step family — the one axis is _who decided not to run_

```
                    plain           gate-closed       no standing demand      + observer
  level node        step            step   (Dark)     step_when_obs (Latent)  step_obs
  emit node         step_emit       step_emit         step_emit(demanded=0)   step_emit_obs

   Dark<B: Bit> dispatches on `DepSet::Lead` — the LEADING dep's `Cell::Gates`:
      Dark<No>  ⇒ unreachable!()      an ungated node has no dark branch to evaluate
      Dark<Yes> ⇒ Latent::latent()    so the `Latent` bound lands on gated nodes ALONE
   `Latent`:  Option<T> ⇒ None   ·   &[T] ⇒ &[]   (not emitting IS the latent reading)
   A suppressed (undemanded) node is bounded by `Latent` outright — it is dark whatever its
   own `Deps` say. That is the whole obligation the demand pass puts back on an author, and
   it asks for the TYPE: an out with no unfired reading cannot be skipped, and says so at
   compile time.
   One observed step family, not two: what a fired node reports is its `Level` kernel's
   business, so an exact reading and a finite-difference one are the same call.
   A declared `CLOCK` adds no row: `Emitter::opens` is read INSIDE `step_emit`, after the gate — so
   a shut node consumes no period, and the first tick it is let through is a boundary rather than
   the remainder of one it slept out.
```

=== 1.3 The dep vocabulary — `type Deps` *is* the graph

Six spellings, no seventh. Every structural fact about an edge is one of these.

```
 spelling                  out read                 resolves against   REACH      FOLDED  RETAINED  Gates
 ─────────────────────────────────────────────────────────────────────────────────────────────────────────
 C                         C::Out<'t>               C                  Unit       false   false     No
 Gating<G: Gate>           bool  — permission       G                  Unit       false   false     YES
 Buffering<C: Series, H>   C::Batch's own view      Buffer<C, K≥H>     H          false   TRUE      No
 Sampling<C: Series>       Option<Item::Val>        Latest<C>          Unit       false   TRUE      No
 Folding<C, H>             C::Out<'t>  (a claim)    C                  H          TRUE    false     No
 Spanning<C, TF>           C::Out<'t>  (a claim)    C                  Span(TF)   TRUE    false     No

 `Spanning` exists only because `Folding<C, { Horizon::Over(TF) }>` does not parse: an enum
 constructor applied to a generic parameter is rejected in const-argument position.

 Every wrapper forwards `Cell::NAME = C::NAME` and `Cell::CLOCK = C::CLOCK` — the graph predicates
 match dep names against frame cell names, and a wrapper that renamed or re-rated its dep would drop
 out of all of them. REACH / FOLDED / RETAINED / Gates are what then say WHICH reading of C is being
 asked for.

 WHAT a `Buffering` reads is the series' own to say: `Series::Batch` is how the engine ACCUMULATES
 the reach, and its `View` is the out. `Rows<Item>` — every row, handed out as a `Hist` — is the
 default and what every series but one takes.

 `Hist<'t, T>` = past ++ fresh, cut to the CONSUMER's declared horizon — so a node reads what a
 frame buffering at exactly its own `H` would hold, and shortening some unrelated consumer's window
 cannot silently change this one's results.
   .fresh()        byte-identical to the unbuffered series out
   .past() .all()  the cross-rate view, for a consumer clocked by a faster series
   .trailing()     one window per fresh element — rate preservation for free
   .narrowed(h)    a shallower view; asserts the retained reach serves it

 The cost of the seventh thing: `Horizon` means whatever the `Batch` impl makes it mean. `Rows`
 SLIDES — it trims by `ts_ns` every tick, so `Over(tf)` is the last `tf` of wall clock ending now.
 A batch that FOLDS cannot un-fold, so it has nothing to trim and can only TUMBLE: reset on the
 absolute boundary, floored from the epoch. Same declaration, different window. What contains it is
 that the views are different TYPES, so no consumer can read one as the other, and that the impl
 owns the meaning and documents it (`trading_data_core::BookChunk` is the one such impl).
```

_Who holds the history_ is the axis the wrappers actually partition:

```
        ┌── engine holds it ──────────┬── node holds it ────────┬── engine holds ONE ──┐
        │   Buffering<C, H>           │   Folding / Spanning    │   Sampling<C>        │
        │   → Buffer<C, K>  (a node)  │   → nothing retained    │   → Latest<C> (node) │
        │   re-warms through a skip   │   CANNOT re-warm ⇒      │   monotone: once it  │
        │   ⇒ darkening a consumer    │   `Gating` + `Folding`  │   holds a level, it  │
        │     is cheap                │   is a COMPILE ERROR    │   holds one forever  │
        │   a collapsing series pays  │                         │                      │
        │   its OWN `Batch` for this  │                         │                      │
        │   RETAINED = true           │   RETAINED = false      │   RETAINED = true    │
        └─────────────────────────────┴─────────────────────────┴──────────────────────┘
                            the two carve-outs the whole design turns on

 `Cell::RETAINED` is that partition as a const, and a bare `C` sits with the middle column: its out
 is this tick's batch and no other. It answers a second question besides re-warming — whether a tick
 may be WITHHELD from a consumer (§1.4). A retained dep is there again next tick; a pass-through dep
 skipped is a batch nobody sees again.
```

Demand reads these edges backwards, and what it derives is a FORMULA, not a set:

```
   demand(i) = ⋁ over consumers c of ( demand(c) ∧ ⋀ gates(c) )
   demand(i) = true   where i is an output, or pinned (own fold · a frame buffer · a latch · a gate)

 A set could only have meant AND, and two consumers behind DIFFERENT gates intersect to ∅ — which
 reads as "always demanded". Sound, and the degenerate answer. `{A}` and `{A,B}` still give `A`;
 `{A}` and `{B}` now give `A ∨ B` where the set gave `true`.

 WHICH gates may appear turns on WHEN a gate is readable, not on what it is:

   stepped earlier  read off the frame     ⇒ suppresses upstream on the SAME tick the consumer
                                             is dark — co-extensive, always safe, nothing declared
   a latch          read from `standing()` ⇒ suppresses ONE TICK AHEAD of the consumer arming,
                    at tick start             so only where the node says it survives that tick

 `Cell::REWARMS` is that permission, false by default — so no existing graph changes, and a node
 that does not opt in is unconditionally demanded wherever a latch reaches it. The consequence is
 not hidden: on the tick a latch ARMS, a node darkened by it is still dark, and wakes the tick
 after. A gate the sweep has not resolved yet is the other exemption — that disjunct degrades to
 standing demand rather than failing the build, which is exactly the answer the intersection gave.
```

#align(center, diagram(
  spacing: (15mm, 10mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node((0.4, 0), align(center)[*consumer's* `type Deps`], fill: luma(235), name: <cons>),

  node((-1.5, 1.3), [`C`], shape: rect, name: <bare>),
  node((-0.5, 1.3), [`Gating<G>`], fill: rgb("#f6e6e6"), name: <gat>),
  node((0.6, 1.3), [`Buffering<C,H>`], fill: rgb("#e4edf5"), name: <buf>),
  node((1.8, 1.3), [`Sampling<C>`], fill: rgb("#e4edf5"), name: <sam>),
  node((3.0, 1.3), align(center)[`Folding<C,H>` \ `Spanning<C,TF>`], fill: rgb("#e9f1e4"), name: <fold>),

  edge(<cons>, <bare>, "->"),
  edge(<cons>, <gat>, "->"),
  edge(<cons>, <buf>, "->"),
  edge(<cons>, <sam>, "->"),
  edge(<cons>, <fold>, "->"),

  node((-1.5, 2.7), [`C` in frame], name: <cellc>),
  node((-0.5, 2.7), [`G: Gate` in frame], name: <cellg>),
  node((0.6, 2.7), [`Buffer<C, K>`], name: <cellb>),
  node((1.8, 2.7), [`Latest<C>`], name: <celll>),
  node((3.0, 2.7), [`C` in frame], name: <cellf>),

  edge(<bare>, <cellc>, "->"),
  edge(<gat>, <cellg>, "->"),
  edge(<buf>, <cellb>, "->", label: [`K.serves(H)`], label-size: 7pt),
  edge(<sam>, <celll>, "->"),
  edge(<fold>, <cellf>, "->"),

  node((-0.5, 4.1), align(center)[permission, not data \ #text(7pt)[closed ⇒ `Dark` ⇒ no dep pulled]], fill: rgb("#f6e6e6"), name: <perm>),
  node((1.2, 4.1), align(center)[*engine* holds the history \ #text(7pt)[re-warms through a skip]], fill: rgb("#e4edf5"), name: <eng>),
  node((3.0, 4.1), align(center)[*node* holds the history \ #text(7pt)[cannot re-warm]], fill: rgb("#e9f1e4"), name: <own>),

  edge(<cellg>, <perm>, "->"),
  edge(<cellb>, <eng>, "->"),
  edge(<celll>, <eng>, "->"),
  edge(<cellf>, <own>, "->"),

  edge(<perm>, <own>, "-|>", bend: -32deg, stroke: (paint: red, thickness: 0.7pt), label: text(fill: red, size: 7pt)[`Gating` + `Folding` = compile error]),
))

The red edge is a fact about the SPELLING, not about any particular node — the fix is always to move
the reach into the frame and read it as `Buffering`. `trading_data_core::Book` is what that looked
like in practice: it held its own reach for as long as the delta lane was no `Series`, and became
gateable the moment the lane became a run of rows the engine could retain for it.

=== 1.4 `Horizon` and `CLOCK` — how far back, and how often

```
   Unit ──────── Elems(n) ──────── Over(tf) ──────── Unbounded         totally ordered
   this tick     n elements        wall clock        start of the run (recurrence / CVD)

   serves(self, req)   Elems(k) ⊒ Elems(j) ⟺ k ≥ j
                       Over(k)  ⊒ Over(j)  ⟺ k ≥ j
                       Over(_)  ⊒ Elems(_) always   (what it dropped is strictly older than
                                                     anything it kept)
                       Elems(_) ⊒ Over(_)  never    (a count cannot promise a span)

   join    the buffer's K. Total order ⇒ a graph can never ask for two reaches that cannot
           both be met at once.

   A `Buffer` const-asserts BOUNDED (Elems(k≥1) | Over(tf>0)). `Unit`/`Unbounded` in a
   `Buffering` are compile errors: Unit is the bare dep, Unbounded names no window.
   `Buffer::watermark` — the highest ts_ns it cannot speak for — is what makes "is this Over
   window complete" exact where "have I been running long enough" is a guess.

   span(self, clock)   Elems(n) ↦ n·clock   Over(tf) ↦ tf   Unit ↦ clock   Unbounded ↦ panic
           a count is a duration only once the PRODUCER's rate is known, and duration is what a
           replay preloads. The inverse (Over ↦ Elems) yields buffer capacity, which `Buffer`
           already gets by trimming on timestamps — no count needed.
```

```
  `Cell::CLOCK: Option<Timeframe>`  —  how often this cell publishes, stated on the cell because a
  rate is a property of what a thing IS and of nothing it reads.
     None      whenever its inputs do — today's behaviour, and the default.
     Some(tf)  over elements whose `tf` period has CLOSED; never re-entered while one is in
               progress, so how a period's messages were cut across its ticks is invisible.

  DECLARED by the node · ENFORCED by whoever can. `Emitter::opens` withholds a tick only from a node
  every one of whose deps is RETAINED (§1.3) — otherwise a withheld tick is a batch delivered to
  nobody. A node reading a batch is already clocked by the element walk it runs over it
  (`rates.folds.exactly-once`), and the engine takes the declaration and nothing else.

     Ohlcs<TF>   Spanning<Trades, TF>          not retained ⇒ its own boundary walk is the rate
     Bars<TF>    (Ohlcs<TF>, Volumes<TF>)      bare deps    ⇒ publishes when its producers do
     an indie    Sampling<C> / Buffering<C,H>  retained     ⇒ the engine opens it once per period

  `clock_divides` (§1.7) is what keeps a declaration honest: every feeding rate must tile it. That
  also pins a period spelled twice — `Bars<TF>` names TF in its type and reads `Ohlcs<TF>`, so any
  CLOCK but `Some(TF)` fails to build. It is the check standing in for a type-level equality Rust
  has no way to write: `Deps = (Spanning<Trades, {C::TF}>,)` wants `generic_const_exprs`.
```

=== 1.5 Node kinds — how a cell computes

```
  Cell                      type Out<'t>: Copy · NAME · REACH · FOLDED · RETAINED · REWARMS
   │                                              · CLOCK · Gates
   │                        the floor. A root is a Cell with no Node impl.
   │
   ├── Node                 type Kernel: Level<Self>   — NO METHOD. Written by `#[node]`,
   │    │                   never by hand: a node names the kernel that computes it, and the
   │    │                   set is SEALED, so there is no body the engine cannot also read.
   │    │                     Level::advance   the value
   │    │                     Level::pre/jac   the ONE-STEP derivative, priced per `Want`
   │    │                     Level::formula   the equation, where the kernel has one
   │    │                     Level::FIDELITY  Exact | Partial(omits) | Opaque(why) — how much
   │    │                                      of what the body READ the Jacobian covers, which
   │    │                                      is not the same question as whether it was
   │    │                                      differentiated or bumped
   │    │
   │    ├── Gate            Out<'t> = bool. Scalar-out always; the gated node may be batch,
   │    │    │              so a gated batch node's episode boundary is quantized to its
   │    │    │              batch window.
   │    │    └── Latch      + type Cut: Cell  ·  fn commutate()
   │    │         │         armed outside, cut from within — an SCR. `Cut` publishes an
   │    │         │         `Episode::terminal` out ⇒ graph! commutates and resets every node
   │    │         │         gated on it to `Default` at the NEXT tick's start (deferred: the
   │    │         │         frame still borrows batch fields at end-of-tick). One episode at
   │    │         │         a time; triggers during one are absorbed.
   │    │         │         + fn standing() -> bool — the contact BEFORE this tick's sweep, the
   │    │         │         one gate whose reading does not depend on sweep order and so the
   │    │         │         only one that can suppress what feeds it (§1.3). Read after the
   │    │         │         commutation, so a spent episode already reads open.
   │    │         └── Armed<N: Episodic>    the sealed-in latch: Cut = N by construction,
   │    │                                   Deps = (Folding<N::Trigger, Unbounded>,)
   │    │
   │    ├── Symbolic       fn body(&self, Vars) -> impl Expr    (Out = f64)  ⇒ kernel `Pure`
   │    │                   the algebra is load-bearing: there is no other way to state the
   │    │                   value. ≤ MAX_VARS = 8 deps, every dep scalar (const-asserted: a
   │    │                   vector dep desyncs Var<I>). Its Jacobian is `grad` — exact, one
   │    │                   pass, no clone and no re-advance.
   │    │
   │    └── Blind           const WHY · fn advance(&'t mut self, ..)          ⇒ kernel `Opaque`
   │                        SELF-BORROWS ⇒ a node lends its own buffer for the whole tick.
   │                        The stated hatch: `WHY` is the cost of using it, and `graph!`
   │                        counts it into `FIDELITY`. `Clone` is a supertrait because the
   │                        finite-difference witness is what it has instead of a derivative.
   │
   ├── Emit: Series         const WHY · fn emit(&mut self, EmitOuts, out: &mut Vec<Item>)
   │                        Out<'t> = &'t [Item]. The ENGINE owns the run (`Emitter<E>`), so
   │                        the struct holds only what it remembers between ticks — and
   │                        `emit` cannot read what it wrote last tick. `&mut self`, not
   │                        `&'t mut self`: only the buffer is lent, not the node.
   │                        `Emitter` also carries `last_period` — the declared rate is not the
   │                        declarer's to fiddle with, for the same reason the buffer is not.
   │
   └── Episodic             type Trigger: Cell  ·  fn arms(TriggerOut) -> bool
                            ⇒ `Armed<Self>` is the only gate it can be. Where a hand-written
                            `Latch` leaves the loop open (nothing checks that the gate you
                            armed is the gate your episode cuts), this closes it in the type.

  ENGINE-OWNED NODES — real frame cells nobody writes:  Buffer<C,H>   Latest<C>   Armed<N>
      each: ungated, historic, exactly one per series/episode, advances EVERY tick.
      Being warm is their whole job — that is what makes darkening a consumer cheap.
      Two `Buffer<C, _>` in one frame make every `Buffering<C, _>` ambiguous: same failure
      as two instances of any node type.
```

#align(center, diagram(
  spacing: (13mm, 10mm),
  node-corner-radius: 2pt,
  node-stroke: 0.7pt,
  label-size: 7.5pt,

  node((0, 0), align(center)[`Cell` \ #text(7pt)[`Out<'t>: Copy` · `NAME` · `REACH` · `FOLDED` · `RETAINED` · `REWARMS` · `CLOCK` · `Gates`]], fill: luma(235), name: <cell>),

  node((-2.4, 1.3), align(center)[`Symbolic` \ #text(7pt)[`body -> impl Expr`]], name: <sy>),
  node((-1.1, 1.3), align(center)[`Node` \ #text(7pt)[`type Kernel`, no method]], name: <nd>),
  node((0.2, 1.3), align(center)[`Emit: Series` \ #text(7pt)[engine owns the run]], name: <em>),
  node((2.4, 1.3), align(center)[`Episodic` \ #text(7pt)[`Trigger` · `arms`]], name: <ep>),

  edge(<cell>, <sy>, "->"),
  edge(<cell>, <nd>, "->"),
  edge(<cell>, <em>, "->"),
  edge(<cell>, <ep>, "->"),

  node((-3.6, 1.3), align(center)[`Blind` \ #text(7pt)[`WHY` · `advance` self-borrows]], name: <bl>),
  node((-3.0, 2.7), align(center)[`Level` (sealed) \ #text(7pt)[`Pure` · `Opaque`] \ #text(7pt)[`FIDELITY`: how much it covers]], fill: rgb("#eef0e4"), name: <lv>),
  node((1.1, 2.7), align(center)[`Gate` \ #text(7pt)[`Out = bool`]], name: <ga>),

  edge(<cell>, <bl>, "->"),
  edge(<sy>, <lv>, "->", label: [`Pure`], label-size: 7pt),
  edge(<bl>, <lv>, "->", label: [`Opaque`], label-size: 7pt),
  edge(<lv>, <nd>, "->", label: [`type Kernel`], label-size: 7pt),
  edge(<nd>, <ga>, "->"),

  node((-1.1, 4.1), align(center)[engine-owned nodes \ #text(7pt)[`Buffer<C,H>` · `Latest<C>`] \ #text(7pt)[ungated · historic · every tick]], fill: rgb("#e4edf5"), name: <eng2>),
  node((1.1, 4.1), align(center)[`Latch` \ #text(7pt)[`Cut` · `commutate` · `standing`]], name: <la>),
  node((2.4, 4.1), align(center)[`Armed<N>` \ #text(7pt)[`Cut = N` by construction]], fill: rgb("#f6e6e6"), name: <ar>),

  edge(<nd>, <eng2>, "->"),
  edge(<ga>, <la>, "->"),
  edge(<ep>, <ar>, "->", label: [the only gate it can be], label-side: left, label-size: 7pt),
  edge(<ar>, <la>, "->", label: [is a], label-size: 7pt),
))

=== 1.6 The out plane — what a value must be able to say about itself

```
  Flat        flat(&self, &mut [f64]) -> fired        DIMS/LEN: any rank, fixed shape
              NaN per SLOT = "no value there". Per-slot because a struct's fields warm
              independently; NaN because the buffer's arithmetic consumer is the
              finite-difference Jacobian, where absence is the absorbing element. The
              encoding STOPS at the engine — every boundary a human reads converts each
              empty slot back to a real `None`.
              &[T] flattens to its LAST element (the observer's end-of-batch view);
              fires() = len — rate is slice length, firing is element Option-ness.
              ABSENCE IS ONE THING. `None` — and the empty batch — IS not firing, not a
              fire carrying nothing: `Option<T>` fills NaN and returns false, so no reading
              downstream may give an absent out a meaning of its own. That makes the fired
              bit redundant with the slots being there, which every consumer is free to rely
              on, and LEN ≥ 1 (§1.7) is what keeps the two inseparable.
  Bump        bump(self, slot, h) -> (Self, dh)       dh = the step ACTUALLY taken. A raw
              column moves in whole ticks; a discrete slot returns 0.0 and its Jacobian
              column stays NaN rather than a fabricated zero.
  Nudge       stage(out) → Scratch → view() at a FRESH lifetime. That untying is the only
              reason re-`advance`ing a short-lived clone typechecks against a self-borrow.
              Declared by `slice_nudge!` / `value_nudge!`.
  DepFlat     the whole dep tuple concatenated, in `Deps` order ⇒ one FD column per element
  Jac         deps + their flattening + the row-major out_len × dep_len buffer a kernel
              fills. ONE array, holding ONE of the two quantities: the ONE-STEP reading —
              each dep's LAST element bumped, prior state held fixed. Differentiating and
              bumping land on that same number, so `exact` labels only how it was reached.
              The derivative over a dep's whole REACH is a different number, and nothing
              carries it yet (`r[kernels.jac.two-quantities]`).
  Glance      the one compact line a card shows; display-dual of `Flat`
  Episode     terminal() — `&[T]` is terminal if ANY element is (deliberately not `.last()`:
              that reads the value standing at end-of-batch, this asks whether the boundary
              was crossed anywhere in the run). Empty ⇒ false, so a dark node never
              self-commutates.
  Present     "did this element carry anything" — what `Latest` must ask before it keeps a
              level. The dominant item is `Option<f64>`, a rate-preserving decline, and
              retaining one of those would hold an absence forever.
  Stamped     ts_ns() — required of every buffered item; a history you cannot index by time
              is one you can only read at an assumed cadence
  Series      Out<'x> = &'x [Item] — the bufferable shape
  Latent      the unfired reading a gated / suppressed node returns
  Plot        slot groups with their own scale, guides, labels, inks, overlay/solo/bars.
              Axes partition by UNIT, so drawing NEVER motivates a node: a step that
              computes nothing, and differentiates to nothing, stays out of the topology.
```

=== 1.7 The enforcement points, in full

```
  Pull::open   const-asserts, at every use:
    (a) gating_leads(GATES)  — every `Gating` dep precedes every plain one. `open` reads the
        tuple left to right, so a closed gate short-circuits before a plain dep is read.
    (b) !any(GATES) || !any(FOLDS) — a gated node cannot hold its own reach. A closed gate
        pulls no deps, so a `Folding` dep never re-warms; retain it in the frame instead.
  step         `N::Deps: Pull<'t, F, I>` — a node stepped before its deps are in the frame
               does not compile, and cycles are unrepresentable. This is the engine's reason
               to exist; everything else is bookkeeping around it.
  Has          `Buffering<C,H>` against `Buffer<C,K>` const-asserts `K.serves(H)`, and that
               H is neither Unit nor Unbounded.
  Flats::of    `O::LEN > 0`, per out type, at every observed node — a zero-slot out would fire
               and leave the buffer indistinguishable from an unfired one, which is the one way
               absence could come to mean two things (§1.6).
  Plot         `Plot::coherent` — a multi-plot node must name each plot's slots.
  Level        sealed by a private supertrait — the kernel set is the framework's, and each
               kernel demands its body trait, so naming one you have no body for does not
               compile (`r[kernels.closed]`). `FIDELITY` is per kernel, and `graph!` counts
               Partial and Opaque apart into `FIDELITY` for a graph to pin
               (`r[kernels.fidelity.stated]`).
  Symbolic     every dep scalar, arity ≤ MAX_VARS.
  graph!       `distinct` node names · `cut_gated` · `deadlocked` · `clock_divides(CLOCK, CLOCKS)`
               — a node's declared rate must be a whole multiple of every rate feeding it, else it
               observes fractions of its inputs' elements. Item-level, so it fires for a node the
               graph merely contains; a `const {}` inside a generic fn waits on monomorphization.
```

== 2. Utilization census

// `.examples` is a symlink to `../examples`: `read` cannot escape the project root, which is this file's own directory

// only the crate roots are named; the rest of each tree is reached the way rustc reaches it
#let roots = (simple: ("lib", "main"), live: ("lib", "main"), live_equiv: ("main",), spl: ("lib", "main"))

#let walk(file, kids) = {
  let src = read(file)
  src.matches(regex("(?m)^\\s*(?:pub\\s+(?:\\([^)]*\\)\\s*)?)?mod\\s+(\\w+)\\s*;")).map(m => m.captures.first()).fold(src, (acc, m) => acc + "\n" + walk(kids + m + ".rs", kids + m + "/"))
}

#let corpus = (
  roots
    .pairs()
    .map(((crate, entries)) => {
      let dir = ".examples/" + crate + "/src/"
      (crate, entries.map(e => walk(dir + e + ".rs", dir)).join("\n"))
    })
)

#let census(probes) = {
  let rows = probes
    .map(((label, pat)) => {
      let hits = corpus.map(((_, src)) => src.matches(regex(pat)).len())
      (label, hits, hits.sum())
    })
    .sorted(key: r => -r.at(2))
  table(
    columns: (auto,) + corpus.map(_ => auto) + (auto,),
    stroke: 0.4pt + luma(180),
    align: (left,) + corpus.map(_ => right) + (right,),
    table.header(
      [*primitive*],
      ..corpus.map(((crate, _)) => raw(crate)),
      [*all*],
    ),
    ..rows.map(((label, hits, total)) => (raw(label), ..hits.map(h => [#h]), strong[#total])).flatten()
  )
}

#census((
  ("Buffering<C, H>", "\bBuffering\s*<"),
  ("Folding<C, H>", "\bFolding\s*<"),
  ("Gating<G>", "\bGating\s*<"),
  ("Spanning<C, TF>", "\bSpanning\s*<"),
  ("Sampling<C>", "\bSampling\s*<"),
))

#census((
  ("Elems(N)", "\bElems\s*\("),
  ("Over(TF)", "\bOver\s*\("),
  ("Unbounded", "\bUnbounded\b"),
  ("Unit", "\bUnit\b"),
  ("CLOCK", "\bCLOCK\b"),
))

#census((
  ("impl Cell", "\bimpl\b[^\n{;]*\bCell\b[^\n{;]*\bfor\b"),
  ("impl Emit", "\bimpl\b[^\n{;]*\bEmit\b[^\n{;]*\bfor\b"),
  ("impl Symbolic", "\bimpl\b[^\n{;]*\bSymbolic\b[^\n{;]*\bfor\b"),
  ("impl Blind", "\bimpl\b[^\n{;]*\bBlind\b[^\n{;]*\bfor\b"),
  ("impl Gate", "\bimpl\b[^\n{;]*\bGate\b[^\n{;]*\bfor\b"),
  ("impl Episodic", "\bimpl\b[^\n{;]*\bEpisodic\b[^\n{;]*\bfor\b"),
  ("Armed<_>", "\bArmed\s*<"),
  ("impl Glance", "\bimpl\b[^\n{;]*\bGlance\b[^\n{;]*\bfor\b"),
  ("impl Flat", "\bimpl\b[^\n{;]*\bFlat\b[^\n{;]*\bfor\b"),
  ("Plot", "\bPlot\b"),
))
