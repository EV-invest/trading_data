#set page(width: 210mm, height: auto, margin: 14mm)
#set text(size: 9.5pt)
#set par(justify: false)
#show raw.where(block: true): it => block(fill: luma(248), inset: 8pt, radius: 3pt, width: 100%, text(size: 7.6pt, it))

// Every number below is read, not typed. `viz_cost` writes `cost.json`; `tests/cost.rs` fails when
// the graph it was taken over stops matching the one that compiles. Nothing here is maintained by
// hand, so nothing here can quietly drift from what the code does.
#let m = json("cost.json")
#let s(x) = str(calc.round(x, digits: 2))
#let pct(x) = str(calc.round(100 * x / m.total_s, digits: 1)) + "%"

= Where the replay's wall clock goes

`examples/spl/examples/viz_cost.rs`, release, #m.ticks ticks over #m.day, on the #m.derived.len()-node
derived closure `graph!` builds for `spl`.

Each row is a *measured leg*, not a share of a sampled profile: the example walks the same day once
per row with one more thing switched on, and the row is the difference. A leg near zero therefore
reads as noise rather than as a small cost — which is the honest rendering of a cost that is small.

== The budget

#let rows = ()
#let group = ""
#for st in m.stages {
  if st.group != group {
    group = st.group
    rows.push((table.cell(colspan: 3, inset: (top: 6pt, bottom: 3pt))[*#raw(group)*],))
  }
  rows.push((st.label, align(right)[#s(st.secs) s], align(right)[#pct(st.secs)]))
}

#table(
  columns: (1fr, auto, auto),
  stroke: none,
  align: left,
  ..rows.flatten(),
  table.hline(),
  [the recorder's whole share], align(right)[#s(m.tape_s) s], align(right)[#pct(m.tape_s)],
  table.cell(inset: (top: 5pt))[total], table.cell(inset: (top: 5pt), align(right)[*#s(m.total_s) s*]), [],
)

What used to sit at the top of this table was `detail` — a clipped `{:?}` of every node's out,
rendered on every tick, 2.7 s and 353 MB a day for a string a hover might read one of. It is gone,
and so is `Fire::debug` and the `Debug` bound the eight observation entry points held for it. What
a hover reads now is the node's own `Flat` under its `plots` labels, which is the same source the
chart's crosshair line has always printed from. `glance` then stopped rendering a node that did not
fire — 86% of the calls — and a tick's storage went columnar, one `String` and two `Vec`s for the
whole tick rather than three heap pointers per node.

What is left at the top is not computation either:

/ `handoff`: `Rec::drop` builds a `TickMsg` with `mem::take(&mut r.acts)`, which hands the tick's
  columns away and leaves empty ones — so the appends are free only while the recycle channel
  returns a buffer in time, and a fresh one regrows its capacity from nothing. The measured leg
  keeps its columns and never hands off, which is precisely what this difference prices.

By contrast the *entire* derived closure and the arrival weave are each at or under the measurement
floor. Nothing in this pipeline is bottlenecked on arithmetic.

== What the rendering bill is run up by

#let tg = m.render.map(r => r.glance).sum()
#let per(b) = str(calc.round(b / m.ticks, digits: 0))

#table(
  columns: (1fr, auto, auto),
  stroke: none,
  align: (left, right, right),
  table.header([node], [glance B/tick], [share]),
  table.hline(),
  ..m.render.slice(0, 10).map(r => (raw(r.node), per(r.glance), str(calc.round(100 * r.glance / tg, digits: 1)) + "%")).flatten(),
  table.hline(),
  [all #m.render.len() observed cells], per(tg), [100%],
  [MB over the day], str(calc.round(tg / 1e6, digits: 1)), [],
)

#per(tg) bytes a tick across #m.render.len() cells, and the distribution is not flat: the top two
cells are #str(calc.round(100 * (m.render.at(0).glance + m.render.at(1).glance) / tg, digits: 0))% of
it between them. A `Glance` is hand-written per out, so a cell that shows up here is a cell whose
author chose what it says — which is the read this table is for.

== Regenerating

```
BENCH_CPUS=12-15,28-31 measure cost                    # rewrites cost.json
typst compile examples/spl/cost.typ /tmp/cost.pdf      # renders it; the pdf is never written locally
```

Through `measure`, and with `BENCH_CPUS` naming both SMT siblings of every core claimed, because a
leg taken on a shared core lands in a committed document and stays there. On this box cores 12-15
and their siblings 28-31 are already held out of `systemd`'s `CPUAffinity`, so `reserved` reaches
them with a plain `taskset` and nothing else is scheduled there. What the reservation buys is
mostly spread rather than mean: the legs that move on a contended core are `handoff` and
`backpressure`, which is what a second runnable thread looks like from the producer side.

`tests/cost.rs` asserts `cost.json`'s derived closure still equals `Graph::NODES`, so adding or
removing a node fails the suite rather than leaving this document quietly describing a graph that no
longer exists. It deliberately does not re-measure: a leg wants the whole parquet cache, which is not
a thing `cargo t` may assume.
