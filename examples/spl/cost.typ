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
  table.cell(inset: (top: 5pt))[total], table.cell(inset: (top: 5pt), align(right)[*#s(m.total_s) s*]), [],
  [had the detail render not been clipped], align(right)[#s(m.unclipped_s) s], [],
)

Two lines are most of it, and neither is computation:

/ `detail`: the `Debug` rendering handed to each `Fire`, stopped at 256 chars by `exec_viz`'s `Clip`.
  The clip is already worth #s(m.unclipped_s - m.total_s) s; what remains is the cost of *reaching*
  256 chars, #m.render.len() times per tick.

/ `handoff`: `Rec::drop` builds a `TickMsg` with `mem::take(&mut r.acts)`, which hands the whole
  `Vec<Act>` away and leaves an empty one — so the `act.out.clear()` slot reuse three lines above it
  is defeated, and #m.render.len() fresh `Act`s are allocated every tick. The measured leg keeps its
  slots and never hands off, which is precisely what this difference prices.

By contrast the *entire* derived closure and the arrival weave are each at or under the measurement
floor. Nothing in this pipeline is bottlenecked on arithmetic; it is bottlenecked on rendering
strings nobody reads and on allocating the buffers to carry them.

== What the rendering bill is run up by

#let tg = m.render.map(r => r.glance).sum()
#let td = m.render.map(r => r.detail).sum()
#let per(b) = str(calc.round(b / m.ticks, digits: 0))

#table(
  columns: (1fr, auto, auto, auto),
  stroke: none,
  align: (left, right, right, right),
  table.header([node], [glance B/tick], [detail B/tick], [share]),
  table.hline(),
  ..m
    .render
    .slice(0, 10)
    .map(r => (
      raw(r.node),
      per(r.glance),
      per(r.detail),
      str(calc.round(100 * (r.glance + r.detail) / (tg + td), digits: 1)) + "%",
    ))
    .flatten(),
  table.hline(),
  [all #m.render.len() observed cells], per(tg), per(td), [100%],
  [MB over the day], str(calc.round(tg / 1e6, digits: 1)), str(calc.round(td / 1e6, digits: 1)), [],
)

The distribution is flat, and that is the finding. Almost every cell emits ≈256 detail bytes per
tick — i.e. almost every cell *hits the clip*. There is no hot node to fix: the cost is
#m.render.len() cells × every tick × a 256-char ceiling, #str(calc.round(td / 1e6, digits: 0)) MB a
day, rendered for a viewer that shows one tick at a time.

That is what makes this a *demand* problem rather than a rendering one. `Want` already gates the
flatten and the Jacobian at the same choke point; what it does not yet gate is the two strings, which
are the largest thing behind it.

== Regenerating

```
cargo r --release -p trading_data_spl --example viz_cost   # rewrites cost.json
typst compile examples/spl/cost.typ                        # rewrites the pdf
```

`tests/cost.rs` asserts `cost.json`'s derived closure still equals `Graph::NODES`, so adding or
removing a node fails the suite rather than leaving this document quietly describing a graph that no
longer exists. It deliberately does not re-measure: a leg is ≈3.5 minutes and wants the whole parquet
cache, which is not a thing `cargo t` may assume.
