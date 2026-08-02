# Feeds

Derived from [ARCHITECTURE.md](../ARCHITECTURE.md#one-graph-one-router-two-feeds).

r[feeds.live.on-arrival]

`Live` MUST emit a tick carrying an event before it blocks on the next one. It MUST NOT wait for
a clock boundary, a timer, a fill level, or a second lane. The only latency the framework may add
between a `Sink` push and the tick that carries it is the work of stamping, weaving and folding
that one event.

The reason is not latency-chasing, it is the seam: hand-rolling the same strategy against a raw
websocket, you would fold each message as it lands. If the framework defers, the framework is
strictly worse than the code it replaces, and every live number it produces is a measurement of
our buffering rather than of the strategy. So `Live` is zero-cost in the C++ sense — choosing the
engine costs nothing you would not have paid by hand.

r[feeds.live.no-quiet-stall]

A consequence, stated separately because it is the one that kills designs: no lane may be held
back pending another lane, and no tick may be held pending the *absence* of an event. A scheme
that must know a window is closed before emitting it cannot know that in live without either
waiting out the window or waiting for the next event — the first violates
[`feeds.live.on-arrival`](#feedsliveon-arrival), the second stalls behind whichever lane is
quietest, which for `Mc` is a day.

r[feeds.replay.grid-optional]

`Replay` is under neither constraint: its whole stream is already in hand, so it MAY group by any
rule — a wall-clock grid, an event count, a lane's own cadence. A grouping rule is therefore
allowed to exist only if it degenerates to on-arrival under `Live` rather than being *disabled*
there: the two feeds must remain one code path with one set of node semantics, or the round-trip
stops being evidence of anything.
