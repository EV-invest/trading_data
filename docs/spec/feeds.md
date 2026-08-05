# Feeds

Derived from [ARCHITECTURE.md](../ARCHITECTURE.md#one-graph-one-router-two-feeds).

r[feeds.live.on-arrival]

`Live` MUST emit a tick carrying an event before it blocks on the next one. It MUST NOT wait for a clock boundary, a timer, a fill level, or a second lane. The only latency the framework may add between a `Sink` push and the tick that carries it is the work of stamping, weaving and folding that one event.

Do not conflate with batching. If by the time we are ready to process, we already have multiple events buffered up, - we do want to batch aggressively, same as in backtest. It's just that we never wait to try to collect more before proceeding.

r[feeds.live.no-quiet-stall]

No lane may be held back pending another lane, and no tick may be held pending the *absence* of an event. A scheme that must know a window is closed before emitting it cannot know that in live without either waiting out the window or waiting for the next event — the first violates [`feeds.live.on-arrival`](#feedsliveon-arrival), the second stalls behind whichever lane is quietest, which for `Mc` is a day.

r[feeds.replay.grid-optional]

`Replay` is under neither constraint: its whole stream is already in hand, so it MAY group by any rule — a wall-clock grid, an event count, a lane's own cadence. A grouping rule is therefore allowed to exist only if it degenerates to on-arrival under `Live` rather than being *disabled* there: the two feeds must remain one code path with one set of node semantics, or the storage round-trip stops being evidence that what was recorded is what comes back.

`Replay`'s rule is a `ReadClock` and it is not free. Events inside one cell reach the graph together, so the strategy never acts between two that arrived apart, and it can decide differently than it would have live. That is bought knowingly — it is what lets a backtest outrun the range it replays — and it is why a backtest is an estimate rather than a rerun. `Live` states no rate: it weaves [`ReadClock::ALL`], folding whatever is buffered when it gets there, which is the same aggressive batching with the *waiting* removed.
