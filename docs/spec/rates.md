# Rates

r[rates.node.declared]

How often a node publishes is a property **of that node**, stated on the node itself. It MUST NOT
be derivable from, or overridable through, anything written in `Deps`. Two consumers naming the
same input do not thereby run at different rates, and no consumer can change the rate of what it
reads.

A rate that lives in a dep is a rate the node does not own: it becomes whatever its inputs happen
to do, which is another way of saying it is whatever the weaver happened to deliver. Naming it on
the node is what makes it a declaration rather than an observation.

r[rates.node.whole-elements]

A node clocked to a timeframe MUST observe only completed elements of it. It MUST NOT be re-run as
the in-progress one moves.

Bounding a computation to a candle and then re-running it on every tick that revises that candle's
last trade asks for two different things at once — the stability of the coarse rate and the latency
of the fine one — and gets neither. What it produces is a value that is neither the 5-minute
reading nor the trade-by-trade one, and no consumer can say which it holds.

r[rates.deps.tick-opaque]

A dep read MUST NOT reveal whether that dep produced anything on this tick. `None` means *no value
has ever been produced*, and carries no other information — in particular a dep whose gate is shut
and a dep that has never published are the same state, deliberately and permanently.

This is the invariant the whole dep vocabulary exists to serve. Where a read exposes the tick, two
groupings of one message sequence give two different results, and the node's output becomes a
measurement of the feed's batching rather than of the market.

Note what this does *not* say. It bounds what a node may read, not what batching costs: a coarser
read clock means fewer points at which nodes evaluate at all, and anything reading a running
extremum or a threshold crossing over *evaluated* states sees that. A backtest batches on purpose,
to finish faster than the range it replays, and is imprecise for it. This requirement keeps that
imprecision out of the *values* a node reads; it does not make a backtest a reproduction of a live
run, and nothing should be asserted as though it did.

r[rates.folds.exactly-once]

A fold or recurrence MUST see every element of what it folds exactly once, in order. This is the
one thing permitted to depend on what arrived, and it is a statement about the *element sequence*,
which is identical under every grouping — not about ticks, which are not.

A windowed read promises no such thing, which is why a recurrence (Wilder RSI/ATR, EMA) and a fold
(a running sum, a partial bar) stay stateful where a window would do for a plain lookback.
