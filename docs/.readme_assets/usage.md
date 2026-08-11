A node states what it reads and how it folds; nothing else. The *reading* a dep asks for — this
tick's batch, a window, the last value whenever it came, or a claim to fold the series itself — is
what tells the engine who holds the history:

```rust
/// Rolling `WIN`-bar quote volume. `None` until the window is whole — a partial sum compared
/// against a threshold is a lie, not a warmup.
#[derive(Clone, Default)]
pub struct RollingVolUsd<const TF: Timeframe, const WIN: usize>;

#[node]
impl<const TF: Timeframe, const WIN: usize> Runs for RollingVolUsd<TF, WIN> {
    type Deps = (Buffering<Bars<TF>, Elems<WIN>>,);

    fn emit(&mut self, (hist,): DepOuts<'_, Self>, out: &mut Vec<Option<f64>>) {
        out.extend(hist.narrowed(Horizon::Elems(WIN)).trailing().map(|w| w.map(|w| w.iter().map(|b| b.vol_base * b.close).sum())));
    }
}
```

The graph names its roots and the handful of nodes anything reads. Everything between — `Ohlcs`,
`Volumes`, `Bars`, their buffers, the order they step in — is walked out of the dependency types;
a node no output reaches is never instantiated, and neither is the lane that would have fed it:

```rust
trading_data::graph! {
    pub struct Graph;
    batches Batches;
    roots { trades: Trades[TradeCols] };
    out TickOut;
    outputs {
        rsi: Rsi<Bars<{ TF_1MIN }>, Len14>,
        vol_usd: RollingVolUsd<{ TF_1MIN }, 60>,
    }
}

/// The whole of an app's routing layer: every lane is present, the graph takes the ones it named.
impl<'t> From<Lanes<'t>> for Batches<'t> {
    fn from(l: Lanes<'t>) -> Self { Self { trades: l.trades } }
}
```

Then pick where events come from. `required_lanes::<Graph>()` is the graph's dep tree read as
storage, so a `Replay` loads exactly what will be looked at:

```rust
let lanes = required_lanes::<Graph>();
let mut feed = Replay::new(&catalog, ExchangeName::Bybit, symbol(), start, end, &lanes, latency, ReadClock::from(Exact::from_nanos(60_000_000_000)));
// ...or the same graph on the wire, which states no clock and cannot be given one:
// let mut feed = Live::new(catalog, ExchangeName::Bybit, symbol(), prec, false, Arc::new(LiveClock));

while let Some(l) = feed.next() {
    for intent in graph.tick(l.ts_venue.as_nanos(), l.into()).signal.iter().flatten() { /* ... */ }
}
```

`Live` tees every event into the lanes a backtest later reads, so the two are the same graph over
the same events — [not the same run](../ARCHITECTURE.md#live-never-clocks-a-backtest-is-expected-to).

Runnable, smallest first: `nix run .#simple` (one day, one root, one RSI chain), `nix run .#live`
(real Bybit, watchable as it runs), `nix run .#spl` (the strategy above).
