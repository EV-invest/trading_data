## Benches

The same strategy, three times over one tape: through this graph, and twice through
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — once with every feed
subscribed for the whole run, once with the delta feed opened on a screener hit and closed when the
situation ends, which is NT's answer to what `Gating<Screener>` is here. Our nodes do the arithmetic
in all three; what differs is the framework carrying them.

`compute = total - feed` is the column that compares — `feed` is the same run with the strategy
removed, so td's per-day parquet decode stops flattering NT's, which happened at setup. Tick counts
differ by construction (a td tick is a `ReadClock` cell of arrivals, an NT pass is one event), so the
shared call sites are counted instead. Every row rests on the `equivalence` bench, which gates the
other three by driving our nodes by hand and demanding the graph's intent stream back.

<!-- bench:begin -->
```
bench            total s    feed s   compute     cpu s   cores  rss p68 MB  rss p95 MB   intents             digest
naive_nt            8.97      3.12      5.85      8.98    1.00        2031        2032    161090   f76140ed3e3cdc78
optimized_nt        8.06      3.08      4.97      8.06    1.00        2031        2031    161076   8799bd3ada1de6f0
td_graph            2.14      1.44      0.70      2.15    1.00         403         403    138805   714c4c8498e78bde
naive_nt: cpus 12-15,28-31 (8 of them)
optimized_nt: cpus 12-15,28-31 (8 of them)
td_graph: cpus 12-15,28-31 (8 of them)

passes          bars_closed     classify     decision       deltas   deprecator     screener       trades
naive_nt               3501          310          310      6751349       729850         2879       910878
optimized_nt           3501          310          310      6751349       519368         2879       910878
td_graph                  0       972857       972857      6751349       972857       972857       910878

DIGESTS DIFFER — the rows above are timing different work, see each row's notes
  naive_nt: every feed subscribed for the whole run
  naive_nt: every indie recomputed on every event it clocks off, hit or miss
  naive_nt: bars are NT's internal TimeBarAggregator, which closes on its clock; ours closes when the next out-of-bucket trade lands
  naive_nt: no tick: each node fires on the event its deps are clocked by, so imbalance/spread read the latest book rather than this tick's
  naive_nt: 4h and 1h closes reach their rings on the next 5m/1m close, since NT does not order aggregators against each other
  optimized_nt: book deltas subscribed only between a screener hit and the situation's terminal intent
  optimized_nt: a reopened book starts empty, so imbalance/spread read None until both sides refill
  optimized_nt: bars are NT's internal TimeBarAggregator, which closes on its clock; ours closes when the next out-of-bucket trade lands
  optimized_nt: no tick: each node fires on the event its deps are clocked by, so imbalance/spread read the latest book rather than this tick's
  optimized_nt: 4h and 1h closes reach their rings on the next 5m/1m close, since NT does not order aggregators against each other
  td_graph: pass counts are tick counts: the gated subtree's skips are not observable from outside the graph
  td_graph: bars_closed is not an output of this graph, so it reads 0 here
  td_graph: also computes Rsi, which the NT rows have no counterpart for
  td_graph: total includes the per-day parquet decode; NT decodes its whole tape at setup, so only the compute column compares
```
<!-- bench:end -->

Read, not typed: `BENCH_CPUS=12-15,28-31 nix run .#spl_bench` splices whatever it measures back into
this section. The CPU list names both SMT siblings of every core it claims, because a leg taken on a
contended core lands in a committed document and stays there. `examples/spl/cost.typ` takes the
other reading — where the replay's own wall clock goes, itemized.
