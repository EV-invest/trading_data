A trading framework whose derived-value DAG **is** a type: a node names its dependencies as types, so the node set, the topological order, every buffer's size, which nodes may go dark and which source lanes are loaded at all are read off that one type and monomorphized into one straight-line sweep — cycles are unrepresentable, and work no output reaches does not exist rather than merely going unused.
The same graph runs live or over a recorded month; the only seam between the two is where the events come from.

<!-- TODO!!!: replace with a video walkthrough of the system -->
![spl replayed — chart panes left, the graph right](./overview.jpeg)

`examples/spl`: a whole strategy over 32 days of Bybit TAO-USDT, scrubbed tick by tick in [exec_viz](https://github.com/EV-invest/exec_viz) — every node's standing value at the cursor, and the edges that fed it.

🌐 **[Live demo](https://ev-invest.github.io/exec_viz/)** — no setup, runs in the browser. A recorded `examples/spl` run, the still above made scrubbable; `nix run .#spl -- --record demo.tape` is what writes one.
