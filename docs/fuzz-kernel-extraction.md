# Proposal: extracting the fuzz kernel to `v_utils`

A written proposal, not a landed change — `~/s/v_utils` is a separate repo, published to crates.io.

`trading_data/tests/fuzz/` was built by porting the deterministic-fuzz kernel from
`~/s/ev_invest/dockviewers/dockviewers_core/tests/integration/`. Two copies of the same idea now
exist. This says which parts are genuinely one thing and which only look like it.

## What is actually there

| | dockviewers | trading_data |
|---|---|---|
| `frng.rs` | 71 | 74 |
| `minimize.rs` | 52 | 48 |
| `corpus.rs` | 71 | 82 |
| driver (`main.rs`) | 98 | 207 |
| domain | `actions` 434, `sim` 220, `oracle` 143 | `stream` 216, `fixture` 360, seven targets 1543 |

## The measured split

**`frng.rs` — byte-identical below the module doc.** A `diff` of everything from `pub struct Frng`
down comes back with two hunks, both of them doc-comment clauses naming the domain the draw is sized
for. `new`, `remaining`, `byte`, `below`, `span`, `weighted`: the same 74 lines, twice.

**`minimize.rs` — the same algorithm, and the trading_data copy is already the general form.** It
takes `fails: &dyn Fn(u64, usize) -> bool` instead of calling one simulator, because there are seven
targets here. Feed dockviewers' single `fails` in and the two are one function. It is also the one
piece here that is not obvious: the bisection tracks the smallest size it *observed* failing rather
than trusting monotonicity, and the statistical shrink is bounded at 256 rounds over
deterministically-derived seeds. That is knowledge worth having one copy of.

**`corpus.rs` — the same idea, a different schema.** dockviewers writes `seed size version`;
trading_data writes `target seed size version`, because one binary carries seven generators and a
`(seed, size)` is meaningless without knowing which one read it. The fingerprint is per target here
and whole-file there. `include_str!` cannot cross a crate boundary, so the fingerprint's *inputs*
have to stay at the call site under any extraction — which is the natural seam, and it leaves about
35 lines of line-parsing on the other side of it.

**The driver — the same three moves, a different shape.** Both do env-var replay, a quiet panic
hook, and minimize-then-record. dockviewers runs one target; trading_data runs a registry with
`FUZZ_TARGET`, a per-target version, and a per-target clean line. The 98 lines and the 207 lines
overlap in about 40, and those 40 are this repo's test UX — which env vars, what gets printed, what
the corpus interaction is.

## Recommendation

**Extract `Frng` and `minimize`. Leave `corpus` and the driver as per-repo copies.**

That is ~122 lines, against the ~250 the plan expected. The two that make it are pure functions with
no policy in them: a seeded byte buffer, and a search over `(seed, size)`. The two that do not are
both *policy* — a file format and a test UX — and the evidence that they are not yet one thing is
that the second consumer changed both on contact.

Shape, following the crate's flat convention:

```
v_utils/src/fuzz/mod.rs     mod frng; mod minimize; pub use frng::*; pub use minimize::*;
```

behind its own `fuzz` feature, off by default. **Zero new dependencies** — the whole point of the
FRNG is a fixed deterministic buffer with the prefix property, and `rand` is irrelevant to that.
Nothing in either file needs `std` beyond `Vec`, so the module can be `no_std + alloc` and stay
usable from the two zero-dep crates here if that ever matters.

## Two facts this rests on

**1. `v_utils` is consumed from crates.io.** `trading_data` pins `^2.17.2`, `dockviewers` pins
`^2.15.0`. Extraction needs a `v_utils` release before *either* consumer can use it, and dockviewers
additionally needs its floor raised and `v_utils` added to `dockviewers_core`'s dev-deps (it is a
workspace dep there today but that crate does not name it). That is real friction, and it is why this
is last rather than first.

**2. `~/.claude/CLAUDE.md` on helper libs: "adding something advanced to then have it be used by a
single consumer is hardly justifiable."** Two consumers clears that bar. But it clears it for the
parts that are *identical*, not for the parts that merely rhyme — and `corpus`/driver are the parts
that rhyme. Extracting those would be publishing a schema and a UX to serve two callers who already
disagree about both, which is the failure mode that rule is pointing at, one level up.

## If the answer is "not now"

Leaving all four copies alone is a defensible call and costs almost nothing: `frng.rs` has not
changed in either repo since it was written, because there is nothing in it to change. The cost of
the duplication is a fix to the FRNG or the shrinker having to land twice, and neither has needed a
fix yet. Revisit when one of them does, or when a third consumer appears.
