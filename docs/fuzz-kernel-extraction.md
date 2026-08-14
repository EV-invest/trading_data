# The fuzz kernel, and where it ended up

Landed. `trading_data/tests/fuzz/` was built by porting the deterministic-fuzz kernel from
`~/s/ev_invest/dockviewers/dockviewers_core/tests/integration/`; both now sit on `v_utils::fuzz`
behind a `fuzz` feature, off by default and a dev-dep on both sides. This records what moved, what
did not, and why the second answer changed.

## What moved

| | before | after |
|---|---|---|
| `frng.rs` | 74 here, 71 there | 71, once, in `v_utils` |
| `minimize.rs` | 48 / 52 | 48, once |
| `corpus.rs` | 82 / 71 | folded into the driver |
| driver (`main.rs`) | 227 / 98 | 233 once; **118 / 36** left as target tables |

`v_utils::fuzz` exports `Frng`, `minimize`, `fnv`, `FRNG_SRC`, and `Suite`/`Target`. A harness is
now a `TARGETS` table, a `const SUITE`, and two one-line `#[test]`s.

## What the original proposal got wrong

It recommended extracting `Frng` and `minimize` and leaving `corpus` and the driver as per-repo
copies, on the grounds that a file format and a test UX are *policy* and the two consumers had
already disagreed about both on contact. That reasoning was sound about the disagreement and wrong
about what it implied.

The disagreement was not two designs. dockviewers wrote `seed size version` and trading_data wrote
`target seed size version` because dockviewers has one generator and trading_data has ten — the
second schema is the first with `N = 1` spelled out. Same for the driver: `FUZZ_TARGET`, the
per-target version and the per-target clean line are all the same code at `N > 1`. Generalizing the
smaller consumer to the larger one's shape cost dockviewers a nine-line `CORPUS.txt` migration and
bought a single implementation. There was no policy to publish, only an arity.

Two facts made that migration free, and both were checked before it was made rather than assumed:

- **Both corpora were 100% dead.** Recomputed FNV-1a over the live sources at the time: trading_data
  had 7 recorded entries and 0 at a current fingerprint, dockviewers 9 and 0. That is the designed
  behaviour of a deliberately over-sensitive fingerprint, not a defect — and it meant re-spelling
  every line cost nothing that was still being bought.
- **`regressions` legitimately reports 0 live in both repos, before and after.** A migration that
  changes that number is a migration that lost a test; this one does not.

## Two constraints the implementation is shaped by

**`include_str!` and `env!("CARGO_MANIFEST_DIR")` cannot cross a crate boundary.** Both fingerprints
hash the FRNG source and both corpus paths are manifest-relative, so both had to stay resolvable at
the consumer's call site. Hence `FRNG_SRC: &str` (data, not a macro) and `Suite::corpus: &str` — a
consumer writes `concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fuzz/CORPUS.txt")` and the fingerprint
stays derived rather than hand-bumped.

**`&dyn Trait` is not `RefUnwindSafe`.** Every target invocation crosses `catch_unwind`, so a trait
object would have cost `&(dyn T + RefUnwindSafe)` at every call site. `Target` is a `struct` holding
a bare `fn(u64, usize, bool) -> Result<(), String>`, which is unconditionally `UnwindSafe`; `fails`
and `replay` are free fns taking `&Target` and copy the pointer out (`let run = t.run;`) before the
boundary, so `Target` can gain fields without the closure stopping to compile.

`run` takes `(seed, size, verbose)` rather than `&mut Frng`, because `minimize` already worked on
`(seed, size)` — which means `Suite` needs no knowledge of `Frng` at all, and dockviewers' `sim::run`
kept its signature unchanged.

## What deliberately did not move

**The two films.** `trading_data/tests/fuzz/film.rs` (293) and
`dockviewers_core/examples/fuzz_film.rs` (542) share about 32 lines of 835, and the shared pieces
disagree *semantically*: the coverage tally counts per seed here and per input event there, and
truncation is asserted against here and silently accepted there. Extracting 32 lines that mean
different things on the two sides would be publishing a bug.

What is shared instead is the *automation*: `nix run .#film` in both repos, and a v_flakes
`asset-gate` job that regenerates the asset and diffs it against the committed one, guarded by a
cache key bucketed on `now / 86400`. That found something on the first run — dockviewers' committed
SVG had been stale since the title-bar-height commit, and nothing had said so.

## Cost

`v_utils` has 17 unconditional dependencies that `default-features = false` does not shed.
dockviewers_core's dev-dep tree was `{serde, serde_json, clap}`; adding `v_utils` multiplies it.
Dev-deps only — published consumers and the wasm path are unaffected, and the `fuzz` feature is
declared on a separate dev-dep entry so that under `resolver = "3"` it stays out of each facade's
published feature set.
