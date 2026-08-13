# Kernels

Derived from the compute plane in [`model.typ`](../../trading_data_dag/model.typ) §1.5.

r[kernels.closed]

A node MUST compute through a kernel the framework provides. `Kernel` is sealed by `sealed::Kernel`,
so the set of kernels is closed and a node's only choice is which one it names; there MUST be no way
for a node to supply a compute body the framework cannot also read. This binds the run side as it
binds the level side: one trait serves both, and an `Emit` names a kernel exactly as a `Node` does.

What used to be two traits — a level-shaped one and a run-shaped copy of it — differed in one
member's signature and nothing else, because a level node's out is borrowed from the node and a run
node's from the engine. `Kernel::Host` names that owner (`N` for a level, `Emitter<E>` for a run), so
the difference is stated once and the readings are declared once.

The set is `Pure` (a scalar `Expr`), `Predicate` (a `bool` one), `Opaque` (the level hatch), `Scan`
(one out element per driving element, no state), `Close` (elements are whole periods), `Fold` (a
recurrence) and `Raw` (the run hatch). Each demands its own body trait — `Symbolic`, `Decides`,
`Blind`, `Scans`, `Closes`, `Folds`, `Runs` — so the body *is* the choice of kernel and there is no
attribute spelling one a node has no body for.

The point of the seal is that every reading the engine offers — the value, the formula, the exact
Jacobian, the value-annotated trace — comes from one declaration. Where a node can write its own
`advance`, the engine holds a number it cannot explain, and every reading beyond the number has to be
reconstructed by observation from outside. Closing the set is what makes the readings a property of
being a node rather than a favour some nodes happen to do.

r[kernels.opaque.stated]

A node that computes without an algebra reading MUST say why, in a string on the node itself. The
count of such nodes in a graph MUST be observable at compile time, and a graph that pins it MUST NOT
see it rise without someone editing the pin.

An escape hatch that costs nothing to use is not a hatch, it is the default with extra steps. The
string is the smallest thing that makes the choice deliberate — `Book`'s "an order book fold is not
a scalar function of its deltas" is a claim someone can disagree with, where a bare `impl Node` is
not. The count is what makes the direction of travel visible: it may fall silently, and it may only
rise in a diff that says so.

r[kernels.pure.zero-cost]

A node whose kernel reads an `Expr` body MUST cost, on the compute path, what the same arithmetic
written by hand costs. Equality is in retired instructions, not in wall clock, and it MUST hold as an
equality rather than as a bound.

Verified by [`trading_data_dag/benches/kernel_cost.rs`](../../trading_data_dag/benches/kernel_cost.rs),
which runs one node's arithmetic through a body trait and through a hand-written one and reports both
`Ir` counts — `pure`/`hand` on the level side, `scan`/`raw` on the run side. The run pair is the
harder question, since a per-element kernel refills its stack env once per *element* where a level
one does it once per tick, and it has to come out the same equality. A divergence is not a tuning problem, it is the signal that the flat env
buffer has to go — replaced by a typed env addressing the pulled dep tuple directly — because a
framework that makes the algebra mandatory has to make it free first.

r[kernels.jac.two-quantities]

A node's one-step Jacobian and its derivative over a dep's whole reach are different quantities and
MUST NOT be conflated. A reading MUST say which of the two it carries, and a consumer that draws one
MUST label it. A kernel that can produce the second MUST offer it as a reading of its own rather
than by widening the first, and one that cannot MUST say so rather than fill a block it cannot
stand behind.

`Fire::jac` is the one-step reading: each dep's *last* element perturbed, prior state held fixed.
Differentiating the body and finite-differencing it both land on that same number — which is why one
array carries both and `exact` says only how it was reached, and why cross-checking the algebra
against a numeric difference belongs in [`trading_data_expr`](../../trading_data_expr)'s own tests
rather than in every tick. What neither says is anything about the rest of a dep's reach: a node
reading `.trailing()` over 181 bars has one column describing bar 180 and silence about bars 0–179.
`slice_nudge!`'s `stage` bumps `s.last_mut()` and nothing else, so the finite difference is a
one-step impulse there by construction, and the algebraic column beside it would be the same. That
silence is not an error — a one-step impulse response is a real quantity — but it means "exact"
cannot be read as "covers what the body read", which is what `r[kernels.fidelity.stated]` is for.

`Fire::exact_block` is the second quantity, asked for separately (`Want::Exact`) because it costs
separately: one column group per *lag* of a dep's reach, per dep, oldest first, so a dep's last group
is exactly its one-step column and the groups before it are the reach that column was silent about.
It is grown rather than shaped, because a wall-clock window over an aperiodic series has no static
element count to declare.

What closes the gap for a per-element kernel is that every slot of its env is a *copy* of one element
slot and never a computation of one — `∂env/∂element` is a 0/1 selection, licensed by
`r[kernels.selection.index-is-not-a-variable]`. So a reading that reports the `(dep, lag, slot)` of
each slot it fills lets the body's gradient scatter over the whole reach in one pass, at no cost to
the value path: `Env<W: Witness>` is generic over whether anyone is watching, and the `scan == raw`
half of `r[kernels.pure.zero-cost]` is what proves the recording monomorphizes away.

A kernel whose omission is not a range still declines. A recurrence's state is no dep and has no lag
to be indexed at; a period's accumulator holds elements that live in the dep's *declaration* rather
than in its out. Both stay partial with a block in hand, which is the distinction
`r[kernels.fidelity.stated]` exists to keep.

This withdraws `r[kernels.jac.one-reading]`, whose justification — that the two readings are the
same quantity — holds only where a dep's reach is `Unit`.

r[kernels.fidelity.stated]

A kernel MUST state how much of what its body read its Jacobian covers: exact, partial with what it
omits, or opaque with why it has no algebra at all. The counts of the latter two in a graph MUST be
observable at compile time, and a graph that pins them MUST NOT see either rise without someone
editing the pin.

Two hatches rather than one, because they are different admissions and only one of them is visible
without being stated. `Opaque` is "no algebra here", which a reader can see from the absence of a
formula. `Partial` is "algebra, and it does not cover everything the body read" — and a partial
derivative of a body that reads a window is indistinguishable, in every reading the engine offers,
from an exact derivative of a body that reads a point. Marking such a node exact is not a wrong
number, it is a true number under a claim it does not support. A fold's omission is permanent by
design — a derivative carrying accumulated state sensitivity is a different quantity again — where a
kernel whose reach outruns what the engine can index is partial until it does not.

r[kernels.selection.index-is-not-a-variable]

Which elements of a dep a body reads MUST NOT depend on any quantity being differentiated. A
selection MUST index by count or by timestamp, and a timestamp MUST NOT be a `Flat` slot.

This is the assumption every reading over a range rests on: at fixed indices the algebraic derivative
is exact, it is merely indexed over a range instead of over a point. A lag and a window index by
count; an as-of read indexes by timestamp, and `Bar::DIMS = &[5]` deliberately excludes `ts_close`,
so no timestamp is ever a variable. The one value-dependent pick the workspace has — `high` and `low`
as max and min over a period — lives inside the algebra as `Min`/`Max`/`Select` with a pinned
tie-break, where each branch is differentiable and the tie resolves the same way in the value and in
the derivative. A value-dependent selector added outside the algebra would break every reading over a
range without changing any type, which is why this is a rule and not an observation about what
happens to exist.

r[observe.noninvasive]

A tick MUST leave the graph in the same state regardless of what was observed during it. The outs of
a tick observed at `Want::Jac` MUST be identical, bit for bit, to the outs of the same tick observed
at `Want::Nothing`.

This is what makes "run one more tick to see the derivatives" a legal thing for a consumer to do.
The finite-difference witness re-advances a clone restored from the pre-advance state and never the
node itself, so the property holds by construction — and stating it as a rule is what stops a later
kernel from quietly borrowing the real node's state for its reading. Without it, pausing to inspect
would perturb the run being inspected, and the inspection would be of a different graph than the one
that produced the value.
