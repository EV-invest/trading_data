# Kernels

Derived from the compute plane in [`model.typ`](../../trading_data_dag/model.typ) §1.5.

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

r[kernels.selection.index-is-not-a-variable]

Which elements of a dep a body reads MUST NOT depend on any quantity being differentiated. A
selection MUST index by count or by timestamp, and a timestamp MUST NOT be a `Flat` slot.

This is the assumption every reading over a range rests on: at fixed indices the algebraic derivative
is exact, it is merely indexed over a range instead of over a point. A lag and a window index by
count; an as-of read indexes by timestamp, and `Bar::DIMS = &[5]` deliberately excludes `ts_close`,
so no timestamp is ever a variable. Every env slot a per-element kernel reads is therefore a *copy*
of one element slot and never a computation of one, so `∂env/∂element` is a 0/1 selection — which is
what lets a body's gradient scatter over a dep's whole reach in one pass. A body states which element
it wants and the dep hands it over carrying where it came from, so the three coordinates of a reading
are what happened rather than what was claimed; a number the body computed instead of copying enters
by its own act and no column stands for it. The one value-dependent pick the workspace has — `high` and `low`
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
