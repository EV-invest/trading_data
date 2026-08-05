# Kernels

Derived from the compute plane in [`model.typ`](../../trading_data_dag/model.typ) §1.5.

r[kernels.closed]

A node MUST compute through a kernel the framework provides. `Level` is sealed, so the set of
kernels is closed and a node's only choice is which one it names; there MUST be no way for a node to
supply a compute body the framework cannot also read.

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

A node whose kernel is `Pure` MUST cost, on the compute path, what the same arithmetic written by
hand costs. Equality is in retired instructions, not in wall clock, and it MUST hold as an equality
rather than as a bound.

Verified by [`trading_data_dag/benches/kernel_cost.rs`](../../trading_data_dag/benches/kernel_cost.rs),
which runs one node's arithmetic through a `Symbolic` body and through a hand-written `advance` and
reports both `Ir` counts. A divergence is not a tuning problem, it is the signal that the flat env
buffer has to go — replaced by a typed env addressing the pulled dep tuple directly — because a
framework that makes the algebra mandatory has to make it free first.

r[kernels.jac.one-reading]

A fire MUST carry at most one Jacobian, and MUST say whether it is exact. An exact reading and a
finite-difference reading of the same node on the same tick MUST NOT both be produced.

The two are the same quantity: a finite-difference column holds prior state fixed and reads a
one-step impulse response, which is exactly what differentiating the body returns. So the exact path
does not extend the finite-difference one, it replaces it — strictly better and strictly cheaper —
and a consumer that could receive both would have to decide which it believed. Cross-checking the
algebra against a numeric difference belongs in
[`trading_data_expr`](../../trading_data_expr)'s own tests, where it runs once instead of every tick.

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
