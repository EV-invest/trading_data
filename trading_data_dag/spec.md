# `trading_data_dag` — derivative readings

Framework-level, below the workspace [Invariants](../docs/spec/): these bind whoever writes or edits
a kernel, and no type checks either of them. What the type system does hold — the kernel set is
sealed, a hatch owes its `WHY`, a kernel owes a `FIDELITY` — is not restated here.

r[kernels.jac.two-quantities]

A node's one-step Jacobian and its derivative over a dep's whole reach are different quantities and
MUST NOT be conflated. A reading MUST say which of the two it carries, and a consumer that draws one
MUST label it. A kernel that can produce the second MUST offer it as a reading of its own rather than
by widening the first, and one that cannot MUST fill nothing rather than a block it cannot stand
behind.

`Fire::jac` is the one-step reading: each dep's *last* element perturbed, prior state held fixed.
Differentiating the body and finite-differencing it both land on that same number — which is why one
array carries both and `exact` says only how it was reached. What neither says is anything about the
rest of a dep's reach: a node reading `.trailing()` over 181 bars has one column describing bar 180
and silence about bars 0–179.

`Fire::exact_block` is the second quantity, asked for separately (`Want::Exact`) because it costs
separately: one column group per *lag* of a dep's reach, per dep, oldest first, so a dep's last group
is exactly its one-step column and the groups before it are the reach that column was silent about.
It is grown rather than shaped, because a wall-clock window over an aperiodic series has no static
element count to declare.

A kernel whose omission is not a range still declines. A recurrence's state is no dep and has no lag
to be indexed at; a period's accumulator holds elements that live in the dep's *declaration* rather
than in its out.

r[kernels.fidelity.stated]

A kernel's stated fidelity MUST be true of the reading it fills. `Exact` MUST NOT be claimed for a
derivative narrower than what the body read, and a `Partial` MUST name what it omits in terms a
reader can check against the body.

A partial derivative of a body that reads a window is indistinguishable, in every reading the engine
offers, from an exact derivative of a body that reads a point. Marking such a node exact is not a
wrong number, it is a true number under a claim it does not support — and it is the one thing about a
kernel that no test and no type can catch, because both readings are numerically fine.
