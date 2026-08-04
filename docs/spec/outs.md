# Outs

Derived from the out plane in [`model.typ`](../../trading_data_dag/model.typ) §1.6.

r[outs.absence.one-reading]

Not firing and carrying nothing are the same fact, and every reading of an out MUST treat them as
one. `None`, the empty batch, and an all-NaN flattening MUST all report unfired; nothing downstream
of the observer may give an absent out a meaning of its own — no "fired, but empty" distinct from
"did not fire", and no state a node is understood to be in only while it is silent.

Absence is the multi-rate channel: a node reading a faster one sees `None` on most ticks purely
because the two rates differ, and any meaning attached to that is a reading of the cadence rather
than of the market. So an absent out says one thing, always, and a node with something to say about
its silence MUST say it as a value — a slot, a level, an enum — where a consumer can read it
without inferring.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

This is what keeps [`outs.absence.one-reading`](#outsabsenceone-reading) decidable rather than
merely intended. Consumers recover the fired bit from the slots being present — a tape stores no
flag, it stores the values — and a zero-slot out would fire and leave a buffer byte-identical to an
unfired one. Absence would then mean two things at exactly one place, which is the place nobody
would look.
