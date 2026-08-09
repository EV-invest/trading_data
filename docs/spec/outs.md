# Outs

r[outs.absence.one-reading]

not that haven't had yet fired, and node that decided to stop producing a value, are the same thing. And `None` is the exact primitive to express both. Consumer nodes shouldn't try to reason about which cause the `None` value had, - it's all the same for them. Equally, it means that meaning of `None` is taken, and no Node should try to attach their own meaning to it.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

This is what keeps [`outs.absence.one-reading`](#outsabsenceone-reading) decidable rather than merely intended. Consumers recover the fired bit from the slots being present — a tape stores no flag, it stores the values — and a zero-slot out would fire and leave a buffer byte-identical to an unfired one. Absence would then mean two things at exactly one place, which is the place nobody would look.
