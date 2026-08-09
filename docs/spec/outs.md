# Outs

r[outs.absence.one-reading]

not that haven't had yet fired, and node that decided to stop producing a value, are the same thing. And `None` is the exact primitive to express both. Consumer nodes shouldn't try to reason about which cause the `None` value had, - it's all the same for them. Equally, it means that meaning of `None` is taken, and no Node should try to attach their own meaning to it.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

A tape stores no flag, it stores the values. A zero-slot out would fire and leave a buffer byte-identical to an unfired one — a publication nothing downstream could record, and nothing could later read back.
