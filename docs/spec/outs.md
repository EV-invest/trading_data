# Outs

r[outs.absence.one-reading]

not that haven't had yet fired, and node that decided to stop producing a value, are the same thing. And `None` is the exact primitive to express both. Consumer nodes shouldn't try to reason about which cause the `None` value had, - it's all the same for them. Equally, it means that meaning of `None` is taken, and no Node should try to attach their own meaning to it.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

A tape stores no flag, it stores the values. A zero-slot out would fire and leave a buffer byte-identical to an unfired one — a publication nothing downstream could record, and nothing could later read back.

r[outs.fired.on-change]

A level node MUST be observed firing only on the ticks its flattening differs from the one it last published. A run's fire count stays its element count: three identical trades are three events, and "unchanged" is not defined for a run.

This is the observation plane and nowhere else. What a consumer reads off the frame is the node's out, which stands either way; the fired bit is an axis no dep read can reach ([`rates.deps.tick-opaque`](rates.md#ratesdepstick-opaque)), which is what leaves it free to mean *moved* rather than *ran*.
