# Outs

r[outs.absence.one-reading]

not that haven't had yet fired, and node that decided to stop producing a value, are the same thing. And `None` is the exact primitive to express both. Consumer nodes shouldn't try to reason about which cause the `None` value had, - it's all the same for them. Equally, it means that meaning of `None` is taken, and no Node should try to attach their own meaning to it.

r[outs.absence.typed]

Absence on the value plane MUST be a *type* whose representation is NaN, never a bare `f64` a reader has to know the convention for. That type MUST NOT expose its payload except as an `Option`, and it MUST NOT derive `PartialEq`. No operator may **compare** a NaN: a body declines by producing one, and every comparison it could then reach — `Min`, `Max`, `Cmp`, a `Select` *condition* — MUST refuse it. A `read` holding an absent dep MUST decline rather than put it.

An `Option<f64>` pays sixteen bytes to say what the payload already said, since every bit pattern of an `f64` is a valid one and the discriminant can never be packed away. The trouble is not the width, it is that the sentinel was a convention: `f64::min`/`f64::max` *ignore* a NaN operand and hand back the other, `Cmp` reads one as false, and a `Select` condition reads it as *taken* — so `max(cold_indicator, threshold)` publishes the threshold and nothing downstream ever sees an absence. `Select`'s two branches stay permissive, because that is where a decline is *written*; there is one named idiom for it (`absent()`), so a body does not spell it `constant(NAN)` per site.

The comparison refusals are `debug_assert`, because `r[kernels.pure.zero-cost]` is an equality in retired instructions and a release-build branch here is one the hand-written arithmetic does not have. `Put::put`'s is the same assert one level up: it is the only route by which an absent *dep* becomes a NaN *operand*, and a partial item that flattens to NaN in one slot — a `Book` with no bids — reaches the algebra past it, which is why both are needed and neither is sufficient.

`NaN != NaN` is what makes the type mandatory rather than a convention: equality has to be hand-written so that absent equals absent, and a bare `f64` gives that impl nowhere to live.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

A tape stores no flag, it stores the values. A zero-slot out would fire and leave a buffer byte-identical to an unfired one — a publication nothing downstream could record, and nothing could later read back.

r[outs.fired.on-change]

A level node MUST be observed firing only on the ticks its flattening differs from the one it last published. A run's fire count stays its element count: three identical trades are three events, and "unchanged" is not defined for a run.

This is the observation plane and nowhere else. What a consumer reads off the frame is the node's out, which stands either way; the fired bit is an axis no dep read can reach ([`rates.deps.tick-opaque`](rates.md#ratesdepstick-opaque)), which is what leaves it free to mean *moved* rather than *ran*.
