# Outs

r[outs.absence.one-reading]

not that haven't had yet fired, and node that decided to stop producing a value, are the same thing. And `None` is the exact primitive to express both. Consumer nodes shouldn't try to reason about which cause the `None` value had, - it's all the same for them. Equally, it means that meaning of `None` is taken, and no Node should try to attach their own meaning to it.

r[outs.absence.typed]

Absence on the value plane MUST be a *type* whose representation is NaN, never a bare `f64` a reader has to know the convention for. That type MUST NOT expose its payload except as an `Option`, and it MUST NOT derive `PartialEq`. Whether an expression may decline MUST be a property of its *type*, and no comparison — `Cmp`, or a `Select` *condition* — may be written over one that may. A body that declines MUST publish through an out that has an absence channel. A `read` holding an absent dep MUST decline rather than put it.

An `Option<f64>` pays sixteen bytes to say what the payload already said, since every bit pattern of an `f64` is a valid one and the discriminant can never be packed away. The trouble is not the width, it is that the sentinel was a convention: `f64::min`/`f64::max` *ignore* a NaN operand and hand back the other, `Cmp` reads one as false, and a `Select` condition reads it as *taken* — so `max(cold_indicator, threshold)` publishing the threshold was a thing nothing had decided.

`NaN != NaN` is what makes the type mandatory rather than a convention: equality has to be hand-written so that absent equals absent, and a bare `f64` gives that impl nowhere to live.

**Definedness is a const, and the two lattices are one character apart.** `Expr::MAYBE` is `false` at every leaf but `absent()`, and each operator derives it: arithmetic, `Sum` and a `Select`'s two *branches* take the `|` — absence **propagates**, so `absent - absent` is absent and never `0` — where `Min`/`Max` take the `&`, because they **skip** an absent operand and are absent only if both are. That is the resolution of the `max(cold, threshold)` footgun, and it is a specification rather than a prohibition: the value skips, the gradient skips, and `Ast::diff` emits the presence tests that make the symbolic reading skip too, so all three readings branch alike. A body that wanted the other answer writes `or(x, d)` and gets a number back — `Or`'s definedness is its fallback's alone, which is why it is a node and not a `Select` spelled out.

`Cmp` and a `Select` condition then refuse a declination at **compile time**, in a `const {}` block on `lt`/`gt`/`select`. The runtime `debug_assert`s stay on both as the backstop for the NaN no type predicts — the one arithmetic produces, `0/0` inside a tree. They are `debug_assert` because `r[kernels.pure.zero-cost]` is an equality in retired instructions and a release-build branch here is one the hand-written arithmetic does not have.

**A body may not see an absent dep at all, which is a rule about deps rather than about the algebra.** A `Level` node's deps MUST be levels — a bare run in dep position is forbidden, save a `Decides` node's *leading* dep, which is the clock it screens per element of. Given that, "no dep has fired" means *has never stood*, which is invariant under how the feed grouped its messages ([`rates.deps.tick-opaque`](rates.md#ratesdepstick-opaque)) — where the same guard over a run dep would itself be a tick observation. So a `Level` kernel runs its body only once every dep has fired: `Pure` publishes absence otherwise, and `Predicate` publishes `false`, which is what its leading dep's fire count already said, widened to the rest. `Put::put`'s assert is the same statement for a per-element kernel, whose env is filled by arbitrary Rust and cannot be checked any other way.

What is left after all of it is arithmetic NaN — `0/0` inside a tree — which the `debug_assert`s catch and which no type can.

r[outs.flat.nonempty]

Every `Flat` MUST occupy at least one slot: no `DIMS` may contain `0`.

A tape stores no flag, it stores the values. A zero-slot out would fire and leave a buffer byte-identical to an unfired one — a publication nothing downstream could record, and nothing could later read back.

r[outs.fired.on-change]

A level node MUST be observed firing only on the ticks its flattening differs from the one it last published. A run's fire count stays its element count: three identical trades are three events, and "unchanged" is not defined for a run.

This is the observation plane and nowhere else. What a consumer reads off the frame is the node's out, which stands either way; the fired bit is an axis no dep read can reach ([`rates.deps.tick-opaque`](rates.md#ratesdepstick-opaque)), which is what leaves it free to mean *moved* rather than *ran*.

r[outs.moved.outputs-plane]

A graph's typed outputs are an observation plane of their own. A level output MUST be read as `Moved`: the out that stands, plus whether this tick's flattening differs from last tick's — computed by the engine, never by the consumer, because value-plane equality (absent equals absent) is the engine's to define and `PartialEq` is refused on anything that may decline ([`outs.absence.typed`](#outsabsencetyped)).

`moved` is the value-plane edge where `fired` is the publication edge, and the two part ways exactly once: a level whose out went *absent* moved without firing — `None` is a value ([`outs.absence.one-reading`](#outsabsenceone-reading)), and a consumer acting on changes has to see it change to nothing. Both compare the `Flat` slots, so what a flattening leaves out (an intent's timestamp) is what a change is not.

A run output carries no `Moved`: its edge is its elements, and what a run publishes is its producer's own policy. The dep plane is untouched either way — a dep read still cannot reach any of it.
