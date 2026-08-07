# Parameters

r[params.newtype.no-fixed-generics]

A type MUST NOT be declared by fixing another type's generic parameter. No `type X = Y<K>`, no
`node_alias!` that supplies a constant, and no node whose `Deps` name a literal where the node
itself could carry the parameter. A parameter is written out at the point the graph is wired, every
time, however long that makes the line.

The parameter is half the type's identity — `Bars<TF_1MIN>` and `Bars<TF_1H>` are two series, not
one series configured twice. Binding it behind a name erases that half everywhere the name is read:
`Change3m`'s reader cannot see which series it fell out of, `RsiSeries`'s cannot see that swapping
it silently reroutes four chains, and neither can be asked for the other period without a second
type being written. What looks like one line saved is the whole parameter space collapsed to the
one point someone happened to need first, and every consumer downstream inherits that point as
though it were the shape of the problem.

A constant a type's own construction fixes is not this. `Buffering<B, Elems<2>>` under a delta is
the arity of a difference, and `Folding<_, Unbounded>` under a recurrence is what a recurrence
reaches over — neither names a choice a caller could have made differently, so neither is a
parameter being pinned. The test is whether a second value of it is meaningful: if it is, it stays
in the signature.
