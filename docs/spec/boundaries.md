# Crate Boundaries

Derived from the crate stack in [ARCHITECTURE.md](../ARCHITECTURE.md#crates).

r[boundaries.examples.facade-only]

An example crate under `examples/` MUST depend on `trading_data` only. Naming any
`trading_data_*` sub-crate — in `Cargo.toml` or by `::trading_data_dag`-style path — is a
violation: it is the facade that defines what a downstream user can build against, and an
example that reaches past it stops being evidence that the facade is sufficient.

Whatever an example needs is re-exported from `trading_data` or does not belong in an example.
