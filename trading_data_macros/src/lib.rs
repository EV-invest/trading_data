//! TODO: `graph!` — declare a frame's node set and emit the hand-written `step` chain in
//! topological order. Pure ergonomics: `trading_data_dag`'s `Pull` bound already rejects bad orders and
//! cycles at compile time, so this only removes wiring noise. Deferred until node count makes
//! hand-wiring noisy (~30+ nodes, where the trait-solver cost also starts to bite).
