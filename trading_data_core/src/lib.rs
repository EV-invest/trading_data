//! Shared trade primitives: the parse boundary between exchange feeds (`v_exchanges`) and the
//! persistence tier. Both sides see the same types, so a websocket stream parses straight into
//! persistence rows with no lossy hand-off.

pub mod batch;
pub use batch::*;

pub mod exchange;
pub use exchange::*;

pub mod klines;
pub use klines::*;

pub mod pair;
pub use pair::*;

pub mod side;
pub use side::*;

pub mod timestamp;
pub use timestamp::*;

pub mod usd;
pub use usd::*;
