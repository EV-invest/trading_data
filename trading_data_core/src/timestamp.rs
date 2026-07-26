use eyre::{Result, bail};
use jiff::Timestamp;

/// Three timestamps describing a piece of data's lifecycle across a communication chain.
///
/// This exists for data that travels through a chain of producers and consumers — most
/// commonly **API / websocket communication**, where there are clear boundaries between
/// the moment an event *happens*, the moment a part of the chain *becomes aware* of it,
/// and the moment a part of the chain *finishes assembling* it. When you have those
/// boundaries, collapsing them into a single timestamp throws away the very information
/// you need to reason about latency and ordering, so implement this trait instead.
///
/// - `ts_event`: when the upstream authority (e.g. the exchange) says the event happened.
///   This is *their* clock, not ours.
/// - `ts_init`: when *we* first became aware of the data — the reception time of the very
///   first contributing message. For a single-message element this is just "when it
///   arrived"; for a batched/merged container it is when the first constituent arrived.
/// - `ts_last`: when *we* last wrote into the container — the reception time of the very
///   last contributing message. Equals `ts_init` for trivial single-event containers, and
///   diverges for anything we accumulate over multiple messages (order-book diffs, trade
///   batches, merged snapshots).
///
/// ## When to implement
/// - Wire types decoded from an exchange REST/websocket payload, where the payload carries
///   an event time but you also want to pin reception time.
/// - Containers you build up incrementally from a stream (a book you fold diffs into, a
///   trade batch you flush periodically): `ts_init`/`ts_last` then bracket the window of
///   reception, while `ts_event` tracks the upstream-reported time of the latest fold.
///
/// ## When NOT to implement
/// - Data that never crosses a communication boundary, where "event time" and "reception
///   time" are the same instant — there is nothing for the three fields to disambiguate.
///
/// All three are required: they are semantically distinct, and a default that silently
/// substituted one for another would mask real bugs (network latency, missed batch-merge
/// bookkeeping). If the three would genuinely coincide, that is a signal the data does not
/// belong on this trait.
pub trait Timestamped {
	fn ts_event(&self) -> Timestamp;
	fn ts_init(&self) -> Timestamp;
	fn ts_last(&self) -> Timestamp;
}

/// Like [`Timestamped`] but for data that always knows when the event happened, yet may not
/// know when we first became aware of it (`ts_init` absent when reception time wasn't captured).
pub trait PartiallyTimestamped {
	fn ts_event(&self) -> Timestamp;
	fn ts_init(&self) -> Option<Timestamp>;
}

/// Doesn't support negative timestamps
pub fn guess_timestamp_unsafe(timestamp: String) -> Result<Timestamp> {
	// Try parsing as ISO 8601 format
	if let Ok(dt) = timestamp.parse::<Timestamp>() {
		return Ok(dt);
	}

	// Try guessing the denominator
	if let Ok(num) = timestamp.parse::<u64>() {
		let len = timestamp.len();
		let nanos = match len {
			10 => num * 1_000_000_000,
			13 => num * 1_000_000,
			16 => num * 1_000,
			19 => num,
			_ => bail!("Invalid timestamp length for guessing: {len}\nTimestamp: {timestamp}"),
		};
		return Ok(Timestamp::from_nanosecond(nanos as i128).unwrap());
	}

	bail!("Couldn't parse timestamp: {timestamp}")
}
