pub trait Clock: Send + Sync {
	fn now_ns(&self) -> i64;
}

pub struct LiveClock;

impl Clock for LiveClock {
	fn now_ns(&self) -> i64 {
		jiff::Timestamp::now().as_nanosecond() as i64
	}
}
