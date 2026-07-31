//! Raw columnar lane holders. Precision lives on the holder — hoisted once per run — so the
//! columns stay exactly what the venue sent and what the disk holds, with no f64 round trip in
//! between. Each holder has an owned twin serving as lane buffer, `Nudge` scratch and ingest
//! accumulator.

use core::ops::Range;

use trading_data_dag::{Flat, Glance};

use crate::{Local, PrecisionPriceQty, RelayCols, Side, Span, Timestamped, Timestamps, Ts, Venue};

#[derive(Clone, Copy, Debug)]
pub struct TradeCols<'a> {
	pub prec: PrecisionPriceQty,
	pub ts: RelayCols<'a, Venue, Local>,
	pub monotonic_seq: &'a [u64],
	pub side: &'a [Side],
	pub price: &'a [i32],
	pub qty: &'a [u32],
}

impl TradeCols<'_> {
	pub fn len(&self) -> usize {
		self.monotonic_seq.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// The per-element venue readings. A trade lane always attests them; the `Option` on
	/// [`RelayCols`] is there for runs whose origin does not.
	pub fn exec(&self) -> &[Ts<Venue>] {
		self.ts.exec.expect("a trade lane attests every element's execution time")
	}
}

impl Timestamped for TradeCols<'_> {
	fn timestamps(&self) -> Timestamps {
		Timestamps::Accumulator {
			venue: span(self.exec()),
			local: self.ts.recv,
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct DeltaCols<'a> {
	pub prec: PrecisionPriceQty,
	pub ts: RelayCols<'a, Venue, Local>,
	pub monotonic_seq: &'a [u64],
	pub side: &'a [Side],
	pub price: &'a [i32],
	/// `0` = delete this level.
	pub qty: &'a [u32],
}

impl DeltaCols<'_> {
	pub fn len(&self) -> usize {
		self.monotonic_seq.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn exec(&self) -> &[Ts<Venue>] {
		self.ts.exec.expect("a book lane attests every element's execution time")
	}
}

impl Timestamped for DeltaCols<'_> {
	fn timestamps(&self) -> Timestamps {
		Timestamps::Accumulator {
			venue: span(self.exec()),
			local: self.ts.recv,
		}
	}
}

/// Whether a run of levels is market activity or our own reconciliation. The kind is the frame's
/// identity, not an attribute: a correction carries levels that are not market activity, and
/// folding one into a flow or imbalance derivation fabricates signal out of a dropped websocket
/// packet.
#[derive(Clone, Copy, Debug)]
pub enum DeltaFrame<'a> {
	Update(DeltaCols<'a>),
	Correction(DeltaCols<'a>),
}

impl<'a> DeltaFrame<'a> {
	pub fn cols(&self) -> DeltaCols<'a> {
		match self {
			Self::Update(c) | Self::Correction(c) => *c,
		}
	}

	pub fn kind(&self) -> FrameKind {
		match self {
			Self::Update(_) => FrameKind::Update,
			Self::Correction(_) => FrameKind::Correction,
		}
	}

	pub fn of(kind: FrameKind, cols: DeltaCols<'a>) -> Self {
		match kind {
			FrameKind::Update => Self::Update(cols),
			FrameKind::Correction => Self::Correction(cols),
		}
	}
}

/// The stored discriminant of [`DeltaFrame`] — a `UInt8` column exactly parallel to the side byte.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FrameKind {
	#[default]
	Update,
	Correction,
}

fn span(exec: &[Ts<Venue>]) -> Span<Venue> {
	match (exec.first(), exec.last()) {
		(Some(&f), Some(&l)) => Span::new(f, l),
		_ => Span::default(),
	}
}

/// Owned twin of [`TradeCols`]: lane buffer, `Nudge` scratch and ingest accumulator in one.
#[derive(Clone, Debug, Default)]
pub struct TradeBuf {
	pub prec: PrecisionPriceQty,
	exec: Vec<Ts<Venue>>,
	/// All-or-nothing across the buffer: a venue either reports a wire time distinct from `exec` or
	/// it does not.
	send: Option<Vec<Ts<Venue>>>,
	recv: Vec<Option<Ts<Local>>>,
	monotonic_seq: Vec<u64>,
	side: Vec<Side>,
	price: Vec<i32>,
	qty: Vec<u32>,
}

impl TradeBuf {
	pub fn new(prec: PrecisionPriceQty) -> Self {
		Self { prec, ..Default::default() }
	}

	pub fn len(&self) -> usize {
		self.monotonic_seq.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn clear(&mut self) {
		self.exec.clear();
		self.send = None;
		self.recv.clear();
		self.monotonic_seq.clear();
		self.side.clear();
		self.price.clear();
		self.qty.clear();
	}

	/// One value per column, which is the whole point of a columnar buffer.
	#[allow(clippy::too_many_arguments)]
	pub fn push(&mut self, exec: Ts<Venue>, send: Option<Ts<Venue>>, recv: Option<Ts<Local>>, seq: u64, side: Side, price: i32, qty: u32) {
		match (send, &mut self.send) {
			(Some(s), Some(col)) => col.push(s),
			(Some(s), slot @ None) => {
				assert!(self.exec.is_empty(), "venue wire time appeared mid-run: it is a per-venue property");
				*slot = Some(vec![s]);
			}
			(None, None) => {}
			(None, Some(_)) => panic!("venue wire time vanished mid-run: it is a per-venue property"),
		}
		self.exec.push(exec);
		self.recv.push(recv);
		self.monotonic_seq.push(seq);
		self.side.push(side);
		self.price.push(price);
		self.qty.push(qty);
	}

	pub fn extend(&mut self, cols: TradeCols<'_>) {
		assert_eq!(self.prec, cols.prec, "precision differs across a trade run");
		let recv = cols.ts.recv;
		for (i, &exec) in cols.exec().iter().enumerate() {
			// A run's reception window collapses onto each element: the whole run arrived within it.
			self.push(
				exec,
				cols.ts.send.map(|s| s[i]),
				recv.map(|r| r.last),
				cols.monotonic_seq[i],
				cols.side[i],
				cols.price[i],
				cols.qty[i],
			);
		}
	}

	pub fn exec_at(&self, i: usize) -> Ts<Venue> {
		self.exec[i]
	}

	pub fn cols(&self, r: Range<usize>) -> TradeCols<'_> {
		TradeCols {
			prec: self.prec,
			ts: RelayCols {
				exec: Some(&self.exec[r.clone()]),
				send: self.send.as_ref().map(|s| &s[r.clone()]),
				recv: recv_span(&self.recv, r.clone()),
			},
			monotonic_seq: &self.monotonic_seq[r.clone()],
			side: &self.side[r.clone()],
			price: &self.price[r.clone()],
			qty: &self.qty[r],
		}
	}

	/// Reclaim an emitted prefix, keeping the lane bounded to its un-emitted window.
	pub fn drain_prefix(&mut self, n: usize) {
		self.exec.drain(..n);
		if let Some(s) = &mut self.send {
			s.drain(..n);
		}
		self.recv.drain(..n);
		self.monotonic_seq.drain(..n);
		self.side.drain(..n);
		self.price.drain(..n);
		self.qty.drain(..n);
	}

	/// Bump one flattened slot of the *last* element, in whole ticks. Returns the perturbation
	/// actually applied in the column's own units; a tick grid cannot represent anything finer.
	pub fn bump_last(&mut self, slot: usize, h: f64) -> f64 {
		let Some(i) = self.len().checked_sub(1) else { return 0.0 };
		match slot {
			0 => {
				let scale = self.prec.price.scale();
				let ticks = ((h * scale).round() as i32).max(1);
				self.price[i] += ticks;
				ticks as f64 / scale
			}
			1 => {
				let scale = self.prec.qty.scale();
				let ticks = ((h * scale).round() as u32).max(1);
				self.qty[i] += ticks;
				ticks as f64 / scale
			}
			_ => unreachable!("TradeCols flattens to (price, qty)"),
		}
	}
}

/// Owned twin of [`DeltaCols`]. Carries the per-element [`FrameKind`] so a lane can break its run
/// where the kind changes — a frame is homogeneous, and the enum wraps the run, not each row.
#[derive(Clone, Debug, Default)]
pub struct DeltaBuf {
	pub prec: PrecisionPriceQty,
	exec: Vec<Ts<Venue>>,
	recv: Vec<Option<Ts<Local>>>,
	monotonic_seq: Vec<u64>,
	kind: Vec<FrameKind>,
	side: Vec<Side>,
	price: Vec<i32>,
	qty: Vec<u32>,
}

impl DeltaBuf {
	pub fn new(prec: PrecisionPriceQty) -> Self {
		Self { prec, ..Default::default() }
	}

	pub fn len(&self) -> usize {
		self.monotonic_seq.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn clear(&mut self) {
		self.exec.clear();
		self.recv.clear();
		self.monotonic_seq.clear();
		self.kind.clear();
		self.side.clear();
		self.price.clear();
		self.qty.clear();
	}

	/// One value per column, which is the whole point of a columnar buffer.
	#[allow(clippy::too_many_arguments)]
	pub fn push(&mut self, exec: Ts<Venue>, recv: Option<Ts<Local>>, seq: u64, kind: FrameKind, side: Side, price: i32, qty: u32) {
		self.exec.push(exec);
		self.recv.push(recv);
		self.monotonic_seq.push(seq);
		self.kind.push(kind);
		self.side.push(side);
		self.price.push(price);
		self.qty.push(qty);
	}

	pub fn extend(&mut self, frame: DeltaFrame<'_>) {
		let (kind, cols) = (frame.kind(), frame.cols());
		assert_eq!(self.prec, cols.prec, "precision differs across a delta run");
		let recv = cols.ts.recv;
		for (i, &exec) in cols.exec().iter().enumerate() {
			self.push(exec, recv.map(|r| r.last), cols.monotonic_seq[i], kind, cols.side[i], cols.price[i], cols.qty[i]);
		}
	}

	pub fn exec_at(&self, i: usize) -> Ts<Venue> {
		self.exec[i]
	}

	pub fn kind_at(&self, i: usize) -> FrameKind {
		self.kind[i]
	}

	/// An empty run is `Update`: it carries no levels, so it asserts nothing about reconciliation —
	/// reading the kind out of the buffer would leak whichever frame happens to sit at `r.start`.
	pub fn frame(&self, r: Range<usize>) -> DeltaFrame<'_> {
		let kind = if r.is_empty() { FrameKind::Update } else { self.kind[r.start] };
		DeltaFrame::of(kind, self.cols(r))
	}

	fn cols(&self, r: Range<usize>) -> DeltaCols<'_> {
		DeltaCols {
			prec: self.prec,
			ts: RelayCols {
				exec: Some(&self.exec[r.clone()]),
				send: None,
				recv: recv_span(&self.recv, r.clone()),
			},
			monotonic_seq: &self.monotonic_seq[r.clone()],
			side: &self.side[r.clone()],
			price: &self.price[r.clone()],
			qty: &self.qty[r],
		}
	}

	pub fn drain_prefix(&mut self, n: usize) {
		self.exec.drain(..n);
		self.recv.drain(..n);
		self.monotonic_seq.drain(..n);
		self.kind.drain(..n);
		self.side.drain(..n);
		self.price.drain(..n);
		self.qty.drain(..n);
	}

	pub fn bump_last(&mut self, slot: usize, h: f64) -> f64 {
		let Some(i) = self.len().checked_sub(1) else { return 0.0 };
		match slot {
			0 => {
				let scale = self.prec.price.scale();
				let ticks = ((h * scale).round() as i32).max(1);
				self.price[i] += ticks;
				ticks as f64 / scale
			}
			1 => {
				let scale = self.prec.qty.scale();
				let ticks = ((h * scale).round() as u32).max(1);
				self.qty[i] += ticks;
				ticks as f64 / scale
			}
			_ => unreachable!("DeltaCols flattens to (price, qty)"),
		}
	}
}

/// A run's reception window, present only when every element of it was recorded live.
fn recv_span(recv: &[Option<Ts<Local>>], r: Range<usize>) -> Option<Span<Local>> {
	let first = (*recv.get(r.start)?)?;
	let last = (*recv.get(r.end.checked_sub(1)?)?)?;
	Some(Span::new(first, last))
}

// Observation only, and the one place a raw column becomes an f64: a columnar out flattens its
// *last* element through the run's precision. Orphan rules put these here rather than in the
// persistence tier, where the rest of the dag impls live.

fn last_level(prec: PrecisionPriceQty, price: &[i32], qty: &[u32], out: &mut [f64]) -> bool {
	match price.len().checked_sub(1) {
		Some(i) => {
			out.copy_from_slice(&[price[i] as f64 / prec.price.scale(), qty[i] as f64 / prec.qty.scale()]);
			true
		}
		None => {
			out.fill(f64::NAN);
			false
		}
	}
}

impl Flat for TradeCols<'_> {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		last_level(self.prec, self.price, self.qty, out)
	}

	fn fires(&self) -> usize {
		self.len()
	}
}

impl Glance for TradeCols<'_> {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match self.len().checked_sub(1) {
			Some(i) => write!(
				f,
				"{} {}@{}",
				self.side[i],
				self.qty[i] as f64 / self.prec.qty.scale(),
				self.price[i] as f64 / self.prec.price.scale()
			),
			None => f.write_str("[]"),
		}
	}
}

impl Flat for DeltaFrame<'_> {
	const DIMS: &'static [usize] = &[2];

	fn flat(&self, out: &mut [f64]) -> bool {
		let c = self.cols();
		last_level(c.prec, c.price, c.qty, out)
	}

	fn fires(&self) -> usize {
		self.cols().len()
	}
}

impl Glance for DeltaFrame<'_> {
	fn glance(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		let c = self.cols();
		match c.len().checked_sub(1) {
			Some(i) => write!(
				f,
				"{:?} {} {}@{}",
				self.kind(),
				c.side[i],
				c.qty[i] as f64 / c.prec.qty.scale(),
				c.price[i] as f64 / c.prec.price.scale()
			),
			None => f.write_str("[]"),
		}
	}
}
