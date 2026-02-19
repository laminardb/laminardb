//! Checkpoint barrier protocol for distributed snapshots.
//!
//! This module implements the Chandy-Lamport style barrier protocol for
//! consistent distributed checkpoints. Barriers flow through the dataflow
//! graph alongside events, triggering state snapshots at each operator.
//!
//! ## Protocol Overview
//!
//! 1. The coordinator injects a [`CheckpointBarrier`] into all sources
//! 2. Barriers propagate through operators via [`StreamMessage::Barrier`]
//! 3. Operators with multiple inputs align barriers before snapshotting
//! 4. Once all sinks acknowledge, the checkpoint is complete
//!
//! ## Fast Path
//!
//! The [`CheckpointBarrierInjector`] uses a packed `AtomicU64` command
//! word for cross-thread signaling. The poll fast path (no pending barrier)
//! is a single atomic load — typically < 10ns.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Barrier flags — packed into the `flags` field.
pub mod flags {
    /// No special behavior.
    pub const NONE: u64 = 0;
    /// This barrier requires a full snapshot (not incremental).
    pub const FULL_SNAPSHOT: u64 = 1 << 0;
    /// This barrier is the final barrier before shutdown.
    pub const DRAIN: u64 = 1 << 1;
    /// Cancel any in-progress checkpoint with this ID.
    pub const CANCEL: u64 = 1 << 2;
}

/// A checkpoint barrier that flows through the dataflow graph.
///
/// This is a 24-byte `#[repr(C)]` value type that can be cheaply copied
/// and embedded in channel messages. It carries the checkpoint identity
/// and behavior flags.
///
/// ## Layout (24 bytes)
///
/// | Field          | Offset | Size | Description                |
/// |----------------|--------|------|----------------------------|
/// | checkpoint_id  | 0      | 8    | Unique checkpoint ID       |
/// | epoch          | 8      | 8    | Monotonic epoch number     |
/// | flags          | 16     | 8    | Behavior flags (see [`flags`]) |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct CheckpointBarrier {
    /// Unique identifier for this checkpoint.
    pub checkpoint_id: u64,
    /// Monotonically increasing epoch number.
    pub epoch: u64,
    /// Behavior flags (see [`flags`] module constants).
    pub flags: u64,
}

// Verify the struct is exactly 24 bytes as promised.
const _: () = assert!(std::mem::size_of::<CheckpointBarrier>() == 24);

impl CheckpointBarrier {
    /// Create a new barrier with the given checkpoint ID and epoch.
    #[must_use]
    pub const fn new(checkpoint_id: u64, epoch: u64) -> Self {
        Self {
            checkpoint_id,
            epoch,
            flags: flags::NONE,
        }
    }

    /// Create a barrier that requests a full snapshot.
    #[must_use]
    pub const fn full_snapshot(checkpoint_id: u64, epoch: u64) -> Self {
        Self {
            checkpoint_id,
            epoch,
            flags: flags::FULL_SNAPSHOT,
        }
    }

    /// Check whether this barrier requests a full (non-incremental) snapshot.
    #[must_use]
    pub const fn is_full_snapshot(&self) -> bool {
        self.flags & flags::FULL_SNAPSHOT != 0
    }

    /// Check whether this barrier signals drain/shutdown.
    #[must_use]
    pub const fn is_drain(&self) -> bool {
        self.flags & flags::DRAIN != 0
    }

    /// Check whether this barrier cancels an in-progress checkpoint.
    #[must_use]
    pub const fn is_cancel(&self) -> bool {
        self.flags & flags::CANCEL != 0
    }
}

/// A message that flows through streaming channels.
///
/// Wraps user events with control messages (watermarks and barriers)
/// in a single enum. Operators pattern-match on this to handle
/// data vs. control flow.
///
/// ## Generic Parameter
///
/// `T` is the event payload type — typically `RecordBatch` or a
/// domain-specific event struct.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamMessage<T> {
    /// A user data event.
    Event(T),
    /// A watermark indicating event-time progress (millis since epoch).
    Watermark(i64),
    /// A checkpoint barrier for consistent snapshots.
    Barrier(CheckpointBarrier),
}

impl<T> StreamMessage<T> {
    /// Returns `true` if this is a barrier message.
    #[must_use]
    pub const fn is_barrier(&self) -> bool {
        matches!(self, Self::Barrier(_))
    }

    /// Returns `true` if this is a watermark message.
    #[must_use]
    pub const fn is_watermark(&self) -> bool {
        matches!(self, Self::Watermark(_))
    }

    /// Returns `true` if this is a data event.
    #[must_use]
    pub const fn is_event(&self) -> bool {
        matches!(self, Self::Event(_))
    }

    /// Extracts the barrier if this is a [`StreamMessage::Barrier`].
    #[must_use]
    pub const fn as_barrier(&self) -> Option<&CheckpointBarrier> {
        match self {
            Self::Barrier(b) => Some(b),
            _ => None,
        }
    }
}

/// Packed barrier command for cross-thread signaling.
///
/// Encodes a `checkpoint_id` (upper 32 bits) and flags (lower 32 bits)
/// into a single `u64` that can be atomically stored. A value of 0
/// means "no pending barrier".
///
/// ## Encoding
///
/// ```text
/// [  checkpoint_id (32)  |  flags (32)  ]
///       bits 63..32          bits 31..0
/// ```
#[inline]
const fn pack_barrier_cmd(checkpoint_id: u32, flags: u32) -> u64 {
    ((checkpoint_id as u64) << 32) | (flags as u64)
}

/// Unpack a barrier command into (`checkpoint_id`, flags).
#[inline]
#[allow(clippy::cast_possible_truncation)]
const fn unpack_barrier_cmd(packed: u64) -> (u32, u32) {
    let checkpoint_id = (packed >> 32) as u32;
    let flags = packed as u32;
    (checkpoint_id, flags)
}

/// Cross-thread barrier injector for source operators.
///
/// The coordinator thread stores a packed barrier command via
/// [`trigger`](Self::trigger). Source operators poll via
/// [`poll`](Self::poll) on each iteration of their event loop.
///
/// ## Fast Path
///
/// The poll path is a single `AtomicU64::load(Relaxed)` — typically < 10ns.
/// Only when a barrier is pending does the source perform a compare-exchange
/// to claim it.
#[derive(Debug)]
pub struct CheckpointBarrierInjector {
    /// Packed command: 0 = no pending, otherwise (`checkpoint_id` << 32 | flags).
    cmd: Arc<AtomicU64>,
    /// The epoch counter, incremented each time a barrier is triggered.
    epoch: AtomicU64,
}

impl CheckpointBarrierInjector {
    /// Create a new injector with no pending barrier.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cmd: Arc::new(AtomicU64::new(0)),
            epoch: AtomicU64::new(0),
        }
    }

    /// Get a handle that source operators use to poll for barriers.
    #[must_use]
    pub fn handle(&self) -> BarrierPollHandle {
        BarrierPollHandle {
            cmd: Arc::clone(&self.cmd),
        }
    }

    /// Trigger a new checkpoint barrier.
    ///
    /// The next [`BarrierPollHandle::poll`] call on any source will
    /// observe this barrier and return it. If a previous barrier has
    /// not been consumed, it is superseded — this is intentional for
    /// the Chandy-Lamport protocol where only the latest checkpoint
    /// matters.
    ///
    /// # Arguments
    ///
    /// * `checkpoint_id` - Unique checkpoint ID (must fit in 32 bits)
    /// * `barrier_flags` - Barrier flags (must fit in 32 bits)
    ///
    /// # Panics
    ///
    /// Debug-asserts that `checkpoint_id` and `barrier_flags` fit in u32.
    #[allow(clippy::cast_possible_truncation)]
    pub fn trigger(&self, checkpoint_id: u64, barrier_flags: u64) {
        debug_assert!(
            u32::try_from(checkpoint_id).is_ok(),
            "checkpoint_id {checkpoint_id} exceeds u32::MAX"
        );
        debug_assert!(
            u32::try_from(barrier_flags).is_ok(),
            "barrier_flags {barrier_flags:#x} exceeds u32::MAX"
        );
        let packed = pack_barrier_cmd(checkpoint_id as u32, barrier_flags as u32);
        self.cmd.store(packed, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current epoch (number of barriers triggered).
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }
}

impl Default for CheckpointBarrierInjector {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle used by source operators to poll for pending barriers.
///
/// Cloned from [`CheckpointBarrierInjector::handle`] and stored in the
/// source operator. The fast path is a single atomic load.
#[derive(Debug, Clone)]
pub struct BarrierPollHandle {
    /// Shared packed command word.
    cmd: Arc<AtomicU64>,
}

impl BarrierPollHandle {
    /// Poll for a pending barrier.
    ///
    /// Returns `Some(CheckpointBarrier)` if a barrier is pending and
    /// this call successfully claimed it (exactly-once delivery across
    /// handles sharing the same injector). Returns `None` if no barrier
    /// is pending or another handle already claimed it.
    ///
    /// The `epoch` parameter is supplied by the caller (typically the
    /// source operator's current epoch) and is embedded in the returned
    /// barrier. The injector does not encode the epoch in the atomic
    /// command word — only checkpoint ID and flags are packed.
    ///
    /// ## Performance
    ///
    /// Fast path (no barrier): single `load(Relaxed)` — < 10ns.
    /// Slow path (barrier pending): one `compare_exchange`.
    #[must_use]
    pub fn poll(&self, epoch: u64) -> Option<CheckpointBarrier> {
        // Fast path: no barrier pending
        let packed = self.cmd.load(Ordering::Relaxed);
        if packed == 0 {
            return None;
        }

        // Barrier pending — try to claim it with compare-exchange
        if self
            .cmd
            .compare_exchange(packed, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let (checkpoint_id, barrier_flags) = unpack_barrier_cmd(packed);
            Some(CheckpointBarrier {
                checkpoint_id: u64::from(checkpoint_id),
                epoch,
                flags: u64::from(barrier_flags),
            })
        } else {
            // Another thread claimed it first
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_size() {
        assert_eq!(std::mem::size_of::<CheckpointBarrier>(), 24);
    }

    #[test]
    fn test_barrier_flags() {
        let barrier = CheckpointBarrier::new(1, 1);
        assert!(!barrier.is_full_snapshot());
        assert!(!barrier.is_drain());
        assert!(!barrier.is_cancel());

        let full = CheckpointBarrier::full_snapshot(1, 1);
        assert!(full.is_full_snapshot());
        assert!(!full.is_drain());

        let drain = CheckpointBarrier {
            checkpoint_id: 1,
            epoch: 1,
            flags: flags::DRAIN,
        };
        assert!(drain.is_drain());
    }

    #[test]
    fn test_pack_unpack_barrier_cmd() {
        let packed = pack_barrier_cmd(42, 0x03);
        let (id, flags) = unpack_barrier_cmd(packed);
        assert_eq!(id, 42);
        assert_eq!(flags, 0x03);

        // Zero encodes "no command"
        let (id, flags) = unpack_barrier_cmd(0);
        assert_eq!(id, 0);
        assert_eq!(flags, 0);
    }

    #[test]
    fn test_stream_message_variants() {
        let event: StreamMessage<String> = StreamMessage::Event("hello".into());
        assert!(event.is_event());
        assert!(!event.is_barrier());
        assert!(!event.is_watermark());

        let watermark: StreamMessage<String> = StreamMessage::Watermark(1000);
        assert!(watermark.is_watermark());

        let barrier: StreamMessage<String> =
            StreamMessage::Barrier(CheckpointBarrier::new(1, 1));
        assert!(barrier.is_barrier());
        assert_eq!(barrier.as_barrier().unwrap().checkpoint_id, 1);
    }

    #[test]
    fn test_injector_poll_no_barrier() {
        let injector = CheckpointBarrierInjector::new();
        let handle = injector.handle();

        // No barrier pending
        assert!(handle.poll(0).is_none());
    }

    #[test]
    fn test_injector_trigger_and_poll() {
        let injector = CheckpointBarrierInjector::new();
        let handle = injector.handle();

        // Trigger barrier
        injector.trigger(42, flags::FULL_SNAPSHOT);
        assert_eq!(injector.epoch(), 1);

        // Poll should return the barrier
        let barrier = handle.poll(1).unwrap();
        assert_eq!(barrier.checkpoint_id, 42);
        assert_eq!(barrier.epoch, 1);
        assert!(barrier.is_full_snapshot());

        // Second poll should return None (already claimed)
        assert!(handle.poll(1).is_none());
    }

    #[test]
    fn test_injector_multiple_handles() {
        let injector = CheckpointBarrierInjector::new();
        let handle1 = injector.handle();
        let handle2 = injector.handle();

        injector.trigger(1, flags::NONE);

        // Only one handle should claim it
        let r1 = handle1.poll(1);
        let r2 = handle2.poll(1);

        // Exactly one should succeed
        assert!(r1.is_some() || r2.is_some());
        if r1.is_some() {
            assert!(r2.is_none());
        }
    }
}
