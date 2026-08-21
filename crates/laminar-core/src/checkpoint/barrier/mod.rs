//! Checkpoint barrier protocol.
//!
//! The coordinator injects barriers into sources via [`CheckpointBarrierInjector`].
//! Sources deliver barriers alongside events. The fast path (no pending barrier)
//! is a single `AtomicU64` load (~10ns).

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
    /// This checkpoint participates in an assignment handoff.
    pub const HANDOFF: u64 = 1 << 3;
}

/// Internal flag layout used only by the clustered shuffle fixed-point flush.
///
/// These bits never appear in a source barrier, checkpoint request, or control-plane
/// announcement. Keeping the tag separate from the wave ordinal lets a receiver reject an
/// untagged or partially encoded marker instead of accidentally treating it as wave zero.
const SHUFFLE_FLUSH_TAG: u64 = 1 << 31;
const SHUFFLE_FLUSH_WAVE_SHIFT: u32 = 32;
const SHUFFLE_FLUSH_WAVE_MASK: u64 = 0x7fff_ffff << SHUFFLE_FLUSH_WAVE_SHIFT;
const SHUFFLE_FLUSH_ACTIVITY: u64 = 1 << 63;
const SHUFFLE_FLUSH_INTERNAL_MASK: u64 =
    SHUFFLE_FLUSH_TAG | SHUFFLE_FLUSH_WAVE_MASK | SHUFFLE_FLUSH_ACTIVITY;

/// Largest shuffle flush wave representable in a checkpoint barrier.
pub const MAX_SHUFFLE_FLUSH_WAVE: u64 = 0x7fff_ffff;

/// Encode one internal clustered-shuffle flush marker.
///
/// The returned value is valid only in the `flags` field of an in-band shuffle barrier. It must
/// not be copied into checkpoint metadata or cluster control traffic.
///
/// # Errors
/// Returns an error instead of truncating a wave ordinal that does not fit the reserved field.
pub fn encode_shuffle_flush_flags(wave: u64, activity: bool) -> Result<u64, &'static str> {
    if wave > MAX_SHUFFLE_FLUSH_WAVE {
        return Err("shuffle flush wave exceeds its reserved checkpoint-barrier field");
    }
    Ok(SHUFFLE_FLUSH_TAG
        | (wave << SHUFFLE_FLUSH_WAVE_SHIFT)
        | if activity { SHUFFLE_FLUSH_ACTIVITY } else { 0 })
}

/// Decode one internal clustered-shuffle flush marker into `(wave, sender_activity)`.
///
/// # Errors
/// Rejects an untagged marker and every bit outside the internal layout. This is deliberately
/// stricter than the general-purpose checkpoint barrier flags because shuffle-wave metadata must
/// never be confused with source/control-plane behavior flags.
pub fn decode_shuffle_flush_flags(flags: u64) -> Result<(u64, bool), &'static str> {
    if flags & SHUFFLE_FLUSH_TAG == 0 {
        return Err("shuffle flush marker is missing its internal tag");
    }
    if flags & !SHUFFLE_FLUSH_INTERNAL_MASK != 0 {
        return Err("shuffle flush marker uses reserved or checkpoint behavior flags");
    }
    Ok((
        (flags & SHUFFLE_FLUSH_WAVE_MASK) >> SHUFFLE_FLUSH_WAVE_SHIFT,
        flags & SHUFFLE_FLUSH_ACTIVITY != 0,
    ))
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

    /// Whether the duplicated wire fields represent one nonzero durable checkpoint identity.
    #[must_use]
    pub const fn is_canonical(self) -> bool {
        self.checkpoint_id != 0 && self.epoch == self.checkpoint_id
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

/// Cross-thread barrier injector for source operators.
///
/// The coordinator thread publishes a barrier command via
/// [`trigger`](Self::trigger). Source operators poll via
/// [`BarrierPollHandle::poll`] on each iteration of their event loop.
///
/// ## Fast Path
///
/// The poll path is a single `AtomicU64::load(Relaxed)` — typically < 10ns.
/// Only when a barrier is pending does the source perform a compare-exchange
/// to claim it.
#[derive(Debug)]
pub struct CheckpointBarrierInjector {
    /// Command lifecycle. Payload fields are published before `PENDING`.
    state: Arc<AtomicU64>,
    /// Full-width checkpoint ID for the pending command.
    checkpoint_id: Arc<AtomicU64>,
    /// Full-width epoch for the pending command.
    epoch: Arc<AtomicU64>,
    /// Full-width flags for the pending command.
    flags: Arc<AtomicU64>,
}

const STATE_IDLE: u64 = 0;
const STATE_WRITING: u64 = 1;
const STATE_PENDING: u64 = 2;
const STATE_CONSUMING: u64 = 3;

impl CheckpointBarrierInjector {
    /// Create a new injector with no pending barrier.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicU64::new(STATE_IDLE)),
            checkpoint_id: Arc::new(AtomicU64::new(0)),
            epoch: Arc::new(AtomicU64::new(0)),
            flags: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get a handle that source operators use to poll for barriers.
    #[must_use]
    pub fn handle(&self) -> BarrierPollHandle {
        BarrierPollHandle {
            state: Arc::clone(&self.state),
            checkpoint_id: Arc::clone(&self.checkpoint_id),
            epoch: Arc::clone(&self.epoch),
            flags: Arc::clone(&self.flags),
        }
    }

    /// Whether no barrier command is currently being published, pending, or consumed.
    /// The coordinator uses this as an all-source preflight before fan-out.
    #[must_use]
    pub fn can_trigger(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_IDLE
    }

    /// Trigger a new checkpoint barrier.
    ///
    /// The next [`BarrierPollHandle::poll`] call on any source will
    /// observe this barrier and return it. Returns `false` without
    /// modifying the pending command if the identity is zero or noncanonical, or
    /// if another trigger is being published, is pending, or is being consumed.
    ///
    /// # Arguments
    ///
    /// * `barrier` - Exact barrier command to publish. `checkpoint_id` and `epoch`
    ///   must name the same nonzero durable checkpoint.
    #[must_use = "a rejected trigger leaves the existing barrier pending"]
    pub fn trigger(&self, barrier: CheckpointBarrier) -> bool {
        if !barrier.is_canonical()
            || self
                .state
                .compare_exchange(
                    STATE_IDLE,
                    STATE_WRITING,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return false;
        }

        // Writers own the payload while state is WRITING. A release-publish of
        // PENDING makes every full-width field visible to the claiming poller.
        self.checkpoint_id
            .store(barrier.checkpoint_id, Ordering::Relaxed);
        self.epoch.store(barrier.epoch, Ordering::Relaxed);
        self.flags.store(barrier.flags, Ordering::Relaxed);
        self.state.store(STATE_PENDING, Ordering::Release);
        true
    }

    /// Cancel the pending barrier with this exact checkpoint identity.
    ///
    /// Returns `true` only when this call claims and cancels a pending command
    /// whose checkpoint ID and epoch both match. A stale identity, an idle
    /// injector, or a barrier already claimed by a poller returns `false`.
    /// Mismatched pending commands remain available to pollers unchanged.
    #[must_use = "a false result means the exact pending barrier was not cancelled"]
    pub fn cancel_exact(&self, barrier: CheckpointBarrier) -> bool {
        if !barrier.is_canonical()
            || self.state.load(Ordering::Acquire) != STATE_PENDING
            || self.checkpoint_id.load(Ordering::Relaxed) != barrier.checkpoint_id
            || self.epoch.load(Ordering::Relaxed) != barrier.epoch
        {
            return false;
        }

        if self
            .state
            .compare_exchange(
                STATE_PENDING,
                STATE_CONSUMING,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return false;
        }

        // Revalidate under exclusive ownership: a poll and a subsequent
        // trigger may have changed the payload after the optimistic check.
        let matches = self.checkpoint_id.load(Ordering::Relaxed) == barrier.checkpoint_id
            && self.epoch.load(Ordering::Relaxed) == barrier.epoch;
        self.state.store(
            if matches { STATE_IDLE } else { STATE_PENDING },
            Ordering::Release,
        );
        matches
    }
}

impl Default for CheckpointBarrierInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CheckpointBarrierInjector {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            checkpoint_id: Arc::clone(&self.checkpoint_id),
            epoch: Arc::clone(&self.epoch),
            flags: Arc::clone(&self.flags),
        }
    }
}

/// Handle used by source operators to poll for pending barriers.
///
/// Cloned from [`CheckpointBarrierInjector::handle`] and stored in the
/// source operator. The fast path is a single atomic load.
#[derive(Debug, Clone)]
pub struct BarrierPollHandle {
    /// Shared command lifecycle.
    state: Arc<AtomicU64>,
    /// Full-width checkpoint ID payload.
    checkpoint_id: Arc<AtomicU64>,
    /// Full-width epoch payload.
    epoch: Arc<AtomicU64>,
    /// Full-width flags payload.
    flags: Arc<AtomicU64>,
}

impl BarrierPollHandle {
    /// Poll for a pending barrier.
    ///
    /// Returns `Some(CheckpointBarrier)` if a barrier is pending and
    /// this call successfully claimed it (exactly-once delivery across
    /// handles sharing the same injector). Returns `None` if no barrier
    /// is pending or another handle already claimed it.
    ///
    /// ## Performance
    ///
    /// Fast path (no barrier): single `load(Relaxed)` — < 10ns.
    /// Slow path (barrier pending): one `compare_exchange`.
    #[must_use]
    pub fn poll(&self) -> Option<CheckpointBarrier> {
        // Fast path: no barrier pending
        if self.state.load(Ordering::Relaxed) != STATE_PENDING {
            return None;
        }

        // Claim before reading. CONSUMING prevents the next trigger from
        // overwriting payload fields until this poller has copied both.
        if self
            .state
            .compare_exchange(
                STATE_PENDING,
                STATE_CONSUMING,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let barrier = CheckpointBarrier {
                checkpoint_id: self.checkpoint_id.load(Ordering::Relaxed),
                epoch: self.epoch.load(Ordering::Relaxed),
                flags: self.flags.load(Ordering::Relaxed),
            };
            self.state.store(STATE_IDLE, Ordering::Release);
            Some(barrier)
        } else {
            // Another thread claimed it first
            None
        }
    }
}

#[cfg(test)]
mod tests;
