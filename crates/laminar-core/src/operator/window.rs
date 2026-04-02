//! Window assignment, emit strategies, and CDC types for stream processing.
//!
//! - [`TumblingWindowAssigner`]: Fixed-size, non-overlapping windows
//! - [`EmitStrategy`]: Controls when window results are emitted
//! - [`ChangelogRecord`]: CDC records with Z-set weights

use super::Event;
use smallvec::SmallVec;
use std::time::Duration;

/// Strategy for when window results should be emitted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EmitStrategy {
    /// Emit when watermark passes window end (default, most efficient).
    #[default]
    OnWatermark,
    /// Emit intermediate results at fixed intervals, plus final on watermark.
    Periodic(Duration),
    /// Emit after every state change (lowest latency, highest overhead).
    OnUpdate,
    /// Emit only when window closes. Append-only safe, no retractions.
    /// SQL: `EMIT ON WINDOW CLOSE`
    OnWindowClose,
    /// Emit changelog records with Z-set weights for CDC pipelines.
    /// SQL: `EMIT CHANGES`
    Changelog,
    /// Suppress all intermediate results, emit only finalized.
    /// SQL: `EMIT FINAL`
    Final,
}

impl EmitStrategy {
    /// Returns true if this strategy requires periodic timer registration.
    #[must_use]
    pub fn needs_periodic_timer(&self) -> bool {
        matches!(self, Self::Periodic(_))
    }

    /// Returns the periodic interval if this is a periodic strategy.
    #[must_use]
    pub fn periodic_interval(&self) -> Option<Duration> {
        match self {
            Self::Periodic(d) => Some(*d),
            _ => None,
        }
    }

    /// Returns true if results should be emitted on every update.
    #[must_use]
    pub fn emits_on_update(&self) -> bool {
        matches!(self, Self::OnUpdate)
    }

    /// Returns true if this strategy emits intermediate results before window close.
    #[must_use]
    pub fn emits_intermediate(&self) -> bool {
        matches!(self, Self::OnUpdate | Self::Periodic(_))
    }

    /// Returns true if this strategy requires changelog/Z-set support.
    #[must_use]
    pub fn requires_changelog(&self) -> bool {
        matches!(self, Self::Changelog)
    }

    /// Returns true if safe for append-only sinks (no retractions).
    #[must_use]
    pub fn is_append_only_compatible(&self) -> bool {
        matches!(self, Self::OnWindowClose | Self::Final)
    }

    /// Returns true if late data should generate retractions.
    #[must_use]
    pub fn generates_retractions(&self) -> bool {
        matches!(self, Self::OnWatermark | Self::OnUpdate | Self::Changelog)
    }

    /// Returns true if this strategy should suppress intermediate emissions.
    #[must_use]
    pub fn suppresses_intermediate(&self) -> bool {
        matches!(self, Self::OnWindowClose | Self::Final)
    }

    /// Returns true if late data should be dropped entirely.
    #[must_use]
    pub fn drops_late_data(&self) -> bool {
        matches!(self, Self::Final)
    }
}

/// Unique identifier for a window (start inclusive, end exclusive, milliseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowId {
    /// Window start timestamp (inclusive, milliseconds).
    pub start: i64,
    /// Window end timestamp (exclusive, milliseconds).
    pub end: i64,
}

impl WindowId {
    /// Creates a new window ID.
    #[must_use]
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Window duration in milliseconds.
    #[must_use]
    pub fn duration_ms(&self) -> i64 {
        self.end - self.start
    }

    /// Converts to a 16-byte big-endian key for state storage.
    #[inline]
    #[must_use]
    pub fn to_key(&self) -> super::TimerKey {
        super::TimerKey::from(self.to_key_inline())
    }

    /// Stack-allocated 16-byte key (zero-allocation hot path).
    #[inline]
    #[must_use]
    pub fn to_key_inline(&self) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&self.start.to_be_bytes());
        key[8..16].copy_from_slice(&self.end.to_be_bytes());
        key
    }

    /// Parses from a 16-byte big-endian key. Returns `None` if wrong length.
    #[must_use]
    pub fn from_key(key: &[u8]) -> Option<Self> {
        if key.len() != 16 {
            return None;
        }
        let start = i64::from_be_bytes(key[0..8].try_into().ok()?);
        let end = i64::from_be_bytes(key[8..16].try_into().ok()?);
        Some(Self { start, end })
    }
}

/// Window assignment results. Inline storage for up to 4 windows (avoids heap).
pub type WindowIdVec = SmallVec<[WindowId; 4]>;

/// CDC operation type with Z-set weights.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    /// +1 weight
    Insert,
    /// -1 weight
    Delete,
    /// -1 weight (retraction before update)
    UpdateBefore,
    /// +1 weight (new value after update)
    UpdateAfter,
}

impl CdcOperation {
    /// Z-set weight: +1 for inserts, -1 for deletes.
    #[must_use]
    pub fn weight(&self) -> i32 {
        match self {
            Self::Insert | Self::UpdateAfter => 1,
            Self::Delete | Self::UpdateBefore => -1,
        }
    }

    /// Returns true for insert-type operations.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self, Self::Insert | Self::UpdateAfter)
    }

    /// Returns true for delete-type operations.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete | Self::UpdateBefore)
    }

    /// Compact u8 encoding for storage.
    #[inline]
    #[must_use]
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Insert => 0,
            Self::Delete => 1,
            Self::UpdateBefore => 2,
            Self::UpdateAfter => 3,
        }
    }

    /// Decode from u8. Returns `None` for out-of-range values.
    #[inline]
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Insert),
            1 => Some(Self::Delete),
            2 => Some(Self::UpdateBefore),
            3 => Some(Self::UpdateAfter),
            _ => None,
        }
    }
}

/// Changelog record with Z-set weight for CDC pipelines.
#[derive(Debug, Clone)]
pub struct ChangelogRecord {
    /// The CDC operation type.
    pub operation: CdcOperation,
    /// Z-set weight (+1 for insert, -1 for delete).
    pub weight: i32,
    /// Timestamp when this change was emitted.
    pub emit_timestamp: i64,
    /// The event data.
    pub event: Event,
}

impl ChangelogRecord {
    /// Creates an insert record (+1 weight).
    #[must_use]
    pub fn insert(event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation: CdcOperation::Insert,
            weight: 1,
            emit_timestamp,
            event,
        }
    }

    /// Creates a delete record (-1 weight).
    #[must_use]
    pub fn delete(event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation: CdcOperation::Delete,
            weight: -1,
            emit_timestamp,
            event,
        }
    }

    /// Creates an update retraction pair (`UpdateBefore`, `UpdateAfter`).
    #[must_use]
    pub fn update(old_event: Event, new_event: Event, emit_timestamp: i64) -> (Self, Self) {
        let before = Self {
            operation: CdcOperation::UpdateBefore,
            weight: -1,
            emit_timestamp,
            event: old_event,
        };
        let after = Self {
            operation: CdcOperation::UpdateAfter,
            weight: 1,
            emit_timestamp,
            event: new_event,
        };
        (before, after)
    }

    /// Creates a record from raw parts.
    #[must_use]
    pub fn new(operation: CdcOperation, event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation,
            weight: operation.weight(),
            emit_timestamp,
            event,
        }
    }

    /// Returns true for insert-type records.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        self.operation.is_insert()
    }

    /// Returns true for delete-type records.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.operation.is_delete()
    }
}

/// Trait for assigning events to windows.
pub trait WindowAssigner: Send {
    /// Assigns a timestamp to one or more windows.
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec;

    /// Maximum timestamp assignable to a window ending at `window_end`.
    fn max_timestamp(&self, window_end: i64) -> i64 {
        window_end - 1
    }
}

/// Tumbling window assigner: fixed-size, non-overlapping windows aligned to epoch.
#[derive(Debug, Clone)]
pub struct TumblingWindowAssigner {
    size_ms: i64,
    offset_ms: i64,
}

impl TumblingWindowAssigner {
    /// # Panics
    /// Panics if size is zero.
    #[must_use]
    pub fn new(size: Duration) -> Self {
        let size_ms = i64::try_from(size.as_millis()).expect("Window size must fit in i64");
        assert!(size_ms > 0, "Window size must be positive");
        Self {
            size_ms,
            offset_ms: 0,
        }
    }

    /// # Panics
    /// Panics if `size_ms` is zero or negative.
    #[must_use]
    pub fn from_millis(size_ms: i64) -> Self {
        assert!(size_ms > 0, "Window size must be positive");
        Self {
            size_ms,
            offset_ms: 0,
        }
    }

    /// Sets window offset in milliseconds for timezone-aligned windows.
    #[must_use]
    pub fn with_offset_ms(mut self, offset_ms: i64) -> Self {
        self.offset_ms = offset_ms;
        self
    }

    /// Window size in milliseconds.
    #[must_use]
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }

    /// Window offset in milliseconds.
    #[must_use]
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms
    }

    /// O(1) window assignment. Floor-divides timestamp into window boundaries.
    #[inline]
    #[must_use]
    pub fn assign(&self, timestamp: i64) -> WindowId {
        let adjusted = timestamp - self.offset_ms;
        let window_start = if adjusted >= 0 {
            (adjusted / self.size_ms) * self.size_ms
        } else {
            ((adjusted - self.size_ms + 1) / self.size_ms) * self.size_ms
        };
        let window_start = window_start + self.offset_ms;
        WindowId::new(window_start, window_start + self.size_ms)
    }
}

impl WindowAssigner for TumblingWindowAssigner {
    #[inline]
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec {
        let mut windows = WindowIdVec::new();
        windows.push(self.assign(timestamp));
        windows
    }
}

#[cfg(test)]
mod tests;
