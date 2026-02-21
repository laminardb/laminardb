//! # Window Operators
//!
//! Implementation of various window types for stream processing.
//!
//! ## Window Types
//!
//! - **Tumbling**: Fixed-size, non-overlapping windows (implemented)
//! - **Sliding**: Fixed-size, overlapping windows (future)
//! - **Session**: Dynamic windows based on activity gaps (future)
//!
//! ## Emit Strategies
//!
//! Windows support different emission strategies via [`EmitStrategy`]:
//!
//! - `OnWatermark` (default): Emit results when watermark passes window end
//! - `Periodic`: Emit intermediate results at fixed intervals
//! - `OnUpdate`: Emit after every state change (most expensive)
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::window::{
//!     TumblingWindowAssigner, TumblingWindowOperator, CountAggregator, EmitStrategy,
//! };
//! use std::time::Duration;
//!
//! // Create a 1-minute tumbling window with count aggregation
//! let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
//! let mut operator = TumblingWindowOperator::new(
//!     assigner,
//!     CountAggregator::new(),
//!     Duration::from_secs(5), // 5 second grace period
//! );
//!
//! // Emit intermediate results every 10 seconds
//! operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));
//! ```

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use crate::state::{StateStore, StateStoreExt};
use arrow_array::{Array as ArrowArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rkyv::{
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RkyvError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use smallvec::SmallVec;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for late data handling.
///
/// Controls what happens to events that arrive after their window has closed
/// (i.e., after `watermark >= window_end + allowed_lateness`).
///
/// This is particularly important for [`EmitStrategy::OnWindowClose`], where
/// late events are **never** re-incorporated into a closed window. The choice
/// is between dropping them silently or routing them to a side output for
/// separate processing (e.g., a `late_events` topic or table).
///
/// For [`EmitStrategy::Final`], late events are always silently dropped
/// regardless of this configuration.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::LateDataConfig;
/// use std::time::Duration;
///
/// // Route late events to a side output called "late_events"
/// let config = LateDataConfig::with_side_output("late_events".to_string());
///
/// // Drop late events (default behavior)
/// let config = LateDataConfig::drop();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LateDataConfig {
    /// Name of the side output for late data (None = drop late events)
    side_output: Option<String>,
}

impl LateDataConfig {
    /// Creates a config that drops late events (default behavior).
    #[must_use]
    pub fn drop() -> Self {
        Self { side_output: None }
    }

    /// Creates a config that routes late events to a named side output.
    #[must_use]
    pub fn with_side_output(name: String) -> Self {
        Self {
            side_output: Some(name),
        }
    }

    /// Returns the side output name, if configured.
    #[must_use]
    pub fn side_output(&self) -> Option<&str> {
        self.side_output.as_deref()
    }

    /// Returns true if late events should be dropped.
    #[must_use]
    pub fn should_drop(&self) -> bool {
        self.side_output.is_none()
    }
}

/// Metrics for tracking late data.
///
/// These counters track the behavior of the late data handling system
/// and can be used for monitoring and alerting.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_field_names)]
pub struct LateDataMetrics {
    /// Total number of late events received
    late_events_total: u64,
    /// Number of late events dropped (no side output configured)
    late_events_dropped: u64,
    /// Number of late events routed to side output
    late_events_side_output: u64,
}

impl LateDataMetrics {
    /// Creates a new metrics tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total number of late events received.
    #[must_use]
    pub fn late_events_total(&self) -> u64 {
        self.late_events_total
    }

    /// Returns the number of late events that were dropped.
    #[must_use]
    pub fn late_events_dropped(&self) -> u64 {
        self.late_events_dropped
    }

    /// Returns the number of late events routed to side output.
    #[must_use]
    pub fn late_events_side_output(&self) -> u64 {
        self.late_events_side_output
    }

    /// Records a dropped late event.
    pub fn record_dropped(&mut self) {
        self.late_events_total += 1;
        self.late_events_dropped += 1;
    }

    /// Records a late event routed to side output.
    pub fn record_side_output(&mut self) {
        self.late_events_total += 1;
        self.late_events_side_output += 1;
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        self.late_events_total = 0;
        self.late_events_dropped = 0;
        self.late_events_side_output = 0;
    }
}

/// Metrics for tracking window close behavior.
///
/// These counters track window lifecycle events and can be used for
/// monitoring watermark lag and window throughput. Particularly useful
/// for [`EmitStrategy::OnWindowClose`] where each window emits exactly
/// once.
#[derive(Debug, Clone, Default)]
pub struct WindowCloseMetrics {
    /// Total number of windows that have emitted and closed
    windows_closed_total: u64,
    /// Sum of close latencies in milliseconds (for computing averages)
    close_latency_sum_ms: i64,
    /// Maximum close latency observed (milliseconds)
    close_latency_max_ms: i64,
}

impl WindowCloseMetrics {
    /// Creates a new metrics tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total number of windows that have emitted and closed.
    #[must_use]
    pub fn windows_closed_total(&self) -> u64 {
        self.windows_closed_total
    }

    /// Returns the average window close latency in milliseconds.
    ///
    /// Close latency measures the delay between `window_end` and the
    /// actual emission time (`processing_time`). This reflects watermark
    /// lag — how long after the window boundary the watermark advances
    /// enough to trigger emission.
    ///
    /// Returns 0 if no windows have been closed.
    #[must_use]
    pub fn avg_close_latency_ms(&self) -> i64 {
        if self.windows_closed_total == 0 {
            0
        } else {
            self.close_latency_sum_ms / i64::try_from(self.windows_closed_total).unwrap_or(i64::MAX)
        }
    }

    /// Returns the maximum close latency observed (milliseconds).
    #[must_use]
    pub fn max_close_latency_ms(&self) -> i64 {
        self.close_latency_max_ms
    }

    /// Records a window close event.
    ///
    /// # Arguments
    ///
    /// * `window_end` - The exclusive upper bound of the closed window
    /// * `processing_time` - The wall-clock time at which the window emitted
    pub fn record_close(&mut self, window_end: i64, processing_time: i64) {
        self.windows_closed_total += 1;
        let latency = processing_time.saturating_sub(window_end).max(0);
        self.close_latency_sum_ms += latency;
        if latency > self.close_latency_max_ms {
            self.close_latency_max_ms = latency;
        }
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        self.windows_closed_total = 0;
        self.close_latency_sum_ms = 0;
        self.close_latency_max_ms = 0;
    }
}

/// Strategy for when window results should be emitted.
///
/// This controls the trade-off between result freshness and efficiency:
/// - `OnWatermark` is most efficient but has highest latency
/// - `Periodic` balances freshness and efficiency
/// - `OnUpdate` provides lowest latency but highest overhead
/// - `OnWindowClose` is for append-only sinks
/// - `Changelog` emits Z-set weighted records for CDC
/// - `Final` suppresses all intermediate results
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EmitStrategy {
    // === Existing ===
    /// Emit final results when watermark passes window end (default).
    ///
    /// This is the most efficient strategy as it only emits once per window.
    /// Results are guaranteed to be complete (within allowed lateness bounds).
    /// May emit retractions if late data arrives within lateness bounds.
    #[default]
    OnWatermark,

    /// Emit intermediate results at fixed intervals.
    ///
    /// Useful for dashboards and monitoring where periodic updates are needed
    /// before the window closes. The final result is still emitted on watermark.
    ///
    /// The duration specifies the interval between periodic emissions.
    Periodic(Duration),

    /// Emit updated results after every state change.
    ///
    /// This provides the lowest latency for result visibility but has the
    /// highest overhead. Each incoming event triggers an emission.
    ///
    /// Use with caution for high-volume streams.
    OnUpdate,

    // === New ===
    /// Emit ONLY when watermark passes window end. No intermediate emissions.
    ///
    /// **Critical for append-only sinks** (Kafka, S3, Delta Lake, Iceberg).
    /// Guarantees exactly one emission per window and strictly append-only
    /// output — no retractions, no updates.
    ///
    /// # Window Close Condition
    ///
    /// A window closes when `watermark >= window_end + allowed_lateness`.
    /// At that point, the timer fires, the final result is emitted, and
    /// window state is purged immediately.
    ///
    /// # Late Data Policy
    ///
    /// Events arriving after window close are classified as **late** and
    /// handled by [`LateDataConfig`]:
    /// - **Default**: dropped (increment `late_events_dropped` metric)
    /// - **With side output**: routed to named side output for separate
    ///   processing (increment `late_events_side_output` metric)
    ///
    /// Late data **never** re-opens a closed window. The single emission
    /// is final and immutable. This is the key contract that makes EOWC
    /// safe for append-only sinks.
    ///
    /// # Requires
    ///
    /// - A watermark definition on the source (otherwise timers never fire)
    /// - A windowed aggregation query
    ///
    /// SQL: `EMIT ON WINDOW CLOSE`
    OnWindowClose,

    /// Emit changelog records with Z-set weights.
    ///
    /// Every emission includes operation type and weight:
    /// - Insert (+1 weight)
    /// - Delete (-1 weight)
    /// - Update (retraction pair: -1 old, +1 new)
    ///
    /// Required for:
    /// - CDC pipelines
    /// - Cascading materialized views
    /// - Downstream consumers that need to track changes
    ///
    /// SQL: `EMIT CHANGES`
    Changelog,

    /// Suppress ALL intermediate results, emit only finalized.
    ///
    /// Similar to `OnWindowClose` but also suppresses:
    /// - Periodic emissions (even if Periodic was set elsewhere)
    /// - Late data retractions (drops late data entirely after window close)
    ///
    /// Use for BI reporting where only final, exact results matter.
    ///
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

    // === Helper Methods ===

    /// Returns true if this strategy emits intermediate results.
    ///
    /// Strategies that emit intermediate results (before window close):
    /// - `OnUpdate`: emits after every state change
    /// - `Periodic`: emits at fixed intervals
    ///
    /// Strategies that do NOT emit intermediate results:
    /// - `OnWatermark`: waits for watermark
    /// - `OnWindowClose`: only emits when window closes
    /// - `Changelog`: depends on trigger, but typically on watermark
    /// - `Final`: only emits final result
    #[must_use]
    pub fn emits_intermediate(&self) -> bool {
        matches!(self, Self::OnUpdate | Self::Periodic(_))
    }

    /// Returns true if this strategy requires changelog/Z-set support.
    ///
    /// The `Changelog` strategy requires the operator to track previous
    /// values and emit insert/delete/update records with weights.
    #[must_use]
    pub fn requires_changelog(&self) -> bool {
        matches!(self, Self::Changelog)
    }

    /// Returns true if this strategy is suitable for append-only sinks.
    ///
    /// Append-only sinks (Kafka, S3, Delta Lake, Iceberg) cannot handle
    /// retractions or updates. Only these strategies are safe:
    /// - `OnWindowClose`: guarantees single emission per window
    /// - `Final`: suppresses all intermediate results
    #[must_use]
    pub fn is_append_only_compatible(&self) -> bool {
        matches!(self, Self::OnWindowClose | Self::Final)
    }

    /// Returns true if late data should generate retractions.
    ///
    /// Strategies that generate retractions for late data:
    /// - `OnWatermark`: may retract previous result
    /// - `OnUpdate`: immediately emits updated result
    /// - `Changelog`: emits -old/+new pair
    ///
    /// Strategies that do NOT generate retractions:
    /// - `OnWindowClose`: drops late data (or routes to side output)
    /// - `Final`: drops late data silently
    /// - `Periodic`: depends on whether window is still open
    #[must_use]
    pub fn generates_retractions(&self) -> bool {
        matches!(self, Self::OnWatermark | Self::OnUpdate | Self::Changelog)
    }

    /// Returns true if this strategy should suppress intermediate emissions.
    ///
    /// Used to override periodic timers when a suppressing strategy is active.
    #[must_use]
    pub fn suppresses_intermediate(&self) -> bool {
        matches!(self, Self::OnWindowClose | Self::Final)
    }

    /// Returns true if late data should be dropped entirely.
    ///
    /// The `Final` strategy drops late data to ensure only exact,
    /// finalized results are emitted.
    #[must_use]
    pub fn drops_late_data(&self) -> bool {
        matches!(self, Self::Final)
    }
}

/// Unique identifier for a window.
///
/// Windows are identified by their start and end timestamps (in milliseconds).
/// For tumbling windows, these are non-overlapping intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct WindowId {
    /// Window start timestamp (inclusive, in milliseconds)
    pub start: i64,
    /// Window end timestamp (exclusive, in milliseconds)
    pub end: i64,
}

impl WindowId {
    /// Creates a new window ID.
    #[must_use]
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Returns the window duration in milliseconds.
    #[must_use]
    pub fn duration_ms(&self) -> i64 {
        self.end - self.start
    }

    /// Converts the window ID to a byte key for state storage.
    ///
    /// Uses `TimerKey` (`SmallVec`) which stores the 16-byte key inline,
    /// avoiding heap allocation on the hot path.
    #[inline]
    #[must_use]
    pub fn to_key(&self) -> super::TimerKey {
        super::TimerKey::from(self.to_key_inline())
    }

    /// Converts the window ID to a stack-allocated byte key.
    ///
    /// This is the zero-allocation version for Ring 0 hot path operations.
    /// Returns a fixed-size array that can be used directly with state stores.
    #[inline]
    #[must_use]
    pub fn to_key_inline(&self) -> [u8; 16] {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&self.start.to_be_bytes());
        key[8..16].copy_from_slice(&self.end.to_be_bytes());
        key
    }

    /// Parses a window ID from a byte key.
    ///
    /// # Errors
    ///
    /// Returns `None` if the key is not exactly 16 bytes.
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

/// Collection type for window assignments.
///
/// Uses `SmallVec` to avoid heap allocation for common cases:
/// - 1 window: tumbling windows (most common)
/// - 2-4 windows: sliding windows with small overlap
pub type WindowIdVec = SmallVec<[WindowId; 4]>;

// === Changelog/Z-Set Support ===

/// CDC operation type for changelog records.
///
/// These map to Z-set weights:
/// - `Insert`: +1 weight
/// - `Delete`: -1 weight
/// - `UpdateBefore`: -1 weight (first half of update)
/// - `UpdateAfter`: +1 weight (second half of update)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum CdcOperation {
    /// Insert a new record (+1 weight)
    Insert,
    /// Delete an existing record (-1 weight)
    Delete,
    /// Retraction of previous value before update (-1 weight)
    UpdateBefore,
    /// New value after update (+1 weight)
    UpdateAfter,
}

impl CdcOperation {
    /// Returns the Z-set weight for this operation.
    ///
    /// - Insert/UpdateAfter: +1
    /// - Delete/UpdateBefore: -1
    #[must_use]
    pub fn weight(&self) -> i32 {
        match self {
            Self::Insert | Self::UpdateAfter => 1,
            Self::Delete | Self::UpdateBefore => -1,
        }
    }

    /// Returns true if this is an insert-type operation.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self, Self::Insert | Self::UpdateAfter)
    }

    /// Returns true if this is a delete-type operation.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete | Self::UpdateBefore)
    }

    /// Returns the Debezium-compatible operation code.
    ///
    /// - 'c': create (insert)
    /// - 'd': delete
    /// - 'u': update (used for both before/after in Debezium)
    #[must_use]
    pub fn debezium_op(&self) -> char {
        match self {
            Self::Insert => 'c',
            Self::Delete => 'd',
            Self::UpdateBefore | Self::UpdateAfter => 'u',
        }
    }

    /// Converts the operation to a u8 for compact storage.
    ///
    /// Used by `ChangelogRef` to store operation type in a single byte.
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

    /// Converts from u8 (defaults to Insert for unknown values).
    #[inline]
    #[must_use]
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Delete,
            2 => Self::UpdateBefore,
            3 => Self::UpdateAfter,
            // 0 and unknown values default to Insert
            _ => Self::Insert,
        }
    }
}

/// A changelog record with Z-set weight for CDC pipelines.
///
/// This wraps an event with metadata needed for change data capture:
/// - Operation type (insert/delete/update)
/// - Z-set weight (+1/-1)
/// - Timestamp of the change
///
/// Used by `EmitStrategy::Changelog` to emit structured change records
/// that can be consumed by downstream systems expecting CDC format.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::{ChangelogRecord, CdcOperation};
/// use laminar_core::operator::Event;
/// # use std::sync::Arc;
/// # use arrow_array::RecordBatch;
/// # use arrow_schema::Schema;
/// # let schema = Arc::new(Schema::empty());
/// # let batch = RecordBatch::new_empty(schema);
/// # let event = Event::new(0, batch.clone());
/// # let old_event = event.clone();
/// # let new_event = event.clone();
///
/// // Create an insert record
/// let record = ChangelogRecord::insert(event, 1000);
/// assert_eq!(record.operation, CdcOperation::Insert);
/// assert_eq!(record.weight, 1);
///
/// // Create a retraction pair for an update
/// let (before, after) = ChangelogRecord::update(old_event, new_event, 1000);
/// assert_eq!(before.weight, -1);  // Retract old
/// assert_eq!(after.weight, 1);    // Insert new
/// ```
#[derive(Debug, Clone)]
pub struct ChangelogRecord {
    /// The CDC operation type
    pub operation: CdcOperation,
    /// Z-set weight (+1 for insert, -1 for delete)
    pub weight: i32,
    /// Timestamp when this change was emitted
    pub emit_timestamp: i64,
    /// The event data
    pub event: Event,
}

impl ChangelogRecord {
    /// Creates an insert changelog record.
    #[must_use]
    pub fn insert(event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation: CdcOperation::Insert,
            weight: 1,
            emit_timestamp,
            event,
        }
    }

    /// Creates a delete changelog record.
    #[must_use]
    pub fn delete(event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation: CdcOperation::Delete,
            weight: -1,
            emit_timestamp,
            event,
        }
    }

    /// Creates an update retraction pair (before and after records).
    ///
    /// Returns a tuple of (`UpdateBefore`, `UpdateAfter`) records.
    /// The first should be emitted before the second to properly
    /// retract the old value.
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

    /// Creates a changelog record from raw parts.
    #[must_use]
    pub fn new(operation: CdcOperation, event: Event, emit_timestamp: i64) -> Self {
        Self {
            operation,
            weight: operation.weight(),
            emit_timestamp,
            event,
        }
    }

    /// Returns true if this is an insert-type record.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        self.operation.is_insert()
    }

    /// Returns true if this is a delete-type record.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.operation.is_delete()
    }
}

/// Trait for assigning events to windows.
pub trait WindowAssigner: Send {
    /// Assigns an event timestamp to zero or more windows.
    ///
    /// For tumbling windows, this returns exactly one window.
    /// For sliding windows, this may return multiple windows.
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec;

    /// Returns the maximum timestamp that could still be assigned to a window
    /// ending at `window_end`.
    ///
    /// Used for determining when a window can be safely triggered.
    fn max_timestamp(&self, window_end: i64) -> i64 {
        window_end - 1
    }
}

/// Tumbling window assigner.
///
/// Assigns each event to exactly one non-overlapping window based on its timestamp.
/// Windows are aligned to epoch (timestamp 0).
#[derive(Debug, Clone)]
pub struct TumblingWindowAssigner {
    /// Window size in milliseconds
    size_ms: i64,
}

impl TumblingWindowAssigner {
    /// Creates a new tumbling window assigner.
    ///
    /// # Arguments
    ///
    /// * `size` - The duration of each window
    ///
    /// # Panics
    ///
    /// Panics if the size is zero.
    #[must_use]
    pub fn new(size: Duration) -> Self {
        // Ensure window size fits in i64 and is positive
        let size_ms = i64::try_from(size.as_millis()).expect("Window size must fit in i64");
        assert!(size_ms > 0, "Window size must be positive");
        Self { size_ms }
    }

    /// Creates a new tumbling window assigner with size in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the size is zero or negative.
    #[must_use]
    pub fn from_millis(size_ms: i64) -> Self {
        assert!(size_ms > 0, "Window size must be positive");
        Self { size_ms }
    }

    /// Returns the window size in milliseconds.
    #[must_use]
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }

    /// Assigns a timestamp to a window.
    ///
    /// This is the core window assignment function with O(1) complexity.
    #[inline]
    #[must_use]
    pub fn assign(&self, timestamp: i64) -> WindowId {
        // Handle negative timestamps correctly
        let window_start = if timestamp >= 0 {
            (timestamp / self.size_ms) * self.size_ms
        } else {
            // For negative timestamps, we need to floor divide
            ((timestamp - self.size_ms + 1) / self.size_ms) * self.size_ms
        };
        let window_end = window_start + self.size_ms;
        WindowId::new(window_start, window_end)
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

/// Trait for converting aggregation results to i64 for output.
///
/// This is needed to produce Arrow `RecordBatch` outputs with numeric results.
pub trait ResultToI64 {
    /// Converts the result to an i64 value.
    fn to_i64(&self) -> i64;
}

impl ResultToI64 for u64 {
    fn to_i64(&self) -> i64 {
        i64::try_from(*self).unwrap_or(i64::MAX)
    }
}

impl ResultToI64 for i64 {
    fn to_i64(&self) -> i64 {
        *self
    }
}

impl ResultToI64 for Option<i64> {
    fn to_i64(&self) -> i64 {
        self.unwrap_or(0)
    }
}

impl ResultToI64 for Option<f64> {
    fn to_i64(&self) -> i64 {
        // Standard SQL behavior: truncate float to int
        #[allow(clippy::cast_possible_truncation)]
        self.map(|f| f as i64).unwrap_or(0)
    }
}

/// Trait for converting aggregation results to Arrow arrays.
///
/// This preserves the native type (e.g., `Float64` for AVG) and nullable
/// semantics (empty windows produce null, not zero).
pub trait ResultToArrow {
    /// Converts the result to a single-element Arrow array.
    fn to_arrow_array(&self) -> Arc<dyn ArrowArray>;

    /// Returns the Arrow [`DataType`] for this result.
    fn arrow_data_type(&self) -> DataType;
}

impl ResultToArrow for i64 {
    fn to_arrow_array(&self) -> Arc<dyn ArrowArray> {
        Arc::new(Int64Array::from(vec![*self]))
    }

    fn arrow_data_type(&self) -> DataType {
        DataType::Int64
    }
}

impl ResultToArrow for u64 {
    fn to_arrow_array(&self) -> Arc<dyn ArrowArray> {
        Arc::new(Int64Array::from(vec![
            i64::try_from(*self).unwrap_or(i64::MAX)
        ]))
    }

    fn arrow_data_type(&self) -> DataType {
        DataType::Int64
    }
}

impl ResultToArrow for Option<i64> {
    fn to_arrow_array(&self) -> Arc<dyn ArrowArray> {
        Arc::new(Int64Array::from(vec![*self]))
    }

    fn arrow_data_type(&self) -> DataType {
        DataType::Int64
    }
}

impl ResultToArrow for Option<f64> {
    fn to_arrow_array(&self) -> Arc<dyn ArrowArray> {
        use arrow_array::Float64Array;
        Arc::new(Float64Array::from(vec![*self]))
    }

    fn arrow_data_type(&self) -> DataType {
        DataType::Float64
    }
}

/// Accumulator state for aggregations.
///
/// This is the state stored per window in the state store.
/// Different aggregators store different types of accumulators.
///
/// Implementors should derive `rkyv::Archive`, `rkyv::Serialize`, and
/// `rkyv::Deserialize` for zero-copy serialization on the hot path.
pub trait Accumulator: Default + Clone + Send {
    /// The input type for the aggregation.
    type Input;
    /// The output type produced by the aggregation.
    type Output: ResultToI64 + ResultToArrow;

    /// Adds a value to the accumulator.
    fn add(&mut self, value: Self::Input);

    /// Merges another accumulator into this one.
    fn merge(&mut self, other: &Self);

    /// Extracts the final result from the accumulator.
    fn result(&self) -> Self::Output;

    /// Returns true if the accumulator is empty (no values added).
    fn is_empty(&self) -> bool;
}

/// Trait for window aggregation functions.
///
/// Aggregators define how events are combined within a window.
/// They must be serializable for checkpointing.
pub trait Aggregator: Send + Clone {
    /// The accumulator type used by this aggregator.
    type Acc: Accumulator;

    /// Creates a new empty accumulator.
    fn create_accumulator(&self) -> Self::Acc;

    /// Extracts a value from an event to be aggregated.
    ///
    /// Returns `None` if the event should be skipped.
    fn extract(&self, event: &Event) -> Option<<Self::Acc as Accumulator>::Input>;

    /// Returns the Arrow [`DataType`] for this aggregator's output.
    ///
    /// Defaults to `Int64`. Override for aggregators that produce different types
    /// (e.g., `AvgAggregator` returns `Float64`).
    fn output_data_type(&self) -> DataType {
        DataType::Int64
    }

    /// Returns whether the output is nullable (e.g., empty windows produce null).
    ///
    /// Defaults to `false`. Override for aggregators like MIN/MAX/AVG.
    fn output_nullable(&self) -> bool {
        false
    }

    /// Extracts all values from a multi-row batch event.
    ///
    /// The default implementation calls [`extract()`](Self::extract) and wraps
    /// the single result. Override this in aggregators that need to process
    /// all rows in a batch (e.g., `AvgAggregator`).
    fn extract_batch(&self, event: &Event) -> SmallVec<[<Self::Acc as Accumulator>::Input; 4]> {
        let mut values = SmallVec::new();
        if let Some(v) = self.extract(event) {
            values.push(v);
        }
        values
    }
}

/// Count aggregator - counts the number of events in a window.
#[derive(Debug, Clone, Default)]
pub struct CountAggregator;

/// Accumulator for count aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct CountAccumulator {
    count: u64,
}

impl CountAggregator {
    /// Creates a new count aggregator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Accumulator for CountAccumulator {
    type Input = ();
    type Output = u64;

    fn add(&mut self, _value: ()) {
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }

    fn result(&self) -> u64 {
        self.count
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Aggregator for CountAggregator {
    type Acc = CountAccumulator;

    fn create_accumulator(&self) -> CountAccumulator {
        CountAccumulator::default()
    }

    fn extract(&self, _event: &Event) -> Option<()> {
        Some(())
    }
}

/// Sum aggregator - sums i64 values from events.
#[derive(Debug, Clone)]
pub struct SumAggregator {
    /// Column index to sum (0-based)
    column_index: usize,
}

/// Accumulator for sum aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SumAccumulator {
    sum: i64,
    count: u64,
}

impl SumAggregator {
    /// Creates a new sum aggregator for the specified column.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl Accumulator for SumAccumulator {
    type Input = i64;
    type Output = i64;

    fn add(&mut self, value: i64) {
        self.sum += value;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }

    fn result(&self) -> i64 {
        self.sum
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Aggregator for SumAggregator {
    type Acc = SumAccumulator;

    fn create_accumulator(&self) -> SumAccumulator {
        SumAccumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return None;
        }

        let column = batch.column(self.column_index);
        let array = column.as_primitive_opt::<Int64Type>()?;

        // Sum all values in the array
        Some(array.iter().flatten().sum())
    }
}

/// Min aggregator - tracks minimum i64 value.
#[derive(Debug, Clone)]
pub struct MinAggregator {
    column_index: usize,
}

/// Accumulator for min aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct MinAccumulator {
    min: Option<i64>,
}

impl MinAggregator {
    /// Creates a new min aggregator for the specified column.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl Accumulator for MinAccumulator {
    type Input = i64;
    type Output = Option<i64>;

    fn add(&mut self, value: i64) {
        self.min = Some(self.min.map_or(value, |m| m.min(value)));
    }

    fn merge(&mut self, other: &Self) {
        if let Some(other_min) = other.min {
            self.add(other_min);
        }
    }

    fn result(&self) -> Option<i64> {
        self.min
    }

    fn is_empty(&self) -> bool {
        self.min.is_none()
    }
}

impl Aggregator for MinAggregator {
    type Acc = MinAccumulator;

    fn create_accumulator(&self) -> MinAccumulator {
        MinAccumulator::default()
    }

    fn output_nullable(&self) -> bool {
        true
    }

    fn extract(&self, event: &Event) -> Option<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return None;
        }

        let column = batch.column(self.column_index);
        let array = column.as_primitive_opt::<Int64Type>()?;

        array.iter().flatten().min()
    }
}

/// Max aggregator - tracks maximum i64 value.
#[derive(Debug, Clone)]
pub struct MaxAggregator {
    column_index: usize,
}

/// Accumulator for max aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct MaxAccumulator {
    max: Option<i64>,
}

impl MaxAggregator {
    /// Creates a new max aggregator for the specified column.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl Accumulator for MaxAccumulator {
    type Input = i64;
    type Output = Option<i64>;

    fn add(&mut self, value: i64) {
        self.max = Some(self.max.map_or(value, |m| m.max(value)));
    }

    fn merge(&mut self, other: &Self) {
        if let Some(other_max) = other.max {
            self.add(other_max);
        }
    }

    fn result(&self) -> Option<i64> {
        self.max
    }

    fn is_empty(&self) -> bool {
        self.max.is_none()
    }
}

impl Aggregator for MaxAggregator {
    type Acc = MaxAccumulator;

    fn create_accumulator(&self) -> MaxAccumulator {
        MaxAccumulator::default()
    }

    fn output_nullable(&self) -> bool {
        true
    }

    fn extract(&self, event: &Event) -> Option<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return None;
        }

        let column = batch.column(self.column_index);
        let array = column.as_primitive_opt::<Int64Type>()?;

        array.iter().flatten().max()
    }
}

/// Average aggregator - computes average of i64 values.
#[derive(Debug, Clone)]
pub struct AvgAggregator {
    column_index: usize,
}

/// Accumulator for average aggregation.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct AvgAccumulator {
    sum: i64,
    count: u64,
}

impl AvgAggregator {
    /// Creates a new average aggregator for the specified column.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }
}

impl Accumulator for AvgAccumulator {
    type Input = i64;
    type Output = Option<f64>;

    fn add(&mut self, value: i64) {
        self.sum += value;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }

    // Precision loss is acceptable for arithmetic mean
    #[allow(clippy::cast_precision_loss)]
    fn result(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum as f64 / self.count as f64)
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Aggregator for AvgAggregator {
    type Acc = AvgAccumulator;

    fn create_accumulator(&self) -> AvgAccumulator {
        AvgAccumulator::default()
    }

    fn output_data_type(&self) -> DataType {
        DataType::Float64
    }

    fn output_nullable(&self) -> bool {
        true
    }

    fn extract(&self, event: &Event) -> Option<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return None;
        }

        let column = batch.column(self.column_index);
        let array = column.as_primitive_opt::<Int64Type>()?;

        // Return the first value for single-row batches
        array.iter().flatten().next()
    }

    fn extract_batch(&self, event: &Event) -> SmallVec<[i64; 4]> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return SmallVec::new();
        }

        let column = batch.column(self.column_index);
        let Some(array) = column.as_primitive_opt::<Int64Type>() else {
            return SmallVec::new();
        };

        // Return ALL values from the batch for correct multi-row averaging
        array.iter().flatten().collect()
    }
}

// FIRST_VALUE / LAST_VALUE Aggregators

/// `FIRST_VALUE` aggregator - returns the first value seen in a window.
///
/// Tracks the value with the earliest timestamp in the window.
/// For deterministic results, uses event timestamp, not arrival order.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::FirstValueAggregator;
///
/// // Track first price by timestamp
/// let first_price = FirstValueAggregator::new(0, 1); // price col 0, timestamp col 1
/// ```
#[derive(Debug, Clone)]
pub struct FirstValueAggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

/// Accumulator for `FIRST_VALUE` aggregation.
///
/// Stores the value with the earliest timestamp seen so far.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct FirstValueAccumulator {
    /// The first value seen (None if no values yet)
    value: Option<i64>,
    /// Timestamp of the first value (for merge ordering)
    timestamp: Option<i64>,
}

impl FirstValueAggregator {
    /// Creates a new `FIRST_VALUE` aggregator.
    ///
    /// # Arguments
    ///
    /// * `value_column_index` - Column to extract value from
    /// * `timestamp_column_index` - Column for event timestamp ordering
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
        }
    }
}

impl Accumulator for FirstValueAccumulator {
    type Input = (i64, i64); // (value, timestamp)
    type Output = Option<i64>;

    fn add(&mut self, (value, timestamp): (i64, i64)) {
        match self.timestamp {
            None => {
                // First value
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp < existing_ts => {
                // Earlier timestamp - replace
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            _ => {
                // Later or equal timestamp - keep existing
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts < self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            _ => {
                // Keep self
            }
        }
    }

    fn result(&self) -> Option<i64> {
        self.value
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}

impl Aggregator for FirstValueAggregator {
    type Acc = FirstValueAccumulator;

    fn create_accumulator(&self) -> FirstValueAccumulator {
        FirstValueAccumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<(i64, i64)> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return None;
        }

        // Extract value
        let value_col = batch.column(self.value_column_index);
        let value_array = value_col.as_primitive_opt::<Int64Type>()?;
        let value = value_array.iter().flatten().next()?;

        // Extract timestamp
        let ts_col = batch.column(self.timestamp_column_index);
        let ts_array = ts_col.as_primitive_opt::<Int64Type>()?;
        let timestamp = ts_array.iter().flatten().next()?;

        Some((value, timestamp))
    }
}

/// `LAST_VALUE` aggregator - returns the last value seen in a window.
///
/// Tracks the value with the latest timestamp in the window.
/// For deterministic results, uses event timestamp, not arrival order.
/// When timestamps are equal, the later arrival wins.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::LastValueAggregator;
///
/// // Track last (closing) price by timestamp
/// let last_price = LastValueAggregator::new(0, 1); // price col 0, timestamp col 1
/// ```
#[derive(Debug, Clone)]
pub struct LastValueAggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

/// Accumulator for `LAST_VALUE` aggregation.
///
/// Stores the value with the latest timestamp seen so far.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct LastValueAccumulator {
    /// The last value seen (None if no values yet)
    value: Option<i64>,
    /// Timestamp of the last value (for merge ordering)
    timestamp: Option<i64>,
}

impl LastValueAggregator {
    /// Creates a new `LAST_VALUE` aggregator.
    ///
    /// # Arguments
    ///
    /// * `value_column_index` - Column to extract value from
    /// * `timestamp_column_index` - Column for event timestamp ordering
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
        }
    }
}

impl Accumulator for LastValueAccumulator {
    type Input = (i64, i64); // (value, timestamp)
    type Output = Option<i64>;

    fn add(&mut self, (value, timestamp): (i64, i64)) {
        match self.timestamp {
            None => {
                // First value
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp > existing_ts => {
                // Later timestamp - replace
                self.value = Some(value);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp == existing_ts => {
                // Same timestamp - keep latest arrival (replace)
                self.value = Some(value);
            }
            _ => {
                // Earlier timestamp - keep existing
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts > self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts == self_ts => {
                // Same timestamp - use other (simulate later arrival)
                self.value = other.value;
            }
            _ => {
                // Keep self
            }
        }
    }

    fn result(&self) -> Option<i64> {
        self.value
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}

impl Aggregator for LastValueAggregator {
    type Acc = LastValueAccumulator;

    fn create_accumulator(&self) -> LastValueAccumulator {
        LastValueAccumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<(i64, i64)> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return None;
        }

        // Extract value
        let value_col = batch.column(self.value_column_index);
        let value_array = value_col.as_primitive_opt::<Int64Type>()?;
        let value = value_array.iter().flatten().next()?;

        // Extract timestamp
        let ts_col = batch.column(self.timestamp_column_index);
        let ts_array = ts_col.as_primitive_opt::<Int64Type>()?;
        let timestamp = ts_array.iter().flatten().next()?;

        Some((value, timestamp))
    }
}

// FIRST_VALUE / LAST_VALUE for Float64

/// Accumulator for `FIRST_VALUE` aggregation on f64 values.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct FirstValueF64Accumulator {
    /// The first value seen (None if no values yet)
    value: Option<i64>, // Store as bits for rkyv compatibility
    /// Timestamp of the first value (for merge ordering)
    timestamp: Option<i64>,
}

impl FirstValueF64Accumulator {
    /// Gets the result as f64.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn result_f64(&self) -> Option<f64> {
        self.value.map(|bits| f64::from_bits(bits as u64))
    }
}

impl Accumulator for FirstValueF64Accumulator {
    type Input = (f64, i64); // (value, timestamp)
    type Output = Option<f64>;

    fn add(&mut self, (value, timestamp): (f64, i64)) {
        // SAFETY: We strictly use this as storage bits and convert back via from_bits
        #[allow(clippy::cast_possible_wrap)]
        let value_bits = value.to_bits() as i64;
        match self.timestamp {
            None => {
                self.value = Some(value_bits);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp < existing_ts => {
                self.value = Some(value_bits);
                self.timestamp = Some(timestamp);
            }
            _ => {}
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts < self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            _ => {}
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn result(&self) -> Option<f64> {
        self.value.map(|bits| f64::from_bits(bits as u64))
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}

/// `FIRST_VALUE` aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct FirstValueF64Aggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

impl FirstValueF64Aggregator {
    /// Creates a new `FIRST_VALUE` aggregator for f64 columns.
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
        }
    }
}

impl Aggregator for FirstValueF64Aggregator {
    type Acc = FirstValueF64Accumulator;

    fn create_accumulator(&self) -> FirstValueF64Accumulator {
        FirstValueF64Accumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<(f64, i64)> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return None;
        }

        // Extract value as f64
        let value_col = batch.column(self.value_column_index);
        let value_array = value_col.as_primitive_opt::<Float64Type>()?;
        let value = value_array.iter().flatten().next()?;

        // Extract timestamp
        let ts_col = batch.column(self.timestamp_column_index);
        let ts_array = ts_col.as_primitive_opt::<Int64Type>()?;
        let timestamp = ts_array.iter().flatten().next()?;

        Some((value, timestamp))
    }
}

/// Accumulator for `LAST_VALUE` aggregation on f64 values.
#[derive(Debug, Clone, Default, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct LastValueF64Accumulator {
    /// The last value seen (None if no values yet)
    value: Option<i64>, // Store as bits for rkyv compatibility
    /// Timestamp of the last value (for merge ordering)
    timestamp: Option<i64>,
}

impl LastValueF64Accumulator {
    /// Gets the result as f64.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn result_f64(&self) -> Option<f64> {
        self.value.map(|bits| f64::from_bits(bits as u64))
    }
}

impl Accumulator for LastValueF64Accumulator {
    type Input = (f64, i64); // (value, timestamp)
    type Output = Option<f64>;

    fn add(&mut self, (value, timestamp): (f64, i64)) {
        // SAFETY: We strictly use this as storage bits and convert back via from_bits
        #[allow(clippy::cast_possible_wrap)]
        let value_bits = value.to_bits() as i64;
        match self.timestamp {
            None => {
                self.value = Some(value_bits);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp > existing_ts => {
                self.value = Some(value_bits);
                self.timestamp = Some(timestamp);
            }
            Some(existing_ts) if timestamp == existing_ts => {
                self.value = Some(value_bits);
            }
            _ => {}
        }
    }

    fn merge(&mut self, other: &Self) {
        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts > self_ts => {
                self.value = other.value;
                self.timestamp = other.timestamp;
            }
            (Some(self_ts), Some(other_ts)) if other_ts == self_ts => {
                self.value = other.value;
            }
            _ => {}
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn result(&self) -> Option<f64> {
        self.value.map(|bits| f64::from_bits(bits as u64))
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }
}

/// `LAST_VALUE` aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct LastValueF64Aggregator {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp (for ordering)
    timestamp_column_index: usize,
}

impl LastValueF64Aggregator {
    /// Creates a new `LAST_VALUE` aggregator for f64 columns.
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
        }
    }
}

impl Aggregator for LastValueF64Aggregator {
    type Acc = LastValueF64Accumulator;

    fn create_accumulator(&self) -> LastValueF64Accumulator {
        LastValueF64Accumulator::default()
    }

    fn extract(&self, event: &Event) -> Option<(f64, i64)> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return None;
        }

        // Extract value as f64
        let value_col = batch.column(self.value_column_index);
        let value_array = value_col.as_primitive_opt::<Float64Type>()?;
        let value = value_array.iter().flatten().next()?;

        // Extract timestamp
        let ts_col = batch.column(self.timestamp_column_index);
        let ts_array = ts_col.as_primitive_opt::<Int64Type>()?;
        let timestamp = ts_array.iter().flatten().next()?;

        Some((value, timestamp))
    }
}

// Composite Aggregator & f64 Type Support

/// Scalar result type supporting multiple numeric types.
///
/// Used by [`DynAccumulator`] for dynamic-dispatch aggregation where
/// the result type is determined at runtime. This enables composite
/// aggregation (multiple aggregates per window) with mixed types.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::ScalarResult;
///
/// let r = ScalarResult::Float64(3.14);
/// assert_eq!(r.to_i64_lossy(), 3);
/// assert_eq!(r.to_f64_lossy(), 3.14);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarResult {
    /// 64-bit signed integer
    Int64(i64),
    /// 64-bit floating point
    Float64(f64),
    /// 64-bit unsigned integer
    UInt64(u64),
    /// Optional 64-bit signed integer
    OptionalInt64(Option<i64>),
    /// Optional 64-bit floating point
    OptionalFloat64(Option<f64>),
    /// Null / no value
    Null,
}

impl ScalarResult {
    /// Converts to i64, truncating floats and saturating unsigned values.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    pub fn to_i64_lossy(&self) -> i64 {
        match self {
            Self::Int64(v) => *v,
            Self::Float64(v) => *v as i64,
            Self::UInt64(v) => i64::try_from(*v).unwrap_or(i64::MAX),
            Self::OptionalInt64(v) => v.unwrap_or(0),
            Self::OptionalFloat64(v) => v.map(|f| f as i64).unwrap_or(0),
            Self::Null => 0,
        }
    }

    /// Converts to f64, with potential precision loss for large integers.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_f64_lossy(&self) -> f64 {
        match self {
            Self::Int64(v) => *v as f64,
            Self::Float64(v) => *v,
            Self::UInt64(v) => *v as f64,
            Self::OptionalInt64(v) => v.map(|i| i as f64).unwrap_or(0.0),
            Self::OptionalFloat64(v) => v.unwrap_or(0.0),
            Self::Null => 0.0,
        }
    }

    /// Returns true if this is a null or None value.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            Self::Null | Self::OptionalInt64(None) | Self::OptionalFloat64(None)
        )
    }

    /// Returns the Arrow [`DataType`] for this result.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int64(_) | Self::OptionalInt64(_) => DataType::Int64,
            Self::Float64(_) | Self::OptionalFloat64(_) => DataType::Float64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Null => DataType::Null,
        }
    }
}

/// Dynamic accumulator trait for composite aggregation.
///
/// Unlike the static [`Accumulator`] trait, this works with events directly
/// and returns [`ScalarResult`] for type-flexible output. Used by
/// [`CompositeAggregator`] to combine multiple aggregates per window.
///
/// # Ring Architecture
///
/// Dynamic dispatch has overhead (~2-5ns per vtable call), so composite
/// aggregation is intended for Ring 1 workloads. Ring 0 continues to use
/// the static [`Aggregator`] + [`Accumulator`] path.
pub trait DynAccumulator: Send {
    /// Adds an event to the accumulator.
    fn add_event(&mut self, event: &Event);

    /// Merges another accumulator of the same type into this one.
    ///
    /// # Panics
    ///
    /// May panic if `other` is not the same concrete type.
    fn merge_dyn(&mut self, other: &dyn DynAccumulator);

    /// Returns the current aggregate result.
    fn result_scalar(&self) -> ScalarResult;

    /// Returns true if no values have been accumulated.
    fn is_empty(&self) -> bool;

    /// Creates a boxed clone of this accumulator.
    fn clone_box(&self) -> Box<dyn DynAccumulator>;

    /// Serializes the accumulator state to bytes (for checkpointing).
    fn serialize(&self) -> Vec<u8>;

    /// Returns the Arrow field descriptor for this accumulator's output.
    fn result_field(&self) -> Field;

    /// Returns a type tag for deserialization dispatch.
    fn type_tag(&self) -> &'static str;

    /// Returns self as `Any` for downcasting (used by `DataFusion` bridge).
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Factory trait for creating [`DynAccumulator`] instances.
///
/// Each factory corresponds to one aggregate function (e.g., SUM, COUNT).
/// The [`CompositeAggregator`] holds multiple factories.
pub trait DynAggregatorFactory: Send + Sync {
    /// Creates a new empty accumulator.
    fn create_accumulator(&self) -> Box<dyn DynAccumulator>;

    /// Returns the Arrow field descriptor for results.
    fn result_field(&self) -> Field;

    /// Creates a boxed clone of this factory.
    fn clone_box(&self) -> Box<dyn DynAggregatorFactory>;

    /// Returns a type tag for deserialization dispatch.
    fn type_tag(&self) -> &'static str;
}

// ── f64 Aggregators ─────────────────────────────────────────────────────────

/// Sum aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct SumF64Aggregator {
    /// Column index to sum
    column_index: usize,
}

/// Accumulator for f64 sum aggregation.
#[derive(Debug, Clone, Default)]
pub struct SumF64Accumulator {
    /// Running sum
    sum: f64,
    /// Count of values for `is_empty` check
    count: u64,
}

impl SumF64Aggregator {
    /// Creates a new f64 sum aggregator for the specified column.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self { column_index }
    }

    /// Returns the column index.
    #[must_use]
    pub fn column_index(&self) -> usize {
        self.column_index
    }
}

impl SumF64Accumulator {
    /// Returns the current sum.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum
    }
}

impl DynAccumulator for SumF64Accumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;

        // Extract from first column by default (factory sets column_index)
        // Note: column_index is embedded in the accumulator at construction
        let batch = &event.data;
        if batch.num_columns() == 0 {
            return;
        }
        // Try first column as f64
        if let Some(array) = batch.column(0).as_primitive_opt::<Float64Type>() {
            for val in array.iter().flatten() {
                self.sum += val;
                self.count += 1;
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        // Downcast via serialize/deserialize for safety
        let data = other.serialize();
        if data.len() == 16 {
            let sum = f64::from_le_bytes(data[..8].try_into().unwrap());
            let count = u64::from_le_bytes(data[8..16].try_into().unwrap());
            self.sum += sum;
            self.count += count;
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        if self.count == 0 {
            ScalarResult::Null
        } else {
            ScalarResult::Float64(self.sum)
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.sum.to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf
    }

    fn result_field(&self) -> Field {
        Field::new("sum_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "sum_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Factory for [`SumF64Accumulator`].
#[derive(Debug, Clone)]
pub struct SumF64Factory {
    /// Column index to sum
    column_index: usize,
    /// Output field name
    field_name: String,
}

impl SumF64Factory {
    /// Creates a new f64 sum factory.
    #[must_use]
    pub fn new(column_index: usize, field_name: impl Into<String>) -> Self {
        Self {
            column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for SumF64Factory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(SumF64IndexedAccumulator::new(self.column_index))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "sum_f64"
    }
}

/// f64 sum accumulator with embedded column index.
#[derive(Debug, Clone)]
pub struct SumF64IndexedAccumulator {
    /// Column index to extract from
    column_index: usize,
    /// Running sum
    sum: f64,
    /// Count of values
    count: u64,
}

impl SumF64IndexedAccumulator {
    /// Creates a new indexed sum accumulator.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self {
            column_index,
            sum: 0.0,
            count: 0,
        }
    }
}

impl DynAccumulator for SumF64IndexedAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return;
        }
        if let Some(array) = batch
            .column(self.column_index)
            .as_primitive_opt::<Float64Type>()
        {
            for val in array.iter().flatten() {
                self.sum += val;
                self.count += 1;
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 16 {
            let sum = f64::from_le_bytes(data[..8].try_into().unwrap());
            let count = u64::from_le_bytes(data[8..16].try_into().unwrap());
            self.sum += sum;
            self.count += count;
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        if self.count == 0 {
            ScalarResult::Null
        } else {
            ScalarResult::Float64(self.sum)
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.sum.to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf
    }

    fn result_field(&self) -> Field {
        Field::new("sum_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "sum_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Min aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct MinF64Factory {
    /// Column index
    column_index: usize,
    /// Output field name
    field_name: String,
}

impl MinF64Factory {
    /// Creates a new f64 min factory.
    #[must_use]
    pub fn new(column_index: usize, field_name: impl Into<String>) -> Self {
        Self {
            column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for MinF64Factory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(MinF64IndexedAccumulator::new(self.column_index))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "min_f64"
    }
}

/// f64 min accumulator with embedded column index.
#[derive(Debug, Clone)]
pub struct MinF64IndexedAccumulator {
    /// Column index
    column_index: usize,
    /// Current minimum
    min: Option<f64>,
}

impl MinF64IndexedAccumulator {
    /// Creates a new indexed min accumulator.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self {
            column_index,
            min: None,
        }
    }
}

impl DynAccumulator for MinF64IndexedAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return;
        }
        if let Some(array) = batch
            .column(self.column_index)
            .as_primitive_opt::<Float64Type>()
        {
            for val in array.iter().flatten() {
                self.min = Some(self.min.map_or(val, |m: f64| m.min(val)));
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 9 && data[0] == 1 {
            let other_min = f64::from_le_bytes(data[1..9].try_into().unwrap());
            self.min = Some(self.min.map_or(other_min, |m: f64| m.min(other_min)));
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        ScalarResult::OptionalFloat64(self.min)
    }

    fn is_empty(&self) -> bool {
        self.min.is_none()
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        match self.min {
            Some(v) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(1); // has value marker
                buf.extend_from_slice(&v.to_le_bytes());
                buf
            }
            None => vec![0],
        }
    }

    fn result_field(&self) -> Field {
        Field::new("min_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "min_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Max aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct MaxF64Factory {
    /// Column index
    column_index: usize,
    /// Output field name
    field_name: String,
}

impl MaxF64Factory {
    /// Creates a new f64 max factory.
    #[must_use]
    pub fn new(column_index: usize, field_name: impl Into<String>) -> Self {
        Self {
            column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for MaxF64Factory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(MaxF64IndexedAccumulator::new(self.column_index))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "max_f64"
    }
}

/// f64 max accumulator with embedded column index.
#[derive(Debug, Clone)]
pub struct MaxF64IndexedAccumulator {
    /// Column index
    column_index: usize,
    /// Current maximum
    max: Option<f64>,
}

impl MaxF64IndexedAccumulator {
    /// Creates a new indexed max accumulator.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self {
            column_index,
            max: None,
        }
    }
}

impl DynAccumulator for MaxF64IndexedAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return;
        }
        if let Some(array) = batch
            .column(self.column_index)
            .as_primitive_opt::<Float64Type>()
        {
            for val in array.iter().flatten() {
                self.max = Some(self.max.map_or(val, |m: f64| m.max(val)));
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 9 && data[0] == 1 {
            let other_max = f64::from_le_bytes(data[1..9].try_into().unwrap());
            self.max = Some(self.max.map_or(other_max, |m: f64| m.max(other_max)));
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        ScalarResult::OptionalFloat64(self.max)
    }

    fn is_empty(&self) -> bool {
        self.max.is_none()
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        match self.max {
            Some(v) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
                buf
            }
            None => vec![0],
        }
    }

    fn result_field(&self) -> Field {
        Field::new("max_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "max_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Avg aggregator for f64 columns.
#[derive(Debug, Clone)]
pub struct AvgF64Factory {
    /// Column index
    column_index: usize,
    /// Output field name
    field_name: String,
}

impl AvgF64Factory {
    /// Creates a new f64 avg factory.
    #[must_use]
    pub fn new(column_index: usize, field_name: impl Into<String>) -> Self {
        Self {
            column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for AvgF64Factory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(AvgF64IndexedAccumulator::new(self.column_index))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "avg_f64"
    }
}

/// f64 avg accumulator with embedded column index.
#[derive(Debug, Clone)]
pub struct AvgF64IndexedAccumulator {
    /// Column index
    column_index: usize,
    /// Running sum
    sum: f64,
    /// Count
    count: u64,
}

impl AvgF64IndexedAccumulator {
    /// Creates a new indexed avg accumulator.
    #[must_use]
    pub fn new(column_index: usize) -> Self {
        Self {
            column_index,
            sum: 0.0,
            count: 0,
        }
    }
}

impl DynAccumulator for AvgF64IndexedAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Float64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return;
        }
        if let Some(array) = batch
            .column(self.column_index)
            .as_primitive_opt::<Float64Type>()
        {
            for val in array.iter().flatten() {
                self.sum += val;
                self.count += 1;
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 16 {
            let sum = f64::from_le_bytes(data[..8].try_into().unwrap());
            let count = u64::from_le_bytes(data[8..16].try_into().unwrap());
            self.sum += sum;
            self.count += count;
        }
    }

    // Precision loss is acceptable for arithmetic mean
    #[allow(clippy::cast_precision_loss)]
    fn result_scalar(&self) -> ScalarResult {
        if self.count == 0 {
            ScalarResult::Null
        } else {
            ScalarResult::Float64(self.sum / self.count as f64)
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.sum.to_le_bytes());
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf
    }

    fn result_field(&self) -> Field {
        Field::new("avg_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "avg_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ── Count DynAccumulator ────────────────────────────────────────────────────

/// Count factory for [`DynAccumulator`].
#[derive(Debug, Clone)]
pub struct CountDynFactory {
    /// Output field name
    field_name: String,
}

impl CountDynFactory {
    /// Creates a new count factory.
    #[must_use]
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for CountDynFactory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(CountDynAccumulator::default())
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Int64, false)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "count"
    }
}

/// Count accumulator implementing [`DynAccumulator`].
#[derive(Debug, Clone, Default)]
pub struct CountDynAccumulator {
    count: u64,
}

impl DynAccumulator for CountDynAccumulator {
    fn add_event(&mut self, event: &Event) {
        let rows = event.data.num_rows();
        self.count += rows as u64;
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 8 {
            let count = u64::from_le_bytes(data[..8].try_into().unwrap());
            self.count += count;
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        ScalarResult::Int64(i64::try_from(self.count).unwrap_or(i64::MAX))
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        self.count.to_le_bytes().to_vec()
    }

    fn result_field(&self) -> Field {
        Field::new("count", DataType::Int64, false)
    }

    fn type_tag(&self) -> &'static str {
        "count"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ── FirstValue / LastValue DynAccumulator ───────────────────────────────────

/// `FIRST_VALUE` factory for f64 columns via [`DynAccumulator`].
#[derive(Debug, Clone)]
pub struct FirstValueF64DynFactory {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp
    timestamp_column_index: usize,
    /// Output field name
    field_name: String,
}

impl FirstValueF64DynFactory {
    /// Creates a new `FIRST_VALUE` factory for f64 columns.
    #[must_use]
    pub fn new(
        value_column_index: usize,
        timestamp_column_index: usize,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for FirstValueF64DynFactory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(FirstValueF64DynAccumulator::new(
            self.value_column_index,
            self.timestamp_column_index,
        ))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "first_value_f64"
    }
}

/// `FIRST_VALUE` accumulator for f64 columns via [`DynAccumulator`].
#[derive(Debug, Clone)]
pub struct FirstValueF64DynAccumulator {
    value_column_index: usize,
    timestamp_column_index: usize,
    value: Option<f64>,
    timestamp: Option<i64>,
}

impl FirstValueF64DynAccumulator {
    /// Creates a new `FIRST_VALUE` dyn accumulator.
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
            value: None,
            timestamp: None,
        }
    }
}

impl DynAccumulator for FirstValueF64DynAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return;
        }

        let val_col = batch.column(self.value_column_index);
        let ts_col = batch.column(self.timestamp_column_index);

        let Some(val_array) = val_col.as_primitive_opt::<Float64Type>() else {
            return;
        };
        let Some(ts_array) = ts_col.as_primitive_opt::<Int64Type>() else {
            return;
        };

        for i in 0..batch.num_rows() {
            if val_array.is_null(i) || ts_array.is_null(i) {
                continue;
            }
            let val = val_array.value(i);
            let ts = ts_array.value(i);

            match self.timestamp {
                None => {
                    self.value = Some(val);
                    self.timestamp = Some(ts);
                }
                Some(existing_ts) if ts < existing_ts => {
                    self.value = Some(val);
                    self.timestamp = Some(ts);
                }
                _ => {}
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 17 && data[0] == 1 {
            let other_val = f64::from_le_bytes(data[1..9].try_into().unwrap());
            let other_ts = i64::from_le_bytes(data[9..17].try_into().unwrap());
            match self.timestamp {
                None => {
                    self.value = Some(other_val);
                    self.timestamp = Some(other_ts);
                }
                Some(self_ts) if other_ts < self_ts => {
                    self.value = Some(other_val);
                    self.timestamp = Some(other_ts);
                }
                _ => {}
            }
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        ScalarResult::OptionalFloat64(self.value)
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        match (self.value, self.timestamp) {
            (Some(v), Some(ts)) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
                buf.extend_from_slice(&ts.to_le_bytes());
                buf
            }
            _ => vec![0],
        }
    }

    fn result_field(&self) -> Field {
        Field::new("first_value_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "first_value_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// `LAST_VALUE` factory for f64 columns via [`DynAccumulator`].
#[derive(Debug, Clone)]
pub struct LastValueF64DynFactory {
    /// Column index to extract value from
    value_column_index: usize,
    /// Column index for event timestamp
    timestamp_column_index: usize,
    /// Output field name
    field_name: String,
}

impl LastValueF64DynFactory {
    /// Creates a new `LAST_VALUE` factory for f64 columns.
    #[must_use]
    pub fn new(
        value_column_index: usize,
        timestamp_column_index: usize,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
            field_name: field_name.into(),
        }
    }
}

impl DynAggregatorFactory for LastValueF64DynFactory {
    fn create_accumulator(&self) -> Box<dyn DynAccumulator> {
        Box::new(LastValueF64DynAccumulator::new(
            self.value_column_index,
            self.timestamp_column_index,
        ))
    }

    fn result_field(&self) -> Field {
        Field::new(&self.field_name, DataType::Float64, true)
    }

    fn clone_box(&self) -> Box<dyn DynAggregatorFactory> {
        Box::new(self.clone())
    }

    fn type_tag(&self) -> &'static str {
        "last_value_f64"
    }
}

/// `LAST_VALUE` accumulator for f64 columns via [`DynAccumulator`].
#[derive(Debug, Clone)]
pub struct LastValueF64DynAccumulator {
    value_column_index: usize,
    timestamp_column_index: usize,
    value: Option<f64>,
    timestamp: Option<i64>,
}

impl LastValueF64DynAccumulator {
    /// Creates a new `LAST_VALUE` dyn accumulator.
    #[must_use]
    pub fn new(value_column_index: usize, timestamp_column_index: usize) -> Self {
        Self {
            value_column_index,
            timestamp_column_index,
            value: None,
            timestamp: None,
        }
    }
}

impl DynAccumulator for LastValueF64DynAccumulator {
    fn add_event(&mut self, event: &Event) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};

        let batch = &event.data;
        if self.value_column_index >= batch.num_columns()
            || self.timestamp_column_index >= batch.num_columns()
        {
            return;
        }

        let val_col = batch.column(self.value_column_index);
        let ts_col = batch.column(self.timestamp_column_index);

        let Some(val_array) = val_col.as_primitive_opt::<Float64Type>() else {
            return;
        };
        let Some(ts_array) = ts_col.as_primitive_opt::<Int64Type>() else {
            return;
        };

        for i in 0..batch.num_rows() {
            if val_array.is_null(i) || ts_array.is_null(i) {
                continue;
            }
            let val = val_array.value(i);
            let ts = ts_array.value(i);

            match self.timestamp {
                None => {
                    self.value = Some(val);
                    self.timestamp = Some(ts);
                }
                Some(existing_ts) if ts >= existing_ts => {
                    self.value = Some(val);
                    self.timestamp = Some(ts);
                }
                _ => {}
            }
        }
    }

    fn merge_dyn(&mut self, other: &dyn DynAccumulator) {
        let data = other.serialize();
        if data.len() >= 17 && data[0] == 1 {
            let other_val = f64::from_le_bytes(data[1..9].try_into().unwrap());
            let other_ts = i64::from_le_bytes(data[9..17].try_into().unwrap());
            match self.timestamp {
                None => {
                    self.value = Some(other_val);
                    self.timestamp = Some(other_ts);
                }
                Some(self_ts) if other_ts >= self_ts => {
                    self.value = Some(other_val);
                    self.timestamp = Some(other_ts);
                }
                _ => {}
            }
        }
    }

    fn result_scalar(&self) -> ScalarResult {
        ScalarResult::OptionalFloat64(self.value)
    }

    fn is_empty(&self) -> bool {
        self.value.is_none()
    }

    fn clone_box(&self) -> Box<dyn DynAccumulator> {
        Box::new(self.clone())
    }

    fn serialize(&self) -> Vec<u8> {
        match (self.value, self.timestamp) {
            (Some(v), Some(ts)) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
                buf.extend_from_slice(&ts.to_le_bytes());
                buf
            }
            _ => vec![0],
        }
    }

    fn result_field(&self) -> Field {
        Field::new("last_value_f64", DataType::Float64, true)
    }

    fn type_tag(&self) -> &'static str {
        "last_value_f64"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ── Composite Aggregator ────────────────────────────────────────────────────

/// Composite aggregator combining multiple [`DynAggregatorFactory`] instances.
///
/// Produces multi-column output: `window_start, window_end, field_0, field_1, ...`
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::window::{
///     CompositeAggregator, CountDynFactory, MaxF64Factory, MinF64Factory,
/// };
///
/// let agg = CompositeAggregator::new(vec![
///     Box::new(CountDynFactory::new("trade_count")),
///     Box::new(MinF64Factory::new(1, "low")),
///     Box::new(MaxF64Factory::new(1, "high")),
/// ]);
/// assert_eq!(agg.num_aggregates(), 3);
/// ```
pub struct CompositeAggregator {
    /// Factories for creating sub-accumulators
    factories: Vec<Box<dyn DynAggregatorFactory>>,
    /// Cached output schema (built once in constructor)
    cached_schema: SchemaRef,
}

impl std::fmt::Debug for CompositeAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeAggregator")
            .field("num_factories", &self.factories.len())
            .finish_non_exhaustive()
    }
}

impl CompositeAggregator {
    /// Creates a new composite aggregator from a list of factories.
    #[must_use]
    pub fn new(factories: Vec<Box<dyn DynAggregatorFactory>>) -> Self {
        let mut fields = vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ];
        fields.extend(factories.iter().map(|f| f.result_field()));
        let cached_schema = Arc::new(Schema::new(fields));
        Self {
            factories,
            cached_schema,
        }
    }

    /// Returns the number of sub-aggregates.
    #[must_use]
    pub fn num_aggregates(&self) -> usize {
        self.factories.len()
    }

    /// Creates a new composite accumulator with all sub-accumulators.
    #[must_use]
    pub fn create_accumulator(&self) -> CompositeAccumulator {
        let accumulators = self
            .factories
            .iter()
            .map(|f| f.create_accumulator())
            .collect();
        CompositeAccumulator { accumulators }
    }

    /// Returns the result fields for all sub-aggregates.
    #[must_use]
    pub fn result_fields(&self) -> Vec<Field> {
        self.factories.iter().map(|f| f.result_field()).collect()
    }

    /// Creates the output schema: `window_start, window_end, [aggregate fields]`.
    #[must_use]
    pub fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.cached_schema)
    }
}

impl Clone for CompositeAggregator {
    fn clone(&self) -> Self {
        let factories: Vec<Box<dyn DynAggregatorFactory>> =
            self.factories.iter().map(|f| f.clone_box()).collect();
        Self {
            cached_schema: Arc::clone(&self.cached_schema),
            factories,
        }
    }
}

/// Composite accumulator holding multiple [`DynAccumulator`] instances.
///
/// Fans out each event to all sub-accumulators and collects results
/// as a multi-column [`RecordBatch`].
pub struct CompositeAccumulator {
    /// Sub-accumulators (one per aggregate function)
    accumulators: Vec<Box<dyn DynAccumulator>>,
}

impl std::fmt::Debug for CompositeAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeAccumulator")
            .field("num_accumulators", &self.accumulators.len())
            .finish()
    }
}

impl CompositeAccumulator {
    /// Adds an event to all sub-accumulators.
    pub fn add_event(&mut self, event: &Event) {
        for acc in &mut self.accumulators {
            acc.add_event(event);
        }
    }

    /// Merges another composite accumulator into this one.
    ///
    /// # Panics
    ///
    /// Panics if the other accumulator has a different number of sub-accumulators.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.accumulators.len(),
            other.accumulators.len(),
            "Cannot merge composite accumulators with different sizes"
        );
        for (self_acc, other_acc) in self.accumulators.iter_mut().zip(&other.accumulators) {
            self_acc.merge_dyn(other_acc.as_ref());
        }
    }

    /// Returns all results as [`ScalarResult`] values.
    #[must_use]
    pub fn results(&self) -> Vec<ScalarResult> {
        self.accumulators
            .iter()
            .map(|a| a.result_scalar())
            .collect()
    }

    /// Returns true if all sub-accumulators are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.accumulators.iter().all(|a| a.is_empty())
    }

    /// Serializes all sub-accumulators for checkpointing.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Wire format uses fixed-width integers
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Header: number of accumulators (u32)
        let n = self.accumulators.len() as u32;
        buf.extend_from_slice(&n.to_le_bytes());
        for acc in &self.accumulators {
            let tag = acc.type_tag();
            let tag_bytes = tag.as_bytes();
            // Tag length (u16) + tag + data length (u32) + data
            buf.extend_from_slice(&(tag_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(tag_bytes);
            let data = acc.serialize();
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&data);
        }
        buf
    }

    /// Creates a multi-column [`RecordBatch`] from the results.
    ///
    /// The batch has columns: `window_start, window_end, [aggregate results]`.
    ///
    /// # Errors
    ///
    /// Returns `None` if the batch cannot be created.
    #[must_use]
    pub fn to_record_batch(&self, window_id: &WindowId, schema: &SchemaRef) -> Option<RecordBatch> {
        use arrow_array::{Float64Array, UInt64Array};

        let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
            Arc::new(Int64Array::from(vec![window_id.start])),
            Arc::new(Int64Array::from(vec![window_id.end])),
        ];

        for result in self.results() {
            let col: Arc<dyn arrow_array::Array> = match result {
                ScalarResult::Int64(v) => Arc::new(Int64Array::from(vec![v])),
                ScalarResult::Float64(v) => Arc::new(Float64Array::from(vec![v])),
                ScalarResult::UInt64(v) => Arc::new(UInt64Array::from(vec![v])),
                ScalarResult::OptionalInt64(v) => Arc::new(Int64Array::from(vec![v])),
                ScalarResult::OptionalFloat64(v) => Arc::new(Float64Array::from(vec![v])),
                ScalarResult::Null => Arc::new(Int64Array::new_null(1)),
            };
            columns.push(col);
        }

        RecordBatch::try_new(Arc::clone(schema), columns).ok()
    }

    /// Returns the number of sub-accumulators.
    #[must_use]
    pub fn num_accumulators(&self) -> usize {
        self.accumulators.len()
    }
}

impl Clone for CompositeAccumulator {
    fn clone(&self) -> Self {
        Self {
            accumulators: self.accumulators.iter().map(|a| a.clone_box()).collect(),
        }
    }
}

/// State key prefix for window accumulators (4 bytes)
const WINDOW_STATE_PREFIX: &[u8; 4] = b"win:";

/// Total size of window state key: prefix (4) + `WindowId` (16) = 20 bytes
const WINDOW_STATE_KEY_SIZE: usize = 4 + 16;

/// Tumbling window operator.
///
/// Processes events through non-overlapping, fixed-size time windows.
/// Events are assigned to windows based on their timestamps, aggregated,
/// and results are emitted based on the configured [`EmitStrategy`].
///
/// # Emit Strategies
///
/// - `OnWatermark` (default): Emit when watermark passes window end
/// - `Periodic`: Emit intermediate results at intervals, final on watermark
/// - `OnUpdate`: Emit after every state update
///
/// # Late Data Handling
///
/// Events that arrive after `window_end + allowed_lateness` are considered late.
/// Their behavior is controlled by [`LateDataConfig`]:
/// - Drop the event (default)
/// - Route to a named side output for separate processing
///
/// # State Management
///
/// Window state is stored in the operator context's state store using
/// prefixed keys:
/// - `win:<window_id>` - Accumulator state
/// - `meta:<window_id>` - Window metadata (registration status, etc.)
///
/// # Watermark Triggering
///
/// Windows are triggered when the watermark advances past `window_end + allowed_lateness`.
/// This ensures late data within the grace period is still processed.
pub struct TumblingWindowOperator<A: Aggregator> {
    /// Window assigner
    assigner: TumblingWindowAssigner,
    /// Aggregator function
    aggregator: A,
    /// Allowed lateness for late data
    allowed_lateness_ms: i64,
    /// Track registered timers to avoid duplicates
    registered_windows: std::collections::HashSet<WindowId>,
    /// Track windows with registered periodic timers
    periodic_timer_windows: std::collections::HashSet<WindowId>,
    /// Emit strategy for controlling when results are output
    emit_strategy: EmitStrategy,
    /// Late data handling configuration
    late_data_config: LateDataConfig,
    /// Metrics for late data tracking
    late_data_metrics: LateDataMetrics,
    /// Metrics for window close tracking
    window_close_metrics: WindowCloseMetrics,
    /// Operator ID for checkpointing
    operator_id: String,
    /// Cached output schema (avoids allocation on every emit)
    output_schema: SchemaRef,
    /// Last emitted events per window for changelog retraction
    last_emitted: std::collections::HashMap<WindowId, Event>,
    /// Phantom data for accumulator type
    _phantom: PhantomData<A::Acc>,
}

/// Static counter for generating unique operator IDs without allocation.
static OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

impl<A: Aggregator> TumblingWindowOperator<A>
where
    A::Acc: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    /// Creates a new tumbling window operator.
    ///
    /// # Arguments
    ///
    /// * `assigner` - Window assigner for determining window boundaries
    /// * `aggregator` - Aggregation function to apply within windows
    /// * `allowed_lateness` - Grace period for late data after window close
    /// # Panics
    ///
    /// Panics if allowed lateness does not fit in i64.
    #[must_use]
    pub fn new(
        assigner: TumblingWindowAssigner,
        aggregator: A,
        allowed_lateness: Duration,
    ) -> Self {
        let operator_num = OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new(
                "result",
                aggregator.output_data_type(),
                aggregator.output_nullable(),
            ),
        ]));
        Self {
            assigner,
            aggregator,
            // Ensure lateness fits in i64
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
            registered_windows: std::collections::HashSet::new(),
            periodic_timer_windows: std::collections::HashSet::new(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            window_close_metrics: WindowCloseMetrics::new(),
            operator_id: format!("tumbling_window_{operator_num}"),
            output_schema,
            last_emitted: std::collections::HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new tumbling window operator with a custom operator ID.
    /// # Panics
    ///
    /// Panics if allowed lateness does not fit in i64.
    #[must_use]
    pub fn with_id(
        assigner: TumblingWindowAssigner,
        aggregator: A,
        allowed_lateness: Duration,
        operator_id: String,
    ) -> Self {
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new(
                "result",
                aggregator.output_data_type(),
                aggregator.output_nullable(),
            ),
        ]));
        Self {
            assigner,
            aggregator,
            // Ensure lateness fits in i64
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
            registered_windows: std::collections::HashSet::new(),
            periodic_timer_windows: std::collections::HashSet::new(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            window_close_metrics: WindowCloseMetrics::new(),
            operator_id,
            output_schema,
            last_emitted: std::collections::HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Sets the emit strategy for this window operator.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The emit strategy to use
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use laminar_core::operator::window::{
    ///     TumblingWindowAssigner, TumblingWindowOperator, CountAggregator, EmitStrategy,
    /// };
    /// use std::time::Duration;
    ///
    /// let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
    /// let mut operator = TumblingWindowOperator::new(
    ///     assigner,
    ///     CountAggregator::new(),
    ///     Duration::from_secs(5),
    /// );
    ///
    /// // Emit every 10 seconds instead of waiting for watermark
    /// operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));
    /// ```
    pub fn set_emit_strategy(&mut self, strategy: EmitStrategy) {
        self.emit_strategy = strategy;
    }

    /// Returns the current emit strategy.
    #[must_use]
    pub fn emit_strategy(&self) -> &EmitStrategy {
        &self.emit_strategy
    }

    /// Sets the late data handling configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The late data configuration to use
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use laminar_core::operator::window::{
    ///     TumblingWindowAssigner, TumblingWindowOperator, CountAggregator, LateDataConfig,
    /// };
    /// use std::time::Duration;
    ///
    /// let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
    /// let mut operator = TumblingWindowOperator::new(
    ///     assigner,
    ///     CountAggregator::new(),
    ///     Duration::from_secs(5),
    /// );
    ///
    /// // Route late events to a side output
    /// operator.set_late_data_config(LateDataConfig::with_side_output("late_events".to_string()));
    /// ```
    pub fn set_late_data_config(&mut self, config: LateDataConfig) {
        self.late_data_config = config;
    }

    /// Returns the current late data configuration.
    #[must_use]
    pub fn late_data_config(&self) -> &LateDataConfig {
        &self.late_data_config
    }

    /// Returns the late data metrics.
    ///
    /// Use this to monitor late data behavior and set up alerts.
    #[must_use]
    pub fn late_data_metrics(&self) -> &LateDataMetrics {
        &self.late_data_metrics
    }

    /// Resets the late data metrics counters.
    pub fn reset_late_data_metrics(&mut self) {
        self.late_data_metrics.reset();
    }

    /// Returns the window close metrics.
    ///
    /// Use this to monitor window close throughput and watermark lag.
    #[must_use]
    pub fn window_close_metrics(&self) -> &WindowCloseMetrics {
        &self.window_close_metrics
    }

    /// Resets the window close metrics counters.
    pub fn reset_window_close_metrics(&mut self) {
        self.window_close_metrics.reset();
    }

    /// Returns the number of windows currently accumulating events.
    #[must_use]
    pub fn active_windows_count(&self) -> usize {
        self.registered_windows.len()
    }

    /// Returns the window assigner.
    #[must_use]
    pub fn assigner(&self) -> &TumblingWindowAssigner {
        &self.assigner
    }

    /// Returns the allowed lateness in milliseconds.
    #[must_use]
    pub fn allowed_lateness_ms(&self) -> i64 {
        self.allowed_lateness_ms
    }

    /// Generates the state key for a window's accumulator.
    ///
    /// Returns a stack-allocated fixed-size array to avoid heap allocation
    /// on the hot path. This is critical for Ring 0 performance.
    #[inline]
    fn state_key(window_id: &WindowId) -> [u8; WINDOW_STATE_KEY_SIZE] {
        let mut key = [0u8; WINDOW_STATE_KEY_SIZE];
        key[..4].copy_from_slice(WINDOW_STATE_PREFIX);
        let window_key = window_id.to_key_inline();
        key[4..20].copy_from_slice(&window_key);
        key
    }

    /// Gets the accumulator for a window, creating a new one if needed.
    fn get_accumulator(&self, window_id: &WindowId, state: &dyn StateStore) -> A::Acc {
        let key = Self::state_key(window_id);
        state
            .get_typed::<A::Acc>(&key)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.aggregator.create_accumulator())
    }

    /// Stores the accumulator for a window.
    fn put_accumulator(
        window_id: &WindowId,
        acc: &A::Acc,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let key = Self::state_key(window_id);
        state
            .put_typed(&key, acc)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))
    }

    /// Deletes the accumulator for a window.
    fn delete_accumulator(
        window_id: &WindowId,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let key = Self::state_key(window_id);
        state
            .delete(&key)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))
    }

    /// Checks if an event is late (after window close + allowed lateness).
    fn is_late(&self, event_time: i64, watermark: i64) -> bool {
        let window_id = self.assigner.assign(event_time);
        let cleanup_time = window_id.end + self.allowed_lateness_ms;
        watermark >= cleanup_time
    }

    /// Registers a timer for window triggering if not already registered.
    fn maybe_register_timer(&mut self, window_id: WindowId, ctx: &mut OperatorContext) {
        if !self.registered_windows.contains(&window_id) {
            // Register timer at window_end + allowed_lateness
            let trigger_time = window_id.end + self.allowed_lateness_ms;
            ctx.timers.register_timer(
                trigger_time,
                Some(window_id.to_key()),
                Some(ctx.operator_index),
            );
            self.registered_windows.insert(window_id);
        }
    }

    /// Registers a periodic timer for intermediate emissions.
    ///
    /// The timer key uses a special encoding to distinguish from final timers:
    /// - Final timers: raw `WindowId` bytes (16 bytes)
    /// - Periodic timers: `WindowId` with high bit set in first byte
    fn maybe_register_periodic_timer(&mut self, window_id: WindowId, ctx: &mut OperatorContext) {
        if let EmitStrategy::Periodic(interval) = &self.emit_strategy {
            if !self.periodic_timer_windows.contains(&window_id) {
                // Register first periodic timer at processing_time + interval
                let interval_ms =
                    i64::try_from(interval.as_millis()).expect("Interval must fit in i64");
                let trigger_time = ctx.processing_time + interval_ms;

                // Create a key with high bit set to distinguish from final timers
                let key = Self::periodic_timer_key(&window_id);

                ctx.timers
                    .register_timer(trigger_time, Some(key), Some(ctx.operator_index));
                self.periodic_timer_windows.insert(window_id);
            }
        }
    }

    /// Creates a periodic timer key from a window ID.
    ///
    /// Uses the high bit of the first byte as a marker to distinguish
    /// periodic timers from final watermark timers.
    #[inline]
    fn periodic_timer_key(window_id: &WindowId) -> super::TimerKey {
        let mut key = window_id.to_key();
        // Set the high bit of the first byte to mark as periodic
        if !key.is_empty() {
            key[0] |= 0x80;
        }
        key
    }

    /// Checks if a timer key is for a periodic timer.
    #[inline]
    fn is_periodic_timer_key(key: &[u8]) -> bool {
        !key.is_empty() && (key[0] & 0x80) != 0
    }

    /// Extracts the window ID from a periodic timer key.
    #[inline]
    fn window_id_from_periodic_key(key: &[u8]) -> Option<WindowId> {
        if key.len() != 16 {
            return None;
        }
        let mut clean_key = [0u8; 16];
        clean_key.copy_from_slice(key);
        // Clear the high bit to get the original window ID
        clean_key[0] &= 0x7F;
        WindowId::from_key(&clean_key)
    }

    /// Creates an intermediate result for a window without cleaning up state.
    ///
    /// Returns `None` if the window is empty.
    /// Creates an intermediate result for a window without cleaning up state.
    ///
    /// Returns `None` if the window is empty. Uses `ResultToArrow` for
    /// type-preserving output.
    fn create_intermediate_result(
        &self,
        window_id: &WindowId,
        state: &dyn crate::state::StateStore,
    ) -> Option<Event> {
        let acc = self.get_accumulator(window_id, state);

        if acc.is_empty() {
            return None;
        }

        let result = acc.result();
        let result_array = result.to_arrow_array();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![window_id.start])),
                Arc::new(Int64Array::from(vec![window_id.end])),
                result_array,
            ],
        )
        .ok()?;

        Some(Event::new(window_id.end, batch))
    }

    /// Handles periodic timer expiration for intermediate emissions.
    fn handle_periodic_timer(
        &mut self,
        window_id: WindowId,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        // Check if window is still valid (not yet closed by watermark)
        if !self.registered_windows.contains(&window_id) {
            // Window already closed, remove from periodic tracking
            self.periodic_timer_windows.remove(&window_id);
            return output;
        }

        // Emit intermediate result
        if let Some(event) = self.create_intermediate_result(&window_id, ctx.state) {
            output.push(Output::Event(event));
        }

        // Schedule next periodic timer if still within window
        if let EmitStrategy::Periodic(interval) = &self.emit_strategy {
            let interval_ms =
                i64::try_from(interval.as_millis()).expect("Interval must fit in i64");
            let next_trigger = ctx.processing_time + interval_ms;

            // Only schedule if the window hasn't closed yet
            let window_close_time = window_id.end + self.allowed_lateness_ms;
            if next_trigger < window_close_time {
                let key = Self::periodic_timer_key(&window_id);
                ctx.timers
                    .register_timer(next_trigger, Some(key), Some(ctx.operator_index));
            }
        }

        output
    }
}

impl<A: Aggregator> Operator for TumblingWindowOperator<A>
where
    A::Acc: 'static
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let event_time = event.timestamp;

        // Update watermark with the new event and get any emitted watermark
        let emitted_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if this event is too late (beyond allowed lateness)
        // Use the current watermark (not just the newly emitted one) for the check
        let current_wm = ctx.watermark_generator.current_watermark();
        if current_wm > i64::MIN && self.is_late(event_time, current_wm) {
            let mut output = OutputVec::new();

            // EMIT FINAL drops late data entirely
            if self.emit_strategy.drops_late_data() {
                self.late_data_metrics.record_dropped();
                return output; // Silently drop - no LateEvent output
            }

            // Handle late event based on configuration
            if let Some(side_output_name) = self.late_data_config.side_output() {
                // Route to named side output
                self.late_data_metrics.record_side_output();
                output.push(Output::SideOutput {
                    name: side_output_name.to_string(),
                    event: event.clone(),
                });
            } else {
                // No side output configured - emit as LateEvent (may be dropped by downstream)
                self.late_data_metrics.record_dropped();
                output.push(Output::LateEvent(event.clone()));
            }
            return output;
        }

        // Assign event to window
        let window_id = self.assigner.assign(event_time);

        // Track if state was updated (for OnUpdate and Changelog strategies)
        let mut state_updated = false;

        // Extract values and update accumulator (handles multi-row batches)
        let values = self.aggregator.extract_batch(event);
        if !values.is_empty() {
            let mut acc = self.get_accumulator(&window_id, ctx.state);
            for value in values {
                acc.add(value);
            }
            if let Err(e) = Self::put_accumulator(&window_id, &acc, ctx.state) {
                // Log error but don't fail - we'll retry on next event
                tracing::error!("Failed to store window state: {e}");
            } else {
                state_updated = true;
            }
        }

        // Register timer for this window (watermark-based final emission)
        self.maybe_register_timer(window_id, ctx);

        // OnWindowClose and Final suppress intermediate emissions
        // Don't register periodic timers for these strategies
        if !self.emit_strategy.suppresses_intermediate() {
            self.maybe_register_periodic_timer(window_id, ctx);
        }

        // Emit watermark update if generated
        let mut output = OutputVec::new();
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        // Handle different emit strategies
        if state_updated {
            match &self.emit_strategy {
                // OnUpdate: emit intermediate result as regular event
                EmitStrategy::OnUpdate => {
                    if let Some(event) = self.create_intermediate_result(&window_id, ctx.state) {
                        output.push(Output::Event(event));
                    }
                }
                // Changelog: emit retraction for old value, then insert new value
                EmitStrategy::Changelog => {
                    if let Some(new_event) = self.create_intermediate_result(&window_id, ctx.state)
                    {
                        // Emit delete for previous value (retraction)
                        if let Some(old_event) = self.last_emitted.get(&window_id) {
                            let delete =
                                ChangelogRecord::delete(old_event.clone(), ctx.processing_time);
                            output.push(Output::Changelog(delete));
                        }
                        // Emit insert for new value
                        let insert =
                            ChangelogRecord::insert(new_event.clone(), ctx.processing_time);
                        output.push(Output::Changelog(insert));
                        self.last_emitted.insert(window_id, new_event);
                    }
                }
                // Other strategies: no intermediate emission
                EmitStrategy::OnWatermark
                | EmitStrategy::Periodic(_)
                | EmitStrategy::OnWindowClose
                | EmitStrategy::Final => {}
            }
        }

        output
    }

    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        // Check if this is a periodic timer (high bit set)
        if Self::is_periodic_timer_key(&timer.key) {
            // OnWindowClose and Final suppress periodic emissions
            if self.emit_strategy.suppresses_intermediate() {
                // Don't emit, just clean up the periodic timer tracking
                if let Some(window_id) = Self::window_id_from_periodic_key(&timer.key) {
                    self.periodic_timer_windows.remove(&window_id);
                }
                return OutputVec::new();
            }

            if let Some(window_id) = Self::window_id_from_periodic_key(&timer.key) {
                return self.handle_periodic_timer(window_id, ctx);
            }
            return OutputVec::new();
        }

        // Parse window ID from timer key (final emission timer)
        let Some(window_id) = WindowId::from_key(&timer.key) else {
            return OutputVec::new();
        };

        // Get the accumulator
        let acc = self.get_accumulator(&window_id, ctx.state);

        // Skip empty windows
        if acc.is_empty() {
            // Clean up state
            let _ = Self::delete_accumulator(&window_id, ctx.state);
            self.registered_windows.remove(&window_id);
            self.periodic_timer_windows.remove(&window_id);
            self.last_emitted.remove(&window_id);
            return OutputVec::new();
        }

        // Get the result
        let result = acc.result();

        // Clean up window state
        let _ = Self::delete_accumulator(&window_id, ctx.state);
        self.registered_windows.remove(&window_id);
        self.periodic_timer_windows.remove(&window_id);
        self.last_emitted.remove(&window_id);

        // Convert result to Arrow array (preserves native type: Float64 for AVG, etc.)
        let result_array = result.to_arrow_array();

        // Create output batch using cached schema (avoids ~200ns allocation per emit)
        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![window_id.start])),
                Arc::new(Int64Array::from(vec![window_id.end])),
                result_array,
            ],
        );

        let mut output = OutputVec::new();
        match batch {
            Ok(data) => {
                let event = Event::new(window_id.end, data);

                // Record window close metrics
                self.window_close_metrics
                    .record_close(window_id.end, ctx.processing_time);

                // Emit based on strategy
                match &self.emit_strategy {
                    // Changelog: wrap in changelog record for CDC
                    EmitStrategy::Changelog => {
                        let record = ChangelogRecord::insert(event, ctx.processing_time);
                        output.push(Output::Changelog(record));
                    }
                    // All other strategies: emit as regular event
                    EmitStrategy::OnWatermark
                    | EmitStrategy::Periodic(_)
                    | EmitStrategy::OnUpdate
                    | EmitStrategy::OnWindowClose
                    | EmitStrategy::Final => {
                        output.push(Output::Event(event));
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to create output batch: {e}");
            }
        }
        output
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize both registered windows and periodic timer windows using rkyv
        let windows: Vec<_> = self.registered_windows.iter().copied().collect();
        let periodic_windows: Vec<_> = self.periodic_timer_windows.iter().copied().collect();

        // Create a tuple of both sets
        let checkpoint_data = (windows, periodic_windows);
        let data = rkyv::to_bytes::<RkyvError>(&checkpoint_data)
            .map(|v| v.to_vec())
            .unwrap_or_default();

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.operator_id != self.operator_id {
            return Err(OperatorError::StateAccessFailed(format!(
                "Operator ID mismatch: expected {}, got {}",
                self.operator_id, state.operator_id
            )));
        }

        // Try to deserialize as the new format (tuple of two vectors)
        if let Ok(archived) =
            rkyv::access::<rkyv::Archived<(Vec<WindowId>, Vec<WindowId>)>, RkyvError>(&state.data)
        {
            if let Ok((windows, periodic_windows)) =
                rkyv::deserialize::<(Vec<WindowId>, Vec<WindowId>), RkyvError>(archived)
            {
                self.registered_windows = windows.into_iter().collect();
                self.periodic_timer_windows = periodic_windows.into_iter().collect();
                return Ok(());
            }
        }

        // Fall back to old format (single vector) for backwards compatibility
        let archived = rkyv::access::<rkyv::Archived<Vec<WindowId>>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let windows: Vec<WindowId> = rkyv::deserialize::<Vec<WindowId>, RkyvError>(archived)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.registered_windows = windows.into_iter().collect();
        self.periodic_timer_windows = std::collections::HashSet::new();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
