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
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use smallvec::SmallVec;
use rkyv::{
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RkyvError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for late data handling.
///
/// Controls what happens to events that arrive after their window has closed
/// (i.e., after `window_end + allowed_lateness`).
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

/// Strategy for when window results should be emitted.
///
/// This controls the trade-off between result freshness and efficiency:
/// - `OnWatermark` is most efficient but has highest latency
/// - `Periodic` balances freshness and efficiency
/// - `OnUpdate` provides lowest latency but highest overhead
/// - `OnWindowClose` (F011B) is for append-only sinks
/// - `Changelog` (F011B) emits Z-set weighted records for CDC
/// - `Final` (F011B) suppresses all intermediate results
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EmitStrategy {
    // === Existing (F011) ===

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

    // === New (F011B) ===

    /// Emit ONLY when watermark passes window end. No intermediate emissions.
    ///
    /// **Critical for append-only sinks** (Kafka, S3, Delta Lake, Iceberg).
    /// Unlike `OnWatermark`, this NEVER emits before window close, even with
    /// late data retractions. Late data is buffered until next window close.
    ///
    /// Key difference from `OnWatermark`:
    /// - `OnWatermark`: May emit retractions for late data
    /// - `OnWindowClose`: Buffers late data, only emits final result
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
    /// - Cascading materialized views (F060)
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

    // === F011B Helper Methods ===

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
    /// - `OnWindowClose`: buffers late data
    /// - `Final`: drops late data
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

// === F011B: Changelog/Z-Set Support ===

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
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(size: Duration) -> Self {
        // Truncation is acceptable: window sizes > 2^63 ms (~292 million years) are not practical
        let size_ms = size.as_millis() as i64;
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
    #[allow(clippy::cast_possible_wrap)]
    fn to_i64(&self) -> i64 {
        *self as i64
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
    #[allow(clippy::cast_possible_truncation)]
    fn to_i64(&self) -> i64 {
        self.map(|f| f as i64).unwrap_or(0)
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
    type Output: ResultToI64;

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

    fn extract(&self, event: &Event) -> Option<i64> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        let batch = &event.data;
        if self.column_index >= batch.num_columns() {
            return None;
        }

        let column = batch.column(self.column_index);
        let array = column.as_primitive_opt::<Int64Type>()?;

        // For average, we add each value individually
        array.iter().flatten().next()
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
    /// Operator ID for checkpointing
    operator_id: String,
    /// Cached output schema (avoids allocation on every emit)
    output_schema: SchemaRef,
    /// Phantom data for accumulator type
    _phantom: PhantomData<A::Acc>,
}

/// Static counter for generating unique operator IDs without allocation.
static OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates the standard window output schema.
///
/// This schema is used for all window aggregation results.
fn create_window_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
        Field::new("result", DataType::Int64, false),
    ]))
}

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
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        assigner: TumblingWindowAssigner,
        aggregator: A,
        allowed_lateness: Duration,
    ) -> Self {
        let operator_num = OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            assigner,
            aggregator,
            // Truncation is acceptable: lateness > 2^63 ms (~292 million years) is not practical
            allowed_lateness_ms: allowed_lateness.as_millis() as i64,
            registered_windows: std::collections::HashSet::new(),
            periodic_timer_windows: std::collections::HashSet::new(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            operator_id: format!("tumbling_window_{operator_num}"),
            output_schema: create_window_output_schema(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new tumbling window operator with a custom operator ID.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn with_id(
        assigner: TumblingWindowAssigner,
        aggregator: A,
        allowed_lateness: Duration,
        operator_id: String,
    ) -> Self {
        Self {
            assigner,
            aggregator,
            // Truncation is acceptable: lateness > 2^63 ms (~292 million years) is not practical
            allowed_lateness_ms: allowed_lateness.as_millis() as i64,
            registered_windows: std::collections::HashSet::new(),
            periodic_timer_windows: std::collections::HashSet::new(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            operator_id,
            output_schema: create_window_output_schema(),
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
    #[allow(clippy::unused_self)] // May use self for namespacing in the future
    fn state_key(&self, window_id: &WindowId) -> [u8; WINDOW_STATE_KEY_SIZE] {
        let mut key = [0u8; WINDOW_STATE_KEY_SIZE];
        key[..4].copy_from_slice(WINDOW_STATE_PREFIX);
        let window_key = window_id.to_key_inline();
        key[4..20].copy_from_slice(&window_key);
        key
    }

    /// Gets the accumulator for a window, creating a new one if needed.
    fn get_accumulator(&self, window_id: &WindowId, state: &dyn StateStore) -> A::Acc {
        let key = self.state_key(window_id);
        state
            .get_typed::<A::Acc>(&key)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.aggregator.create_accumulator())
    }

    /// Stores the accumulator for a window.
    fn put_accumulator(
        &self,
        window_id: &WindowId,
        acc: &A::Acc,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let key = self.state_key(window_id);
        state
            .put_typed(&key, acc)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))
    }

    /// Deletes the accumulator for a window.
    fn delete_accumulator(
        &self,
        window_id: &WindowId,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let key = self.state_key(window_id);
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
            ctx.timers
                .register_timer(trigger_time, Some(window_id.to_key()), Some(ctx.operator_index));
            self.registered_windows.insert(window_id);
        }
    }

    /// Registers a periodic timer for intermediate emissions.
    ///
    /// The timer key uses a special encoding to distinguish from final timers:
    /// - Final timers: raw `WindowId` bytes (16 bytes)
    /// - Periodic timers: `WindowId` with high bit set in first byte
    #[allow(clippy::cast_possible_truncation)]
    fn maybe_register_periodic_timer(
        &mut self,
        window_id: WindowId,
        ctx: &mut OperatorContext,
    ) {
        if let EmitStrategy::Periodic(interval) = &self.emit_strategy {
            if !self.periodic_timer_windows.contains(&window_id) {
                // Register first periodic timer at processing_time + interval
                let interval_ms = interval.as_millis() as i64;
                let trigger_time = ctx.processing_time + interval_ms;

                // Create a key with high bit set to distinguish from final timers
                let key = Self::periodic_timer_key(&window_id);

                ctx.timers.register_timer(trigger_time, Some(key), Some(ctx.operator_index));
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
        let result_i64 = result.to_i64();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![window_id.start])),
                Arc::new(Int64Array::from(vec![window_id.end])),
                Arc::new(Int64Array::from(vec![result_i64])),
            ],
        ).ok()?;

        Some(Event {
            timestamp: window_id.end,
            data: batch,
        })
    }

    /// Handles periodic timer expiration for intermediate emissions.
    #[allow(clippy::cast_possible_truncation)]
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
            let interval_ms = interval.as_millis() as i64;
            let next_trigger = ctx.processing_time + interval_ms;

            // Only schedule if the window hasn't closed yet
            let window_close_time = window_id.end + self.allowed_lateness_ms;
            if next_trigger < window_close_time {
                let key = Self::periodic_timer_key(&window_id);
                ctx.timers.register_timer(next_trigger, Some(key), Some(ctx.operator_index));
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

            // F011B: EMIT FINAL drops late data entirely
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

        // Extract value and update accumulator
        if let Some(value) = self.aggregator.extract(event) {
            let mut acc = self.get_accumulator(&window_id, ctx.state);
            acc.add(value);
            if let Err(e) = self.put_accumulator(&window_id, &acc, ctx.state) {
                // Log error but don't fail - we'll retry on next event
                eprintln!("Failed to store window state: {e}");
            } else {
                state_updated = true;
            }
        }

        // Register timer for this window (watermark-based final emission)
        self.maybe_register_timer(window_id, ctx);

        // F011B: OnWindowClose and Final suppress intermediate emissions
        // Don't register periodic timers for these strategies
        if !self.emit_strategy.suppresses_intermediate() {
            self.maybe_register_periodic_timer(window_id, ctx);
        }

        // Emit watermark update if generated
        let mut output = OutputVec::new();
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        // F011B: Handle different emit strategies
        if state_updated {
            match &self.emit_strategy {
                // OnUpdate: emit intermediate result as regular event
                EmitStrategy::OnUpdate => {
                    if let Some(event) = self.create_intermediate_result(&window_id, ctx.state) {
                        output.push(Output::Event(event));
                    }
                }
                // Changelog: emit changelog record on every update
                EmitStrategy::Changelog => {
                    if let Some(event) = self.create_intermediate_result(&window_id, ctx.state) {
                        // For intermediate updates in changelog mode, we emit as insert
                        // Full CDC support (with retractions) requires F063
                        let record = ChangelogRecord::insert(event, ctx.processing_time);
                        output.push(Output::Changelog(record));
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
            // F011B: OnWindowClose and Final suppress periodic emissions
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
            let _ = self.delete_accumulator(&window_id, ctx.state);
            self.registered_windows.remove(&window_id);
            self.periodic_timer_windows.remove(&window_id);
            return OutputVec::new();
        }

        // Get the result
        let result = acc.result();

        // Clean up window state
        let _ = self.delete_accumulator(&window_id, ctx.state);
        self.registered_windows.remove(&window_id);
        self.periodic_timer_windows.remove(&window_id);

        // Convert result to i64 for the batch
        let result_i64 = result.to_i64();

        // Create output batch using cached schema (avoids ~200ns allocation per emit)
        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![window_id.start])),
                Arc::new(Int64Array::from(vec![window_id.end])),
                Arc::new(Int64Array::from(vec![result_i64])),
            ],
        );

        let mut output = OutputVec::new();
        match batch {
            Ok(data) => {
                let event = Event {
                    timestamp: window_id.end,
                    data,
                };

                // F011B: Emit based on strategy
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
                eprintln!("Failed to create output batch: {e}");
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
        if let Ok(archived) = rkyv::access::<rkyv::Archived<(Vec<WindowId>, Vec<WindowId>)>, RkyvError>(&state.data) {
            if let Ok((windows, periodic_windows)) = rkyv::deserialize::<(Vec<WindowId>, Vec<WindowId>), RkyvError>(archived) {
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

/// Configuration for tumbling windows (legacy - use `TumblingWindowAssigner` instead).
#[derive(Debug, Clone)]
pub struct TumblingWindowConfig {
    /// Window duration
    pub duration: Duration,
    /// Grace period for late data
    pub allowed_lateness: Duration,
}

/// Legacy tumbling window operator (deprecated).
///
/// Use [`TumblingWindowOperator`] with [`TumblingWindowAssigner`] instead.
#[deprecated(
    since = "0.1.0",
    note = "Use TumblingWindowOperator with TumblingWindowAssigner instead"
)]
pub struct TumblingWindow {
    config: TumblingWindowConfig,
}

#[allow(deprecated)]
impl TumblingWindow {
    /// Get the window duration.
    #[must_use]
    pub fn duration(&self) -> Duration {
        self.config.duration
    }

    /// Get the allowed lateness.
    #[must_use]
    pub fn allowed_lateness(&self) -> Duration {
        self.config.allowed_lateness
    }

    /// Creates a new tumbling window operator.
    #[must_use]
    pub fn new(config: TumblingWindowConfig) -> Self {
        Self { config }
    }
}

#[allow(deprecated)]
impl Operator for TumblingWindow {
    fn process(&mut self, _event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        // Legacy stub - use TumblingWindowOperator instead
        OutputVec::new()
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "legacy_tumbling_window".to_string(),
            data: vec![],
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_event(timestamp: i64, value: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
        Event {
            timestamp,
            data: batch,
        }
    }

    fn create_test_context<'a>(
        timers: &'a mut TimerService,
        state: &'a mut dyn StateStore,
        watermark_gen: &'a mut dyn crate::time::WatermarkGenerator,
    ) -> OperatorContext<'a> {
        OperatorContext {
            event_time: 0,
            processing_time: 0,
            timers,
            state,
            watermark_generator: watermark_gen,
            operator_index: 0,
        }
    }

    #[test]
    fn test_window_id_creation() {
        let window = WindowId::new(1000, 2000);
        assert_eq!(window.start, 1000);
        assert_eq!(window.end, 2000);
        assert_eq!(window.duration_ms(), 1000);
    }

    #[test]
    fn test_window_id_serialization() {
        let window = WindowId::new(1000, 2000);
        let key = window.to_key();
        assert_eq!(key.len(), 16);

        let restored = WindowId::from_key(&key).unwrap();
        assert_eq!(restored, window);
    }

    #[test]
    fn test_tumbling_assigner_positive_timestamps() {
        let assigner = TumblingWindowAssigner::from_millis(1000);

        // Events at different times within same window
        assert_eq!(assigner.assign(0), WindowId::new(0, 1000));
        assert_eq!(assigner.assign(500), WindowId::new(0, 1000));
        assert_eq!(assigner.assign(999), WindowId::new(0, 1000));

        // Event at window boundary goes to next window
        assert_eq!(assigner.assign(1000), WindowId::new(1000, 2000));
        assert_eq!(assigner.assign(1500), WindowId::new(1000, 2000));
    }

    #[test]
    fn test_tumbling_assigner_negative_timestamps() {
        let assigner = TumblingWindowAssigner::from_millis(1000);

        // Negative timestamps
        assert_eq!(assigner.assign(-1), WindowId::new(-1000, 0));
        assert_eq!(assigner.assign(-500), WindowId::new(-1000, 0));
        assert_eq!(assigner.assign(-1000), WindowId::new(-1000, 0));
        assert_eq!(assigner.assign(-1001), WindowId::new(-2000, -1000));
    }

    #[test]
    fn test_count_aggregator() {
        let mut acc = CountAccumulator::default();
        assert!(acc.is_empty());
        assert_eq!(acc.result(), 0);

        acc.add(());
        acc.add(());
        acc.add(());

        assert!(!acc.is_empty());
        assert_eq!(acc.result(), 3);
    }

    #[test]
    fn test_sum_accumulator() {
        let mut acc = SumAccumulator::default();
        acc.add(10);
        acc.add(20);
        acc.add(30);

        assert_eq!(acc.result(), 60);
    }

    #[test]
    fn test_min_accumulator() {
        let mut acc = MinAccumulator::default();
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);

        acc.add(50);
        acc.add(10);
        acc.add(30);

        assert_eq!(acc.result(), Some(10));
    }

    #[test]
    fn test_max_accumulator() {
        let mut acc = MaxAccumulator::default();
        acc.add(10);
        acc.add(50);
        acc.add(30);

        assert_eq!(acc.result(), Some(50));
    }

    #[test]
    fn test_avg_accumulator() {
        let mut acc = AvgAccumulator::default();
        acc.add(10);
        acc.add(20);
        acc.add(30);

        let result = acc.result().unwrap();
        assert!((result - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_accumulator_merge() {
        let mut acc1 = SumAccumulator::default();
        acc1.add(10);
        acc1.add(20);

        let mut acc2 = SumAccumulator::default();
        acc2.add(30);
        acc2.add(40);

        acc1.merge(&acc2);
        assert_eq!(acc1.result(), 100);
    }

    #[test]
    fn test_tumbling_window_operator_basic() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events in the same window
        let event1 = create_test_event(100, 1);
        let event2 = create_test_event(500, 2);
        let event3 = create_test_event(900, 3);

        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx);
        }
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event3, &mut ctx);
        }

        // Check that a timer was registered
        assert_eq!(operator.registered_windows.len(), 1);
        assert!(operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
    }

    #[test]
    fn test_tumbling_window_operator_trigger() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events
        for ts in [100, 500, 900] {
            let event = create_test_event(ts, 1);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Trigger the window via timer
        let timer = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };

        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        let outputs = operator.on_timer(timer, &mut ctx);

        assert_eq!(outputs.len(), 1);
        match &outputs[0] {
            Output::Event(event) => {
                assert_eq!(event.timestamp, 1000); // window end
                                                   // Check the result column (count = 3)
                let result_col = event.data.column(2);
                let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(result_array.value(0), 3);
            }
            _ => panic!("Expected Event output"),
        }

        // Window should be cleaned up
        assert!(operator.registered_windows.is_empty());
    }

    #[test]
    fn test_tumbling_window_multiple_windows() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Events in different windows
        let events = [
            create_test_event(100, 1),  // Window [0, 1000)
            create_test_event(500, 2),  // Window [0, 1000)
            create_test_event(1100, 3), // Window [1000, 2000)
            create_test_event(1500, 4), // Window [1000, 2000)
            create_test_event(2500, 5), // Window [2000, 3000)
        ];

        for event in &events {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(event, &mut ctx);
        }

        // Should have 3 windows registered
        assert_eq!(operator.registered_windows.len(), 3);
    }

    #[test]
    fn test_tumbling_window_checkpoint_restore() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Register some windows
        operator.registered_windows.insert(WindowId::new(0, 1000));
        operator
            .registered_windows
            .insert(WindowId::new(1000, 2000));

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Create a new operator and restore
        let mut restored_operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        restored_operator.restore(checkpoint).unwrap();

        assert_eq!(restored_operator.registered_windows.len(), 2);
        assert!(restored_operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
        assert!(restored_operator
            .registered_windows
            .contains(&WindowId::new(1000, 2000)));
    }

    #[test]
    fn test_sum_aggregator_extraction() {
        let aggregator = SumAggregator::new(0);
        let event = create_test_event(100, 42);

        let extracted = aggregator.extract(&event);
        assert_eq!(extracted, Some(42));
    }

    #[test]
    fn test_empty_window_trigger() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Trigger without any events
        let timer = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };

        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        let outputs = operator.on_timer(timer, &mut ctx);

        // Empty window should produce no output
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_window_assigner_trait() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let windows = assigner.assign_windows(500);

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], WindowId::new(0, 1000));
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_tumbling_window_config() {
        let config = TumblingWindowConfig {
            duration: Duration::from_secs(60),
            allowed_lateness: Duration::from_secs(5),
        };

        let window = TumblingWindow::new(config);
        assert_eq!(window.duration(), Duration::from_secs(60));
        assert_eq!(window.allowed_lateness(), Duration::from_secs(5));
    }

    // ==================== EmitStrategy Tests ====================

    #[test]
    fn test_emit_strategy_default() {
        let strategy = EmitStrategy::default();
        assert_eq!(strategy, EmitStrategy::OnWatermark);
    }

    #[test]
    fn test_emit_strategy_on_watermark() {
        let strategy = EmitStrategy::OnWatermark;
        assert!(!strategy.needs_periodic_timer());
        assert!(strategy.periodic_interval().is_none());
        assert!(!strategy.emits_on_update());
    }

    #[test]
    fn test_emit_strategy_periodic() {
        let interval = Duration::from_secs(10);
        let strategy = EmitStrategy::Periodic(interval);
        assert!(strategy.needs_periodic_timer());
        assert_eq!(strategy.periodic_interval(), Some(interval));
        assert!(!strategy.emits_on_update());
    }

    #[test]
    fn test_emit_strategy_on_update() {
        let strategy = EmitStrategy::OnUpdate;
        assert!(!strategy.needs_periodic_timer());
        assert!(strategy.periodic_interval().is_none());
        assert!(strategy.emits_on_update());
    }

    #[test]
    fn test_window_operator_set_emit_strategy() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Default is OnWatermark
        assert_eq!(*operator.emit_strategy(), EmitStrategy::OnWatermark);

        // Set to Periodic
        operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(5)));
        assert_eq!(
            *operator.emit_strategy(),
            EmitStrategy::Periodic(Duration::from_secs(5))
        );

        // Set to OnUpdate
        operator.set_emit_strategy(EmitStrategy::OnUpdate);
        assert_eq!(*operator.emit_strategy(), EmitStrategy::OnUpdate);
    }

    #[test]
    fn test_emit_on_update_emits_intermediate_results() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set emit strategy to OnUpdate
        operator.set_emit_strategy(EmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process first event - should emit intermediate result
        let event1 = create_test_event(100, 1);
        let outputs1 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx)
        };

        // Should have at least one event output (intermediate result)
        let has_event = outputs1.iter().any(|o| matches!(o, Output::Event(_)));
        assert!(has_event, "OnUpdate should emit intermediate result after first event");

        // Process second event - should emit another intermediate result
        let event2 = create_test_event(500, 2);
        let outputs2 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx)
        };

        // Should have intermediate result with count = 2
        let event_output = outputs2.iter().find_map(|o| {
            if let Output::Event(e) = o {
                Some(e)
            } else {
                None
            }
        });

        assert!(event_output.is_some(), "OnUpdate should emit after second event");
        if let Some(event) = event_output {
            let result_col = event.data.column(2);
            let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_array.value(0), 2, "Intermediate count should be 2");
        }
    }

    #[test]
    fn test_emit_on_watermark_no_intermediate_results() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Default is OnWatermark - no intermediate emissions
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events
        let event1 = create_test_event(100, 1);
        let outputs1 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx)
        };

        // Should NOT have event output (only watermark update if any)
        let has_intermediate_event = outputs1.iter().any(|o| matches!(o, Output::Event(_)));
        assert!(
            !has_intermediate_event,
            "OnWatermark should not emit intermediate results"
        );
    }

    #[test]
    fn test_checkpoint_restore_with_emit_strategy() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set emit strategy and register some windows
        operator.set_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(10)));
        operator.registered_windows.insert(WindowId::new(0, 1000));
        operator.periodic_timer_windows.insert(WindowId::new(0, 1000));

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Create a new operator and restore
        let mut restored_operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        restored_operator.restore(checkpoint).unwrap();

        // Both registered_windows and periodic_timer_windows should be restored
        assert_eq!(restored_operator.registered_windows.len(), 1);
        assert_eq!(restored_operator.periodic_timer_windows.len(), 1);
        assert!(restored_operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
        assert!(restored_operator
            .periodic_timer_windows
            .contains(&WindowId::new(0, 1000)));
    }

    #[test]
    fn test_periodic_timer_key_format() {
        // Verify the periodic timer key format
        let window_id = WindowId::new(1000, 2000);

        // Create periodic key using the helper
        let periodic_key = TumblingWindowOperator::<CountAggregator>::periodic_timer_key(&window_id);

        // Periodic key should be 16 bytes (same as window key, but with high bit set)
        assert_eq!(periodic_key.len(), 16);

        // First byte should have high bit set
        assert!(TumblingWindowOperator::<CountAggregator>::is_periodic_timer_key(&periodic_key));

        // Extract window ID from periodic key
        let extracted = TumblingWindowOperator::<CountAggregator>::window_id_from_periodic_key(&periodic_key);
        assert_eq!(extracted, Some(window_id));

        // Regular window key should not be detected as periodic
        let regular_key = window_id.to_key();
        assert!(!TumblingWindowOperator::<CountAggregator>::is_periodic_timer_key(&regular_key));
    }

    // ==================== Late Data Handling Tests (F012) ====================

    #[test]
    fn test_late_data_config_default() {
        let config = LateDataConfig::default();
        assert!(config.should_drop());
        assert!(config.side_output().is_none());
    }

    #[test]
    fn test_late_data_config_drop() {
        let config = LateDataConfig::drop();
        assert!(config.should_drop());
        assert!(config.side_output().is_none());
    }

    #[test]
    fn test_late_data_config_with_side_output() {
        let config = LateDataConfig::with_side_output("late_events".to_string());
        assert!(!config.should_drop());
        assert_eq!(config.side_output(), Some("late_events"));
    }

    #[test]
    fn test_late_data_metrics_initial() {
        let metrics = LateDataMetrics::new();
        assert_eq!(metrics.late_events_total(), 0);
        assert_eq!(metrics.late_events_dropped(), 0);
        assert_eq!(metrics.late_events_side_output(), 0);
    }

    #[test]
    fn test_late_data_metrics_tracking() {
        let mut metrics = LateDataMetrics::new();

        metrics.record_dropped();
        metrics.record_dropped();
        metrics.record_side_output();

        assert_eq!(metrics.late_events_total(), 3);
        assert_eq!(metrics.late_events_dropped(), 2);
        assert_eq!(metrics.late_events_side_output(), 1);
    }

    #[test]
    fn test_late_data_metrics_reset() {
        let mut metrics = LateDataMetrics::new();

        metrics.record_dropped();
        metrics.record_side_output();

        assert_eq!(metrics.late_events_total(), 2);

        metrics.reset();

        assert_eq!(metrics.late_events_total(), 0);
        assert_eq!(metrics.late_events_dropped(), 0);
        assert_eq!(metrics.late_events_side_output(), 0);
    }

    #[test]
    fn test_window_operator_set_late_data_config() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(100),
            "test_op".to_string(),
        );

        // Default is drop
        assert!(operator.late_data_config().should_drop());

        // Set to side output
        operator.set_late_data_config(LateDataConfig::with_side_output("late".to_string()));
        assert!(!operator.late_data_config().should_drop());
        assert_eq!(operator.late_data_config().side_output(), Some("late"));
    }

    #[test]
    fn test_late_event_dropped_without_side_output() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0), // No allowed lateness
            "test_op".to_string(),
        );

        // Default: drop late events
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        // Use a watermark generator with high max lateness so watermarks advance quickly
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Process an event to advance the watermark to 1000
        let event1 = create_test_event(1000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process a late event (timestamp 500 when watermark is at 1000)
        let late_event = create_test_event(500, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should emit a LateEvent (dropped)
        assert!(!outputs.is_empty());
        let is_late_event = outputs.iter().any(|o| matches!(o, Output::LateEvent(_)));
        assert!(is_late_event, "Expected LateEvent output");

        // Metrics should show dropped
        assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
        assert_eq!(operator.late_data_metrics().late_events_side_output(), 0);
    }

    #[test]
    fn test_late_event_routed_to_side_output() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0), // No allowed lateness
            "test_op".to_string(),
        );

        // Configure side output for late events
        operator.set_late_data_config(LateDataConfig::with_side_output("late_events".to_string()));

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Process an event to advance the watermark to 1000
        let event1 = create_test_event(1000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process a late event
        let late_event = create_test_event(500, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should emit a SideOutput
        assert!(!outputs.is_empty());
        let side_output = outputs.iter().find_map(|o| {
            if let Output::SideOutput { name, .. } = o {
                Some(name.clone())
            } else {
                None
            }
        });
        assert_eq!(side_output, Some("late_events".to_string()));

        // Metrics should show side output
        assert_eq!(operator.late_data_metrics().late_events_dropped(), 0);
        assert_eq!(operator.late_data_metrics().late_events_side_output(), 1);
    }

    #[test]
    fn test_event_within_lateness_not_late() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(500), // 500ms allowed lateness
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Process an event to advance the watermark to 1200
        // This would close window [0, 1000) at time 1000 + 0 (no lateness from watermark gen)
        // But with 500ms allowed lateness, window cleanup is at 1500
        let event1 = create_test_event(1200, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process an event for window [0, 1000) at timestamp 800
        // Watermark is at 1200, window cleanup time is 1000 + 500 = 1500
        // Since 1200 < 1500, the event should NOT be late
        let event2 = create_test_event(800, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx)
        };

        // Should NOT be a late event - should be processed normally
        let is_late_event = outputs.iter().any(|o| matches!(o, Output::LateEvent(_) | Output::SideOutput { .. }));
        assert!(!is_late_event, "Event within lateness period should not be marked as late");

        // No late events recorded
        assert_eq!(operator.late_data_metrics().late_events_total(), 0);
    }

    #[test]
    fn test_reset_late_data_metrics() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Generate a late event
        let event1 = create_test_event(1000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }
        let late_event = create_test_event(500, 2);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx);
        }

        assert_eq!(operator.late_data_metrics().late_events_total(), 1);

        // Reset metrics
        operator.reset_late_data_metrics();

        assert_eq!(operator.late_data_metrics().late_events_total(), 0);
    }

    // ==================== F011B Tests ====================

    #[test]
    fn test_emit_strategy_helper_methods() {
        // OnWatermark
        assert!(!EmitStrategy::OnWatermark.emits_intermediate());
        assert!(!EmitStrategy::OnWatermark.requires_changelog());
        assert!(!EmitStrategy::OnWatermark.is_append_only_compatible());
        assert!(EmitStrategy::OnWatermark.generates_retractions());
        assert!(!EmitStrategy::OnWatermark.suppresses_intermediate());
        assert!(!EmitStrategy::OnWatermark.drops_late_data());

        // OnUpdate
        assert!(EmitStrategy::OnUpdate.emits_intermediate());
        assert!(!EmitStrategy::OnUpdate.requires_changelog());
        assert!(!EmitStrategy::OnUpdate.is_append_only_compatible());
        assert!(EmitStrategy::OnUpdate.generates_retractions());
        assert!(!EmitStrategy::OnUpdate.suppresses_intermediate());

        // Periodic
        let periodic = EmitStrategy::Periodic(Duration::from_secs(10));
        assert!(periodic.emits_intermediate());
        assert!(!periodic.requires_changelog());
        assert!(!periodic.is_append_only_compatible());
        assert!(!periodic.generates_retractions());
        assert!(!periodic.suppresses_intermediate());

        // OnWindowClose (F011B)
        assert!(!EmitStrategy::OnWindowClose.emits_intermediate());
        assert!(!EmitStrategy::OnWindowClose.requires_changelog());
        assert!(EmitStrategy::OnWindowClose.is_append_only_compatible());
        assert!(!EmitStrategy::OnWindowClose.generates_retractions());
        assert!(EmitStrategy::OnWindowClose.suppresses_intermediate());
        assert!(!EmitStrategy::OnWindowClose.drops_late_data());

        // Changelog (F011B)
        assert!(!EmitStrategy::Changelog.emits_intermediate());
        assert!(EmitStrategy::Changelog.requires_changelog());
        assert!(!EmitStrategy::Changelog.is_append_only_compatible());
        assert!(EmitStrategy::Changelog.generates_retractions());
        assert!(!EmitStrategy::Changelog.suppresses_intermediate());

        // Final (F011B)
        assert!(!EmitStrategy::Final.emits_intermediate());
        assert!(!EmitStrategy::Final.requires_changelog());
        assert!(EmitStrategy::Final.is_append_only_compatible());
        assert!(!EmitStrategy::Final.generates_retractions());
        assert!(EmitStrategy::Final.suppresses_intermediate());
        assert!(EmitStrategy::Final.drops_late_data());
    }

    #[test]
    fn test_cdc_operation() {
        assert_eq!(CdcOperation::Insert.weight(), 1);
        assert_eq!(CdcOperation::Delete.weight(), -1);
        assert_eq!(CdcOperation::UpdateBefore.weight(), -1);
        assert_eq!(CdcOperation::UpdateAfter.weight(), 1);

        assert!(CdcOperation::Insert.is_insert());
        assert!(CdcOperation::UpdateAfter.is_insert());
        assert!(!CdcOperation::Delete.is_insert());
        assert!(!CdcOperation::UpdateBefore.is_insert());

        assert!(CdcOperation::Delete.is_delete());
        assert!(CdcOperation::UpdateBefore.is_delete());
        assert!(!CdcOperation::Insert.is_delete());
        assert!(!CdcOperation::UpdateAfter.is_delete());

        assert_eq!(CdcOperation::Insert.debezium_op(), 'c');
        assert_eq!(CdcOperation::Delete.debezium_op(), 'd');
        assert_eq!(CdcOperation::UpdateBefore.debezium_op(), 'u');
        assert_eq!(CdcOperation::UpdateAfter.debezium_op(), 'u');
    }

    #[test]
    fn test_changelog_record_insert() {
        let event = create_test_event(1000, 42);
        let record = ChangelogRecord::insert(event.clone(), 2000);

        assert_eq!(record.operation, CdcOperation::Insert);
        assert_eq!(record.weight, 1);
        assert_eq!(record.emit_timestamp, 2000);
        assert_eq!(record.event.timestamp, 1000);
        assert!(record.is_insert());
        assert!(!record.is_delete());
    }

    #[test]
    fn test_changelog_record_delete() {
        let event = create_test_event(1000, 42);
        let record = ChangelogRecord::delete(event.clone(), 2000);

        assert_eq!(record.operation, CdcOperation::Delete);
        assert_eq!(record.weight, -1);
        assert_eq!(record.emit_timestamp, 2000);
        assert!(record.is_delete());
        assert!(!record.is_insert());
    }

    #[test]
    fn test_changelog_record_update() {
        let old_event = create_test_event(1000, 10);
        let new_event = create_test_event(1000, 20);
        let (before, after) = ChangelogRecord::update(old_event, new_event, 2000);

        assert_eq!(before.operation, CdcOperation::UpdateBefore);
        assert_eq!(before.weight, -1);
        assert!(before.is_delete());

        assert_eq!(after.operation, CdcOperation::UpdateAfter);
        assert_eq!(after.weight, 1);
        assert!(after.is_insert());
    }

    #[test]
    fn test_emit_strategy_on_window_close() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set OnWindowClose strategy
        operator.set_emit_strategy(EmitStrategy::OnWindowClose);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events - should NOT emit intermediate results
        let event1 = create_test_event(100, 1);
        let event2 = create_test_event(200, 2);

        let outputs1 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx)
        };
        let outputs2 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx)
        };

        // No Event outputs (only watermark updates)
        let event_outputs1: Vec<_> = outputs1
            .iter()
            .filter(|o| matches!(o, Output::Event(_)))
            .collect();
        let event_outputs2: Vec<_> = outputs2
            .iter()
            .filter(|o| matches!(o, Output::Event(_)))
            .collect();

        assert!(event_outputs1.is_empty(), "OnWindowClose should not emit intermediate results");
        assert!(event_outputs2.is_empty(), "OnWindowClose should not emit intermediate results");
    }

    #[test]
    fn test_emit_strategy_final_drops_late_data() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set Final strategy
        operator.set_emit_strategy(EmitStrategy::Final);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark past first window
        let event1 = create_test_event(1500, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Send late event - should be silently dropped (no LateEvent output)
        let late_event = create_test_event(500, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should have NO output at all (silently dropped)
        assert!(outputs.is_empty(), "EMIT FINAL should silently drop late data");
        assert_eq!(
            operator.late_data_metrics().late_events_dropped(),
            1,
            "Late event should be recorded as dropped"
        );
    }

    #[test]
    fn test_emit_strategy_changelog_emits_records() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set Changelog strategy
        operator.set_emit_strategy(EmitStrategy::Changelog);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process event - Changelog emits on every update
        let event = create_test_event(100, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };

        // Should emit Changelog record
        let changelog_outputs: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, Output::Changelog(_)))
            .collect();

        assert_eq!(
            changelog_outputs.len(),
            1,
            "Changelog strategy should emit changelog record on update"
        );

        if let Output::Changelog(record) = &changelog_outputs[0] {
            assert_eq!(record.operation, CdcOperation::Insert);
            assert_eq!(record.weight, 1);
        } else {
            panic!("Expected Changelog output");
        }
    }

    #[test]
    fn test_emit_strategy_changelog_on_timer() {
        let assigner = TumblingWindowAssigner::from_millis(1000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Set Changelog strategy
        operator.set_emit_strategy(EmitStrategy::Changelog);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events to populate window
        let event = create_test_event(100, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Trigger window timer - should emit Changelog
        let timer = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };

        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(timer, &mut ctx)
        };

        // Should emit Changelog record on final emission
        let changelog_outputs: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, Output::Changelog(_)))
            .collect();

        assert_eq!(
            changelog_outputs.len(),
            1,
            "Changelog strategy should emit changelog record on timer"
        );

        if let Output::Changelog(record) = &changelog_outputs[0] {
            assert_eq!(record.operation, CdcOperation::Insert);
        } else {
            panic!("Expected Changelog output");
        }
    }
}
