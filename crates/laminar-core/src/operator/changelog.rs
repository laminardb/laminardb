//! # Changelog and Retraction Support
//!
//! Z-set style changelog records with integer weights for incremental computation.
//! This is the foundation for exactly-once sinks, cascading materialized views,
//! and CDC connectors.
//!
//! ## Key Concepts
//!
//! - **Z-sets**: Elements have integer weights. Weight > 0 → insert, weight < 0 → delete.
//! - **Retraction**: Emitting (-old, +new) pairs to correct previous results.
//! - **CDC Envelope**: Debezium-compatible format for downstream systems.
//!
//! ## Ring Architecture
//!
//! - **Ring 0**: `ChangelogRef` and `ChangelogBuffer` for zero-allocation hot path
//! - **Ring 1**: `LateDataRetractionGenerator` and `CdcEnvelope` serialization
//! - **Ring 2**: Changelog configuration and CDC format selection
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::changelog::{
//!     ChangelogBuffer, ChangelogRef, RetractableCountAccumulator,
//!     RetractableAccumulator, CdcEnvelope, CdcSource,
//! };
//! use laminar_core::operator::window::CdcOperation;
//!
//! // Ring 0: Zero-allocation changelog tracking
//! let mut buffer = ChangelogBuffer::with_capacity(1024);
//! buffer.push(ChangelogRef::insert(0, 0));
//! buffer.push(ChangelogRef::delete(0, 1));
//!
//! // Ring 1: Retractable aggregation
//! let mut agg = RetractableCountAccumulator::default();
//! agg.add(());
//! agg.add(());
//! assert_eq!(agg.result(), 2);
//! agg.retract(&());
//! assert_eq!(agg.result(), 1);
//!
//! // CDC envelope for sinks
//! let source = CdcSource::new("laminardb", "default", "orders");
//! let envelope = CdcEnvelope::insert(serde_json::json!({"id": 1}), source, 1000);
//! ```

use super::window::{CdcOperation, WindowId};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

// Ring 0: Zero-Allocation Types

/// Zero-allocation changelog reference for Ring 0 hot path.
///
/// Instead of allocating a full `ChangelogRecord`, this stores
/// offsets into the event batch with the operation type.
///
/// Size: 12 bytes (u32 + u32 + i16 + u8 + padding)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ChangelogRef {
    /// Offset into the current batch
    pub batch_offset: u32,
    /// Row index within the batch
    pub row_index: u32,
    /// Z-set weight (+1 or -1)
    pub weight: i16,
    /// Operation type (stored as u8 for compactness)
    operation_raw: u8,
}

impl ChangelogRef {
    /// Creates a new changelog reference.
    #[inline]
    #[must_use]
    pub fn new(batch_offset: u32, row_index: u32, weight: i16, operation: CdcOperation) -> Self {
        Self {
            batch_offset,
            row_index,
            weight,
            operation_raw: operation.to_u8(),
        }
    }

    /// Creates an insert reference.
    #[inline]
    #[must_use]
    pub fn insert(batch_offset: u32, row_index: u32) -> Self {
        Self::new(batch_offset, row_index, 1, CdcOperation::Insert)
    }

    /// Creates a delete reference.
    #[inline]
    #[must_use]
    pub fn delete(batch_offset: u32, row_index: u32) -> Self {
        Self::new(batch_offset, row_index, -1, CdcOperation::Delete)
    }

    /// Creates an update-before reference (retraction).
    #[inline]
    #[must_use]
    pub fn update_before(batch_offset: u32, row_index: u32) -> Self {
        Self::new(batch_offset, row_index, -1, CdcOperation::UpdateBefore)
    }

    /// Creates an update-after reference.
    #[inline]
    #[must_use]
    pub fn update_after(batch_offset: u32, row_index: u32) -> Self {
        Self::new(batch_offset, row_index, 1, CdcOperation::UpdateAfter)
    }

    /// Returns the CDC operation type.
    #[inline]
    #[must_use]
    pub fn operation(&self) -> CdcOperation {
        CdcOperation::from_u8(self.operation_raw)
    }

    /// Returns true if this is an insert-type operation.
    #[inline]
    #[must_use]
    pub fn is_insert(&self) -> bool {
        self.weight > 0
    }

    /// Returns true if this is a delete-type operation.
    #[inline]
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.weight < 0
    }
}

/// Ring 0 changelog buffer (pre-allocated, reused per epoch).
///
/// This buffer stores changelog references without allocating on the hot path
/// (after initial warmup). When the buffer is full, it signals backpressure.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::changelog::{ChangelogBuffer, ChangelogRef};
///
/// let mut buffer = ChangelogBuffer::with_capacity(1024);
///
/// // Push references (no allocation after warmup)
/// for i in 0..100 {
///     buffer.push(ChangelogRef::insert(i, 0));
/// }
///
/// // Drain for Ring 1 processing
/// for changelog_ref in buffer.drain() {
///     // Process in Ring 1
/// }
/// ```
pub struct ChangelogBuffer {
    /// Pre-allocated changelog references
    refs: Vec<ChangelogRef>,
    /// Current write position
    len: usize,
    /// Capacity
    capacity: usize,
}

impl ChangelogBuffer {
    /// Creates a new buffer with the given capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut refs = Vec::with_capacity(capacity);
        // Pre-warm the buffer to avoid allocations during hot path
        refs.resize(
            capacity,
            ChangelogRef {
                batch_offset: 0,
                row_index: 0,
                weight: 0,
                operation_raw: 0,
            },
        );
        Self {
            refs,
            len: 0,
            capacity,
        }
    }

    /// Pushes a changelog reference (no allocation if under capacity).
    ///
    /// Returns `true` if the reference was added, `false` if buffer is full
    /// (backpressure signal).
    #[inline]
    pub fn push(&mut self, changelog_ref: ChangelogRef) -> bool {
        if self.len < self.capacity {
            self.refs[self.len] = changelog_ref;
            self.len += 1;
            true
        } else {
            false // Buffer full - backpressure signal
        }
    }

    /// Pushes a retraction pair (update-before, update-after).
    ///
    /// Returns `true` if both references were added, `false` if buffer is full.
    #[inline]
    pub fn push_retraction(
        &mut self,
        batch_offset: u32,
        old_row_index: u32,
        new_row_index: u32,
    ) -> bool {
        if self.len + 2 <= self.capacity {
            self.refs[self.len] = ChangelogRef::update_before(batch_offset, old_row_index);
            self.refs[self.len + 1] = ChangelogRef::update_after(batch_offset, new_row_index);
            self.len += 2;
            true
        } else {
            false
        }
    }

    /// Drains references for Ring 1 processing.
    ///
    /// After draining, the buffer is empty but retains its capacity.
    pub fn drain(&mut self) -> impl Iterator<Item = ChangelogRef> + '_ {
        let len = self.len;
        self.len = 0;
        self.refs[..len].iter().copied()
    }

    /// Returns current count of references.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns true if the buffer is full.
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    /// Returns the buffer capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns available space in the buffer.
    #[inline]
    #[must_use]
    pub fn available(&self) -> usize {
        self.capacity.saturating_sub(self.len)
    }

    /// Clears the buffer without deallocating.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Returns a slice of the current references.
    #[must_use]
    pub fn as_slice(&self) -> &[ChangelogRef] {
        &self.refs[..self.len]
    }
}

impl Default for ChangelogBuffer {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

// Retractable Aggregators

/// Extension trait for accumulators that support retractions.
///
/// Retractable accumulators can "un-apply" a value, which is essential for:
/// - Late data corrections (emit -old, +new pairs)
/// - Cascading materialized views
/// - Changelog-based downstream consumers
///
/// # Retraction Efficiency
///
/// Some aggregators support O(1) retraction (count, sum, avg), while others
/// may require O(n) recomputation (min, max without value tracking).
/// Use `supports_efficient_retraction()` to check.
pub trait RetractableAccumulator: Default + Clone + Send {
    /// The input type for the aggregation.
    type Input;
    /// The output type produced by the aggregation.
    type Output;

    /// Adds a value to the accumulator.
    fn add(&mut self, value: Self::Input);

    /// Retracts (un-applies) a value from the accumulator.
    ///
    /// This is the inverse of `add`. For example:
    /// - Count: decrement by 1
    /// - Sum: subtract the value
    /// - Avg: update sum and count
    fn retract(&mut self, value: &Self::Input);

    /// Merges another accumulator into this one.
    fn merge(&mut self, other: &Self);

    /// Extracts the final result from the accumulator.
    fn result(&self) -> Self::Output;

    /// Returns true if the accumulator is empty (no values added).
    fn is_empty(&self) -> bool;

    /// Returns true if this accumulator can efficiently retract.
    ///
    /// Some aggregators (like Min/Max without value tracking) may need to
    /// scan all values on retraction if the retracted value was the current
    /// min/max.
    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    /// Resets the accumulator to its initial state.
    fn reset(&mut self);
}

/// Retractable count accumulator.
///
/// Uses signed integer to support negative counts from retractions.
/// In a correct pipeline, the count should never go negative.
#[derive(Debug, Clone, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RetractableCountAccumulator {
    /// Signed count to support retraction
    count: i64,
}

impl RetractableCountAccumulator {
    /// Creates a new count accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current count (may be negative during retraction).
    #[must_use]
    pub fn count(&self) -> i64 {
        self.count
    }
}

impl RetractableAccumulator for RetractableCountAccumulator {
    type Input = ();
    type Output = i64;

    #[inline]
    fn add(&mut self, _value: ()) {
        self.count += 1;
    }

    #[inline]
    fn retract(&mut self, _value: &()) {
        self.count -= 1;
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }

    fn result(&self) -> i64 {
        self.count
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

/// Retractable sum accumulator.
///
/// Supports O(1) retraction by simple subtraction.
#[derive(Debug, Clone, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RetractableSumAccumulator {
    /// Running sum (signed)
    sum: i64,
    /// Count of values for `is_empty` check
    count: i64,
}

impl RetractableSumAccumulator {
    /// Creates a new sum accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current sum.
    #[must_use]
    pub fn sum(&self) -> i64 {
        self.sum
    }
}

impl RetractableAccumulator for RetractableSumAccumulator {
    type Input = i64;
    type Output = i64;

    #[inline]
    fn add(&mut self, value: i64) {
        self.sum += value;
        self.count += 1;
    }

    #[inline]
    fn retract(&mut self, value: &i64) {
        self.sum -= value;
        self.count -= 1;
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

    fn reset(&mut self) {
        self.sum = 0;
        self.count = 0;
    }
}

/// Retractable average accumulator.
///
/// Supports O(1) retraction by updating sum and count.
#[derive(Debug, Clone, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct RetractableAvgAccumulator {
    /// Running sum
    sum: i64,
    /// Count of values
    count: i64,
}

impl RetractableAvgAccumulator {
    /// Creates a new average accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current sum.
    #[must_use]
    pub fn sum(&self) -> i64 {
        self.sum
    }

    /// Returns the current count.
    #[must_use]
    pub fn count(&self) -> i64 {
        self.count
    }
}

impl RetractableAccumulator for RetractableAvgAccumulator {
    type Input = i64;
    type Output = Option<f64>;

    #[inline]
    fn add(&mut self, value: i64) {
        self.sum += value;
        self.count += 1;
    }

    #[inline]
    fn retract(&mut self, value: &i64) {
        self.sum -= value;
        self.count -= 1;
    }

    fn merge(&mut self, other: &Self) {
        self.sum += other.sum;
        self.count += other.count;
    }

    #[allow(clippy::cast_precision_loss)]
    fn result(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum as f64 / self.count as f64)
        } else {
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn reset(&mut self) {
        self.sum = 0;
        self.count = 0;
    }
}

/// Retractable min accumulator with counted value tracking.
///
/// Uses a `BTreeMap<i64, usize>` (value → count) for O(log n) insert/remove
/// and O(1) min via `first_key_value()`. Bounded memory proportional to
/// distinct values, not total event count.
#[derive(Debug, Clone, Default)]
pub struct RetractableMinAccumulator {
    /// Value → occurrence count (sorted for O(1) min access)
    counts: std::collections::BTreeMap<i64, usize>,
}

impl RetractableMinAccumulator {
    /// Creates a new min accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RetractableAccumulator for RetractableMinAccumulator {
    type Input = i64;
    type Output = Option<i64>;

    fn add(&mut self, value: i64) {
        *self.counts.entry(value).or_insert(0) += 1;
    }

    fn retract(&mut self, value: &i64) {
        if let Some(count) = self.counts.get_mut(value) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(value);
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&val, &cnt) in &other.counts {
            *self.counts.entry(val).or_insert(0) += cnt;
        }
    }

    fn result(&self) -> Option<i64> {
        self.counts.keys().next().copied()
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.counts.clear();
    }
}

/// Retractable max accumulator with counted value tracking.
///
/// Uses a `BTreeMap<i64, usize>` (value → count) for O(log n) insert/remove
/// and O(1) max via `last_key_value()`. Bounded memory proportional to
/// distinct values, not total event count.
#[derive(Debug, Clone, Default)]
pub struct RetractableMaxAccumulator {
    /// Value → occurrence count (sorted for O(1) max access)
    counts: std::collections::BTreeMap<i64, usize>,
}

impl RetractableMaxAccumulator {
    /// Creates a new max accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RetractableAccumulator for RetractableMaxAccumulator {
    type Input = i64;
    type Output = Option<i64>;

    fn add(&mut self, value: i64) {
        *self.counts.entry(value).or_insert(0) += 1;
    }

    fn retract(&mut self, value: &i64) {
        if let Some(count) = self.counts.get_mut(value) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(value);
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&val, &cnt) in &other.counts {
            *self.counts.entry(val).or_insert(0) += cnt;
        }
    }

    fn result(&self) -> Option<i64> {
        self.counts.keys().next_back().copied()
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.counts.clear();
    }
}

// Late Data Retraction Generator

/// Tracks previously emitted results for generating late data retractions.
#[derive(Debug, Clone)]
struct EmittedResult {
    /// The emitted data (serialized for comparison)
    data: Vec<u8>,
    /// Timestamp when emitted
    emit_time: i64,
    /// Number of times re-emitted (for metrics)
    version: u32,
}

/// Generates retractions for late data corrections.
///
/// When late data arrives and updates an already-emitted window result,
/// this generator produces:
/// 1. A retraction (-1 weight) for the old result
/// 2. An insert (+1 weight) for the new result
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::changelog::LateDataRetractionGenerator;
/// use laminar_core::operator::window::WindowId;
///
/// let mut gen = LateDataRetractionGenerator::new(true);
/// let window_id = WindowId::new(0, 60000);
///
/// // First emission - no retraction needed
/// let result1 = gen.check_retraction(&window_id, b"count=5", 1000);
/// assert!(result1.is_none());
///
/// // Late data changes result - generates retraction
/// let result2 = gen.check_retraction(&window_id, b"count=7", 2000);
/// assert!(result2.is_some());
/// let (old, new) = result2.unwrap();
/// assert_eq!(old.as_slice(), b"count=5");
/// assert_eq!(new.as_slice(), b"count=7");
///
/// // Same result - no retraction
/// let result3 = gen.check_retraction(&window_id, b"count=7", 3000);
/// assert!(result3.is_none());
/// ```
pub struct LateDataRetractionGenerator {
    /// Previously emitted results (for generating retractions)
    emitted_results: FxHashMap<WindowId, EmittedResult>,
    /// Whether retraction generation is enabled
    enabled: bool,
    /// Metrics: total retractions generated
    retractions_generated: u64,
    /// Metrics: total windows tracked
    windows_tracked: u64,
}

impl LateDataRetractionGenerator {
    /// Creates a new generator.
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            emitted_results: FxHashMap::default(),
            enabled,
            retractions_generated: 0,
            windows_tracked: 0,
        }
    }

    /// Creates a disabled generator (no-op).
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(false)
    }

    /// Returns true if retraction generation is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enables or disables retraction generation.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Checks if we need to generate a retraction for this window.
    ///
    /// Returns `Some((old_data, new_data))` if the window was previously
    /// emitted with different data. Returns `None` if this is the first
    /// emission or the data hasn't changed.
    pub fn check_retraction(
        &mut self,
        window_id: &WindowId,
        new_data: &[u8],
        timestamp: i64,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.enabled {
            return None;
        }

        if let Some(prev) = self.emitted_results.get_mut(window_id) {
            if prev.data != new_data {
                let old_data = std::mem::replace(&mut prev.data, new_data.to_vec());
                prev.emit_time = timestamp;
                prev.version += 1;
                self.retractions_generated += 1;
                return Some((old_data, new_data.to_vec()));
            }
        } else {
            self.emitted_results.insert(
                *window_id,
                EmittedResult {
                    data: new_data.to_vec(),
                    emit_time: timestamp,
                    version: 1,
                },
            );
            self.windows_tracked += 1;
        }

        None
    }

    /// Checks for retraction and returns borrowed slices (avoiding allocation
    /// when no retraction is needed).
    ///
    /// Returns `Some(old_data)` if retraction is needed. The caller should
    /// then emit the retraction for `old_data` and insert for `new_data`.
    pub fn check_retraction_ref(
        &mut self,
        window_id: &WindowId,
        new_data: &[u8],
        timestamp: i64,
    ) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }

        if let Some(prev) = self.emitted_results.get_mut(window_id) {
            if prev.data != new_data {
                let old_data = std::mem::replace(&mut prev.data, new_data.to_vec());
                prev.emit_time = timestamp;
                prev.version += 1;
                self.retractions_generated += 1;
                return Some(old_data);
            }
        } else {
            self.emitted_results.insert(
                *window_id,
                EmittedResult {
                    data: new_data.to_vec(),
                    emit_time: timestamp,
                    version: 1,
                },
            );
            self.windows_tracked += 1;
        }

        None
    }

    /// Cleans up state for closed windows.
    ///
    /// Call this when a window is closed to prevent unbounded memory growth.
    pub fn cleanup_window(&mut self, window_id: &WindowId) {
        self.emitted_results.remove(window_id);
    }

    /// Cleans up state for windows that ended before the given watermark.
    ///
    /// This should be called periodically to bound memory usage.
    pub fn cleanup_before_watermark(&mut self, watermark: i64) {
        self.emitted_results
            .retain(|window_id, _| window_id.end > watermark);
    }

    /// Returns the number of retractions generated.
    #[must_use]
    pub fn retractions_generated(&self) -> u64 {
        self.retractions_generated
    }

    /// Returns the number of windows currently being tracked.
    #[must_use]
    pub fn windows_tracked(&self) -> usize {
        self.emitted_results.len()
    }

    /// Resets all metrics.
    pub fn reset_metrics(&mut self) {
        self.retractions_generated = 0;
        self.windows_tracked = 0;
    }

    /// Clears all tracked state.
    pub fn clear(&mut self) {
        self.emitted_results.clear();
        self.reset_metrics();
    }
}

impl Default for LateDataRetractionGenerator {
    fn default() -> Self {
        Self::new(true)
    }
}

// CDC Envelope (Debezium-Compatible)

/// Source metadata for CDC envelope.
///
/// Contains information about the origin of the change event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcSource {
    /// Source name (e.g., "laminardb")
    pub name: String,
    /// Database/schema name
    pub db: String,
    /// Table/view name
    pub table: String,
    /// Sequence number for ordering
    #[serde(default)]
    pub sequence: u64,
}

impl CdcSource {
    /// Creates a new CDC source.
    #[must_use]
    pub fn new(name: impl Into<String>, db: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            db: db.into(),
            table: table.into(),
            sequence: 0,
        }
    }

    /// Creates a new CDC source with sequence number.
    #[must_use]
    pub fn with_sequence(
        name: impl Into<String>,
        db: impl Into<String>,
        table: impl Into<String>,
        sequence: u64,
    ) -> Self {
        Self {
            name: name.into(),
            db: db.into(),
            table: table.into(),
            sequence,
        }
    }

    /// Increments and returns the sequence number.
    pub fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }
}

/// CDC envelope for sink serialization.
///
/// Compatible with Debezium envelope format for interoperability with
/// downstream systems (Kafka Connect, data lakes, etc.).
///
/// # Debezium Operation Codes
///
/// - `"c"`: Create (insert)
/// - `"u"`: Update
/// - `"d"`: Delete
/// - `"r"`: Read (snapshot)
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::changelog::{CdcEnvelope, CdcSource};
/// use serde_json::json;
///
/// let source = CdcSource::new("laminardb", "default", "orders");
///
/// // Insert
/// let insert = CdcEnvelope::insert(json!({"id": 1, "amount": 100}), source.clone(), 1000);
/// assert_eq!(insert.op, "c");
///
/// // Delete
/// let delete = CdcEnvelope::delete(json!({"id": 1}), source.clone(), 2000);
/// assert_eq!(delete.op, "d");
///
/// // Update
/// let update = CdcEnvelope::update(
///     json!({"id": 1, "amount": 100}),
///     json!({"id": 1, "amount": 150}),
///     source,
///     3000,
/// );
/// assert_eq!(update.op, "u");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEnvelope<T> {
    /// Operation type: "c" (create), "u" (update), "d" (delete), "r" (read/snapshot)
    pub op: String,
    /// Timestamp in milliseconds since epoch
    pub ts_ms: i64,
    /// Source metadata
    pub source: CdcSource,
    /// Value before change (for updates/deletes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<T>,
    /// Value after change (for inserts/updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<T>,
}

impl<T> CdcEnvelope<T> {
    /// Creates an insert (create) envelope.
    #[must_use]
    pub fn insert(after: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "c".to_string(),
            ts_ms,
            source,
            before: None,
            after: Some(after),
        }
    }

    /// Creates a delete envelope.
    #[must_use]
    pub fn delete(before: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "d".to_string(),
            ts_ms,
            source,
            before: Some(before),
            after: None,
        }
    }

    /// Creates an update envelope.
    #[must_use]
    pub fn update(before: T, after: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "u".to_string(),
            ts_ms,
            source,
            before: Some(before),
            after: Some(after),
        }
    }

    /// Creates a read (snapshot) envelope.
    #[must_use]
    pub fn read(after: T, source: CdcSource, ts_ms: i64) -> Self {
        Self {
            op: "r".to_string(),
            ts_ms,
            source,
            before: None,
            after: Some(after),
        }
    }

    /// Returns true if this is an insert operation.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        self.op == "c"
    }

    /// Returns true if this is a delete operation.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.op == "d"
    }

    /// Returns true if this is an update operation.
    #[must_use]
    pub fn is_update(&self) -> bool {
        self.op == "u"
    }

    /// Returns the Z-set weight for this operation.
    ///
    /// - Insert/Read: +1
    /// - Delete: -1
    /// - Update: 0 (net effect of -1 for before + +1 for after)
    #[must_use]
    pub fn weight(&self) -> i32 {
        match self.op.as_str() {
            "c" | "r" => 1,
            "d" => -1,
            // "u" (update) and unknown operations have net weight of 0
            _ => 0,
        }
    }
}

impl<T: Serialize> CdcEnvelope<T> {
    /// Serializes the envelope to JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Serializes the envelope to pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Serializes the envelope to JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

// Retractable FIRST/LAST Accumulators

/// Retractable `FIRST_VALUE` accumulator for changelog/retraction mode.
///
/// Stores all `(timestamp, value)` entries sorted by timestamp ascending.
/// On retraction, removes the entry and recomputes the first value.
/// This is necessary for `EMIT CHANGES` with OHLC queries where the
/// open price may need to be retracted.
///
/// # Ring Architecture
///
/// This is a Ring 1 structure (allocates). Ring 0 uses the non-retractable
/// [`super::window::FirstValueAccumulator`] via the static dispatch path.
///
/// Uses `BTreeMap<i64, Vec<i64>>` (timestamp → values) for O(log n) insert/remove
/// and O(1) first-value access via `first_key_value()`.
#[derive(Debug, Clone, Default)]
pub struct RetractableFirstValueAccumulator {
    /// Timestamp to values at that timestamp (`BTreeMap` for O(log n) ops)
    entries: std::collections::BTreeMap<i64, Vec<i64>>,
    /// Total entry count across all timestamps
    total_count: usize,
}

impl RetractableFirstValueAccumulator {
    /// Creates a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Returns true if no entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

impl RetractableAccumulator for RetractableFirstValueAccumulator {
    type Input = (i64, i64); // (timestamp, value)
    type Output = Option<i64>;

    fn add(&mut self, (timestamp, value): (i64, i64)) {
        self.entries.entry(timestamp).or_default().push(value);
        self.total_count += 1;
    }

    fn retract(&mut self, (timestamp, value): &(i64, i64)) {
        if let Some(values) = self.entries.get_mut(timestamp) {
            if let Some(pos) = values.iter().position(|v| v == value) {
                values.swap_remove(pos);
                self.total_count -= 1;
                if values.is_empty() {
                    self.entries.remove(timestamp);
                }
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&ts, vals) in &other.entries {
            self.entries.entry(ts).or_default().extend(vals);
        }
        self.total_count += other.total_count;
    }

    fn result(&self) -> Option<i64> {
        self.entries
            .values()
            .next()
            .and_then(|vals| vals.first().copied())
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.total_count = 0;
    }
}

/// Retractable `LAST_VALUE` accumulator for changelog/retraction mode.
///
/// Uses `BTreeMap<i64, Vec<i64>>` (timestamp → values) for O(log n) insert/remove
/// and O(1) last-value access via `last_key_value()`.
#[derive(Debug, Clone, Default)]
pub struct RetractableLastValueAccumulator {
    /// Timestamp to values at that timestamp (`BTreeMap` for O(log n) ops)
    entries: std::collections::BTreeMap<i64, Vec<i64>>,
    /// Total entry count across all timestamps
    total_count: usize,
}

impl RetractableLastValueAccumulator {
    /// Creates a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Returns true if no entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }
}

impl RetractableAccumulator for RetractableLastValueAccumulator {
    type Input = (i64, i64); // (timestamp, value)
    type Output = Option<i64>;

    fn add(&mut self, (timestamp, value): (i64, i64)) {
        self.entries.entry(timestamp).or_default().push(value);
        self.total_count += 1;
    }

    fn retract(&mut self, (timestamp, value): &(i64, i64)) {
        if let Some(values) = self.entries.get_mut(timestamp) {
            if let Some(pos) = values.iter().position(|v| v == value) {
                values.swap_remove(pos);
                self.total_count -= 1;
                if values.is_empty() {
                    self.entries.remove(timestamp);
                }
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&ts, vals) in &other.entries {
            self.entries.entry(ts).or_default().extend(vals);
        }
        self.total_count += other.total_count;
    }

    fn result(&self) -> Option<i64> {
        self.entries
            .values()
            .next_back()
            .and_then(|vals| vals.last().copied())
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.total_count = 0;
    }
}

/// Retractable `FIRST_VALUE` accumulator for f64 values.
///
/// Uses `f64::to_bits()` / `f64::from_bits()` for lossless i64 storage.
/// Uses `BTreeMap<i64, Vec<i64>>` (timestamp to `value_bits`) for O(log n) ops.
#[derive(Debug, Clone, Default)]
pub struct RetractableFirstValueF64Accumulator {
    /// Timestamp to `value_bits` at that timestamp
    entries: std::collections::BTreeMap<i64, Vec<i64>>,
    /// Total entry count across all timestamps
    total_count: usize,
}

impl RetractableFirstValueF64Accumulator {
    /// Creates a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Returns true if no entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Returns the result as f64.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn result_f64(&self) -> Option<f64> {
        self.entries
            .values()
            .next()
            .and_then(|vals| vals.first())
            .map(|bits| f64::from_bits(*bits as u64))
    }
}

impl RetractableAccumulator for RetractableFirstValueF64Accumulator {
    type Input = (i64, f64); // (timestamp, value)
    type Output = Option<i64>; // value_bits for compatibility

    #[allow(clippy::cast_possible_wrap)]
    fn add(&mut self, (timestamp, value): (i64, f64)) {
        let value_bits = value.to_bits() as i64;
        self.entries.entry(timestamp).or_default().push(value_bits);
        self.total_count += 1;
    }

    fn retract(&mut self, (timestamp, value): &(i64, f64)) {
        #[allow(clippy::cast_possible_wrap)]
        let value_bits = value.to_bits() as i64;
        if let Some(values) = self.entries.get_mut(timestamp) {
            if let Some(pos) = values.iter().position(|v| *v == value_bits) {
                values.swap_remove(pos);
                self.total_count -= 1;
                if values.is_empty() {
                    self.entries.remove(timestamp);
                }
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&ts, vals) in &other.entries {
            self.entries.entry(ts).or_default().extend(vals);
        }
        self.total_count += other.total_count;
    }

    fn result(&self) -> Option<i64> {
        self.entries
            .values()
            .next()
            .and_then(|vals| vals.first().copied())
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.total_count = 0;
    }
}

/// Retractable `LAST_VALUE` accumulator for f64 values.
///
/// Uses `f64::to_bits()` / `f64::from_bits()` for lossless i64 storage.
/// Uses `BTreeMap<i64, Vec<i64>>` (timestamp to `value_bits`) for O(log n) ops.
#[derive(Debug, Clone, Default)]
pub struct RetractableLastValueF64Accumulator {
    /// Timestamp to `value_bits` at that timestamp
    entries: std::collections::BTreeMap<i64, Vec<i64>>,
    /// Total entry count across all timestamps
    total_count: usize,
}

impl RetractableLastValueF64Accumulator {
    /// Creates a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Returns true if no entries are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Returns the result as f64.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn result_f64(&self) -> Option<f64> {
        self.entries
            .values()
            .next_back()
            .and_then(|vals| vals.last())
            .map(|bits| f64::from_bits(*bits as u64))
    }
}

impl RetractableAccumulator for RetractableLastValueF64Accumulator {
    type Input = (i64, f64); // (timestamp, value)
    type Output = Option<i64>; // value_bits for compatibility

    #[allow(clippy::cast_possible_wrap)]
    fn add(&mut self, (timestamp, value): (i64, f64)) {
        let value_bits = value.to_bits() as i64;
        self.entries.entry(timestamp).or_default().push(value_bits);
        self.total_count += 1;
    }

    fn retract(&mut self, (timestamp, value): &(i64, f64)) {
        #[allow(clippy::cast_possible_wrap)]
        let value_bits = value.to_bits() as i64;
        if let Some(values) = self.entries.get_mut(timestamp) {
            if let Some(pos) = values.iter().position(|v| *v == value_bits) {
                values.swap_remove(pos);
                self.total_count -= 1;
                if values.is_empty() {
                    self.entries.remove(timestamp);
                }
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        for (&ts, vals) in &other.entries {
            self.entries.entry(ts).or_default().extend(vals);
        }
        self.total_count += other.total_count;
    }

    fn result(&self) -> Option<i64> {
        self.entries
            .values()
            .next_back()
            .and_then(|vals| vals.last().copied())
    }

    fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn supports_efficient_retraction(&self) -> bool {
        true
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.total_count = 0;
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    // ChangelogRef Tests

    #[test]
    fn test_changelog_ref_insert() {
        let cr = ChangelogRef::insert(10, 5);
        assert_eq!(cr.batch_offset, 10);
        assert_eq!(cr.row_index, 5);
        assert_eq!(cr.weight, 1);
        assert_eq!(cr.operation(), CdcOperation::Insert);
        assert!(cr.is_insert());
        assert!(!cr.is_delete());
    }

    #[test]
    fn test_changelog_ref_delete() {
        let cr = ChangelogRef::delete(20, 3);
        assert_eq!(cr.batch_offset, 20);
        assert_eq!(cr.row_index, 3);
        assert_eq!(cr.weight, -1);
        assert_eq!(cr.operation(), CdcOperation::Delete);
        assert!(!cr.is_insert());
        assert!(cr.is_delete());
    }

    #[test]
    fn test_changelog_ref_update() {
        let before = ChangelogRef::update_before(5, 1);
        let after = ChangelogRef::update_after(5, 2);

        assert_eq!(before.weight, -1);
        assert_eq!(after.weight, 1);
        assert_eq!(before.operation(), CdcOperation::UpdateBefore);
        assert_eq!(after.operation(), CdcOperation::UpdateAfter);
    }

    #[test]
    fn test_changelog_ref_size() {
        // Verify compact size
        assert!(std::mem::size_of::<ChangelogRef>() <= 16);
    }

    // ChangelogBuffer Tests

    #[test]
    fn test_changelog_buffer_basic() {
        let mut buffer = ChangelogBuffer::with_capacity(10);
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 10);

        assert!(buffer.push(ChangelogRef::insert(0, 0)));
        assert!(buffer.push(ChangelogRef::delete(1, 0)));

        assert_eq!(buffer.len(), 2);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_changelog_buffer_full() {
        let mut buffer = ChangelogBuffer::with_capacity(2);

        assert!(buffer.push(ChangelogRef::insert(0, 0)));
        assert!(buffer.push(ChangelogRef::insert(1, 0)));
        assert!(!buffer.push(ChangelogRef::insert(2, 0))); // Full

        assert!(buffer.is_full());
        assert_eq!(buffer.available(), 0);
    }

    #[test]
    fn test_changelog_buffer_drain() {
        let mut buffer = ChangelogBuffer::with_capacity(10);

        for i in 0..5 {
            buffer.push(ChangelogRef::insert(i, 0));
        }

        let drained: Vec<_> = buffer.drain().collect();
        assert_eq!(drained.len(), 5);
        assert!(buffer.is_empty());

        // Buffer can be reused
        for i in 0..3 {
            buffer.push(ChangelogRef::delete(i, 0));
        }
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_changelog_buffer_retraction() {
        let mut buffer = ChangelogBuffer::with_capacity(10);

        assert!(buffer.push_retraction(0, 1, 2));
        assert_eq!(buffer.len(), 2);

        let refs: Vec<_> = buffer.as_slice().to_vec();
        assert_eq!(refs[0].operation(), CdcOperation::UpdateBefore);
        assert_eq!(refs[0].row_index, 1);
        assert_eq!(refs[1].operation(), CdcOperation::UpdateAfter);
        assert_eq!(refs[1].row_index, 2);
    }

    #[test]
    fn test_changelog_buffer_zero_alloc_reuse() {
        let mut buffer = ChangelogBuffer::with_capacity(100);

        // First pass
        for i in 0..50 {
            buffer.push(ChangelogRef::insert(i, 0));
        }
        let _: Vec<_> = buffer.drain().collect();

        // Second pass - should not allocate
        for i in 0..50 {
            buffer.push(ChangelogRef::insert(i, 0));
        }

        assert_eq!(buffer.len(), 50);
    }

    // Retractable Accumulator Tests

    #[test]
    fn test_retractable_count() {
        let mut agg = RetractableCountAccumulator::default();

        agg.add(());
        agg.add(());
        agg.add(());
        assert_eq!(agg.result(), 3);

        agg.retract(&());
        assert_eq!(agg.result(), 2);

        agg.retract(&());
        agg.retract(&());
        assert_eq!(agg.result(), 0);
    }

    #[test]
    fn test_retractable_count_negative() {
        let mut agg = RetractableCountAccumulator::default();

        agg.add(());
        agg.retract(&());
        agg.retract(&()); // Extra retraction

        // Count can go negative (indicates an error in the pipeline)
        assert_eq!(agg.result(), -1);
    }

    #[test]
    fn test_retractable_sum() {
        let mut agg = RetractableSumAccumulator::default();

        agg.add(10);
        agg.add(20);
        agg.add(30);
        assert_eq!(agg.result(), 60);

        agg.retract(&20);
        assert_eq!(agg.result(), 40);

        agg.retract(&10);
        agg.retract(&30);
        assert_eq!(agg.result(), 0);
    }

    #[test]
    fn test_retractable_sum_merge() {
        let mut agg1 = RetractableSumAccumulator::default();
        agg1.add(10);
        agg1.add(20);

        let mut agg2 = RetractableSumAccumulator::default();
        agg2.add(30);
        agg2.retract(&5);

        agg1.merge(&agg2);
        assert_eq!(agg1.result(), 55); // 10 + 20 + 30 - 5
    }

    #[test]
    fn test_retractable_avg() {
        let mut agg = RetractableAvgAccumulator::default();

        agg.add(10);
        agg.add(20);
        agg.add(30);
        let avg = agg.result().unwrap();
        assert!((avg - 20.0).abs() < f64::EPSILON);

        agg.retract(&30);
        let avg = agg.result().unwrap();
        assert!((avg - 15.0).abs() < f64::EPSILON); // (10 + 20) / 2
    }

    #[test]
    fn test_retractable_avg_empty() {
        let mut agg = RetractableAvgAccumulator::default();
        assert!(agg.result().is_none());

        agg.add(10);
        agg.retract(&10);
        assert!(agg.result().is_none());
    }

    #[test]
    fn test_retractable_min() {
        let mut agg = RetractableMinAccumulator::default();

        agg.add(30);
        agg.add(10);
        agg.add(20);
        assert_eq!(agg.result(), Some(10));

        // Retract the minimum
        agg.retract(&10);
        assert_eq!(agg.result(), Some(20));

        // Retract a non-minimum
        agg.retract(&30);
        assert_eq!(agg.result(), Some(20));

        agg.retract(&20);
        assert_eq!(agg.result(), None);
    }

    #[test]
    fn test_retractable_max() {
        let mut agg = RetractableMaxAccumulator::default();

        agg.add(10);
        agg.add(30);
        agg.add(20);
        assert_eq!(agg.result(), Some(30));

        // Retract the maximum
        agg.retract(&30);
        assert_eq!(agg.result(), Some(20));

        agg.retract(&20);
        agg.retract(&10);
        assert_eq!(agg.result(), None);
    }

    #[test]
    fn test_retractable_efficiency_flags() {
        let count = RetractableCountAccumulator::default();
        let sum = RetractableSumAccumulator::default();
        let avg = RetractableAvgAccumulator::default();
        let min = RetractableMinAccumulator::default();
        let max = RetractableMaxAccumulator::default();

        // Count, sum, avg have O(1) retraction
        assert!(count.supports_efficient_retraction());
        assert!(sum.supports_efficient_retraction());
        assert!(avg.supports_efficient_retraction());

        // Min/max use BTreeMap — O(log n) retraction, no recomputation
        assert!(min.supports_efficient_retraction());
        assert!(max.supports_efficient_retraction());
    }

    // LateDataRetractionGenerator Tests

    #[test]
    fn test_late_data_retraction_first_emission() {
        let mut gen = LateDataRetractionGenerator::new(true);
        let window_id = WindowId::new(0, 60000);

        // First emission - no retraction
        let result = gen.check_retraction(&window_id, b"count=5", 1000);
        assert!(result.is_none());
        assert_eq!(gen.windows_tracked(), 1);
    }

    #[test]
    fn test_late_data_retraction_changed_result() {
        let mut gen = LateDataRetractionGenerator::new(true);
        let window_id = WindowId::new(0, 60000);

        // First emission
        gen.check_retraction(&window_id, b"count=5", 1000);

        // Late data causes different result - generates retraction
        let result = gen.check_retraction(&window_id, b"count=7", 2000);
        assert!(result.is_some());

        let (old, new) = result.unwrap();
        assert_eq!(old, b"count=5");
        assert_eq!(new, b"count=7");
        assert_eq!(gen.retractions_generated(), 1);
    }

    #[test]
    fn test_late_data_retraction_same_result() {
        let mut gen = LateDataRetractionGenerator::new(true);
        let window_id = WindowId::new(0, 60000);

        // First emission
        gen.check_retraction(&window_id, b"count=5", 1000);

        // Same result - no retraction
        let result = gen.check_retraction(&window_id, b"count=5", 2000);
        assert!(result.is_none());
        assert_eq!(gen.retractions_generated(), 0);
    }

    #[test]
    fn test_late_data_retraction_disabled() {
        let mut gen = LateDataRetractionGenerator::new(false);
        let window_id = WindowId::new(0, 60000);

        gen.check_retraction(&window_id, b"count=5", 1000);
        let result = gen.check_retraction(&window_id, b"count=7", 2000);

        // No retraction when disabled
        assert!(result.is_none());
    }

    #[test]
    fn test_late_data_cleanup() {
        let mut gen = LateDataRetractionGenerator::new(true);

        let w1 = WindowId::new(0, 1000);
        let w2 = WindowId::new(1000, 2000);

        gen.check_retraction(&w1, b"a", 100);
        gen.check_retraction(&w2, b"b", 200);
        assert_eq!(gen.windows_tracked(), 2);

        gen.cleanup_window(&w1);
        assert_eq!(gen.windows_tracked(), 1);

        gen.cleanup_before_watermark(2000);
        assert_eq!(gen.windows_tracked(), 0);
    }

    // CdcEnvelope Tests

    #[test]
    fn test_cdc_envelope_insert() {
        let source = CdcSource::new("laminardb", "default", "orders");
        let envelope = CdcEnvelope::insert(
            serde_json::json!({"id": 1, "amount": 100}),
            source,
            1_706_140_800_000,
        );

        assert_eq!(envelope.op, "c");
        assert!(envelope.is_insert());
        assert!(envelope.before.is_none());
        assert!(envelope.after.is_some());
        assert_eq!(envelope.weight(), 1);
    }

    #[test]
    fn test_cdc_envelope_delete() {
        let source = CdcSource::new("laminardb", "default", "orders");
        let envelope = CdcEnvelope::delete(serde_json::json!({"id": 1}), source, 1_706_140_800_000);

        assert_eq!(envelope.op, "d");
        assert!(envelope.is_delete());
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_none());
        assert_eq!(envelope.weight(), -1);
    }

    #[test]
    fn test_cdc_envelope_update() {
        let source = CdcSource::new("laminardb", "default", "orders");
        let envelope = CdcEnvelope::update(
            serde_json::json!({"id": 1, "amount": 100}),
            serde_json::json!({"id": 1, "amount": 150}),
            source,
            1_706_140_800_000,
        );

        assert_eq!(envelope.op, "u");
        assert!(envelope.is_update());
        assert!(envelope.before.is_some());
        assert!(envelope.after.is_some());
        assert_eq!(envelope.weight(), 0);
    }

    #[test]
    fn test_cdc_envelope_json_serialization() {
        let source = CdcSource::new("laminardb", "default", "orders");
        let envelope = CdcEnvelope::insert(
            serde_json::json!({"id": 1, "amount": 100}),
            source,
            1_706_140_800_000,
        );

        let json = envelope.to_json().unwrap();
        assert!(json.contains("\"op\":\"c\""));
        assert!(json.contains("\"after\""));
        assert!(!json.contains("\"before\""));
        assert!(json.contains("\"ts_ms\":1706140800000"));
    }

    #[test]
    fn test_cdc_envelope_debezium_compatible() {
        let source = CdcSource::with_sequence("laminardb", "test_db", "users", 42);
        let envelope = CdcEnvelope::insert(
            serde_json::json!({"user_id": 123, "name": "Alice"}),
            source,
            1_706_140_800_000,
        );

        let json = envelope.to_json().unwrap();

        // Verify Debezium-compatible fields
        assert!(json.contains("\"op\":\"c\""));
        assert!(json.contains("\"source\""));
        assert!(json.contains("\"name\":\"laminardb\""));
        assert!(json.contains("\"db\":\"test_db\""));
        assert!(json.contains("\"table\":\"users\""));
        assert!(json.contains("\"sequence\":42"));
    }

    #[test]
    fn test_cdc_source_sequence() {
        let mut source = CdcSource::new("laminardb", "db", "table");
        assert_eq!(source.sequence, 0);

        assert_eq!(source.next_sequence(), 1);
        assert_eq!(source.next_sequence(), 2);
        assert_eq!(source.sequence, 2);
    }

    // CdcOperation Tests

    #[test]
    fn test_cdc_operation_roundtrip() {
        for op in [
            CdcOperation::Insert,
            CdcOperation::Delete,
            CdcOperation::UpdateBefore,
            CdcOperation::UpdateAfter,
        ] {
            let u8_val = op.to_u8();
            let restored = CdcOperation::from_u8(u8_val);
            assert_eq!(op, restored);
        }
    }

    #[test]
    fn test_cdc_operation_unknown_u8() {
        // Unknown values default to Insert
        assert_eq!(CdcOperation::from_u8(255), CdcOperation::Insert);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Retractable FIRST/LAST Accumulator Tests
    // ════════════════════════════════════════════════════════════════════════

    // ── RetractableFirstValueAccumulator ─────────────────────────────────────

    #[test]
    fn test_retractable_first_value_basic() {
        let mut acc = RetractableFirstValueAccumulator::new();
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);

        // Add entries out of order
        acc.add((200, 20));
        acc.add((100, 10));
        acc.add((300, 30));

        assert!(!acc.is_empty());
        assert_eq!(acc.len(), 3);
        // First value = earliest timestamp (100) → value 10
        assert_eq!(acc.result(), Some(10));
    }

    #[test]
    fn test_retractable_first_value_retract_non_first() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));
        acc.add((300, 30));

        // Retract a non-first entry → first value unchanged
        acc.retract(&(200, 20));
        assert_eq!(acc.len(), 2);
        assert_eq!(acc.result(), Some(10));
    }

    #[test]
    fn test_retractable_first_value_retract_first() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));
        acc.add((300, 30));

        // Retract the first entry → next earliest becomes first
        acc.retract(&(100, 10));
        assert_eq!(acc.len(), 2);
        assert_eq!(acc.result(), Some(20)); // ts=200
    }

    #[test]
    fn test_retractable_first_value_retract_all() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));

        acc.retract(&(100, 10));
        acc.retract(&(200, 20));
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);
    }

    #[test]
    fn test_retractable_first_value_retract_nonexistent() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));

        // Retract something that doesn't exist → no effect
        acc.retract(&(999, 99));
        assert_eq!(acc.len(), 1);
        assert_eq!(acc.result(), Some(10));
    }

    #[test]
    fn test_retractable_first_value_duplicate_timestamps() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));
        acc.add((100, 20)); // Same timestamp, different value

        assert_eq!(acc.len(), 2);
        // First at timestamp 100, first inserted value
        assert_eq!(acc.result(), Some(10));

        // Retract one → other remains
        acc.retract(&(100, 10));
        assert_eq!(acc.result(), Some(20));
    }

    // ── RetractableLastValueAccumulator ──────────────────────────────────────

    #[test]
    fn test_retractable_last_value_basic() {
        let mut acc = RetractableLastValueAccumulator::new();
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);

        acc.add((100, 10));
        acc.add((300, 30));
        acc.add((200, 20));

        assert_eq!(acc.len(), 3);
        // Last value = latest timestamp (300) → value 30
        assert_eq!(acc.result(), Some(30));
    }

    #[test]
    fn test_retractable_last_value_retract_non_last() {
        let mut acc = RetractableLastValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));
        acc.add((300, 30));

        // Retract a non-last entry → last value unchanged
        acc.retract(&(200, 20));
        assert_eq!(acc.result(), Some(30));
    }

    #[test]
    fn test_retractable_last_value_retract_last() {
        let mut acc = RetractableLastValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));
        acc.add((300, 30));

        // Retract the last entry → next latest becomes last
        acc.retract(&(300, 30));
        assert_eq!(acc.result(), Some(20)); // ts=200
    }

    #[test]
    fn test_retractable_last_value_retract_all() {
        let mut acc = RetractableLastValueAccumulator::new();
        acc.add((100, 10));
        acc.retract(&(100, 10));
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);
    }

    // ── Merge tests ─────────────────────────────────────────────────────────

    #[test]
    fn test_retractable_first_value_merge() {
        let mut acc1 = RetractableFirstValueAccumulator::new();
        let mut acc2 = RetractableFirstValueAccumulator::new();

        acc1.add((200, 20));
        acc1.add((400, 40));
        acc2.add((100, 10));
        acc2.add((300, 30));

        acc1.merge(&acc2);
        assert_eq!(acc1.len(), 4);
        // Merged: sorted by timestamp, first = (100, 10)
        assert_eq!(acc1.result(), Some(10));
    }

    #[test]
    fn test_retractable_last_value_merge() {
        let mut acc1 = RetractableLastValueAccumulator::new();
        let mut acc2 = RetractableLastValueAccumulator::new();

        acc1.add((100, 10));
        acc1.add((300, 30));
        acc2.add((200, 20));
        acc2.add((400, 40));

        acc1.merge(&acc2);
        assert_eq!(acc1.len(), 4);
        // Last = (400, 40)
        assert_eq!(acc1.result(), Some(40));
    }

    #[test]
    fn test_retractable_first_value_merge_empty() {
        let mut acc1 = RetractableFirstValueAccumulator::new();
        let acc2 = RetractableFirstValueAccumulator::new();

        acc1.add((100, 10));
        acc1.merge(&acc2); // Merge empty into non-empty
        assert_eq!(acc1.result(), Some(10));

        let mut acc3 = RetractableFirstValueAccumulator::new();
        let acc4 = RetractableFirstValueAccumulator::new();
        acc3.merge(&acc4); // Merge empty into empty
        assert!(acc3.is_empty());
    }

    // ── Reset/clear tests ───────────────────────────────────────────────────

    #[test]
    fn test_retractable_first_value_reset() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 10));
        acc.add((200, 20));
        assert!(!acc.is_empty());

        acc.reset();
        assert!(acc.is_empty());
        assert_eq!(acc.result(), None);
    }

    #[test]
    fn test_retractable_last_value_reset() {
        let mut acc = RetractableLastValueAccumulator::new();
        acc.add((100, 10));
        acc.reset();
        assert!(acc.is_empty());
    }

    // ── f64 variant tests ───────────────────────────────────────────────────

    #[test]
    fn test_retractable_first_value_f64_basic() {
        let mut acc = RetractableFirstValueF64Accumulator::new();
        acc.add((200, 20.5));
        acc.add((100, 10.5));
        acc.add((300, 30.5));

        assert_eq!(acc.len(), 3);
        // First = earliest timestamp (100) → value 10.5
        let result = acc.result_f64().unwrap();
        assert!((result - 10.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retractable_first_value_f64_retract() {
        let mut acc = RetractableFirstValueF64Accumulator::new();
        acc.add((100, 10.5));
        acc.add((200, 20.5));

        // Retract first → next becomes first
        acc.retract(&(100, 10.5));
        let result = acc.result_f64().unwrap();
        assert!((result - 20.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retractable_last_value_f64_basic() {
        let mut acc = RetractableLastValueF64Accumulator::new();
        acc.add((100, 10.5));
        acc.add((300, 30.5));
        acc.add((200, 20.5));

        let result = acc.result_f64().unwrap();
        assert!((result - 30.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retractable_last_value_f64_retract() {
        let mut acc = RetractableLastValueF64Accumulator::new();
        acc.add((100, 10.5));
        acc.add((200, 20.5));
        acc.add((300, 30.5));

        acc.retract(&(300, 30.5));
        let result = acc.result_f64().unwrap();
        assert!((result - 20.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retractable_first_value_f64_merge() {
        let mut acc1 = RetractableFirstValueF64Accumulator::new();
        let mut acc2 = RetractableFirstValueF64Accumulator::new();
        acc1.add((200, 20.5));
        acc2.add((100, 10.5));
        acc1.merge(&acc2);
        let result = acc1.result_f64().unwrap();
        assert!((result - 10.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retractable_last_value_f64_merge() {
        let mut acc1 = RetractableLastValueF64Accumulator::new();
        let mut acc2 = RetractableLastValueF64Accumulator::new();
        acc1.add((100, 10.5));
        acc2.add((300, 30.5));
        acc1.merge(&acc2);
        let result = acc1.result_f64().unwrap();
        assert!((result - 30.5).abs() < f64::EPSILON);
    }

    // ── Edge cases ──────────────────────────────────────────────────────────

    #[test]
    fn test_retractable_first_value_single_entry() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, 42));
        assert_eq!(acc.result(), Some(42));
        acc.retract(&(100, 42));
        assert_eq!(acc.result(), None);
    }

    #[test]
    fn test_retractable_last_value_single_entry() {
        let mut acc = RetractableLastValueAccumulator::new();
        acc.add((100, 42));
        assert_eq!(acc.result(), Some(42));
        acc.retract(&(100, 42));
        assert_eq!(acc.result(), None);
    }

    #[test]
    fn test_retractable_first_value_negative_values() {
        let mut acc = RetractableFirstValueAccumulator::new();
        acc.add((100, -10));
        acc.add((200, -20));
        assert_eq!(acc.result(), Some(-10));
    }

    #[test]
    fn test_retractable_supports_efficient_retraction() {
        let acc = RetractableFirstValueAccumulator::new();
        assert!(acc.supports_efficient_retraction());

        let acc2 = RetractableLastValueAccumulator::new();
        assert!(acc2.supports_efficient_retraction());

        let acc3 = RetractableFirstValueF64Accumulator::new();
        assert!(acc3.supports_efficient_retraction());

        let acc4 = RetractableLastValueF64Accumulator::new();
        assert!(acc4.supports_efficient_retraction());
    }

    // ── OHLC retraction simulation ──────────────────────────────────────────

    #[test]
    fn test_ohlc_retraction_simulation() {
        // Simulate an OHLC window where trades arrive out of order
        // and one needs to be retracted
        let mut open_acc = RetractableFirstValueAccumulator::new();
        let mut close_acc = RetractableLastValueAccumulator::new();

        // Trade 1: price=100 at t=1000
        open_acc.add((1000, 100));
        close_acc.add((1000, 100));

        // Trade 2: price=105 at t=2000
        open_acc.add((2000, 105));
        close_acc.add((2000, 105));

        // Trade 3: price=98 at t=3000
        open_acc.add((3000, 98));
        close_acc.add((3000, 98));

        assert_eq!(open_acc.result(), Some(100)); // Open = earliest
        assert_eq!(close_acc.result(), Some(98)); // Close = latest

        // Retract trade 1 (correction: it was a bad trade)
        open_acc.retract(&(1000, 100));
        close_acc.retract(&(1000, 100));

        // Open now = trade 2 (earliest remaining)
        assert_eq!(open_acc.result(), Some(105));
        // Close still = trade 3 (latest)
        assert_eq!(close_acc.result(), Some(98));
    }

    #[test]
    fn test_ohlc_retraction_f64_simulation() {
        let mut open_acc = RetractableFirstValueF64Accumulator::new();
        let mut close_acc = RetractableLastValueF64Accumulator::new();

        open_acc.add((1000, 100.50));
        close_acc.add((1000, 100.50));
        open_acc.add((2000, 105.25));
        close_acc.add((2000, 105.25));
        open_acc.add((3000, 98.75));
        close_acc.add((3000, 98.75));

        let open = open_acc.result_f64().unwrap();
        let close = close_acc.result_f64().unwrap();
        assert!((open - 100.50).abs() < f64::EPSILON);
        assert!((close - 98.75).abs() < f64::EPSILON);

        // Retract trade at t=1000
        open_acc.retract(&(1000, 100.50));
        close_acc.retract(&(1000, 100.50));

        let open2 = open_acc.result_f64().unwrap();
        assert!((open2 - 105.25).abs() < f64::EPSILON);
    }
}
