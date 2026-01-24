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
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::window::{
//!     TumblingWindowAssigner, TumblingWindowOperator, CountAggregator,
//! };
//! use std::time::Duration;
//!
//! // Create a 1-minute tumbling window with count aggregation
//! let assigner = TumblingWindowAssigner::new(Duration::from_secs(60));
//! let operator = TumblingWindowOperator::new(
//!     assigner,
//!     CountAggregator::new(),
//!     Duration::from_secs(5), // 5 second grace period
//! );
//! ```

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use crate::state::{StateStore, StateStoreExt};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
    /// This method allocates a `Vec<u8>`. For hot path operations,
    /// prefer [`to_key_inline`] which returns a stack-allocated array.
    #[must_use]
    pub fn to_key(&self) -> Vec<u8> {
        self.to_key_inline().to_vec()
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

/// Trait for assigning events to windows.
pub trait WindowAssigner: Send {
    /// Assigns an event timestamp to zero or more windows.
    ///
    /// For tumbling windows, this returns exactly one window.
    /// For sliding windows, this may return multiple windows.
    fn assign_windows(&self, timestamp: i64) -> Vec<WindowId>;

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
    fn assign_windows(&self, timestamp: i64) -> Vec<WindowId> {
        vec![self.assign(timestamp)]
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
/// and results are emitted when the watermark passes the window end.
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
            operator_id,
            output_schema: create_window_output_schema(),
            _phantom: PhantomData,
        }
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
                .register_timer(trigger_time, Some(window_id.to_key()));
            self.registered_windows.insert(window_id);
        }
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

        // Check for late data using current watermark
        let current_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if this event is too late
        if let Some(wm) = &current_watermark {
            if self.is_late(event_time, wm.timestamp()) {
                let mut output = OutputVec::new();
                output.push(Output::LateEvent(event.clone()));
                return output;
            }
        }

        // Assign event to window
        let window_id = self.assigner.assign(event_time);

        // Extract value and update accumulator
        if let Some(value) = self.aggregator.extract(event) {
            let mut acc = self.get_accumulator(&window_id, ctx.state);
            acc.add(value);
            if let Err(e) = self.put_accumulator(&window_id, &acc, ctx.state) {
                // Log error but don't fail - we'll retry on next event
                eprintln!("Failed to store window state: {e}");
            }
        }

        // Register timer for this window
        self.maybe_register_timer(window_id, ctx);

        // Emit watermark update if generated
        let mut output = OutputVec::new();
        if let Some(wm) = current_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }
        output
    }

    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        // Parse window ID from timer key
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
            return OutputVec::new();
        }

        // Get the result
        let result = acc.result();

        // Clean up window state
        let _ = self.delete_accumulator(&window_id, ctx.state);
        self.registered_windows.remove(&window_id);

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
                output.push(Output::Event(Event {
                    timestamp: window_id.end,
                    data,
                }));
            }
            Err(e) => {
                eprintln!("Failed to create output batch: {e}");
            }
        }
        output
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize the registered windows set using rkyv
        let windows: Vec<_> = self.registered_windows.iter().copied().collect();
        let data = rkyv::to_bytes::<RkyvError>(&windows)
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

        // Deserialize using rkyv
        let archived = rkyv::access::<rkyv::Archived<Vec<WindowId>>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let windows: Vec<WindowId> = rkyv::deserialize::<Vec<WindowId>, RkyvError>(archived)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.registered_windows = windows.into_iter().collect();
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
}
