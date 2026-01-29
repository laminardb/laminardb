//! # Sliding Window Operators
//!
//! Implementation of sliding (hopping) windows for stream processing.
//!
//! Sliding windows are fixed-size windows that overlap. Each event can belong
//! to multiple windows. The window size defines the duration, and the slide
//! defines how much each window advances.
//!
//! ## Example
//!
//! ```text
//! Window size: 1 hour, Slide: 15 minutes
//!
//! Window 1: [00:00, 01:00)
//! Window 2: [00:15, 01:15)
//! Window 3: [00:30, 01:30)
//! Window 4: [00:45, 01:45)
//!
//! An event at 00:40 belongs to windows 1, 2, 3
//! ```
//!
//! ## Performance
//!
//! - Each event is assigned to `ceil(size / slide)` windows
//! - Uses `SmallVec<[WindowId; 4]>` to avoid heap allocation for common cases
//! - State is stored per-window, so memory usage scales with active windows
//!
//! ## Usage
//!
//! ```rust,no_run
//! use laminar_core::operator::sliding_window::{
//!     SlidingWindowAssigner, SlidingWindowOperator,
//! };
//! use laminar_core::operator::window::CountAggregator;
//! use std::time::Duration;
//!
//! // Create a 1-hour sliding window with 15-minute slide
//! let assigner = SlidingWindowAssigner::new(
//!     Duration::from_secs(3600),  // 1 hour window
//!     Duration::from_secs(900),   // 15 minute slide
//! );
//! let operator = SlidingWindowOperator::new(
//!     assigner,
//!     CountAggregator::new(),
//!     Duration::from_secs(60), // 1 minute grace period
//! );
//! ```

use super::window::{
    Accumulator, Aggregator, ChangelogRecord, EmitStrategy, LateDataConfig, LateDataMetrics,
    ResultToI64, WindowAssigner, WindowId, WindowIdVec,
};
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

/// Sliding window assigner.
///
/// Assigns each event to multiple overlapping windows based on its timestamp.
/// Windows are aligned to epoch (timestamp 0).
///
/// # Parameters
///
/// - `size_ms`: The duration of each window in milliseconds
/// - `slide_ms`: The advance interval between windows in milliseconds
///
/// # Window Assignment
///
/// An event at timestamp `t` belongs to all windows `[start, start + size)`
/// where `start <= t < start + size` and `start` is a multiple of `slide`.
///
/// The number of windows per event is `ceil(size / slide)`.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::operator::sliding_window::SlidingWindowAssigner;
/// use std::time::Duration;
///
/// // 1-minute window with 20-second slide
/// let assigner = SlidingWindowAssigner::new(
///     Duration::from_secs(60),
///     Duration::from_secs(20),
/// );
///
/// // Event at t=50 belongs to windows: [0,60), [20,80), [40,100)
/// ```
#[derive(Debug, Clone)]
pub struct SlidingWindowAssigner {
    /// Window size in milliseconds
    size_ms: i64,
    /// Slide interval in milliseconds
    slide_ms: i64,
    /// Number of windows per event (cached for performance)
    windows_per_event: usize,
}

impl SlidingWindowAssigner {
    /// Creates a new sliding window assigner.
    ///
    /// # Arguments
    ///
    /// * `size` - The duration of each window
    /// * `slide` - The advance interval between windows
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Size is zero or negative
    /// - Slide is zero or negative
    /// - Slide is greater than size (use tumbling windows instead)
    #[must_use]
    pub fn new(size: Duration, slide: Duration) -> Self {
        // Ensure size and slide fit in i64
        let size_ms = i64::try_from(size.as_millis()).expect("Window size must fit in i64");
        let slide_ms = i64::try_from(slide.as_millis()).expect("Slide interval must fit in i64");

        assert!(size_ms > 0, "Window size must be positive");
        assert!(slide_ms > 0, "Slide interval must be positive");
        assert!(
            slide_ms <= size_ms,
            "Slide must not exceed size (use tumbling windows for non-overlapping)"
        );

        // Calculate the number of windows each event belongs to
        // This is ceil(size / slide)
        let windows_per_event = usize::try_from((size_ms + slide_ms - 1) / slide_ms)
            .expect("Windows per event should fit in usize");

        Self {
            size_ms,
            slide_ms,
            windows_per_event,
        }
    }

    /// Creates a new sliding window assigner with sizes in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if size or slide is zero/negative, or if slide > size.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn from_millis(size_ms: i64, slide_ms: i64) -> Self {
        assert!(size_ms > 0, "Window size must be positive");
        assert!(slide_ms > 0, "Slide interval must be positive");
        assert!(
            slide_ms <= size_ms,
            "Slide must not exceed size (use tumbling windows for non-overlapping)"
        );

        // Truncation is acceptable: number of windows per event will never exceed reasonable limits
        let windows_per_event = usize::try_from((size_ms + slide_ms - 1) / slide_ms).unwrap_or(usize::MAX);

        Self {
            size_ms,
            slide_ms,
            windows_per_event,
        }
    }

    /// Returns the window size in milliseconds.
    #[must_use]
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }

    /// Returns the slide interval in milliseconds.
    #[must_use]
    pub fn slide_ms(&self) -> i64 {
        self.slide_ms
    }

    /// Returns the number of windows each event belongs to.
    #[must_use]
    pub fn windows_per_event(&self) -> usize {
        self.windows_per_event
    }

    /// Computes the last window start that could contain this timestamp.
    ///
    /// This is the window with the largest start time where start <= timestamp.
    #[inline]
    fn last_window_start(&self, timestamp: i64) -> i64 {
        if timestamp >= 0 {
            (timestamp / self.slide_ms) * self.slide_ms
        } else {
            // For negative timestamps, use floor division
            ((timestamp - self.slide_ms + 1) / self.slide_ms) * self.slide_ms
        }
    }
}

impl WindowAssigner for SlidingWindowAssigner {
    /// Assigns a timestamp to all overlapping windows.
    ///
    /// Returns windows in order from earliest to latest start time.
    #[inline]
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec {
        let mut windows = WindowIdVec::new();

        // Find the last window that could contain this timestamp
        let last_start = self.last_window_start(timestamp);

        // Walk backwards through all windows that contain this timestamp
        let mut window_start = last_start;
        while window_start + self.size_ms > timestamp {
            let window_end = window_start + self.size_ms;
            windows.push(WindowId::new(window_start, window_end));
            window_start -= self.slide_ms;
        }

        // Reverse to get windows in chronological order (earliest first)
        windows.reverse();
        windows
    }

    /// Returns the maximum timestamp that could still be assigned to a window
    /// ending at `window_end`.
    fn max_timestamp(&self, window_end: i64) -> i64 {
        window_end - 1
    }
}

/// State key prefix for window accumulators (4 bytes)
const WINDOW_STATE_PREFIX: &[u8; 4] = b"slw:";

/// Total size of window state key: prefix (4) + `WindowId` (16) = 20 bytes
const WINDOW_STATE_KEY_SIZE: usize = 4 + 16;

/// Static counter for generating unique operator IDs without allocation.
static SLIDING_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates the standard window output schema.
fn create_window_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
        Field::new("result", DataType::Int64, false),
    ]))
}

/// Sliding window operator.
///
/// Processes events through overlapping, fixed-size time windows.
/// Each event is assigned to multiple windows based on the slide interval.
/// Results are emitted based on the configured [`EmitStrategy`].
///
/// # Emit Strategies
///
/// - `OnWatermark` (default): Emit when watermark passes window end
/// - `Periodic`: Emit intermediate results at intervals, final on watermark
/// - `OnUpdate`: Emit after every state update (can produce many outputs)
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
/// prefixed keys: `slw:<window_id>` - Accumulator state
///
/// # Performance Considerations
///
/// Each event updates `ceil(size / slide)` windows. For example:
/// - 1-hour window, 15-minute slide = 4 windows per event
/// - 1-minute window, 10-second slide = 6 windows per event
///
/// State usage scales linearly with active windows.
pub struct SlidingWindowOperator<A: Aggregator> {
    /// Window assigner
    assigner: SlidingWindowAssigner,
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

impl<A: Aggregator> SlidingWindowOperator<A>
where
    A::Acc: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    /// Creates a new sliding window operator.
    ///
    /// # Arguments
    ///
    /// * `assigner` - Window assigner for determining window boundaries
    /// * `aggregator` - Aggregation function to apply within windows
    /// * `allowed_lateness` - Grace period for late data after window close
    /// # Panics
    ///
    /// Panics if allowed lateness does not fit in i64.
    pub fn new(assigner: SlidingWindowAssigner, aggregator: A, allowed_lateness: Duration) -> Self {
        let operator_num = SLIDING_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            assigner,
            aggregator,
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
            registered_windows: std::collections::HashSet::new(),
            periodic_timer_windows: std::collections::HashSet::new(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            operator_id: format!("sliding_window_{operator_num}"),
            output_schema: create_window_output_schema(),
            _phantom: PhantomData,
        }
    }

    /// Creates a new sliding window operator with a custom operator ID.
    /// # Panics
    ///
    /// Panics if allowed lateness does not fit in i64.
    pub fn with_id(
        assigner: SlidingWindowAssigner,
        aggregator: A,
        allowed_lateness: Duration,
        operator_id: String,
    ) -> Self {
        Self {
            assigner,
            aggregator,
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
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
    pub fn set_emit_strategy(&mut self, strategy: EmitStrategy) {
        self.emit_strategy = strategy;
    }

    /// Returns the current emit strategy.
    #[must_use]
    pub fn emit_strategy(&self) -> &EmitStrategy {
        &self.emit_strategy
    }

    /// Sets the late data handling configuration.
    pub fn set_late_data_config(&mut self, config: LateDataConfig) {
        self.late_data_config = config;
    }

    /// Returns the current late data configuration.
    #[must_use]
    pub fn late_data_config(&self) -> &LateDataConfig {
        &self.late_data_config
    }

    /// Returns the late data metrics.
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
    pub fn assigner(&self) -> &SlidingWindowAssigner {
        &self.assigner
    }

    /// Returns the allowed lateness in milliseconds.
    #[must_use]
    pub fn allowed_lateness_ms(&self) -> i64 {
        self.allowed_lateness_ms
    }

    /// Generates the state key for a window's accumulator.
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

    /// Checks if an event is late for all possible windows.
    ///
    /// An event is late if all windows it would belong to have already closed
    /// (watermark has passed their end + allowed lateness).
    fn is_late(&self, event_time: i64, watermark: i64) -> bool {
        // Get all windows this event would belong to
        let windows = self.assigner.assign_windows(event_time);

        // Event is late only if ALL its windows have closed
        windows.iter().all(|window_id| {
            let cleanup_time = window_id.end + self.allowed_lateness_ms;
            watermark >= cleanup_time
        })
    }

    /// Registers a timer for window triggering if not already registered.
    fn maybe_register_timer(&mut self, window_id: WindowId, ctx: &mut OperatorContext) {
        if !self.registered_windows.contains(&window_id) {
            let trigger_time = window_id.end + self.allowed_lateness_ms;
            ctx.timers
                .register_timer(trigger_time, Some(window_id.to_key()), Some(ctx.operator_index));
            self.registered_windows.insert(window_id);
        }
    }

    /// Registers a periodic timer for intermediate emissions.
    fn maybe_register_periodic_timer(&mut self, window_id: WindowId, ctx: &mut OperatorContext) {
        if let EmitStrategy::Periodic(interval) = &self.emit_strategy {
            if !self.periodic_timer_windows.contains(&window_id) {
                let interval_ms = i64::try_from(interval.as_millis()).expect("Interval must fit in i64");
                let trigger_time = ctx.processing_time + interval_ms;
                let key = Self::periodic_timer_key(&window_id);
                ctx.timers.register_timer(trigger_time, Some(key), Some(ctx.operator_index));
                self.periodic_timer_windows.insert(window_id);
            }
        }
    }

    /// Creates a periodic timer key from a window ID.
    #[inline]
    fn periodic_timer_key(window_id: &WindowId) -> super::TimerKey {
        let mut key = window_id.to_key();
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
        clean_key[0] &= 0x7F;
        WindowId::from_key(&clean_key)
    }

    /// Creates an intermediate result for a window without cleaning up state.
    fn create_intermediate_result(
        &self,
        window_id: &WindowId,
        state: &dyn StateStore,
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
        )
        .ok()?;

        Some(Event {
            timestamp: window_id.end,
            data: batch,
        })
    }

    /// Handles periodic timer expiration for intermediate emissions.
    fn handle_periodic_timer(
        &mut self,
        window_id: WindowId,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        if !self.registered_windows.contains(&window_id) {
            self.periodic_timer_windows.remove(&window_id);
            return output;
        }

        if let Some(event) = self.create_intermediate_result(&window_id, ctx.state) {
            output.push(Output::Event(event));
        }

        if let EmitStrategy::Periodic(interval) = &self.emit_strategy {
            let interval_ms = i64::try_from(interval.as_millis()).expect("Interval must fit in i64");
            let next_trigger = ctx.processing_time + interval_ms;
            let window_close_time = window_id.end + self.allowed_lateness_ms;
            if next_trigger < window_close_time {
                let key = Self::periodic_timer_key(&window_id);
                ctx.timers.register_timer(next_trigger, Some(key), Some(ctx.operator_index));
            }
        }

        output
    }
}

impl<A: Aggregator> Operator for SlidingWindowOperator<A>
where
    A::Acc: 'static
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let event_time = event.timestamp;

        // Update watermark with the new event
        let emitted_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if this event is too late (all windows closed)
        let current_wm = ctx.watermark_generator.current_watermark();
        if current_wm > i64::MIN && self.is_late(event_time, current_wm) {
            let mut output = OutputVec::new();

            // F011B: EMIT FINAL drops late data entirely
            if self.emit_strategy.drops_late_data() {
                self.late_data_metrics.record_dropped();
                return output; // Silently drop - no LateEvent output
            }

            if let Some(side_output_name) = self.late_data_config.side_output() {
                self.late_data_metrics.record_side_output();
                output.push(Output::SideOutput {
                    name: side_output_name.to_string(),
                    event: event.clone(),
                });
            } else {
                self.late_data_metrics.record_dropped();
                output.push(Output::LateEvent(event.clone()));
            }
            return output;
        }

        // Assign event to all overlapping windows
        let windows = self.assigner.assign_windows(event_time);

        // Track windows that were updated (for OnUpdate and Changelog strategies)
        let mut updated_windows = Vec::new();

        // Update accumulator for each window
        for window_id in &windows {
            // Skip windows that have already closed
            let cleanup_time = window_id.end + self.allowed_lateness_ms;
            if current_wm > i64::MIN && current_wm >= cleanup_time {
                continue;
            }

            // Extract value for each window (re-extract since Input may not be Clone)
            if let Some(value) = self.aggregator.extract(event) {
                let mut acc = self.get_accumulator(window_id, ctx.state);
                acc.add(value);
                if Self::put_accumulator(window_id, &acc, ctx.state).is_ok() {
                    updated_windows.push(*window_id);
                }
            }

            // Register timers for this window
            self.maybe_register_timer(*window_id, ctx);

            // F011B: OnWindowClose and Final suppress intermediate emissions
            if !self.emit_strategy.suppresses_intermediate() {
                self.maybe_register_periodic_timer(*window_id, ctx);
            }
        }

        // Build output
        let mut output = OutputVec::new();

        // Emit watermark update if generated
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        // F011B: Handle different emit strategies for intermediate emissions
        if !updated_windows.is_empty() {
            match &self.emit_strategy {
                // OnUpdate: emit intermediate result as regular event
                EmitStrategy::OnUpdate => {
                    for window_id in &updated_windows {
                        if let Some(event) = self.create_intermediate_result(window_id, ctx.state) {
                            output.push(Output::Event(event));
                        }
                    }
                }
                // Changelog: emit changelog record on every update
                EmitStrategy::Changelog => {
                    for window_id in &updated_windows {
                        if let Some(event) = self.create_intermediate_result(window_id, ctx.state) {
                            let record = ChangelogRecord::insert(event, ctx.processing_time);
                            output.push(Output::Changelog(record));
                        }
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
        // Check if this is a periodic timer
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
            let _ = Self::delete_accumulator(&window_id, ctx.state);
            self.registered_windows.remove(&window_id);
            self.periodic_timer_windows.remove(&window_id);
            return OutputVec::new();
        }

        // Get the result
        let result = acc.result();

        // Clean up window state
        let _ = Self::delete_accumulator(&window_id, ctx.state);
        self.registered_windows.remove(&window_id);
        self.periodic_timer_windows.remove(&window_id);

        // Convert result to i64 for the batch
        let result_i64 = result.to_i64();

        // Create output batch
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
                tracing::error!("Failed to create output batch: {e}");
            }
        }
        output
    }

    fn checkpoint(&self) -> OperatorState {
        let windows: Vec<_> = self.registered_windows.iter().copied().collect();
        let periodic_windows: Vec<_> = self.periodic_timer_windows.iter().copied().collect();

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

        // Fall back to old format for backwards compatibility
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
mod tests {
    use super::*;
    use crate::operator::window::{CountAggregator, CountAccumulator, SumAggregator};
    use crate::state::InMemoryStore;
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

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
    fn test_sliding_assigner_creation() {
        let assigner =
            SlidingWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(20));

        assert_eq!(assigner.size_ms(), 60_000);
        assert_eq!(assigner.slide_ms(), 20_000);
        assert_eq!(assigner.windows_per_event(), 3); // ceil(60/20) = 3
    }

    #[test]
    fn test_sliding_assigner_from_millis() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 200);

        assert_eq!(assigner.size_ms(), 1000);
        assert_eq!(assigner.slide_ms(), 200);
        assert_eq!(assigner.windows_per_event(), 5); // ceil(1000/200) = 5
    }

    #[test]
    #[should_panic(expected = "Window size must be positive")]
    fn test_sliding_assigner_zero_size() {
        let _ = SlidingWindowAssigner::from_millis(0, 100);
    }

    #[test]
    #[should_panic(expected = "Slide interval must be positive")]
    fn test_sliding_assigner_zero_slide() {
        let _ = SlidingWindowAssigner::from_millis(1000, 0);
    }

    #[test]
    #[should_panic(expected = "Slide must not exceed size")]
    fn test_sliding_assigner_slide_exceeds_size() {
        let _ = SlidingWindowAssigner::from_millis(100, 200);
    }

    #[test]
    fn test_sliding_assigner_basic_assignment() {
        // 1-minute window with 20-second slide
        let assigner = SlidingWindowAssigner::from_millis(60_000, 20_000);

        // Event at t=50000 should belong to 3 windows
        let windows = assigner.assign_windows(50_000);

        assert_eq!(windows.len(), 3);

        // Windows should be (in chronological order):
        // [20000, 80000), [40000, 100000), [60000, 120000) - but wait, 50000 is NOT in [60000, 120000)
        // Let me recalculate:
        // last_window_start(50000) = (50000 / 20000) * 20000 = 40000
        // Window [40000, 100000) contains 50000? Yes (40000 <= 50000 < 100000)
        // Window [20000, 80000) contains 50000? Yes (20000 <= 50000 < 80000)
        // Window [0, 60000) contains 50000? Yes (0 <= 50000 < 60000)
        // Window [-20000, 40000) contains 50000? No (50000 >= 40000)

        assert!(windows.contains(&WindowId::new(0, 60_000)));
        assert!(windows.contains(&WindowId::new(20_000, 80_000)));
        assert!(windows.contains(&WindowId::new(40_000, 100_000)));
    }

    #[test]
    fn test_sliding_assigner_boundary_event() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);

        // Event exactly at window boundary (t=1000)
        let windows = assigner.assign_windows(1000);

        // Should belong to windows starting at 500 and 1000
        // Window [1000, 2000) contains 1000? Yes
        // Window [500, 1500) contains 1000? Yes
        // Window [0, 1000) contains 1000? No (end is exclusive)
        assert_eq!(windows.len(), 2);
        assert!(windows.contains(&WindowId::new(500, 1500)));
        assert!(windows.contains(&WindowId::new(1000, 2000)));
    }

    #[test]
    fn test_sliding_assigner_negative_timestamp() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);

        // Event at t=-500
        let windows = assigner.assign_windows(-500);

        // Should belong to windows containing -500
        // last_window_start(-500) = floor(-500 / 500) * 500 = -500
        // Window [-500, 500) contains -500? Yes
        // Window [-1000, 0) contains -500? Yes
        // Window [-1500, -500) contains -500? No (end is exclusive)
        assert_eq!(windows.len(), 2);
        assert!(windows.contains(&WindowId::new(-1000, 0)));
        assert!(windows.contains(&WindowId::new(-500, 500)));
    }

    #[test]
    fn test_sliding_assigner_equal_size_and_slide() {
        // When size == slide, should behave like tumbling windows
        let assigner = SlidingWindowAssigner::from_millis(1000, 1000);

        assert_eq!(assigner.windows_per_event(), 1);

        let windows = assigner.assign_windows(500);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], WindowId::new(0, 1000));
    }

    #[test]
    fn test_sliding_assigner_small_slide() {
        // 1 second window, 100ms slide = 10 windows per event
        let assigner = SlidingWindowAssigner::from_millis(1000, 100);

        assert_eq!(assigner.windows_per_event(), 10);

        let windows = assigner.assign_windows(500);
        assert_eq!(windows.len(), 10);
    }


    #[test]
    fn test_sliding_operator_creation() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 200);
        let aggregator = CountAggregator::new();
        let operator = SlidingWindowOperator::new(assigner, aggregator, Duration::from_millis(100));

        assert_eq!(operator.allowed_lateness_ms(), 100);
        assert_eq!(*operator.emit_strategy(), EmitStrategy::OnWatermark);
        assert!(operator.late_data_config().should_drop());
    }

    #[test]
    fn test_sliding_operator_with_id() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 200);
        let aggregator = CountAggregator::new();
        let operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_sliding".to_string(),
        );

        assert_eq!(operator.operator_id, "test_sliding");
    }

    #[test]
    fn test_sliding_operator_process_single_event() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process event at t=600
        // Should belong to windows [0, 1000) and [500, 1500)
        let event = create_test_event(600, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Should have 2 registered windows
        assert_eq!(operator.registered_windows.len(), 2);
        assert!(operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
        assert!(operator
            .registered_windows
            .contains(&WindowId::new(500, 1500)));
    }

    #[test]
    fn test_sliding_operator_accumulates_correctly() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner.clone(),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events at t=100, t=600, t=800
        // t=100: belongs to [0, 1000)
        // t=600: belongs to [0, 1000), [500, 1500)
        // t=800: belongs to [0, 1000), [500, 1500)
        for ts in [100, 600, 800] {
            let event = create_test_event(ts, 1);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Window [0, 1000) should have count = 3
        let window_0_1000 = WindowId::new(0, 1000);
        let acc: CountAccumulator = operator.get_accumulator(&window_0_1000, &state);
        assert_eq!(acc.result(), 3);

        // Window [500, 1500) should have count = 2
        let window_500_1500 = WindowId::new(500, 1500);
        let acc: CountAccumulator = operator.get_accumulator(&window_500_1500, &state);
        assert_eq!(acc.result(), 2);
    }

    #[test]
    fn test_sliding_operator_window_trigger() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process 3 events in window [0, 1000)
        for ts in [100, 200, 300] {
            let event = create_test_event(ts, 1);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Trigger window [0, 1000)
        let timer = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };

        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        let outputs = operator.on_timer(timer, &mut ctx);

        assert_eq!(outputs.len(), 1);
        match &outputs[0] {
            Output::Event(event) => {
                assert_eq!(event.timestamp, 1000);
                let result_col = event.data.column(2);
                let result_array = result_col.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(result_array.value(0), 3);
            }
            _ => panic!("Expected Event output"),
        }

        // Window should be cleaned up
        assert!(!operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
    }

    #[test]
    fn test_sliding_operator_multiple_window_triggers() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = SumAggregator::new(0);
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process event at t=600 with value 10
        // Belongs to [0, 1000) and [500, 1500)
        let event = create_test_event(600, 10);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Trigger first window [0, 1000)
        let t1 = Timer {
            key: WindowId::new(0, 1000).to_key(),
            timestamp: 1000,
        };
        let outputs1 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(t1, &mut ctx)
        };

        assert_eq!(outputs1.len(), 1);
        if let Output::Event(e) = &outputs1[0] {
            let result = e
                .data
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(result, 10);
        }

        // Trigger second window [500, 1500)
        let t2 = Timer {
            key: WindowId::new(500, 1500).to_key(),
            timestamp: 1500,
        };
        let outputs2 = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(t2, &mut ctx)
        };

        assert_eq!(outputs2.len(), 1);
        if let Output::Event(e) = &outputs2[0] {
            let result = e
                .data
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(result, 10);
        }

        // Both windows should be cleaned up
        assert!(operator.registered_windows.is_empty());
    }

    #[test]
    fn test_sliding_operator_late_event() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark to 2000
        let event1 = create_test_event(2000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process late event at t=500 (all windows closed)
        let late_event = create_test_event(500, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should emit LateEvent
        let is_late = outputs.iter().any(|o| matches!(o, Output::LateEvent(_)));
        assert!(is_late);
        assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
    }

    #[test]
    fn test_sliding_operator_late_event_side_output() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        operator.set_late_data_config(LateDataConfig::with_side_output("late".to_string()));

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark
        let event1 = create_test_event(2000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process late event
        let late_event = create_test_event(500, 2);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should emit SideOutput
        let side_output = outputs.iter().find_map(|o| {
            if let Output::SideOutput { name, .. } = o {
                Some(name.clone())
            } else {
                None
            }
        });
        assert_eq!(side_output, Some("late".to_string()));
        assert_eq!(operator.late_data_metrics().late_events_side_output(), 1);
    }

    #[test]
    fn test_sliding_operator_emit_on_update() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        operator.set_emit_strategy(EmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process event - should emit intermediate results for both windows
        let event = create_test_event(600, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };

        // Should have 2 Event outputs (one per window)
        let event_count = outputs.iter().filter(|o| matches!(o, Output::Event(_))).count();
        assert_eq!(event_count, 2);
    }

    #[test]
    fn test_sliding_operator_checkpoint_restore() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        // Register some windows
        operator.registered_windows.insert(WindowId::new(0, 1000));
        operator
            .registered_windows
            .insert(WindowId::new(500, 1500));
        operator
            .periodic_timer_windows
            .insert(WindowId::new(0, 1000));

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Create new operator and restore
        let mut restored = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        restored.restore(checkpoint).unwrap();

        assert_eq!(restored.registered_windows.len(), 2);
        assert_eq!(restored.periodic_timer_windows.len(), 1);
        assert!(restored
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
        assert!(restored
            .registered_windows
            .contains(&WindowId::new(500, 1500)));
        assert!(restored
            .periodic_timer_windows
            .contains(&WindowId::new(0, 1000)));
    }

    #[test]
    fn test_sliding_operator_empty_window_trigger() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
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
    fn test_sliding_operator_periodic_timer_key() {
        let window_id = WindowId::new(1000, 2000);

        let periodic_key = SlidingWindowOperator::<CountAggregator>::periodic_timer_key(&window_id);
        assert!(SlidingWindowOperator::<CountAggregator>::is_periodic_timer_key(&periodic_key));

        let extracted =
            SlidingWindowOperator::<CountAggregator>::window_id_from_periodic_key(&periodic_key);
        assert_eq!(extracted, Some(window_id));

        // Regular key should not be detected as periodic
        let regular_key = window_id.to_key();
        assert!(!SlidingWindowOperator::<CountAggregator>::is_periodic_timer_key(&regular_key));
    }

    #[test]
    fn test_sliding_operator_skips_closed_windows() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark to 1100 (window [0, 1000) is closed, [500, 1500) is open)
        let event1 = create_test_event(1100, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process event at t=800 - belongs to [0, 1000) and [500, 1500)
        // But [0, 1000) is closed, so should only update [500, 1500)
        let event2 = create_test_event(800, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx);
        }

        // Only [500, 1500) and [1000, 2000) should be registered (not [0, 1000))
        assert!(!operator
            .registered_windows
            .contains(&WindowId::new(0, 1000)));
        assert!(operator
            .registered_windows
            .contains(&WindowId::new(500, 1500)));
    }

    #[test]
    fn test_sliding_assigner_window_assigner_trait() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);

        // Test the WindowAssigner trait method
        let windows = assigner.assign_windows(600);
        assert_eq!(windows.len(), 2);

        // Test max_timestamp
        assert_eq!(assigner.max_timestamp(1000), 999);
    }

    #[test]
    fn test_sliding_operator_allowed_lateness() {
        let assigner = SlidingWindowAssigner::from_millis(1000, 500);
        let aggregator = CountAggregator::new();
        let mut operator = SlidingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(500), // 500ms allowed lateness
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark to 1200
        let event1 = create_test_event(1200, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process event at t=800 - window [0, 1000) cleanup is at 1500
        // Watermark (1200) < cleanup time (1500), so NOT late
        let event2 = create_test_event(800, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx)
        };

        // Should NOT be late
        let is_late = outputs
            .iter()
            .any(|o| matches!(o, Output::LateEvent(_) | Output::SideOutput { .. }));
        assert!(!is_late);
        assert_eq!(operator.late_data_metrics().late_events_total(), 0);
    }
}
