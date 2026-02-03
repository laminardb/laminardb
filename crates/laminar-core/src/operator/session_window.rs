//! # Session Window Operators
//!
//! Implementation of session windows for stream processing.
//!
//! Session windows are dynamic windows that group events by activity periods
//! separated by gaps. Unlike tumbling and sliding windows which have fixed
//! boundaries, session windows grow with activity and close after inactivity.
//!
//! ## Key Characteristics
//!
//! - **Dynamic boundaries**: Sessions start with the first event and extend
//!   with each new event within the gap period
//! - **Per-key tracking**: Each key maintains independent session state
//! - **Gap-based closure**: Sessions close when no events arrive within the gap
//! - **Session merging**: Late data can merge previously separate sessions
//!
//! ## Example
//!
//! ```text
//! Gap: 30 seconds
//!
//! Events: [t=0] [t=10] [t=20]  ...gap...  [t=100] [t=110]
//!         |<---- Session 1 ---->|          |<- Session 2 ->|
//!         [0, 50)                          [100, 140)
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use laminar_core::operator::session_window::SessionWindowOperator;
//! use laminar_core::operator::window::CountAggregator;
//! use std::time::Duration;
//!
//! // Create a session window with 30-second gap
//! let operator = SessionWindowOperator::new(
//!     Duration::from_secs(30),    // gap timeout
//!     CountAggregator::new(),
//!     Duration::from_secs(60),    // allowed lateness
//! );
//! ```

use super::window::{
    Accumulator, Aggregator, ChangelogRecord, EmitStrategy, LateDataConfig, LateDataMetrics,
    ResultToI64, WindowId,
};
use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};
use crate::state::{StateStore, StateStoreExt};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fxhash::FxHashMap;
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

/// State key prefix for session state (4 bytes)
const SESSION_STATE_PREFIX: &[u8; 4] = b"ses:";

/// State key prefix for session accumulator (4 bytes)
const SESSION_ACC_PREFIX: &[u8; 4] = b"sac:";

/// Timer key prefix for session closure (1 byte)
const SESSION_TIMER_PREFIX: u8 = 0x01;

/// Static counter for generating unique operator IDs.
static SESSION_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Session state tracking start/end times and key.
///
/// This is the metadata for an active session, stored separately from
/// the accumulator state for efficient updates.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SessionState {
    /// Session start timestamp (inclusive)
    pub start: i64,
    /// Session end timestamp (exclusive, = last event time + gap)
    pub end: i64,
    /// Key bytes for this session
    pub key: Vec<u8>,
}

impl SessionState {
    /// Creates a new session state from an event timestamp.
    fn new(timestamp: i64, gap_ms: i64, key: Vec<u8>) -> Self {
        Self {
            start: timestamp,
            end: timestamp + gap_ms,
            key,
        }
    }

    /// Returns the window ID for this session.
    #[must_use]
    pub fn window_id(&self) -> WindowId {
        WindowId::new(self.start, self.end)
    }

    /// Checks if a timestamp falls within this session (including gap).
    fn contains(&self, timestamp: i64, gap_ms: i64) -> bool {
        // Event is within session if it's between start and end,
        // or within gap of the current end
        timestamp >= self.start && timestamp < self.end + gap_ms
    }

    /// Extends the session to include a new timestamp.
    fn extend(&mut self, timestamp: i64, gap_ms: i64) {
        self.start = self.start.min(timestamp);
        self.end = self.end.max(timestamp + gap_ms);
    }

    /// Merges another session into this one.
    ///
    /// # Future Enhancement
    ///
    /// This method is prepared for session merging when late data arrives
    /// that bridges two previously separate sessions.
    #[allow(dead_code)]
    fn merge(&mut self, other: &SessionState) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
    }
}

/// Creates the standard session output schema.
fn create_session_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_start", DataType::Int64, false),
        Field::new("session_end", DataType::Int64, false),
        Field::new("result", DataType::Int64, false),
    ]))
}

/// Session window operator.
///
/// Groups events by activity periods separated by gaps. Each unique key
/// maintains its own session state independently.
///
/// # Session Lifecycle
///
/// 1. **Start**: First event for a key creates a new session
/// 2. **Extend**: Events within gap period extend the session
/// 3. **Close**: Timer fires when gap expires, emitting results
/// 4. **Merge**: Late data may merge previously separate sessions
///
/// # State Management
///
/// Session state is stored using prefixed keys:
/// - `ses:<key_hash>` - Session metadata (start, end, key)
/// - `sac:<key_hash>` - Accumulator state
///
/// # Emit Strategies
///
/// - `OnWatermark`: Emit when watermark passes session end
/// - `OnUpdate`: Emit after every state update
/// - `OnWindowClose`: Only emit on final closure
/// - `Changelog`: Emit CDC records with Z-set weights
/// - `Final`: Suppress all intermediate, drop late data
pub struct SessionWindowOperator<A: Aggregator> {
    /// Gap timeout in milliseconds
    gap_ms: i64,
    /// Aggregator function
    aggregator: A,
    /// Allowed lateness for late data
    allowed_lateness_ms: i64,
    /// Active sessions by key hash (in-memory index)
    active_sessions: FxHashMap<u64, SessionState>,
    /// Pending timers by key hash
    pending_timers: FxHashMap<u64, i64>,
    /// Emit strategy
    emit_strategy: EmitStrategy,
    /// Late data configuration
    late_data_config: LateDataConfig,
    /// Late data metrics
    late_data_metrics: LateDataMetrics,
    /// Operator ID for checkpointing
    operator_id: String,
    /// Cached output schema
    output_schema: SchemaRef,
    /// Key column index for partitioning (None = global session)
    key_column: Option<usize>,
    /// Phantom data for accumulator type
    _phantom: PhantomData<A::Acc>,
}

impl<A: Aggregator> SessionWindowOperator<A>
where
    A::Acc: Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    /// Creates a new session window operator.
    ///
    /// # Arguments
    ///
    /// * `gap` - The inactivity gap that closes a session
    /// * `aggregator` - Aggregation function to apply within sessions
    /// * `allowed_lateness` - Grace period for late data after session close
    ///
    /// # Panics
    ///
    /// Panics if gap or allowed lateness does not fit in i64.
    pub fn new(gap: Duration, aggregator: A, allowed_lateness: Duration) -> Self {
        let operator_num = SESSION_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            gap_ms: i64::try_from(gap.as_millis()).expect("Gap must fit in i64"),
            aggregator,
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
            active_sessions: FxHashMap::default(),
            pending_timers: FxHashMap::default(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            operator_id: format!("session_window_{operator_num}"),
            output_schema: create_session_output_schema(),
            key_column: None,
            _phantom: PhantomData,
        }
    }

    /// Creates a new session window operator with a custom operator ID.
    ///
    /// # Panics
    ///
    /// Panics if gap or allowed lateness does not fit in i64.
    pub fn with_id(
        gap: Duration,
        aggregator: A,
        allowed_lateness: Duration,
        operator_id: String,
    ) -> Self {
        Self {
            gap_ms: i64::try_from(gap.as_millis()).expect("Gap must fit in i64"),
            aggregator,
            allowed_lateness_ms: i64::try_from(allowed_lateness.as_millis())
                .expect("Allowed lateness must fit in i64"),
            active_sessions: FxHashMap::default(),
            pending_timers: FxHashMap::default(),
            emit_strategy: EmitStrategy::default(),
            late_data_config: LateDataConfig::default(),
            late_data_metrics: LateDataMetrics::new(),
            operator_id,
            output_schema: create_session_output_schema(),
            key_column: None,
            _phantom: PhantomData,
        }
    }

    /// Sets the key column for per-key session tracking.
    ///
    /// If not set, a single global session is maintained.
    pub fn set_key_column(&mut self, column_index: usize) {
        self.key_column = Some(column_index);
    }

    /// Returns the key column index if set.
    #[must_use]
    pub fn key_column(&self) -> Option<usize> {
        self.key_column
    }

    /// Sets the emit strategy for this operator.
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

    /// Returns the gap timeout in milliseconds.
    #[must_use]
    pub fn gap_ms(&self) -> i64 {
        self.gap_ms
    }

    /// Returns the allowed lateness in milliseconds.
    #[must_use]
    pub fn allowed_lateness_ms(&self) -> i64 {
        self.allowed_lateness_ms
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn active_session_count(&self) -> usize {
        self.active_sessions.len()
    }

    /// Extracts the key from an event.
    fn extract_key(&self, event: &Event) -> Vec<u8> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;

        if let Some(col_idx) = self.key_column {
            if col_idx < event.data.num_columns() {
                let column = event.data.column(col_idx);
                if let Some(array) = column.as_primitive_opt::<Int64Type>() {
                    if !array.is_empty() && !array.is_null(0) {
                        return array.value(0).to_be_bytes().to_vec();
                    }
                }
                // Try string column
                if let Some(array) = column.as_string_opt::<i32>() {
                    if !array.is_empty() && !array.is_null(0) {
                        return array.value(0).as_bytes().to_vec();
                    }
                }
            }
        }
        // Default: global session (empty key)
        Vec::new()
    }

    /// Computes a hash for the key.
    fn key_hash(key: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = fxhash::FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Generates the state key for session metadata.
    fn session_state_key(key_hash: u64) -> [u8; 12] {
        let mut key = [0u8; 12];
        key[..4].copy_from_slice(SESSION_STATE_PREFIX);
        key[4..12].copy_from_slice(&key_hash.to_be_bytes());
        key
    }

    /// Generates the state key for session accumulator.
    fn session_acc_key(key_hash: u64) -> [u8; 12] {
        let mut key = [0u8; 12];
        key[..4].copy_from_slice(SESSION_ACC_PREFIX);
        key[4..12].copy_from_slice(&key_hash.to_be_bytes());
        key
    }

    /// Generates the timer key for session closure.
    fn timer_key(key_hash: u64) -> super::TimerKey {
        let mut key = super::TimerKey::new();
        key.push(SESSION_TIMER_PREFIX);
        key.extend_from_slice(&key_hash.to_be_bytes());
        key
    }

    /// Parses a key hash from a timer key.
    fn key_hash_from_timer(timer_key: &[u8]) -> Option<u64> {
        if timer_key.len() != 9 || timer_key[0] != SESSION_TIMER_PREFIX {
            return None;
        }
        let hash_bytes: [u8; 8] = timer_key[1..9].try_into().ok()?;
        Some(u64::from_be_bytes(hash_bytes))
    }

    /// Gets or creates a session for a key.
    fn get_or_create_session(
        &mut self,
        key_hash: u64,
        key: Vec<u8>,
        timestamp: i64,
        state: &mut dyn StateStore,
    ) -> SessionState {
        // Check in-memory cache first
        if let Some(session) = self.active_sessions.get(&key_hash) {
            return session.clone();
        }

        // Check persistent state
        let state_key = Self::session_state_key(key_hash);
        if let Ok(Some(session)) = state.get_typed::<SessionState>(&state_key) {
            self.active_sessions.insert(key_hash, session.clone());
            return session;
        }

        // Create new session
        let session = SessionState::new(timestamp, self.gap_ms, key);
        self.active_sessions.insert(key_hash, session.clone());
        session
    }

    /// Gets the accumulator for a session.
    fn get_accumulator(&self, key_hash: u64, state: &dyn StateStore) -> A::Acc {
        let acc_key = Self::session_acc_key(key_hash);
        state
            .get_typed::<A::Acc>(&acc_key)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.aggregator.create_accumulator())
    }

    /// Stores session state and accumulator.
    fn put_session(
        key_hash: u64,
        session: &SessionState,
        acc: &A::Acc,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let state_key = Self::session_state_key(key_hash);
        let acc_key = Self::session_acc_key(key_hash);

        state
            .put_typed(&state_key, session)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;
        state
            .put_typed(&acc_key, acc)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;

        Ok(())
    }

    /// Deletes session state and accumulator.
    fn delete_session(
        &mut self,
        key_hash: u64,
        state: &mut dyn StateStore,
    ) -> Result<(), OperatorError> {
        let state_key = Self::session_state_key(key_hash);
        let acc_key = Self::session_acc_key(key_hash);

        state
            .delete(&state_key)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;
        state
            .delete(&acc_key)
            .map_err(|e| OperatorError::StateAccessFailed(e.to_string()))?;

        self.active_sessions.remove(&key_hash);
        self.pending_timers.remove(&key_hash);

        Ok(())
    }

    /// Registers or updates a timer for session closure.
    fn register_timer(&mut self, key_hash: u64, session: &SessionState, ctx: &mut OperatorContext) {
        let trigger_time = session.end + self.allowed_lateness_ms;

        // Cancel previous timer if different
        if let Some(&old_time) = self.pending_timers.get(&key_hash) {
            if old_time == trigger_time {
                return; // Timer already set for correct time
            }
            // Note: We can't cancel timers, but the handler will check if session still exists
        }

        let timer_key = Self::timer_key(key_hash);
        ctx.timers
            .register_timer(trigger_time, Some(timer_key), Some(ctx.operator_index));
        self.pending_timers.insert(key_hash, trigger_time);
    }

    /// Checks if an event is late for its potential session.
    fn is_late(&self, timestamp: i64, watermark: i64) -> bool {
        // An event is late if its session would have already closed
        // Session end = timestamp + gap, cleanup = session end + allowed lateness
        let potential_cleanup = timestamp + self.gap_ms + self.allowed_lateness_ms;
        watermark >= potential_cleanup
    }

    /// Creates an output event from a session.
    fn create_output(&self, session: &SessionState, acc: &A::Acc) -> Option<Event> {
        if acc.is_empty() {
            return None;
        }

        let result = acc.result();
        let result_i64 = result.to_i64();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            vec![
                Arc::new(Int64Array::from(vec![session.start])),
                Arc::new(Int64Array::from(vec![session.end])),
                Arc::new(Int64Array::from(vec![result_i64])),
            ],
        )
        .ok()?;

        Some(Event::new(session.end, batch))
    }

    /// Finds overlapping sessions for potential merging.
    ///
    /// # Future Enhancement
    ///
    /// This method is prepared for session merging when late data arrives
    /// that bridges two previously separate sessions. Currently only single
    /// session per key is supported.
    #[allow(dead_code)]
    fn find_overlapping_sessions(&self, key_hash: u64, _timestamp: i64) -> Vec<u64> {
        // For now, we only support single session per key
        // Future: Could scan for sessions with overlapping time ranges
        if self.active_sessions.contains_key(&key_hash) {
            vec![key_hash]
        } else {
            vec![]
        }
    }
}

impl<A: Aggregator> Operator for SessionWindowOperator<A>
where
    A::Acc: 'static
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <A::Acc as Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
        + RkyvDeserialize<A::Acc, HighDeserializer<RkyvError>>,
{
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let event_time = event.timestamp;
        let mut output = OutputVec::new();

        // Update watermark
        let emitted_watermark = ctx.watermark_generator.on_event(event_time);

        // Check if event is late
        let current_wm = ctx.watermark_generator.current_watermark();
        if current_wm > i64::MIN && self.is_late(event_time, current_wm) {
            // F011B: EMIT FINAL drops late data entirely
            if self.emit_strategy.drops_late_data() {
                self.late_data_metrics.record_dropped();
                return output;
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

        // Extract key and compute hash
        let key = self.extract_key(event);
        let key_hash = Self::key_hash(&key);

        // Get or create session
        let mut session = self.get_or_create_session(key_hash, key.clone(), event_time, ctx.state);

        // Check if event extends existing session or needs a new one
        if session.contains(event_time, self.gap_ms) {
            // Extend existing session
            session.extend(event_time, self.gap_ms);
        } else if event_time < session.start {
            // Event is before session start - extend backwards
            session.extend(event_time, self.gap_ms);
        } else {
            // Event is after gap - this is a new session
            // First, emit the old session if OnUpdate strategy
            if matches!(self.emit_strategy, EmitStrategy::OnUpdate) {
                let old_acc = self.get_accumulator(key_hash, ctx.state);
                if let Some(old_event) = self.create_output(&session, &old_acc) {
                    output.push(Output::Event(old_event));
                }
            }

            // Start new session
            session = SessionState::new(event_time, self.gap_ms, key);
            // Reset accumulator for new session
            let new_acc = self.aggregator.create_accumulator();
            let _ = Self::put_session(key_hash, &session, &new_acc, ctx.state);
        }

        // Get and update accumulator
        let mut acc = self.get_accumulator(key_hash, ctx.state);
        if let Some(value) = self.aggregator.extract(event) {
            acc.add(value);
        }

        // Store updated state
        if Self::put_session(key_hash, &session, &acc, ctx.state).is_ok() {
            self.active_sessions.insert(key_hash, session.clone());
        }

        // Register timer for session closure
        self.register_timer(key_hash, &session, ctx);

        // Emit watermark if generated
        if let Some(wm) = emitted_watermark {
            output.push(Output::Watermark(wm.timestamp()));
        }

        // Handle emit strategy
        match &self.emit_strategy {
            EmitStrategy::OnUpdate => {
                if let Some(event) = self.create_output(&session, &acc) {
                    output.push(Output::Event(event));
                }
            }
            EmitStrategy::Changelog => {
                if let Some(event) = self.create_output(&session, &acc) {
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

        output
    }

    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        let mut output = OutputVec::new();

        // Parse key hash from timer
        let Some(key_hash) = Self::key_hash_from_timer(&timer.key) else {
            return output;
        };

        // Check if this timer is still valid
        let Some(expected_time) = self.pending_timers.get(&key_hash) else {
            return output; // Timer was cancelled or session no longer exists
        };

        if *expected_time != timer.timestamp {
            return output; // Stale timer, session was extended
        }

        // Get session state
        let Some(session) = self.active_sessions.get(&key_hash).cloned() else {
            self.pending_timers.remove(&key_hash);
            return output;
        };

        // Get accumulator
        let acc = self.get_accumulator(key_hash, ctx.state);

        // Create output
        if let Some(event) = self.create_output(&session, &acc) {
            match &self.emit_strategy {
                EmitStrategy::Changelog => {
                    let record = ChangelogRecord::insert(event, ctx.processing_time);
                    output.push(Output::Changelog(record));
                }
                _ => {
                    output.push(Output::Event(event));
                }
            }
        }

        // Clean up session
        let _ = self.delete_session(key_hash, ctx.state);

        output
    }

    fn checkpoint(&self) -> OperatorState {
        // Serialize active session key hashes and their timer times
        let checkpoint_data: Vec<(u64, i64)> =
            self.pending_timers.iter().map(|(&k, &v)| (k, v)).collect();

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

        if state.data.is_empty() {
            return Ok(());
        }

        let archived = rkyv::access::<rkyv::Archived<Vec<(u64, i64)>>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let timers: Vec<(u64, i64)> = rkyv::deserialize::<Vec<(u64, i64)>, RkyvError>(archived)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.pending_timers = timers.into_iter().collect();
        // Note: active_sessions will be populated lazily from state store

        Ok(())
    }
}

/// Session metrics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct SessionMetrics {
    /// Total sessions created
    pub sessions_created: u64,
    /// Total sessions closed
    pub sessions_closed: u64,
    /// Total sessions merged
    pub sessions_merged: u64,
    /// Current active sessions
    pub active_sessions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::window::{CountAccumulator, CountAggregator, SumAggregator};
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
        Event::new(timestamp, batch)
    }

    fn create_keyed_event(timestamp: i64, key: i64, value: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![key])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap();
        Event::new(timestamp, batch)
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
    fn test_session_operator_creation() {
        let aggregator = CountAggregator::new();
        let operator = SessionWindowOperator::new(
            Duration::from_secs(30),
            aggregator,
            Duration::from_secs(60),
        );

        assert_eq!(operator.gap_ms(), 30_000);
        assert_eq!(operator.allowed_lateness_ms(), 60_000);
        assert_eq!(operator.active_session_count(), 0);
        assert_eq!(*operator.emit_strategy(), EmitStrategy::OnWatermark);
    }

    #[test]
    fn test_session_operator_with_id() {
        let aggregator = CountAggregator::new();
        let operator = SessionWindowOperator::with_id(
            Duration::from_secs(30),
            aggregator,
            Duration::from_secs(0),
            "test_session".to_string(),
        );

        assert_eq!(operator.operator_id, "test_session");
    }

    #[test]
    fn test_session_state_creation() {
        let state = SessionState::new(1000, 5000, vec![1, 2, 3]);

        assert_eq!(state.start, 1000);
        assert_eq!(state.end, 6000); // start + gap
        assert_eq!(state.key, vec![1, 2, 3]);
    }

    #[test]
    fn test_session_state_contains() {
        let state = SessionState::new(1000, 5000, vec![]);

        // Within session
        assert!(state.contains(1000, 5000));
        assert!(state.contains(3000, 5000));
        assert!(state.contains(5999, 5000)); // Just before end

        // Within gap extension
        assert!(state.contains(6000, 5000));
        assert!(state.contains(10999, 5000)); // end + gap - 1

        // Outside
        assert!(!state.contains(999, 5000)); // Before start
        assert!(!state.contains(11000, 5000)); // After end + gap
    }

    #[test]
    fn test_session_state_extend() {
        let mut state = SessionState::new(1000, 5000, vec![]);

        // Extend forward
        state.extend(8000, 5000);
        assert_eq!(state.start, 1000);
        assert_eq!(state.end, 13000); // 8000 + 5000

        // Extend backward
        state.extend(500, 5000);
        assert_eq!(state.start, 500);
        assert_eq!(state.end, 13000); // Unchanged (max)
    }

    #[test]
    fn test_session_state_merge() {
        let mut state1 = SessionState::new(1000, 5000, vec![]);
        let state2 = SessionState::new(8000, 5000, vec![]);

        state1.merge(&state2);
        assert_eq!(state1.start, 1000);
        assert_eq!(state1.end, 13000); // max(6000, 13000)
    }

    #[test]
    fn test_session_single_event() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        let event = create_test_event(500, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        assert_eq!(operator.active_session_count(), 1);
        assert_eq!(operator.pending_timers.len(), 1);
    }

    #[test]
    fn test_session_multiple_events_same_session() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Events within gap (1000ms)
        for ts in [100, 500, 900, 1500] {
            let event = create_test_event(ts, 1);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // All events should be in the same session
        assert_eq!(operator.active_session_count(), 1);

        // Verify accumulator
        let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
        let acc: CountAccumulator = operator.get_accumulator(key_hash, &state);
        assert_eq!(acc.result(), 4);
    }

    #[test]
    fn test_session_gap_creates_new_session() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_emit_strategy(EmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First session
        let event1 = create_test_event(100, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Gap > 1000ms, should create new session
        let event2 = create_test_event(3000, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx)
        };

        // Should have emitted old session (OnUpdate) and new session update
        let event_count = outputs
            .iter()
            .filter(|o| matches!(o, Output::Event(_)))
            .count();
        assert!(event_count >= 1);
    }

    #[test]
    fn test_session_timer_triggers_emission() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Create session
        let event = create_test_event(500, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Get the pending timer
        let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
        let timer_time = *operator.pending_timers.get(&key_hash).unwrap();

        // Fire timer
        let timer = Timer {
            key: SessionWindowOperator::<CountAggregator>::timer_key(key_hash),
            timestamp: timer_time,
        };

        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(timer, &mut ctx)
        };

        assert_eq!(outputs.len(), 1);
        match &outputs[0] {
            Output::Event(e) => {
                assert_eq!(e.timestamp, 1500); // 500 + gap (1000)
                let result = e
                    .data
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(result, 1);
            }
            _ => panic!("Expected Event output"),
        }

        // Session should be cleaned up
        assert_eq!(operator.active_session_count(), 0);
    }

    #[test]
    fn test_session_keyed_tracking() {
        let aggregator = SumAggregator::new(1); // Sum column 1 (value)
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_key_column(0); // Key by column 0

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Events for key 1
        let event1 = create_keyed_event(100, 1, 10);
        let event2 = create_keyed_event(500, 1, 20);

        // Events for key 2
        let event3 = create_keyed_event(200, 2, 100);
        let event4 = create_keyed_event(600, 2, 200);

        for event in [event1, event2, event3, event4] {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Should have 2 active sessions (one per key)
        assert_eq!(operator.active_session_count(), 2);
    }

    #[test]
    fn test_session_late_event_dropped() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark far ahead
        let event1 = create_test_event(10000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process late event
        let late_event = create_test_event(100, 1);
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
    fn test_session_late_event_side_output() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_late_data_config(LateDataConfig::with_side_output("late".to_string()));

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark
        let event1 = create_test_event(10000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process late event
        let late_event = create_test_event(100, 1);
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
    fn test_session_emit_on_update() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_emit_strategy(EmitStrategy::OnUpdate);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        let event = create_test_event(500, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };

        // Should emit intermediate result
        let event_count = outputs
            .iter()
            .filter(|o| matches!(o, Output::Event(_)))
            .count();
        assert_eq!(event_count, 1);
    }

    #[test]
    fn test_session_emit_changelog() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_emit_strategy(EmitStrategy::Changelog);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        let event = create_test_event(500, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx)
        };

        // Should emit changelog record
        let changelog_count = outputs
            .iter()
            .filter(|o| matches!(o, Output::Changelog(_)))
            .count();
        assert_eq!(changelog_count, 1);
    }

    #[test]
    fn test_session_emit_final_drops_late() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        operator.set_emit_strategy(EmitStrategy::Final);

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(0);

        // Advance watermark
        let event1 = create_test_event(10000, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        // Process late event - should be silently dropped
        let late_event = create_test_event(100, 1);
        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&late_event, &mut ctx)
        };

        // Should NOT emit LateEvent (dropped silently)
        assert!(outputs.is_empty());
        assert_eq!(operator.late_data_metrics().late_events_dropped(), 1);
    }

    #[test]
    fn test_session_checkpoint_restore() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator.clone(),
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Create some sessions
        for ts in [100, 500] {
            let event = create_test_event(ts, 1);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Create new operator and restore
        let mut restored = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );
        restored.restore(checkpoint).unwrap();

        // Pending timers should be restored
        assert_eq!(restored.pending_timers.len(), 1);
    }

    #[test]
    fn test_session_stale_timer_ignored() {
        let aggregator = CountAggregator::new();
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Create session
        let event1 = create_test_event(500, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event1, &mut ctx);
        }

        let key_hash = SessionWindowOperator::<CountAggregator>::key_hash(&[]);
        let old_timer_time = *operator.pending_timers.get(&key_hash).unwrap();

        // Extend session
        let event2 = create_test_event(1200, 1);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event2, &mut ctx);
        }

        // Fire stale timer
        let stale_timer = Timer {
            key: SessionWindowOperator::<CountAggregator>::timer_key(key_hash),
            timestamp: old_timer_time,
        };

        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(stale_timer, &mut ctx)
        };

        // Stale timer should be ignored
        assert!(outputs.is_empty());
        // Session should still be active
        assert_eq!(operator.active_session_count(), 1);
    }

    #[test]
    fn test_session_window_id() {
        let state = SessionState::new(1000, 5000, vec![]);
        let window_id = state.window_id();

        assert_eq!(window_id.start, 1000);
        assert_eq!(window_id.end, 6000);
    }

    #[test]
    fn test_timer_key_roundtrip() {
        let key_hash = 0x1234_5678_9ABC_DEF0u64;
        let timer_key = SessionWindowOperator::<CountAggregator>::timer_key(key_hash);
        let parsed = SessionWindowOperator::<CountAggregator>::key_hash_from_timer(&timer_key);

        assert_eq!(parsed, Some(key_hash));
    }

    #[test]
    fn test_timer_key_invalid() {
        // Wrong prefix
        let invalid1 = vec![0x02, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(SessionWindowOperator::<CountAggregator>::key_hash_from_timer(&invalid1).is_none());

        // Wrong length
        let invalid2 = vec![SESSION_TIMER_PREFIX, 0, 0, 0];
        assert!(SessionWindowOperator::<CountAggregator>::key_hash_from_timer(&invalid2).is_none());
    }

    #[test]
    fn test_session_sum_aggregation() {
        let aggregator = SumAggregator::new(0);
        let mut operator = SessionWindowOperator::with_id(
            Duration::from_millis(1000),
            aggregator,
            Duration::from_millis(0),
            "test_op".to_string(),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process events with different values
        for (ts, value) in [(100, 10), (500, 20), (800, 30)] {
            let event = create_test_event(ts, value);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.process(&event, &mut ctx);
        }

        // Fire timer
        let key_hash = SessionWindowOperator::<SumAggregator>::key_hash(&[]);
        let timer_time = *operator.pending_timers.get(&key_hash).unwrap();
        let timer = Timer {
            key: SessionWindowOperator::<SumAggregator>::timer_key(key_hash),
            timestamp: timer_time,
        };

        let outputs = {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            operator.on_timer(timer, &mut ctx)
        };

        match &outputs[0] {
            Output::Event(e) => {
                let result = e
                    .data
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                assert_eq!(result, 60); // 10 + 20 + 30
            }
            _ => panic!("Expected Event output"),
        }
    }

    #[test]
    fn test_session_output_schema() {
        let schema = create_session_output_schema();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "session_start");
        assert_eq!(schema.field(1).name(), "session_end");
        assert_eq!(schema.field(2).name(), "result");
    }
}
