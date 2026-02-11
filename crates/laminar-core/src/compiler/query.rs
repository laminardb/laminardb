//! Streaming query lifecycle management.
//!
//! [`StreamingQuery`] is the unified runtime object that wires compiled (or fallback)
//! pipelines to their Ring 0 / Ring 1 bridges and tracks lifecycle state across the
//! Ring 0 / Ring 1 boundary.
//!
//! # Usage
//!
//! ```ignore
//! let query = StreamingQueryBuilder::new("SELECT ts, val FROM stream WHERE val > 10")
//!     .add_pipeline(executable, bridge, consumer, schema)
//!     .build()?;
//! query.start()?;
//! query.submit_row(&row, event_time, key_hash)?;
//! let actions = query.poll_ring1();
//! query.stop()?;
//! ```

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use smallvec::SmallVec;

use super::fallback::ExecutablePipeline;
use super::metrics::{
    QueryConfig, QueryError, QueryId, QueryMetadata, QueryMetrics, QueryState, SubmitResult,
};
use super::pipeline::PipelineAction;
use super::pipeline_bridge::{BridgeConsumer, PipelineBridge, Ring1Action};
use super::row::{EventRow, RowSchema};

// ────────────────────────────── Builder ──────────────────────────────

/// Builder for constructing a [`StreamingQuery`] from pre-compiled components.
///
/// Each pipeline is added as a triplet: `(ExecutablePipeline, PipelineBridge, BridgeConsumer)`
/// with a corresponding output schema. The builder validates that all parallel vectors
/// have equal length before producing the query.
pub struct StreamingQueryBuilder {
    sql: String,
    pipelines: Vec<ExecutablePipeline>,
    bridges: Vec<PipelineBridge>,
    consumers: Vec<BridgeConsumer>,
    schemas: Vec<Arc<RowSchema>>,
    config: QueryConfig,
    metadata: QueryMetadata,
}

impl StreamingQueryBuilder {
    /// Creates a new builder for the given SQL query text.
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            pipelines: Vec::new(),
            bridges: Vec::new(),
            consumers: Vec::new(),
            schemas: Vec::new(),
            config: QueryConfig::default(),
            metadata: QueryMetadata::default(),
        }
    }

    /// Adds a pipeline triplet (executable + bridge + consumer) with its output schema.
    #[must_use]
    pub fn add_pipeline(
        mut self,
        executable: ExecutablePipeline,
        bridge: PipelineBridge,
        consumer: BridgeConsumer,
        schema: Arc<RowSchema>,
    ) -> Self {
        self.pipelines.push(executable);
        self.bridges.push(bridge);
        self.consumers.push(consumer);
        self.schemas.push(schema);
        self
    }

    /// Sets the query configuration.
    #[must_use]
    pub fn with_config(mut self, config: QueryConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the compilation metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: QueryMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Builds the [`StreamingQuery`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::NoPipelines`] if no pipelines were added.
    /// Returns [`QueryError::Build`] if the parallel vectors have mismatched lengths.
    pub fn build(self) -> Result<StreamingQuery, QueryError> {
        if self.pipelines.is_empty() {
            return Err(QueryError::NoPipelines);
        }

        let n = self.pipelines.len();
        if self.bridges.len() != n || self.consumers.len() != n || self.schemas.len() != n {
            return Err(QueryError::Build(format!(
                "mismatched component counts: pipelines={n}, bridges={}, consumers={}, schemas={}",
                self.bridges.len(),
                self.consumers.len(),
                self.schemas.len()
            )));
        }

        let id = QueryId(fxhash::hash64(self.sql.as_bytes()));

        let output_buffers = (0..n)
            .map(|_| vec![0u8; self.config.output_buffer_size])
            .collect();

        Ok(StreamingQuery {
            id,
            sql: self.sql,
            pipelines: self.pipelines,
            bridges: self.bridges,
            consumers: self.consumers,
            schemas: self.schemas,
            output_buffers,
            metadata: self.metadata,
            state: QueryState::Ready,
        })
    }
}

// ────────────────────────────── StreamingQuery ───────────────────────

/// A running streaming query that connects compiled pipelines to Ring 1 via bridges.
///
/// `StreamingQuery` manages the lifecycle (start/pause/resume/stop), processes
/// events through compiled or fallback pipelines, and produces [`Ring1Action`]s
/// for downstream stateful operators.
pub struct StreamingQuery {
    id: QueryId,
    sql: String,
    pipelines: Vec<ExecutablePipeline>,
    bridges: Vec<PipelineBridge>,
    consumers: Vec<BridgeConsumer>,
    schemas: Vec<Arc<RowSchema>>,
    output_buffers: Vec<Vec<u8>>,
    metadata: QueryMetadata,
    state: QueryState,
}

impl StreamingQuery {
    // ── Lifecycle ────────────────────────────────────────────────────

    /// Transitions from [`QueryState::Ready`] to [`QueryState::Running`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Ready` state.
    pub fn start(&mut self) -> Result<(), QueryError> {
        if self.state != QueryState::Ready {
            return Err(QueryError::InvalidState {
                expected: "Ready",
                actual: self.state,
            });
        }
        self.state = QueryState::Running;
        Ok(())
    }

    /// Transitions from [`QueryState::Running`] to [`QueryState::Paused`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Running` state.
    pub fn pause(&mut self) -> Result<(), QueryError> {
        if self.state != QueryState::Running {
            return Err(QueryError::InvalidState {
                expected: "Running",
                actual: self.state,
            });
        }
        self.state = QueryState::Paused;
        Ok(())
    }

    /// Transitions from [`QueryState::Paused`] to [`QueryState::Running`].
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Paused` state.
    pub fn resume(&mut self) -> Result<(), QueryError> {
        if self.state != QueryState::Paused {
            return Err(QueryError::InvalidState {
                expected: "Paused",
                actual: self.state,
            });
        }
        self.state = QueryState::Running;
        Ok(())
    }

    /// Transitions to [`QueryState::Stopped`] from any non-terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if already in `Stopped` state.
    pub fn stop(&mut self) -> Result<(), QueryError> {
        if self.state == QueryState::Stopped {
            return Err(QueryError::InvalidState {
                expected: "Ready|Running|Paused",
                actual: self.state,
            });
        }
        self.state = QueryState::Stopped;
        Ok(())
    }

    // ── Ring 0 — event submission ───────────────────────────────────

    /// Submits an event row for processing through all pipelines.
    ///
    /// For compiled pipelines, the row is executed through the native function and
    /// the result is sent to the corresponding bridge. For fallback pipelines, the
    /// input row is sent directly (passthrough) for Ring 1 interpreted execution.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Running` state.
    /// Returns [`QueryError::PipelineError`] if a compiled pipeline returns `Error`.
    /// Returns [`QueryError::Bridge`] if a bridge send fails.
    pub fn submit_row(
        &mut self,
        row: &EventRow<'_>,
        event_time: i64,
        key_hash: u64,
    ) -> Result<SubmitResult, QueryError> {
        if self.state != QueryState::Running {
            return Err(QueryError::InvalidState {
                expected: "Running",
                actual: self.state,
            });
        }

        let mut any_emitted = false;

        for i in 0..self.pipelines.len() {
            match &self.pipelines[i] {
                ExecutablePipeline::Compiled(compiled) => {
                    let start = Instant::now();
                    let action = {
                        let output_buf = &mut self.output_buffers[i];
                        // SAFETY: input row data matches the compiled pipeline's input schema.
                        // Output buffer is pre-allocated to output_buffer_size bytes.
                        unsafe { compiled.execute(row.data().as_ptr(), output_buf.as_mut_ptr()) }
                    };
                    #[allow(clippy::cast_possible_truncation)]
                    let elapsed_ns = start.elapsed().as_nanos() as u64;
                    compiled.stats.record(action, elapsed_ns);

                    match action {
                        PipelineAction::Emit => {
                            // Send the output row through the bridge.
                            let output_schema = &compiled.output_schema;
                            let output_row = EventRow::new(
                                &self.output_buffers[i][..output_schema.min_row_size()],
                                output_schema,
                            );
                            self.bridges[i].send_event(&output_row, event_time, key_hash)?;
                            any_emitted = true;
                        }
                        PipelineAction::Drop => {
                            // Filtered out — nothing to send.
                        }
                        PipelineAction::Error => {
                            return Err(QueryError::PipelineError { pipeline_idx: i });
                        }
                    }
                }
                ExecutablePipeline::Fallback { .. } => {
                    // Passthrough: send the input row directly for Ring 1 processing.
                    self.bridges[i].send_event(row, event_time, key_hash)?;
                    any_emitted = true;
                }
            }
        }

        if any_emitted {
            Ok(SubmitResult::Emitted)
        } else {
            Ok(SubmitResult::Filtered)
        }
    }

    // ── Ring 0 — control messages ───────────────────────────────────

    /// Sends a watermark advance through all bridges.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Running` state.
    /// Returns [`QueryError::Bridge`] if any bridge send fails.
    pub fn advance_watermark(&self, timestamp: i64) -> Result<(), QueryError> {
        if self.state != QueryState::Running {
            return Err(QueryError::InvalidState {
                expected: "Running",
                actual: self.state,
            });
        }
        for bridge in &self.bridges {
            bridge.send_watermark(timestamp)?;
        }
        Ok(())
    }

    /// Sends a checkpoint barrier through all bridges.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if not in `Running` state.
    /// Returns [`QueryError::Bridge`] if any bridge send fails.
    pub fn checkpoint(&self, epoch: u64) -> Result<(), QueryError> {
        if self.state != QueryState::Running {
            return Err(QueryError::InvalidState {
                expected: "Running",
                actual: self.state,
            });
        }
        for bridge in &self.bridges {
            bridge.send_checkpoint(epoch)?;
        }
        Ok(())
    }

    /// Sends an end-of-stream marker through all bridges.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Bridge`] if any bridge send fails.
    pub fn send_eof(&self) -> Result<(), QueryError> {
        for bridge in &self.bridges {
            bridge.send_eof()?;
        }
        Ok(())
    }

    // ── Ring 1 — output polling ─────────────────────────────────────

    /// Drains all consumers and returns concatenated Ring 1 actions.
    pub fn poll_ring1(&mut self) -> SmallVec<[Ring1Action; 4]> {
        let mut actions: SmallVec<[Ring1Action; 4]> = SmallVec::new();
        for consumer in &mut self.consumers {
            actions.extend(consumer.drain());
        }
        actions
    }

    /// Checks all consumers for latency-triggered flushes.
    pub fn check_latency_flush(&mut self) -> SmallVec<[Ring1Action; 4]> {
        let mut actions: SmallVec<[Ring1Action; 4]> = SmallVec::new();
        for consumer in &mut self.consumers {
            if let Some(action) = consumer.check_latency_flush() {
                actions.push(action);
            }
        }
        actions
    }

    // ── Hot-swap ────────────────────────────────────────────────────

    /// Swaps this query's pipelines, bridges, and consumers with those from `new`.
    ///
    /// Both queries must have the same number of pipelines. The new query
    /// inherits the current `Running` or `Paused` state.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::InvalidState`] if in `Ready` or `Stopped` state.
    /// Returns [`QueryError::IncompatibleSchemas`] if pipeline counts differ.
    pub fn swap(&mut self, mut new: StreamingQuery) -> Result<StreamingQuery, QueryError> {
        if self.state != QueryState::Running && self.state != QueryState::Paused {
            return Err(QueryError::InvalidState {
                expected: "Running|Paused",
                actual: self.state,
            });
        }
        if self.pipelines.len() != new.pipelines.len() {
            return Err(QueryError::IncompatibleSchemas(format!(
                "pipeline count mismatch: current={}, new={}",
                self.pipelines.len(),
                new.pipelines.len()
            )));
        }

        // Swap internals, new query gets old components.
        std::mem::swap(&mut self.pipelines, &mut new.pipelines);
        std::mem::swap(&mut self.bridges, &mut new.bridges);
        std::mem::swap(&mut self.consumers, &mut new.consumers);
        std::mem::swap(&mut self.schemas, &mut new.schemas);
        std::mem::swap(&mut self.output_buffers, &mut new.output_buffers);
        std::mem::swap(&mut self.metadata, &mut new.metadata);
        std::mem::swap(&mut self.sql, &mut new.sql);
        self.id = QueryId(fxhash::hash64(self.sql.as_bytes()));
        new.state = QueryState::Stopped;

        Ok(new)
    }

    // ── Accessors ───────────────────────────────────────────────────

    /// Returns the query identifier.
    #[must_use]
    pub fn id(&self) -> QueryId {
        self.id
    }

    /// Returns the SQL text for this query.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> QueryState {
        self.state
    }

    /// Returns compilation metadata.
    #[must_use]
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    /// Returns the number of pipelines in this query.
    #[must_use]
    pub fn pipeline_count(&self) -> usize {
        self.pipelines.len()
    }

    /// Aggregates runtime metrics from all pipelines and bridges.
    #[must_use]
    pub fn metrics(&self) -> QueryMetrics {
        let mut m = QueryMetrics::default();

        for pipeline in &self.pipelines {
            match pipeline {
                ExecutablePipeline::Compiled(compiled) => {
                    m.pipelines_compiled += 1;
                    m.ring0_events_in += compiled.stats.events_processed.load(Ordering::Relaxed);
                    m.ring0_events_out += compiled.stats.events_emitted.load(Ordering::Relaxed);
                    m.ring0_events_dropped += compiled.stats.events_dropped.load(Ordering::Relaxed);
                    m.ring0_total_ns += compiled.stats.total_ns.load(Ordering::Relaxed);
                }
                ExecutablePipeline::Fallback { .. } => {
                    m.pipelines_fallback += 1;
                }
            }
        }

        for consumer in &self.consumers {
            let snap = consumer.stats().snapshot();
            m.bridge_backpressure_drops += snap.events_dropped;
            m.bridge_batches_flushed += snap.batches_flushed;
            m.ring1_rows_flushed += snap.rows_flushed;
        }

        m
    }
}

impl std::fmt::Debug for StreamingQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingQuery")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("pipeline_count", &self.pipelines.len())
            .finish_non_exhaustive()
    }
}

// ────────────────────────────── Tests ────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::pipeline::{CompiledPipeline, PipelineId};
    use crate::compiler::pipeline_bridge::create_pipeline_bridge;
    use crate::compiler::policy::{BackpressureStrategy, BatchPolicy};
    use crate::compiler::row::MutableEventRow;
    use arrow_schema::{DataType, Field, Schema};
    use bumpalo::Bump;

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt)| Field::new(name, dt, false))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_row_schema(fields: Vec<(&str, DataType)>) -> Arc<RowSchema> {
        let arrow = make_schema(fields);
        Arc::new(RowSchema::from_arrow(&arrow).unwrap())
    }

    /// Creates a Fallback pipeline + bridge + consumer triplet.
    fn make_fallback(
        id: u32,
        row_schema: &Arc<RowSchema>,
    ) -> (
        ExecutablePipeline,
        PipelineBridge,
        BridgeConsumer,
        Arc<RowSchema>,
    ) {
        let exec = ExecutablePipeline::Fallback {
            pipeline_id: PipelineId(id),
            reason: crate::compiler::error::CompileError::UnsupportedExpr(
                "test fallback".to_string(),
            ),
        };
        let (bridge, consumer) = create_pipeline_bridge(
            Arc::clone(row_schema),
            64,
            1024,
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();
        (exec, bridge, consumer, Arc::clone(row_schema))
    }

    /// Creates a Compiled pipeline (always-emit) + bridge + consumer triplet.
    fn make_compiled_emit(
        id: u32,
        row_schema: &Arc<RowSchema>,
    ) -> (
        ExecutablePipeline,
        PipelineBridge,
        BridgeConsumer,
        Arc<RowSchema>,
    ) {
        unsafe extern "C" fn always_emit(input: *const u8, output: *mut u8) -> u8 {
            // Copy 64 bytes from input to output as a simple passthrough.
            std::ptr::copy_nonoverlapping(input, output, 64);
            1 // Emit
        }

        let compiled = Arc::new(CompiledPipeline::new(
            PipelineId(id),
            always_emit,
            Arc::clone(row_schema),
            Arc::clone(row_schema),
        ));
        let exec = ExecutablePipeline::Compiled(compiled);
        let (bridge, consumer) = create_pipeline_bridge(
            Arc::clone(row_schema),
            64,
            1024,
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();
        (exec, bridge, consumer, Arc::clone(row_schema))
    }

    /// Creates a Compiled pipeline (always-drop) + bridge + consumer triplet.
    fn make_compiled_drop(
        id: u32,
        row_schema: &Arc<RowSchema>,
    ) -> (
        ExecutablePipeline,
        PipelineBridge,
        BridgeConsumer,
        Arc<RowSchema>,
    ) {
        unsafe extern "C" fn always_drop(_: *const u8, _: *mut u8) -> u8 {
            0 // Drop
        }

        let compiled = Arc::new(CompiledPipeline::new(
            PipelineId(id),
            always_drop,
            Arc::clone(row_schema),
            Arc::clone(row_schema),
        ));
        let exec = ExecutablePipeline::Compiled(compiled);
        let (bridge, consumer) = create_pipeline_bridge(
            Arc::clone(row_schema),
            64,
            1024,
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();
        (exec, bridge, consumer, Arc::clone(row_schema))
    }

    fn make_event_row<'a>(
        arena: &'a Bump,
        schema: &'a RowSchema,
        ts: i64,
        val: f64,
    ) -> EventRow<'a> {
        let mut row = MutableEventRow::new_in(arena, schema, 0);
        row.set_i64(0, ts);
        row.set_f64(1, val);
        row.freeze()
    }

    fn default_schema() -> Arc<RowSchema> {
        make_row_schema(vec![("ts", DataType::Int64), ("val", DataType::Float64)])
    }

    fn build_query_with_fallback(sql: &str) -> StreamingQuery {
        let schema = default_schema();
        let (exec, bridge, consumer, s) = make_fallback(0, &schema);
        StreamingQueryBuilder::new(sql)
            .add_pipeline(exec, bridge, consumer, s)
            .build()
            .unwrap()
    }

    fn build_query_with_compiled(sql: &str) -> StreamingQuery {
        let schema = default_schema();
        let (exec, bridge, consumer, s) = make_compiled_emit(0, &schema);
        StreamingQueryBuilder::new(sql)
            .add_pipeline(exec, bridge, consumer, s)
            .build()
            .unwrap()
    }

    // ── Builder tests ───────────────────────────────────────────────

    #[test]
    fn builder_empty_error() {
        let result = StreamingQueryBuilder::new("SELECT 1").build();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::NoPipelines));
    }

    #[test]
    fn builder_single_fallback() {
        let query = build_query_with_fallback("SELECT * FROM t");
        assert_eq!(query.pipeline_count(), 1);
        assert_eq!(query.state(), QueryState::Ready);
    }

    #[test]
    fn builder_single_compiled() {
        let query = build_query_with_compiled("SELECT * FROM t WHERE val > 10");
        assert_eq!(query.pipeline_count(), 1);
        assert_eq!(query.state(), QueryState::Ready);
    }

    #[test]
    fn builder_multiple_pipelines() {
        let schema = default_schema();
        let (e1, b1, c1, s1) = make_fallback(0, &schema);
        let (e2, b2, c2, s2) = make_compiled_emit(1, &schema);
        let query = StreamingQueryBuilder::new("SELECT * FROM t")
            .add_pipeline(e1, b1, c1, s1)
            .add_pipeline(e2, b2, c2, s2)
            .build()
            .unwrap();
        assert_eq!(query.pipeline_count(), 2);
    }

    // ── Lifecycle tests ─────────────────────────────────────────────

    #[test]
    fn lifecycle_ready_to_running() {
        let mut query = build_query_with_fallback("SELECT 1");
        assert!(query.start().is_ok());
        assert_eq!(query.state(), QueryState::Running);
    }

    #[test]
    fn lifecycle_running_to_paused() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        assert!(query.pause().is_ok());
        assert_eq!(query.state(), QueryState::Paused);
    }

    #[test]
    fn lifecycle_paused_to_running() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        query.pause().unwrap();
        assert!(query.resume().is_ok());
        assert_eq!(query.state(), QueryState::Running);
    }

    #[test]
    fn lifecycle_running_to_stopped() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        assert!(query.stop().is_ok());
        assert_eq!(query.state(), QueryState::Stopped);
    }

    #[test]
    fn lifecycle_stopped_terminal() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        query.stop().unwrap();

        // Cannot start, pause, resume, or stop again.
        assert!(query.start().is_err());
        assert!(query.pause().is_err());
        assert!(query.resume().is_err());
        assert!(query.stop().is_err());
    }

    // ── Submit tests ────────────────────────────────────────────────

    #[test]
    fn submit_requires_running() {
        let schema = default_schema();
        let mut query = build_query_with_fallback("SELECT 1");
        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 1.0);

        // Not started yet — should fail.
        let result = query.submit_row(&row, 1000, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::InvalidState { .. }
        ));
    }

    #[test]
    fn submit_fallback_passthrough() {
        let schema = default_schema();
        let mut query = build_query_with_fallback("SELECT * FROM t");
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 42.0);
        let result = query.submit_row(&row, 1000, 0).unwrap();
        assert_eq!(result, SubmitResult::Emitted);

        // Flush via watermark and poll.
        query.advance_watermark(2000).unwrap();
        let actions = query.poll_ring1();
        assert!(!actions.is_empty());
    }

    #[test]
    fn submit_compiled_emit() {
        let schema = default_schema();
        let mut query = build_query_with_compiled("SELECT * FROM t WHERE val > 0");
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 5.0);
        let result = query.submit_row(&row, 1000, 0).unwrap();
        assert_eq!(result, SubmitResult::Emitted);
    }

    #[test]
    fn submit_compiled_filter_drop() {
        let schema = default_schema();
        let (exec, bridge, consumer, s) = make_compiled_drop(0, &schema);
        let mut query = StreamingQueryBuilder::new("SELECT * FROM t WHERE val > 100")
            .add_pipeline(exec, bridge, consumer, s)
            .build()
            .unwrap();
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 5.0);
        let result = query.submit_row(&row, 1000, 0).unwrap();
        assert_eq!(result, SubmitResult::Filtered);
    }

    #[test]
    fn submit_multiple_pipelines() {
        let schema = default_schema();
        let (e1, b1, c1, s1) = make_compiled_emit(0, &schema);
        let (e2, b2, c2, s2) = make_fallback(1, &schema);
        let mut query = StreamingQueryBuilder::new("SELECT * FROM t")
            .add_pipeline(e1, b1, c1, s1)
            .add_pipeline(e2, b2, c2, s2)
            .build()
            .unwrap();
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 1.0);
        let result = query.submit_row(&row, 1000, 0).unwrap();
        assert_eq!(result, SubmitResult::Emitted);
    }

    #[test]
    fn submit_advance_watermark() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        assert!(query.advance_watermark(5000).is_ok());
    }

    #[test]
    fn submit_checkpoint() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        assert!(query.checkpoint(42).is_ok());
    }

    // ── Poll tests ──────────────────────────────────────────────────

    #[test]
    fn poll_empty() {
        let mut query = build_query_with_fallback("SELECT 1");
        query.start().unwrap();
        let actions = query.poll_ring1();
        assert!(actions.is_empty());
    }

    #[test]
    fn poll_with_events() {
        let schema = default_schema();
        let mut query = build_query_with_fallback("SELECT * FROM t");
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 1.0);
        query.submit_row(&row, 1000, 0).unwrap();
        query.advance_watermark(2000).unwrap();

        let actions = query.poll_ring1();
        assert!(!actions.is_empty());
        // Should have ProcessBatch + AdvanceWatermark.
        assert!(actions.len() >= 2);
    }

    #[test]
    fn poll_watermark_flush() {
        let schema = default_schema();
        let mut query = build_query_with_fallback("SELECT * FROM t");
        query.start().unwrap();

        let arena = Bump::new();
        for i in 0..3 {
            let row = make_event_row(&arena, &schema, i * 100, i as f64);
            query.submit_row(&row, i * 100, 0).unwrap();
        }
        query.advance_watermark(1000).unwrap();

        let actions = query.poll_ring1();
        let batch_count = actions
            .iter()
            .filter(|a| matches!(a, Ring1Action::ProcessBatch(_)))
            .count();
        assert!(batch_count >= 1);
    }

    #[test]
    fn poll_multiple_consumers() {
        let schema = default_schema();
        let (e1, b1, c1, s1) = make_fallback(0, &schema);
        let (e2, b2, c2, s2) = make_fallback(1, &schema);
        let mut query = StreamingQueryBuilder::new("SELECT * FROM t")
            .add_pipeline(e1, b1, c1, s1)
            .add_pipeline(e2, b2, c2, s2)
            .build()
            .unwrap();
        query.start().unwrap();

        let arena = Bump::new();
        let row = make_event_row(&arena, &schema, 1000, 1.0);
        query.submit_row(&row, 1000, 0).unwrap();
        query.advance_watermark(2000).unwrap();

        let actions = query.poll_ring1();
        // Both consumers should have actions.
        let watermark_count = actions
            .iter()
            .filter(|a| matches!(a, Ring1Action::AdvanceWatermark(_)))
            .count();
        assert_eq!(watermark_count, 2);
    }

    // ── Hot-swap tests ──────────────────────────────────────────────

    #[test]
    fn swap_compatible() {
        let mut query = build_query_with_fallback("SELECT 1 FROM t");
        query.start().unwrap();

        let new_query = build_query_with_compiled("SELECT 2 FROM t");
        let old = query.swap(new_query).unwrap();

        assert_eq!(old.state(), QueryState::Stopped);
        assert_eq!(query.state(), QueryState::Running);
    }

    #[test]
    fn swap_incompatible_count() {
        let mut query = build_query_with_fallback("SELECT 1 FROM t");
        query.start().unwrap();

        // Build a query with 2 pipelines.
        let schema = default_schema();
        let (e1, b1, c1, s1) = make_fallback(0, &schema);
        let (e2, b2, c2, s2) = make_fallback(1, &schema);
        let new_query = StreamingQueryBuilder::new("SELECT 2 FROM t")
            .add_pipeline(e1, b1, c1, s1)
            .add_pipeline(e2, b2, c2, s2)
            .build()
            .unwrap();

        let result = query.swap(new_query);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::IncompatibleSchemas(_)
        ));
    }

    #[test]
    fn swap_requires_running_or_paused() {
        let mut query = build_query_with_fallback("SELECT 1 FROM t");
        let new_query = build_query_with_fallback("SELECT 2 FROM t");

        // Still in Ready state — should fail.
        let result = query.swap(new_query);
        assert!(result.is_err());
    }

    // ── Metrics tests ───────────────────────────────────────────────

    #[test]
    fn metrics_initial_zero() {
        let query = build_query_with_compiled("SELECT 1 FROM t");
        let m = query.metrics();
        assert_eq!(m.ring0_events_in, 0);
        assert_eq!(m.ring0_events_out, 0);
        assert_eq!(m.ring0_events_dropped, 0);
        assert_eq!(m.pipelines_compiled, 1);
        assert_eq!(m.pipelines_fallback, 0);
    }

    #[test]
    fn metrics_after_submit() {
        let schema = default_schema();
        let mut query = build_query_with_compiled("SELECT * FROM t WHERE val > 0");
        query.start().unwrap();

        let arena = Bump::new();
        for i in 0..5 {
            let row = make_event_row(&arena, &schema, i * 100, i as f64);
            query.submit_row(&row, i * 100, 0).unwrap();
        }

        let m = query.metrics();
        assert_eq!(m.ring0_events_in, 5);
        assert_eq!(m.ring0_events_out, 5);
        assert_eq!(m.pipelines_compiled, 1);
    }

    // ── Accessor tests ──────────────────────────────────────────────

    #[test]
    fn query_id_deterministic() {
        let q1 = build_query_with_fallback("SELECT * FROM t");
        let q2 = build_query_with_fallback("SELECT * FROM t");
        assert_eq!(q1.id(), q2.id());

        let q3 = build_query_with_fallback("SELECT * FROM other");
        assert_ne!(q1.id(), q3.id());
    }

    #[test]
    fn metadata_preserved() {
        let schema = default_schema();
        let (exec, bridge, consumer, s) = make_fallback(0, &schema);
        let meta = QueryMetadata {
            compiled_pipeline_count: 5,
            fallback_pipeline_count: 3,
            jit_enabled: true,
            ..Default::default()
        };
        let query = StreamingQueryBuilder::new("SELECT 1")
            .add_pipeline(exec, bridge, consumer, s)
            .with_metadata(meta)
            .build()
            .unwrap();

        assert_eq!(query.metadata().compiled_pipeline_count, 5);
        assert_eq!(query.metadata().fallback_pipeline_count, 3);
        assert!(query.metadata().jit_enabled);
        assert_eq!(query.metadata().total_pipelines(), 8);
    }
}
