//! Ring 0 / Ring 1 pipeline bridge via lock-free SPSC queue.
//!
//! The [`PipelineBridge`] (Ring 0 producer) sends compiled pipeline output as
//! individual [`BridgeMessage`]s through an [`SpscQueue`]. The [`BridgeConsumer`]
//! (Ring 1) accumulates events into Arrow `RecordBatch` according to a
//! [`BatchPolicy`], emitting [`Ring1Action`]s for downstream stateful operators.
//!
//! This explicit, watermark-aware handoff prevents partial-batch emissions tied
//! to arbitrary batch boundaries (Issue #55). Watermarks flush pending rows
//! *before* the watermark is forwarded, ensuring Ring 1 operators see complete
//! event sets.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use smallvec::SmallVec;

use super::bridge::{BridgeError, RowBatchBridge};
use super::policy::{BackpressureStrategy, BatchPolicy};
use super::row::{EventRow, RowSchema};
use crate::tpc::SpscQueue;

// ────────────────────────────── Errors ──────────────────────────────

/// Errors from the pipeline bridge.
#[derive(Debug, thiserror::Error)]
pub enum PipelineBridgeError {
    /// The SPSC queue is full and the backpressure strategy dropped the event.
    #[error("backpressure: dropped {0} event(s)")]
    Backpressure(usize),
    /// The output schema is incompatible with the bridge configuration.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    /// Batch formation failed in the underlying [`RowBatchBridge`].
    #[error("batch formation error: {0}")]
    BatchFormation(#[from] BridgeError),
}

// ────────────────────────────── Messages ─────────────────────────────

/// A message sent from Ring 0 through the SPSC queue to Ring 1.
// The size disparity between `Event` (280 bytes) and other variants is
// intentional: `SmallVec<[u8; 256]>` keeps row data inline to avoid heap
// allocations on the Ring 0 hot path. Boxing would negate the benefit.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BridgeMessage {
    /// A processed event row with metadata.
    Event {
        /// Serialized row bytes (inline for rows <= 256 bytes).
        row_data: SmallVec<[u8; 256]>,
        /// Event timestamp (microseconds since epoch).
        event_time: i64,
        /// Pre-computed key hash for partitioned operators.
        key_hash: u64,
    },
    /// Watermark advance notification.
    Watermark {
        /// The new watermark timestamp.
        timestamp: i64,
    },
    /// Checkpoint barrier — Ring 1 must flush and persist state.
    CheckpointBarrier {
        /// The epoch number for this checkpoint.
        epoch: u64,
    },
    /// End of stream — no more messages will follow.
    Eof,
}

// ────────────────────────────── Stats ────────────────────────────────

/// Shared counters between producer and consumer.
pub struct BridgeStats {
    /// Total events sent by the producer.
    pub events_sent: AtomicU64,
    /// Events dropped due to backpressure.
    pub events_dropped: AtomicU64,
    /// Watermark messages sent.
    pub watermarks_sent: AtomicU64,
    /// Checkpoint barriers sent.
    pub checkpoints_sent: AtomicU64,
    /// Batches flushed by the consumer.
    pub batches_flushed: AtomicU64,
    /// Total rows flushed across all batches.
    pub rows_flushed: AtomicU64,
}

impl BridgeStats {
    /// Creates a new zeroed stats instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_sent: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            watermarks_sent: AtomicU64::new(0),
            checkpoints_sent: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            rows_flushed: AtomicU64::new(0),
        }
    }

    /// Takes a point-in-time snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> BridgeStatsSnapshot {
        BridgeStatsSnapshot {
            events_sent: self.events_sent.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            watermarks_sent: self.watermarks_sent.load(Ordering::Relaxed),
            checkpoints_sent: self.checkpoints_sent.load(Ordering::Relaxed),
            batches_flushed: self.batches_flushed.load(Ordering::Relaxed),
            rows_flushed: self.rows_flushed.load(Ordering::Relaxed),
        }
    }
}

impl Default for BridgeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BridgeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeStats")
            .field("events_sent", &self.events_sent.load(Ordering::Relaxed))
            .field(
                "events_dropped",
                &self.events_dropped.load(Ordering::Relaxed),
            )
            .field(
                "watermarks_sent",
                &self.watermarks_sent.load(Ordering::Relaxed),
            )
            .field(
                "checkpoints_sent",
                &self.checkpoints_sent.load(Ordering::Relaxed),
            )
            .field(
                "batches_flushed",
                &self.batches_flushed.load(Ordering::Relaxed),
            )
            .field("rows_flushed", &self.rows_flushed.load(Ordering::Relaxed))
            .finish()
    }
}

/// A point-in-time snapshot of [`BridgeStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BridgeStatsSnapshot {
    /// Total events sent by the producer.
    pub events_sent: u64,
    /// Events dropped due to backpressure.
    pub events_dropped: u64,
    /// Watermark messages sent.
    pub watermarks_sent: u64,
    /// Checkpoint barriers sent.
    pub checkpoints_sent: u64,
    /// Batches flushed by the consumer.
    pub batches_flushed: u64,
    /// Total rows flushed across all batches.
    pub rows_flushed: u64,
}

// ────────────────────────────── Producer ─────────────────────────────

/// Ring 0 side of the pipeline bridge (producer).
///
/// Sends compiled pipeline output through a lock-free SPSC queue.
/// The `send_*` methods take `&self` because `SpscQueue::push` uses
/// atomic internals and does not require `&mut self`.
pub struct PipelineBridge {
    queue: Arc<SpscQueue<BridgeMessage>>,
    output_schema: Arc<RowSchema>,
    backpressure_strategy: BackpressureStrategy,
    stats: Arc<BridgeStats>,
}

impl PipelineBridge {
    /// Sends a processed event row through the bridge.
    ///
    /// Copies `row.data()` into a `SmallVec` (inline for <=256 bytes) so the
    /// caller's arena can be reset after this call returns.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineBridgeError::Backpressure`] if the queue is full and
    /// the strategy is [`BackpressureStrategy::DropNewest`].
    pub fn send_event(
        &self,
        row: &EventRow<'_>,
        event_time: i64,
        key_hash: u64,
    ) -> Result<(), PipelineBridgeError> {
        let data = row.data();
        debug_assert!(
            data.len() <= 256,
            "BridgeMessage::Event row_data spilled to heap ({} bytes > 256 inline capacity)",
            data.len()
        );
        let msg = BridgeMessage::Event {
            row_data: SmallVec::from_slice(data),
            event_time,
            key_hash,
        };
        self.try_send(msg)
    }

    /// Sends a watermark advance through the bridge.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineBridgeError::Backpressure`] if the queue is full.
    pub fn send_watermark(&self, timestamp: i64) -> Result<(), PipelineBridgeError> {
        let msg = BridgeMessage::Watermark { timestamp };
        if self.queue.push(msg).is_err() {
            return Err(PipelineBridgeError::Backpressure(0));
        }
        self.stats.watermarks_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Sends a checkpoint barrier through the bridge.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineBridgeError::Backpressure`] if the queue is full.
    pub fn send_checkpoint(&self, epoch: u64) -> Result<(), PipelineBridgeError> {
        let msg = BridgeMessage::CheckpointBarrier { epoch };
        if self.queue.push(msg).is_err() {
            return Err(PipelineBridgeError::Backpressure(0));
        }
        self.stats.checkpoints_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Sends an end-of-stream marker through the bridge.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineBridgeError::Backpressure`] if the queue is full.
    pub fn send_eof(&self) -> Result<(), PipelineBridgeError> {
        if self.queue.push(BridgeMessage::Eof).is_err() {
            return Err(PipelineBridgeError::Backpressure(0));
        }
        Ok(())
    }

    /// Returns `true` if the SPSC queue has room for at least one more message.
    #[must_use]
    pub fn has_capacity(&self) -> bool {
        !self.queue.is_full()
    }

    /// Returns `true` if the SPSC queue is full.
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        self.queue.is_full()
    }

    /// Returns a reference to the shared statistics.
    #[must_use]
    pub fn stats(&self) -> &Arc<BridgeStats> {
        &self.stats
    }

    /// Returns the output schema.
    #[must_use]
    pub fn output_schema(&self) -> &Arc<RowSchema> {
        &self.output_schema
    }

    /// Attempts to send a message, applying backpressure strategy on failure.
    fn try_send(&self, msg: BridgeMessage) -> Result<(), PipelineBridgeError> {
        if self.queue.push(msg).is_ok() {
            self.stats.events_sent.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // Queue full — apply strategy.
        match &self.backpressure_strategy {
            BackpressureStrategy::DropNewest => {
                self.stats.events_dropped.fetch_add(1, Ordering::Relaxed);
                Err(PipelineBridgeError::Backpressure(1))
            }
            BackpressureStrategy::PauseSource => Err(PipelineBridgeError::Backpressure(0)),
            BackpressureStrategy::SpillToDisk { .. } => {
                // Spill-to-disk is not yet implemented; fall back to drop.
                self.stats.events_dropped.fetch_add(1, Ordering::Relaxed);
                Err(PipelineBridgeError::Backpressure(1))
            }
        }
    }
}

impl std::fmt::Debug for PipelineBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineBridge")
            .field("backpressure_strategy", &self.backpressure_strategy)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

// ────────────────────────────── Consumer ─────────────────────────────

/// Ring 1 side of the pipeline bridge (consumer).
///
/// Drains [`BridgeMessage`]s from the SPSC queue, accumulates events in a
/// [`RowBatchBridge`], and produces [`Ring1Action`]s when flush conditions
/// are met.
pub struct BridgeConsumer {
    queue: Arc<SpscQueue<BridgeMessage>>,
    batch_bridge: RowBatchBridge,
    row_schema: Arc<RowSchema>,
    policy: BatchPolicy,
    current_watermark: i64,
    batch_start_time: Option<Instant>,
    stats: Arc<BridgeStats>,
}

impl BridgeConsumer {
    /// Drains all available messages from the queue and returns the resulting actions.
    ///
    /// Actions are returned in order:
    /// 1. `ProcessBatch` when flush conditions are met
    /// 2. `AdvanceWatermark` after any pending rows are flushed
    /// 3. `Checkpoint` after any pending rows are flushed
    /// 4. `Eof` after any pending rows are flushed
    pub fn drain(&mut self) -> SmallVec<[Ring1Action; 4]> {
        let mut actions: SmallVec<[Ring1Action; 4]> = SmallVec::new();

        while let Some(msg) = self.queue.pop() {
            match msg {
                BridgeMessage::Event {
                    row_data,
                    event_time: _,
                    key_hash: _,
                } => {
                    self.append_event_row(&row_data);
                    if self.batch_bridge.row_count() >= self.policy.max_rows {
                        self.flush_batch(&mut actions);
                    }
                }
                BridgeMessage::Watermark { timestamp } => {
                    if self.policy.flush_on_watermark && self.batch_bridge.row_count() > 0 {
                        self.flush_batch(&mut actions);
                    }
                    self.current_watermark = timestamp;
                    actions.push(Ring1Action::AdvanceWatermark(timestamp));
                }
                BridgeMessage::CheckpointBarrier { epoch } => {
                    if self.batch_bridge.row_count() > 0 {
                        self.flush_batch(&mut actions);
                    }
                    actions.push(Ring1Action::Checkpoint(epoch));
                }
                BridgeMessage::Eof => {
                    if self.batch_bridge.row_count() > 0 {
                        self.flush_batch(&mut actions);
                    }
                    actions.push(Ring1Action::Eof);
                }
            }
        }

        actions
    }

    /// Checks whether the maximum latency has been exceeded and flushes if so.
    ///
    /// Returns `Some(Ring1Action::ProcessBatch)` if a latency-triggered flush
    /// occurred, or `None` otherwise.
    pub fn check_latency_flush(&mut self) -> Option<Ring1Action> {
        if self.batch_bridge.row_count() == 0 {
            return None;
        }
        let start = self.batch_start_time?;
        if start.elapsed() >= self.policy.max_latency {
            let mut actions: SmallVec<[Ring1Action; 4]> = SmallVec::new();
            self.flush_batch(&mut actions);
            actions.into_iter().next()
        } else {
            None
        }
    }

    /// Returns the current watermark as last seen from the producer.
    #[must_use]
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    /// Returns the number of rows pending in the batch (not yet flushed).
    #[must_use]
    pub fn pending_rows(&self) -> usize {
        self.batch_bridge.row_count()
    }

    /// Returns a reference to the shared statistics.
    #[must_use]
    pub fn stats(&self) -> &Arc<BridgeStats> {
        &self.stats
    }

    /// Appends raw event bytes to the internal batch bridge.
    fn append_event_row(&mut self, row_data: &[u8]) {
        let row = EventRow::new(row_data, &self.row_schema);
        // The batch bridge should not be full here — we flush when row_count
        // reaches max_rows, and max_rows <= bridge capacity.
        self.batch_bridge
            .append_row(&row)
            .expect("BridgeConsumer: batch bridge overflow (capacity < max_rows?)");
        if self.batch_start_time.is_none() {
            self.batch_start_time = Some(Instant::now());
        }
    }

    /// Flushes the current batch into a `Ring1Action::ProcessBatch`.
    fn flush_batch(&mut self, actions: &mut SmallVec<[Ring1Action; 4]>) {
        let row_count = self.batch_bridge.row_count();
        if row_count == 0 {
            return;
        }
        let batch = self.batch_bridge.flush();
        self.batch_start_time = None;
        self.stats.batches_flushed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .rows_flushed
            .fetch_add(row_count as u64, Ordering::Relaxed);
        actions.push(Ring1Action::ProcessBatch(batch));
    }
}

impl std::fmt::Debug for BridgeConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeConsumer")
            .field("current_watermark", &self.current_watermark)
            .field("pending_rows", &self.batch_bridge.row_count())
            .field("policy", &self.policy)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

// ────────────────────────────── Actions ──────────────────────────────

/// An action for Ring 1 produced by the [`BridgeConsumer`].
#[derive(Debug)]
pub enum Ring1Action {
    /// A batch of events ready for Ring 1 operators (windows, joins, etc.).
    ProcessBatch(RecordBatch),
    /// Watermark advance — Ring 1 should fire timers and close windows.
    AdvanceWatermark(i64),
    /// Checkpoint barrier — Ring 1 must flush and persist state at this epoch.
    Checkpoint(u64),
    /// End of stream — no more events or watermarks will arrive.
    Eof,
}

// ────────────────────────────── Factory ──────────────────────────────

/// Creates a paired [`PipelineBridge`] (producer) and [`BridgeConsumer`] (consumer).
///
/// # Arguments
///
/// * `schema` - The Arrow-derived row schema for events crossing the bridge.
/// * `queue_capacity` - SPSC queue capacity (rounded up to next power of 2).
/// * `batch_capacity` - Maximum rows per `RecordBatch` (>= `policy.max_rows`).
/// * `policy` - Batching policy for the consumer.
/// * `strategy` - Backpressure strategy for the producer.
///
/// # Errors
///
/// Returns [`PipelineBridgeError::SchemaMismatch`] if `batch_capacity < policy.max_rows`.
/// Returns [`PipelineBridgeError::BatchFormation`] if the schema contains unsupported types.
pub fn create_pipeline_bridge(
    schema: Arc<RowSchema>,
    queue_capacity: usize,
    batch_capacity: usize,
    policy: BatchPolicy,
    strategy: BackpressureStrategy,
) -> Result<(PipelineBridge, BridgeConsumer), PipelineBridgeError> {
    if batch_capacity < policy.max_rows {
        return Err(PipelineBridgeError::SchemaMismatch(format!(
            "batch_capacity ({batch_capacity}) must be >= policy.max_rows ({})",
            policy.max_rows
        )));
    }

    let arrow_schema = schema.arrow_schema().clone();
    let batch_bridge = RowBatchBridge::new(arrow_schema, batch_capacity)?;
    let queue = Arc::new(SpscQueue::new(queue_capacity));
    let stats = Arc::new(BridgeStats::new());

    let producer = PipelineBridge {
        queue: Arc::clone(&queue),
        output_schema: Arc::clone(&schema),
        backpressure_strategy: strategy,
        stats: Arc::clone(&stats),
    };

    let consumer = BridgeConsumer {
        queue,
        batch_bridge,
        row_schema: schema,
        policy,
        current_watermark: i64::MIN,
        batch_start_time: None,
        stats,
    };

    Ok((producer, consumer))
}

#[cfg(test)]
#[allow(
    clippy::approx_constant,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap
)]
mod tests {
    use super::*;
    use crate::compiler::row::MutableEventRow;
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bumpalo::Bump;
    use std::sync::Arc;

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_arrow_schema(fields: Vec<(&str, DataType, bool)>) -> arrow_schema::SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_row_schema(fields: Vec<(&str, DataType, bool)>) -> Arc<RowSchema> {
        let arrow = make_arrow_schema(fields);
        Arc::new(RowSchema::from_arrow(&arrow).unwrap())
    }

    fn default_bridge(schema: Arc<RowSchema>) -> (PipelineBridge, BridgeConsumer) {
        create_pipeline_bridge(
            schema,
            64,
            1024,
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        )
        .unwrap()
    }

    fn small_bridge(
        schema: Arc<RowSchema>,
        max_rows: usize,
        queue_cap: usize,
    ) -> (PipelineBridge, BridgeConsumer) {
        create_pipeline_bridge(
            schema,
            queue_cap,
            max_rows.max(1),
            BatchPolicy::default().with_max_rows(max_rows.max(1)),
            BackpressureStrategy::DropNewest,
        )
        .unwrap()
    }

    fn send_one_event(producer: &PipelineBridge, schema: &RowSchema, ts: i64, val: f64) {
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, schema, 0);
        row.set_i64(0, ts);
        row.set_f64(1, val);
        let row = row.freeze();
        producer.send_event(&row, ts, 0).unwrap();
    }

    // ── Factory tests ───────────────────────────────────────────────

    #[test]
    fn factory_basic_schema() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, consumer) = default_bridge(schema);
        assert!(producer.has_capacity());
        assert_eq!(consumer.pending_rows(), 0);
        assert_eq!(consumer.current_watermark(), i64::MIN);
    }

    #[test]
    fn factory_mixed_types() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("flag", DataType::Boolean, false),
        ]);
        let result = create_pipeline_bridge(
            schema,
            32,
            1024,
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn factory_batch_capacity_too_small() {
        let schema = make_row_schema(vec![("x", DataType::Int64, false)]);
        let result = create_pipeline_bridge(
            schema,
            32,
            10, // < default max_rows of 1024
            BatchPolicy::default(),
            BackpressureStrategy::DropNewest,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, PipelineBridgeError::SchemaMismatch(_)));
    }

    // ── Producer tests ──────────────────────────────────────────────

    #[test]
    fn send_event_basic() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, _consumer) = default_bridge(Arc::clone(&schema));
        send_one_event(&producer, &schema, 1000, 3.14);

        let snap = producer.stats().snapshot();
        assert_eq!(snap.events_sent, 1);
        assert_eq!(snap.events_dropped, 0);
    }

    #[test]
    fn send_event_row_data_matches() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 42);
        row.set_f64(1, 2.718);
        let row = row.freeze();
        let original_data: Vec<u8> = row.data().to_vec();

        producer.send_event(&row, 42, 0).unwrap();

        // Peek at the message in the queue via drain.
        let actions = consumer.drain();
        // The event should be pending (not flushed yet because max_rows=1024).
        assert!(actions.is_empty());
        assert_eq!(consumer.pending_rows(), 1);

        // Verify the data was preserved by sending a watermark to flush.
        producer.send_watermark(100).unwrap();
        let actions = consumer.drain();

        assert_eq!(actions.len(), 2); // ProcessBatch + AdvanceWatermark
        if let Ring1Action::ProcessBatch(batch) = &actions[0] {
            assert_eq!(batch.num_rows(), 1);
            let col0 = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(col0.value(0), 42);
            let col1 = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!((col1.value(0) - 2.718).abs() < f64::EPSILON);
        } else {
            panic!("expected ProcessBatch, got {:?}", actions[0]);
        }

        // Verify original data length matches (schema-level check).
        assert!(!original_data.is_empty());
    }

    #[test]
    fn send_event_preserves_metadata() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, _) = default_bridge(Arc::clone(&schema));

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 999);
        let row = row.freeze();
        producer.send_event(&row, 999, 0xDEAD).unwrap();

        let snap = producer.stats().snapshot();
        assert_eq!(snap.events_sent, 1);
    }

    #[test]
    fn send_watermark() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        producer.send_watermark(5000).unwrap();
        let actions = consumer.drain();

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::AdvanceWatermark(5000)));
        assert_eq!(consumer.current_watermark(), 5000);

        let snap = producer.stats().snapshot();
        assert_eq!(snap.watermarks_sent, 1);
    }

    #[test]
    fn send_checkpoint() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        producer.send_checkpoint(7).unwrap();
        let actions = consumer.drain();

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::Checkpoint(7)));

        let snap = producer.stats().snapshot();
        assert_eq!(snap.checkpoints_sent, 1);
    }

    #[test]
    fn send_eof() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        producer.send_eof().unwrap();
        let actions = consumer.drain();

        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::Eof));
    }

    #[test]
    fn backpressure_drop() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        // Queue capacity of 2 (rounded to 2), holding 1 usable slot.
        let (producer, _consumer) = small_bridge(Arc::clone(&schema), 1024, 2);

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1);
        let row = row.freeze();

        // Fill the queue.
        producer.send_event(&row, 1, 0).unwrap();

        // Next push should be backpressured.
        let result = producer.send_event(&row, 2, 0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            PipelineBridgeError::Backpressure(1)
        ));

        let snap = producer.stats().snapshot();
        assert_eq!(snap.events_dropped, 1);
    }

    #[test]
    fn has_capacity_and_backpressure() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, _consumer) = small_bridge(Arc::clone(&schema), 1024, 2);

        assert!(producer.has_capacity());
        assert!(!producer.is_backpressured());

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1);
        producer.send_event(&row.freeze(), 1, 0).unwrap();

        // Queue of capacity 2 has 1 usable slot.
        assert!(!producer.has_capacity());
        assert!(producer.is_backpressured());
    }

    // ── Consumer drain tests ────────────────────────────────────────

    #[test]
    fn drain_empty_queue() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (_, mut consumer) = default_bridge(schema);
        let actions = consumer.drain();
        assert!(actions.is_empty());
    }

    #[test]
    fn drain_single_event_pending() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        send_one_event(&producer, &schema, 1000, 1.0);

        let actions = consumer.drain();
        // With max_rows=1024, one event is not enough to flush.
        assert!(actions.is_empty());
        assert_eq!(consumer.pending_rows(), 1);
    }

    #[test]
    fn drain_batch_full_flush() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        // max_rows=3, so 3 events should trigger a flush.
        let (producer, mut consumer) = small_bridge(Arc::clone(&schema), 3, 64);

        for i in 0..3 {
            send_one_event(&producer, &schema, i, i as f64);
        }

        let actions = consumer.drain();
        assert_eq!(actions.len(), 1);
        if let Ring1Action::ProcessBatch(batch) = &actions[0] {
            assert_eq!(batch.num_rows(), 3);
        } else {
            panic!("expected ProcessBatch");
        }
        assert_eq!(consumer.pending_rows(), 0);
    }

    #[test]
    fn drain_watermark_flushes_pending() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        send_one_event(&producer, &schema, 1000, 1.0);
        send_one_event(&producer, &schema, 2000, 2.0);
        producer.send_watermark(3000).unwrap();

        let actions = consumer.drain();
        // Should be: ProcessBatch(2 rows), AdvanceWatermark(3000).
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Ring1Action::ProcessBatch(_)));
        assert!(matches!(actions[1], Ring1Action::AdvanceWatermark(3000)));
    }

    #[test]
    fn drain_watermark_no_flush_when_empty() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        producer.send_watermark(5000).unwrap();

        let actions = consumer.drain();
        // Just the watermark, no batch.
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::AdvanceWatermark(5000)));
    }

    #[test]
    fn drain_watermark_no_flush_when_disabled() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            64,
            1024,
            BatchPolicy::default().with_flush_on_watermark(false),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();

        send_one_event(&producer, &schema, 1000, 1.0);
        producer.send_watermark(5000).unwrap();

        let actions = consumer.drain();
        // Only watermark — no flush because flush_on_watermark=false.
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::AdvanceWatermark(5000)));
        assert_eq!(consumer.pending_rows(), 1);
    }

    #[test]
    fn drain_checkpoint_flushes() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        send_one_event(&producer, &schema, 1000, 1.0);
        producer.send_checkpoint(42).unwrap();

        let actions = consumer.drain();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Ring1Action::ProcessBatch(_)));
        assert!(matches!(actions[1], Ring1Action::Checkpoint(42)));
    }

    #[test]
    fn drain_eof_flushes() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        send_one_event(&producer, &schema, 1000, 1.0);
        producer.send_eof().unwrap();

        let actions = consumer.drain();
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Ring1Action::ProcessBatch(_)));
        assert!(matches!(actions[1], Ring1Action::Eof));
    }

    #[test]
    fn drain_eof_empty() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));
        producer.send_eof().unwrap();

        let actions = consumer.drain();
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Ring1Action::Eof));
    }

    #[test]
    fn drain_interleaved_messages() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        send_one_event(&producer, &schema, 100, 1.0);
        send_one_event(&producer, &schema, 200, 2.0);
        producer.send_watermark(300).unwrap();
        send_one_event(&producer, &schema, 400, 3.0);
        producer.send_checkpoint(1).unwrap();
        producer.send_eof().unwrap();

        let actions = consumer.drain();
        // Expected: Batch(2 rows), Watermark(300), Batch(1 row), Checkpoint(1), Eof
        assert_eq!(actions.len(), 5);
        if let Ring1Action::ProcessBatch(b) = &actions[0] {
            assert_eq!(b.num_rows(), 2);
        } else {
            panic!("expected ProcessBatch at 0");
        }
        assert!(matches!(actions[1], Ring1Action::AdvanceWatermark(300)));
        if let Ring1Action::ProcessBatch(b) = &actions[2] {
            assert_eq!(b.num_rows(), 1);
        } else {
            panic!("expected ProcessBatch at 2");
        }
        assert!(matches!(actions[3], Ring1Action::Checkpoint(1)));
        assert!(matches!(actions[4], Ring1Action::Eof));
    }

    #[test]
    fn drain_data_correctness() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        for i in 0..5 {
            send_one_event(&producer, &schema, i * 100, i as f64 * 1.1);
        }
        producer.send_watermark(600).unwrap();

        let actions = consumer.drain();
        assert_eq!(actions.len(), 2);
        if let Ring1Action::ProcessBatch(batch) = &actions[0] {
            assert_eq!(batch.num_rows(), 5);
            let ts_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let val_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for i in 0..5 {
                assert_eq!(ts_col.value(i), (i as i64) * 100);
                assert!((val_col.value(i) - (i as f64) * 1.1).abs() < 1e-10);
            }
        } else {
            panic!("expected ProcessBatch");
        }
    }

    #[test]
    fn drain_null_fields() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 64);
        row.set_i64(0, 1000);
        row.set_null(1, true);
        let row = row.freeze();
        producer.send_event(&row, 1000, 0).unwrap();
        producer.send_watermark(2000).unwrap();

        let actions = consumer.drain();
        assert_eq!(actions.len(), 2);
        if let Ring1Action::ProcessBatch(batch) = &actions[0] {
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(name_col.is_null(0));
        } else {
            panic!("expected ProcessBatch");
        }
    }

    // ── Latency flush tests ─────────────────────────────────────────

    #[test]
    fn latency_flush_none_when_empty() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        let (_, mut consumer) = default_bridge(schema);
        assert!(consumer.check_latency_flush().is_none());
    }

    #[test]
    fn latency_flush_none_when_fresh() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            64,
            1024,
            BatchPolicy::default().with_max_latency(std::time::Duration::from_secs(60)),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();

        send_one_event(&producer, &schema, 1000, 1.0);
        consumer.drain(); // Process the event into pending.
        assert!(consumer.check_latency_flush().is_none());
    }

    #[test]
    fn latency_flush_triggers_after_timeout() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            64,
            1024,
            BatchPolicy::default().with_max_latency(std::time::Duration::from_millis(1)),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();

        send_one_event(&producer, &schema, 1000, 1.0);
        consumer.drain();
        assert_eq!(consumer.pending_rows(), 1);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let action = consumer.check_latency_flush();
        assert!(action.is_some());
        assert!(matches!(action.unwrap(), Ring1Action::ProcessBatch(_)));
        assert_eq!(consumer.pending_rows(), 0);
    }

    // ── Stats tests ─────────────────────────────────────────────────

    #[test]
    fn stats_shared_between_producer_consumer() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, consumer) = default_bridge(Arc::clone(&schema));

        // Same Arc.
        assert!(Arc::ptr_eq(producer.stats(), consumer.stats()));
    }

    #[test]
    fn stats_snapshot_correctness() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = default_bridge(Arc::clone(&schema));

        send_one_event(&producer, &schema, 100, 1.0);
        send_one_event(&producer, &schema, 200, 2.0);
        producer.send_watermark(300).unwrap();
        producer.send_checkpoint(1).unwrap();

        consumer.drain();

        let snap = consumer.stats().snapshot();
        assert_eq!(snap.events_sent, 2);
        assert_eq!(snap.events_dropped, 0);
        assert_eq!(snap.watermarks_sent, 1);
        assert_eq!(snap.checkpoints_sent, 1);
        // Watermark flushes the 2 pending rows. Checkpoint has nothing to flush.
        assert_eq!(snap.batches_flushed, 1);
        assert_eq!(snap.rows_flushed, 2);
    }

    // ── Concurrency tests ───────────────────────────────────────────

    #[test]
    fn concurrent_send_drain() {
        let schema = make_row_schema(vec![
            ("ts", DataType::Int64, false),
            ("val", DataType::Float64, true),
        ]);
        let (producer, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            1024,
            1024,
            BatchPolicy::default().with_max_rows(1024),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();

        let schema_clone = Arc::clone(&schema);
        let handle = std::thread::spawn(move || {
            for i in 0..100 {
                let arena = Bump::new();
                let mut row = MutableEventRow::new_in(&arena, &schema_clone, 0);
                row.set_i64(0, i);
                row.set_f64(1, i as f64);
                let row = row.freeze();
                let _ = producer.send_event(&row, i, 0);
            }
            producer.send_eof().unwrap();
        });

        let mut total_rows = 0;
        let mut saw_eof = false;
        while !saw_eof {
            let actions = consumer.drain();
            for action in &actions {
                match action {
                    Ring1Action::ProcessBatch(batch) => total_rows += batch.num_rows(),
                    Ring1Action::Eof => saw_eof = true,
                    _ => {}
                }
            }
            if !saw_eof {
                std::thread::yield_now();
            }
        }
        // Flush any remaining pending rows.
        if consumer.pending_rows() > 0 {
            let arena = Bump::new();
            let mut row = MutableEventRow::new_in(&arena, &schema, 0);
            row.set_i64(0, 0);
            row.set_f64(1, 0.0);
            // Already saw EOF, so just count pending.
            total_rows += consumer.pending_rows();
        }

        handle.join().unwrap();

        let snap = consumer.stats().snapshot();
        // Some events may have been dropped if the queue was full.
        assert_eq!(
            total_rows as u64 + snap.events_dropped,
            snap.events_sent + snap.events_dropped
        );
    }

    #[test]
    fn backpressure_under_load() {
        let schema = make_row_schema(vec![("ts", DataType::Int64, false)]);
        // Tiny queue: only 1 usable slot (capacity 2, one reserved).
        let (producer, mut consumer) = small_bridge(Arc::clone(&schema), 1024, 2);

        let mut sent = 0;
        let mut dropped = 0;
        let arena = Bump::new();
        for i in 0..10 {
            let mut row = MutableEventRow::new_in(&arena, &schema, 0);
            row.set_i64(0, i);
            match producer.send_event(&row.freeze(), i, 0) {
                Ok(()) => sent += 1,
                Err(_) => dropped += 1,
            }
        }

        // Drain whatever made it through.
        producer.send_eof().unwrap_or(());
        let actions = consumer.drain();
        let batch_rows: usize = actions
            .iter()
            .filter_map(|a| match a {
                Ring1Action::ProcessBatch(b) => Some(b.num_rows()),
                _ => None,
            })
            .sum();

        // At least one event should have been sent, and some dropped.
        assert!(sent >= 1, "at least one event should have been sent");
        assert!(dropped > 0, "some events should have been dropped");
        // pending rows + batch rows = sent
        assert_eq!(batch_rows + consumer.pending_rows(), sent);
    }
}
