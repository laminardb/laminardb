//! Pipeline configuration.

use std::time::Duration;

use laminar_connectors::connector::DeliveryGuarantee;

use crate::config::BackpressurePolicy;

/// Configuration for the event-driven connector pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum records per `poll_batch()` call.
    pub max_poll_records: usize,

    /// Channel capacity for per-source `mpsc` sender → coordinator.
    pub channel_capacity: usize,

    /// Fallback poll interval when a source returns `data_ready_notify() == None`.
    pub fallback_poll_interval: Duration,

    /// Checkpoint interval (if checkpointing is enabled).
    ///
    /// This controls the **timer-based** checkpoint mode (at-least-once).
    /// For exactly-once semantics, use barrier-aligned checkpoints via
    /// [`PipelineCallback::checkpoint_with_barrier`](super::callback::PipelineCallback::checkpoint_with_barrier) instead. Timer-based
    /// checkpoints capture offsets *before* operator state, which means
    /// on recovery the consumer replays from the offset and operators may
    /// re-process some records (at-least-once, not exactly-once).
    pub checkpoint_interval: Option<Duration>,

    /// Coordinator micro-batch window.
    ///
    /// After receiving the first event in a cycle, the coordinator sleeps
    /// for this duration to let more events accumulate in the channel
    /// before executing the SQL cycle. This bounds the number of SQL
    /// executions per second while preserving low latency.
    ///
    /// During this window the coordinator does **not** drain the channel,
    /// so the bounded channel provides natural backpressure to source
    /// tasks (they block on `tx.send().await` when the channel fills).
    ///
    /// Set to `Duration::ZERO` to execute immediately (no batching).
    pub batch_window: Duration,

    /// Maximum time to wait for all sources to align on a checkpoint
    /// barrier before cancelling the checkpoint.
    pub barrier_alignment_timeout: Duration,

    /// End-to-end delivery guarantee for the pipeline.
    ///
    /// Validated at startup: `ExactlyOnce` requires all sources to support
    /// replay, all sinks to support exactly-once, and checkpointing to be
    /// enabled. See [`DeliveryGuarantee`] for details.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Maximum wall-clock time for a single processing cycle (nanoseconds).
    /// Logged at DEBUG when exceeded. Default: 10ms.
    pub cycle_budget_ns: u64,

    /// Maximum wall-clock time for the message drain phase (nanoseconds).
    /// The drain loop terminates early when this budget is exhausted.
    /// Default: 1ms.
    pub drain_budget_ns: u64,

    /// Maximum wall-clock time for per-query execution within a cycle
    /// (nanoseconds). When elapsed time exceeds this budget, remaining
    /// queries are deferred to the next cycle. At least one query always
    /// executes for forward progress. Default: 8ms.
    pub query_budget_ns: u64,

    /// Maximum wall-clock time for background work (barriers, checkpoint,
    /// table polling) after SQL execution (nanoseconds). When exceeded,
    /// low-priority tasks (table polling) are skipped. Default: 5ms.
    pub background_budget_ns: u64,

    /// Per-input-port batch cap. Default: 256.
    pub max_input_buf_batches: usize,

    /// Per-input-port byte cap. `None` = disabled.
    pub max_input_buf_bytes: Option<usize>,

    /// What to do when either cap is exceeded.
    pub backpressure_policy: BackpressurePolicy,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_poll_records: 1024,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_interval: None,
            batch_window: Duration::from_millis(5),
            barrier_alignment_timeout: Duration::from_secs(30),
            delivery_guarantee: DeliveryGuarantee::default(),
            cycle_budget_ns: 10_000_000,     // 10ms
            drain_budget_ns: 1_000_000,      // 1ms
            query_budget_ns: 8_000_000,      // 8ms
            background_budget_ns: 5_000_000, // 5ms
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: BackpressurePolicy::default(),
        }
    }
}
