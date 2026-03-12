//! Pipeline configuration.

use std::time::Duration;

use laminar_connectors::connector::DeliveryGuarantee;

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
        }
    }
}
