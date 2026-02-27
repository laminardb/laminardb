//! Pipeline configuration.

use std::time::Duration;

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
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_poll_records: 1024,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_interval: None,
            batch_window: Duration::from_millis(5),
        }
    }
}
