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

    /// Fallback poll interval when a source returns no `data_ready_notify`.
    pub fallback_poll_interval: Duration,

    /// Timer-based checkpoint interval (at-least-once). `None` disables auto-checkpointing.
    ///
    /// For exactly-once, use barrier-aligned checkpoints via
    /// [`PipelineCallback::checkpoint_with_barrier`](super::callback::PipelineCallback::checkpoint_with_barrier).
    pub checkpoint_interval: Option<Duration>,

    /// Sleep after the first event in a cycle to let more data accumulate.
    ///
    /// Bounds SQL executions per second without sacrificing data. The bounded
    /// channel provides natural backpressure during the window. `ZERO` = no batching.
    pub batch_window: Duration,

    /// Maximum time to wait for all sources to align on a checkpoint barrier.
    pub barrier_alignment_timeout: Duration,

    /// End-to-end delivery guarantee for the pipeline.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Maximum wall-clock time for a single processing cycle (nanoseconds). Default: 10ms.
    pub cycle_budget_ns: u64,

    /// Maximum wall-clock time for the message drain phase (nanoseconds). Default: 1ms.
    pub drain_budget_ns: u64,

    /// Maximum wall-clock time for per-query execution within a cycle (nanoseconds). Default: 8ms.
    pub query_budget_ns: u64,

    /// Maximum wall-clock time for background work after SQL execution (nanoseconds). Default: 5ms.
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
