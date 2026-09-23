//! Pipeline configuration.

use std::time::Duration;

use laminar_connectors::connector::DeliveryGuarantee;

use crate::config::BackpressurePolicy;

/// Checkpoint triggering configured for the running pipeline.
///
/// The three states mirror [`LaminarConfig::checkpoint`](crate::config::LaminarConfig::checkpoint)
/// without allowing an enabled flag and timer interval to contradict each other.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CheckpointSchedule {
    /// No durable checkpoint service is configured.
    #[default]
    Disabled,
    /// Checkpoints run only when explicitly requested.
    Manual,
    /// Checkpoints normally wait this long after a terminal outcome. Bounded durable-output
    /// pressure may accelerate the next automatic admission; requests may also be explicit.
    Periodic(Duration),
}

impl CheckpointSchedule {
    /// Whether the durable checkpoint service is configured.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// The periodic cadence, if automatic checkpoints are configured.
    #[must_use]
    pub const fn periodic_interval(self) -> Option<Duration> {
        match self {
            Self::Periodic(interval) => Some(interval),
            Self::Disabled | Self::Manual => None,
        }
    }
}

/// Configuration for the event-driven connector pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum records per `poll_batch()` call.
    pub max_poll_records: usize,

    /// Shared channel capacity for all source senders → coordinator.
    pub channel_capacity: usize,

    /// Shared Arrow-byte limit for source messages, including parked input (default 64 MiB).
    /// Each producer may additionally hold one batch up to this limit while waiting for a
    /// cursor or capacity. Staged/graph buffers and connector decode scratch are excluded.
    pub source_queue_max_bytes: usize,

    /// Fallback poll interval when a source returns no `data_ready_notify`.
    pub fallback_poll_interval: Duration,

    /// Whether checkpoints are disabled, manual-only, or periodic.
    pub checkpoint_schedule: CheckpointSchedule,

    /// Sleep after the first event in a cycle to let more data accumulate.
    ///
    /// Bounds SQL executions per second without sacrificing data. The bounded
    /// channel provides natural backpressure during the window. `ZERO` = no batching.
    pub batch_window: Duration,

    /// Sole checkpoint-derived control-plane budget. Each connector startup stage and checkpoint
    /// barrier creates one absolute deadline from this duration; individual connectors cannot
    /// reset it.
    pub checkpoint_timeout: Duration,

    /// End-to-end delivery guarantee for the pipeline.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Maximum wall-clock time for a single processing cycle (nanoseconds). Default: 10ms.
    pub cycle_budget_ns: u64,

    /// Maximum wall-clock time for the message drain phase (nanoseconds). Default: 1ms.
    pub drain_budget_ns: u64,

    /// Maximum wall-clock time for per-query execution within a cycle (nanoseconds). Default: 8ms.
    pub query_budget_ns: u64,

    /// Per-input-port batch cap. Default: 256.
    pub max_input_buf_batches: usize,

    /// Per-input-port retained Arrow-byte cap, including source priming. `None` = disabled.
    /// Shared buffers are charged independently on each port; executed output that would
    /// exceed a limit halts the pipeline before publication under lossless policies.
    pub max_input_buf_bytes: Option<usize>,

    /// What to do when either cap is exceeded.
    pub backpressure_policy: BackpressurePolicy,

    /// Isolate queries that share a source into independent failure domains.
    /// Off: they fault and recover together; on: a fault holds back only its own offset.
    pub shared_source_isolation: bool,

    /// Per-source cap on the in-memory replay buffer used by `shared_source_isolation`.
    /// On overflow the engine falls back to whole-pipeline recovery. Ignored when the
    /// flag is off.
    pub max_replay_buffer_bytes: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_poll_records: 1024,
            channel_capacity: 64,
            source_queue_max_bytes: crate::config::DEFAULT_SOURCE_QUEUE_MAX_BYTES,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            batch_window: Duration::from_millis(5),
            checkpoint_timeout: Duration::from_secs(30),
            delivery_guarantee: DeliveryGuarantee::default(),
            cycle_budget_ns: 10_000_000, // 10ms
            drain_budget_ns: 1_000_000,  // 1ms
            query_budget_ns: 8_000_000,  // 8ms
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: BackpressurePolicy::default(),
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024, // 256 MiB per source
        }
    }
}

impl PipelineConfig {
    /// Private rollback budget for a failed connector startup stage. This is deliberately fixed:
    /// cleanup is fail-safe implementation policy, not another latency tuning dimension.
    pub(crate) const CONNECTOR_STARTUP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);
}
