//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

/// What to do when an operator's input buffer exceeds its cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Defer the producer; sources block on `send`. No data loss.
    #[default]
    Backpressure,
    /// Drop oldest batches; counted in `shed_records_total`.
    ShedOldest,
    /// Error out the cycle.
    Fail,
}

/// String wrapper whose `Debug` redacts the value, for credentials held in
/// [`LaminarConfig`].
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the underlying secret. Call only at the point of use.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

/// Auto-restart policy for the fault supervisor (see `LaminarDB::enable_supervision`).
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Max restarts within `window` before the pipeline is left hard-faulted.
    pub max_restarts: usize,
    /// Sliding window over which `max_restarts` is counted.
    pub window: std::time::Duration,
    /// Backoff before the first restart in a window.
    pub initial_backoff: std::time::Duration,
    /// Cap on the exponential backoff.
    pub max_backoff: std::time::Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            window: std::time::Duration::from_secs(60),
            initial_backoff: std::time::Duration::from_millis(500),
            max_backoff: std::time::Duration::from_secs(30),
        }
    }
}

/// Configuration for a `LaminarDB` instance.
#[derive(Debug, Clone)]
pub struct LaminarConfig {
    /// Streaming channel buffer size.
    pub default_buffer_size: usize,
    /// Backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Checkpoint directory. `None` = in-memory only.
    pub storage_dir: Option<PathBuf>,
    /// Checkpoint config. `None` = disabled.
    pub checkpoint: Option<StreamCheckpointConfig>,
    /// Cloud checkpoint URL, e.g. `s3://bucket/prefix`.
    pub object_store_url: Option<String>,
    /// Credential/config overrides for the object store.
    pub object_store_options: HashMap<String, String>,
    /// Bearer token presented when forwarding requests to the cluster leader's
    /// HTTP API (set when the server gates `/api/v1` with `console_token`).
    pub http_auth_token: Option<SecretString>,
    /// Delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Per-operator state limit. At 80% warns, at 100% errors. `None` = unlimited.
    pub max_state_bytes_per_operator: Option<usize>,
    /// Node-level cap on total operator state held in memory. Crossing it
    /// pauses source intake (backpressure, not failure) until state drains
    /// below the budget. `None` = unlimited.
    pub state_memory_budget_bytes: Option<usize>,
    /// Local directory for the disk cold tier. When set together with
    /// `state_memory_budget_bytes`, operator state approaching the budget is
    /// demoted here (off-heap) instead of backpressuring. `None` = no tier.
    /// Requires the `state-tier` build feature; ignored otherwise.
    pub state_tier_dir: Option<PathBuf>,

    /// Source-to-coordinator channel capacity. `None` = 64.
    pub pipeline_channel_capacity: Option<usize>,
    /// Micro-batch coalescing window. `None` = 5ms connectors / 0 embedded.
    pub pipeline_batch_window: Option<std::time::Duration>,
    /// Drain budget per cycle (ns). `None` = 1ms.
    pub pipeline_drain_budget_ns: Option<u64>,
    /// Per-query budget (ns). `None` = 8ms.
    pub pipeline_query_budget_ns: Option<u64>,
    /// Per-port operator input-buffer cap (batches). `None` = 256.
    pub pipeline_max_input_buf_batches: Option<usize>,
    /// Per-port operator input-buffer cap (bytes). `None` = disabled.
    pub pipeline_max_input_buf_bytes: Option<usize>,
    /// Backpressure policy. See [`BackpressurePolicy`].
    pub pipeline_backpressure_policy: BackpressurePolicy,
    /// Auto-restart policy applied when supervision is enabled.
    pub restart_policy: RestartPolicy,
    /// Cluster mode: on a fatal fault, the leader rewinds every node to the highest
    /// cluster-wide committed epoch instead of a local-only restart. Default off.
    pub coordinated_recovery: bool,
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            object_store_url: None,
            object_store_options: HashMap::new(),
            http_auth_token: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            max_state_bytes_per_operator: None,
            state_memory_budget_bytes: None,
            state_tier_dir: None,
            pipeline_channel_capacity: None,
            pipeline_batch_window: None,
            pipeline_drain_budget_ns: None,
            pipeline_query_budget_ns: None,
            pipeline_max_input_buf_batches: None,
            pipeline_max_input_buf_bytes: None,
            pipeline_backpressure_policy: BackpressurePolicy::default(),
            restart_policy: RestartPolicy::default(),
            coordinated_recovery: false,
        }
    }
}
