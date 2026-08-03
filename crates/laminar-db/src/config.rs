//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

/// Default pipeline-wide lower-bound charge allowed for managed operator working state.
///
/// This execution budget is independent of checkpoint storage.
pub const DEFAULT_MAX_MANAGED_STATE_BYTES: usize = 256 * 1024 * 1024;

/// Default pre-encoding work charge allowed for one retractable MIN/MAX checkpoint capture.
///
/// This limit is independent of checkpoint storage.
/// It is a cached accumulator work proxy, not an encoded-payload or process-RSS limit.
pub const DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES: usize = 1024 * 1024;

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

/// String wrapper whose `Debug` redacts the value, for credentials in [`LaminarConfig`].
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
    /// Emit dirty-only changelogs for keyed non-windowed aggregate materialized views instead of
    /// re-materializing every group each cycle. This is query execution policy, not checkpointing.
    pub incremental_emit: bool,
    /// Cloud checkpoint URL, e.g. `s3://bucket/prefix`.
    pub object_store_url: Option<String>,
    /// Credential/config overrides for the object store.
    pub object_store_options: HashMap<String, String>,
    /// Bearer token presented when forwarding requests to the cluster leader's
    /// HTTP API (set when the server gates `/api/v1` with `console_token`).
    pub http_auth_token: Option<SecretString>,
    /// Delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
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
    /// Pipeline-wide managed working-state budget in charged bytes. `None` resolves to
    /// [`DEFAULT_MAX_MANAGED_STATE_BYTES`] when the database is constructed.
    pub pipeline_max_managed_state_bytes: Option<usize>,
    /// Pre-encoding work budget for one retractable MIN/MAX checkpoint capture. `None` resolves to
    /// [`DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES`] when the database is constructed.
    /// This charge is not an encoded-payload or process-RSS limit. Database construction rejects
    /// zero.
    pub pipeline_max_retractable_extremum_checkpoint_bytes: Option<usize>,
    /// Backpressure policy. See [`BackpressurePolicy`].
    pub pipeline_backpressure_policy: BackpressurePolicy,
    /// Auto-restart policy applied when supervision is enabled.
    pub restart_policy: RestartPolicy,
    /// Isolate queries that share a source into independent failure domains.
    /// Default off; when off, shared-source queries fault and recover together.
    pub shared_source_isolation: bool,
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            incremental_emit: true,
            object_store_url: None,
            object_store_options: HashMap::new(),
            http_auth_token: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            pipeline_channel_capacity: None,
            pipeline_batch_window: None,
            pipeline_drain_budget_ns: None,
            pipeline_query_budget_ns: None,
            pipeline_max_input_buf_batches: None,
            pipeline_max_input_buf_bytes: None,
            pipeline_max_managed_state_bytes: None,
            pipeline_max_retractable_extremum_checkpoint_bytes: None,
            pipeline_backpressure_policy: BackpressurePolicy::default(),
            restart_policy: RestartPolicy::default(),
            shared_source_isolation: false,
        }
    }
}
