//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

/// How unquoted SQL identifiers are matched against Arrow schema field names.
///
/// Defaults to `CaseSensitive` so mixed-case columns from external sources
/// (Kafka, CDC, WebSocket) work without double-quoting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdentifierCaseSensitivity {
    /// Preserve case as-written (default).
    #[default]
    CaseSensitive,
    /// Normalize unquoted identifiers to lowercase (standard SQL).
    Lowercase,
}

/// S3 storage class tiering for checkpoint cost optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringConfig {
    /// e.g. `"EXPRESS_ONE_ZONE"`, `"STANDARD"`.
    pub hot_class: String,
    /// e.g. `"STANDARD"`.
    pub warm_class: String,
    /// e.g. `"GLACIER_IR"`. Empty = no cold tier.
    pub cold_class: String,
    /// Seconds before hot-to-warm transition.
    pub hot_retention_secs: u64,
    /// Seconds before warm-to-cold transition. 0 = no cold tier.
    pub warm_retention_secs: u64,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            hot_class: "STANDARD".to_string(),
            warm_class: "STANDARD".to_string(),
            cold_class: String::new(),
            hot_retention_secs: 86400,    // 24h
            warm_retention_secs: 604_800, // 7d
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
    /// Identifier case sensitivity.
    pub identifier_case: IdentifierCaseSensitivity,
    /// Cloud checkpoint URL, e.g. `s3://bucket/prefix`.
    pub object_store_url: Option<String>,
    /// Credential/config overrides for the object store.
    pub object_store_options: HashMap<String, String>,
    /// S3 tiering. `None` = default STANDARD.
    pub tiering: Option<TieringConfig>,
    /// Delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Per-operator state limit. At 80% warns, at 100% errors. `None` = unlimited.
    pub max_state_bytes_per_operator: Option<usize>,

    /// Source-to-coordinator channel capacity. `None` = 64.
    pub pipeline_channel_capacity: Option<usize>,
    /// Micro-batch coalescing window. `None` = 5ms connectors / 0 embedded.
    pub pipeline_batch_window: Option<std::time::Duration>,
    /// Drain budget per cycle (ns). `None` = 1ms.
    pub pipeline_drain_budget_ns: Option<u64>,
    /// Per-query budget (ns). `None` = 8ms.
    pub pipeline_query_budget_ns: Option<u64>,
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            identifier_case: IdentifierCaseSensitivity::default(),
            object_store_url: None,
            object_store_options: HashMap::new(),
            tiering: None,
            delivery_guarantee: DeliveryGuarantee::default(),
            max_state_bytes_per_operator: None,
            pipeline_channel_capacity: None,
            pipeline_batch_window: None,
            pipeline_drain_budget_ns: None,
            pipeline_query_budget_ns: None,
        }
    }
}
