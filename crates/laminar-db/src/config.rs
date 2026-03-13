//! Configuration for `LaminarDB`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

/// SQL identifier case sensitivity mode.
///
/// Controls how unquoted SQL identifiers are matched against Arrow
/// schema field names.
///
/// `LaminarDB` defaults to [`CaseSensitive`](IdentifierCaseSensitivity::CaseSensitive)
/// (normalization disabled) so that mixed-case column names from
/// external sources (Kafka, CDC, `WebSocket`) work without double-quoting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdentifierCaseSensitivity {
    /// Preserve case as-written, case-sensitive matching (default).
    ///
    /// `SELECT tradeId` matches only `tradeId` in the schema.
    /// This is the recommended mode for financial / `IoT` data sources
    /// that use `camelCase` or `PascalCase` field names.
    #[default]
    CaseSensitive,
    /// Normalize unquoted identifiers to lowercase (standard SQL behaviour).
    ///
    /// `SELECT TradeId` becomes `SELECT tradeid` before schema matching.
    /// Use this if all your schemas use lowercase column names.
    Lowercase,
}

/// S3 storage class tiering configuration.
///
/// Controls how checkpoint objects are assigned to S3 storage classes
/// for cost optimization. Active checkpoints use the hot tier (fast access),
/// older checkpoints are moved to warm/cold tiers via S3 Lifecycle rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringConfig {
    /// Storage class for active checkpoints (e.g., `"EXPRESS_ONE_ZONE"`, `"STANDARD"`).
    pub hot_class: String,
    /// Storage class for older checkpoints (e.g., `"STANDARD"`).
    pub warm_class: String,
    /// Storage class for archive checkpoints (e.g., `"GLACIER_IR"`). Empty = no cold tier.
    pub cold_class: String,
    /// Time before moving objects from hot to warm tier (seconds).
    pub hot_retention_secs: u64,
    /// Time before moving objects from warm to cold tier (seconds). 0 = no cold tier.
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

/// Thread-per-core runtime configuration.
///
/// Controls CPU pinning, core count, and NUMA settings for the TPC
/// pipeline. When `None` on [`LaminarConfig`], auto-detected defaults
/// are used.
#[derive(Debug, Clone)]
pub struct TpcRuntimeConfig {
    /// Number of cores (`None` = auto-detect via `available_parallelism`).
    pub num_cores: Option<usize>,
    /// Pin core threads to CPUs starting from `cpu_start`.
    pub cpu_pinning: bool,
    /// First CPU ID for pinning.
    pub cpu_start: usize,
    /// Enable NUMA-aware memory allocation.
    pub numa_aware: bool,
}

impl TpcRuntimeConfig {
    /// Auto-detect system capabilities.
    #[must_use]
    pub fn auto() -> Self {
        Self {
            num_cores: None,
            cpu_pinning: cfg!(target_os = "linux"),
            cpu_start: 0,
            numa_aware: cfg!(target_os = "linux"),
        }
    }
}

impl Default for TpcRuntimeConfig {
    fn default() -> Self {
        Self::auto()
    }
}

/// Configuration for a `LaminarDB` instance.
#[derive(Debug, Clone)]
pub struct LaminarConfig {
    /// Default buffer size for streaming channels.
    pub default_buffer_size: usize,
    /// Default backpressure strategy.
    pub default_backpressure: BackpressureStrategy,
    /// Storage directory for WAL and checkpoints (`None` = in-memory only).
    pub storage_dir: Option<PathBuf>,
    /// Streaming checkpoint configuration (`None` = disabled).
    pub checkpoint: Option<StreamCheckpointConfig>,
    /// SQL identifier case sensitivity mode.
    pub identifier_case: IdentifierCaseSensitivity,
    /// Object store URL for cloud checkpoint storage (e.g., `s3://bucket/prefix`).
    pub object_store_url: Option<String>,
    /// Explicit credential/config overrides for the object store builder.
    pub object_store_options: HashMap<String, String>,
    /// S3 storage class tiering configuration (`None` = use default STANDARD).
    pub tiering: Option<TieringConfig>,
    /// Thread-per-core runtime configuration (`None` = use tokio mode).
    pub tpc: Option<TpcRuntimeConfig>,
    /// End-to-end delivery guarantee (default: at-least-once).
    pub delivery_guarantee: DeliveryGuarantee,
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
            tpc: None,
            delivery_guarantee: DeliveryGuarantee::default(),
        }
    }
}
