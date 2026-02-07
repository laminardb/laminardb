//! Configuration for `LaminarDB`.

use std::path::PathBuf;

use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

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
    /// Row count threshold for auto-spilling in-memory tables to `RocksDB`.
    ///
    /// When a table exceeds this many rows and a `RocksDB` instance is
    /// available, the table is automatically migrated to persistent storage.
    /// Default: 1,000,000.
    pub table_spill_threshold: usize,
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            table_spill_threshold: 1_000_000,
        }
    }
}
