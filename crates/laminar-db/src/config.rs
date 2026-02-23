//! Configuration for `LaminarDB`.

use std::path::PathBuf;

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
}

impl Default for LaminarConfig {
    fn default() -> Self {
        Self {
            default_buffer_size: 65536,
            default_backpressure: BackpressureStrategy::Block,
            storage_dir: None,
            checkpoint: None,
            identifier_case: IdentifierCaseSensitivity::default(),
        }
    }
}
