//! Streaming checkpoint configuration.

use std::fmt;

/// Configuration for streaming checkpoints.
#[derive(Debug, Clone, Default)]
pub struct StreamCheckpointConfig {
    /// Checkpoint interval in milliseconds. `None` = manual only.
    pub interval_ms: Option<u64>,
    /// Directory for persisting checkpoints. `None` = in-memory only.
    pub data_dir: Option<std::path::PathBuf>,
    /// Maximum number of retained checkpoints. `None` = default (3).
    pub max_retained: Option<usize>,
    /// Barrier-alignment timeout in milliseconds at fan-in operators.
    /// `None` = default (`30_000`).
    pub alignment_timeout_ms: Option<u64>,
    /// Max epochs admitted between capture and restorable (the upload
    /// backlog). `None` = default (4). Exactly-once pipelines are
    /// capped at 1 regardless.
    pub max_in_flight_epochs: Option<u64>,
    /// Cap on captured-state bytes held by in-flight epochs; admission
    /// pauses at the cap. `None` = default (512 MiB).
    pub max_staged_bytes: Option<u64>,
}

/// Errors from checkpoint operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    /// Checkpointing is disabled.
    Disabled,
    /// A data directory is required.
    DataDirRequired,
    /// No checkpoint available for restore.
    NoCheckpoint,
    /// Operation timed out.
    Timeout,
    /// Invalid configuration.
    InvalidConfig(String),
    /// I/O error (stored as string for Clone/PartialEq).
    IoError(String),
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => write!(f, "checkpointing is disabled"),
            Self::DataDirRequired => write!(f, "data directory is required"),
            Self::NoCheckpoint => write!(f, "no checkpoint available"),
            Self::Timeout => write!(f, "checkpoint operation timed out"),
            Self::InvalidConfig(msg) => write!(f, "invalid checkpoint config: {msg}"),
            Self::IoError(msg) => write!(f, "checkpoint I/O error: {msg}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StreamCheckpointConfig::default();
        assert!(config.interval_ms.is_none());
        assert!(config.data_dir.is_none());
        assert!(config.max_retained.is_none());
        assert!(config.alignment_timeout_ms.is_none());
    }
}
