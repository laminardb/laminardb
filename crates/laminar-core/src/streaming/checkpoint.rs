//! Streaming checkpoint configuration.
//!
//! Provides configuration types for the checkpoint subsystem.
//! The actual checkpoint coordination is handled by
//! `laminar_db::checkpoint_coordinator::CheckpointCoordinator`.

use std::fmt;

/// WAL mode for checkpoint durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    /// Asynchronous WAL writes (faster, may lose last few entries on crash).
    Async,
    /// Synchronous WAL writes (slower, durable).
    Sync,
}

/// Configuration for streaming checkpoints.
///
/// All fields default to `None`/disabled. Checkpointing is opt-in.
#[derive(Debug, Clone)]
pub struct StreamCheckpointConfig {
    /// Checkpoint interval in milliseconds. `None` = manual only.
    pub interval_ms: Option<u64>,
    /// WAL mode. Requires `data_dir` to be set.
    pub wal_mode: Option<WalMode>,
    /// Directory for persisting checkpoints/WAL. `None` = in-memory only.
    pub data_dir: Option<std::path::PathBuf>,
    /// Changelog buffer capacity. `None` = no changelog buffer.
    pub changelog_capacity: Option<usize>,
    /// Maximum number of retained checkpoints. `None` = unlimited.
    pub max_retained: Option<usize>,
    /// Overflow policy for the changelog buffer.
    pub overflow_policy: OverflowPolicy,
}

impl Default for StreamCheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: None,
            wal_mode: None,
            data_dir: None,
            changelog_capacity: None,
            max_retained: None,
            overflow_policy: OverflowPolicy::DropNew,
        }
    }
}

impl StreamCheckpointConfig {
    /// Validates the configuration, returning an error if invalid.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointError::InvalidConfig` if WAL mode is set without
    /// `data_dir`, or if `changelog_capacity` is zero.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.wal_mode.is_some() && self.data_dir.is_none() {
            return Err(CheckpointError::InvalidConfig(
                "WAL mode requires data_dir to be set".into(),
            ));
        }
        if let Some(cap) = self.changelog_capacity {
            if cap == 0 {
                return Err(CheckpointError::InvalidConfig(
                    "changelog_capacity must be > 0".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Errors from checkpoint operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    /// Checkpointing is disabled.
    Disabled,
    /// A data directory is required for this operation.
    DataDirRequired,
    /// WAL mode requires checkpointing to be enabled.
    WalRequiresCheckpoint,
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
            Self::WalRequiresCheckpoint => {
                write!(f, "WAL mode requires checkpointing")
            }
            Self::NoCheckpoint => write!(f, "no checkpoint available"),
            Self::Timeout => write!(f, "checkpoint operation timed out"),
            Self::InvalidConfig(msg) => {
                write!(f, "invalid checkpoint config: {msg}")
            }
            Self::IoError(msg) => write!(f, "checkpoint I/O error: {msg}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// Policy when the changelog buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop new entries when buffer is full.
    DropNew,
    /// Overwrite the oldest entry.
    OverwriteOldest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate_ok() {
        let config = StreamCheckpointConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_wal_without_data_dir() {
        let config = StreamCheckpointConfig {
            wal_mode: Some(WalMode::Sync),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_zero_changelog_capacity() {
        let config = StreamCheckpointConfig {
            changelog_capacity: Some(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
