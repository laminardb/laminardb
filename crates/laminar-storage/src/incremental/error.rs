//! Error types for incremental checkpointing.

use thiserror::Error;

/// Errors that can occur during incremental checkpointing.
#[derive(Debug, Error)]
pub enum IncrementalCheckpointError {
    /// I/O error during checkpoint operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// `RocksDB` error.
    #[cfg(feature = "rocksdb")]
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// WAL error.
    #[error("WAL error: {0}")]
    Wal(String),

    /// Checkpoint not found.
    #[error("Checkpoint not found: {0}")]
    NotFound(String),

    /// Checkpoint corruption detected.
    #[error("Checkpoint corruption: {0}")]
    Corruption(String),

    /// Recovery failed.
    #[error("Recovery failed: {0}")]
    Recovery(String),

    /// Buffer overflow (backpressure signal).
    #[error("Changelog buffer overflow at epoch {epoch}")]
    BufferOverflow {
        /// The epoch at which overflow occurred.
        epoch: u64,
    },

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Checkpoint already exists.
    #[error("Checkpoint already exists for epoch {0}")]
    AlreadyExists(u64),

    /// Epoch mismatch during recovery.
    #[error("Epoch mismatch: expected {expected}, found {found}")]
    EpochMismatch {
        /// Expected epoch.
        expected: u64,
        /// Found epoch.
        found: u64,
    },

    /// CRC checksum mismatch.
    #[error("CRC mismatch at offset {offset}: expected {expected:#x}, computed {computed:#x}")]
    CrcMismatch {
        /// Offset where mismatch occurred.
        offset: u64,
        /// Expected CRC.
        expected: u32,
        /// Computed CRC.
        computed: u32,
    },
}

impl IncrementalCheckpointError {
    /// Returns true if this error indicates a transient failure that may succeed on retry.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Io(_) | Self::BufferOverflow { .. })
    }

    /// Returns true if this error indicates data corruption.
    #[must_use]
    pub fn is_corruption(&self) -> bool {
        matches!(self, Self::Corruption(_) | Self::CrcMismatch { .. })
    }
}
