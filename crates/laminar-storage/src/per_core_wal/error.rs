//! Error types for per-core WAL operations.

use std::path::PathBuf;

/// Errors that can occur in per-core WAL operations.
#[derive(Debug, thiserror::Error)]
pub enum PerCoreWalError {
    /// IO error during WAL operations.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error when writing entries.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error when reading entries.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// CRC32 checksum mismatch.
    #[error("CRC32 checksum mismatch at position {position} in segment {core_id}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Core ID of the segment.
        core_id: usize,
        /// Position of the corrupted record.
        position: u64,
        /// Expected CRC32 value.
        expected: u32,
        /// Actual CRC32 value.
        actual: u32,
    },

    /// Torn write detected (partial record at end of WAL).
    #[error("Torn write detected at position {position} in segment {core_id}: {reason}")]
    TornWrite {
        /// Core ID of the segment.
        core_id: usize,
        /// Position where torn write was detected.
        position: u64,
        /// Description of the torn write.
        reason: String,
    },

    /// Invalid core ID.
    #[error("Invalid core ID {core_id}: max is {max_core_id}")]
    InvalidCoreId {
        /// The invalid core ID.
        core_id: usize,
        /// The maximum valid core ID.
        max_core_id: usize,
    },

    /// Segment file not found.
    #[error("Segment file not found for core {core_id}: {path}")]
    SegmentNotFound {
        /// Core ID.
        core_id: usize,
        /// Expected path.
        path: PathBuf,
    },

    /// Checkpoint not found.
    #[error("Checkpoint not found at {path}")]
    CheckpointNotFound {
        /// Expected path.
        path: PathBuf,
    },

    /// Recovery failed.
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    /// Writer not initialized.
    #[error("Writer for core {0} not initialized")]
    WriterNotInitialized(usize),

    /// Epoch mismatch during checkpoint.
    #[error("Epoch mismatch: expected {expected}, got {actual}")]
    EpochMismatch {
        /// Expected epoch.
        expected: u64,
        /// Actual epoch.
        actual: u64,
    },

    /// Incremental checkpoint error.
    #[error("Incremental checkpoint error: {0}")]
    IncrementalCheckpoint(#[from] crate::incremental::IncrementalCheckpointError),
}
