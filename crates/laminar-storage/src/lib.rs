//! # `LaminarDB` Storage
//!
//! Durability layer for `LaminarDB` - WAL, checkpointing, and recovery.
//!
//! ## Module Overview
//!
//! - [`wal`]: Write-ahead log for durability and exactly-once semantics
//! - [`checkpoint`]: Basic checkpointing for fast recovery
//! - [`incremental`]: Incremental checkpointing
//! - [`per_core_wal`]: Per-core WAL segments for thread-per-core architecture
//! - [`wal_state_store`]: Combines `MmapStateStore` with WAL for durability
//!
//! **Note:** Lakehouse sinks (Delta Lake, Iceberg) are in `laminar-connectors` crate,
//! not here. This crate handles `LaminarDB`'s internal durability, not external storage formats.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Write-ahead log implementation - WAL for durability and exactly-once semantics
pub mod wal;

/// WAL-backed state store - Combines MmapStateStore with WAL for durability
pub mod wal_state_store;

/// Checkpointing for fast recovery
pub mod checkpoint;

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem store
pub mod checkpoint_store;

/// Ring 1 changelog drainer
pub mod changelog_drainer;

/// Incremental checkpointing - Directory-based checkpoint architecture
pub mod incremental;

/// Per-core WAL segments - Thread-per-core WAL for lock-free writes
pub mod per_core_wal;

/// `io_uring`-backed Write-Ahead Log for high-performance durability (Linux only).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring_wal;

// Re-export key types
pub use changelog_drainer::ChangelogDrainer;
pub use checkpoint::checkpointer::{
    verify_integrity, Checkpointer, CheckpointerError, ObjectStoreCheckpointer,
};
pub use checkpoint::layout::{
    CheckpointId, CheckpointManifestV2, CheckpointPaths, OperatorSnapshotEntry,
    PartitionSnapshotEntry, SourceOffsetEntry,
};
pub use checkpoint::source_offsets::{
    DeterminismValidator, DeterminismWarning, FilePosition, GenericPosition, KafkaPartitionOffset,
    KafkaPosition, MysqlCdcPosition, OperatorDescriptor, OperatorDeterminismWarning,
    PostgresCdcPosition, RecoveryPlan, SourceId, SourceOffset, SourcePosition, WarningSeverity,
};
pub use checkpoint::{Checkpoint, CheckpointManager, CheckpointMetadata};
pub use checkpoint_manifest::{CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint};
pub use checkpoint_store::{CheckpointStore, CheckpointStoreError, FileSystemCheckpointStore};
pub use wal::{WalEntry, WalError, WalPosition, WriteAheadLog};
pub use wal_state_store::WalStateStore;

// Re-export incremental checkpoint types
pub use incremental::{
    validate_checkpoint, wal_size, CheckpointConfig, IncrementalCheckpointError,
    IncrementalCheckpointManager, IncrementalCheckpointMetadata, RecoveredState, RecoveryConfig,
    RecoveryManager, StateChangelogBuffer, StateChangelogEntry, StateOp,
};

// Re-export per-core WAL types
pub use per_core_wal::{
    recover_per_core, CheckpointCoordinator, CoreWalWriter, PerCoreRecoveredState,
    PerCoreRecoveryManager, PerCoreWalConfig, PerCoreWalEntry, PerCoreWalError, PerCoreWalManager,
    PerCoreWalReader, SegmentStats, WalOperation,
};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_wal::IoUringWal;
