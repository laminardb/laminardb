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
//!
//! **Note:** Lakehouse sinks (Delta Lake, Iceberg) are in `laminar-connectors` crate,
//! not here. This crate handles `LaminarDB`'s internal durability, not external storage formats.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::disallowed_types)] // cold path: all storage modules (WAL, checkpoint, recovery) are off hot path

/// Write-ahead log implementation - WAL for durability and exactly-once semantics
pub mod wal;

/// Checkpointing for fast recovery
pub mod checkpoint;

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem store
pub mod checkpoint_store;

/// S3 storage class tiering for cost optimization
pub mod tiering;

/// Ring 1 changelog drainer
pub mod changelog_drainer;

/// Incremental checkpointing - Directory-based checkpoint architecture
pub mod incremental;

/// Per-core WAL segments - Thread-per-core WAL for lock-free writes
pub mod per_core_wal;

/// Object store factory — builds S3, GCS, Azure, or local backends from URL schemes.
pub mod object_store_builder;

// Re-export key types
pub use changelog_drainer::ChangelogDrainer;
pub use checkpoint::checkpointer::{verify_integrity, CheckpointerError, ObjectStoreCheckpointer};
pub use checkpoint::layout::{
    CheckpointId, CheckpointManifestV2, CheckpointPaths, OperatorSnapshotEntry,
    PartitionSnapshotEntry, SourceOffsetEntry,
};
pub use checkpoint::source_offsets::{
    DeterminismWarning, FilePosition, GenericPosition, KafkaPartitionOffset, KafkaPosition,
    MysqlCdcPosition, PostgresCdcPosition, RecoveryPlan, SourceId, SourceOffset, SourcePosition,
};
pub use checkpoint::{Checkpoint, CheckpointMetadata};
pub use checkpoint_manifest::{CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint};
pub use checkpoint_store::{
    CheckpointStore, CheckpointStoreError, FileSystemCheckpointStore, ObjectStoreCheckpointStore,
    RecoveryReport, ValidationResult,
};
pub use laminar_core::error_codes::WarningSeverity;
pub use tiering::{StorageClass, StorageTier, TieringPolicy};
pub use wal::{WalEntry, WalError, WalPosition, WriteAheadLog};

// Re-export incremental checkpoint types
pub use incremental::{
    validate_checkpoint, wal_size, CheckpointConfig, IncrementalCheckpointError,
    IncrementalCheckpointManager, IncrementalCheckpointMetadata, RecoveredState, RecoveryConfig,
    RecoveryManager, StateChangelogBuffer, StateChangelogEntry, StateOp,
};

// Re-export per-core WAL types
pub use per_core_wal::{
    recover_per_core, CoreWalWriter, PerCoreCheckpointCoordinator, PerCoreRecoveredState,
    PerCoreRecoveryManager, PerCoreWalConfig, PerCoreWalEntry, PerCoreWalError, PerCoreWalManager,
    PerCoreWalReader, SegmentStats, WalOperation,
};
