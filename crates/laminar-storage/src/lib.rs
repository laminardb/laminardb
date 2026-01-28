//! # `LaminarDB` Storage
//!
//! Durability layer for `LaminarDB` - WAL, checkpointing, and lakehouse integration.
//!
//! ## Module Overview
//!
//! - [`wal`]: Write-ahead log for durability and exactly-once semantics
//! - [`checkpoint`]: Basic checkpointing for fast recovery
//! - [`incremental`]: F022 Incremental checkpointing with `RocksDB` backend
//! - [`per_core_wal`]: F062 Per-core WAL segments for thread-per-core architecture
//! - [`lakehouse`]: Delta Lake and Iceberg sink support
//! - [`wal_state_store`]: Combines `MmapStateStore` with WAL for durability

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Write-ahead log implementation - WAL for durability and exactly-once semantics
pub mod wal;

/// WAL-backed state store - Combines MmapStateStore with WAL for durability
pub mod wal_state_store;

/// Checkpointing for fast recovery
pub mod checkpoint;

/// Incremental checkpointing (F022) - Three-tier architecture with RocksDB backend
pub mod incremental;

/// Per-core WAL segments (F062) - Thread-per-core WAL for lock-free writes
pub mod per_core_wal;

/// Lakehouse format integration - Delta Lake and Iceberg sink support
pub mod lakehouse;

/// `io_uring`-backed Write-Ahead Log for high-performance durability (Linux only).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring_wal;

// Re-export key types
pub use checkpoint::{Checkpoint, CheckpointManager, CheckpointMetadata};
pub use wal::{WalEntry, WalError, WalPosition, WriteAheadLog};
pub use wal_state_store::WalStateStore;

// Re-export incremental checkpoint types
pub use incremental::{
    validate_checkpoint, wal_size, ChangelogEntryBuilder, CheckpointConfig,
    IncrementalCheckpointError, IncrementalCheckpointManager, IncrementalCheckpointMetadata,
    RecoveredState, RecoveryConfig, RecoveryManager, StateChangelogBuffer, StateChangelogEntry,
    StateOp,
};

// Re-export per-core WAL types
pub use per_core_wal::{
    recover_per_core, CheckpointCoordinator, CoreWalWriter, PerCoreRecoveredState,
    PerCoreRecoveryManager, PerCoreWalConfig, PerCoreWalEntry, PerCoreWalError, PerCoreWalManager,
    PerCoreWalReader, SegmentStats, WalOperation,
};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_wal::IoUringWal;