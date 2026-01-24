//! # `LaminarDB` Storage
//!
//! Durability layer for `LaminarDB` - WAL, checkpointing, and lakehouse integration.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Write-ahead log implementation - WAL for durability and exactly-once semantics
pub mod wal;

/// WAL-backed state store - Combines MmapStateStore with WAL for durability
pub mod wal_state_store;

/// Checkpointing for fast recovery
pub mod checkpoint;

/// Lakehouse format integration - Delta Lake and Iceberg sink support
pub mod lakehouse;

/// `io_uring`-backed Write-Ahead Log for high-performance durability (Linux only).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring_wal;

// Re-export key types
pub use checkpoint::{Checkpoint, CheckpointManager, CheckpointMetadata};
pub use wal::{WalEntry, WalError, WalPosition, WriteAheadLog};
pub use wal_state_store::WalStateStore;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_wal::IoUringWal;