//! # Per-Core WAL
//!
//! Per-core WAL segments for thread-per-core architecture. Each core writes to its own
//! WAL file without contention, and segments are merged during checkpoint for global recovery.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Thread-Per-Core with Per-Core WAL            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
//! │  │   Core 0     │  │   Core 1     │  │   Core 2     │          │
//! │  │              │  │              │  │              │          │
//! │  │  Changelog   │  │  Changelog   │  │  Changelog   │          │
//! │  │      │       │  │      │       │  │      │       │          │
//! │  │      ▼       │  │      ▼       │  │      ▼       │          │
//! │  │  wal-0.log   │  │  wal-1.log   │  │  wal-2.log   │          │
//! │  └──────────────┘  └──────────────┘  └──────────────┘          │
//! │         │                 │                 │                   │
//! │         └─────────────────┼─────────────────┘                   │
//! │                           │                                     │
//! │                           ▼                                     │
//! │                  ┌─────────────────┐                            │
//! │                  │ Checkpoint      │                            │
//! │                  │ Coordinator     │                            │
//! │                  │                 │                            │
//! │                  │ Merge segments  │                            │
//! │                  │ by epoch order  │                            │
//! │                  └─────────────────┘                            │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! - [`CoreWalWriter`]: Per-core WAL writer (lock-free, no cross-core sync)
//! - [`PerCoreWalEntry`]: WAL entry with epoch for cross-core ordering
//! - [`PerCoreWalReader`]: Reader for a single segment file
//! - [`PerCoreWalManager`]: Coordinates all core writers
//! - [`CheckpointCoordinator`]: Merges segments during checkpoint
//! - [`PerCoreRecoveryManager`]: Recovery from multiple segments
//!
//! ## Core Invariant
//!
//! ```text
//! Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State
//! ```
//!
//! Entries are ordered by (epoch, timestamp_ns) for deterministic recovery.
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_storage::per_core_wal::{
//!     PerCoreWalConfig, PerCoreWalManager, WalOperation,
//! };
//! use std::path::Path;
//!
//! // Create manager for 4 cores
//! let config = PerCoreWalConfig::new(Path::new("/data/wal"), 4);
//! let mut manager = PerCoreWalManager::new(config).unwrap();
//!
//! // Each core writes to its own segment (lock-free)
//! manager.writer(0).append_put(b"key1", b"value1").unwrap();
//! manager.writer(1).append_put(b"key2", b"value2").unwrap();
//! manager.writer(0).sync().unwrap();
//! manager.writer(1).sync().unwrap();
//!
//! // During checkpoint, merge all segments
//! let merged = manager.merge_segments().unwrap();
//! // merged is sorted by (epoch, timestamp_ns)
//! ```

mod coordinator;
mod entry;
mod error;
mod manager;
mod reader;
mod recovery;
mod writer;

pub use coordinator::CheckpointCoordinator;
pub use entry::{PerCoreWalEntry, WalOperation};
pub use error::PerCoreWalError;
pub use manager::{PerCoreWalConfig, PerCoreWalManager};
pub use reader::PerCoreWalReader;
pub use recovery::{recover_per_core, PerCoreRecoveredState, PerCoreRecoveryManager, SegmentStats};
pub use writer::CoreWalWriter;
