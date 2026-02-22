//! # Incremental Checkpointing
//!
//! Three-tier incremental checkpoint architecture that maintains Ring 0 latency (<500ns)
//! while providing durable, incremental state snapshots.
//!
//! ## Architecture
//!
//! ```text
//! Ring 0 (Hot, <500ns):
//!   Event ──▶ mmap_state.put() ──▶ changelog.push(offset_ref)
//!                                   (zero-alloc)
//!                                        │
//!                                        ▼ async drain when idle
//! Ring 1 (Background):
//!   changelog.drain() ──▶ wal.append() ──▶ wal.sync()
//!                         (group commit, fdatasync)
//!                                        │
//!                                        ▼ periodic checkpoint
//!   wal.replay(last_ckpt..now) ──▶ state.apply_batch()
//!   state.create_checkpoint() ──▶ snapshot to directory
//!   wal.truncate(checkpoint_epoch)
//! ```
//!
//! ## Core Invariant
//!
//! ```text
//! Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State
//! ```
//!
//! ## Key Components
//!
//! - [`StateChangelogEntry`]: Zero-alloc changelog entry (32 bytes)
//! - [`StateChangelogBuffer`]: Ring 0 SPSC changelog buffer
//! - [`IncrementalCheckpointManager`]: Directory-based incremental checkpoints
//! - [`RecoveryManager`]: Checkpoint + WAL recovery
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_storage::incremental::{
//!     IncrementalCheckpointManager, CheckpointConfig, RecoveryConfig, RecoveryManager,
//! };
//! use std::path::Path;
//! use std::time::Duration;
//!
//! // Create checkpoint manager
//! let config = CheckpointConfig::new(Path::new("/data/checkpoints"))
//!     .with_wal_path(Path::new("/data/wal"))
//!     .with_interval(Duration::from_secs(60))
//!     .with_max_retained(3);
//!
//! let mut manager = IncrementalCheckpointManager::new(config).unwrap();
//!
//! // Create incremental checkpoint
//! let metadata = manager.create_checkpoint(100).unwrap();
//! println!("Created checkpoint {} at epoch {}", metadata.id, metadata.epoch);
//!
//! // Recovery
//! let recovery_config = RecoveryConfig::new(Path::new("/data/checkpoints"), Path::new("/data/wal"));
//! let recovery_manager = RecoveryManager::new(recovery_config);
//! let recovered = recovery_manager.recover().unwrap();
//! println!("Recovered to epoch {} with {} source offsets",
//!     recovered.epoch, recovered.source_offsets.len());
//! ```

mod changelog;
mod error;
mod manager;
mod recovery;

pub use changelog::{StateChangelogBuffer, StateChangelogEntry, StateOp};
pub use error::IncrementalCheckpointError;
pub use manager::{CheckpointConfig, IncrementalCheckpointManager, IncrementalCheckpointMetadata};
pub use recovery::{
    validate_checkpoint, wal_size, RecoveredState, RecoveryConfig, RecoveryManager,
};
