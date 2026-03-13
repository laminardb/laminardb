//! Checkpoint infrastructure for state persistence and recovery.
//!
//! - `layout`: object-store checkpoint layout
//! - `checkpointer`: Async checkpoint persistence via object stores
//! - `source_offsets`: Typed source position tracking

/// Async checkpoint persistence via object stores.
pub mod checkpointer;
/// Object-store checkpoint layout with UUID v7 identifiers.
pub mod layout;
/// Typed source position tracking for checkpoint recovery.
pub mod source_offsets;

#[allow(clippy::disallowed_types)] // cold path: checkpoint recovery
use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::wal::WalPosition;

/// Checkpoint metadata stored alongside checkpoint data.
#[derive(Debug)]
pub struct CheckpointMetadata {
    /// Unique checkpoint ID (monotonically increasing).
    pub id: u64,

    /// Unix timestamp when checkpoint was created.
    pub timestamp: u64,

    /// WAL position at time of checkpoint.
    pub wal_position: WalPosition,

    /// Source offsets for exactly-once semantics.
    pub source_offsets: HashMap<String, u64>,

    /// Size of the state snapshot in bytes.
    pub state_size: u64,

    /// Current watermark at checkpoint time (for recovery).
    pub watermark: Option<i64>,
}

/// A completed checkpoint on disk.
#[derive(Debug)]
pub struct Checkpoint {
    /// Checkpoint metadata.
    pub metadata: CheckpointMetadata,

    /// Path to checkpoint directory.
    pub path: PathBuf,
}

impl Checkpoint {
    /// Path to the metadata file.
    #[must_use]
    pub fn metadata_path(&self) -> PathBuf {
        self.path.join("metadata.rkyv")
    }

    /// Path to the state snapshot file.
    #[must_use]
    pub fn state_path(&self) -> PathBuf {
        self.path.join("state.rkyv")
    }

    /// Path to the source offsets file.
    #[must_use]
    pub fn offsets_path(&self) -> PathBuf {
        self.path.join("offsets.json")
    }

    /// Load the state snapshot from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the state file cannot be read.
    pub fn load_state(&self) -> Result<Vec<u8>> {
        std::fs::read(self.state_path()).context("Failed to read state snapshot")
    }

    /// Load source offsets from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the offsets file cannot be read or parsed.
    pub fn load_offsets(&self) -> Result<HashMap<String, u64>> {
        let path = self.offsets_path();
        if path.exists() {
            let data = std::fs::read_to_string(&path).context("Failed to read source offsets")?;
            serde_json::from_str(&data).context("Failed to parse source offsets")
        } else {
            Ok(HashMap::new())
        }
    }
}
