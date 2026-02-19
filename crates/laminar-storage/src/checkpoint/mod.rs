//! Checkpoint management for state persistence and recovery.
//!
//! - [`CheckpointManager`]: Phase 1-3 file-system checkpoint manager
//! - [`layout`]: Phase 6a object-store checkpoint layout
//! - [`checkpointer`]: Async checkpoint persistence via object stores

/// Async checkpoint persistence via object stores.
pub mod checkpointer;
/// Object-store checkpoint layout with UUID v7 identifiers.
pub mod layout;
/// Distributed recovery manager for Phase 6a.
pub mod recovery;
/// Typed source position tracking for checkpoint recovery.
pub mod source_offsets;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wal::WalPosition;

/// Internal checkpoint metadata for rkyv serialization
#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(compare(PartialEq))]
struct CheckpointMetadataInternal {
    /// Unique checkpoint ID (monotonically increasing)
    pub id: u64,

    /// Unix timestamp when checkpoint was created
    pub timestamp: u64,

    /// WAL position at time of checkpoint
    pub wal_position: WalPosition,

    /// Size of the state snapshot in bytes
    pub state_size: u64,

    /// Current watermark at checkpoint time (for recovery)
    pub watermark: Option<i64>,
}

/// Checkpoint metadata stored alongside checkpoint data
#[derive(Debug)]
pub struct CheckpointMetadata {
    /// Unique checkpoint ID (monotonically increasing)
    pub id: u64,

    /// Unix timestamp when checkpoint was created
    pub timestamp: u64,

    /// WAL position at time of checkpoint
    pub wal_position: WalPosition,

    /// Source offsets for exactly-once semantics
    pub source_offsets: HashMap<String, u64>,

    /// Size of the state snapshot in bytes
    pub state_size: u64,

    /// Current watermark at checkpoint time (for recovery)
    pub watermark: Option<i64>,
}

/// A completed checkpoint on disk
#[derive(Debug)]
pub struct Checkpoint {
    /// Checkpoint metadata
    pub metadata: CheckpointMetadata,

    /// Path to checkpoint directory
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
        fs::read(self.state_path()).context("Failed to read state snapshot")
    }

    /// Load source offsets from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the offsets file cannot be read or parsed.
    pub fn load_offsets(&self) -> Result<HashMap<String, u64>> {
        let path = self.offsets_path();
        if path.exists() {
            let data = fs::read_to_string(&path).context("Failed to read source offsets")?;
            serde_json::from_str(&data).context("Failed to parse source offsets")
        } else {
            Ok(HashMap::new())
        }
    }
}

/// Manages checkpointing for state stores
pub struct CheckpointManager {
    /// Directory where checkpoints are stored
    checkpoint_dir: PathBuf,

    /// How often to create checkpoints
    interval: Duration,

    /// Maximum number of checkpoints to retain
    max_retained: usize,

    /// Next checkpoint ID
    next_id: AtomicU64,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be created
    /// or if existing checkpoints cannot be scanned.
    pub fn new(checkpoint_dir: PathBuf, interval: Duration, max_retained: usize) -> Result<Self> {
        // Ensure checkpoint directory exists
        fs::create_dir_all(&checkpoint_dir).context("Failed to create checkpoint directory")?;

        // Find the highest checkpoint ID to continue from
        let next_id = Self::find_highest_checkpoint_id(&checkpoint_dir)?.map_or(0, |id| id + 1);

        Ok(Self {
            checkpoint_dir,
            interval,
            max_retained,
            next_id: AtomicU64::new(next_id),
        })
    }

    /// Create a new checkpoint from the given state snapshot.
    ///
    /// # Arguments
    ///
    /// * `state_snapshot` - The serialized state to checkpoint
    /// * `wal_position` - Current WAL position for recovery
    /// * `source_offsets` - Source offsets for exactly-once semantics
    /// * `watermark` - Current watermark for recovery
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be created,
    /// if files cannot be written, or if serialization fails.
    ///
    /// # Panics
    ///
    /// Panics if the system time is before the Unix epoch.
    pub fn create_checkpoint(
        &self,
        state_snapshot: &[u8],
        wal_position: WalPosition,
        source_offsets: HashMap<String, u64>,
        watermark: Option<i64>,
    ) -> Result<Checkpoint> {
        // Generate checkpoint ID
        let checkpoint_id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Create checkpoint directory
        let checkpoint_path = self.checkpoint_path(checkpoint_id);
        fs::create_dir_all(&checkpoint_path).context("Failed to create checkpoint directory")?;

        // Create metadata
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = CheckpointMetadata {
            id: checkpoint_id,
            timestamp,
            wal_position,
            source_offsets,
            state_size: state_snapshot.len() as u64,
            watermark,
        };

        // Write state snapshot
        let state_path = checkpoint_path.join("state.rkyv");
        fs::write(&state_path, state_snapshot).context("Failed to write state snapshot")?;

        // Write source offsets as JSON (since HashMap<String, u64> doesn't serialize well with rkyv)
        if !metadata.source_offsets.is_empty() {
            let offsets_path = checkpoint_path.join("offsets.json");
            let offsets_json = serde_json::to_string_pretty(&metadata.source_offsets)
                .context("Failed to serialize source offsets")?;
            fs::write(&offsets_path, offsets_json).context("Failed to write source offsets")?;
        }

        // Write metadata (convert to internal format)
        let metadata_internal = CheckpointMetadataInternal {
            id: metadata.id,
            timestamp: metadata.timestamp,
            wal_position: metadata.wal_position,
            state_size: metadata.state_size,
            watermark: metadata.watermark,
        };

        let metadata_path = checkpoint_path.join("metadata.rkyv");
        let metadata_bytes = rkyv::to_bytes::<Error>(&metadata_internal)?;
        fs::write(&metadata_path, &metadata_bytes)
            .context("Failed to write checkpoint metadata")?;

        let checkpoint = Checkpoint {
            metadata,
            path: checkpoint_path,
        };

        Ok(checkpoint)
    }

    /// Find the latest valid checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be read.
    pub fn find_latest_checkpoint(&self) -> Result<Option<Checkpoint>> {
        let mut latest: Option<Checkpoint> = None;
        let mut latest_id = 0u64;

        // Scan checkpoint directory
        let entries =
            fs::read_dir(&self.checkpoint_dir).context("Failed to read checkpoint directory")?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            // Skip non-directories
            if !path.is_dir() {
                continue;
            }

            // Try to parse checkpoint ID from directory name
            let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if let Some(id) = Self::parse_checkpoint_id(dir_name) {
                if id > latest_id {
                    // Try to load checkpoint
                    if let Ok(checkpoint) = self.load_checkpoint(id) {
                        latest_id = id;
                        latest = Some(checkpoint);
                    }
                }
            }
        }

        Ok(latest)
    }

    /// Load a checkpoint by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint metadata or state cannot be read,
    /// or if the checkpoint ID does not match.
    pub fn load_checkpoint(&self, checkpoint_id: u64) -> Result<Checkpoint> {
        let checkpoint_path = self.checkpoint_path(checkpoint_id);

        // Load metadata
        let metadata_path = checkpoint_path.join("metadata.rkyv");
        let metadata_bytes =
            fs::read(&metadata_path).context("Failed to read checkpoint metadata")?;

        let mut aligned_bytes = AlignedVec::<16>::with_capacity(metadata_bytes.len());
        aligned_bytes.extend_from_slice(&metadata_bytes);
        let archived =
            rkyv::access::<rkyv::Archived<CheckpointMetadataInternal>, Error>(&aligned_bytes)?;
        let metadata_internal: CheckpointMetadataInternal =
            rkyv::deserialize::<CheckpointMetadataInternal, Error>(archived)?;

        // Verify checkpoint ID matches
        if metadata_internal.id != checkpoint_id {
            anyhow::bail!(
                "Checkpoint ID mismatch: expected {}, got {}",
                checkpoint_id,
                metadata_internal.id
            );
        }

        // Convert to public metadata
        let metadata = CheckpointMetadata {
            id: metadata_internal.id,
            timestamp: metadata_internal.timestamp,
            wal_position: metadata_internal.wal_position,
            source_offsets: HashMap::new(), // Will be loaded separately
            state_size: metadata_internal.state_size,
            watermark: metadata_internal.watermark,
        };

        // Verify state file exists
        let state_path = checkpoint_path.join("state.rkyv");
        if !state_path.exists() {
            anyhow::bail!("State file missing for checkpoint {checkpoint_id}");
        }

        // Load source offsets if they exist
        let mut checkpoint = Checkpoint {
            metadata,
            path: checkpoint_path,
        };

        if let Ok(offsets) = checkpoint.load_offsets() {
            checkpoint.metadata.source_offsets = offsets;
        }

        Ok(checkpoint)
    }

    /// Clean up old checkpoints, keeping only the most recent ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be read
    /// or if old checkpoints cannot be removed.
    pub fn cleanup_old_checkpoints(&self) -> Result<()> {
        // Get all checkpoint IDs
        let mut checkpoint_ids = Vec::new();

        let entries =
            fs::read_dir(&self.checkpoint_dir).context("Failed to read checkpoint directory")?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if let Some(id) = Self::parse_checkpoint_id(dir_name) {
                    checkpoint_ids.push((id, path));
                }
            }
        }

        // Sort by ID (newest last)
        checkpoint_ids.sort_by_key(|(id, _)| *id);

        // Remove old checkpoints
        if checkpoint_ids.len() > self.max_retained {
            let to_remove = checkpoint_ids.len() - self.max_retained;

            for (id, path) in checkpoint_ids.into_iter().take(to_remove) {
                fs::remove_dir_all(&path)
                    .with_context(|| format!("Failed to remove checkpoint {id}"))?;
            }
        }

        Ok(())
    }

    /// Get the checkpoint interval.
    #[must_use]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Generate checkpoint directory path
    fn checkpoint_path(&self, checkpoint_id: u64) -> PathBuf {
        self.checkpoint_dir
            .join(format!("checkpoint-{checkpoint_id:020}"))
    }

    /// Parse checkpoint ID from directory name
    fn parse_checkpoint_id(dir_name: &str) -> Option<u64> {
        dir_name
            .strip_prefix("checkpoint-")
            .and_then(|id_str| id_str.parse().ok())
    }

    /// Find the highest checkpoint ID in the directory
    fn find_highest_checkpoint_id(checkpoint_dir: &Path) -> Result<Option<u64>> {
        let mut highest_id = None;

        if let Ok(entries) = fs::read_dir(checkpoint_dir) {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                    if let Some(id) = Self::parse_checkpoint_id(dir_name) {
                        highest_id = Some(highest_id.map_or(id, |h: u64| h.max(id)));
                    }
                }
            }
        }

        Ok(highest_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_creation() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            CheckpointManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(60), 3)
                .unwrap();

        // Create test state
        let state = b"test state data";
        let wal_position = WalPosition { offset: 100 };
        let mut offsets = HashMap::new();
        offsets.insert("source1".to_string(), 42);

        // Create checkpoint
        let checkpoint = manager
            .create_checkpoint(
                state,
                wal_position,
                offsets.clone(),
                Some(5000), // watermark
            )
            .unwrap();

        assert_eq!(checkpoint.metadata.id, 0);
        assert_eq!(checkpoint.metadata.wal_position, wal_position);
        assert_eq!(checkpoint.metadata.source_offsets, offsets);
        assert_eq!(checkpoint.metadata.state_size, state.len() as u64);
        assert_eq!(checkpoint.metadata.watermark, Some(5000));

        // Verify files exist
        assert!(checkpoint.metadata_path().exists());
        assert!(checkpoint.state_path().exists());

        // Load state back
        let loaded_state = checkpoint.load_state().unwrap();
        assert_eq!(loaded_state, state);
    }

    #[test]
    fn test_find_latest_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            CheckpointManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(60), 3)
                .unwrap();

        // No checkpoints yet
        assert!(manager.find_latest_checkpoint().unwrap().is_none());

        // Create multiple checkpoints
        for i in 0..3 {
            let wal_position = WalPosition { offset: i * 100 };
            manager
                .create_checkpoint(
                    b"state",
                    wal_position,
                    HashMap::new(),
                    Some(i64::try_from(i * 1000).unwrap()),
                )
                .unwrap();
        }

        // Find latest
        let latest = manager.find_latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.metadata.id, 2);
        assert_eq!(latest.metadata.wal_position.offset, 200);
    }

    #[test]
    fn test_checkpoint_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CheckpointManager::new(
            temp_dir.path().to_path_buf(),
            Duration::from_secs(60),
            2, // Keep only 2
        )
        .unwrap();

        // Create 5 checkpoints
        for i in 0..5 {
            let wal_position = WalPosition { offset: i * 100 };
            manager
                .create_checkpoint(
                    b"state",
                    wal_position,
                    HashMap::new(),
                    None, // no watermark for this test
                )
                .unwrap();
        }

        // Cleanup
        manager.cleanup_old_checkpoints().unwrap();

        // Verify only 2 remain
        let mut count = 0;
        for entry in fs::read_dir(&temp_dir).unwrap() {
            if entry.unwrap().path().is_dir() {
                count += 1;
            }
        }
        assert_eq!(count, 2);

        // Verify the latest ones are kept
        assert!(temp_dir
            .path()
            .join("checkpoint-00000000000000000003")
            .exists());
        assert!(temp_dir
            .path()
            .join("checkpoint-00000000000000000004")
            .exists());
    }

    #[test]
    fn test_checkpoint_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Create first manager and checkpoint
        let checkpoint_id = {
            let manager =
                CheckpointManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(60), 3)
                    .unwrap();

            let checkpoint = manager
                .create_checkpoint(
                    b"state data",
                    WalPosition { offset: 123 },
                    HashMap::new(),
                    Some(9999),
                )
                .unwrap();

            checkpoint.metadata.id
        };

        // Create new manager (simulating restart)
        let manager =
            CheckpointManager::new(temp_dir.path().to_path_buf(), Duration::from_secs(60), 3)
                .unwrap();

        // Load checkpoint
        let loaded = manager.load_checkpoint(checkpoint_id).unwrap();
        assert_eq!(loaded.metadata.id, checkpoint_id);
        assert_eq!(loaded.metadata.wal_position.offset, 123);
        assert_eq!(loaded.metadata.watermark, Some(9999));

        // Next checkpoint should have incremented ID
        let next = manager
            .create_checkpoint(
                b"new state",
                WalPosition { offset: 200 },
                HashMap::new(),
                Some(12345),
            )
            .unwrap();
        assert_eq!(next.metadata.id, checkpoint_id + 1);
        assert_eq!(next.metadata.watermark, Some(12345));
    }
}
