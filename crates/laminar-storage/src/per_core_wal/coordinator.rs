//! Checkpoint coordination for per-core WAL.
//!
//! Coordinates checkpoint creation across all cores by merging WAL segments
//! and creating incremental checkpoints.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::incremental::{CheckpointConfig, IncrementalCheckpointManager, IncrementalCheckpointMetadata};

use super::entry::{PerCoreWalEntry, WalOperation};
use super::error::PerCoreWalError;
use super::manager::PerCoreWalManager;

/// Per-core checkpoint metadata.
///
/// Extends the incremental checkpoint metadata with per-core WAL positions.
#[derive(Debug, Clone)]
pub struct PerCoreCheckpointMetadata {
    /// The incremental checkpoint metadata.
    pub checkpoint: IncrementalCheckpointMetadata,
    /// Per-core WAL positions at checkpoint time.
    pub wal_positions: Vec<u64>,
    /// Number of entries merged from all segments.
    pub entries_merged: usize,
}

/// Coordinates checkpointing across all per-core WAL segments.
///
/// During checkpoint:
/// 1. Advances global epoch
/// 2. Writes epoch barriers to all segments
/// 3. Syncs all segments
/// 4. Merges segments in epoch order
/// 5. Creates incremental checkpoint
/// 6. Truncates all segments
pub struct CheckpointCoordinator {
    /// Per-core WAL manager.
    wal_manager: PerCoreWalManager,
    /// Incremental checkpoint manager.
    checkpoint_manager: IncrementalCheckpointManager,
    /// Directory for checkpoint data.
    checkpoint_dir: PathBuf,
}

impl CheckpointCoordinator {
    /// Creates a new checkpoint coordinator.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint manager cannot be initialized.
    pub fn new(
        wal_manager: PerCoreWalManager,
        checkpoint_config: CheckpointConfig,
    ) -> Result<Self, PerCoreWalError> {
        let checkpoint_dir = checkpoint_config.checkpoint_dir.clone();
        let checkpoint_manager = IncrementalCheckpointManager::new(checkpoint_config)?;

        Ok(Self {
            wal_manager,
            checkpoint_manager,
            checkpoint_dir,
        })
    }

    /// Returns a reference to the WAL manager.
    #[must_use]
    pub fn wal_manager(&self) -> &PerCoreWalManager {
        &self.wal_manager
    }

    /// Returns a mutable reference to the WAL manager.
    pub fn wal_manager_mut(&mut self) -> &mut PerCoreWalManager {
        &mut self.wal_manager
    }

    /// Returns a reference to the checkpoint manager.
    #[must_use]
    pub fn checkpoint_manager(&self) -> &IncrementalCheckpointManager {
        &self.checkpoint_manager
    }

    /// Returns a mutable reference to the checkpoint manager.
    pub fn checkpoint_manager_mut(&mut self) -> &mut IncrementalCheckpointManager {
        &mut self.checkpoint_manager
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.wal_manager.epoch()
    }

    /// Creates a checkpoint by merging all core segments.
    ///
    /// # Arguments
    ///
    /// * `checkpoint_id` - Unique identifier for this checkpoint
    ///
    /// # Errors
    ///
    /// Returns an error if any step fails.
    pub fn create_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<PerCoreCheckpointMetadata, PerCoreWalError> {
        // 1. Advance epoch
        let epoch = self.wal_manager.advance_epoch();
        self.wal_manager.set_epoch_all(epoch);

        // 2. Write epoch barriers to all segments
        self.wal_manager.write_epoch_barrier_all()?;

        // 3. Sync all segments to ensure durability
        self.wal_manager.sync_all()?;

        // 4. Record current positions before merge
        let wal_positions = self.wal_manager.positions();

        // 5. Merge WAL segments in epoch order
        let merged_entries = self.wal_manager.merge_segments()?;
        let entries_merged = merged_entries.len();

        // 6. Apply merged entries to checkpoint manager
        self.apply_entries_to_checkpoint(&merged_entries)?;

        // 7. Create incremental checkpoint
        let checkpoint = self.checkpoint_manager.create_checkpoint(checkpoint_id)?;

        // 8. Truncate all segments (WAL is now backed by checkpoint)
        self.wal_manager.reset_all()?;

        Ok(PerCoreCheckpointMetadata {
            checkpoint,
            wal_positions,
            entries_merged,
        })
    }

    /// Creates a checkpoint without truncating WAL segments.
    ///
    /// Useful for creating backup checkpoints while maintaining WAL history.
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint creation fails.
    pub fn create_checkpoint_no_truncate(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<PerCoreCheckpointMetadata, PerCoreWalError> {
        let epoch = self.wal_manager.advance_epoch();
        self.wal_manager.set_epoch_all(epoch);

        self.wal_manager.write_epoch_barrier_all()?;
        self.wal_manager.sync_all()?;

        let wal_positions = self.wal_manager.positions();
        let merged_entries = self.wal_manager.merge_segments()?;
        let entries_merged = merged_entries.len();

        self.apply_entries_to_checkpoint(&merged_entries)?;

        let checkpoint = self.checkpoint_manager.create_checkpoint(checkpoint_id)?;

        Ok(PerCoreCheckpointMetadata {
            checkpoint,
            wal_positions,
            entries_merged,
        })
    }

    /// Applies merged WAL entries to the checkpoint manager's state.
    fn apply_entries_to_checkpoint(
        &mut self,
        entries: &[PerCoreWalEntry],
    ) -> Result<(), PerCoreWalError> {
        for entry in entries {
            match &entry.operation {
                WalOperation::Put { key, value } => {
                    self.checkpoint_manager.put(key, value)?;
                }
                WalOperation::Delete { key } => {
                    self.checkpoint_manager.delete(key)?;
                }
                WalOperation::Commit { offsets, watermark } => {
                    // Update source offsets and watermark in checkpoint manager
                    for (source, offset) in offsets {
                        self.checkpoint_manager.set_source_offset(source.clone(), *offset);
                    }
                    if let Some(wm) = watermark {
                        self.checkpoint_manager.set_watermark(*wm);
                    }
                }
                WalOperation::Checkpoint { .. } | WalOperation::EpochBarrier { .. } => {
                    // These are markers, no state change
                }
            }
        }
        Ok(())
    }

    /// Flushes pending changes without creating a full checkpoint.
    ///
    /// Useful for periodic WAL syncs without the overhead of a full checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if any sync fails.
    pub fn flush(&mut self) -> Result<(), PerCoreWalError> {
        self.wal_manager.sync_all()
    }

    /// Gets the aggregate source offsets from the latest checkpoint.
    #[must_use]
    pub fn source_offsets(&self) -> HashMap<String, u64> {
        self.checkpoint_manager.source_offsets().clone()
    }

    /// Gets the watermark from the latest checkpoint.
    #[must_use]
    pub fn watermark(&self) -> Option<i64> {
        self.checkpoint_manager.watermark()
    }

    /// Gets the checkpoint directory path.
    #[must_use]
    pub fn checkpoint_dir(&self) -> &Path {
        &self.checkpoint_dir
    }

    /// Lists all available checkpoints.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub fn list_checkpoints(&self) -> Result<Vec<IncrementalCheckpointMetadata>, PerCoreWalError> {
        Ok(self.checkpoint_manager.list_checkpoints()?)
    }

    /// Cleans up old checkpoints, keeping only the most recent `keep_count`.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    pub fn cleanup_old_checkpoints(&mut self, keep_count: usize) -> Result<(), PerCoreWalError> {
        Ok(self.checkpoint_manager.cleanup_old_checkpoints_keep(keep_count)?)
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointCoordinator")
            .field("checkpoint_dir", &self.checkpoint_dir)
            .field("epoch", &self.epoch())
            .field("num_cores", &self.wal_manager.num_cores())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::per_core_wal::PerCoreWalConfig;
    use tempfile::TempDir;

    fn setup_coordinator(num_cores: usize) -> (CheckpointCoordinator, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir).unwrap();

        let wal_config = PerCoreWalConfig::new(&wal_dir, num_cores);
        let wal_manager = PerCoreWalManager::new(wal_config).unwrap();

        let checkpoint_config = CheckpointConfig::new(&checkpoint_dir)
            .with_wal_path(&wal_dir)
            .with_max_retained(3);

        let coordinator = CheckpointCoordinator::new(wal_manager, checkpoint_config).unwrap();

        (coordinator, temp_dir)
    }

    #[test]
    fn test_coordinator_creation() {
        let (coordinator, _temp_dir) = setup_coordinator(4);
        assert_eq!(coordinator.wal_manager().num_cores(), 4);
        assert_eq!(coordinator.epoch(), 0);
    }

    #[test]
    fn test_create_checkpoint_empty() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        let metadata = coordinator.create_checkpoint(1).unwrap();

        assert_eq!(metadata.checkpoint.id, 1);
        assert_eq!(metadata.entries_merged, 2); // 2 epoch barriers
        assert_eq!(metadata.wal_positions.len(), 2);
    }

    #[test]
    fn test_create_checkpoint_with_data() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        // Write some data
        coordinator.wal_manager_mut().set_epoch_all(1);
        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_put(b"key0", b"value0")
            .unwrap();
        coordinator
            .wal_manager_mut()
            .writer(1)
            .append_put(b"key1", b"value1")
            .unwrap();

        let metadata = coordinator.create_checkpoint(1).unwrap();

        // 2 puts + 2 epoch barriers
        assert_eq!(metadata.entries_merged, 4);

        // WAL should be truncated
        assert_eq!(coordinator.wal_manager().total_size(), 0);
    }

    #[test]
    fn test_create_checkpoint_no_truncate() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        coordinator.wal_manager_mut().set_epoch_all(1);
        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_put(b"key0", b"value0")
            .unwrap();

        let metadata = coordinator.create_checkpoint_no_truncate(1).unwrap();

        assert_eq!(metadata.checkpoint.id, 1);

        // WAL should NOT be truncated
        assert!(coordinator.wal_manager().total_size() > 0);
    }

    #[test]
    fn test_multiple_checkpoints() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        // Checkpoint 1
        coordinator.wal_manager_mut().set_epoch_all(1);
        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_put(b"key1", b"value1")
            .unwrap();
        let _cp1 = coordinator.create_checkpoint(1).unwrap();

        // Checkpoint 2
        coordinator.wal_manager_mut().set_epoch_all(2);
        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_put(b"key2", b"value2")
            .unwrap();
        let _cp2 = coordinator.create_checkpoint(2).unwrap();

        let checkpoints = coordinator.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);
    }

    #[test]
    fn test_source_offsets_and_watermark() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        // Write commit entry with offsets and watermark
        let mut offsets = HashMap::new();
        offsets.insert("topic1".to_string(), 100);
        offsets.insert("topic2".to_string(), 200);

        coordinator.wal_manager_mut().set_epoch_all(1);
        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_commit(offsets, Some(5000))
            .unwrap();

        coordinator.create_checkpoint(1).unwrap();

        // Verify source offsets and watermark
        let offsets = coordinator.source_offsets();
        assert_eq!(offsets.get("topic1"), Some(&100));
        assert_eq!(offsets.get("topic2"), Some(&200));
        assert_eq!(coordinator.watermark(), Some(5000));
    }

    #[test]
    fn test_flush() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        coordinator
            .wal_manager_mut()
            .writer(0)
            .append_put(b"key0", b"value0")
            .unwrap();

        // Should not fail
        coordinator.flush().unwrap();
    }

    #[test]
    fn test_cleanup_old_checkpoints() {
        let (mut coordinator, _temp_dir) = setup_coordinator(2);

        // Create multiple checkpoints
        for i in 1..=5 {
            coordinator.create_checkpoint(i).unwrap();
        }

        // Keep only 2
        coordinator.cleanup_old_checkpoints(2).unwrap();

        let checkpoints = coordinator.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);
    }

    #[test]
    fn test_debug_format() {
        let (coordinator, _temp_dir) = setup_coordinator(4);
        let debug_str = format!("{coordinator:?}");
        assert!(debug_str.contains("CheckpointCoordinator"));
        assert!(debug_str.contains("num_cores"));
    }
}
