//! Recovery from per-core WAL segments.
//!
//! Provides functionality to recover state from checkpoint + WAL segments.

use std::path::{Path, PathBuf};

use crate::incremental::{RecoveredState as BaseRecoveredState, RecoveryConfig, RecoveryManager};

use super::entry::{PerCoreWalEntry, WalOperation};
use super::error::PerCoreWalError;
use super::manager::PerCoreWalConfig;
use super::reader::PerCoreWalReader;

/// Recovered state from per-core WAL.
#[derive(Debug)]
pub struct PerCoreRecoveredState {
    /// Base recovered state from checkpoint.
    pub base_state: BaseRecoveredState,
    /// State changes from WAL replay.
    pub state_changes: Vec<StateChange>,
    /// Per-core WAL positions after replay.
    pub wal_positions: Vec<u64>,
    /// Number of WAL entries replayed.
    pub entries_replayed: usize,
    /// Final epoch after recovery.
    pub final_epoch: u64,
}

/// A single state change from WAL replay.
#[derive(Debug, Clone)]
pub struct StateChange {
    /// The key.
    pub key: Vec<u8>,
    /// The value (None for deletes).
    pub value: Option<Vec<u8>>,
    /// Epoch when the change was made.
    pub epoch: u64,
    /// Core that made the change.
    pub core_id: u16,
}

/// Recovery manager for per-core WAL segments.
pub struct PerCoreRecoveryManager {
    /// WAL configuration.
    wal_config: PerCoreWalConfig,
    /// Checkpoint recovery configuration.
    recovery_config: RecoveryConfig,
}

impl PerCoreRecoveryManager {
    /// Creates a new per-core recovery manager.
    #[must_use]
    pub fn new(wal_config: PerCoreWalConfig, recovery_config: RecoveryConfig) -> Self {
        Self {
            wal_config,
            recovery_config,
        }
    }

    /// Recovers state from checkpoint + all WAL segments.
    ///
    /// # Process
    ///
    /// 1. Load latest checkpoint using `RecoveryManager`
    /// 2. Read all WAL segments from checkpoint positions
    /// 3. Merge and sort entries by (epoch, timestamp)
    /// 4. Apply merged entries to state
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub fn recover(&self) -> Result<PerCoreRecoveredState, PerCoreWalError> {
        // 1. Recover base state from checkpoint
        let recovery_manager = RecoveryManager::new(self.recovery_config.clone());
        let base_state = recovery_manager.recover()?;

        // 2. Determine starting positions for each segment
        // For now, we replay from the start of each segment
        // In a production system, checkpoint would store per-core positions
        let starting_positions = vec![0u64; self.wal_config.num_cores];

        // 3. Read and merge WAL segments
        let (entries, wal_positions) = self.read_all_segments(&starting_positions)?;

        // 4. Process entries into state changes
        let mut state_changes = Vec::new();
        let mut final_epoch = base_state.epoch;

        for entry in &entries {
            final_epoch = final_epoch.max(entry.epoch);

            match &entry.operation {
                WalOperation::Put { key, value } => {
                    state_changes.push(StateChange {
                        key: key.clone(),
                        value: Some(value.clone()),
                        epoch: entry.epoch,
                        core_id: entry.core_id,
                    });
                }
                WalOperation::Delete { key } => {
                    state_changes.push(StateChange {
                        key: key.clone(),
                        value: None,
                        epoch: entry.epoch,
                        core_id: entry.core_id,
                    });
                }
                _ => {}
            }
        }

        let entries_replayed = entries.len();

        Ok(PerCoreRecoveredState {
            base_state,
            state_changes,
            wal_positions,
            entries_replayed,
            final_epoch,
        })
    }

    /// Recovers state from WAL segments only (no checkpoint).
    ///
    /// # Errors
    ///
    /// Returns an error if reading segments fails.
    pub fn recover_wal_only(&self) -> Result<Vec<PerCoreWalEntry>, PerCoreWalError> {
        let starting_positions = vec![0u64; self.wal_config.num_cores];
        let (entries, _) = self.read_all_segments(&starting_positions)?;
        Ok(entries)
    }

    /// Reads and merges all WAL segments from given positions.
    fn read_all_segments(
        &self,
        starting_positions: &[u64],
    ) -> Result<(Vec<PerCoreWalEntry>, Vec<u64>), PerCoreWalError> {
        let mut all_entries = Vec::new();
        let mut final_positions = Vec::with_capacity(self.wal_config.num_cores);

        for core_id in 0..self.wal_config.num_cores {
            let path = self.wal_config.segment_path(core_id);

            if path.exists() {
                let start_pos = starting_positions
                    .get(core_id)
                    .copied()
                    .unwrap_or(0);

                let mut reader = PerCoreWalReader::open_from(core_id, &path, start_pos)?;
                let entries = reader.read_all()?;
                all_entries.extend(entries);
                final_positions.push(reader.position());
            } else {
                final_positions.push(0);
            }
        }

        // Sort by (epoch, timestamp_ns, core_id, sequence)
        all_entries.sort();

        Ok((all_entries, final_positions))
    }

    /// Repairs all WAL segments by truncating at torn writes.
    ///
    /// # Returns
    ///
    /// Returns the valid end positions for each segment.
    ///
    /// # Errors
    ///
    /// Returns an error if repair fails.
    pub fn repair_all_segments(&self) -> Result<Vec<u64>, PerCoreWalError> {
        let mut valid_positions = Vec::with_capacity(self.wal_config.num_cores);

        for core_id in 0..self.wal_config.num_cores {
            let path = self.wal_config.segment_path(core_id);

            if path.exists() {
                let mut reader = PerCoreWalReader::open(core_id, &path)?;
                let valid_end = reader.find_valid_end()?;

                // Truncate to valid end if needed
                if valid_end < reader.file_len() {
                    use std::fs::OpenOptions;
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(valid_end)?;
                }

                valid_positions.push(valid_end);
            } else {
                valid_positions.push(0);
            }
        }

        Ok(valid_positions)
    }

    /// Gets statistics about all WAL segments.
    ///
    /// # Errors
    ///
    /// Returns an error if reading segments fails.
    pub fn segment_stats(&self) -> Result<Vec<SegmentStats>, PerCoreWalError> {
        let mut stats = Vec::with_capacity(self.wal_config.num_cores);

        for core_id in 0..self.wal_config.num_cores {
            let path = self.wal_config.segment_path(core_id);

            if path.exists() {
                let mut reader = PerCoreWalReader::open(core_id, &path)?;
                let entries = reader.read_all()?;

                let min_epoch = entries.iter().map(|e| e.epoch).min().unwrap_or(0);
                let max_epoch = entries.iter().map(|e| e.epoch).max().unwrap_or(0);

                stats.push(SegmentStats {
                    core_id,
                    path: path.clone(),
                    file_size: reader.file_len(),
                    entry_count: entries.len(),
                    min_epoch,
                    max_epoch,
                });
            } else {
                stats.push(SegmentStats {
                    core_id,
                    path,
                    file_size: 0,
                    entry_count: 0,
                    min_epoch: 0,
                    max_epoch: 0,
                });
            }
        }

        Ok(stats)
    }
}

/// Statistics for a single WAL segment.
#[derive(Debug, Clone)]
pub struct SegmentStats {
    /// Core ID.
    pub core_id: usize,
    /// Path to the segment file.
    pub path: PathBuf,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of entries.
    pub entry_count: usize,
    /// Minimum epoch in segment.
    pub min_epoch: u64,
    /// Maximum epoch in segment.
    pub max_epoch: u64,
}

impl std::fmt::Debug for PerCoreRecoveryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerCoreRecoveryManager")
            .field("num_cores", &self.wal_config.num_cores)
            .field("base_dir", &self.wal_config.base_dir)
            .finish_non_exhaustive()
    }
}

/// Convenience function to recover from per-core WAL.
///
/// # Errors
///
/// Returns an error if recovery fails.
pub fn recover_per_core(
    wal_dir: &Path,
    checkpoint_dir: &Path,
    num_cores: usize,
) -> Result<PerCoreRecoveredState, PerCoreWalError> {
    // Use the first core's WAL file as the "main" WAL for recovery config
    // (the per-core recovery will read all segments separately)
    let wal_path = wal_dir.join("wal-0.log");
    let wal_config = PerCoreWalConfig::new(wal_dir, num_cores);
    let recovery_config = RecoveryConfig::new(checkpoint_dir, &wal_path);

    let manager = PerCoreRecoveryManager::new(wal_config, recovery_config);
    manager.recover()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental::CheckpointConfig;
    use crate::per_core_wal::{CheckpointCoordinator, PerCoreWalManager};
    use tempfile::TempDir;

    fn setup_recovery_test() -> (TempDir, PathBuf, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        (temp_dir, wal_dir, checkpoint_dir)
    }

    #[test]
    fn test_recover_empty() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Create checkpoint first
        let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
        let wal_manager = PerCoreWalManager::new(wal_config.clone()).unwrap();
        let checkpoint_config = CheckpointConfig::new(&checkpoint_dir).with_wal_path(&wal_dir);
        let mut coordinator = CheckpointCoordinator::new(wal_manager, checkpoint_config).unwrap();
        coordinator.create_checkpoint(1).unwrap();

        // Now recover
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_dir.join("wal-0.log"));
        let manager = PerCoreRecoveryManager::new(wal_config, recovery_config);

        let state = manager.recover().unwrap();
        assert_eq!(state.entries_replayed, 0);
        assert!(state.state_changes.is_empty());
    }

    #[test]
    fn test_recover_with_data() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Create data and checkpoint
        {
            let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
            let wal_manager = PerCoreWalManager::new(wal_config).unwrap();
            let checkpoint_config = CheckpointConfig::new(&checkpoint_dir).with_wal_path(&wal_dir);
            let mut coordinator = CheckpointCoordinator::new(wal_manager, checkpoint_config).unwrap();

            coordinator.wal_manager_mut().set_epoch_all(1);
            coordinator
                .wal_manager_mut()
                .writer(0)
                .append_put(b"key1", b"value1")
                .unwrap();
            coordinator
                .wal_manager_mut()
                .writer(1)
                .append_put(b"key2", b"value2")
                .unwrap();

            coordinator.create_checkpoint(1).unwrap();

            // Write more data after checkpoint
            coordinator.wal_manager_mut().set_epoch_all(2);
            coordinator
                .wal_manager_mut()
                .writer(0)
                .append_put(b"key3", b"value3")
                .unwrap();
            coordinator.wal_manager_mut().sync_all().unwrap();
        }

        // Recover
        let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_dir.join("wal-0.log"));
        let manager = PerCoreRecoveryManager::new(wal_config, recovery_config);

        let state = manager.recover().unwrap();

        // Should have key3 in state changes (written after checkpoint)
        assert!(!state.state_changes.is_empty());
        assert!(state.state_changes.iter().any(|c| c.key == b"key3"));
    }

    #[test]
    fn test_recover_wal_only() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Write to WAL directly
        {
            let config = PerCoreWalConfig::new(&wal_dir, 2);
            let mut manager = PerCoreWalManager::new(config).unwrap();

            manager.set_epoch_all(1);
            manager.writer(0).append_put(b"key1", b"value1").unwrap();
            manager.writer(1).append_put(b"key2", b"value2").unwrap();
            manager.set_epoch_all(2);
            manager.writer(0).append_put(b"key3", b"value3").unwrap();
            manager.sync_all().unwrap();
        }

        let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_dir.join("wal-0.log"));
        let manager = PerCoreRecoveryManager::new(wal_config, recovery_config);

        let entries = manager.recover_wal_only().unwrap();
        assert_eq!(entries.len(), 3);

        // Check ordering: epoch 1 first, then epoch 2
        assert_eq!(entries[0].epoch, 1);
        assert_eq!(entries[1].epoch, 1);
        assert_eq!(entries[2].epoch, 2);
    }

    #[test]
    fn test_repair_segments() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Write valid data
        {
            let config = PerCoreWalConfig::new(&wal_dir, 2);
            let mut manager = PerCoreWalManager::new(config).unwrap();
            manager.writer(0).append_put(b"key1", b"value1").unwrap();
            manager.sync_all().unwrap();
        }

        // Append garbage to simulate torn write
        {
            use std::io::Write;
            let path = wal_dir.join("wal-0.log");
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_dir.join("wal-0.log"));
        let manager = PerCoreRecoveryManager::new(wal_config.clone(), recovery_config);

        let valid_positions = manager.repair_all_segments().unwrap();

        // Segment 0 should be truncated
        let path = wal_config.segment_path(0);
        let file_size = std::fs::metadata(&path).unwrap().len();
        assert_eq!(file_size, valid_positions[0]);
    }

    #[test]
    fn test_segment_stats() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Write data
        {
            let config = PerCoreWalConfig::new(&wal_dir, 2);
            let mut manager = PerCoreWalManager::new(config).unwrap();
            manager.set_epoch_all(1);
            manager.writer(0).append_put(b"key1", b"value1").unwrap();
            manager.writer(0).append_put(b"key2", b"value2").unwrap();
            manager.set_epoch_all(2);
            manager.writer(0).append_put(b"key3", b"value3").unwrap();
            manager.writer(1).append_put(b"key4", b"value4").unwrap();
            manager.sync_all().unwrap();
        }

        let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_dir.join("wal-0.log"));
        let manager = PerCoreRecoveryManager::new(wal_config, recovery_config);

        let stats = manager.segment_stats().unwrap();

        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].entry_count, 3); // 3 entries on core 0
        assert_eq!(stats[1].entry_count, 1); // 1 entry on core 1
        assert_eq!(stats[0].min_epoch, 1);
        assert_eq!(stats[0].max_epoch, 2);
    }

    #[test]
    fn test_convenience_function() {
        let (_temp_dir, wal_dir, checkpoint_dir) = setup_recovery_test();

        // Create checkpoint
        {
            let wal_config = PerCoreWalConfig::new(&wal_dir, 2);
            let wal_manager = PerCoreWalManager::new(wal_config).unwrap();
            let checkpoint_config = CheckpointConfig::new(&checkpoint_dir).with_wal_path(&wal_dir);
            let mut coordinator = CheckpointCoordinator::new(wal_manager, checkpoint_config).unwrap();
            coordinator.create_checkpoint(1).unwrap();
        }

        let state = recover_per_core(&wal_dir, &checkpoint_dir, 2).unwrap();
        assert!(state.state_changes.is_empty());
    }
}
