//! Recovery manager for checkpoint + WAL replay.
//!
//! This module provides recovery functionality that combines:
//! 1. Loading state from the latest checkpoint
//! 2. Replaying WAL entries after the checkpoint
//!
//! ## Core Invariant
//!
//! ```text
//! Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State
//! ```
//!
//! ## Recovery Process
//!
//! 1. Find the latest valid checkpoint
//! 2. Load checkpoint state (mmap state snapshot)
//! 3. Replay WAL entries from checkpoint's WAL position
//! 4. Restore watermark and source offsets

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use laminar_core::state::StateSnapshot;
use tracing::{debug, info, warn};

use super::error::IncrementalCheckpointError;
use super::manager::{
    CheckpointConfig, IncrementalCheckpointManager, IncrementalCheckpointMetadata,
};
use crate::wal::{WalEntry, WalReadResult, WriteAheadLog};

/// Result of WAL replay operation.
struct WalReplayResult {
    /// Number of entries replayed.
    entries_replayed: u64,
    /// Final WAL position after replay.
    final_position: u64,
    /// Source offsets from commit entries.
    source_offsets: HashMap<String, u64>,
    /// Watermark from commit entries.
    watermark: Option<i64>,
    /// State changes (key, value or None for delete).
    state_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// Recovered state from checkpoint + WAL replay.
#[derive(Debug)]
pub struct RecoveredState {
    /// The epoch at which recovery completed.
    pub epoch: u64,
    /// State snapshot (if checkpoint had state data).
    pub state_snapshot: Option<StateSnapshot>,
    /// WAL position after replay.
    pub wal_position: u64,
    /// Number of WAL entries replayed.
    pub wal_entries_replayed: u64,
    /// Source offsets for exactly-once semantics.
    pub source_offsets: HashMap<String, u64>,
    /// Watermark at recovery point.
    pub watermark: Option<i64>,
    /// Checkpoint ID that was used (if any).
    pub checkpoint_id: Option<u64>,
    /// State changes from WAL replay (key, value or None for delete).
    pub state_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl RecoveredState {
    /// Creates a new empty recovered state.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            epoch: 0,
            state_snapshot: None,
            wal_position: 0,
            wal_entries_replayed: 0,
            source_offsets: HashMap::new(),
            watermark: None,
            checkpoint_id: None,
            state_changes: Vec::new(),
        }
    }

    /// Returns true if recovery found any state.
    #[must_use]
    pub fn has_state(&self) -> bool {
        self.state_snapshot.is_some() || !self.state_changes.is_empty()
    }
}

/// Configuration for recovery.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Path to checkpoint directory.
    pub checkpoint_dir: PathBuf,
    /// Path to WAL file.
    pub wal_path: PathBuf,
    /// Whether to repair torn writes in WAL.
    pub repair_wal: bool,
    /// Whether to collect state changes for incremental application.
    pub collect_state_changes: bool,
    /// Maximum WAL entries to replay (0 = unlimited).
    pub max_wal_entries: u64,
}

impl RecoveryConfig {
    /// Creates a new recovery configuration.
    #[must_use]
    pub fn new(checkpoint_dir: &Path, wal_path: &Path) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.to_path_buf(),
            wal_path: wal_path.to_path_buf(),
            repair_wal: true,
            collect_state_changes: false,
            max_wal_entries: 0,
        }
    }

    /// Enables WAL repair on recovery.
    #[must_use]
    pub fn with_repair_wal(mut self, enabled: bool) -> Self {
        self.repair_wal = enabled;
        self
    }

    /// Enables collection of state changes.
    #[must_use]
    pub fn with_collect_state_changes(mut self, enabled: bool) -> Self {
        self.collect_state_changes = enabled;
        self
    }

    /// Sets maximum WAL entries to replay.
    #[must_use]
    pub fn with_max_wal_entries(mut self, max: u64) -> Self {
        self.max_wal_entries = max;
        self
    }
}

/// Recovery manager for restoring state from checkpoints and WAL.
pub struct RecoveryManager {
    /// Recovery configuration.
    config: RecoveryConfig,
}

impl RecoveryManager {
    /// Creates a new recovery manager.
    #[must_use]
    pub fn new(config: RecoveryConfig) -> Self {
        Self { config }
    }

    /// Performs full recovery from checkpoint + WAL.
    ///
    /// This is the main recovery entry point that:
    /// 1. Finds the latest valid checkpoint
    /// 2. Loads checkpoint state
    /// 3. Replays WAL entries after the checkpoint
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub fn recover(&self) -> Result<RecoveredState, IncrementalCheckpointError> {
        info!(
            checkpoint_dir = %self.config.checkpoint_dir.display(),
            wal_path = %self.config.wal_path.display(),
            "Starting recovery"
        );

        let mut result = RecoveredState::empty();

        // Step 1: Find and load latest checkpoint
        let checkpoint_config = CheckpointConfig::new(&self.config.checkpoint_dir);
        let manager = IncrementalCheckpointManager::new(checkpoint_config)?;

        if let Some(checkpoint) = manager.find_latest_checkpoint()? {
            result = Self::load_checkpoint(&manager, &checkpoint);
            info!(
                checkpoint_id = checkpoint.id,
                epoch = checkpoint.epoch,
                wal_position = checkpoint.wal_position,
                "Loaded checkpoint"
            );
        } else {
            debug!("No checkpoint found, starting from WAL beginning");
        }

        // Step 2: Replay WAL
        if self.config.wal_path.exists() {
            let wal_result = self.replay_wal(result.wal_position)?;
            result.wal_entries_replayed = wal_result.entries_replayed;
            result.wal_position = wal_result.final_position;

            // Update source offsets and watermark from WAL commits
            for (source, offset) in wal_result.source_offsets {
                result.source_offsets.insert(source, offset);
            }
            if wal_result.watermark.is_some() {
                result.watermark = wal_result.watermark;
            }

            // Collect state changes if configured
            if self.config.collect_state_changes {
                result.state_changes = wal_result.state_changes;
            }

            info!(
                entries_replayed = wal_result.entries_replayed,
                final_position = wal_result.final_position,
                "WAL replay complete"
            );
        }

        Ok(result)
    }

    /// Loads state from a checkpoint.
    fn load_checkpoint(
        manager: &IncrementalCheckpointManager,
        checkpoint: &IncrementalCheckpointMetadata,
    ) -> RecoveredState {
        let mut result = RecoveredState::empty();
        result.checkpoint_id = Some(checkpoint.id);
        result.epoch = checkpoint.epoch;
        result.wal_position = checkpoint.wal_position;
        result.source_offsets.clone_from(&checkpoint.source_offsets);
        result.watermark = checkpoint.watermark;

        // Try to load state snapshot
        if let Ok(state_data) = manager.load_checkpoint_state(checkpoint.id) {
            match StateSnapshot::from_bytes(&state_data) {
                Ok(snapshot) => {
                    result.state_snapshot = Some(snapshot);
                }
                Err(e) => {
                    warn!(
                        checkpoint_id = checkpoint.id,
                        error = %e,
                        "Failed to deserialize state snapshot"
                    );
                }
            }
        }

        result
    }

    /// Replays WAL from the given position.
    fn replay_wal(
        &self,
        start_position: u64,
    ) -> Result<WalReplayResult, IncrementalCheckpointError> {
        let mut wal = WriteAheadLog::new(&self.config.wal_path, Duration::from_millis(100))
            .map_err(|e| IncrementalCheckpointError::Wal(e.to_string()))?;

        // Repair WAL if configured
        if self.config.repair_wal {
            if let Err(e) = wal.repair() {
                warn!(error = %e, "WAL repair failed, continuing anyway");
            }
        }

        let mut reader = wal
            .read_from(start_position)
            .map_err(|e| IncrementalCheckpointError::Wal(e.to_string()))?;

        let mut result = WalReplayResult {
            entries_replayed: 0,
            final_position: start_position,
            source_offsets: HashMap::new(),
            watermark: None,
            state_changes: Vec::new(),
        };

        let max_entries = if self.config.max_wal_entries > 0 {
            self.config.max_wal_entries
        } else {
            u64::MAX
        };

        loop {
            if result.entries_replayed >= max_entries {
                debug!(max_entries, "Reached max WAL entries limit");
                break;
            }

            match reader.read_next() {
                Ok(WalReadResult::Entry(entry)) => {
                    result.final_position = reader.position();
                    result.entries_replayed += 1;

                    match entry {
                        WalEntry::Put { key, value } => {
                            if self.config.collect_state_changes {
                                result.state_changes.push((key, Some(value)));
                            }
                        }
                        WalEntry::Delete { key } => {
                            if self.config.collect_state_changes {
                                result.state_changes.push((key, None));
                            }
                        }
                        WalEntry::Commit { offsets, watermark } => {
                            for (source, offset) in offsets {
                                result.source_offsets.insert(source, offset);
                            }
                            if watermark.is_some() {
                                result.watermark = watermark;
                            }
                        }
                        WalEntry::Checkpoint { id } => {
                            debug!(checkpoint_id = id, "Skipping checkpoint marker in WAL");
                        }
                    }
                }
                Ok(WalReadResult::Eof) => {
                    debug!("Reached end of WAL");
                    break;
                }
                Ok(WalReadResult::TornWrite { position, reason }) => {
                    warn!(position, reason, "Torn write detected, stopping replay");
                    break;
                }
                Ok(WalReadResult::ChecksumMismatch { position }) => {
                    warn!(position, "CRC mismatch detected, stopping replay");
                    break;
                }
                Ok(WalReadResult::Corrupted { position, reason }) => {
                    warn!(
                        position,
                        reason,
                        "[LDB-6006] Corrupted WAL entry detected, stopping replay"
                    );
                    break;
                }
                Err(e) => {
                    return Err(IncrementalCheckpointError::Wal(format!(
                        "WAL read error: {e}"
                    )));
                }
            }
        }

        Ok(result)
    }

    /// Convenience method for simple recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub fn recover_simple(
        checkpoint_dir: &Path,
        wal_path: &Path,
    ) -> Result<RecoveredState, IncrementalCheckpointError> {
        let config = RecoveryConfig::new(checkpoint_dir, wal_path);
        let manager = RecoveryManager::new(config);
        manager.recover()
    }

    /// Recovers and returns state changes for application to state store.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub fn recover_with_changes(
        checkpoint_dir: &Path,
        wal_path: &Path,
    ) -> Result<RecoveredState, IncrementalCheckpointError> {
        let config = RecoveryConfig::new(checkpoint_dir, wal_path).with_collect_state_changes(true);
        let manager = RecoveryManager::new(config);
        manager.recover()
    }
}

/// Validates a checkpoint directory.
///
/// # Errors
///
/// Returns an error if the checkpoint is invalid.
pub fn validate_checkpoint(
    checkpoint_dir: &Path,
) -> Result<IncrementalCheckpointMetadata, IncrementalCheckpointError> {
    let metadata_path = checkpoint_dir.join("metadata.json");

    if !metadata_path.exists() {
        return Err(IncrementalCheckpointError::NotFound(
            "metadata.json not found".to_string(),
        ));
    }

    let metadata_json = fs::read_to_string(&metadata_path)?;
    let metadata = IncrementalCheckpointMetadata::from_json(&metadata_json)?;

    // Validate state file if expected
    let state_path = checkpoint_dir.join("state.bin");
    if state_path.exists() {
        let state_data = fs::read(&state_path)?;
        StateSnapshot::from_bytes(&state_data)
            .map_err(|e| IncrementalCheckpointError::Corruption(e.to_string()))?;
    }

    Ok(metadata)
}

/// Calculates the WAL size for checkpoint decisions.
///
/// # Errors
///
/// Returns an error if the file cannot be read.
pub fn wal_size(wal_path: &Path) -> Result<u64, IncrementalCheckpointError> {
    if !wal_path.exists() {
        return Ok(0);
    }

    let metadata = fs::metadata(wal_path)?;
    Ok(metadata.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_recovered_state_empty() {
        let state = RecoveredState::empty();
        assert_eq!(state.epoch, 0);
        assert!(state.state_snapshot.is_none());
        assert!(state.source_offsets.is_empty());
        assert!(!state.has_state());
    }

    #[test]
    fn test_recovery_config() {
        let config = RecoveryConfig::new(Path::new("/checkpoints"), Path::new("/wal.log"))
            .with_repair_wal(true)
            .with_collect_state_changes(true)
            .with_max_wal_entries(1000);

        assert!(config.repair_wal);
        assert!(config.collect_state_changes);
        assert_eq!(config.max_wal_entries, 1000);
    }

    #[test]
    fn test_recovery_no_checkpoint_no_wal() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("wal.log");

        fs::create_dir_all(&checkpoint_dir).unwrap();

        let config = RecoveryConfig::new(&checkpoint_dir, &wal_path);
        let manager = RecoveryManager::new(config);

        let result = manager.recover().unwrap();
        assert_eq!(result.epoch, 0);
        assert!(result.state_snapshot.is_none());
        assert!(result.checkpoint_id.is_none());
        assert_eq!(result.wal_entries_replayed, 0);
    }

    #[test]
    fn test_recovery_with_checkpoint_only() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("wal.log");

        // Create a checkpoint
        let config = CheckpointConfig::new(&checkpoint_dir);
        let mut ckpt_manager = IncrementalCheckpointManager::new(config).unwrap();
        ckpt_manager.set_epoch(42);

        let mut offsets = HashMap::new();
        offsets.insert("source1".to_string(), 100);

        let state_data = StateSnapshot::new(vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ])
        .to_bytes()
        .unwrap();

        let metadata = ckpt_manager
            .create_checkpoint_with_state(500, offsets, Some(5000), &state_data)
            .unwrap();

        // Recover
        let recovery_config = RecoveryConfig::new(&checkpoint_dir, &wal_path);
        let recovery_manager = RecoveryManager::new(recovery_config);

        let result = recovery_manager.recover().unwrap();
        assert_eq!(result.epoch, 42);
        assert_eq!(result.checkpoint_id, Some(metadata.id));
        assert_eq!(result.wal_position, 500);
        assert_eq!(result.watermark, Some(5000));
        assert_eq!(result.source_offsets.get("source1"), Some(&100));
        assert!(result.state_snapshot.is_some());

        let snapshot = result.state_snapshot.unwrap();
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn test_recovery_with_wal_only() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("wal.log");

        fs::create_dir_all(&checkpoint_dir).unwrap();

        // Create WAL entries
        {
            let mut wal = WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();
            wal.set_sync_on_write(true);

            wal.append(&WalEntry::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();

            wal.append(&WalEntry::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            })
            .unwrap();

            let mut offsets = HashMap::new();
            offsets.insert("source1".to_string(), 50);
            wal.append(&WalEntry::Commit {
                offsets,
                watermark: Some(1000),
            })
            .unwrap();

            wal.sync().unwrap();
        }

        // Recover with state changes collection
        let config =
            RecoveryConfig::new(&checkpoint_dir, &wal_path).with_collect_state_changes(true);
        let manager = RecoveryManager::new(config);

        let result = manager.recover().unwrap();
        assert!(result.checkpoint_id.is_none());
        assert_eq!(result.wal_entries_replayed, 3);
        assert_eq!(result.watermark, Some(1000));
        assert_eq!(result.source_offsets.get("source1"), Some(&50));
        assert_eq!(result.state_changes.len(), 2);
    }

    #[test]
    fn test_recovery_checkpoint_plus_wal() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("wal.log");

        // Create checkpoint
        let config = CheckpointConfig::new(&checkpoint_dir);
        let mut ckpt_manager = IncrementalCheckpointManager::new(config).unwrap();
        ckpt_manager.set_epoch(10);

        let state_data = StateSnapshot::new(vec![(b"key1".to_vec(), b"value1".to_vec())])
            .to_bytes()
            .unwrap();

        ckpt_manager
            .create_checkpoint_with_state(0, HashMap::new(), Some(1000), &state_data)
            .unwrap();

        // Create WAL entries after checkpoint
        {
            let mut wal = WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();
            wal.set_sync_on_write(true);

            wal.append(&WalEntry::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            })
            .unwrap();

            wal.append(&WalEntry::Delete {
                key: b"key1".to_vec(),
            })
            .unwrap();

            let mut offsets = HashMap::new();
            offsets.insert("source1".to_string(), 100);
            wal.append(&WalEntry::Commit {
                offsets,
                watermark: Some(2000),
            })
            .unwrap();

            wal.sync().unwrap();
        }

        // Recover
        let recovery_config =
            RecoveryConfig::new(&checkpoint_dir, &wal_path).with_collect_state_changes(true);
        let recovery_manager = RecoveryManager::new(recovery_config);

        let result = recovery_manager.recover().unwrap();
        assert!(result.checkpoint_id.is_some());
        assert_eq!(result.wal_entries_replayed, 3);
        assert_eq!(result.watermark, Some(2000)); // Updated by WAL
        assert_eq!(result.source_offsets.get("source1"), Some(&100));
        assert!(result.state_snapshot.is_some());
        assert_eq!(result.state_changes.len(), 2);
    }

    #[test]
    fn test_validate_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        // Create checkpoint
        let config = CheckpointConfig::new(&checkpoint_dir);
        let mut manager = IncrementalCheckpointManager::new(config).unwrap();
        manager.set_epoch(5);

        let state_data = StateSnapshot::new(vec![(b"key".to_vec(), b"value".to_vec())])
            .to_bytes()
            .unwrap();

        let metadata = manager
            .create_checkpoint_with_state(100, HashMap::new(), None, &state_data)
            .unwrap();

        // Validate
        let checkpoint_path = metadata.checkpoint_path(&checkpoint_dir);
        let validated = validate_checkpoint(&checkpoint_path).unwrap();
        assert_eq!(validated.id, metadata.id);
        assert_eq!(validated.epoch, 5);
    }

    #[test]
    fn test_wal_size() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        // Non-existent WAL
        assert_eq!(wal_size(&wal_path).unwrap(), 0);

        // Create WAL with data
        {
            let mut wal = WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();
            wal.append(&WalEntry::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            })
            .unwrap();
            wal.sync().unwrap();
        }

        let size = wal_size(&wal_path).unwrap();
        assert!(size > 0);
    }

    #[test]
    fn test_recovery_max_entries() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let wal_path = temp_dir.path().join("wal.log");

        fs::create_dir_all(&checkpoint_dir).unwrap();

        // Create many WAL entries
        {
            let mut wal = WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();
            wal.set_sync_on_write(true);

            for i in 0..100 {
                wal.append(&WalEntry::Put {
                    key: format!("key{i}").into_bytes(),
                    value: format!("value{i}").into_bytes(),
                })
                .unwrap();
            }
            wal.sync().unwrap();
        }

        // Recover with limit
        let config = RecoveryConfig::new(&checkpoint_dir, &wal_path).with_max_wal_entries(10);
        let manager = RecoveryManager::new(config);

        let result = manager.recover().unwrap();
        assert_eq!(result.wal_entries_replayed, 10);
    }
}
