//! Incremental checkpoint manager.
//!
//! This module provides incremental checkpointing using directory-based
//! snapshots with metadata tracking.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::error::IncrementalCheckpointError;

/// Configuration for incremental checkpointing.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Directory for checkpoint storage.
    pub checkpoint_dir: PathBuf,
    /// Path to the WAL file.
    pub wal_path: Option<PathBuf>,
    /// Checkpoint interval.
    pub interval: Duration,
    /// Maximum number of checkpoints to retain.
    pub max_retained: usize,
    /// Enable WAL truncation after checkpoint.
    pub truncate_wal: bool,
    /// Minimum WAL size before checkpoint (bytes).
    pub min_wal_size_for_checkpoint: u64,
    /// Enable incremental checkpoints.
    pub incremental: bool,
}

impl CheckpointConfig {
    /// Creates a new checkpoint configuration with the given directory.
    #[must_use]
    pub fn new(checkpoint_dir: &Path) -> Self {
        Self {
            checkpoint_dir: checkpoint_dir.to_path_buf(),
            wal_path: None,
            interval: Duration::from_secs(60),
            max_retained: 3,
            truncate_wal: true,
            min_wal_size_for_checkpoint: 64 * 1024 * 1024, // 64MB
            incremental: true,
        }
    }

    /// Sets the WAL path.
    #[must_use]
    pub fn with_wal_path(mut self, path: &Path) -> Self {
        self.wal_path = Some(path.to_path_buf());
        self
    }

    /// Sets the checkpoint interval.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the maximum number of retained checkpoints.
    #[must_use]
    pub fn with_max_retained(mut self, max: usize) -> Self {
        self.max_retained = max;
        self
    }

    /// Enables or disables WAL truncation.
    #[must_use]
    pub fn with_truncate_wal(mut self, enabled: bool) -> Self {
        self.truncate_wal = enabled;
        self
    }

    /// Sets the minimum WAL size for triggering checkpoint.
    #[must_use]
    pub fn with_min_wal_size(mut self, size: u64) -> Self {
        self.min_wal_size_for_checkpoint = size;
        self
    }

    /// Enables or disables incremental checkpoints.
    #[must_use]
    pub fn with_incremental(mut self, enabled: bool) -> Self {
        self.incremental = enabled;
        self
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), IncrementalCheckpointError> {
        if self.max_retained == 0 {
            return Err(IncrementalCheckpointError::InvalidConfig(
                "max_retained must be > 0".to_string(),
            ));
        }
        if self.interval.is_zero() {
            return Err(IncrementalCheckpointError::InvalidConfig(
                "interval must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Metadata for an incremental checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalCheckpointMetadata {
    /// Unique checkpoint ID.
    pub id: u64,
    /// Epoch at which the checkpoint was taken.
    pub epoch: u64,
    /// Unix timestamp when checkpoint was created.
    pub timestamp: u64,
    /// WAL position at checkpoint time.
    pub wal_position: u64,
    /// Source offsets for exactly-once semantics.
    pub source_offsets: HashMap<String, u64>,
    /// Watermark at checkpoint time.
    pub watermark: Option<i64>,
    /// Size of the checkpoint in bytes.
    pub size_bytes: u64,
    /// Number of keys in the checkpoint.
    pub key_count: u64,
    /// Whether this is an incremental checkpoint.
    pub is_incremental: bool,
    /// Parent checkpoint ID (for incremental).
    pub parent_id: Option<u64>,
    /// `SSTable` files included (for incremental, relative paths).
    pub sst_files: Vec<String>,
}

impl IncrementalCheckpointMetadata {
    /// Creates a new checkpoint metadata instance.
    #[must_use]
    pub fn new(id: u64, epoch: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            epoch,
            timestamp,
            wal_position: 0,
            source_offsets: HashMap::new(),
            watermark: None,
            size_bytes: 0,
            key_count: 0,
            is_incremental: true,
            parent_id: None,
            sst_files: Vec::new(),
        }
    }

    /// Returns the path to this checkpoint's directory.
    #[must_use]
    pub fn checkpoint_path(&self, base_dir: &Path) -> PathBuf {
        base_dir.join(format!("checkpoint_{:016x}", self.id))
    }

    /// Serializes metadata to JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, IncrementalCheckpointError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| IncrementalCheckpointError::Serialization(e.to_string()))
    }

    /// Deserializes metadata from JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_json(json: &str) -> Result<Self, IncrementalCheckpointError> {
        serde_json::from_str(json)
            .map_err(|e| IncrementalCheckpointError::Deserialization(e.to_string()))
    }
}

/// Incremental checkpoint manager.
///
/// This manager creates and manages incremental checkpoints using
/// directory-based snapshots with metadata tracking.
pub struct IncrementalCheckpointManager {
    /// Configuration.
    config: CheckpointConfig,
    /// Next checkpoint ID.
    next_id: AtomicU64,
    /// Current epoch.
    current_epoch: AtomicU64,
    /// Last checkpoint time (Unix timestamp).
    last_checkpoint_time: AtomicU64,
    /// Latest checkpoint ID.
    latest_checkpoint_id: Option<u64>,
    /// In-memory state store.
    state: FxHashMap<Vec<u8>, Vec<u8>>,
    /// Source offsets for exactly-once semantics.
    source_offsets: HashMap<String, u64>,
    /// Current watermark.
    watermark: Option<i64>,
}

impl IncrementalCheckpointManager {
    /// Creates a new incremental checkpoint manager.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid or directory creation fails.
    pub fn new(config: CheckpointConfig) -> Result<Self, IncrementalCheckpointError> {
        config.validate()?;

        // Create checkpoint directory
        fs::create_dir_all(&config.checkpoint_dir)?;

        // Find existing checkpoints
        let (next_id, latest_id) = Self::scan_checkpoints(&config.checkpoint_dir)?;

        Ok(Self {
            config,
            next_id: AtomicU64::new(next_id),
            current_epoch: AtomicU64::new(0),
            last_checkpoint_time: AtomicU64::new(0),
            latest_checkpoint_id: latest_id,
            state: FxHashMap::default(),
            source_offsets: HashMap::new(),
            watermark: None,
        })
    }

    /// Scans the checkpoint directory to find existing checkpoints.
    ///
    /// `next_id` is based on the highest directory ID (including partial
    /// dirs) to avoid ID collisions. `latest_id` only considers dirs with
    /// a valid `metadata.json` to prevent partial checkpoint directories
    /// from poisoning the incremental `parent_id` chain.
    fn scan_checkpoints(dir: &Path) -> Result<(u64, Option<u64>), IncrementalCheckpointError> {
        let mut max_dir_id = 0u64;
        let mut latest_valid_id = None;

        if dir.exists() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if let Some(id_str) = name_str.strip_prefix("checkpoint_") {
                    if let Ok(id) = u64::from_str_radix(id_str, 16) {
                        // Track highest dir ID for next_id (avoids collisions).
                        if id >= max_dir_id {
                            max_dir_id = id;
                        }
                        // Only set latest_id if metadata.json exists.
                        // Partial dirs (crash before metadata write) must not
                        // set latest_id or they poison parent_id chains.
                        let metadata_path = dir.join(name_str.as_ref()).join("metadata.json");
                        if !metadata_path.exists() {
                            debug!(
                                checkpoint_id = id,
                                "skipping partial checkpoint dir (no metadata.json)"
                            );
                            continue;
                        }
                        if latest_valid_id.is_none_or(|prev| id >= prev) {
                            latest_valid_id = Some(id);
                        }
                    }
                }
            }
        }

        Ok((max_dir_id + 1, latest_valid_id))
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Sets the current epoch.
    pub fn set_epoch(&self, epoch: u64) {
        self.current_epoch.store(epoch, Ordering::Release);
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    /// Puts a key-value pair into the state.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    #[allow(clippy::unnecessary_wraps)]
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), IncrementalCheckpointError> {
        self.state.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Deletes a key from the state.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    #[allow(clippy::unnecessary_wraps)]
    pub fn delete(&mut self, key: &[u8]) -> Result<(), IncrementalCheckpointError> {
        self.state.remove(key);
        Ok(())
    }

    /// Sets a source offset for exactly-once semantics.
    pub fn set_source_offset(&mut self, source: String, offset: u64) {
        self.source_offsets.insert(source, offset);
    }

    /// Returns the source offsets.
    #[must_use]
    pub fn source_offsets(&self) -> &HashMap<String, u64> {
        &self.source_offsets
    }

    /// Sets the current watermark.
    pub fn set_watermark(&mut self, watermark: i64) {
        self.watermark = Some(watermark);
    }

    /// Returns the current watermark.
    #[must_use]
    pub fn watermark(&self) -> Option<i64> {
        self.watermark
    }

    /// Returns the latest checkpoint ID.
    #[must_use]
    pub fn latest_checkpoint_id(&self) -> Option<u64> {
        self.latest_checkpoint_id
    }

    /// Checks if it's time to create a checkpoint.
    #[must_use]
    pub fn should_checkpoint(&self) -> bool {
        let last = self.last_checkpoint_time.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(last) >= self.config.interval.as_secs()
    }

    /// Creates a new incremental checkpoint.
    ///
    /// This creates a checkpoint of the current state using directory-based
    /// snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint creation fails.
    pub fn create_checkpoint(
        &mut self,
        wal_position: u64,
    ) -> Result<IncrementalCheckpointMetadata, IncrementalCheckpointError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let epoch = self.current_epoch.load(Ordering::Acquire);

        let mut metadata = IncrementalCheckpointMetadata::new(id, epoch);
        metadata.wal_position = wal_position;
        metadata.parent_id = self.latest_checkpoint_id;
        metadata.is_incremental = self.config.incremental && self.latest_checkpoint_id.is_some();

        let checkpoint_path = metadata.checkpoint_path(&self.config.checkpoint_dir);

        // Create checkpoint directory
        fs::create_dir_all(&checkpoint_path)?;

        // Write metadata
        let metadata_path = checkpoint_path.join("metadata.json");
        let metadata_json = metadata.to_json()?;
        fs::write(&metadata_path, &metadata_json)?;

        // Update tracking state
        self.latest_checkpoint_id = Some(id);
        self.last_checkpoint_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );

        // Cleanup old checkpoints
        self.cleanup_old_checkpoints()?;

        Ok(metadata)
    }

    /// Creates a checkpoint with additional state data.
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint creation fails.
    pub fn create_checkpoint_with_state(
        &mut self,
        wal_position: u64,
        source_offsets: HashMap<String, u64>,
        watermark: Option<i64>,
        state_data: &[u8],
    ) -> Result<IncrementalCheckpointMetadata, IncrementalCheckpointError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let epoch = self.current_epoch.load(Ordering::Acquire);

        let mut metadata = IncrementalCheckpointMetadata::new(id, epoch);
        metadata.wal_position = wal_position;
        metadata.source_offsets = source_offsets;
        metadata.watermark = watermark;
        metadata.parent_id = self.latest_checkpoint_id;
        metadata.is_incremental = self.config.incremental && self.latest_checkpoint_id.is_some();

        let checkpoint_path = metadata.checkpoint_path(&self.config.checkpoint_dir);
        fs::create_dir_all(&checkpoint_path)?;

        // Write state data
        let state_path = checkpoint_path.join("state.bin");
        fs::write(&state_path, state_data)?;

        #[allow(clippy::cast_possible_truncation)]
        // usize → u64: lossless on 64-bit, acceptable on 32-bit
        {
            metadata.size_bytes = state_data.len() as u64;
        }

        // Write metadata
        let metadata_path = checkpoint_path.join("metadata.json");
        let metadata_json = metadata.to_json()?;
        fs::write(&metadata_path, &metadata_json)?;

        // Update tracking state
        self.latest_checkpoint_id = Some(id);
        self.last_checkpoint_time.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );

        info!(
            checkpoint_id = id,
            epoch = epoch,
            wal_position = wal_position,
            size_bytes = metadata.size_bytes,
            "Created checkpoint with state"
        );

        self.cleanup_old_checkpoints()?;

        Ok(metadata)
    }

    /// Finds the latest checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if reading checkpoint metadata fails.
    pub fn find_latest_checkpoint(
        &self,
    ) -> Result<Option<IncrementalCheckpointMetadata>, IncrementalCheckpointError> {
        let Some(id) = self.latest_checkpoint_id else {
            return Ok(None);
        };

        self.load_checkpoint_metadata(id)
    }

    /// Loads checkpoint metadata by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if reading metadata fails.
    pub fn load_checkpoint_metadata(
        &self,
        id: u64,
    ) -> Result<Option<IncrementalCheckpointMetadata>, IncrementalCheckpointError> {
        let checkpoint_dir = self
            .config
            .checkpoint_dir
            .join(format!("checkpoint_{id:016x}"));
        let metadata_path = checkpoint_dir.join("metadata.json");

        if !metadata_path.exists() {
            return Ok(None);
        }

        let metadata_json = fs::read_to_string(&metadata_path)?;
        let metadata = IncrementalCheckpointMetadata::from_json(&metadata_json)?;
        Ok(Some(metadata))
    }

    /// Loads state data from a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if reading state data fails.
    pub fn load_checkpoint_state(&self, id: u64) -> Result<Vec<u8>, IncrementalCheckpointError> {
        let checkpoint_dir = self
            .config
            .checkpoint_dir
            .join(format!("checkpoint_{id:016x}"));
        let state_path = checkpoint_dir.join("state.bin");

        if !state_path.exists() {
            return Err(IncrementalCheckpointError::NotFound(format!(
                "State file not found for checkpoint {id}"
            )));
        }

        Ok(fs::read(&state_path)?)
    }

    /// Lists all checkpoints sorted by ID (newest first).
    ///
    /// # Errors
    ///
    /// Returns an error if reading checkpoints fails.
    pub fn list_checkpoints(
        &self,
    ) -> Result<Vec<IncrementalCheckpointMetadata>, IncrementalCheckpointError> {
        let mut checkpoints = Vec::new();

        if !self.config.checkpoint_dir.exists() {
            return Ok(checkpoints);
        }

        for entry in fs::read_dir(&self.config.checkpoint_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(id_str) = name_str.strip_prefix("checkpoint_") {
                if let Ok(id) = u64::from_str_radix(id_str, 16) {
                    if let Ok(Some(metadata)) = self.load_checkpoint_metadata(id) {
                        checkpoints.push(metadata);
                    }
                }
            }
        }

        // Sort by ID descending (newest first)
        checkpoints.sort_by(|a, b| b.id.cmp(&a.id));

        Ok(checkpoints)
    }

    /// Cleans up old checkpoints beyond the retention limit.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    pub fn cleanup_old_checkpoints(&self) -> Result<(), IncrementalCheckpointError> {
        self.cleanup_old_checkpoints_keep(self.config.max_retained)
    }

    /// Cleans up old checkpoints, keeping only `keep_count` most recent.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    pub fn cleanup_old_checkpoints_keep(
        &self,
        keep_count: usize,
    ) -> Result<(), IncrementalCheckpointError> {
        let checkpoints = self.list_checkpoints()?;

        if checkpoints.len() <= keep_count {
            return Ok(());
        }

        // Remove checkpoints beyond retention limit
        for checkpoint in checkpoints.iter().skip(keep_count) {
            let checkpoint_dir = checkpoint.checkpoint_path(&self.config.checkpoint_dir);
            if checkpoint_dir.exists() {
                debug!(checkpoint_id = checkpoint.id, "Removing old checkpoint");
                fs::remove_dir_all(&checkpoint_dir)?;
            }
        }

        Ok(())
    }

    /// Deletes a specific checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    pub fn delete_checkpoint(&mut self, id: u64) -> Result<(), IncrementalCheckpointError> {
        let checkpoint_dir = self
            .config
            .checkpoint_dir
            .join(format!("checkpoint_{id:016x}"));

        if !checkpoint_dir.exists() {
            return Err(IncrementalCheckpointError::NotFound(format!(
                "Checkpoint {id} not found"
            )));
        }

        fs::remove_dir_all(&checkpoint_dir)?;

        // Update latest checkpoint if we deleted it
        if self.latest_checkpoint_id == Some(id) {
            let checkpoints = self.list_checkpoints()?;
            self.latest_checkpoint_id = checkpoints.first().map(|c| c.id);
        }

        info!(checkpoint_id = id, "Deleted checkpoint");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_checkpoint_config_validation() {
        let temp_dir = TempDir::new().unwrap();

        // Valid config
        let config = CheckpointConfig::new(temp_dir.path())
            .with_interval(Duration::from_secs(60))
            .with_max_retained(3);
        assert!(config.validate().is_ok());

        // Invalid: zero max_retained
        let invalid = CheckpointConfig::new(temp_dir.path()).with_max_retained(0);
        assert!(invalid.validate().is_err());

        // Invalid: zero interval
        let invalid = CheckpointConfig::new(temp_dir.path()).with_interval(Duration::ZERO);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_checkpoint_metadata() {
        let metadata = IncrementalCheckpointMetadata::new(1, 100);
        assert_eq!(metadata.id, 1);
        assert_eq!(metadata.epoch, 100);
        assert!(metadata.is_incremental);
        assert!(metadata.parent_id.is_none());

        // Test JSON roundtrip
        let json = metadata.to_json().unwrap();
        let restored = IncrementalCheckpointMetadata::from_json(&json).unwrap();
        assert_eq!(restored.id, metadata.id);
        assert_eq!(restored.epoch, metadata.epoch);
    }

    #[test]
    fn test_checkpoint_path() {
        let metadata = IncrementalCheckpointMetadata::new(0x1234_5678_9abc_def0, 1);
        let base = Path::new("/data/checkpoints");
        let path = metadata.checkpoint_path(base);
        assert_eq!(
            path,
            PathBuf::from("/data/checkpoints/checkpoint_123456789abcdef0")
        );
    }

    #[test]
    fn test_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path());

        let manager = IncrementalCheckpointManager::new(config).unwrap();
        assert!(manager.latest_checkpoint_id().is_none());
        assert_eq!(manager.epoch(), 0);
    }

    #[test]
    fn test_manager_create_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path());

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();
        manager.set_epoch(42);

        let metadata = manager.create_checkpoint(1000).unwrap();
        assert_eq!(metadata.epoch, 42);
        assert_eq!(metadata.wal_position, 1000);
        assert!(metadata.parent_id.is_none()); // First checkpoint

        // Second checkpoint should have parent
        let metadata2 = manager.create_checkpoint(2000).unwrap();
        assert_eq!(metadata2.parent_id, Some(metadata.id));
    }

    #[test]
    fn test_manager_create_checkpoint_with_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path());

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();
        manager.set_epoch(10);

        let mut offsets = HashMap::new();
        offsets.insert("source1".to_string(), 100);
        offsets.insert("source2".to_string(), 200);

        let state_data = b"test state data";
        let metadata = manager
            .create_checkpoint_with_state(500, offsets.clone(), Some(5000), state_data)
            .unwrap();

        assert_eq!(metadata.epoch, 10);
        assert_eq!(metadata.wal_position, 500);
        assert_eq!(metadata.watermark, Some(5000));
        assert_eq!(metadata.source_offsets.len(), 2);
        assert_eq!(metadata.source_offsets.get("source1"), Some(&100));

        // Load state back
        let loaded = manager.load_checkpoint_state(metadata.id).unwrap();
        assert_eq!(loaded, state_data);
    }

    #[test]
    fn test_manager_list_checkpoints() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path()).with_max_retained(10);

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();

        // Create multiple checkpoints
        for i in 0..5 {
            manager.set_epoch(i);
            manager.create_checkpoint(i * 100).unwrap();
        }

        let checkpoints = manager.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 5);

        // Should be sorted newest first
        assert!(checkpoints[0].id > checkpoints[4].id);
    }

    #[test]
    fn test_manager_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path()).with_max_retained(2);

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();

        // Create 5 checkpoints (should only keep 2)
        for i in 0..5 {
            manager.set_epoch(i);
            manager.create_checkpoint(i * 100).unwrap();
        }

        let checkpoints = manager.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);

        // Should have the 2 newest
        assert_eq!(checkpoints[0].epoch, 4);
        assert_eq!(checkpoints[1].epoch, 3);
    }

    #[test]
    fn test_manager_find_latest() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path());

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();

        // No checkpoints yet
        assert!(manager.find_latest_checkpoint().unwrap().is_none());

        // Create a checkpoint
        manager.set_epoch(1);
        let metadata = manager.create_checkpoint(100).unwrap();

        let latest = manager.find_latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.id, metadata.id);
    }

    #[test]
    fn test_manager_delete_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path()).with_max_retained(10);

        let mut manager = IncrementalCheckpointManager::new(config).unwrap();

        manager.set_epoch(1);
        let meta1 = manager.create_checkpoint(100).unwrap();
        manager.set_epoch(2);
        let meta2 = manager.create_checkpoint(200).unwrap();

        assert_eq!(manager.list_checkpoints().unwrap().len(), 2);

        manager.delete_checkpoint(meta1.id).unwrap();

        let checkpoints = manager.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].id, meta2.id);
    }

    #[test]
    fn test_manager_should_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config = CheckpointConfig::new(temp_dir.path()).with_interval(Duration::from_secs(1));

        let manager = IncrementalCheckpointManager::new(config).unwrap();

        // Initially should checkpoint (last_checkpoint_time is 0)
        assert!(manager.should_checkpoint());
    }

    #[test]
    fn test_scan_existing_checkpoints() {
        let temp_dir = TempDir::new().unwrap();

        // Create checkpoint directories with metadata.json so they're recognized
        for id in [1u64, 2, 3] {
            let dir = temp_dir.path().join(format!("checkpoint_{id:016x}"));
            fs::create_dir_all(&dir).unwrap();
            let metadata = IncrementalCheckpointMetadata::new(id, id * 10);
            fs::write(dir.join("metadata.json"), metadata.to_json().unwrap()).unwrap();
        }

        let config = CheckpointConfig::new(temp_dir.path());
        let manager = IncrementalCheckpointManager::new(config).unwrap();

        // Next ID should be 4 (max 3 + 1)
        assert_eq!(manager.next_id.load(Ordering::Relaxed), 4);
        // Latest should be 3
        assert_eq!(manager.latest_checkpoint_id, Some(3));
    }

    #[test]
    fn test_scan_skips_partial_checkpoint_dirs() {
        let temp_dir = TempDir::new().unwrap();

        // Valid checkpoint with metadata.json
        let dir1 = temp_dir.path().join("checkpoint_0000000000000001");
        fs::create_dir_all(&dir1).unwrap();
        let metadata = IncrementalCheckpointMetadata::new(1, 10);
        fs::write(dir1.join("metadata.json"), metadata.to_json().unwrap()).unwrap();

        // Partial checkpoint dir (no metadata.json) — should be skipped for latest_id
        let dir3 = temp_dir.path().join("checkpoint_0000000000000003");
        fs::create_dir_all(&dir3).unwrap();

        let config = CheckpointConfig::new(temp_dir.path());
        let manager = IncrementalCheckpointManager::new(config).unwrap();

        // Latest should be 1 (dir 3 was skipped due to missing metadata)
        assert_eq!(manager.latest_checkpoint_id, Some(1));
        // next_id should still be 4 (max dir id 3 + 1) to avoid ID collision
        assert_eq!(manager.next_id.load(Ordering::Relaxed), 4);
    }
}
