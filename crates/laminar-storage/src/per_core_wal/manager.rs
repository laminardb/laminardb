//! Per-core WAL manager for coordinating multiple core writers.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::entry::PerCoreWalEntry;
use super::error::PerCoreWalError;
use super::reader::PerCoreWalReader;
use super::writer::CoreWalWriter;

/// Configuration for per-core WAL.
#[derive(Debug, Clone)]
pub struct PerCoreWalConfig {
    /// Base directory for WAL segments.
    pub base_dir: PathBuf,
    /// Number of cores (determines number of segments).
    pub num_cores: usize,
    /// Segment file name pattern (default: "wal-{core_id}.log").
    pub segment_pattern: String,
}

impl PerCoreWalConfig {
    /// Creates a new per-core WAL configuration.
    #[must_use]
    pub fn new(base_dir: &Path, num_cores: usize) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            num_cores,
            segment_pattern: "wal-{core_id}.log".to_string(),
        }
    }

    /// Sets a custom segment file pattern.
    ///
    /// The pattern must contain `{core_id}` which will be replaced with the core ID.
    #[must_use]
    pub fn with_segment_pattern(mut self, pattern: &str) -> Self {
        self.segment_pattern = pattern.to_string();
        self
    }

    /// Returns the path to a segment file for a given core.
    #[must_use]
    pub fn segment_path(&self, core_id: usize) -> PathBuf {
        let filename = self
            .segment_pattern
            .replace("{core_id}", &core_id.to_string());
        self.base_dir.join(filename)
    }
}

/// Per-core WAL manager.
///
/// Coordinates all core WAL writers and provides epoch management.
/// Each core has its own writer, eliminating cross-core synchronization on writes.
pub struct PerCoreWalManager {
    /// Configuration.
    config: PerCoreWalConfig,
    /// Per-core writers (index = `core_id`).
    writers: Vec<CoreWalWriter>,
    /// Global epoch counter (shared, atomic).
    global_epoch: Arc<AtomicU64>,
}

impl PerCoreWalManager {
    /// Creates a new per-core WAL manager.
    ///
    /// Creates segment files for all cores if they don't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation or writer initialization fails.
    pub fn new(config: PerCoreWalConfig) -> Result<Self, PerCoreWalError> {
        // Ensure base directory exists
        std::fs::create_dir_all(&config.base_dir)?;

        // Create writers for each core
        let mut writers = Vec::with_capacity(config.num_cores);
        for core_id in 0..config.num_cores {
            let path = config.segment_path(core_id);
            let writer = CoreWalWriter::new(core_id, &path)?;
            writers.push(writer);
        }

        Ok(Self {
            config,
            writers,
            global_epoch: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Opens an existing per-core WAL manager.
    ///
    /// All segment files must exist.
    ///
    /// # Errors
    ///
    /// Returns an error if any segment file doesn't exist or can't be opened.
    pub fn open(config: PerCoreWalConfig) -> Result<Self, PerCoreWalError> {
        let mut writers = Vec::with_capacity(config.num_cores);
        let mut max_epoch = 0u64;

        for core_id in 0..config.num_cores {
            let path = config.segment_path(core_id);
            if !path.exists() {
                return Err(PerCoreWalError::SegmentNotFound { core_id, path });
            }

            // Read to find last valid position and max epoch
            let mut reader = PerCoreWalReader::open(core_id, &path)?;
            let valid_end = reader.find_valid_end()?;

            // Read entries to find max epoch
            let mut reader = PerCoreWalReader::open(core_id, &path)?;
            for entry in reader.read_all()? {
                max_epoch = max_epoch.max(entry.epoch);
            }

            let writer = CoreWalWriter::open_at(core_id, &path, valid_end)?;
            writers.push(writer);
        }

        Ok(Self {
            config,
            writers,
            global_epoch: Arc::new(AtomicU64::new(max_epoch)),
        })
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &PerCoreWalConfig {
        &self.config
    }

    /// Returns the number of cores.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.config.num_cores
    }

    /// Returns the current global epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Acquire)
    }

    /// Returns a shared reference to the global epoch counter.
    #[must_use]
    pub fn epoch_ref(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.global_epoch)
    }

    /// Gets the writer for a specific core.
    ///
    /// # Panics
    ///
    /// Panics if `core_id >= num_cores`.
    #[must_use]
    pub fn writer(&mut self, core_id: usize) -> &mut CoreWalWriter {
        &mut self.writers[core_id]
    }

    /// Gets the writer for a specific core, checking bounds.
    ///
    /// # Errors
    ///
    /// Returns an error if `core_id >= num_cores`.
    pub fn writer_checked(
        &mut self,
        core_id: usize,
    ) -> Result<&mut CoreWalWriter, PerCoreWalError> {
        if core_id >= self.config.num_cores {
            return Err(PerCoreWalError::InvalidCoreId {
                core_id,
                max_core_id: self.config.num_cores - 1,
            });
        }
        Ok(&mut self.writers[core_id])
    }

    /// Advances the global epoch and returns the new epoch.
    #[must_use]
    pub fn advance_epoch(&self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Sets the epoch on all writers.
    pub fn set_epoch_all(&mut self, epoch: u64) {
        for writer in &mut self.writers {
            writer.set_epoch(epoch);
        }
    }

    /// Syncs all segment files.
    ///
    /// # Errors
    ///
    /// Returns an error if any sync fails.
    pub fn sync_all(&mut self) -> Result<(), PerCoreWalError> {
        for writer in &mut self.writers {
            writer.sync()?;
        }
        Ok(())
    }

    /// Returns the positions of all writers.
    #[must_use]
    pub fn positions(&self) -> Vec<u64> {
        self.writers.iter().map(CoreWalWriter::position).collect()
    }

    /// Truncates all segments at the specified positions.
    ///
    /// # Errors
    ///
    /// Returns an error if any truncation fails.
    pub fn truncate_all(&mut self, positions: &[u64]) -> Result<(), PerCoreWalError> {
        if positions.len() != self.config.num_cores {
            return Err(PerCoreWalError::InvalidCoreId {
                core_id: positions.len(),
                max_core_id: self.config.num_cores - 1,
            });
        }

        for (core_id, &position) in positions.iter().enumerate() {
            self.writers[core_id].truncate(position)?;
        }
        Ok(())
    }

    /// Resets (truncates to zero) all segments.
    ///
    /// Used after successful checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if any reset fails.
    pub fn reset_all(&mut self) -> Result<(), PerCoreWalError> {
        for writer in &mut self.writers {
            writer.reset()?;
        }
        Ok(())
    }

    /// Merges entries from all segments, sorted by (epoch, timestamp).
    ///
    /// # Errors
    ///
    /// Returns an error if reading any segment fails.
    pub fn merge_segments(&self) -> Result<Vec<PerCoreWalEntry>, PerCoreWalError> {
        let mut entries = Vec::new();

        for core_id in 0..self.config.num_cores {
            let path = self.config.segment_path(core_id);
            if path.exists() {
                let mut reader = PerCoreWalReader::open(core_id, &path)?;
                entries.extend(reader.read_all()?);
            }
        }

        // Sort by (epoch, timestamp_ns, core_id, sequence)
        entries.sort();

        Ok(entries)
    }

    /// Merges entries from all segments up to a specific epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if reading any segment fails.
    pub fn merge_segments_up_to_epoch(
        &self,
        max_epoch: u64,
    ) -> Result<Vec<PerCoreWalEntry>, PerCoreWalError> {
        let mut entries = Vec::new();

        for core_id in 0..self.config.num_cores {
            let path = self.config.segment_path(core_id);
            if path.exists() {
                let mut reader = PerCoreWalReader::open(core_id, &path)?;
                entries.extend(reader.read_up_to_epoch(max_epoch)?);
            }
        }

        // Sort by (epoch, timestamp_ns, core_id, sequence)
        entries.sort();

        Ok(entries)
    }

    /// Writes an epoch barrier to all segments.
    ///
    /// Used during checkpoint to mark epoch boundaries.
    ///
    /// # Errors
    ///
    /// Returns an error if any write fails.
    pub fn write_epoch_barrier_all(&mut self) -> Result<(), PerCoreWalError> {
        for writer in &mut self.writers {
            writer.append_epoch_barrier()?;
        }
        Ok(())
    }

    /// Returns total size of all segments.
    #[must_use]
    pub fn total_size(&self) -> u64 {
        self.writers.iter().map(CoreWalWriter::position).sum()
    }

    /// Returns segment path for a core.
    #[must_use]
    pub fn segment_path(&self, core_id: usize) -> PathBuf {
        self.config.segment_path(core_id)
    }
}

impl std::fmt::Debug for PerCoreWalManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerCoreWalManager")
            .field("config", &self.config)
            .field("num_cores", &self.config.num_cores)
            .field("epoch", &self.epoch())
            .field("total_size", &self.total_size())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_manager(num_cores: usize) -> (PerCoreWalManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = PerCoreWalConfig::new(temp_dir.path(), num_cores);
        let manager = PerCoreWalManager::new(config).unwrap();
        (manager, temp_dir)
    }

    #[test]
    fn test_manager_creation() {
        let (manager, _temp_dir) = setup_manager(4);
        assert_eq!(manager.num_cores(), 4);
        assert_eq!(manager.epoch(), 0);
    }

    #[test]
    fn test_writer_access() {
        let (mut manager, _temp_dir) = setup_manager(4);

        let writer = manager.writer(0);
        assert_eq!(writer.core_id(), 0);

        let writer = manager.writer(3);
        assert_eq!(writer.core_id(), 3);
    }

    #[test]
    fn test_writer_checked_invalid() {
        let (mut manager, _temp_dir) = setup_manager(4);

        let result = manager.writer_checked(5);
        assert!(matches!(result, Err(PerCoreWalError::InvalidCoreId { .. })));
    }

    #[test]
    fn test_advance_epoch() {
        let (manager, _temp_dir) = setup_manager(4);

        assert_eq!(manager.epoch(), 0);
        let new_epoch = manager.advance_epoch();
        assert_eq!(new_epoch, 1);
        assert_eq!(manager.epoch(), 1);

        let new_epoch = manager.advance_epoch();
        assert_eq!(new_epoch, 2);
    }

    #[test]
    fn test_set_epoch_all() {
        let (mut manager, _temp_dir) = setup_manager(4);

        manager.set_epoch_all(5);

        for core_id in 0..4 {
            assert_eq!(manager.writer(core_id).epoch(), 5);
        }
    }

    #[test]
    fn test_parallel_writes() {
        let (mut manager, _temp_dir) = setup_manager(4);
        manager.set_epoch_all(1);

        // Write to different cores
        manager.writer(0).append_put(b"key0", b"value0").unwrap();
        manager.writer(1).append_put(b"key1", b"value1").unwrap();
        manager.writer(2).append_put(b"key2", b"value2").unwrap();
        manager.writer(3).append_put(b"key3", b"value3").unwrap();

        manager.sync_all().unwrap();

        // Verify positions
        let positions = manager.positions();
        assert!(positions[0] > 0);
        assert!(positions[1] > 0);
        assert!(positions[2] > 0);
        assert!(positions[3] > 0);
    }

    #[test]
    fn test_merge_segments() {
        let (mut manager, _temp_dir) = setup_manager(2);

        // Epoch 1 writes
        manager.set_epoch_all(1);
        manager.writer(0).append_put(b"key0a", b"value0a").unwrap();
        manager.writer(1).append_put(b"key1a", b"value1a").unwrap();

        // Epoch 2 writes
        manager.set_epoch_all(2);
        manager.writer(0).append_put(b"key0b", b"value0b").unwrap();
        manager.writer(1).append_put(b"key1b", b"value1b").unwrap();

        manager.sync_all().unwrap();

        // Merge and verify ordering
        let entries = manager.merge_segments().unwrap();
        assert_eq!(entries.len(), 4);

        // Epoch 1 entries should come first
        assert_eq!(entries[0].epoch, 1);
        assert_eq!(entries[1].epoch, 1);
        // Epoch 2 entries next
        assert_eq!(entries[2].epoch, 2);
        assert_eq!(entries[3].epoch, 2);
    }

    #[test]
    fn test_merge_segments_up_to_epoch() {
        let (mut manager, _temp_dir) = setup_manager(2);

        manager.set_epoch_all(1);
        manager.writer(0).append_put(b"key0a", b"value0a").unwrap();

        manager.set_epoch_all(2);
        manager.writer(0).append_put(b"key0b", b"value0b").unwrap();

        manager.set_epoch_all(3);
        manager.writer(0).append_put(b"key0c", b"value0c").unwrap();

        manager.sync_all().unwrap();

        let entries = manager.merge_segments_up_to_epoch(2).unwrap();
        assert_eq!(entries.len(), 2); // Only epochs 1 and 2
    }

    #[test]
    fn test_reset_all() {
        let (mut manager, _temp_dir) = setup_manager(2);

        manager.writer(0).append_put(b"key0", b"value0").unwrap();
        manager.writer(1).append_put(b"key1", b"value1").unwrap();
        manager.sync_all().unwrap();

        assert!(manager.total_size() > 0);

        manager.reset_all().unwrap();

        assert_eq!(manager.total_size(), 0);
        assert_eq!(manager.positions(), vec![0, 0]);
    }

    #[test]
    fn test_truncate_all() {
        let (mut manager, _temp_dir) = setup_manager(2);

        manager.writer(0).append_put(b"key0", b"value0").unwrap();
        let pos0 = manager.writer(0).position();

        manager.writer(0).append_put(b"key0b", b"value0b").unwrap();
        manager.writer(1).append_put(b"key1", b"value1").unwrap();

        manager.sync_all().unwrap();

        manager.truncate_all(&[pos0, 0]).unwrap();

        let positions = manager.positions();
        assert_eq!(positions[0], pos0);
        assert_eq!(positions[1], 0);
    }

    #[test]
    fn test_write_epoch_barrier_all() {
        let (mut manager, _temp_dir) = setup_manager(2);
        manager.set_epoch_all(1);

        manager.write_epoch_barrier_all().unwrap();
        manager.sync_all().unwrap();

        let entries = manager.merge_segments().unwrap();
        assert_eq!(entries.len(), 2);
        for entry in entries {
            assert!(matches!(
                entry.operation,
                super::super::entry::WalOperation::EpochBarrier { .. }
            ));
        }
    }

    #[test]
    fn test_open_existing() {
        let temp_dir = TempDir::new().unwrap();
        let config = PerCoreWalConfig::new(temp_dir.path(), 2);

        // Create and write
        {
            let mut manager = PerCoreWalManager::new(config.clone()).unwrap();
            manager.set_epoch_all(5);
            manager.writer(0).append_put(b"key0", b"value0").unwrap();
            manager.writer(1).append_put(b"key1", b"value1").unwrap();
            manager.sync_all().unwrap();
        }

        // Reopen
        let manager = PerCoreWalManager::open(config).unwrap();
        assert_eq!(manager.epoch(), 5);

        let entries = manager.merge_segments().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_segment_path() {
        let config = PerCoreWalConfig::new(Path::new("/data/wal"), 4);
        assert_eq!(config.segment_path(0), PathBuf::from("/data/wal/wal-0.log"));
        assert_eq!(config.segment_path(3), PathBuf::from("/data/wal/wal-3.log"));

        let custom = config.with_segment_pattern("segment-{core_id}.wal");
        assert_eq!(
            custom.segment_path(1),
            PathBuf::from("/data/wal/segment-1.wal")
        );
    }

    #[test]
    fn test_debug_format() {
        let (manager, _temp_dir) = setup_manager(4);
        let debug_str = format!("{manager:?}");
        assert!(debug_str.contains("PerCoreWalManager"));
        assert!(debug_str.contains("num_cores"));
    }
}
