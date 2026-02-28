//! WAL-backed state store implementation.
//!
//! This module provides a state store that logs all mutations to a Write-Ahead Log
//! before applying them, enabling recovery after crashes and supporting exactly-once
//! semantics.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use laminar_core::state::{MmapStateStore, StateError, StateSnapshot, StateStore};

use crate::{CheckpointManager, WalEntry, WalPosition, WriteAheadLog};

/// A state store backed by Write-Ahead Log for durability.
///
/// This wraps `MmapStateStore` and logs all mutations to a WAL before
/// applying them. On recovery, it replays the WAL to rebuild the index
/// that was lost (since `MmapStateStore` doesn't persist the index).
pub struct WalStateStore {
    /// The underlying state store.
    store: MmapStateStore,
    /// Write-Ahead Log for durability.
    wal: WriteAheadLog,
    /// Path to the WAL file (kept for future use).
    _wal_path: PathBuf,
    /// Whether to sync WAL on every write (for testing).
    sync_on_write: bool,
    /// Checkpoint manager for periodic snapshots.
    checkpoint_manager: Option<CheckpointManager>,
    /// Last checkpoint time.
    last_checkpoint: AtomicU64,
    /// Current source offsets for exactly-once semantics.
    source_offsets: HashMap<String, u64>,
    /// Current watermark for recovery.
    current_watermark: Option<i64>,
}

impl WalStateStore {
    /// Create a new WAL-backed state store.
    ///
    /// # Arguments
    ///
    /// * `state_path` - Path to the state file (for `MmapStateStore`)
    /// * `wal_path` - Path to the WAL file
    /// * `initial_capacity` - Initial capacity for the state store
    /// * `sync_interval` - WAL sync interval for group commit
    ///
    /// # Errors
    ///
    /// Returns an error if file operations fail.
    pub fn new(
        state_path: &Path,
        wal_path: &Path,
        initial_capacity: usize,
        sync_interval: Duration,
    ) -> Result<Self, StateError> {
        let wal_path_buf = wal_path.to_path_buf();
        let wal = WriteAheadLog::new(&wal_path_buf, sync_interval)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        let store = MmapStateStore::persistent(state_path, initial_capacity)?;

        Ok(Self {
            store,
            wal,
            _wal_path: wal_path_buf,
            sync_on_write: false,
            checkpoint_manager: None,
            last_checkpoint: AtomicU64::new(0),
            source_offsets: HashMap::new(),
            current_watermark: None,
        })
    }

    /// Create an in-memory WAL-backed state store (mainly for testing).
    ///
    /// # Arguments
    ///
    /// * `wal_path` - Path to the WAL file
    /// * `capacity` - Initial capacity for the in-memory store
    /// * `sync_interval` - WAL sync interval
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be created.
    pub fn in_memory(
        wal_path: &Path,
        capacity: usize,
        sync_interval: Duration,
    ) -> Result<Self, StateError> {
        let wal_path_buf = wal_path.to_path_buf();
        let wal = WriteAheadLog::new(&wal_path_buf, sync_interval)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        let store = MmapStateStore::in_memory(capacity);

        Ok(Self {
            store,
            wal,
            _wal_path: wal_path_buf,
            sync_on_write: false,
            checkpoint_manager: None,
            last_checkpoint: AtomicU64::new(0),
            source_offsets: HashMap::new(),
            current_watermark: None,
        })
    }

    /// Enable sync on every write (for testing).
    pub fn set_sync_on_write(&mut self, enabled: bool) {
        self.sync_on_write = enabled;
        self.wal.set_sync_on_write(enabled);
    }

    /// Enable checkpointing with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `checkpoint_dir` - Directory to store checkpoint files
    /// * `interval` - How often to create checkpoints
    /// * `max_retained` - Maximum number of checkpoints to retain
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint directory cannot be created.
    pub fn enable_checkpointing(
        &mut self,
        checkpoint_dir: PathBuf,
        interval: Duration,
        max_retained: usize,
    ) -> Result<(), StateError> {
        let manager = CheckpointManager::new(checkpoint_dir, interval, max_retained)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        self.checkpoint_manager = Some(manager);
        Ok(())
    }

    /// Check if it's time to create a checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if the system time is before the Unix epoch.
    #[must_use]
    pub fn should_checkpoint(&self) -> bool {
        if let Some(ref manager) = self.checkpoint_manager {
            let last = self.last_checkpoint.load(Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_secs();

            now - last >= manager.interval().as_secs()
        } else {
            false
        }
    }

    /// Update source offset for exactly-once semantics.
    pub fn update_source_offset(&mut self, source: String, offset: u64) {
        self.source_offsets.insert(source, offset);
    }

    /// Get source offset for a given source.
    #[must_use]
    pub fn get_source_offset(&self, source: &str) -> Option<u64> {
        self.source_offsets.get(source).copied()
    }

    /// Update the current watermark.
    pub fn update_watermark(&mut self, watermark: i64) {
        self.current_watermark = Some(watermark);
    }

    /// Get the current watermark.
    #[must_use]
    pub fn current_watermark(&self) -> Option<i64> {
        self.current_watermark
    }

    /// Commit the current state with watermark to WAL.
    ///
    /// This writes a commit entry to the WAL that includes:
    /// - Source offsets for exactly-once replay
    /// - Current watermark for recovery
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails.
    pub fn commit(&mut self) -> Result<(), StateError> {
        self.wal
            .append(&WalEntry::Commit {
                offsets: self.source_offsets.clone(),
                watermark: self.current_watermark,
            })
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        if self.sync_on_write {
            self.wal
                .sync()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;
        }

        Ok(())
    }

    /// Recover state from WAL.
    ///
    /// This reads the WAL from the beginning and replays all entries
    /// to rebuild the state. This is necessary because `MmapStateStore`
    /// doesn't persist its index.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint or WAL cannot be read,
    /// or if state restoration fails.
    pub fn recover(&mut self) -> Result<(), StateError> {
        let mut start_position = 0u64;

        // Try to recover from checkpoint first
        if let Some(ref manager) = self.checkpoint_manager {
            if let Ok(Some(checkpoint)) = manager.find_latest_checkpoint() {
                // Load checkpoint state
                let state_data = checkpoint
                    .load_state()
                    .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

                // Deserialize and restore snapshot
                let snapshot = StateSnapshot::from_bytes(&state_data)?;
                self.store.restore(snapshot);

                // Update recovery state
                start_position = checkpoint.metadata.wal_position.offset;
                self.source_offsets
                    .clone_from(&checkpoint.metadata.source_offsets);
                self.current_watermark = checkpoint.metadata.watermark;
                self.last_checkpoint
                    .store(checkpoint.metadata.timestamp, Ordering::Relaxed);
            }
        }

        // Read WAL entries from start position
        let mut reader = self
            .wal
            .read_from(start_position)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        // Replay entries after checkpoint
        // Use read_next() to handle torn writes gracefully
        loop {
            use crate::wal::WalReadResult;

            match reader.read_next() {
                Ok(WalReadResult::Entry(entry)) => {
                    match entry {
                        WalEntry::Put { key, value } => {
                            // Apply without logging (we're replaying from log)
                            self.store.put(&key, &value)?;
                        }
                        WalEntry::Delete { key } => {
                            // Apply without logging
                            self.store.delete(&key)?;
                        }
                        WalEntry::Checkpoint { .. } => {
                            // Skip checkpoint markers during recovery
                        }
                        WalEntry::Commit { offsets, watermark } => {
                            // Update source offsets for exactly-once semantics
                            for (source, offset) in offsets {
                                self.source_offsets.insert(source, offset);
                            }
                            // Restore watermark for recovery
                            if watermark.is_some() {
                                self.current_watermark = watermark;
                            }
                        }
                    }
                }
                Ok(WalReadResult::Eof) => {
                    // Clean end of WAL - done
                    break;
                }
                Ok(WalReadResult::TornWrite { position, reason }) => {
                    // Torn write at tail - this is expected after crash
                    // Log it and stop reading (remaining data is garbage)
                    tracing::warn!(
                        position,
                        reason,
                        "Torn write detected during recovery - truncating WAL"
                    );
                    break;
                }
                Ok(WalReadResult::ChecksumMismatch { position }) => {
                    // Checksum error - corruption or torn write
                    // Treat as end of valid data
                    tracing::warn!(position, "CRC mismatch during recovery - truncating WAL");
                    break;
                }
                Ok(WalReadResult::Corrupted { position, reason }) => {
                    tracing::warn!(
                        position,
                        reason,
                        "[LDB-6006] Corrupted WAL entry during recovery - truncating WAL"
                    );
                    break;
                }
                Err(e) => {
                    // Real error (I/O, deserialization)
                    return Err(StateError::Corruption(format!("WAL read error: {e}")));
                }
            }
        }

        Ok(())
    }

    /// Create a checkpoint.
    ///
    /// This creates a snapshot of the current state and writes it to disk,
    /// along with a checkpoint marker in the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be created or written.
    pub fn checkpoint(&mut self) -> Result<(), StateError> {
        if let Some(ref manager) = self.checkpoint_manager {
            // Get current state snapshot
            let snapshot = self.store.snapshot();
            let state_data = snapshot
                .to_bytes()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Get current WAL position
            let wal_position = WalPosition {
                offset: self.wal.position(),
            };

            // Create checkpoint
            let checkpoint = manager
                .create_checkpoint(
                    &state_data,
                    wal_position,
                    self.source_offsets.clone(),
                    self.current_watermark,
                )
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Write checkpoint marker to WAL
            self.wal
                .append(&WalEntry::Checkpoint {
                    id: checkpoint.metadata.id,
                })
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Force sync after checkpoint
            self.wal
                .sync()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Update last checkpoint time
            self.last_checkpoint
                .store(checkpoint.metadata.timestamp, Ordering::Relaxed);

            // Clean up old checkpoints
            manager
                .cleanup_old_checkpoints()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Optionally truncate WAL if safe
            // This would require tracking minimum required WAL position
            // across all consumers
        }

        Ok(())
    }

    /// Get the current WAL position.
    #[must_use]
    pub fn wal_position(&self) -> u64 {
        self.wal.position()
    }

    /// Truncate the WAL at the given position.
    ///
    /// Used after successful checkpointing to remove old entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL cannot be truncated.
    pub fn truncate_wal(&mut self, position: u64) -> Result<(), StateError> {
        self.wal
            .truncate(position)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;
        Ok(())
    }
}

impl StateStore for WalStateStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.store.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        // Log to WAL first
        self.wal
            .append(&WalEntry::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        // Then apply to store
        self.store.put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        // Log to WAL first
        self.wal
            .append(&WalEntry::Delete { key: key.to_vec() })
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        // Then apply to store
        self.store.delete(key)
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        self.store.prefix_scan(prefix)
    }

    fn range_scan<'a>(
        &'a self,
        range: std::ops::Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        self.store.range_scan(range)
    }

    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.store.contains(key)
    }

    fn size_bytes(&self) -> usize {
        self.store.size_bytes()
    }

    fn len(&self) -> usize {
        self.store.len()
    }

    fn snapshot(&self) -> StateSnapshot {
        self.store.snapshot()
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        // Note: This bypasses the WAL. In production, we'd want to
        // log this operation or handle it differently.
        self.store.restore(snapshot);
    }

    fn clear(&mut self) {
        // Note: This bypasses the WAL. In production, we'd want to
        // log this operation or handle it differently.
        self.store.clear();
    }

    fn flush(&mut self) -> Result<(), StateError> {
        // Sync WAL first
        self.wal
            .sync()
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        // Then flush the store
        self.store.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_state_store_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        let mut store =
            WalStateStore::new(&state_path, &wal_path, 1024 * 1024, Duration::from_secs(1))
                .unwrap();

        // Test put and get
        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap().as_ref(), b"value1");

        // Test delete
        store.delete(b"key1").unwrap();
        assert!(store.get(b"key1").is_none());

        // Test multiple operations
        store.put(b"key2", b"value2").unwrap();
        store.put(b"key3", b"value3").unwrap();
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_wal_state_store_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        // Create store and add data
        {
            let mut store =
                WalStateStore::new(&state_path, &wal_path, 1024 * 1024, Duration::from_secs(1))
                    .unwrap();

            store.put(b"key1", b"value1").unwrap();
            store.put(b"key2", b"value2").unwrap();
            store.delete(b"key1").unwrap();
            store.put(b"key3", b"value3").unwrap();
            store.flush().unwrap();
        }

        // Create new store and recover
        {
            let mut store =
                WalStateStore::new(&state_path, &wal_path, 1024 * 1024, Duration::from_secs(1))
                    .unwrap();

            // With index persistence, data is available immediately!
            // assert_eq!(store.len(), 0);
            // Note: Even though we satisfy len check, we still run recover() to ensure WAL replay
            // respects existing state and doesn't duplicate/corrupt.
            // Actually, recover() is needed to restore watermarks/offsets which are NOT in the index yet.

            // Recover from WAL (should be idempotent regarding data)
            store.recover().unwrap();

            // After recovery, data should be restored (and correctly maintained)
            assert_eq!(store.len(), 2);
            assert!(store.get(b"key1").is_none()); // Was deleted
            assert_eq!(store.get(b"key2").unwrap().as_ref(), b"value2");
            assert_eq!(store.get(b"key3").unwrap().as_ref(), b"value3");
        }
    }

    #[test]
    fn test_wal_state_store_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        let mut store =
            WalStateStore::new(&state_path, &wal_path, 1024 * 1024, Duration::from_secs(1))
                .unwrap();

        // Add some data
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        // Create checkpoint
        let _checkpoint_pos = store.wal_position();
        store.checkpoint().unwrap();

        // Add more data after checkpoint
        store.put(b"key3", b"value3").unwrap();

        // In the future, we could truncate the WAL after checkpoint
        // and still recover from the checkpoint + remaining WAL
    }

    // These tests verify the complete recovery flow including:
    // - Checkpoint + WAL replay
    // - Watermark restoration
    // - Source offset restoration
    // - Torn write recovery

    #[test]
    fn test_recovery_checkpoint_plus_wal_replay() {
        // This test verifies:
        // 1. Create state with multiple keys
        // 2. Create checkpoint
        // 3. Add more WAL entries after checkpoint
        // 4. "Crash" (drop store)
        // 5. Recover and verify all state is restored

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        // Phase 1: Create initial state and checkpoint
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            // Enable checkpointing
            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
                .unwrap();

            // Add initial data
            store.put(b"key1", b"value1").unwrap();
            store.put(b"key2", b"value2").unwrap();
            store.put(b"key3", b"value3").unwrap();

            // Set watermark and source offset
            store.update_watermark(5000);
            store.update_source_offset("kafka-topic-0".to_string(), 100);
            store.commit().unwrap();

            // Create checkpoint (captures keys 1-3, watermark 5000, offset 100)
            store.checkpoint().unwrap();

            // Add MORE data after checkpoint (this is in WAL only)
            store.put(b"key4", b"value4").unwrap();
            store.put(b"key5", b"value5").unwrap();
            store.delete(b"key2").unwrap(); // Delete key2

            // Update watermark and offset again
            store.update_watermark(7500);
            store.update_source_offset("kafka-topic-0".to_string(), 150);
            store.commit().unwrap();

            store.flush().unwrap();
            // Drop store - simulates crash
        }

        // Phase 2: Recover and verify
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            // Enable checkpointing (needed to find checkpoint)
            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
                .unwrap();

            // Recover from checkpoint + WAL
            store.recover().unwrap();

            // Verify all state is restored correctly
            assert_eq!(store.get(b"key1").unwrap().as_ref(), b"value1");
            assert!(store.get(b"key2").is_none()); // Was deleted after checkpoint
            assert_eq!(store.get(b"key3").unwrap().as_ref(), b"value3");
            assert_eq!(store.get(b"key4").unwrap().as_ref(), b"value4"); // Added after checkpoint
            assert_eq!(store.get(b"key5").unwrap().as_ref(), b"value5"); // Added after checkpoint

            // Verify watermark is restored (should be 7500 from last commit)
            assert_eq!(store.current_watermark(), Some(7500));

            // Verify source offset is restored
            assert_eq!(store.get_source_offset("kafka-topic-0"), Some(150));

            // Total count: key1, key3, key4, key5 (key2 was deleted)
            assert_eq!(store.len(), 4);
        }
    }

    #[test]
    fn test_recovery_watermark_from_checkpoint_only() {
        // This test verifies watermark is restored from checkpoint
        // when there are no commits in the WAL after checkpoint

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        // Phase 1: Create state with watermark and checkpoint
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
                .unwrap();

            store.put(b"data", b"test").unwrap();
            store.update_watermark(12345);
            store.update_source_offset("source-1".to_string(), 999);
            store.commit().unwrap();

            // Checkpoint captures watermark 12345
            store.checkpoint().unwrap();

            // Add data but NO commit after checkpoint
            store.put(b"more_data", b"more_test").unwrap();
            store.flush().unwrap();
        }

        // Phase 2: Recover - watermark should come from checkpoint
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
                .unwrap();

            store.recover().unwrap();

            // Data should be restored
            assert_eq!(store.get(b"data").unwrap().as_ref(), b"test");
            assert_eq!(store.get(b"more_data").unwrap().as_ref(), b"more_test");

            // Watermark should be from checkpoint (12345)
            assert_eq!(store.current_watermark(), Some(12345));

            // Source offset should be from checkpoint
            assert_eq!(store.get_source_offset("source-1"), Some(999));
        }
    }

    #[test]
    fn test_recovery_torn_write_at_wal_tail() {
        // This test verifies recovery works when there's a torn write
        // (partial record) at the end of the WAL

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        // Phase 1: Create valid state
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            store.put(b"key1", b"value1").unwrap();
            store.put(b"key2", b"value2").unwrap();
            store.flush().unwrap();
        }

        // Phase 2: Simulate torn write by appending garbage to WAL
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            // Write partial header (only 3 bytes of a 4-byte length field)
            file.write_all(&[0x10, 0x00, 0x00]).unwrap();
            file.sync_all().unwrap();
        }

        // Phase 3: Recover - should handle torn write gracefully
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            // Recovery should succeed despite torn write
            // The torn write at the tail is ignored
            store.recover().unwrap();

            // Valid data should be restored
            assert_eq!(store.get(b"key1").unwrap().as_ref(), b"value1");
            assert_eq!(store.get(b"key2").unwrap().as_ref(), b"value2");
            assert_eq!(store.len(), 2);
        }
    }

    #[test]
    fn test_recovery_with_wal_repair() {
        // This test verifies the WAL repair() function works correctly
        // to truncate corrupted/torn records

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        // Phase 1: Create valid state
        let valid_wal_len: u64;
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            store.put(b"good1", b"data1").unwrap();
            store.put(b"good2", b"data2").unwrap();
            store.flush().unwrap();

            valid_wal_len = store.wal_position();
        }

        // Phase 2: Append garbage (simulating crash during write)
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            // Write incomplete record: length says 100 bytes but only 5 bytes follow
            let len: u32 = 100;
            let crc: u32 = 0xDEAD_BEEF;
            file.write_all(&len.to_le_bytes()).unwrap();
            file.write_all(&crc.to_le_bytes()).unwrap();
            file.write_all(&[1, 2, 3, 4, 5]).unwrap(); // Only 5 bytes, not 100
            file.sync_all().unwrap();
        }

        // Phase 3: Repair WAL and verify
        {
            let mut wal = crate::WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();
            let repaired_len = wal.repair().unwrap();

            // Should have truncated back to valid length
            assert_eq!(repaired_len, valid_wal_len);

            // Verify we can still read valid entries
            let reader = wal.read_from(0).unwrap();
            let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(entries.len(), 2);
        }

        // Phase 4: Recover after repair
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            store.recover().unwrap();

            assert_eq!(store.get(b"good1").unwrap().as_ref(), b"data1");
            assert_eq!(store.get(b"good2").unwrap().as_ref(), b"data2");
            assert_eq!(store.len(), 2);
        }
    }

    #[test]
    fn test_recovery_multiple_checkpoints() {
        // This test verifies recovery uses the LATEST checkpoint

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        // Phase 1: Create multiple checkpoints
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            store
                .enable_checkpointing(
                    checkpoint_dir.clone(),
                    Duration::from_secs(60),
                    5, // Keep 5 checkpoints
                )
                .unwrap();

            // Checkpoint 1: only key1
            store.put(b"key1", b"v1").unwrap();
            store.update_watermark(1000);
            store.commit().unwrap();
            store.checkpoint().unwrap();

            // Checkpoint 2: key1 + key2
            store.put(b"key2", b"v2").unwrap();
            store.update_watermark(2000);
            store.commit().unwrap();
            store.checkpoint().unwrap();

            // Checkpoint 3: key1 + key2 + key3
            store.put(b"key3", b"v3").unwrap();
            store.update_watermark(3000);
            store.commit().unwrap();
            store.checkpoint().unwrap();

            store.flush().unwrap();
        }

        // Phase 2: Recover - should use checkpoint 3 (latest)
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 5)
                .unwrap();

            store.recover().unwrap();

            // Should have all 3 keys from latest checkpoint
            assert_eq!(store.get(b"key1").unwrap().as_ref(), b"v1");
            assert_eq!(store.get(b"key2").unwrap().as_ref(), b"v2");
            assert_eq!(store.get(b"key3").unwrap().as_ref(), b"v3");
            assert_eq!(store.len(), 3);

            // Watermark should be from latest checkpoint (3000)
            assert_eq!(store.current_watermark(), Some(3000));
        }
    }

    #[test]
    fn test_recovery_no_checkpoint_wal_only() {
        // This test verifies recovery works with WAL only (no checkpoint)

        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.db");
        let wal_path = temp_dir.path().join("state.wal");

        // Phase 1: Create state without checkpointing
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();
            store.set_sync_on_write(true);

            store.put(b"a", b"1").unwrap();
            store.put(b"b", b"2").unwrap();
            store.put(b"c", b"3").unwrap();
            store.delete(b"b").unwrap();

            store.update_watermark(9999);
            store.update_source_offset("src".to_string(), 42);
            store.commit().unwrap();

            store.flush().unwrap();
        }

        // Phase 2: Recover from WAL only
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_millis(10),
            )
            .unwrap();

            // No checkpointing enabled - pure WAL replay
            store.recover().unwrap();

            assert_eq!(store.get(b"a").unwrap().as_ref(), b"1");
            assert!(store.get(b"b").is_none()); // Was deleted
            assert_eq!(store.get(b"c").unwrap().as_ref(), b"3");
            assert_eq!(store.len(), 2);

            // Watermark and offset from commit in WAL
            assert_eq!(store.current_watermark(), Some(9999));
            assert_eq!(store.get_source_offset("src"), Some(42));
        }
    }
}
