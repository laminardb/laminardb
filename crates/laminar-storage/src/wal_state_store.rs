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

use crate::{CheckpointManager, WriteAheadLog, WalEntry, WalPosition};

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
                .unwrap()
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
                let state_data = checkpoint.load_state()
                    .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

                // Deserialize and restore snapshot
                let snapshot = StateSnapshot::from_bytes(&state_data)?;
                self.store.restore(snapshot);

                // Update recovery state
                start_position = checkpoint.metadata.wal_position.offset;
                self.source_offsets.clone_from(&checkpoint.metadata.source_offsets);
                self.last_checkpoint
                    .store(checkpoint.metadata.timestamp, Ordering::Relaxed);
            }
        }

        // Read WAL entries from start position
        let reader = self.wal.read_from(start_position)
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

        // Replay entries after checkpoint
        for entry_result in reader {
            let entry = entry_result
                .map_err(|e| StateError::Corruption(format!("WAL read error: {e}")))?;

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
                WalEntry::Commit { offsets } => {
                    // Update source offsets for exactly-once semantics
                    for (source, offset) in offsets {
                        self.source_offsets.insert(source, offset);
                    }
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
            let state_data = snapshot.to_bytes()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Get current WAL position
            let wal_position = WalPosition {
                offset: self.wal.position(),
            };

            // Create checkpoint
            let checkpoint = manager.create_checkpoint(
                &state_data,
                wal_position,
                self.source_offsets.clone(),
            )
            .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Write checkpoint marker to WAL
            self.wal
                .append(&WalEntry::Checkpoint {
                    id: checkpoint.metadata.id,
                })
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Force sync after checkpoint
            self.wal.sync()
                .map_err(|e| StateError::Io(std::io::Error::other(e)))?;

            // Update last checkpoint time
            self.last_checkpoint.store(checkpoint.metadata.timestamp, Ordering::Relaxed);

            // Clean up old checkpoints
            manager.cleanup_old_checkpoints()
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
        self.wal.truncate(position)
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
            .append(&WalEntry::Delete {
                key: key.to_vec(),
            })
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
        self.wal.sync()
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

        let mut store = WalStateStore::new(
            &state_path,
            &wal_path,
            1024 * 1024,
            Duration::from_secs(1),
        ).unwrap();

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
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_secs(1),
            ).unwrap();

            store.put(b"key1", b"value1").unwrap();
            store.put(b"key2", b"value2").unwrap();
            store.delete(b"key1").unwrap();
            store.put(b"key3", b"value3").unwrap();
            store.flush().unwrap();
        }

        // Create new store and recover
        {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                1024 * 1024,
                Duration::from_secs(1),
            ).unwrap();

            // Before recovery, the index is empty (MmapStateStore doesn't persist it)
            assert_eq!(store.len(), 0);

            // Recover from WAL
            store.recover().unwrap();

            // After recovery, data should be restored
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

        let mut store = WalStateStore::new(
            &state_path,
            &wal_path,
            1024 * 1024,
            Duration::from_secs(1),
        ).unwrap();

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
}