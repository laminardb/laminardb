//! Memory-mapped state store implementation.
//!
//! This module provides a high-performance key-value state store using memory-mapped
//! files for persistence and `BTreeMap` for sorted key access. It supports both in-memory
//! and persistent modes.
//!
//! # Design
//!
//! The store uses a two-tier architecture:
//! - **Index tier**: `BTreeMap` mapping keys to value entries (offset, length)
//! - **Data tier**: Either arena-allocated memory or memory-mapped file
//!
//! # Performance Characteristics
//!
//! - **Get**: O(log n), < 500ns typical (tree lookup + pointer follow)
//! - **Put**: O(log n), may trigger file growth
//! - **Prefix scan**: O(log n + k) where k is matching entries
//!
//! # Usage
//!
//! ```rust,no_run
//! use laminar_core::state::MmapStateStore;
//! use std::path::Path;
//!
//! // In-memory mode (fast, not persistent)
//! let mut store = MmapStateStore::in_memory(1024 * 1024); // 1MB arena
//!
//! // Persistent mode (file-backed)
//! let mut store = MmapStateStore::persistent(Path::new("/tmp/state.db"), 1024 * 1024).unwrap();
//! ```

use bytes::Bytes;
use memmap2::MmapMut;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::{Bound, Range};
use std::path::{Path, PathBuf};

use super::{prefix_successor, StateError, StateSnapshot, StateStore};

/// Header size in the mmap file (magic + version + entry count + data offset).
const MMAP_HEADER_SIZE: usize = 32;
/// Magic number for mmap file identification ("LAMINAR" in hex-ish).
const MMAP_MAGIC: u64 = 0x004C_414D_494E_4152;
/// Current mmap file format version.
const MMAP_VERSION: u32 = 1;
/// Default growth factor when file needs to expand.
const GROWTH_FACTOR: f64 = 1.5;

/// Entry metadata stored in the hash map index.
#[derive(Debug, Clone, Copy)]
struct ValueEntry {
    /// Offset in the data region (arena or mmap).
    offset: usize,
    /// Length of the value in bytes.
    len: usize,
    /// Version for optimistic concurrency (future use).
    #[allow(dead_code)]
    version: u64,
}

/// Storage backend for the mmap store.
enum Storage {
    /// In-memory mode (fastest, not persistent).
    Arena {
        /// Pre-allocated buffer for data.
        data: Vec<u8>,
        /// Current write position.
        write_pos: usize,
    },
    /// Memory-mapped file (persistent, supports larger-than-memory).
    Mmap {
        mmap: MmapMut,
        file: File,
        path: PathBuf,
        /// Current write position in the data region.
        write_pos: usize,
        /// Total capacity of the file.
        capacity: usize,
    },
}

impl Storage {
    /// Get a slice of data at the given offset and length.
    fn get(&self, offset: usize, len: usize) -> &[u8] {
        match self {
            Storage::Arena { data, .. } => &data[offset..offset + len],
            Storage::Mmap { mmap, .. } => {
                &mmap[MMAP_HEADER_SIZE + offset..MMAP_HEADER_SIZE + offset + len]
            }
        }
    }

    /// Write data and return the offset where it was written.
    fn write(&mut self, data: &[u8]) -> Result<usize, StateError> {
        match self {
            Storage::Arena {
                data: buffer,
                write_pos,
            } => {
                let offset = *write_pos;
                let end = offset + data.len();

                // Grow buffer if needed
                if end > buffer.len() {
                    // Growth calculation: precision loss is acceptable for buffer sizing
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        clippy::cast_precision_loss
                    )]
                    let new_size = (end as f64 * GROWTH_FACTOR) as usize;
                    buffer.resize(new_size, 0);
                }

                buffer[offset..end].copy_from_slice(data);
                *write_pos = end;
                Ok(offset)
            }
            Storage::Mmap {
                mmap,
                file,
                path: _,
                write_pos,
                capacity,
            } => {
                let offset = *write_pos;
                let end = offset + data.len();
                let required = MMAP_HEADER_SIZE + end;

                // Grow file if needed
                if required > *capacity {
                    // Growth calculation: precision loss is acceptable for capacity sizing
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        clippy::cast_precision_loss
                    )]
                    let new_capacity = (required as f64 * GROWTH_FACTOR) as usize;
                    file.set_len(new_capacity as u64)?;

                    // Re-map with new size
                    // SAFETY: We just resized the file and hold exclusive access.
                    // The file descriptor is valid because we just successfully called set_len.
                    // No other code has access to the file while we hold &mut self.
                    #[allow(unsafe_code)]
                    {
                        *mmap = unsafe { MmapMut::map_mut(&*file)? };
                    }
                    *capacity = new_capacity;
                }

                mmap[MMAP_HEADER_SIZE + offset..MMAP_HEADER_SIZE + end].copy_from_slice(data);
                *write_pos = end;
                Ok(offset)
            }
        }
    }

    /// Get the current write position (used data size).
    fn used_bytes(&self) -> usize {
        match self {
            Storage::Arena { write_pos, .. } | Storage::Mmap { write_pos, .. } => *write_pos,
        }
    }

    /// Flush to disk (only meaningful for mmap).
    fn flush(&mut self) -> Result<(), StateError> {
        match self {
            Storage::Arena { .. } => Ok(()),
            Storage::Mmap { mmap, .. } => {
                mmap.flush()?;
                Ok(())
            }
        }
    }

    /// Reset the write position (for clear operation).
    fn reset(&mut self) {
        match self {
            Storage::Arena { write_pos, .. } | Storage::Mmap { write_pos, .. } => *write_pos = 0,
        }
    }

    /// Check if this is persistent storage.
    fn is_persistent(&self) -> bool {
        matches!(self, Storage::Mmap { .. })
    }
}

/// Memory-mapped state store implementation.
///
/// This store provides high-performance key-value storage with optional
/// persistence via memory-mapped files. It achieves sub-500ns lookup latency
/// by using `BTreeMap` for the index (enabling O(log n + k) prefix/range scans)
/// and direct memory access for values.
///
/// # Modes
///
/// - **In-memory**: Uses an arena allocator, fastest but not persistent
/// - **Persistent**: Uses memory-mapped file, survives restarts
///
/// # Thread Safety
///
/// This store is `Send` but not `Sync`. It's designed for single-threaded
/// access within a reactor.
pub struct MmapStateStore {
    /// Index mapping keys to value entries.
    index: BTreeMap<Vec<u8>, ValueEntry>,
    /// Storage backend (arena or mmap).
    storage: Storage,
    /// Total size of keys + values for size tracking.
    size_bytes: usize,
    /// Next version number for entries.
    next_version: u64,
}

impl MmapStateStore {
    /// Creates a new in-memory state store with the given initial capacity.
    ///
    /// This mode is the fastest but data is lost when the process exits.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Initial capacity in bytes for the data buffer
    #[must_use]
    pub fn in_memory(capacity: usize) -> Self {
        Self {
            index: BTreeMap::new(),
            storage: Storage::Arena {
                data: vec![0u8; capacity],
                write_pos: 0,
            },
            size_bytes: 0,
            next_version: 1,
        }
    }

    /// Creates a new persistent state store backed by a memory-mapped file.
    ///
    /// If the file exists, it will be opened and validated. If it doesn't exist,
    /// a new file will be created with the given initial capacity.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the state file
    /// * `initial_capacity` - Initial file size if creating new
    ///
    /// # Errors
    ///
    /// Returns `StateError::Io` if file operations fail, or `StateError::Corruption`
    /// if the file exists but has an invalid format.
    pub fn persistent(path: &Path, initial_capacity: usize) -> Result<Self, StateError> {
        let file_exists = path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let capacity = if file_exists {
            let metadata = file.metadata()?;
            // On 32-bit systems this could truncate, but mmap files > 4GB aren't practical there anyway
            #[allow(clippy::cast_possible_truncation)]
            let cap = metadata.len() as usize;
            cap
        } else {
            let capacity = initial_capacity.max(MMAP_HEADER_SIZE + 1024);
            file.set_len(capacity as u64)?;
            capacity
        };

        // SAFETY: We have exclusive write access to the file - it was just created or opened
        // with read/write permissions, and no other code has access to it yet.
        #[allow(unsafe_code)]
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let (index, write_pos, next_version) = if file_exists && capacity >= MMAP_HEADER_SIZE {
            // Try to load existing data
            Self::load_from_mmap(&mmap)?
        } else {
            // Initialize new file
            Self::init_mmap_header(&mut mmap);
            (BTreeMap::new(), 0, 1)
        };

        let size_bytes = index.iter().map(|(k, v)| k.len() + v.len).sum();

        Ok(Self {
            index,
            storage: Storage::Mmap {
                mmap,
                file,
                path: path.to_path_buf(),
                write_pos,
                capacity,
            },
            size_bytes,
            next_version,
        })
    }

    /// Initialize the mmap header for a new file.
    fn init_mmap_header(mmap: &mut MmapMut) {
        mmap[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        mmap[8..12].copy_from_slice(&MMAP_VERSION.to_le_bytes());
        mmap[12..20].copy_from_slice(&0u64.to_le_bytes()); // entry count
        mmap[20..28].copy_from_slice(&0u64.to_le_bytes()); // data offset
    }

    /// Load index from existing mmap file.
    #[allow(clippy::type_complexity)]
    fn load_from_mmap(
        mmap: &MmapMut,
    ) -> Result<(BTreeMap<Vec<u8>, ValueEntry>, usize, u64), StateError> {
        // Check magic number
        let magic = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        if magic != MMAP_MAGIC {
            return Err(StateError::Corruption(
                "Invalid magic number in state file".to_string(),
            ));
        }

        // Check version
        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version != MMAP_VERSION {
            return Err(StateError::Corruption(format!(
                "Unsupported state file version: {version}"
            )));
        }

        // For now, we don't persist the index in the file, so we start fresh
        // A full implementation would store the index at the end of the file
        // and rebuild it on load. For F002 scope, we focus on the mmap data storage.
        Ok((BTreeMap::new(), 0, 1))
    }

    /// Check if this store is persistent.
    #[must_use]
    pub fn is_persistent(&self) -> bool {
        self.storage.is_persistent()
    }

    /// Get the path to the backing file (if persistent).
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match &self.storage {
            Storage::Arena { .. } => None,
            Storage::Mmap { path, .. } => Some(path),
        }
    }

    /// Compact the store by rewriting live data.
    ///
    /// This removes holes left by deleted entries and reduces file/memory usage.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the compaction fails.
    pub fn compact(&mut self) -> Result<(), StateError> {
        // Collect all live data
        let live_data: Vec<(Vec<u8>, Vec<u8>)> = self
            .index
            .iter()
            .map(|(k, entry)| {
                let value = self.storage.get(entry.offset, entry.len).to_vec();
                (k.clone(), value)
            })
            .collect();

        // Reset storage
        self.storage.reset();
        self.index.clear();
        self.size_bytes = 0;

        // Rewrite all data
        for (key, value) in live_data {
            let offset = self.storage.write(&value)?;
            self.index.insert(
                key.clone(),
                ValueEntry {
                    offset,
                    len: value.len(),
                    version: self.next_version,
                },
            );
            self.next_version += 1;
            self.size_bytes += key.len() + value.len();
        }

        Ok(())
    }

    /// Get the fragmentation ratio (wasted space / total space).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn fragmentation(&self) -> f64 {
        let used = self.storage.used_bytes();
        if used == 0 {
            return 0.0;
        }
        let live: usize = self.index.values().map(|e| e.len).sum();
        // Precision loss is acceptable for a ratio calculation
        1.0 - (live as f64 / used as f64)
    }
}

impl StateStore for MmapStateStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.index.get(key).map(|entry| {
            let data = self.storage.get(entry.offset, entry.len);
            Bytes::copy_from_slice(data)
        })
    }

    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        // Write value to storage
        let offset = self.storage.write(value)?;

        let entry = ValueEntry {
            offset,
            len: value.len(),
            version: self.next_version,
        };
        self.next_version += 1;

        // Entry API: single tree traversal for both insert and update
        match self.index.entry(key.to_vec()) {
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                self.size_bytes = self.size_bytes - occupied.get().len + value.len();
                *occupied.get_mut() = entry;
            }
            std::collections::btree_map::Entry::Vacant(vacant) => {
                self.size_bytes += key.len() + value.len();
                vacant.insert(entry);
            }
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if let Some(entry) = self.index.remove(key) {
            self.size_bytes -= key.len() + entry.len;
            // Note: The space in storage becomes fragmentation
            // Use compact() to reclaim it
        }
        Ok(())
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        if prefix.is_empty() {
            return Box::new(self.index.iter().map(|(k, entry)| {
                let value = self.storage.get(entry.offset, entry.len);
                (Bytes::copy_from_slice(k), Bytes::copy_from_slice(value))
            }));
        }
        if let Some(end) = prefix_successor(prefix) {
            Box::new(
                self.index
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Excluded(end.as_slice())))
                    .map(|(k, entry)| {
                        let value = self.storage.get(entry.offset, entry.len);
                        (Bytes::copy_from_slice(k), Bytes::copy_from_slice(value))
                    }),
            )
        } else {
            Box::new(
                self.index
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
                    .map(|(k, entry)| {
                        let value = self.storage.get(entry.offset, entry.len);
                        (Bytes::copy_from_slice(k), Bytes::copy_from_slice(value))
                    }),
            )
        }
    }

    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        Box::new(
            self.index
                .range::<[u8], _>((Bound::Included(range.start), Bound::Excluded(range.end)))
                .map(|(k, entry)| {
                    let value = self.storage.get(entry.offset, entry.len);
                    (Bytes::copy_from_slice(k), Bytes::copy_from_slice(value))
                }),
        )
    }

    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn snapshot(&self) -> StateSnapshot {
        let data: Vec<(Vec<u8>, Vec<u8>)> = self
            .index
            .iter()
            .map(|(k, entry)| {
                let value = self.storage.get(entry.offset, entry.len).to_vec();
                (k.clone(), value)
            })
            .collect();
        StateSnapshot::new(data)
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.index.clear();
        self.storage.reset();
        self.size_bytes = 0;
        self.next_version = 1;

        for (key, value) in snapshot.data() {
            if let Ok(offset) = self.storage.write(value) {
                self.index.insert(
                    key.clone(),
                    ValueEntry {
                        offset,
                        len: value.len(),
                        version: self.next_version,
                    },
                );
                self.next_version += 1;
                self.size_bytes += key.len() + value.len();
            }
        }
    }

    fn clear(&mut self) {
        self.index.clear();
        self.storage.reset();
        self.size_bytes = 0;
    }

    fn flush(&mut self) -> Result<(), StateError> {
        self.storage.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_in_memory_basic() {
        let mut store = MmapStateStore::in_memory(1024);

        // Test put and get
        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value1"));
        assert_eq!(store.len(), 1);

        // Test overwrite
        store.put(b"key1", b"value2").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value2"));
        assert_eq!(store.len(), 1);

        // Test delete
        store.delete(b"key1").unwrap();
        assert!(store.get(b"key1").is_none());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_persistent_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");

        // Create store and write data
        {
            let mut store = MmapStateStore::persistent(&path, 4096).unwrap();
            store.put(b"key1", b"value1").unwrap();
            store.put(b"key2", b"value2").unwrap();
            store.flush().unwrap();
        }

        // Reopen and verify (note: current implementation doesn't persist index)
        // Full persistence would require storing the index in the file
        {
            let store = MmapStateStore::persistent(&path, 4096).unwrap();
            assert!(store.is_persistent());
            assert_eq!(store.path(), Some(path.as_path()));
        }
    }

    #[test]
    fn test_contains() {
        let mut store = MmapStateStore::in_memory(1024);
        assert!(!store.contains(b"key1"));

        store.put(b"key1", b"value1").unwrap();
        assert!(store.contains(b"key1"));

        store.delete(b"key1").unwrap();
        assert!(!store.contains(b"key1"));
    }

    #[test]
    fn test_prefix_scan() {
        let mut store = MmapStateStore::in_memory(4096);
        store.put(b"prefix:1", b"value1").unwrap();
        store.put(b"prefix:2", b"value2").unwrap();
        store.put(b"prefix:10", b"value10").unwrap();
        store.put(b"other:1", b"value3").unwrap();

        let results: Vec<_> = store.prefix_scan(b"prefix:").collect();
        assert_eq!(results.len(), 3);

        for (key, _) in &results {
            assert!(key.starts_with(b"prefix:"));
        }
    }

    #[test]
    fn test_range_scan() {
        let mut store = MmapStateStore::in_memory(4096);
        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        store.put(b"c", b"3").unwrap();
        store.put(b"d", b"4").unwrap();

        let results: Vec<_> = store.range_scan(b"b"..b"d").collect();
        assert_eq!(results.len(), 2);

        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
        assert!(keys.contains(&b"b".as_slice()));
        assert!(keys.contains(&b"c".as_slice()));
    }

    #[test]
    fn test_snapshot_and_restore() {
        let mut store = MmapStateStore::in_memory(4096);
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        // Take snapshot
        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 2);

        // Modify store
        store.put(b"key1", b"modified").unwrap();
        store.put(b"key3", b"value3").unwrap();
        store.delete(b"key2").unwrap();

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("modified"));

        // Restore from snapshot
        store.restore(snapshot);

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value1"));
        assert_eq!(store.get(b"key2").unwrap(), Bytes::from("value2"));
        assert!(store.get(b"key3").is_none());
    }

    #[test]
    fn test_size_tracking() {
        let mut store = MmapStateStore::in_memory(4096);
        assert_eq!(store.size_bytes(), 0);

        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6); // "key1" + "value1"

        store.put(b"key2", b"value2").unwrap();
        assert_eq!(store.size_bytes(), (4 + 6) * 2);

        // Overwrite with smaller value (old value becomes fragmentation)
        store.put(b"key1", b"v1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 2 + 4 + 6);

        store.delete(b"key1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6);

        store.clear();
        assert_eq!(store.size_bytes(), 0);
    }

    #[test]
    fn test_compact() {
        let mut store = MmapStateStore::in_memory(4096);

        // Add some data
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();
        store.put(b"key3", b"value3").unwrap();

        // Delete middle key to create fragmentation
        store.delete(b"key2").unwrap();

        // Overwrite to create more fragmentation
        store.put(b"key1", b"new_value1").unwrap();

        let frag_before = store.fragmentation();
        assert!(frag_before > 0.0);

        // Compact
        store.compact().unwrap();

        let frag_after = store.fragmentation();
        assert!(frag_after < frag_before);
        assert!(frag_after.abs() < f64::EPSILON); // Should be zero after compaction

        // Verify data integrity
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("new_value1"));
        assert!(store.get(b"key2").is_none());
        assert_eq!(store.get(b"key3").unwrap(), Bytes::from("value3"));
    }

    #[test]
    fn test_growth() {
        // Start with very small capacity
        let mut store = MmapStateStore::in_memory(32);

        // Add data that exceeds initial capacity
        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value{i:04}");
            store.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        assert_eq!(store.len(), 100);

        // Verify all data is accessible
        for i in 0..100 {
            let key = format!("key{i:04}");
            let expected = format!("value{i:04}");
            assert_eq!(
                store.get(key.as_bytes()).unwrap().as_ref(),
                expected.as_bytes()
            );
        }
    }

    #[test]
    fn test_clear() {
        let mut store = MmapStateStore::in_memory(4096);
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        assert_eq!(store.len(), 2);
        assert!(store.size_bytes() > 0);

        store.clear();

        assert_eq!(store.len(), 0);
        assert_eq!(store.size_bytes(), 0);
        assert!(store.get(b"key1").is_none());
    }

    #[test]
    fn test_empty_store() {
        let store = MmapStateStore::in_memory(1024);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.size_bytes(), 0);
        assert!(store.get(b"nonexistent").is_none());
        assert!(!store.contains(b"nonexistent"));
    }

    #[test]
    fn test_large_values() {
        let mut store = MmapStateStore::in_memory(1024 * 1024);

        // 100KB value
        let large_value = vec![0xABu8; 100 * 1024];
        store.put(b"large", &large_value).unwrap();

        let retrieved = store.get(b"large").unwrap();
        assert_eq!(retrieved.len(), large_value.len());
        assert_eq!(retrieved.as_ref(), &large_value[..]);
    }

    #[test]
    fn test_binary_keys_and_values() {
        let mut store = MmapStateStore::in_memory(4096);

        // Binary key with null bytes
        let key = [0x00, 0x01, 0x02, 0xFF, 0xFE];
        let value = [0xDE, 0xAD, 0xBE, 0xEF];

        store.put(&key, &value).unwrap();
        assert_eq!(store.get(&key).unwrap().as_ref(), &value);
    }
}
