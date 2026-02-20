//! # State Store Module
//!
//! High-performance state storage for streaming operators.
//!
//! ## Design Goals
//!
//! - **< 500ns lookup latency** for point queries
//! - **Zero-copy** access where possible
//! - **Lock-free** for single-threaded access
//! - **Memory-mapped** for large state
//!
//! ## State Backends
//!
//! - **[`InMemoryStore`]**: BTreeMap-based, fast lookups with O(log n + k) prefix/range scans
//! - **[`MmapStateStore`]**: Memory-mapped, supports larger-than-memory state with optional persistence
//! - **Hybrid**: Combination with hot/cold separation (future)
//!
//! ## Example
//!
//! ```rust
//! use laminar_core::state::{StateStore, StateStoreExt, InMemoryStore};
//!
//! let mut store = InMemoryStore::new();
//!
//! // Basic key-value operations
//! store.put(b"user:1", b"alice").unwrap();
//! assert_eq!(store.get(b"user:1").unwrap().as_ref(), b"alice");
//!
//! // Typed state access (requires StateStoreExt)
//! store.put_typed(b"count", &42u64).unwrap();
//! let count: u64 = store.get_typed(b"count").unwrap().unwrap();
//! assert_eq!(count, 42);
//!
//! // Snapshots for checkpointing
//! let snapshot = store.snapshot();
//! store.delete(b"user:1").unwrap();
//! assert!(store.get(b"user:1").is_none());
//!
//! // Restore from snapshot
//! store.restore(snapshot);
//! assert_eq!(store.get(b"user:1").unwrap().as_ref(), b"alice");
//! ```
//!
//! ## Memory-Mapped Store Example
//!
//! ```rust,no_run
//! use laminar_core::state::{StateStore, MmapStateStore};
//! use std::path::Path;
//!
//! // In-memory mode (fast, not persistent)
//! let mut store = MmapStateStore::in_memory(1024 * 1024);
//! store.put(b"key", b"value").unwrap();
//!
//! // Persistent mode (survives restarts)
//! let mut persistent = MmapStateStore::persistent(
//!     Path::new("/tmp/state.db"),
//!     1024 * 1024
//! ).unwrap();
//! persistent.put(b"key", b"value").unwrap();
//! persistent.flush().unwrap();
//! ```

use bytes::Bytes;
use rkyv::{
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RkyvError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::ops::Range;

/// A single mutation entry within an incremental snapshot.
///
/// Captures the exact key/value bytes for each state change, enabling
/// delta-based checkpointing to object stores without full state copies.
///
/// - `Put(key, value)` — a key was inserted or updated
/// - `Delete(key)` — a key was removed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeEntry {
    /// A key was inserted or updated: `(key, value)`.
    Put(Vec<u8>, Vec<u8>),
    /// A key was deleted.
    Delete(Vec<u8>),
}

/// An incremental snapshot capturing only mutations since the last checkpoint.
///
/// State stores that support incremental snapshots return this from
/// [`StateStore::incremental_snapshot`]. The recovery system applies
/// these deltas on top of a base snapshot to reconstruct full state.
#[derive(Debug, Clone)]
pub struct IncrementalSnapshot {
    /// The ordered list of mutations since `base_epoch`.
    pub changes: Vec<ChangeEntry>,
    /// The epoch of the base (full) snapshot these changes apply to.
    pub base_epoch: u64,
    /// The epoch this incremental snapshot represents.
    pub epoch: u64,
}

/// Compute the lexicographic successor of a byte prefix.
///
/// Returns `None` if no successor exists (empty prefix or all bytes are 0xFF).
/// Used by `BTreeMap::range()` to efficiently bound prefix scans.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut successor = prefix.to_vec();
    // Walk backwards, incrementing the last non-0xFF byte
    while let Some(last) = successor.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(successor);
        }
        successor.pop();
    }
    // All bytes were 0xFF — no successor exists
    None
}

/// Trait for state store implementations.
///
/// This is the core abstraction for operator state in Ring 0 (hot path).
/// All implementations must achieve < 500ns lookup latency for point queries.
///
/// # Thread Safety
///
/// State stores are `Send` but not `Sync`. They are designed for single-threaded
/// access within a reactor. Cross-thread communication uses SPSC queues.
///
/// # Memory Model
///
/// - `get()` returns `Bytes` which is a cheap reference-counted handle
/// - `put()` copies the input to internal storage
/// - Snapshots are copy-on-write where possible
///
/// # Dyn Compatibility
///
/// This trait is dyn-compatible for use with `Box<dyn StateStore>`. For generic
/// convenience methods like `get_typed` and `put_typed`, use the [`StateStoreExt`]
/// extension trait.
pub trait StateStore: Send {
    /// Get a value by key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Performance
    ///
    /// Target: < 500ns for in-memory stores.
    fn get(&self, key: &[u8]) -> Option<Bytes>;

    /// Store a key-value pair.
    ///
    /// If the key already exists, the value is overwritten.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the operation fails (e.g., disk full for
    /// memory-mapped stores).
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    /// Delete a key.
    ///
    /// No error is returned if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the operation fails.
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;

    /// Scan all keys with a given prefix.
    ///
    /// Returns an iterator over matching (key, value) pairs in
    /// lexicographic order.
    ///
    /// # Performance
    ///
    /// O(log n + k) where n is the total number of keys and k is the
    /// number of matching entries.
    fn prefix_scan<'a>(&'a self, prefix: &'a [u8])
        -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Range scan between two keys (exclusive end).
    ///
    /// Returns an iterator over keys where `start <= key < end`
    /// in lexicographic order.
    ///
    /// # Performance
    ///
    /// O(log n + k) where n is the total number of keys and k is the
    /// number of matching entries.
    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;

    /// Check if a key exists.
    ///
    /// More efficient than `get()` when you don't need the value.
    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Get approximate size in bytes.
    ///
    /// This includes both keys and values. The exact accounting may vary
    /// by implementation.
    fn size_bytes(&self) -> usize;

    /// Get the number of entries in the store.
    fn len(&self) -> usize;

    /// Check if the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a snapshot for checkpointing.
    ///
    /// The snapshot captures the current state and can be used to restore
    /// the store to this point in time. Snapshots are serializable for
    /// persistence.
    ///
    /// # Implementation Notes
    ///
    /// For in-memory stores, this clones the data. For memory-mapped stores,
    /// this may use copy-on-write semantics.
    fn snapshot(&self) -> StateSnapshot;

    /// Restore from a snapshot.
    ///
    /// This replaces the current state with the snapshot's state.
    /// Any changes since the snapshot was taken are lost.
    fn restore(&mut self, snapshot: StateSnapshot);

    /// Clear all entries.
    fn clear(&mut self);

    /// Flush any pending writes to durable storage.
    ///
    /// For in-memory stores, this is a no-op. For memory-mapped or
    /// disk-backed stores, this ensures data is persisted.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the flush operation fails.
    fn flush(&mut self) -> Result<(), StateError> {
        Ok(()) // Default no-op for in-memory stores
    }

    /// Get a zero-copy reference to a value by key.
    ///
    /// Returns a direct `&[u8]` slice into the store's internal buffer,
    /// avoiding the `Bytes` ref-count overhead. Only backends that own
    /// their storage contiguously (e.g., `AHashMapStore`) can implement
    /// this; others return `None` and callers fall back to [`get`](Self::get).
    ///
    /// # Lifetime
    ///
    /// The returned slice borrows `self`, so no mutations are allowed
    /// while the reference is live.
    fn get_ref(&self, _key: &[u8]) -> Option<&[u8]> {
        None
    }

    /// Return an incremental snapshot of mutations since the last checkpoint.
    ///
    /// Backends that track per-mutation changelogs return
    /// `Some(IncrementalSnapshot)` containing only the puts and deletes
    /// since `base_epoch`. Backends without changelog support return `None`,
    /// and the checkpointer falls back to a full [`snapshot`](Self::snapshot).
    fn incremental_snapshot(&self) -> Option<IncrementalSnapshot> {
        None
    }

    /// Get a value or insert a default.
    ///
    /// If the key doesn't exist, the default is inserted and returned.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if inserting the default value fails.
    fn get_or_insert(&mut self, key: &[u8], default: &[u8]) -> Result<Bytes, StateError> {
        if let Some(value) = self.get(key) {
            Ok(value)
        } else {
            self.put(key, default)?;
            Ok(Bytes::copy_from_slice(default))
        }
    }
}

/// Extension trait for [`StateStore`] providing typed access methods.
///
/// These methods use generics and thus cannot be part of the dyn-compatible
/// `StateStore` trait. Import this trait to use typed access on any state store.
///
/// Uses rkyv for zero-copy serialization. Types must derive `Archive`,
/// `rkyv::Serialize`, and `rkyv::Deserialize`.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::state::{StateStore, StateStoreExt, InMemoryStore};
/// use rkyv::{Archive, Deserialize, Serialize};
///
/// #[derive(Archive, Serialize, Deserialize)]
/// #[rkyv(check_bytes)]
/// struct Counter { value: u64 }
///
/// let mut store = InMemoryStore::new();
/// store.put_typed(b"count", &Counter { value: 42 }).unwrap();
/// let count: Counter = store.get_typed(b"count").unwrap().unwrap();
/// assert_eq!(count.value, 42);
/// ```
pub trait StateStoreExt: StateStore {
    /// Get a value and deserialize it using rkyv.
    ///
    /// Uses zero-copy access where possible, falling back to full
    /// deserialization to return an owned value.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Serialization` if deserialization fails.
    fn get_typed<T>(&self, key: &[u8]) -> Result<Option<T>, StateError>
    where
        T: Archive,
        T::Archived: for<'a> CheckBytes<HighValidator<'a, RkyvError>>
            + RkyvDeserialize<T, HighDeserializer<RkyvError>>,
    {
        match self.get(key) {
            Some(bytes) => {
                let archived = rkyv::access::<T::Archived, RkyvError>(&bytes)
                    .map_err(|e| StateError::Serialization(e.to_string()))?;
                let value = rkyv::deserialize::<T, RkyvError>(archived)
                    .map_err(|e| StateError::Serialization(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Serialize and store a value using rkyv.
    ///
    /// Uses aligned buffers for optimal performance on the hot path.
    ///
    /// # Errors
    ///
    /// Returns `StateError::Serialization` if serialization fails.
    fn put_typed<T>(&mut self, key: &[u8], value: &T) -> Result<(), StateError>
    where
        T: for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    {
        let bytes = rkyv::to_bytes::<RkyvError>(value)
            .map_err(|e| StateError::Serialization(e.to_string()))?;
        self.put(key, &bytes)
    }

    /// Update a value in place using a closure.
    ///
    /// The update function receives the current value (or None) and returns
    /// the new value. If `None` is returned, the key is deleted.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the put or delete operation fails.
    fn update<F>(&mut self, key: &[u8], f: F) -> Result<(), StateError>
    where
        F: FnOnce(Option<Bytes>) -> Option<Vec<u8>>,
    {
        let current = self.get(key);
        match f(current) {
            Some(new_value) => self.put(key, &new_value),
            None => self.delete(key),
        }
    }
}

// Blanket implementation for all StateStore types
impl<T: StateStore + ?Sized> StateStoreExt for T {}

/// A snapshot of state store contents for checkpointing.
///
/// Snapshots can be serialized for persistence and restored later.
/// They capture the complete state at a point in time.
///
/// Uses rkyv for zero-copy deserialization on the hot path.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct StateSnapshot {
    /// Serialized state data
    data: Vec<(Vec<u8>, Vec<u8>)>,
    /// Timestamp when snapshot was created (nanoseconds since epoch)
    timestamp_ns: u64,
    /// Version for forward compatibility
    version: u32,
}

impl StateSnapshot {
    /// Create a new snapshot from key-value pairs.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            data,
            // Truncation is acceptable here - we won't hit u64 overflow until ~584 years from epoch
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
            version: 1,
        }
    }

    /// Get the snapshot data.
    #[must_use]
    pub fn data(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.data
    }

    /// Get the snapshot timestamp.
    #[must_use]
    pub fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }

    /// Get the number of entries in the snapshot.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the snapshot is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the approximate size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.data.iter().map(|(k, v)| k.len() + v.len()).sum()
    }

    /// Serialize the snapshot to bytes using rkyv.
    ///
    /// Returns an aligned byte vector for optimal zero-copy access.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_bytes(&self) -> Result<AlignedVec, StateError> {
        rkyv::to_bytes::<RkyvError>(self).map_err(|e| StateError::Serialization(e.to_string()))
    }

    /// Deserialize a snapshot from bytes using rkyv.
    ///
    /// Uses zero-copy access internally for performance.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StateError> {
        let archived = rkyv::access::<<Self as Archive>::Archived, RkyvError>(bytes)
            .map_err(|e| StateError::Serialization(e.to_string()))?;
        rkyv::deserialize::<Self, RkyvError>(archived)
            .map_err(|e| StateError::Serialization(e.to_string()))
    }
}

/// In-memory state store using `BTreeMap` for sorted key access.
///
/// This state store is suitable for state that fits in memory. It uses
/// `BTreeMap` which provides O(log n + k) prefix and range scans, making
/// it efficient for join state and windowed aggregation lookups.
///
/// # Performance Characteristics
///
/// - **Get**: O(log n), < 500ns typical
/// - **Put**: O(log n), may allocate
/// - **Delete**: O(log n)
/// - **Prefix scan**: O(log n + k) where k is matching entries
/// - **Range scan**: O(log n + k) where k is matching entries
///
/// # Memory Usage
///
/// Keys and values are stored as owned `Vec<u8>` and `Bytes` respectively.
/// Use `size_bytes()` to monitor memory usage.
pub struct InMemoryStore {
    /// The underlying sorted map
    data: BTreeMap<Vec<u8>, Bytes>,
    /// Track total size for monitoring
    size_bytes: usize,
}

impl InMemoryStore {
    /// Creates a new empty in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Creates a new in-memory store.
    ///
    /// The capacity hint is accepted for API compatibility but has no
    /// effect — `BTreeMap` does not support pre-allocation.
    #[must_use]
    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    /// Returns the number of entries in the store.
    ///
    /// `BTreeMap` does not expose a capacity concept, so this returns
    /// the current entry count.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// No-op for API compatibility.
    ///
    /// `BTreeMap` manages its own memory and does not support
    /// explicit shrinking.
    pub fn shrink_to_fit(&mut self) {
        // BTreeMap does not support shrink_to_fit
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for InMemoryStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.data.get(key).cloned()
    }

    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        let value_bytes = Bytes::copy_from_slice(value);

        // Entry API: single tree traversal for both insert and update
        match self.data.entry(key.to_vec()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                self.size_bytes -= entry.get().len();
                self.size_bytes += value.len();
                *entry.get_mut() = value_bytes;
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.size_bytes += key.len() + value.len();
                entry.insert(value_bytes);
            }
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if let Some(old_value) = self.data.remove(key) {
            self.size_bytes -= key.len() + old_value.len();
        }
        Ok(())
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        if prefix.is_empty() {
            // Empty prefix matches everything
            return Box::new(
                self.data
                    .iter()
                    .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
            );
        }
        if let Some(end) = prefix_successor(prefix) {
            Box::new(
                self.data
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Excluded(end.as_slice())))
                    .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
            )
        } else {
            // All-0xFF prefix: scan from prefix to end
            Box::new(
                self.data
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
                    .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
            )
        }
    }

    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        Box::new(
            self.data
                .range::<[u8], _>((Bound::Included(range.start), Bound::Excluded(range.end)))
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
        )
    }

    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key.as_ref())
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn snapshot(&self) -> StateSnapshot {
        let data: Vec<(Vec<u8>, Vec<u8>)> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.to_vec()))
            .collect();
        StateSnapshot::new(data)
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.data.clear();
        self.size_bytes = 0;

        for (key, value) in snapshot.data {
            self.size_bytes += key.len() + value.len();
            self.data.insert(key, Bytes::from(value));
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.size_bytes = 0;
    }
}

/// Errors that can occur in state operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Corruption error
    #[error("Corruption error: {0}")]
    Corruption(String),

    /// Operation not supported by this store type
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Key not found (for operations that require existing key)
    #[error("Key not found")]
    KeyNotFound,

    /// Store capacity exceeded
    #[error("Store capacity exceeded: {0}")]
    CapacityExceeded(String),
}

mod mmap;

/// AHashMap-backed state store with O(1) lookups and zero-copy reads.
pub mod ahash_store;

/// Changelog-aware state store wrapper.
pub mod changelog_aware;

// Re-export main types
pub use self::StateError as Error;
pub use ahash_store::AHashMapStore;
pub use changelog_aware::{ChangelogAwareStore, ChangelogSink};
pub use mmap::MmapStateStore;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_store_basic() {
        let mut store = InMemoryStore::new();

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

        // Test delete non-existent key (should not error)
        store.delete(b"nonexistent").unwrap();
    }

    #[test]
    fn test_contains() {
        let mut store = InMemoryStore::new();
        assert!(!store.contains(b"key1"));

        store.put(b"key1", b"value1").unwrap();
        assert!(store.contains(b"key1"));

        store.delete(b"key1").unwrap();
        assert!(!store.contains(b"key1"));
    }

    #[test]
    fn test_prefix_scan() {
        let mut store = InMemoryStore::new();
        store.put(b"prefix:1", b"value1").unwrap();
        store.put(b"prefix:2", b"value2").unwrap();
        store.put(b"prefix:10", b"value10").unwrap();
        store.put(b"other:1", b"value3").unwrap();

        let results: Vec<_> = store.prefix_scan(b"prefix:").collect();
        assert_eq!(results.len(), 3);

        // All results should have the prefix
        for (key, _) in &results {
            assert!(key.starts_with(b"prefix:"));
        }

        // Empty prefix returns all
        let all: Vec<_> = store.prefix_scan(b"").collect();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_range_scan() {
        let mut store = InMemoryStore::new();
        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        store.put(b"c", b"3").unwrap();
        store.put(b"d", b"4").unwrap();

        let results: Vec<_> = store.range_scan(b"b".as_slice()..b"d".as_slice()).collect();
        assert_eq!(results.len(), 2);

        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
        assert!(keys.contains(&b"b".as_slice()));
        assert!(keys.contains(&b"c".as_slice()));
    }

    #[test]
    fn test_snapshot_and_restore() {
        let mut store = InMemoryStore::new();
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
    fn test_snapshot_serialization() {
        let mut store = InMemoryStore::new();
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        let snapshot = store.snapshot();

        // Serialize and deserialize
        let bytes = snapshot.to_bytes().unwrap();
        let restored = StateSnapshot::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), snapshot.len());
        assert_eq!(restored.data(), snapshot.data());
    }

    #[test]
    fn test_typed_access() {
        let mut store = InMemoryStore::new();

        // Test with integer
        store.put_typed(b"count", &42u64).unwrap();
        let count: u64 = store.get_typed(b"count").unwrap().unwrap();
        assert_eq!(count, 42);

        // Test with string
        store.put_typed(b"name", &String::from("alice")).unwrap();
        let name: String = store.get_typed(b"name").unwrap().unwrap();
        assert_eq!(name, "alice");

        // Test with vector (complex type)
        let nums = vec![1i64, 2, 3, 4, 5];
        store.put_typed(b"nums", &nums).unwrap();
        let restored: Vec<i64> = store.get_typed(b"nums").unwrap().unwrap();
        assert_eq!(restored, nums);

        // Test non-existent key
        let missing: Option<u64> = store.get_typed(b"missing").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_get_or_insert() {
        let mut store = InMemoryStore::new();

        // First call inserts default
        let value = store.get_or_insert(b"key1", b"default").unwrap();
        assert_eq!(value, Bytes::from("default"));
        assert_eq!(store.len(), 1);

        // Second call returns existing
        store.put(b"key1", b"modified").unwrap();
        let value = store.get_or_insert(b"key1", b"default").unwrap();
        assert_eq!(value, Bytes::from("modified"));
    }

    #[test]
    fn test_update() {
        let mut store = InMemoryStore::new();
        store.put(b"counter", b"\x00\x00\x00\x00").unwrap();

        // Update existing
        store
            .update(b"counter", |current| {
                let val = current.map_or(0u32, |b| {
                    u32::from_le_bytes(b.as_ref().try_into().unwrap_or([0; 4]))
                });
                Some((val + 1).to_le_bytes().to_vec())
            })
            .unwrap();

        let bytes = store.get(b"counter").unwrap();
        let val = u32::from_le_bytes(bytes.as_ref().try_into().unwrap());
        assert_eq!(val, 1);

        // Update to delete
        store.update(b"counter", |_| None).unwrap();
        assert!(store.get(b"counter").is_none());
    }

    #[test]
    fn test_size_tracking() {
        let mut store = InMemoryStore::new();
        assert_eq!(store.size_bytes(), 0);

        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6); // "key1" + "value1"

        store.put(b"key2", b"value2").unwrap();
        assert_eq!(store.size_bytes(), (4 + 6) * 2);

        // Overwrite with smaller value
        store.put(b"key1", b"v1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 2 + 4 + 6); // "key1" + "v1" + "key2" + "value2"

        store.delete(b"key1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6);

        store.clear();
        assert_eq!(store.size_bytes(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let store = InMemoryStore::with_capacity(1000);
        // BTreeMap does not pre-allocate; capacity() returns len() which is 0
        assert_eq!(store.capacity(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut store = InMemoryStore::new();
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
    fn test_prefix_successor() {
        // Normal case
        assert_eq!(prefix_successor(b"abc"), Some(b"abd".to_vec()));

        // Empty prefix
        assert_eq!(prefix_successor(b""), None);

        // All 0xFF bytes — no successor
        assert_eq!(prefix_successor(&[0xFF, 0xFF, 0xFF]), None);

        // Trailing 0xFF bytes are truncated and previous byte incremented
        assert_eq!(prefix_successor(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(
            prefix_successor(&[0x01, 0x02, 0xFF]),
            Some(vec![0x01, 0x03])
        );

        // Single byte
        assert_eq!(prefix_successor(&[0x00]), Some(vec![0x01]));
        assert_eq!(prefix_successor(&[0xFE]), Some(vec![0xFF]));
        assert_eq!(prefix_successor(&[0xFF]), None);
    }

    #[test]
    fn test_prefix_scan_binary_keys() {
        let mut store = InMemoryStore::new();

        // Simulate join state keys: partition_prefix + key_hash
        let prefix_a = [0x00, 0x01]; // partition 0, stream 1
        let prefix_b = [0x00, 0x02]; // partition 0, stream 2

        store.put(&[0x00, 0x01, 0xAA], b"val1").unwrap();
        store.put(&[0x00, 0x01, 0xBB], b"val2").unwrap();
        store.put(&[0x00, 0x02, 0xCC], b"val3").unwrap();
        store.put(&[0x00, 0x02, 0xDD], b"val4").unwrap();
        store.put(&[0x01, 0x01, 0xEE], b"val5").unwrap();

        // Prefix scan for partition_a
        let results_a: Vec<_> = store.prefix_scan(&prefix_a).collect();
        assert_eq!(results_a.len(), 2);
        for (key, _) in &results_a {
            assert!(key.starts_with(&prefix_a));
        }

        // Prefix scan for partition_b
        let results_b: Vec<_> = store.prefix_scan(&prefix_b).collect();
        assert_eq!(results_b.len(), 2);
        for (key, _) in &results_b {
            assert!(key.starts_with(&prefix_b));
        }

        // Prefix scan with all-0xFF prefix
        let results_ff: Vec<_> = store.prefix_scan(&[0xFF, 0xFF]).collect();
        assert_eq!(results_ff.len(), 0);
    }

    #[test]
    fn test_prefix_scan_returns_sorted() {
        let mut store = InMemoryStore::new();
        store.put(b"prefix:c", b"3").unwrap();
        store.put(b"prefix:a", b"1").unwrap();
        store.put(b"prefix:b", b"2").unwrap();

        let results: Vec<_> = store.prefix_scan(b"prefix:").collect();
        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref().to_vec()).collect();
        assert_eq!(
            keys,
            vec![
                b"prefix:a".to_vec(),
                b"prefix:b".to_vec(),
                b"prefix:c".to_vec()
            ]
        );
    }
}
