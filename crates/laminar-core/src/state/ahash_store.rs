//! AHashMap-backed state store with dual-structure design.
//!
//! [`AHashMapStore`] uses `AHashMap<Vec<u8>, Vec<u8>>` for O(1) point lookups
//! and a `BTreeMap<Vec<u8>, ()>` index for efficient prefix/range scans.
//! This is the first backend that supports zero-copy `get_ref`.
//!
//! ## Performance Characteristics
//!
//! - **Get**: O(1) average via AHashMap, < 200ns typical
//! - **Get (zero-copy)**: O(1) via `get_ref()`, < 150ns typical
//! - **Put**: O(1) amortized (hash) + O(log n) (BTreeMap index)
//! - **Delete**: O(1) (hash) + O(log n) (BTreeMap index)
//! - **Prefix scan**: O(log n + k) via BTreeMap index
//! - **Range scan**: O(log n + k) via BTreeMap index

use ahash::AHashMap;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::ops::Range;

use super::{prefix_successor, StateError, StateSnapshot, StateStore};

/// High-performance state store using `AHashMap` for point lookups and
/// `BTreeMap` for ordered scans.
///
/// This dual-structure design provides:
/// - O(1) point lookups (vs O(log n) for `InMemoryStore`)
/// - Zero-copy reads via [`get_ref`](StateStore::get_ref)
/// - Same O(log n + k) scan performance as `InMemoryStore`
///
/// Trade-off: ~2x memory for keys (stored in both maps) and slightly
/// slower writes due to dual-map maintenance.
pub struct AHashMapStore {
    /// Primary data store for O(1) point lookups.
    data: AHashMap<Vec<u8>, Vec<u8>>,
    /// Sorted index for prefix/range scans (keys only).
    index: BTreeSet<Vec<u8>>,
    /// Track total size in bytes (keys + values).
    size_bytes: usize,
}

impl AHashMapStore {
    /// Creates a new empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: AHashMap::new(),
            index: BTreeSet::new(),
            size_bytes: 0,
        }
    }

    /// Creates a new store with pre-allocated capacity for the hash map.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: AHashMap::with_capacity(capacity),
            index: BTreeSet::new(),
            size_bytes: 0,
        }
    }
}

impl Default for AHashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for AHashMapStore {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.data.get(key).map(|v| Bytes::copy_from_slice(v))
    }

    #[inline]
    fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(Vec::as_slice)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        let key_vec = key.to_vec();
        if let Some(old_value) = self.data.get_mut(key) {
            // Update existing: only value size changes
            self.size_bytes -= old_value.len();
            self.size_bytes += value.len();
            old_value.clear();
            old_value.extend_from_slice(value);
        } else {
            // New key: insert into both structures
            self.size_bytes += key.len() + value.len();
            self.index.insert(key_vec.clone());
            self.data.insert(key_vec, value.to_vec());
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if let Some(old_value) = self.data.remove(key) {
            self.size_bytes -= key.len() + old_value.len();
            self.index.remove(key);
        }
        Ok(())
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        if prefix.is_empty() {
            return Box::new(self.index.iter().map(move |k| {
                let v = &self.data[k.as_slice()];
                (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
            }));
        }
        if let Some(end) = prefix_successor(prefix) {
            Box::new(
                self.index
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Excluded(end.as_slice())))
                    .map(move |k| {
                        let v = &self.data[k.as_slice()];
                        (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
                    }),
            )
        } else {
            Box::new(
                self.index
                    .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
                    .map(move |k| {
                        let v = &self.data[k.as_slice()];
                        (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
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
                .map(move |k| {
                    let v = &self.data[k.as_slice()];
                    (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
                }),
        )
    }

    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn snapshot(&self) -> StateSnapshot {
        let data: Vec<(Vec<u8>, Vec<u8>)> = self
            .index
            .iter()
            .map(|k| {
                let v = self.data[k.as_slice()].clone();
                (k.clone(), v)
            })
            .collect();
        StateSnapshot::new(data)
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.data.clear();
        self.index.clear();
        self.size_bytes = 0;

        for (key, value) in snapshot.data() {
            self.size_bytes += key.len() + value.len();
            self.index.insert(key.clone());
            self.data.insert(key.clone(), value.clone());
        }
    }

    fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
        self.size_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut store = AHashMapStore::new();

        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value1"));
        assert_eq!(store.len(), 1);

        // Overwrite
        store.put(b"key1", b"value2").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value2"));
        assert_eq!(store.len(), 1);

        // Delete
        store.delete(b"key1").unwrap();
        assert!(store.get(b"key1").is_none());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_get_ref_zero_copy() {
        let mut store = AHashMapStore::new();
        store.put(b"key1", b"value1").unwrap();

        // get_ref returns a direct slice
        let slice = store.get_ref(b"key1").unwrap();
        assert_eq!(slice, b"value1");

        // Missing key returns None
        assert!(store.get_ref(b"missing").is_none());
    }

    #[test]
    fn test_contains() {
        let mut store = AHashMapStore::new();
        assert!(!store.contains(b"key1"));

        store.put(b"key1", b"value1").unwrap();
        assert!(store.contains(b"key1"));

        store.delete(b"key1").unwrap();
        assert!(!store.contains(b"key1"));
    }

    #[test]
    fn test_prefix_scan() {
        let mut store = AHashMapStore::new();
        store.put(b"prefix:1", b"value1").unwrap();
        store.put(b"prefix:2", b"value2").unwrap();
        store.put(b"prefix:10", b"value10").unwrap();
        store.put(b"other:1", b"value3").unwrap();

        let results: Vec<_> = store.prefix_scan(b"prefix:").collect();
        assert_eq!(results.len(), 3);
        for (key, _) in &results {
            assert!(key.starts_with(b"prefix:"));
        }

        // Empty prefix returns all
        let all: Vec<_> = store.prefix_scan(b"").collect();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_prefix_scan_sorted() {
        let mut store = AHashMapStore::new();
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

    #[test]
    fn test_range_scan() {
        let mut store = AHashMapStore::new();
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
        let mut store = AHashMapStore::new();
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 2);

        store.put(b"key1", b"modified").unwrap();
        store.put(b"key3", b"value3").unwrap();
        store.delete(b"key2").unwrap();

        store.restore(snapshot);
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(b"key1").unwrap(), Bytes::from("value1"));
        assert_eq!(store.get(b"key2").unwrap(), Bytes::from("value2"));
        assert!(store.get(b"key3").is_none());
    }

    #[test]
    fn test_size_tracking() {
        let mut store = AHashMapStore::new();
        assert_eq!(store.size_bytes(), 0);

        store.put(b"key1", b"value1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6);

        store.put(b"key2", b"value2").unwrap();
        assert_eq!(store.size_bytes(), (4 + 6) * 2);

        // Overwrite with smaller value
        store.put(b"key1", b"v1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 2 + 4 + 6);

        store.delete(b"key1").unwrap();
        assert_eq!(store.size_bytes(), 4 + 6);

        store.clear();
        assert_eq!(store.size_bytes(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let store = AHashMapStore::with_capacity(1000);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut store = AHashMapStore::new();
        store.delete(b"nonexistent").unwrap();
        assert_eq!(store.len(), 0);
        assert_eq!(store.size_bytes(), 0);
    }
}
