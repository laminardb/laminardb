//! Changelog-aware state store wrapper (F-CKP-005).
//!
//! Wraps any [`StateStore`] to record mutations to a [`ChangelogSink`],
//! enabling Ring 1 WAL writes to track Ring 0 state changes.
//!
//! ## Ring 0 Cost
//!
//! Each `put()` or `delete()` adds one call to `ChangelogSink::record_put()`
//! or `record_delete()`. With a pre-allocated SPSC buffer, this is a single
//! atomic CAS (~2-5ns overhead per mutation).

use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

use super::{StateError, StateSnapshot, StateStore};

/// Trait for recording state mutations from Ring 0.
///
/// Implementations should be zero-allocation on the hot path.
/// The `StateChangelogBuffer` in laminar-storage implements this via a
/// pre-allocated SPSC ring buffer.
pub trait ChangelogSink: Send + Sync {
    /// Records a put (insert/update) mutation.
    ///
    /// Returns `true` if recorded, `false` if the sink is full (backpressure).
    /// A `false` return does NOT mean the mutation failed — the state store
    /// is the source of truth. The changelog is best-effort for WAL.
    fn record_put(&self, key: &[u8], value_len: u32) -> bool;

    /// Records a delete mutation.
    ///
    /// Returns `true` if recorded, `false` if the sink is full.
    fn record_delete(&self, key: &[u8]) -> bool;
}

/// A state store wrapper that records all mutations to a changelog sink.
///
/// This is the bridge between Ring 0 state mutations and Ring 1 WAL writes.
/// Wraps any `StateStore` and intercepts `put()` and `delete()` to also
/// record the mutation in the changelog.
///
/// ## Performance
///
/// - `get()`, `prefix_scan()`, `range_scan()`: Delegated directly, zero overhead.
/// - `put()`, `delete()`: Original operation + one `ChangelogSink` call (~2-5ns).
/// - `contains()`, `len()`, `is_empty()`: Delegated directly.
pub struct ChangelogAwareStore<S: StateStore> {
    inner: S,
    changelog: Arc<dyn ChangelogSink>,
}

impl<S: StateStore> ChangelogAwareStore<S> {
    /// Wraps a state store with changelog recording.
    pub fn new(inner: S, changelog: Arc<dyn ChangelogSink>) -> Self {
        Self { inner, changelog }
    }

    /// Returns a reference to the inner state store.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns a mutable reference to the inner state store.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Unwraps the store, returning the inner state store.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: StateStore> StateStore for ChangelogAwareStore<S> {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.get(key)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        self.inner.put(key, value)?;
        // Record the mutation; ignore backpressure (state store is source of truth).
        #[allow(clippy::cast_possible_truncation)]
        let _ = self.changelog.record_put(key, value.len() as u32);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        self.inner.delete(key)?;
        let _ = self.changelog.record_delete(key);
        Ok(())
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        self.inner.prefix_scan(prefix)
    }

    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        self.inner.range_scan(range)
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }

    fn size_bytes(&self) -> usize {
        self.inner.size_bytes()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn snapshot(&self) -> StateSnapshot {
        self.inner.snapshot()
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.inner.restore(snapshot);
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn flush(&mut self) -> Result<(), StateError> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test changelog sink that counts operations.
    struct CountingSink {
        puts: AtomicUsize,
        deletes: AtomicUsize,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                puts: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
            }
        }
    }

    impl ChangelogSink for CountingSink {
        fn record_put(&self, _key: &[u8], _value_len: u32) -> bool {
            self.puts.fetch_add(1, Ordering::Relaxed);
            true
        }

        fn record_delete(&self, _key: &[u8]) -> bool {
            self.deletes.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    #[test]
    fn test_changelog_aware_put_records() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        assert_eq!(sink.puts.load(Ordering::Relaxed), 2);
        assert_eq!(sink.deletes.load(Ordering::Relaxed), 0);

        // Verify the underlying store has the data
        assert_eq!(store.get(b"key1").unwrap().as_ref(), b"value1");
        assert_eq!(store.get(b"key2").unwrap().as_ref(), b"value2");
    }

    #[test]
    fn test_changelog_aware_delete_records() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"key1", b"value1").unwrap();
        store.delete(b"key1").unwrap();

        assert_eq!(sink.puts.load(Ordering::Relaxed), 1);
        assert_eq!(sink.deletes.load(Ordering::Relaxed), 1);

        assert!(store.get(b"key1").is_none());
    }

    #[test]
    fn test_changelog_aware_get_no_overhead() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"key1", b"value1").unwrap();
        let _ = store.get(b"key1");
        let _ = store.get(b"nonexistent");

        // get should not record anything
        assert_eq!(sink.puts.load(Ordering::Relaxed), 1); // only the put
    }

    #[test]
    fn test_changelog_aware_prefix_scan() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"user:1", b"alice").unwrap();
        store.put(b"user:2", b"bob").unwrap();
        store.put(b"order:1", b"item").unwrap();

        let results: Vec<_> = store.prefix_scan(b"user:").collect();
        assert_eq!(results.len(), 2);

        // Scan should not record
        assert_eq!(sink.puts.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_changelog_aware_len_and_empty() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        store.put(b"key", b"value").unwrap();
        assert!(!store.is_empty());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_changelog_aware_inner_access() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"key", b"value").unwrap();
        assert_eq!(store.inner().len(), 1);
        assert_eq!(store.inner_mut().len(), 1);
    }

    #[test]
    fn test_changelog_aware_into_inner() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"key", b"value").unwrap();
        let inner = store.into_inner();
        assert_eq!(inner.len(), 1);
    }

    /// Test that backpressure from sink doesn't affect store operations.
    #[test]
    fn test_changelog_aware_backpressure_no_effect() {
        struct FullSink;
        impl ChangelogSink for FullSink {
            fn record_put(&self, _key: &[u8], _value_len: u32) -> bool {
                false // Always full
            }
            fn record_delete(&self, _key: &[u8]) -> bool {
                false // Always full
            }
        }

        let sink = Arc::new(FullSink);
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        // Mutations should still succeed even though changelog is full
        store.put(b"key", b"value").unwrap();
        assert_eq!(store.get(b"key").unwrap().as_ref(), b"value");

        store.delete(b"key").unwrap();
        assert!(store.get(b"key").is_none());
    }

    #[test]
    fn test_changelog_aware_snapshot_restore() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        let snapshot = store.snapshot();

        store.put(b"key3", b"value3").unwrap();
        store.delete(b"key1").unwrap();

        // Restore resets state
        store.restore(snapshot);
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(b"key1").unwrap().as_ref(), b"value1");
        assert!(store.get(b"key3").is_none());

        // Restore doesn't record to changelog (it's a control operation, not data)
        // puts: 3 (key1, key2, key3), deletes: 1 (key1)
        assert_eq!(sink.puts.load(Ordering::Relaxed), 3);
        assert_eq!(sink.deletes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_changelog_aware_clear() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.size_bytes(), 0);
    }

    #[test]
    fn test_changelog_aware_size_bytes() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        assert_eq!(store.size_bytes(), 0);

        store.put(b"key1", b"value1").unwrap();
        assert!(store.size_bytes() > 0);
    }

    #[test]
    fn test_changelog_aware_range_scan() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        store.put(b"c", b"3").unwrap();
        store.put(b"d", b"4").unwrap();

        let results: Vec<_> = store.range_scan(b"b".as_slice()..b"d".as_slice()).collect();
        assert_eq!(results.len(), 2);
    }
}
