//! Changelog-aware state store wrapper.
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

use super::{ChangeEntry, IncrementalSnapshot, StateError, StateSnapshot, StateStore};

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

    /// Drain all accumulated entries since the last drain/reset.
    ///
    /// Returns the ordered list of mutations. After this call, the sink
    /// is empty and ready for the next checkpoint interval.
    fn drain(&self) -> Vec<ChangeEntry>;

    /// Reset the sink, discarding all accumulated entries.
    ///
    /// Called during `restore()` and `clear()` to ensure the changelog
    /// does not contain stale deltas from before the state reset.
    fn reset(&self);
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
        // Reset changelog so incremental deltas start from the restored baseline.
        self.changelog.reset();
    }

    fn clear(&mut self) {
        self.inner.clear();
        // Reset changelog so incremental deltas start from the cleared state.
        self.changelog.reset();
    }

    fn incremental_snapshot(&self) -> Option<IncrementalSnapshot> {
        let changes = self.changelog.drain();
        if changes.is_empty() {
            return None;
        }
        Some(IncrementalSnapshot {
            changes,
            base_epoch: 0, // Caller sets the correct base epoch
            epoch: 0,      // Caller sets the correct epoch
        })
    }

    fn flush(&mut self) -> Result<(), StateError> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test changelog sink that counts operations and accumulates entries.
    struct CountingSink {
        puts: AtomicUsize,
        deletes: AtomicUsize,
        entries: Mutex<Vec<ChangeEntry>>,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                puts: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                entries: Mutex::new(Vec::new()),
            }
        }
    }

    impl ChangelogSink for CountingSink {
        fn record_put(&self, key: &[u8], _value_len: u32) -> bool {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.entries
                .lock()
                .push(ChangeEntry::Put(key.to_vec(), Vec::new()));
            true
        }

        fn record_delete(&self, key: &[u8]) -> bool {
            self.deletes.fetch_add(1, Ordering::Relaxed);
            self.entries.lock().push(ChangeEntry::Delete(key.to_vec()));
            true
        }

        fn drain(&self) -> Vec<ChangeEntry> {
            std::mem::take(&mut *self.entries.lock())
        }

        fn reset(&self) {
            self.entries.lock().clear();
            self.puts.store(0, Ordering::Relaxed);
            self.deletes.store(0, Ordering::Relaxed);
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
            fn drain(&self) -> Vec<ChangeEntry> {
                Vec::new()
            }
            fn reset(&self) {}
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

        // Restore resets the changelog so incremental deltas start fresh.
        // Counters are reset to 0 by restore().
        assert_eq!(sink.puts.load(Ordering::Relaxed), 0);
        assert_eq!(sink.deletes.load(Ordering::Relaxed), 0);
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

    #[test]
    fn test_incremental_snapshot_returns_changes() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"k1", b"v1").unwrap();
        store.put(b"k2", b"v2").unwrap();
        store.delete(b"k1").unwrap();

        let snap = store.incremental_snapshot().unwrap();
        assert_eq!(snap.changes.len(), 3);
        assert!(matches!(&snap.changes[0], ChangeEntry::Put(k, _) if k == b"k1"));
        assert!(matches!(&snap.changes[1], ChangeEntry::Put(k, _) if k == b"k2"));
        assert!(matches!(&snap.changes[2], ChangeEntry::Delete(k) if k == b"k1"));
    }

    #[test]
    fn test_incremental_snapshot_empty_returns_none() {
        let sink = Arc::new(CountingSink::new());
        let store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        assert!(store.incremental_snapshot().is_none());
    }

    #[test]
    fn test_incremental_snapshot_drains_on_call() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink);

        store.put(b"k1", b"v1").unwrap();
        let snap = store.incremental_snapshot().unwrap();
        assert_eq!(snap.changes.len(), 1);

        // Second call should return None (drained)
        assert!(store.incremental_snapshot().is_none());

        // New mutations produce a new snapshot
        store.put(b"k2", b"v2").unwrap();
        let snap2 = store.incremental_snapshot().unwrap();
        assert_eq!(snap2.changes.len(), 1);
    }

    #[test]
    fn test_restore_resets_changelog() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"k1", b"v1").unwrap();
        store.put(b"k2", b"v2").unwrap();
        let snapshot = store.snapshot();

        store.put(b"k3", b"v3").unwrap();
        // 3 puts recorded before restore
        assert_eq!(sink.puts.load(Ordering::Relaxed), 3);

        store.restore(snapshot);
        // Counters reset by restore
        assert_eq!(sink.puts.load(Ordering::Relaxed), 0);
        // Changelog drained
        assert!(store.incremental_snapshot().is_none());

        // Post-restore mutations are tracked fresh
        store.put(b"k4", b"v4").unwrap();
        let snap = store.incremental_snapshot().unwrap();
        assert_eq!(snap.changes.len(), 1);
    }

    #[test]
    fn test_clear_resets_changelog() {
        let sink = Arc::new(CountingSink::new());
        let mut store = ChangelogAwareStore::new(InMemoryStore::new(), sink.clone());

        store.put(b"k1", b"v1").unwrap();
        store.put(b"k2", b"v2").unwrap();
        assert_eq!(sink.puts.load(Ordering::Relaxed), 2);

        store.clear();
        assert_eq!(sink.puts.load(Ordering::Relaxed), 0);
        assert!(store.incremental_snapshot().is_none());
    }
}
