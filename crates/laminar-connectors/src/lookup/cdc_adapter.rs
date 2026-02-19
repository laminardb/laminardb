//! CDC-to-cache adapter for lookup table refresh.
//!
//! [`CdcCacheAdapter`] translates Change Data Capture events into
//! cache mutations on a [`LookupTable`]. It bridges the gap between
//! CDC connectors (Postgres, MySQL, Kafka) and the in-memory lookup
//! cache used by Ring 0 lookup join operators.
//!
//! ## Event flow
//!
//! ```text
//! CDC Source ──▶ CdcCacheAdapter ──▶ LookupTable (FoyerMemoryCache)
//!   Insert        cache.insert()
//!   Update        cache.insert()  (upsert)
//!   Delete        cache.invalidate()
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use laminar_core::lookup::LookupTable;

/// CDC event types the adapter understands.
#[derive(Debug, Clone)]
pub enum CdcOperation {
    /// A new row was inserted.
    Insert {
        /// Row key.
        key: Vec<u8>,
        /// Serialized row value.
        value: Vec<u8>,
    },
    /// An existing row was updated.
    Update {
        /// Row key.
        key: Vec<u8>,
        /// New serialized row value.
        value: Vec<u8>,
    },
    /// A row was deleted.
    Delete {
        /// Row key.
        key: Vec<u8>,
    },
}

/// Configuration for [`CdcCacheAdapter`].
#[derive(Debug, Clone)]
pub struct CdcCacheAdapterConfig {
    /// Lookup table identifier (for logging).
    pub table_id: u32,
    /// Batch size for initial snapshot loading.
    pub snapshot_batch_size: usize,
    /// Maximum acceptable replication lag before alerting.
    pub max_lag_threshold: Duration,
}

impl Default for CdcCacheAdapterConfig {
    fn default() -> Self {
        Self {
            table_id: 0,
            snapshot_batch_size: 10_000,
            max_lag_threshold: Duration::from_secs(5),
        }
    }
}

/// Translates CDC events into cache mutations.
///
/// Generic over the cache implementation (`C: LookupTable`) so it
/// works with `FoyerMemoryCache`, in-memory test caches, or any
/// future cache backend.
pub struct CdcCacheAdapter<C: LookupTable> {
    cache: Arc<C>,
    config: CdcCacheAdapterConfig,
    events_processed: AtomicU64,
    last_lsn: AtomicU64,
}

impl<C: LookupTable> CdcCacheAdapter<C> {
    /// Create a new adapter.
    pub fn new(cache: Arc<C>, config: CdcCacheAdapterConfig) -> Self {
        Self {
            cache,
            config,
            events_processed: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
        }
    }

    /// Process a single CDC event.
    pub fn process_event(&self, op: CdcOperation) {
        match op {
            CdcOperation::Insert { key, value } | CdcOperation::Update { key, value } => {
                self.cache.insert(&key, Bytes::from(value));
            }
            CdcOperation::Delete { key } => {
                self.cache.invalidate(&key);
            }
        }
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Process a batch of CDC events.
    pub fn process_batch(&self, ops: &[CdcOperation]) {
        for op in ops {
            // Clone needed because we consume the key/value in insert.
            self.process_event(op.clone());
        }
    }

    /// Update the replication position (LSN / offset).
    pub fn update_lsn(&self, lsn: u64) {
        self.last_lsn.store(lsn, Ordering::Release);
    }

    /// Current replication position.
    #[must_use]
    pub fn lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::Acquire)
    }

    /// Total number of events processed since creation.
    #[must_use]
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    /// Reference to the underlying cache.
    #[must_use]
    pub fn cache(&self) -> &C {
        &self.cache
    }

    /// The adapter's configuration.
    #[must_use]
    pub fn config(&self) -> &CdcCacheAdapterConfig {
        &self.config
    }
}

impl<C: LookupTable> std::fmt::Debug for CdcCacheAdapter<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcCacheAdapter")
            .field("table_id", &self.config.table_id)
            .field("events_processed", &self.events_processed())
            .field("last_lsn", &self.lsn())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::lookup::table::LookupResult;
    use std::sync::atomic::AtomicUsize;

    /// Minimal in-memory `LookupTable` for testing.
    struct TestCache {
        data: parking_lot::RwLock<rustc_hash::FxHashMap<Vec<u8>, Bytes>>,
        len: AtomicUsize,
    }

    impl TestCache {
        fn new() -> Self {
            Self {
                data: parking_lot::RwLock::new(rustc_hash::FxHashMap::default()),
                len: AtomicUsize::new(0),
            }
        }
    }

    impl LookupTable for TestCache {
        fn get_cached(&self, key: &[u8]) -> LookupResult {
            match self.data.read().get(key) {
                Some(v) => LookupResult::Hit(v.clone()),
                None => LookupResult::NotFound,
            }
        }

        fn get(&self, key: &[u8]) -> LookupResult {
            self.get_cached(key)
        }

        fn insert(&self, key: &[u8], value: Bytes) {
            let mut data = self.data.write();
            if !data.contains_key(key) {
                self.len.fetch_add(1, Ordering::Relaxed);
            }
            data.insert(key.to_vec(), value);
        }

        fn invalidate(&self, key: &[u8]) {
            if self.data.write().remove(key).is_some() {
                self.len.fetch_sub(1, Ordering::Relaxed);
            }
        }

        fn len(&self) -> usize {
            self.len.load(Ordering::Relaxed)
        }
    }

    fn make_adapter() -> (Arc<TestCache>, CdcCacheAdapter<TestCache>) {
        let cache = Arc::new(TestCache::new());
        let adapter = CdcCacheAdapter::new(
            Arc::clone(&cache),
            CdcCacheAdapterConfig::default(),
        );
        (cache, adapter)
    }

    #[test]
    fn test_cdc_insert_update_delete() {
        let (cache, adapter) = make_adapter();

        // Insert
        adapter.process_event(CdcOperation::Insert {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        });
        assert!(cache.get_cached(b"k1").is_hit());

        // Update (upsert)
        adapter.process_event(CdcOperation::Update {
            key: b"k1".to_vec(),
            value: b"v1_updated".to_vec(),
        });
        let hit = cache.get_cached(b"k1");
        assert_eq!(
            hit.into_bytes().unwrap(),
            Bytes::from_static(b"v1_updated")
        );

        // Delete
        adapter.process_event(CdcOperation::Delete {
            key: b"k1".to_vec(),
        });
        assert!(cache.get_cached(b"k1").is_not_found());
    }

    #[test]
    fn test_cdc_batch_processing() {
        let (cache, adapter) = make_adapter();

        let ops = vec![
            CdcOperation::Insert {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            CdcOperation::Insert {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            CdcOperation::Delete {
                key: b"a".to_vec(),
            },
            CdcOperation::Update {
                key: b"b".to_vec(),
                value: b"3".to_vec(),
            },
        ];

        adapter.process_batch(&ops);

        assert!(cache.get_cached(b"a").is_not_found());
        assert_eq!(
            cache.get_cached(b"b").into_bytes().unwrap(),
            Bytes::from_static(b"3")
        );
        assert_eq!(adapter.events_processed(), 4);
    }

    #[test]
    fn test_cdc_idempotent_upsert() {
        let (cache, adapter) = make_adapter();

        // Insert same key twice — second is an upsert
        adapter.process_event(CdcOperation::Insert {
            key: b"k".to_vec(),
            value: b"first".to_vec(),
        });
        adapter.process_event(CdcOperation::Update {
            key: b"k".to_vec(),
            value: b"second".to_vec(),
        });

        assert_eq!(
            cache.get_cached(b"k").into_bytes().unwrap(),
            Bytes::from_static(b"second")
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cdc_lsn_tracking() {
        let (_, adapter) = make_adapter();

        assert_eq!(adapter.lsn(), 0);
        adapter.update_lsn(100);
        assert_eq!(adapter.lsn(), 100);
        adapter.update_lsn(200);
        assert_eq!(adapter.lsn(), 200);
    }

    #[test]
    fn test_cdc_events_processed_counter() {
        let (_, adapter) = make_adapter();

        assert_eq!(adapter.events_processed(), 0);
        adapter.process_event(CdcOperation::Insert {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        });
        assert_eq!(adapter.events_processed(), 1);
    }

    #[test]
    fn test_cdc_adapter_debug() {
        let (_, adapter) = make_adapter();
        let debug = format!("{adapter:?}");
        assert!(debug.contains("CdcCacheAdapter"));
    }

    #[test]
    fn test_cdc_default_config() {
        let config = CdcCacheAdapterConfig::default();
        assert_eq!(config.table_id, 0);
        assert_eq!(config.snapshot_batch_size, 10_000);
        assert_eq!(config.max_lag_threshold, Duration::from_secs(5));
    }
}
