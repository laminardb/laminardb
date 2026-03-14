//! foyer-backed in-memory cache for lookup tables.
//!
//! ## Ring 0 — [`FoyerMemoryCache`]
//!
//! Synchronous [`foyer::Cache`] with S3-FIFO eviction. Checked per-event
//! on the operator hot path — sub-microsecond latency.
//!
//! `RecordBatch` clone is Arc bumps only (~16-48ns), within Ring 0 budget.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use equivalent::Equivalent;
use foyer::{Cache, CacheBuilder};

use crate::lookup::table::LookupResult;

/// Composite cache key: table ID + raw key bytes.
///
/// The `table_id` ensures that caches for different lookup tables
/// never collide, even if they share a `foyer::Cache` instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct LookupCacheKey {
    /// Lookup table identifier.
    pub table_id: u32,
    /// Raw key bytes.
    pub key: Vec<u8>,
}

/// Borrowed view of [`LookupCacheKey`] that avoids heap allocation.
///
/// Used with foyer's `Cache::get<Q>()` where `Q: Hash + Equivalent<K>`.
/// Hashes identically to `LookupCacheKey` because `Vec<u8>` and `[u8]`
/// produce the same hash output.
pub(crate) struct LookupCacheKeyRef<'a> {
    pub(crate) table_id: u32,
    pub(crate) key: &'a [u8],
}

impl Hash for LookupCacheKeyRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must match the derived Hash for LookupCacheKey:
        // Hash::hash(&self.table_id, state) then Hash::hash(&self.key, state).
        // Vec<u8>::hash delegates to [u8]::hash, so this is identical.
        self.table_id.hash(state);
        self.key.hash(state);
    }
}

impl Equivalent<LookupCacheKey> for LookupCacheKeyRef<'_> {
    fn equivalent(&self, other: &LookupCacheKey) -> bool {
        self.table_id == other.table_id && self.key == other.key.as_slice()
    }
}

/// Configuration for [`FoyerMemoryCache`].
#[derive(Debug, Clone, Copy)]
pub struct FoyerMemoryCacheConfig {
    /// Maximum number of entries in the cache.
    pub capacity: usize,
    /// Number of shards for concurrent access (should be a power of 2).
    pub shards: usize,
}

impl Default for FoyerMemoryCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 256 * 1024, // 256K entries
            shards: 16,
        }
    }
}

/// foyer-backed in-memory lookup table cache.
///
/// Wraps [`foyer::Cache`] with hit/miss counters and lookup-table
/// semantics. Designed for Ring 0 (< 500ns per operation).
///
/// # Thread safety
///
/// `foyer::Cache` is internally sharded and lock-free on the read path.
/// `FoyerMemoryCache` is `Send + Sync`.
pub struct FoyerMemoryCache {
    cache: Cache<LookupCacheKey, RecordBatch>,
    table_id: u32,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl FoyerMemoryCache {
    /// Create a new cache with the given configuration.
    #[must_use]
    pub fn new(table_id: u32, config: FoyerMemoryCacheConfig) -> Self {
        let cache = CacheBuilder::new(config.capacity)
            .with_shards(config.shards)
            .build();

        Self {
            cache,
            table_id,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a cache with default configuration.
    #[must_use]
    pub fn with_defaults(table_id: u32) -> Self {
        Self::new(table_id, FoyerMemoryCacheConfig::default())
    }

    /// Total cache hits since creation.
    #[must_use]
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total cache misses since creation.
    #[must_use]
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Cache hit ratio (0.0 – 1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// The table ID this cache is associated with.
    #[must_use]
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Build a composite key.
    fn make_key(&self, key: &[u8]) -> LookupCacheKey {
        LookupCacheKey {
            table_id: self.table_id,
            key: key.to_vec(),
        }
    }

    /// Look up a key in the in-memory cache only (Ring 0, < 500ns).
    pub fn get_cached(&self, key: &[u8]) -> LookupResult {
        let ref_key = LookupCacheKeyRef {
            table_id: self.table_id,
            key,
        };
        if let Some(entry) = self.cache.get(&ref_key) {
            let value = entry.value().clone();
            self.hits.fetch_add(1, Ordering::Relaxed);
            LookupResult::Hit(value)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            LookupResult::NotFound
        }
    }

    /// Alias for [`get_cached`](Self::get_cached). No slower storage tiers are wired yet.
    pub fn get(&self, key: &[u8]) -> LookupResult {
        self.get_cached(key)
    }

    /// Insert or update a cached entry.
    pub fn insert(&self, key: &[u8], value: RecordBatch) {
        let cache_key = self.make_key(key);
        self.cache.insert(cache_key, value);
    }

    /// Invalidate a cached entry.
    pub fn invalidate(&self, key: &[u8]) {
        let ref_key = LookupCacheKeyRef {
            table_id: self.table_id,
            key,
        };
        self.cache.remove(&ref_key);
    }

    /// Number of entries currently in the cache.
    ///
    /// Returns `foyer::Cache::usage()` which equals the entry count for
    /// unweighted caches (the default). If custom weighers are added,
    /// this will return total weight instead.
    pub fn len(&self) -> usize {
        self.cache.usage()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for FoyerMemoryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerMemoryCache")
            .field("table_id", &self.table_id)
            .field("entries", &self.cache.usage())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch(val: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![val]))]).unwrap()
    }

    fn small_cache(table_id: u32) -> FoyerMemoryCache {
        FoyerMemoryCache::new(
            table_id,
            FoyerMemoryCacheConfig {
                capacity: 64,
                shards: 4,
            },
        )
    }

    #[test]
    fn test_foyer_cache_hit_miss() {
        let cache = small_cache(1);

        let result = cache.get_cached(b"key1");
        assert!(result.is_not_found());
        assert_eq!(cache.miss_count(), 1);

        cache.insert(b"key1", test_batch("value1"));
        let result = cache.get_cached(b"key1");
        assert!(result.is_hit());
        let batch = result.into_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(cache.hit_count(), 1);
    }

    #[test]
    fn test_foyer_cache_eviction() {
        let cache = FoyerMemoryCache::new(
            1,
            FoyerMemoryCacheConfig {
                capacity: 8,
                shards: 1,
            },
        );

        for i in 0..20u8 {
            cache.insert(&[i], test_batch(&format!("v{i}")));
        }

        assert!(cache.len() <= 8, "len {} > capacity 8", cache.len());
    }

    #[test]
    fn test_foyer_cache_invalidation() {
        let cache = small_cache(1);

        cache.insert(b"key1", test_batch("value1"));
        assert!(cache.get_cached(b"key1").is_hit());

        cache.invalidate(b"key1");
        assert!(cache.get_cached(b"key1").is_not_found());
    }

    #[test]
    fn test_foyer_cache_table_id_isolation() {
        let cache_a = small_cache(1);
        let cache_b = small_cache(2);

        cache_a.insert(b"key1", test_batch("from_a"));
        cache_b.insert(b"key1", test_batch("from_b"));

        let batch_a = cache_a.get_cached(b"key1").into_batch().unwrap();
        let batch_b = cache_b.get_cached(b"key1").into_batch().unwrap();

        assert_eq!(batch_a.num_rows(), 1);
        assert_eq!(batch_b.num_rows(), 1);
        assert_ne!(batch_a, batch_b);
    }

    #[test]
    fn test_foyer_cache_lookup_methods() {
        let cache = small_cache(1);

        cache.insert(b"k", test_batch("v"));
        assert!(!cache.is_empty());
        assert!(cache.get(b"k").is_hit());
    }

    #[test]
    fn test_foyer_cache_hit_ratio() {
        let cache = small_cache(1);
        cache.insert(b"k1", test_batch("v1"));

        // 1 hit
        cache.get_cached(b"k1");
        // 1 miss
        cache.get_cached(b"k2");

        assert!((cache.hit_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_foyer_cache_debug() {
        let cache = small_cache(42);
        let debug = format!("{cache:?}");
        assert!(debug.contains("FoyerMemoryCache"));
        assert!(debug.contains("table_id: 42"));
    }

    #[test]
    fn test_foyer_cache_default_config() {
        let config = FoyerMemoryCacheConfig::default();
        assert_eq!(config.capacity, 256 * 1024);
        assert_eq!(config.shards, 16);
    }

    #[test]
    fn test_foyer_cache_recordbatch_clone_is_cheap() {
        let cache = small_cache(1);
        let batch = test_batch("value");
        cache.insert(b"k", batch.clone());

        let hit1 = cache.get_cached(b"k").into_batch().unwrap();
        let hit2 = cache.get_cached(b"k").into_batch().unwrap();
        assert_eq!(hit1, hit2);
        assert_eq!(hit1.num_rows(), 1);
    }
}
