//! foyer-backed in-memory cache for lookup tables.
//!
//! [`FoyerMemoryCache`] implements [`LookupTable`] using foyer's
//! high-performance in-memory cache with S3-FIFO eviction. Only the
//! synchronous [`foyer::Cache`] is used (not `HybridCache`) to stay
//! within Ring 0 latency requirements.
//!
//! ## Performance targets
//!
//! - Cache hit `get()`: < 500ns
//! - Cache miss: < 200ns
//! - `insert()`: < 1us

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use foyer::{Cache, CacheBuilder};

use crate::lookup::table::{LookupResult, LookupTable};

/// Composite cache key: table ID + raw key bytes.
///
/// The `table_id` ensures that caches for different lookup tables
/// never collide, even if they share a `foyer::Cache` instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupCacheKey {
    /// Lookup table identifier.
    pub table_id: u32,
    /// Raw key bytes.
    pub key: Vec<u8>,
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
    cache: Cache<LookupCacheKey, Bytes>,
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
}

impl LookupTable for FoyerMemoryCache {
    fn get_cached(&self, key: &[u8]) -> LookupResult {
        let cache_key = self.make_key(key);
        if let Some(entry) = self.cache.get(&cache_key) {
            // Clone the Bytes value (Arc bump only), then drop the
            // CacheEntry handle immediately — do NOT hold it across
            // operator boundaries (audit fix C3).
            let value = entry.value().clone();
            self.hits.fetch_add(1, Ordering::Relaxed);
            LookupResult::Hit(value)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            LookupResult::NotFound
        }
    }

    fn get(&self, key: &[u8]) -> LookupResult {
        // Memory-only cache — no deeper tier to check.
        self.get_cached(key)
    }

    fn insert(&self, key: &[u8], value: Bytes) {
        let cache_key = self.make_key(key);
        self.cache.insert(cache_key, value);
    }

    fn invalidate(&self, key: &[u8]) {
        let cache_key = self.make_key(key);
        self.cache.remove(&cache_key);
    }

    fn len(&self) -> usize {
        self.cache.usage()
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

        // Miss
        let result = cache.get_cached(b"key1");
        assert!(result.is_not_found());
        assert_eq!(cache.miss_count(), 1);

        // Insert + hit
        cache.insert(b"key1", Bytes::from_static(b"value1"));
        let result = cache.get_cached(b"key1");
        assert!(result.is_hit());
        assert_eq!(
            result.into_bytes().unwrap(),
            Bytes::from_static(b"value1")
        );
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

        // Insert more than capacity
        for i in 0..20u8 {
            cache.insert(&[i], Bytes::from(vec![i]));
        }

        // Some entries should have been evicted
        assert!(cache.len() <= 8, "len {} > capacity 8", cache.len());
    }

    #[test]
    fn test_foyer_cache_invalidation() {
        let cache = small_cache(1);

        cache.insert(b"key1", Bytes::from_static(b"value1"));
        assert!(cache.get_cached(b"key1").is_hit());

        cache.invalidate(b"key1");
        assert!(cache.get_cached(b"key1").is_not_found());
    }

    #[test]
    fn test_foyer_cache_table_id_isolation() {
        let cache_a = small_cache(1);
        let cache_b = small_cache(2);

        cache_a.insert(b"key1", Bytes::from_static(b"from_a"));
        cache_b.insert(b"key1", Bytes::from_static(b"from_b"));

        // Each cache should return its own value
        let val_a = cache_a.get_cached(b"key1").into_bytes().unwrap();
        let val_b = cache_b.get_cached(b"key1").into_bytes().unwrap();

        assert_eq!(val_a, Bytes::from_static(b"from_a"));
        assert_eq!(val_b, Bytes::from_static(b"from_b"));
    }

    #[test]
    fn test_foyer_cache_implements_lookup_table() {
        // Verify trait object compatibility.
        let cache = small_cache(1);
        let table: &dyn LookupTable = &cache;

        table.insert(b"k", Bytes::from_static(b"v"));
        assert!(!table.is_empty());
        assert!(table.get(b"k").is_hit());
    }

    #[test]
    fn test_foyer_cache_hit_ratio() {
        let cache = small_cache(1);
        cache.insert(b"k1", Bytes::from_static(b"v1"));

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
}
