//! foyer-backed caches for lookup tables.
//!
//! ## Ring 0 — [`FoyerMemoryCache`]
//!
//! Synchronous [`foyer::Cache`] with S3-FIFO eviction. Checked per-event
//! on the operator hot path — sub-microsecond latency.
//!
//! ## Ring 1 — [`LookupCacheHierarchy`]
//!
//! Async two-tier cache (`HybridCache` memory + disk) with source fallback.
//! Used only on Ring 0 miss (cold path). foyer's built-in request
//! deduplication eliminates the need for external concurrency control.
//!
//! ```text
//! Ring 0 (sync, <500ns)              Ring 1 (async, 10us-100ms)
//! ┌──────────────────┐    miss    ┌──────────────────────────────┐
//! │ FoyerMemoryCache │ ────────→ │ LookupCacheHierarchy         │
//! │ (foyer::Cache)   │ ←──────── │   HybridCache mem → disk     │
//! └──────────────────┘  promote  │   └→ LookupSource (external)  │
//!                                └──────────────────────────────┘
//! ```

use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use equivalent::Equivalent;
use foyer::{
    BlockEngineBuilder, Cache, CacheBuilder, DeviceBuilder, FsDeviceBuilder,
    HybridCache, NoopDeviceBuilder,
};
use tokio::sync::Semaphore;

use crate::lookup::source::{LookupError, LookupSource};
use crate::lookup::table::{LookupResult, LookupTable};

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
struct LookupCacheKeyRef<'a> {
    table_id: u32,
    key: &'a [u8],
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
        let ref_key = LookupCacheKeyRef {
            table_id: self.table_id,
            key,
        };
        if let Some(entry) = self.cache.get(&ref_key) {
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
        // insert() requires an owned key — allocation is acceptable here
        // because inserts are on the cold path (cache miss → source query).
        let cache_key = self.make_key(key);
        self.cache.insert(cache_key, value);
    }

    fn invalidate(&self, key: &[u8]) {
        let ref_key = LookupCacheKeyRef {
            table_id: self.table_id,
            key,
        };
        self.cache.remove(&ref_key);
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

// ---------------------------------------------------------------------------
// Ring 1: HybridCache hierarchy
// ---------------------------------------------------------------------------

/// Configuration for [`LookupCacheHierarchy`].
#[derive(Debug, Clone)]
pub struct HybridCacheConfig {
    /// Memory capacity for the hybrid cache's internal memory tier (bytes).
    pub memory_capacity: usize,
    /// Disk capacity for the hybrid cache's disk tier (bytes).
    pub disk_capacity: usize,
    /// Directory for disk-backed cache files.
    pub disk_dir: PathBuf,
    /// Optional TTL for cached entries. `None` means no expiry.
    pub ttl: Option<Duration>,
    /// Number of shards for the disk engine (reserved for future foyer API).
    pub disk_shards: usize,
    /// Maximum number of concurrent source fetches allowed.
    pub max_concurrent_fetches: usize,
}

impl Default for HybridCacheConfig {
    fn default() -> Self {
        Self {
            memory_capacity: 32 * 1024 * 1024, // 32 MiB
            disk_capacity: 1024 * 1024 * 1024,  // 1 GiB
            disk_dir: std::env::temp_dir().join("laminar").join("lookup_cache"),
            ttl: None,
            disk_shards: 4,
            max_concurrent_fetches: 64,
        }
    }
}

/// Value stored in the [`HybridCache`] tier.
///
/// Wraps raw data bytes with an insertion timestamp for TTL checks.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CachedValue {
    /// Raw value bytes.
    pub data: Vec<u8>,
    /// Milliseconds since UNIX epoch when this entry was cached.
    pub cached_at_ms: u64,
}

impl CachedValue {
    /// Create a new cached value stamped with the current time.
    #[allow(clippy::cast_possible_truncation)]
    fn new(data: Vec<u8>) -> Self {
        let cached_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self { data, cached_at_ms }
    }

    /// Check whether this entry has expired given an optional TTL.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn is_expired(&self, ttl: Option<Duration>) -> bool {
        match ttl {
            None => false,
            Some(ttl) => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now_ms.saturating_sub(self.cached_at_ms) > ttl.as_millis() as u64
            }
        }
    }
}

/// Metrics snapshot from [`LookupCacheHierarchy`].
#[derive(Debug, Clone)]
pub struct HierarchyMetrics {
    /// Bytes used by the hybrid cache's in-memory tier.
    pub hybrid_memory_usage: usize,
    /// Number of successful source queries.
    pub source_hits: u64,
    /// Number of source queries that returned no data.
    pub source_misses: u64,
    /// Number of source fetches currently in-flight.
    pub inflight_fetches: usize,
}

/// Two-tier async cache hierarchy for lookup tables.
///
/// Sits between the hot [`FoyerMemoryCache`] (Ring 0) and the external
/// [`LookupSource`]. Uses foyer's [`HybridCache`] for a memory + disk
/// tier with built-in request deduplication on disk reads.
///
/// Generic over the source type `S` because [`LookupSource`] uses RPITIT
/// and is not dyn-compatible.
pub struct LookupCacheHierarchy<S> {
    hot_cache: Arc<FoyerMemoryCache>,
    hybrid: HybridCache<LookupCacheKey, CachedValue>,
    source: S,
    config: HybridCacheConfig,
    source_hits: AtomicU64,
    source_misses: AtomicU64,
    fetch_semaphore: Semaphore,
}

impl<S: LookupSource> LookupCacheHierarchy<S> {
    /// Build a hierarchy backed by a real filesystem disk tier.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Internal`] if directory creation, device
    /// initialization, or hybrid cache construction fails.
    pub async fn new(
        hot_cache: Arc<FoyerMemoryCache>,
        source: S,
        config: HybridCacheConfig,
    ) -> Result<Self, LookupError> {
        std::fs::create_dir_all(&config.disk_dir).map_err(|e| {
            LookupError::Internal(format!("failed to create cache dir: {e}"))
        })?;

        let device = FsDeviceBuilder::new(&config.disk_dir)
            .with_capacity(config.disk_capacity)
            .build()
            .map_err(|e| LookupError::Internal(format!("device build: {e}")))?;

        let hybrid = HybridCache::builder()
            .memory(config.memory_capacity)
            .storage()
            .with_engine_config(BlockEngineBuilder::new(device))
            .build()
            .await
            .map_err(|e| LookupError::Internal(format!("hybrid build: {e}")))?;

        let fetch_semaphore = Semaphore::new(config.max_concurrent_fetches);
        Ok(Self {
            hot_cache,
            hybrid,
            source,
            config,
            source_hits: AtomicU64::new(0),
            source_misses: AtomicU64::new(0),
            fetch_semaphore,
        })
    }

    /// Build a hierarchy with a noop storage backend (no disk I/O).
    ///
    /// Useful for tests or environments without a writable filesystem.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Internal`] if hybrid cache construction fails.
    pub async fn with_noop_storage(
        hot_cache: Arc<FoyerMemoryCache>,
        source: S,
        config: HybridCacheConfig,
    ) -> Result<Self, LookupError> {
        let device = NoopDeviceBuilder::default()
            .build()
            .map_err(|e| LookupError::Internal(format!("noop device: {e}")))?;

        let hybrid = HybridCache::builder()
            .memory(config.memory_capacity)
            .storage()
            .with_engine_config(BlockEngineBuilder::new(device))
            .build()
            .await
            .map_err(|e| LookupError::Internal(format!("hybrid build: {e}")))?;

        let fetch_semaphore = Semaphore::new(config.max_concurrent_fetches);
        Ok(Self {
            hot_cache,
            hybrid,
            source,
            config,
            source_hits: AtomicU64::new(0),
            source_misses: AtomicU64::new(0),
            fetch_semaphore,
        })
    }

    /// Fetch a value through the cache hierarchy.
    ///
    /// 1. Check hybrid cache (memory + disk) via `obtain()` (deduplicates
    ///    concurrent disk reads for the same key).
    /// 2. On hit + not expired → promote to hot cache, return value.
    /// 3. On miss or expired → query source, insert into hybrid + hot cache.
    /// 4. Source miss → return `Ok(None)` without caching.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Internal`] on hybrid cache I/O failure, or
    /// propagates errors from the underlying [`LookupSource::query`].
    pub async fn fetch(
        &self,
        table_id: u32,
        key: &[u8],
    ) -> Result<Option<Bytes>, LookupError> {
        let cache_key = LookupCacheKey {
            table_id,
            key: key.to_vec(),
        };

        // Check hybrid cache (memory + disk tiers)
        let mut stale_data: Option<Vec<u8>> = None;
        if let Some(entry) = self
            .hybrid
            .obtain(cache_key.clone())
            .await
            .map_err(|e| LookupError::Internal(format!("hybrid obtain: {e}")))?
        {
            let cached = entry.value();
            if !cached.is_expired(self.config.ttl) {
                // Promote to hot cache and return
                let data = Bytes::from(cached.data.clone());
                self.hot_cache.insert(key, data.clone());
                return Ok(Some(data));
            }
            // Expired — save stale data for fallback before removing
            stale_data = Some(cached.data.clone());
            self.hybrid.remove(&cache_key);
        }

        // Cache miss or expired — acquire semaphore permit, then query source
        let _permit = self.fetch_semaphore.acquire().await.map_err(|_| {
            LookupError::Internal("fetch semaphore closed".to_string())
        })?;

        let keys: Vec<&[u8]> = vec![key];
        let source_result = self.source.query(&keys, &[], &[]).await;

        match source_result {
            Ok(results) => {
                if let Some(value) = results.into_iter().next().flatten() {
                    self.source_hits.fetch_add(1, Ordering::Relaxed);
                    let cached_value = CachedValue::new(value.clone());
                    self.hybrid.insert(cache_key, cached_value);
                    let data = Bytes::from(value);
                    self.hot_cache.insert(key, data.clone());
                    Ok(Some(data))
                } else {
                    self.source_misses.fetch_add(1, Ordering::Relaxed);
                    Ok(None)
                }
            }
            Err(err) => {
                // Stale fallback: if we had an expired entry, serve it
                // instead of propagating the source error
                if let Some(stale) = stale_data {
                    let data = Bytes::from(stale);
                    self.hot_cache.insert(key, data.clone());
                    Ok(Some(data))
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Bulk-insert entries into the hybrid cache for warming.
    ///
    /// Returns the number of entries inserted.
    pub fn warm(
        &self,
        table_id: u32,
        entries: &[(&[u8], &[u8])],
    ) -> usize {
        for (key, value) in entries {
            let cache_key = LookupCacheKey {
                table_id,
                key: key.to_vec(),
            };
            let cached_value = CachedValue::new(value.to_vec());
            self.hybrid.insert(cache_key, cached_value);
            // Also insert into hot cache for immediate Ring 0 availability
            self.hot_cache.insert(key, Bytes::from(value.to_vec()));
        }
        entries.len()
    }

    /// Snapshot of cache hierarchy metrics.
    pub fn metrics(&self) -> HierarchyMetrics {
        let max = self.config.max_concurrent_fetches;
        let available = self.fetch_semaphore.available_permits();
        HierarchyMetrics {
            hybrid_memory_usage: self.hybrid.memory().usage(),
            source_hits: self.source_hits.load(Ordering::Relaxed),
            source_misses: self.source_misses.load(Ordering::Relaxed),
            inflight_fetches: max - available,
        }
    }

    /// Reference to the underlying source.
    pub fn source(&self) -> &S {
        &self.source
    }
}

impl<S: LookupSource> std::fmt::Debug for LookupCacheHierarchy<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupCacheHierarchy")
            .field("source", &self.source.source_name())
            .field("hybrid_memory_usage", &self.hybrid.memory().usage())
            .field("source_hits", &self.source_hits.load(Ordering::Relaxed))
            .field("source_misses", &self.source_misses.load(Ordering::Relaxed))
            .finish_non_exhaustive()
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

    // -----------------------------------------------------------------------
    // Ring 1 hierarchy tests
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use std::future::Future;

    use crate::lookup::source::{
        ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
    };
    use crate::lookup::predicate::Predicate;

    /// In-memory lookup source for hierarchy tests.
    struct TestSource {
        data: HashMap<Vec<u8>, Vec<u8>>,
    }

    impl TestSource {
        fn new(entries: &[(&[u8], &[u8])]) -> Self {
            let mut data = HashMap::new();
            for (k, v) in entries {
                data.insert(k.to_vec(), v.to_vec());
            }
            Self { data }
        }

        fn empty() -> Self {
            Self {
                data: HashMap::new(),
            }
        }
    }

    impl LookupSource for TestSource {
        fn query(
            &self,
            keys: &[&[u8]],
            _predicates: &[Predicate],
            _projection: &[ColumnId],
        ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send
        {
            let results: Vec<Option<Vec<u8>>> = keys
                .iter()
                .map(|k| self.data.get(k.as_ref()).cloned())
                .collect();
            async move { Ok(results) }
        }

        fn capabilities(&self) -> LookupSourceCapabilities {
            LookupSourceCapabilities::default()
        }

        fn source_name(&self) -> &str {
            "test_source"
        }
    }

    /// Helper to build a hierarchy with noop storage for testing.
    async fn test_hierarchy(
        source: TestSource,
    ) -> LookupCacheHierarchy<TestSource> {
        let hot = Arc::new(small_cache(1));
        let config = HybridCacheConfig {
            memory_capacity: 4 * 1024 * 1024,
            ..Default::default()
        };
        LookupCacheHierarchy::with_noop_storage(hot, source, config)
            .await
            .expect("noop hierarchy should build")
    }

    /// Lookup source that always returns an error.
    struct FailingSource;

    impl LookupSource for FailingSource {
        fn query(
            &self,
            _keys: &[&[u8]],
            _predicates: &[Predicate],
            _projection: &[ColumnId],
        ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send
        {
            async { Err(LookupError::Internal("source unavailable".to_string())) }
        }

        fn capabilities(&self) -> LookupSourceCapabilities {
            LookupSourceCapabilities::default()
        }

        fn source_name(&self) -> &str {
            "failing_source"
        }
    }

    #[test]
    fn test_hybrid_cache_config_defaults() {
        let config = HybridCacheConfig::default();
        assert_eq!(config.memory_capacity, 32 * 1024 * 1024);
        assert_eq!(config.disk_capacity, 1024 * 1024 * 1024);
        assert!(config.ttl.is_none());
        assert!(config.disk_dir.ends_with("lookup_cache"));
        assert_eq!(config.disk_shards, 4);
        assert_eq!(config.max_concurrent_fetches, 64);
    }

    #[test]
    fn test_cached_value_no_ttl() {
        let cv = CachedValue::new(b"hello".to_vec());
        assert!(!cv.is_expired(None));
    }

    #[test]
    fn test_cached_value_not_expired() {
        let cv = CachedValue::new(b"hello".to_vec());
        // 1 hour TTL — should not be expired yet
        assert!(!cv.is_expired(Some(Duration::from_secs(3600))));
    }

    #[test]
    fn test_cached_value_expired() {
        let mut cv = CachedValue::new(b"hello".to_vec());
        // Set cached_at to 2 seconds ago
        cv.cached_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 2000;
        // 1-second TTL — should be expired
        assert!(cv.is_expired(Some(Duration::from_secs(1))));
    }

    #[tokio::test]
    async fn test_hierarchy_fetch_populates_hybrid() {
        let source = TestSource::new(&[(b"k1", b"v1"), (b"k2", b"v2")]);
        let h = test_hierarchy(source).await;

        // First fetch — goes to source
        let result = h.fetch(1, b"k1").await.unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"v1")));

        // Verify it was promoted to hot cache
        let hot_result = h.hot_cache.get_cached(b"k1");
        assert!(hot_result.is_hit());

        // Verify source_hits counter
        assert_eq!(h.metrics().source_hits, 1);
    }

    #[tokio::test]
    async fn test_hierarchy_fetch_source_miss() {
        let source = TestSource::empty();
        let h = test_hierarchy(source).await;

        let result = h.fetch(1, b"missing").await.unwrap();
        assert_eq!(result, None);

        // Should NOT be in hot cache
        let hot_result = h.hot_cache.get_cached(b"missing");
        assert!(hot_result.is_not_found());

        // Source miss counter should increment
        assert_eq!(h.metrics().source_misses, 1);
    }

    #[tokio::test]
    async fn test_hierarchy_warm() {
        let source = TestSource::empty();
        let h = test_hierarchy(source).await;

        let entries: Vec<(&[u8], &[u8])> =
            vec![(b"w1", b"val1"), (b"w2", b"val2"), (b"w3", b"val3")];
        let count = h.warm(1, &entries);
        assert_eq!(count, 3);

        // Warmed entries should be retrievable via obtain (hybrid hit)
        let result = h.fetch(1, b"w1").await.unwrap();
        assert_eq!(result, Some(Bytes::from(b"val1".to_vec())));

        // Source should not have been queried (source_hits stays 0)
        assert_eq!(h.metrics().source_hits, 0);
    }

    #[tokio::test]
    async fn test_hierarchy_metrics() {
        let source = TestSource::new(&[(b"k1", b"v1")]);
        let h = test_hierarchy(source).await;

        // Initial state
        let m = h.metrics();
        assert_eq!(m.source_hits, 0);
        assert_eq!(m.source_misses, 0);
        assert_eq!(m.inflight_fetches, 0);

        // One hit, one miss
        h.fetch(1, b"k1").await.unwrap();
        h.fetch(1, b"nope").await.unwrap();

        let m = h.metrics();
        assert_eq!(m.source_hits, 1);
        assert_eq!(m.source_misses, 1);
    }

    #[tokio::test]
    async fn test_stale_fallback_on_source_failure() {
        // Build a hierarchy with a short TTL and a normal source first
        let hot = Arc::new(small_cache(1));
        let config = HybridCacheConfig {
            memory_capacity: 4 * 1024 * 1024,
            ttl: Some(Duration::from_millis(50)),
            ..Default::default()
        };
        let source = TestSource::new(&[(b"k1", b"v1")]);
        let h = LookupCacheHierarchy::with_noop_storage(hot, source, config)
            .await
            .unwrap();

        // Populate the cache
        h.fetch(1, b"k1").await.unwrap();

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now build a hierarchy with a failing source using the same hybrid cache
        // We can't easily swap sources, so test the stale path by directly
        // inserting an expired entry and using a FailingSource from the start
        let hot2 = Arc::new(small_cache(1));
        let config2 = HybridCacheConfig {
            memory_capacity: 4 * 1024 * 1024,
            ttl: Some(Duration::from_millis(1)),
            ..Default::default()
        };
        let h2 =
            LookupCacheHierarchy::with_noop_storage(hot2, FailingSource, config2)
                .await
                .unwrap();

        // Warm with an entry, then wait for it to expire
        h2.warm(1, &[(b"stale_key", b"stale_val")]);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Fetch should return stale data instead of propagating error
        let result = h2.fetch(1, b"stale_key").await.unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"stale_val")));
    }

    #[tokio::test]
    async fn test_source_error_no_stale_propagates() {
        // When there's no stale entry, source errors should propagate
        let hot = Arc::new(small_cache(1));
        let config = HybridCacheConfig {
            memory_capacity: 4 * 1024 * 1024,
            ..Default::default()
        };
        let h =
            LookupCacheHierarchy::with_noop_storage(hot, FailingSource, config)
                .await
                .unwrap();

        let result = h.fetch(1, b"missing").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_warm_populates_hot_cache() {
        let source = TestSource::empty();
        let h = test_hierarchy(source).await;

        let entries: Vec<(&[u8], &[u8])> = vec![(b"h1", b"hot1"), (b"h2", b"hot2")];
        h.warm(1, &entries);

        // Entries should be in hot cache immediately (no fetch needed)
        let r1 = h.hot_cache.get_cached(b"h1");
        assert!(r1.is_hit());
        assert_eq!(r1.into_bytes().unwrap(), Bytes::from_static(b"hot1"));

        let r2 = h.hot_cache.get_cached(b"h2");
        assert!(r2.is_hit());
    }

    #[tokio::test]
    async fn test_inflight_metric_reflects_semaphore() {
        let source = TestSource::new(&[(b"k1", b"v1")]);
        let hot = Arc::new(small_cache(1));
        let config = HybridCacheConfig {
            memory_capacity: 4 * 1024 * 1024,
            max_concurrent_fetches: 8,
            ..Default::default()
        };
        let h = LookupCacheHierarchy::with_noop_storage(hot, source, config)
            .await
            .unwrap();

        // Before any fetch, inflight should be 0
        assert_eq!(h.metrics().inflight_fetches, 0);

        // After a fetch completes, inflight should return to 0
        h.fetch(1, b"k1").await.unwrap();
        assert_eq!(h.metrics().inflight_fetches, 0);
    }
}
