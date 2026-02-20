# F-LOOKUP-005: foyer Hybrid Cache (Ring 1)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-005 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-LOOKUP-004 (foyer In-Memory Cache) |
| **Blocks** | None (enhances existing lookup path) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/foyer_cache.rs` |

## Summary

Ring 1 warm cache using foyer's `HybridCache<K,V>` for lookup table entries. This is Tier 1 in the three-tier caching hierarchy: the async, microsecond-latency disk cache that catches misses from the Ring 0 in-memory cache (Tier 0) before falling back to external source queries (Tier 2).

foyer's HybridCache combines an in-memory cache with a disk-based store. When entries are evicted from the in-memory tier (F-LOOKUP-004), they spill to disk automatically. On lookup, if the in-memory tier misses, the disk tier is checked before querying the external source. This dramatically reduces source query volume for working sets larger than memory.

The disk cache is shared across all lookup table partitions on the same node. It is configured with a separate size budget (default 1GB, up to available disk on NVMe). The async API means all HybridCache operations happen off the Ring 0 hot path.

## Goals

- Provide async disk-backed warm cache as Ring 1 fallback after Ring 0 miss
- Use foyer::HybridCache for combined memory + disk caching
- Automatic spill from in-memory to disk on eviction (WriteOnEviction policy)
- Shared disk cache across all lookup table partitions on same node
- 10-100 microsecond reads for disk cache hits (NVMe)
- TTL support for SourceDirect mode (stale data eviction)
- Configurable memory and disk size budgets
- Async fetch pipeline: miss -> disk check -> source fetch -> populate both tiers
- Cache warming: populate warm cache from Parquet snapshot on startup

## Non-Goals

- Synchronous access (that is Ring 0, see F-LOOKUP-004)
- External source queries (see F-LOOKUP-002, F-LOOKUP-007, F-LOOKUP-008)
- Distributed cache coherence (each node manages independently)
- Compression of cached values (foyer handles this internally)
- Custom serialization format (foyer uses its own format for disk)
- Persistent cache across node restarts (nice-to-have for later)

## Technical Design

### Architecture

**Ring**: Ring 1 (Async Path) -- off hot path, async I/O
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/foyer_cache.rs` (same file as F-LOOKUP-004)

The HybridCache sits between the Ring 0 hot cache and the external source. It is the first async component in the lookup chain. When the hot cache misses, the lookup table implementation spawns an async task that:

1. Checks the HybridCache disk tier (10-100us on NVMe)
2. On disk hit: populates the hot cache and returns the value
3. On disk miss: queries the external source (1-100ms)
4. Inserts the result into both the HybridCache and the hot cache

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Ring 0: Hot Cache Miss                                                  │
│  ──────────────────────                                                  │
│  hot_cache.get(key) → None                                               │
│  enqueue_async_lookup(key, buffered_event)                               │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│  Ring 1: Async Fetch Pipeline                                            │
│  ────────────────────────────                                            │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │  Step 1: HybridCache Lookup (async, 10-100μs on NVMe)            │   │
│  │                                                                   │   │
│  │  hybrid_cache.get(&key).await                                     │   │
│  │    ├── HIT:  → hot_cache.insert(key, value)                       │   │
│  │    │         → notify operator (ready to emit)                    │   │
│  │    │                                                              │   │
│  │    └── MISS: → proceed to Step 2                                  │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐   │
│  │  Step 2: Source Fetch (async, 1-100ms)                            │   │
│  │                                                                   │   │
│  │  source.query(&[key], predicates, projection).await               │   │
│  │    ├── FOUND: → hybrid_cache.insert(key, value)                   │   │
│  │    │          → hot_cache.insert(key, value)                      │   │
│  │    │          → notify operator (ready to emit)                   │   │
│  │    │                                                              │   │
│  │    └── NOT FOUND: → notify operator (emit with NULLs)            │   │
│  └───────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use bytes::Bytes;
use foyer::HybridCache;
use crate::lookup::foyer_cache::{LookupCacheKey, FoyerMemoryCache, CacheMetrics};
use crate::lookup::source::LookupSource;
use crate::lookup::LookupError;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the foyer HybridCache (Ring 1).
#[derive(Debug, Clone)]
pub struct FoyerHybridCacheConfig {
    /// In-memory portion of HybridCache.
    /// This is SEPARATE from the Ring 0 foyer::Cache (F-LOOKUP-004).
    /// It acts as a read-through buffer for the disk tier.
    /// Default: 32MB.
    pub memory_size_bytes: usize,

    /// Disk cache size in bytes.
    /// Shared across all lookup tables on this node.
    /// Default: 1GB. Should match available NVMe capacity.
    pub disk_size_bytes: usize,

    /// Directory for disk cache files.
    /// Should be on NVMe for best performance.
    /// Default: /tmp/laminar/lookup_cache
    pub disk_dir: std::path::PathBuf,

    /// Number of disk cache shards.
    /// Higher values improve concurrent I/O throughput.
    /// Default: 8.
    pub disk_shards: usize,

    /// TTL for cached entries (SourceDirect mode only).
    /// Entries older than TTL are considered stale and re-fetched.
    /// Default: None (no expiry for Replicated/Partitioned modes).
    pub ttl: Option<Duration>,

    /// Maximum concurrent async fetches.
    /// Bounds memory usage for in-flight source queries.
    /// Default: 256.
    pub max_concurrent_fetches: usize,

    /// Whether evictions from the Ring 0 hot cache should
    /// automatically populate the HybridCache disk tier.
    /// Default: true.
    pub write_on_eviction: bool,

    /// Flush interval for disk cache writes.
    /// Batches writes to reduce I/O syscalls.
    /// Default: 100ms.
    pub flush_interval: Duration,
}

impl Default for FoyerHybridCacheConfig {
    fn default() -> Self {
        Self {
            memory_size_bytes: 32 * 1024 * 1024,      // 32MB
            disk_size_bytes: 1024 * 1024 * 1024,       // 1GB
            disk_dir: std::path::PathBuf::from("/tmp/laminar/lookup_cache"),
            disk_shards: 8,
            ttl: None,
            max_concurrent_fetches: 256,
            write_on_eviction: true,
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// Combined Ring 0 + Ring 1 lookup cache.
///
/// Manages the full cache hierarchy:
/// - Tier 0 (Ring 0): foyer::Cache (synchronous, sub-μs)
/// - Tier 1 (Ring 1): foyer::HybridCache (async, 10-100μs disk)
/// - Tier 2 (Ring 1): LookupSource (async, 1-100ms external)
///
/// The async fetch pipeline handles cache miss resolution,
/// populating both tiers on successful fetch.
pub struct LookupCacheHierarchy {
    /// Ring 0: synchronous in-memory cache.
    hot_cache: Arc<FoyerMemoryCache>,

    /// Ring 1: async hybrid cache (memory + disk).
    hybrid_cache: HybridCache<LookupCacheKey, Vec<u8>>,

    /// Tier 2: external data source.
    source: Arc<dyn LookupSource>,

    /// Configuration.
    config: FoyerHybridCacheConfig,

    /// Semaphore to bound concurrent fetches.
    fetch_semaphore: tokio::sync::Semaphore,

    /// In-flight fetch deduplication.
    /// Prevents multiple concurrent fetches for the same key.
    inflight: dashmap::DashMap<LookupCacheKey, tokio::sync::broadcast::Sender<()>>,
}

impl LookupCacheHierarchy {
    /// Create a new cache hierarchy with the given configuration.
    pub async fn new(
        hot_cache: Arc<FoyerMemoryCache>,
        source: Arc<dyn LookupSource>,
        config: FoyerHybridCacheConfig,
    ) -> Result<Self, LookupError> {
        let hybrid_cache = HybridCache::builder()
            .memory(config.memory_size_bytes)
            .storage()
            .with_device_options(
                foyer::DirectFsDeviceOptions::new(&config.disk_dir)
                    .with_capacity(config.disk_size_bytes),
            )
            .build()
            .await
            .map_err(|e| LookupError::Internal(format!("Failed to build HybridCache: {}", e)))?;

        Ok(Self {
            hot_cache,
            hybrid_cache,
            source,
            fetch_semaphore: tokio::sync::Semaphore::new(config.max_concurrent_fetches),
            inflight: dashmap::DashMap::new(),
            config,
        })
    }

    /// Async lookup through the full cache hierarchy.
    ///
    /// 1. Check hot cache (sync, should already be checked by caller)
    /// 2. Check HybridCache disk tier (async, 10-100μs)
    /// 3. Query external source (async, 1-100ms)
    /// 4. Populate both caches on success
    ///
    /// **AUDIT FIX (C3)**: Returns `Result<Option<Bytes>, LookupError>`
    /// instead of `Result<Option<Vec<u8>>, LookupError>`. Values are
    /// converted to `Bytes` before returning, consistent with the
    /// `LookupTable` trait's `Bytes`-based API. The hot cache `get()`
    /// now also returns `Option<Bytes>` (see F-LOOKUP-004 C3 fix).
    pub async fn fetch(&self, key: &[u8]) -> Result<Option<Bytes>, LookupError> {
        let cache_key = LookupCacheKey {
            table_id: self.hot_cache.table_id(),
            key: key.to_vec(),
        };

        // Step 1: Check HybridCache (includes its own in-memory + disk)
        if let Some(entry) = self.hybrid_cache.get(&cache_key).await {
            let value = entry.value().clone();
            // Promote to hot cache
            self.hot_cache.insert(key, value.clone());
            // AUDIT FIX (C3): Convert to Bytes for consistent return type
            return Ok(Some(Bytes::from(value)));
        }

        // Step 2: Deduplicate in-flight fetches for same key
        if let Some(sender) = self.inflight.get(&cache_key) {
            let mut rx = sender.subscribe();
            drop(sender);
            let _ = rx.recv().await;
            // After notification, value should be in hot cache
            // AUDIT FIX (C3): hot_cache.get() now returns Option<Bytes>
            return Ok(self.hot_cache.get(key));
        }

        // Step 3: Acquire fetch semaphore (bounds concurrency)
        let _permit = self.fetch_semaphore.acquire().await
            .map_err(|_| LookupError::Internal("Fetch semaphore closed".into()))?;

        // Register in-flight
        let (tx, _) = tokio::sync::broadcast::channel(1);
        self.inflight.insert(cache_key.clone(), tx.clone());

        // Step 4: Query external source
        let result = self.source.query(
            &[key],
            &[],  // No predicates for single-key lookup
            &[],  // No projection: fetch all columns
        ).await;

        // Cleanup in-flight tracking
        self.inflight.remove(&cache_key);
        let _ = tx.send(());

        match result {
            Ok(mut values) => {
                if let Some(Some(value)) = values.pop() {
                    // Populate both caches
                    self.hybrid_cache.insert(cache_key, value.clone());
                    self.hot_cache.insert(key, value.clone());
                    // AUDIT FIX (C3): Convert to Bytes
                    Ok(Some(Bytes::from(value)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Warm the cache from a batch of entries.
    ///
    /// Used during startup to populate the cache from a Parquet snapshot
    /// or CDC replay. Inserts into both hot cache and HybridCache.
    pub async fn warm(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize, LookupError> {
        let count = entries.len();
        for (key, value) in entries {
            let cache_key = LookupCacheKey {
                table_id: self.hot_cache.table_id(),
                key: key.clone(),
            };
            self.hot_cache.insert(&key, value.clone());
            self.hybrid_cache.insert(cache_key, value);
        }
        Ok(count)
    }

    /// Get combined metrics from all cache tiers.
    pub fn metrics(&self) -> HierarchyMetrics {
        HierarchyMetrics {
            hot_cache: self.hot_cache.metrics(),
            hybrid_cache_entry_count: self.hybrid_cache.usage(),
            inflight_fetches: self.inflight.len(),
        }
    }
}
```

### Data Structures

```rust
/// Combined metrics from all cache tiers.
#[derive(Debug, Clone)]
pub struct HierarchyMetrics {
    /// Ring 0 hot cache metrics.
    pub hot_cache: CacheMetrics,
    /// Number of entries in HybridCache (memory + disk).
    pub hybrid_cache_entry_count: usize,
    /// Number of in-flight async fetches.
    pub inflight_fetches: usize,
}

/// TTL-aware cache entry wrapper for SourceDirect mode.
///
/// Wraps a cached value with an insertion timestamp for TTL-based
/// expiry. When `ttl` is configured, entries older than the TTL
/// are treated as stale and re-fetched from the source.
#[derive(Debug, Clone)]
pub struct TtlCacheEntry {
    /// The cached value bytes.
    pub value: Vec<u8>,
    /// Timestamp when this entry was inserted (epoch millis).
    pub inserted_at_ms: u64,
}

impl TtlCacheEntry {
    /// Check if this entry has expired relative to the given TTL.
    pub fn is_expired(&self, ttl: Duration, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.inserted_at_ms) > ttl.as_millis() as u64
    }
}
```

### Algorithm/Flow

#### Async Fetch Pipeline

```
1. Ring 0 operator detects cache miss
2. Operator enqueues (key, event_ref) to async fetch channel
3. Ring 1 fetch task receives the request:
   a. Check HybridCache: hybrid_cache.get(&key).await
      - HIT (10-100μs): promote to hot cache, notify operator
      - MISS: continue to source
   b. Check in-flight map: is another fetch for this key running?
      - YES: subscribe to completion broadcast, wait
      - NO: register this fetch, continue
   c. Acquire fetch semaphore (bounds concurrent source queries)
   d. Query source: source.query(&[key], &[], &[]).await
      - SUCCESS: insert into hybrid_cache AND hot_cache
      - NOT_FOUND: record negative result
      - ERROR: retry or propagate
   e. Remove from in-flight map, broadcast completion
   f. Release semaphore permit
4. Operator receives notification
5. Operator retrieves buffered event
6. hot_cache.get(key) → now hits
7. Operator emits joined event
```

#### Cache Warming Strategy

```
Startup sequence:

1. Build foyer::Cache (Ring 0) and HybridCache (Ring 1)
2. If strategy is Replicated and prefetch_on_startup:
   a. Load Parquet snapshot → iterate all rows
   b. Insert each (key, value) into hot_cache + hybrid_cache
   c. Start CDC stream for live updates
3. If strategy is SourceDirect:
   a. Do NOT prefetch
   b. Cache will populate on-demand as lookups occur
   c. TTL governs stale eviction
4. If strategy is Partitioned:
   a. Load only keys in local partition range
   b. Use Parquet with predicate pushdown for partition-aware loading
```

#### TTL Expiry (SourceDirect Mode)

```
1. On HybridCache hit, check TtlCacheEntry.is_expired()
2. If expired:
   a. Treat as a miss → query source for fresh value
   b. If source returns same value → update timestamp only
   c. If source returns different value → update both caches
   d. If source fails → return stale value (degraded mode)
3. If not expired:
   a. Return value normally
4. Background task periodically scans for expired entries
   and proactively re-fetches (optional, reduces latency spikes)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| HybridCache build failure | Disk dir not writable, not enough space | Fail startup with clear error |
| Disk I/O error on read | NVMe failure, corrupt file | Treat as miss, fetch from source |
| Disk I/O error on write | Disk full | Log warning, continue without disk tier |
| Fetch semaphore closed | Cache hierarchy shut down | Return error, operator handles gracefully |
| Source query failure | Network timeout, auth failure | Retry with backoff, circuit breaker |
| In-flight broadcast dropped | All receivers dropped | Stale entry in inflight map, cleanup on next access |
| TTL expiry + source failure | Stale data, source unreachable | Return stale value, log warning, metric |
| OOM during cache warming | Too many entries for memory | Cap warm batch size, log warning |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| HybridCache disk read | 10-100μs | NVMe, 4KB entry | `bench_hybrid_cache_disk_read` |
| HybridCache disk write | 50-200μs | NVMe, 4KB entry | `bench_hybrid_cache_disk_write` |
| Full fetch pipeline (disk hit) | < 200μs | NVMe, warm cache | `bench_fetch_pipeline_disk_hit` |
| Full fetch pipeline (source miss) | 1-10ms | Postgres source | `bench_fetch_pipeline_source` |
| In-flight deduplication | < 1μs overhead | DashMap lookup | `bench_inflight_dedup` |
| Cache warming throughput | > 100K entries/sec | 256-byte entries | `bench_cache_warm_throughput` |
| TTL check overhead | < 50ns | Timestamp comparison | `bench_ttl_check` |
| Concurrent fetches | 256 parallel | Semaphore bounded | `bench_concurrent_fetches` |

## Test Plan

### Unit Tests

- [ ] `test_hybrid_cache_config_default_values` -- verify defaults
- [ ] `test_hybrid_cache_build_success` -- create with valid config
- [ ] `test_hybrid_cache_build_invalid_dir` -- error on non-writable dir
- [ ] `test_ttl_cache_entry_not_expired` -- within TTL
- [ ] `test_ttl_cache_entry_expired` -- beyond TTL
- [ ] `test_ttl_cache_entry_no_ttl` -- None TTL means never expires
- [ ] `test_hierarchy_metrics_populated` -- all fields present

### Integration Tests

- [ ] `test_cache_hierarchy_hot_miss_disk_hit` -- value in hybrid, not hot
- [ ] `test_cache_hierarchy_hot_miss_disk_miss_source_hit` -- full pipeline
- [ ] `test_cache_hierarchy_hot_miss_source_miss` -- returns None
- [ ] `test_cache_hierarchy_promotes_to_hot` -- disk hit populates hot cache
- [ ] `test_cache_hierarchy_deduplicates_inflight` -- two concurrent fetches for same key
- [ ] `test_cache_hierarchy_semaphore_bounds_concurrency` -- max_concurrent_fetches
- [ ] `test_cache_hierarchy_warm_from_entries` -- bulk warm, verify hits
- [ ] `test_cache_hierarchy_ttl_expiry` -- SourceDirect TTL works
- [ ] `test_cache_hierarchy_ttl_stale_on_source_failure` -- returns stale on error
- [ ] `test_cache_hierarchy_source_error_propagation` -- error bubbles up
- [ ] `test_cache_hierarchy_write_on_eviction` -- hot eviction → disk
- [ ] `test_cache_hierarchy_fetch_returns_bytes` -- (C3) fetch() returns Result<Option<Bytes>>, not Vec<u8>
- [ ] `test_cache_hierarchy_inflight_dedup_returns_bytes` -- (C3) in-flight dedup path returns Bytes from hot cache

### Benchmarks

- [ ] `bench_hybrid_cache_disk_read` -- Target: 10-100μs on NVMe
- [ ] `bench_hybrid_cache_disk_write` -- Target: 50-200μs on NVMe
- [ ] `bench_fetch_pipeline_disk_hit` -- Target: < 200μs
- [ ] `bench_inflight_dedup` -- Target: < 1μs
- [ ] `bench_cache_warm_throughput` -- Target: > 100K entries/sec
- [ ] `bench_ttl_check` -- Target: < 50ns
- [ ] `bench_concurrent_fetches` -- Target: 256 parallel, < 5ms p99

## Rollout Plan

1. **Step 1**: Add `foyer` HybridCache builder setup in `foyer_cache.rs`
2. **Step 2**: Implement `FoyerHybridCacheConfig` with defaults
3. **Step 3**: Implement `LookupCacheHierarchy` struct
4. **Step 4**: Implement `fetch()` method with full pipeline
5. **Step 5**: Implement in-flight deduplication with DashMap
6. **Step 6**: Implement `warm()` for cache warming
7. **Step 7**: Implement TTL support for SourceDirect mode
8. **Step 8**: Unit and integration tests
9. **Step 9**: Benchmarks on NVMe
10. **Step 10**: Code review and merge

## Open Questions

- [ ] Should the HybridCache disk directory be shared across lookup tables or one directory per table? Shared is simpler; per-table allows independent cleanup.
- [ ] Should we implement proactive background re-fetching for TTL entries approaching expiry? This reduces latency spikes but adds source load.
- [ ] How should the disk cache handle node restarts? foyer HybridCache may persist data across restarts. Should we validate cache contents on startup or always start cold?
- [ ] Should the fetch semaphore be global (across all lookup tables) or per-table? Global prevents source overload; per-table allows independent scaling.
- [ ] Should negative lookups (key not found) be cached? Caching negatives prevents repeated source queries for non-existent keys but risks stale negatives when keys are added.

## Completion Checklist

- [ ] `FoyerHybridCacheConfig` struct with defaults
- [ ] `LookupCacheHierarchy` implementation
- [ ] `fetch()` async method with full pipeline
- [ ] In-flight deduplication
- [ ] Cache warming (`warm()` method)
- [ ] TTL support with `TtlCacheEntry`
- [ ] `HierarchyMetrics` struct
- [ ] Unit tests passing
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-004: foyer In-Memory Cache](./F-LOOKUP-004-foyer-memory-cache.md) -- Ring 0 hot cache
- [F-LOOKUP-002: LookupSource Trait](./F-LOOKUP-002-lookup-source-trait.md) -- external source
- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- parent abstraction
- [foyer HybridCache docs](https://docs.rs/foyer/) -- foyer HybridCache API
- [RisingWave caching architecture](https://github.com/risingwavelabs/risingwave) -- production reference
- [dashmap crate](https://docs.rs/dashmap/) -- concurrent HashMap for in-flight tracking
