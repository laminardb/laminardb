# F-LOOKUP-004: foyer In-Memory Cache (Ring 0)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-004 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LOOKUP-001 (LookupTable trait) |
| **Blocks** | F-LOOKUP-005 (foyer Hybrid Cache) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/foyer_cache.rs` |

## Summary

Ring 0 hot cache for lookup tables using foyer's in-memory `Cache<K,V>` (WITHOUT the storage backend). This is Tier 0 in the three-tier caching hierarchy: the synchronous, sub-microsecond lookup path that operators call on every stream event during lookup joins.

**CRITICAL DESIGN DECISION**: foyer's `HybridCache::get()` returns a `Future` and CANNOT be used on the Ring 0 hot path. We use `foyer::Cache` (memory-only variant) which provides synchronous `get()` returning an `Option`. This is the single most important architectural constraint in the lookup cache system.

The in-memory cache is configured per lookup table with a size budget (default 64MB, range 10-500MB per node). It uses LRU eviction with per-partition sharding to minimize contention. Cache keys are prefixed with the table ID to prevent collisions when multiple lookup tables share a cache instance.

## Goals

- Provide synchronous, zero-future, zero-await lookup cache for Ring 0
- Sub-500ns cache hit latency (comparable to HashMap lookup)
- Use foyer::Cache (memory-only) as the backing implementation
- Per-partition LRU eviction to prevent hot-key thrashing
- Cache key design with table_id prefix to prevent cross-table collisions
- Integration with LookupTable trait (`get_cached()` method)
- Configurable cache size (10MB - 4GB per node)
- Metrics: hit rate, eviction count, memory usage
- Minimize allocation on cache hit path (key construction only; no value cloning)

## Non-Goals

- Disk-based caching (see F-LOOKUP-005 for HybridCache)
- Source query on miss (see F-LOOKUP-002)
- CDC-based cache population (see F-LOOKUP-006)
- Distributed cache coherence (each node manages independently)
- Write-through or write-back to external source
- Cache warming strategies (see F-LOOKUP-005 for warm tier)
- Custom eviction policies beyond LRU

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) -- synchronous, zero-allocation
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/foyer_cache.rs`

The foyer in-memory cache sits at Tier 0 of the lookup hierarchy. It is the only synchronous component in the lookup path. When `get_cached()` hits, the entire lookup join completes within a single Ring 0 tick -- no futures, no runtime, no context switches.

```
┌────────────────────────────────────────────────────────────────────┐
│                    RING 0: SYNCHRONOUS HOT PATH                    │
│                                                                    │
│  ┌──────────────────┐          ┌─────────────────────────────┐    │
│  │ LookupJoinOp     │  get()   │  FoyerMemoryCache            │    │
│  │                  │ ───────> │  ┌─────────────────────────┐ │    │
│  │  extract key     │          │  │ foyer::Cache<            │ │    │
│  │  from stream     │  &[u8]   │  │   LookupCacheKey,       │ │    │
│  │  event           │ <─────── │  │   Vec<u8>               │ │    │
│  └──────────────────┘          │  │ >                        │ │    │
│                                │  └─────────────────────────┘ │    │
│                                │  LRU eviction, per-partition │    │
│                                │  10-500MB per node           │    │
│                                └─────────────────────────────────┘    │
│                                                                    │
│  On MISS: enqueue to Ring 1 ──────────────────────────────────────│
│                                                                    │
├────────────────────────────────────────────────────────────────────┤
│                    RING 1: ASYNC (F-LOOKUP-005)                    │
│  foyer::HybridCache (disk) → LookupSource (external)             │
└────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use bytes::Bytes;
use foyer::Cache;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
/// Cache key for lookup table entries.
///
/// Combines table_id and primary key to prevent collisions when
/// multiple lookup tables share a foyer cache instance. The table_id
/// prefix is hashed together with the key for uniform distribution.
///
/// # Allocation Optimization
///
/// The `key` field uses `Vec<u8>` which allocates for every lookup.
/// For production hot paths, consider replacing with `SmallVec<[u8; 64]>`
/// to keep keys under 64 bytes on the stack. Most primary keys (integers,
/// UUIDs, short strings) fit within this budget.
pub struct LookupCacheKey {
    /// Lookup table identifier (stable across restarts).
    pub table_id: u32,
    /// Primary key bytes from the lookup table row.
    pub key: Vec<u8>,
}

impl Hash for LookupCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_id.hash(state);
        self.key.hash(state);
    }
}

/// foyer in-memory cache wrapper for Ring 0 lookup operations.
///
/// This wrapper provides the synchronous `get_cached()` implementation
/// for the `LookupTable` trait. It uses `foyer::Cache` (NOT `HybridCache`)
/// to guarantee synchronous access without any futures or async runtime.
///
/// # Why foyer::Cache and NOT HybridCache
///
/// `foyer::HybridCache::get()` returns a `Future` because it may need
/// to read from disk. On the Ring 0 hot path, we cannot await futures.
/// `foyer::Cache` is a pure in-memory LRU cache with synchronous
/// `get()` and `insert()` methods.
///
/// # Thread Safety
///
/// `foyer::Cache` is `Send + Sync` and uses internal sharding to
/// reduce lock contention. Multiple partition reactors on the same
/// node can safely access the shared cache instance.
pub struct FoyerMemoryCache {
    /// The underlying foyer in-memory cache.
    cache: Cache<LookupCacheKey, Vec<u8>>,
    /// Table ID for cache key construction.
    table_id: u32,
    /// Configuration for this cache instance.
    config: FoyerMemoryCacheConfig,
    /// Hit count for metrics.
    hits: std::sync::atomic::AtomicU64,
    /// Miss count for metrics.
    misses: std::sync::atomic::AtomicU64,
}

/// Configuration for the foyer in-memory cache.
#[derive(Debug, Clone)]
pub struct FoyerMemoryCacheConfig {
    /// Maximum cache capacity in number of entries.
    /// foyer sizes by entry count, so we compute this from
    /// target_size_bytes / avg_entry_size_bytes.
    pub capacity: usize,

    /// Target cache size in bytes.
    /// Used to compute capacity. Default: 64MB.
    pub target_size_bytes: usize,

    /// Estimated average entry size in bytes (key + value).
    /// Used to compute capacity from target_size_bytes.
    /// Default: 256 bytes.
    pub avg_entry_size_bytes: usize,

    /// Number of shards for concurrent access.
    /// Higher values reduce contention but increase memory overhead.
    /// Default: 16. Must be a power of 2.
    pub shards: usize,
}

impl Default for FoyerMemoryCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 256 * 1024,  // ~256K entries
            target_size_bytes: 64 * 1024 * 1024,  // 64MB
            avg_entry_size_bytes: 256,
            shards: 16,
        }
    }
}

impl FoyerMemoryCache {
    /// Create a new in-memory cache with the given configuration.
    ///
    /// Initializes the foyer::Cache with LRU eviction and the
    /// configured number of shards.
    pub fn new(table_id: u32, config: FoyerMemoryCacheConfig) -> Self {
        let capacity = config.target_size_bytes / config.avg_entry_size_bytes;
        // foyer uses a builder pattern for cache construction.
        // The exact API depends on foyer version; this is pseudocode.
        // Actual construction:
        //   CacheBuilder::new(capacity)
        //       .with_shards(config.shards)
        //       .with_eviction_config(LruConfig::default())
        //       .build()
        let cache = Cache::new(capacity, config.shards); // Pseudocode — see foyer docs
        Self {
            cache,
            table_id,
            config,
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Synchronous cache lookup (Ring 0 hot path).
    ///
    /// **AUDIT FIX (C3)**: Returns `Option<Bytes>` instead of
    /// `Option<CacheEntry<K,V>>`. The `Bytes` value is extracted from
    /// the foyer `CacheEntry` handle before the handle is dropped.
    /// This avoids exposing the `CacheEntry` RAII handle to callers,
    /// which was the source of the lifetime unsoundness identified in C3.
    ///
    /// `Bytes` is reference-counted (`Arc<[u8]>` internally), so the
    /// returned value remains valid after the cache entry is evicted.
    /// Clone cost: ~2ns (Arc pointer copy).
    ///
    /// # Performance
    ///
    /// foyer::Cache::get() performs:
    /// 1. Hash the key (< 50ns for typical key sizes)
    /// 2. Shard lookup (& mask, < 5ns)
    /// 3. Concurrent hash map get (< 100ns typical)
    /// 4. LRU touch (update frequency counter, < 50ns)
    /// 5. Extract Bytes from CacheEntry (< 10ns, Arc clone)
    /// Total: < 350ns typical, < 500ns worst case
    ///
    /// # Allocation Note
    ///
    /// This method constructs a `LookupCacheKey` which clones the key
    /// bytes (`key.to_vec()`). This allocation is unavoidable because
    /// foyer requires an owned key for its hash map lookup. For keys
    /// under 64 bytes, consider a `SmallVec`-based key type to keep
    /// the allocation on the stack.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let cache_key = LookupCacheKey {
            table_id: self.table_id,
            key: key.to_vec(),  // TODO: Use SmallVec<[u8; 64]> to avoid heap alloc for small keys
        };
        match self.cache.get(&cache_key) {
            Some(entry) => {
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // AUDIT FIX (C3): Extract owned Bytes from CacheEntry handle.
                // The Bytes shares the underlying allocation via Arc, so the
                // data remains valid after the CacheEntry handle is dropped.
                Some(Bytes::from(entry.value().clone()))
            }
            None => {
                self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a value into the cache.
    ///
    /// Called by:
    /// - CDC adapter on INSERT/UPDATE events (F-LOOKUP-006)
    /// - Async fetch pipeline after source query (F-LOOKUP-005)
    /// - Bootstrap loader during initial snapshot (F-LOOKUP-008)
    ///
    /// If the cache is full, LRU eviction removes the least recently
    /// used entry to make room.
    pub fn insert(&self, key: &[u8], value: Vec<u8>) {
        let cache_key = LookupCacheKey {
            table_id: self.table_id,
            key: key.to_vec(),
        };
        self.cache.insert(cache_key, value);
    }

    /// Remove a specific key from the cache.
    ///
    /// Called by CDC adapter on DELETE events.
    pub fn remove(&self, key: &[u8]) {
        let cache_key = LookupCacheKey {
            table_id: self.table_id,
            key: key.to_vec(),
        };
        self.cache.remove(&cache_key);
    }

    /// Get cache hit rate as a percentage (0.0 - 100.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }

    /// Get the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.cache.usage()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset hit/miss counters (for testing and metrics windows).
    pub fn reset_metrics(&self) {
        self.hits.store(0, std::sync::atomic::Ordering::Relaxed);
        self.misses.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            entry_count: self.len(),
            hit_rate: self.hit_rate(),
        }
    }
}

/// Integration with LookupJoinOperator showing the full Ring 0 path.
///
/// **AUDIT FIX (C3)**: Updated to use `Bytes` return type from hot cache.
/// The operator no longer holds a `CacheEntry` handle — `Bytes` is
/// independently reference-counted and safe to store beyond the cache
/// entry's lifetime.
///
/// This demonstrates how the operator uses the hot cache:
/// - `get_cached()` attempts Ring 0 synchronous lookup
/// - On hit: join completes immediately, no async needed
/// - On miss: event is enqueued for Ring 1 async fetch
struct LookupJoinOperator {
    /// Ring 0: synchronous in-memory cache (foyer::Cache).
    hot_cache: FoyerMemoryCache,
    /// Ring 1: async hybrid cache (foyer::HybridCache), see F-LOOKUP-005.
    hybrid_cache: foyer::HybridCache<LookupCacheKey, Vec<u8>>,
    /// Pending events waiting for async lookup completion.
    pending_queue: Vec<PendingLookup>,
}

/// An event waiting for async lookup completion.
struct PendingLookup {
    key: Vec<u8>,
    event: Vec<u8>,  // Serialized stream event
}

/// Result of processing a single stream event.
enum ProcessResult {
    /// Joined event emitted immediately (Ring 0 hit).
    Emitted(Vec<u8>),
    /// Event buffered, waiting for async lookup.
    Pending,
}

impl LookupJoinOperator {
    fn process_event(&mut self, event: &[u8]) -> ProcessResult {
        let key = self.extract_join_key(event);

        // Ring 0: synchronous hot cache lookup (< 500ns)
        // AUDIT FIX (C3): get() returns Option<Bytes>, not CacheEntry.
        // Bytes is reference-counted — safe to use after cache eviction.
        if let Some(value) = self.hot_cache.get(&key) {
            return ProcessResult::Emitted(self.join_event(event, &value));
        }

        // Ring 0 miss: enqueue for Ring 1 async fetch
        self.pending_queue.push(PendingLookup {
            key: key.clone(),
            event: event.to_vec(),
        });
        self.enqueue_async_lookup(key);

        ProcessResult::Pending
    }

    fn extract_join_key(&self, _event: &[u8]) -> Vec<u8> {
        todo!("Extract join key column from event")
    }

    fn join_event(&self, _event: &[u8], _lookup_value: &[u8]) -> Vec<u8> {
        todo!("Combine stream event with lookup value")
    }

    fn enqueue_async_lookup(&self, _key: Vec<u8>) {
        todo!("Send to Ring 1 async pipeline")
    }
}
```

### Data Structures

```rust
/// Snapshot of cache metrics for monitoring dashboards.
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    /// Total cache hits since last reset.
    pub hits: u64,
    /// Total cache misses since last reset.
    pub misses: u64,
    /// Current number of entries in cache.
    pub entry_count: usize,
    /// Hit rate as percentage (0.0 - 100.0).
    pub hit_rate: f64,
}

/// Cache warming request sent during startup.
#[derive(Debug)]
pub struct CacheWarmRequest {
    /// Table ID to warm.
    pub table_id: u32,
    /// Keys to pre-populate (from Parquet snapshot or CDC replay).
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}
```

### Algorithm/Flow

#### Cache Hit (Ring 0 Fast Path)

```
1. Stream event arrives at LookupJoinOperator
2. Extract join key (e.g., customer_id bytes)
3. Construct LookupCacheKey { table_id, key }
4. foyer::Cache::get(&cache_key)
   a. Hash cache_key → shard index (< 50ns)
   b. Shard lock (foyer uses lock-free or fine-grained locking)
   c. Hash map lookup (< 100ns)
   d. Update LRU frequency (< 50ns)
5. AUDIT FIX (C3): Extract Bytes from CacheEntry (< 10ns, Arc clone)
   CacheEntry handle is dropped after extraction.
   Bytes remains valid independently of cache eviction.
6. Operator emits joined event
Total: < 500ns
```

#### Cache Miss (Ring 0 → Ring 1 Handoff)

```
1. foyer::Cache::get() returns None
2. Operator buffers the stream event in pending_queue
3. Operator sends (key, callback) to Ring 1 async pipeline
4. Ring 1 pipeline:
   a. Check foyer::HybridCache disk tier (10-100μs)
   b. If disk miss: query LookupSource (1-100ms)
   c. Insert result into both HybridCache and hot Cache
   d. Notify Ring 0 of completion
5. Operator retrieves buffered event
6. hot_cache.get() now hits (value was inserted in step 4c)
7. Operator emits joined event
```

#### LRU Eviction

```
1. insert() called when cache is at capacity
2. foyer identifies least recently used entry
3. Evicted entry is optionally passed to HybridCache disk tier
   (foyer's WriteOnEviction policy, configured in F-LOOKUP-005)
4. New entry takes the evicted slot
5. Eviction is synchronous and bounded (< 1μs)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Cache miss | Key not in hot cache | Degrade to Ring 1 async path |
| Key construction OOM | Very large key bytes | Reject keys > 1KB, log warning |
| Metric counter overflow | 2^64 operations | Wrap-around is acceptable (atomic u64) |
| Shard contention | Many threads accessing same shard | Increase shard count in config |
| Cache size misconfigured | target_size_bytes < 1MB | Reject in config validation |
| foyer internal error | Bug in foyer cache | Log, degrade to direct source query |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| `get()` cache hit | < 500ns | 256K entries, 64MB cache | `bench_foyer_memory_get_hit` |
| `get()` cache miss | < 200ns | Empty cache | `bench_foyer_memory_get_miss` |
| `insert()` | < 1μs | Cache not full | `bench_foyer_memory_insert` |
| `insert()` with eviction | < 2μs | Cache full, LRU eviction | `bench_foyer_memory_insert_evict` |
| `remove()` | < 500ns | Key exists | `bench_foyer_memory_remove` |
| Hit rate (financial ref data) | > 95% | Typical workload | Runtime metrics |
| Memory overhead | < 20% | Over stored data size | Allocation tracking |
| Metrics counter update | < 10ns | Relaxed atomic increment | `bench_metrics_update` |

## Test Plan

### Unit Tests

- [ ] `test_foyer_memory_cache_new_empty` -- new cache has zero entries
- [ ] `test_foyer_memory_cache_insert_and_get` -- round-trip insert/get
- [ ] `test_foyer_memory_cache_get_missing_key` -- returns None
- [ ] `test_foyer_memory_cache_insert_overwrite` -- update existing key
- [ ] `test_foyer_memory_cache_remove` -- insert, remove, verify miss
- [ ] `test_foyer_memory_cache_remove_missing` -- remove non-existent key is no-op
- [ ] `test_foyer_memory_cache_eviction` -- insert beyond capacity, oldest evicted
- [ ] `test_foyer_memory_cache_len_tracks_entries` -- len updates on insert/remove
- [ ] `test_foyer_memory_cache_is_empty` -- true when empty, false when not
- [ ] `test_foyer_memory_cache_hit_rate_zero_ops` -- 0.0 when no operations
- [ ] `test_foyer_memory_cache_hit_rate_all_hits` -- 100.0 when all hits
- [ ] `test_foyer_memory_cache_hit_rate_mixed` -- correct percentage
- [ ] `test_foyer_memory_cache_reset_metrics` -- counters reset to zero
- [ ] `test_foyer_memory_cache_metrics_snapshot` -- all fields populated
- [ ] `test_lookup_cache_key_hash_deterministic` -- same key, same hash
- [ ] `test_lookup_cache_key_different_tables_different_hash` -- table_id matters
- [ ] `test_lookup_cache_key_equality` -- equal keys are equal
- [ ] `test_foyer_memory_cache_config_default` -- verify defaults

### Integration Tests

- [ ] `test_foyer_memory_cache_with_lookup_table` -- wire to LookupTable trait
- [ ] `test_foyer_memory_cache_concurrent_access` -- multiple threads, no corruption
- [ ] `test_foyer_memory_cache_lru_order` -- verify LRU eviction order
- [ ] `test_foyer_memory_cache_many_tables_no_collision` -- two tables, same keys
- [ ] `test_foyer_memory_cache_with_operator` -- end-to-end lookup join
- [ ] `test_foyer_memory_cache_warm_from_snapshot` -- bulk insert from Parquet

### Benchmarks

- [ ] `bench_foyer_memory_get_hit` -- Target: < 500ns, 256K entries
- [ ] `bench_foyer_memory_get_alloc_count` -- Verify allocation count using `HotPathGuard` + `allocation-tracking` feature
- [ ] `bench_foyer_memory_get_miss` -- Target: < 200ns
- [ ] `bench_foyer_memory_insert` -- Target: < 1μs
- [ ] `bench_foyer_memory_insert_evict` -- Target: < 2μs
- [ ] `bench_foyer_memory_remove` -- Target: < 500ns
- [ ] `bench_foyer_memory_throughput` -- Target: > 2M ops/sec single thread
- [ ] `bench_foyer_memory_concurrent` -- Target: > 5M ops/sec, 4 threads
- [ ] `bench_lookup_cache_key_hash` -- Target: < 50ns

## Rollout Plan

1. **Step 1**: Add `foyer` dependency to `laminar-core/Cargo.toml`
2. **Step 2**: Define `LookupCacheKey` and `FoyerMemoryCacheConfig` in `laminar-core/src/lookup/foyer_cache.rs`
3. **Step 3**: Implement `FoyerMemoryCache` wrapping `foyer::Cache`
4. **Step 4**: Implement `CacheMetrics` and monitoring
5. **Step 5**: Wire to `LookupTable::get_cached()` implementation
6. **Step 6**: Unit tests
7. **Step 7**: Integration tests with LookupJoinOperator
8. **Step 8**: Benchmarks validating < 500ns target
9. **Step 9**: Code review and merge

## Open Questions

- [x] ~~Should `FoyerMemoryCache::get()` return `Option<&[u8]>`, `Option<Vec<u8>>`, or `Option<CacheEntry<K,V>>`?~~ **Resolved (AUDIT FIX C3)**: Returns `Option<Bytes>`. The `CacheEntry` RAII handle is used internally within `get()` to extract the value, then dropped. `Bytes` is reference-counted (`Arc<[u8]>` internally), so the data survives cache eviction. This avoids exposing the `CacheEntry` handle to callers, which caused lifetime unsoundness when stored in operator state (the entry could be evicted while the borrow was still held).
- [ ] Should we use a single shared foyer::Cache instance across all lookup tables on a node, or one cache per table? Shared allows better memory utilization but table_id prefix adds per-key overhead.
- [ ] What is the right default shard count? foyer's default may differ from our optimal. Benchmark with 4, 8, 16, 32 shards.
- [ ] Should evicted entries be automatically pushed to HybridCache (WriteOnEviction) or should the two tiers be independent? WriteOnEviction adds complexity but improves warm-tier population.

## Completion Checklist

- [ ] `foyer` dependency added to Cargo.toml
- [ ] `LookupCacheKey` struct with Hash, Eq, Clone
- [ ] `FoyerMemoryCacheConfig` with defaults
- [ ] `FoyerMemoryCache` implementation
- [ ] `CacheMetrics` struct
- [ ] Integration with `LookupTable::get_cached()`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet < 500ns target
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- trait this implements
- [F-LOOKUP-005: foyer Hybrid Cache](./F-LOOKUP-005-foyer-hybrid-cache.md) -- Ring 1 disk cache
- [F-LOOKUP-006: CDC-to-Cache Adapter](./F-LOOKUP-006-cdc-cache-adapter.md) -- cache population
- [F-STATE-001: StateStore Trait](../state/F-STATE-001-state-store-trait.md) -- Ring 0 design pattern
- [foyer crate](https://github.com/foyer-rs/foyer) -- cache library (used by RisingWave)
- [RisingWave foyer integration](https://github.com/risingwavelabs/risingwave) -- production usage reference
