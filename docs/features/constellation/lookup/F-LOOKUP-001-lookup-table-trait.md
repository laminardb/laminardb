# F-LOOKUP-001: LookupTable Trait & Strategy Enum

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 (StateStore trait) |
| **Blocks** | F-LOOKUP-002, F-LOOKUP-003, F-LOOKUP-004, F-LOOKUP-005, F-LOOKUP-006, F-LOOKUP-007, F-LOOKUP-008, F-LOOKUP-009 |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/mod.rs` |

## Summary

Core abstraction for lookup tables in LaminarDB's Constellation Architecture. **Evolution from Phase 3**: The existing codebase has two lookup abstractions: `TableLoader` trait (`laminar_connectors::lookup`) providing async `lookup(&[u8]) -> LookupResult` with Arrow `RecordBatch` results, and `ReferenceTableSource` trait (`laminar_connectors::reference`) for CDC/snapshot-based table loading. There is also an existing `LookupJoinOperator` (`laminar_core::operator::lookup_join`) that caches results in `StateStore` using prefix `b"lkc:"`. The `LookupTable` trait unifies and replaces these abstractions with a tiered caching model (Ring 0 hot cache + Ring 1 async fallback), while the existing `LookupJoinOperator` is migrated to use the new trait. Lookup tables enable stream-to-static joins: enriching streaming events (e.g., trades) with reference data (e.g., customer metadata, instrument details). The `LookupTable` trait provides a unified interface that hides the deployment topology from operator code. An operator calls `lookup.get(key)` and never knows whether it is running single-process or across a 5-node constellation. The difference between modes only shows up in the `LookupStrategy` configuration.

Three strategies govern how lookup data is distributed:

- **Replicated**: Full copy of the lookup table on every node. Best for small-to-medium reference tables (< 1GB). Sub-microsecond reads, higher memory usage.
- **Partitioned**: Hash-partitioned by primary key across constellation nodes. Saves memory, adds network latency for remote keys. Only meaningful in constellation mode; degrades to replicated in embedded.
- **SourceDirect**: No materialization. Every cache miss queries the external source directly. Best for rarely-accessed large tables or when freshness is paramount.

## Goals

- Define the `LookupTable` trait as the single abstraction all operators use for lookup access
- Define `LookupStrategy` enum covering replicated, partitioned, and source-direct modes
- Provide `LookupTableConfig` for declarative configuration of lookup behavior
- Provide `LookupTableBuilder` for constructing lookup table instances from config
- Ensure `get()` on Ring 0 (hot cache hit) is synchronous and sub-microsecond
- Ensure `get_cached()` provides explicit Ring 0 synchronous-only access
- Support transparent fallback: Ring 0 cache miss enqueues async Ring 1 lookup
- Keep the trait topology-agnostic: embedded and constellation use identical operator code

## Non-Goals

- Implementing specific cache backends (see F-LOOKUP-004, F-LOOKUP-005)
- Implementing specific source connectors (see F-LOOKUP-007, F-LOOKUP-008)
- CDC integration (see F-LOOKUP-006)
- SQL DDL parsing for `CREATE LOOKUP TABLE` (see F-LSQL-003)
- Cross-partition join coordination (see F-LOOKUP-009 for partitioned strategy)
- Schema evolution or versioning of lookup table data
- Transaction semantics across lookup and state stores

## Technical Design

### Architecture

**Ring**: Ring 0 (hot cache hit) / Ring 1 (cache miss, async fetch)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/mod.rs`

The `LookupTable` trait is the lookup-side counterpart of `StateStore`. While `StateStore` manages mutable operator state, `LookupTable` manages read-only reference data accessed during stream processing. The key architectural decision is the two-tier access pattern:

1. **`get_cached(key)` -- Ring 0**: Synchronous lookup against the foyer in-memory cache. Returns immediately with `Some(value)` on hit or `None` on miss. Zero allocation. This is what operators call on the hot path.

2. **`get(key)` -- Ring 0 + Ring 1**: Tries the hot cache first, then falls back to async lookup (foyer HybridCache disk tier, then external source). Returns a `LookupResult` indicating whether the value was found synchronously or requires async completion.

```
┌────────────────────────────────────────────────────────────────────────┐
│                         RING 0: HOT PATH                               │
│                                                                        │
│  ┌──────────────┐    get_cached(key)    ┌───────────────────────────┐  │
│  │   Operator   │ ───────────────────> │  LookupTable              │  │
│  │  (Lookup     │    Option<&[u8]>      │  ┌─────────────────────┐  │  │
│  │   Join)      │ <─────────────────── │  │ foyer::Cache (Tier0)│  │  │
│  └──────┬───────┘                       │  └─────────────────────┘  │  │
│         │                               └──────────────┬────────────┘  │
│         │ get(key) -> LookupResult                     │               │
├─────────┼──────────────────────────────────────────────┼───────────────┤
│         │              RING 1: ASYNC FETCH              │               │
│         ▼                                              ▼               │
│  ┌──────────────┐                       ┌───────────────────────────┐  │
│  │  Pending     │                       │  foyer::HybridCache       │  │
│  │  Queue       │ ──────────────────>  │  (Tier 1: disk)           │  │
│  └──────────────┘                       └──────────────┬────────────┘  │
│                                                        │               │
│                                         ┌──────────────▼────────────┐  │
│                                         │  LookupSource (Tier 2)    │  │
│                                         │  (Postgres / Parquet / …) │  │
│                                         └───────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use bytes::Bytes;
use std::sync::Arc;

/// Result of a lookup operation that may require async completion.
///
/// When the hot cache (Ring 0) has the value, `Hit` is returned
/// immediately. When the value is not cached, `Pending` indicates
/// that an async fetch has been enqueued and the operator should
/// buffer the event for later re-processing.
///
/// **AUDIT FIX (C3)**: `Hit` now carries `Bytes` (reference-counted)
/// instead of `&'a [u8]`. This avoids lifetime unsoundness with
/// cache-backed stores where the entry may be evicted after the
/// borrow is returned. `Bytes` clone is ~2ns (Arc pointer copy).
#[derive(Debug)]
pub enum LookupResult {
    /// Value found in Ring 0 hot cache (synchronous, sub-μs).
    /// `Bytes` is reference-counted — no data copy on clone.
    Hit(Bytes),
    /// Value not cached. Async fetch enqueued. The operator should
    /// buffer the triggering event and retry after the fetch completes.
    Pending,
    /// Key does not exist in the lookup table (confirmed by source).
    NotFound,
}

/// Strategy for how lookup table data is distributed across nodes.
///
/// This enum controls the materialization and routing behavior.
/// Operator code is identical regardless of strategy -- the difference
/// is in how the `LookupTable` implementation distributes data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LookupStrategy {
    /// Full copy of the lookup table on every node.
    ///
    /// Best for small-to-medium reference tables (< 1GB).
    /// Each node independently subscribes to the source (CDC or snapshot).
    /// Sub-microsecond reads guaranteed (always local).
    Replicated,

    /// Hash-partitioned by primary key across constellation nodes.
    ///
    /// Each node owns a shard of the lookup table. Lookups for keys
    /// owned by other nodes require a gRPC call. Saves memory at the
    /// cost of network latency for remote keys. In embedded mode,
    /// degrades to Replicated (single node owns all partitions).
    Partitioned,

    /// No materialization. Cache misses query the external source directly.
    ///
    /// Best for very large tables where materializing the full dataset
    /// is impractical, or when data freshness is paramount. Higher
    /// latency on cache miss (1-10ms for Postgres, 10-100ms for S3).
    SourceDirect,
}

/// Core trait for lookup table access.
///
/// This is the abstraction all operators use for stream-to-static joins.
/// Implementations handle caching, source queries, and distribution
/// strategy transparently. Operator code calls `get()` or `get_cached()`
/// and is agnostic to whether the system is embedded or distributed.
///
/// **AUDIT FIX (C3)**: All value-returning methods use `Bytes` (reference-counted)
/// instead of `&[u8]`. This eliminates lifetime unsoundness with cache-backed
/// stores where `foyer::CacheEntry` RAII handles pin entries during access —
/// a returned `&[u8]` could dangle after the entry is evicted. `Bytes` shares
/// the underlying allocation via `Arc`, so values remain valid after the
/// cache entry handle is dropped. Clone cost: ~2ns (Arc pointer copy).
///
/// # Thread Safety
///
/// `LookupTable` is `Send + Sync` because the underlying foyer caches
/// are thread-safe. However, in Ring 0, operators access the table through
/// a borrowed reference within a single partition reactor. Cross-partition
/// access is mediated by the partitioned strategy (F-LOOKUP-009).
///
/// # Performance
///
/// | Access Pattern | Latency | Ring |
/// |---------------|---------|------|
/// | `get_cached()` hit | < 500ns | Ring 0 |
/// | `get()` hot hit | < 500ns | Ring 0 |
/// | `get()` disk hit | 10-100μs | Ring 1 |
/// | `get()` source fetch | 1-10ms | Ring 1 |
pub trait LookupTable: Send + Sync {
    /// Synchronous lookup against Ring 0 hot cache only.
    ///
    /// Returns `Some(value)` if the key is in the foyer in-memory cache,
    /// `None` otherwise. This method NEVER triggers an async fetch.
    /// Suitable for Ring 0 hot path.
    ///
    /// **AUDIT FIX (C3)**: Returns `Option<Bytes>` instead of `Option<&[u8]>`.
    /// The `Bytes` is extracted from the `foyer::CacheEntry` handle before
    /// the handle is dropped, keeping the data alive independently.
    ///
    /// Operators use this when they can tolerate cache misses (e.g.,
    /// optional enrichment fields). For mandatory joins, use `get()`.
    fn get_cached(&self, key: &[u8]) -> Option<Bytes>;

    /// Lookup with automatic async fallback.
    ///
    /// First checks Ring 0 hot cache. On miss, enqueues an async fetch
    /// through Ring 1 (foyer HybridCache disk tier, then external source).
    /// Returns `LookupResult::Pending` if an async fetch was enqueued.
    ///
    /// **AUDIT FIX (C3)**: Returns `LookupResult` (no lifetime parameter)
    /// with `Hit(Bytes)` instead of `Hit(&[u8])`.
    ///
    /// The operator should buffer the event and retry when notified of
    /// fetch completion. This is the primary method for mandatory joins.
    fn get(&self, key: &[u8]) -> LookupResult;

    /// Returns the distribution strategy for this lookup table.
    fn strategy(&self) -> LookupStrategy;

    /// Returns the unique identifier for this lookup table.
    fn table_id(&self) -> &str;

    /// Returns the number of entries currently cached (all tiers).
    fn cached_entry_count(&self) -> usize;

    /// Returns approximate memory usage of the hot cache in bytes.
    fn hot_cache_size_bytes(&self) -> usize;

    /// Invalidate a specific key across all cache tiers.
    ///
    /// Used by CDC adapter when a DELETE event arrives.
    fn invalidate(&self, key: &[u8]);

    /// Insert or update a value in the hot cache (Ring 0).
    ///
    /// Used by CDC adapter for INSERT/UPDATE events and by
    /// the async fetch pipeline to populate the cache after a
    /// source query completes.
    fn insert(&self, key: &[u8], value: &[u8]);
}
```

### Data Structures

```rust
use std::time::Duration;

/// Configuration for a lookup table instance.
///
/// Populated from SQL DDL (`CREATE LOOKUP TABLE ... WITH (...)`)
/// or from programmatic API. Validated by `LookupTableBuilder`.
#[derive(Debug, Clone)]
pub struct LookupTableConfig {
    /// Unique identifier for this lookup table.
    pub table_id: String,

    /// Distribution strategy.
    pub strategy: LookupStrategy,

    /// Maximum size of the Ring 0 hot cache in bytes.
    /// Default: 64MB. Range: 1MB - 4GB.
    pub hot_cache_size_bytes: usize,

    /// Maximum size of the Ring 1 disk cache in bytes.
    /// Default: 1GB. Range: 0 (disabled) - available disk.
    pub disk_cache_size_bytes: usize,

    /// Time-to-live for cached entries (SourceDirect mode).
    /// Entries older than TTL are evicted on next access.
    /// Default: None (no expiry for Replicated/Partitioned).
    pub ttl: Option<Duration>,

    /// Primary key column(s) used for cache key construction.
    pub primary_key_columns: Vec<String>,

    /// Source connector identifier (e.g., "postgres", "parquet").
    pub source_type: String,

    /// Source-specific configuration (connection string, table name, etc.).
    pub source_config: std::collections::HashMap<String, String>,

    /// Number of partitions for the lookup table hash ring.
    /// Only used with Partitioned strategy. Default: node count.
    pub partition_count: Option<u32>,

    /// Whether to prefetch the entire table on startup.
    /// Default: true for Replicated, false for SourceDirect.
    pub prefetch_on_startup: bool,

    /// Maximum number of concurrent async fetches.
    /// Bounds memory for pending lookups. Default: 1024.
    pub max_pending_fetches: usize,
}

impl Default for LookupTableConfig {
    fn default() -> Self {
        Self {
            table_id: String::new(),
            strategy: LookupStrategy::Replicated,
            hot_cache_size_bytes: 64 * 1024 * 1024,      // 64MB
            disk_cache_size_bytes: 1024 * 1024 * 1024,    // 1GB
            ttl: None,
            primary_key_columns: Vec::new(),
            source_type: String::new(),
            source_config: std::collections::HashMap::new(),
            partition_count: None,
            prefetch_on_startup: true,
            max_pending_fetches: 1024,
        }
    }
}

/// Errors that can occur in lookup table operations.
#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    /// Key not found in any tier after full lookup chain.
    #[error("Key not found in lookup table '{table_id}'")]
    KeyNotFound { table_id: String },

    /// Source query failed (network, timeout, auth).
    #[error("Source query failed for table '{table_id}': {source}")]
    SourceError {
        table_id: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Cache capacity exceeded, cannot insert.
    #[error("Cache capacity exceeded for table '{table_id}'")]
    CacheCapacityExceeded { table_id: String },

    /// Configuration validation error.
    #[error("Invalid lookup table config: {reason}")]
    ConfigError { reason: String },

    /// Serialization/deserialization error for cache values.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Partition routing error (partitioned strategy).
    #[error("Partition routing failed for key: {reason}")]
    PartitionRoutingError { reason: String },

    /// Timeout waiting for async fetch to complete.
    #[error("Lookup timeout after {elapsed:?} for table '{table_id}'")]
    Timeout {
        table_id: String,
        elapsed: Duration,
    },

    /// Internal error (bug).
    #[error("Internal lookup error: {0}")]
    Internal(String),
}

/// Builder for constructing LookupTable instances from configuration.
///
/// Validates config, initializes caches, connects to sources, and
/// returns a boxed `LookupTable` implementation based on the strategy.
pub struct LookupTableBuilder {
    config: LookupTableConfig,
}

impl LookupTableBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(config: LookupTableConfig) -> Self {
        Self { config }
    }

    /// Validate the configuration.
    ///
    /// Checks:
    /// - table_id is non-empty
    /// - hot_cache_size_bytes >= 1MB
    /// - primary_key_columns is non-empty
    /// - source_type is recognized
    /// - partition_count > 0 if specified
    pub fn validate(&self) -> Result<(), LookupError> {
        if self.config.table_id.is_empty() {
            return Err(LookupError::ConfigError {
                reason: "table_id must not be empty".into(),
            });
        }
        if self.config.hot_cache_size_bytes < 1024 * 1024 {
            return Err(LookupError::ConfigError {
                reason: "hot_cache_size_bytes must be >= 1MB".into(),
            });
        }
        if self.config.primary_key_columns.is_empty() {
            return Err(LookupError::ConfigError {
                reason: "primary_key_columns must not be empty".into(),
            });
        }
        if let Some(count) = self.config.partition_count {
            if count == 0 {
                return Err(LookupError::ConfigError {
                    reason: "partition_count must be > 0".into(),
                });
            }
        }
        Ok(())
    }

    /// Build the lookup table instance.
    ///
    /// Returns the appropriate implementation based on the strategy:
    /// - Replicated -> ReplicatedLookupTable (F-LOOKUP-006)
    /// - Partitioned -> PartitionedLookupTable (F-LOOKUP-009)
    /// - SourceDirect -> SourceDirectLookupTable (uses F-LOOKUP-007/008)
    pub async fn build(self) -> Result<Arc<dyn LookupTable>, LookupError> {
        self.validate()?;
        // Delegate to strategy-specific constructors.
        // Implementation provided by downstream features.
        todo!("Strategy-specific construction in F-LOOKUP-004..009")
    }
}
```

### Algorithm/Flow

#### Operator Lookup Flow (Strategy-Agnostic)

```
1. Stream event arrives at LookupJoinOperator
2. Operator extracts join key from event (e.g., customer_id)
3. Operator calls lookup.get_cached(&key)
   - If Some(bytes): emit joined event immediately (Ring 0, < 500ns)
     (AUDIT FIX C3: `bytes` is `Bytes`, cheap to clone via Arc)
   - If None: call lookup.get(&key)
     - If LookupResult::Hit(bytes): emit joined event (was in disk cache)
     - If LookupResult::Pending: buffer event, wait for async notification
     - If LookupResult::NotFound: emit event with NULL enrichment fields
4. On async fetch completion notification:
   - Retrieve buffered event
   - Value is now in hot cache
   - Call lookup.get_cached(&key) -> guaranteed hit (returns Bytes)
   - Emit joined event
```

#### Strategy Selection Decision Tree

```
Is the lookup table < 1GB?
├── Yes: Use Replicated
│   - Sub-μs reads, no network latency
│   - Each node gets full copy via CDC or snapshot
│   - Memory cost = table_size × node_count
└── No:
    Is low-latency critical (< 1μs)?
    ├── Yes: Use Partitioned
    │   - Local keys: sub-μs, remote keys: 1-5ms
    │   - Memory cost = table_size (distributed)
    │   - Requires constellation mode
    └── No: Use SourceDirect
        - Cache hits: sub-μs, misses: 1-100ms
        - Memory cost = cache_size only
        - No materialization overhead
```

#### Cache Key Construction

```
Cache key = table_id_bytes ++ primary_key_bytes

Example for table "customers", key customer_id=42:
  key = b"customers\x00" ++ customer_id.to_be_bytes()

The table_id prefix prevents collisions when multiple lookup tables
share a foyer cache instance.
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `LookupError::KeyNotFound` | Key genuinely absent from source | Operator emits event with NULL join fields |
| `LookupError::SourceError` | Network failure, source down | Retry with exponential backoff, circuit breaker |
| `LookupError::CacheCapacityExceeded` | Hot cache full, eviction insufficient | Increase cache size or switch to SourceDirect |
| `LookupError::ConfigError` | Invalid builder configuration | Fail fast at pipeline startup, user fixes config |
| `LookupError::SerializationError` | Corrupt cache entry | Evict entry, re-fetch from source |
| `LookupError::PartitionRoutingError` | Node down in partitioned mode | Route to replica or fall back to source |
| `LookupError::Timeout` | Source query exceeds deadline | Return stale cached value if available, else propagate |
| `LookupError::Internal` | Bug in lookup implementation | Log, metrics alert, panic in debug mode |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| `get_cached()` hit | < 500ns | Hot cache hit | `bench_lookup_get_cached_hit` |
| `get()` hot hit | < 500ns | Hot cache hit | `bench_lookup_get_hot_hit` |
| `get()` disk hit | 10-100μs | Disk cache hit (NVMe) | `bench_lookup_get_disk_hit` |
| `get()` source fetch | 1-10ms | Postgres source | `bench_lookup_get_source` |
| `insert()` | < 1μs | Hot cache insert | `bench_lookup_insert` |
| `invalidate()` | < 1μs | Hot cache remove | `bench_lookup_invalidate` |
| Memory overhead | < 5% | Over raw data size | Allocation tracking |
| Hot cache hit rate | > 95% | Financial reference data | Metrics dashboard |
| Zero alloc on hit | 0 allocs | `get_cached()` path | Allocation tracker bench |

## Migration from Existing Lookup Infrastructure

### Existing Abstractions to Replace

| Existing | Location | Replacement |
|----------|----------|-------------|
| `TableLoader` trait | `laminar_connectors::lookup` | `LookupTable` trait (this feature) |
| `ReferenceTableSource` trait | `laminar_connectors::reference` | `LookupSource` trait (F-LOOKUP-002) |
| `LookupJoinOperator` | `laminar_core::operator::lookup_join` | Updated to use `LookupTable` |
| `InMemoryTableLoader` | `laminar_connectors::lookup` | `FoyerMemoryCache` (F-LOOKUP-004) |
| `TableStore` (pub(crate)) | `laminar_db::table_store` | Bridged via `LookupTable` impl |

### Migration Path

1. Implement `LookupTable` trait with foyer backing (F-LOOKUP-004)
2. Create adapter: `impl LookupTable for TableLoaderAdapter<T: TableLoader>` for gradual migration
3. Update `LookupJoinOperator` to accept `Arc<dyn LookupTable>` instead of internal `StateStore` cache
4. Migrate `ReferenceTableSource` implementations to `LookupSource` trait
5. Deprecate `TableLoader` and `ReferenceTableSource` once all consumers migrated
6. `laminar_db::TableStore` remains `pub(crate)` — `LookupTable` implementations in `laminar-db` bridge to it internally

### Compatibility Notes

- The existing `LookupJoinOperator` returns Arrow `RecordBatch` values, while `LookupTable` returns `Bytes` (AUDIT FIX C3). A codec layer converts between Arrow IPC bytes and `RecordBatch` at the boundary. `Bytes` is cheap to slice and share — no copy needed to pass to Arrow IPC deserialization.
- The existing `TableLoader` is `async` (`#[async_trait]`). The new `LookupTable::get_cached()` is sync (Ring 0). The async path is preserved in `LookupTable::get()` which returns `LookupResult::Pending` for async fallback.

## Test Plan

### Unit Tests

- [ ] `test_lookup_strategy_enum_values` -- verify Debug, Clone, Copy, Eq, Hash
- [ ] `test_lookup_result_hit_returns_borrowed_slice`
- [ ] `test_lookup_result_pending_variant`
- [ ] `test_lookup_result_not_found_variant`
- [ ] `test_lookup_table_config_default_values`
- [ ] `test_lookup_table_config_custom_values`
- [ ] `test_lookup_table_builder_validates_empty_table_id`
- [ ] `test_lookup_table_builder_validates_min_cache_size`
- [ ] `test_lookup_table_builder_validates_empty_primary_key`
- [ ] `test_lookup_table_builder_validates_zero_partition_count`
- [ ] `test_lookup_table_builder_valid_config_passes`
- [ ] `test_lookup_error_display_messages`
- [ ] `test_lookup_error_key_not_found_includes_table_id`
- [ ] `test_lookup_error_source_error_wraps_inner`
- [ ] `test_lookup_error_timeout_includes_elapsed`
- [ ] `test_lookup_result_hit_carries_bytes` - (C3) Hit variant contains Bytes, clone is cheap
- [ ] `test_lookup_table_get_cached_returns_bytes` - (C3) get_cached returns Option<Bytes> not &\[u8\]
- [ ] `test_lookup_table_get_returns_lookup_result_no_lifetime` - (C3) LookupResult has no lifetime parameter

### Integration Tests

- [ ] `test_lookup_table_get_cached_hit_and_miss` -- populate cache, verify hit/miss
- [ ] `test_lookup_table_get_with_async_fallback` -- miss triggers async, completes
- [ ] `test_lookup_table_insert_then_get_cached` -- round-trip insert/get
- [ ] `test_lookup_table_invalidate_removes_from_cache` -- insert, invalidate, verify miss
- [ ] `test_lookup_table_strategy_replicated_all_local` -- no network calls
- [ ] `test_lookup_table_strategy_source_direct_no_prefetch` -- no materialization
- [ ] `test_lookup_table_with_operator` -- wire to LookupJoinOperator, process events
- [ ] `test_lookup_table_concurrent_access` -- multiple tasks reading simultaneously

### Benchmarks

- [ ] `bench_lookup_get_cached_hit` -- Target: < 500ns, 100K entries
- [ ] `bench_lookup_get_hot_hit` -- Target: < 500ns, verify same as get_cached
- [ ] `bench_lookup_insert` -- Target: < 1μs
- [ ] `bench_lookup_invalidate` -- Target: < 1μs
- [ ] `bench_lookup_get_cached_zero_alloc` -- Verify zero allocations via `#[global_allocator]`
- [ ] `bench_lookup_cache_key_construction` -- Target: < 50ns

## Rollout Plan

1. **Step 1**: Define `LookupStrategy`, `LookupResult`, `LookupError` enums in `laminar-core/src/lookup/mod.rs`
2. **Step 2**: Define `LookupTable` trait with all methods
3. **Step 3**: Define `LookupTableConfig` and `LookupTableBuilder`
4. **Step 4**: Implement a `MockLookupTable` for testing
5. **Step 5**: Unit tests for all types and builder validation
6. **Step 6**: Integration tests with mock implementation
7. **Step 7**: Benchmarks for cache key construction overhead
8. **Step 8**: Code review and merge

## Open Questions

- [x] ~~Should `get_cached()` return `Option<&[u8]>` or `Option<CacheEntry>`?~~ **Resolved (AUDIT FIX C3)**: Returns `Option<Bytes>`. `Bytes` is reference-counted (`Arc<[u8]>` internally), avoiding lifetime unsoundness with foyer `CacheEntry` handles. The `CacheEntry` is used internally; `Bytes` is extracted before the handle is dropped. Metadata (hit count, insert time) is tracked separately via `CacheMetrics`.
- [ ] Should `LookupTable` have a `get_batch()` method for batch lookups, or should batching be handled at the operator level? Batch lookups could amortize overhead for partitioned strategy.
- [ ] How should TTL expiry interact with the hot cache? Should expired entries return `None` from `get_cached()` or should expiry only happen on eviction?
- [ ] Should `LookupTableConfig` support multiple source types (e.g., Parquet snapshot + Postgres CDC) or should that be handled by composing sources externally?

## Completion Checklist

- [ ] `LookupStrategy` enum defined with Replicated, Partitioned, SourceDirect
- [ ] `LookupResult` enum defined with Hit, Pending, NotFound
- [ ] `LookupTable` trait defined with all methods
- [ ] `LookupTableConfig` struct with defaults
- [ ] `LookupTableBuilder` with validation
- [ ] `LookupError` enum with all variants
- [ ] `MockLookupTable` for testing
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-STATE-001: Revised StateStore Trait](../state/F-STATE-001-state-store-trait.md) -- Ring 0 state access pattern
- [F-LOOKUP-002: LookupSource Trait](./F-LOOKUP-002-lookup-source-trait.md) -- source query abstraction
- [F-LOOKUP-004: foyer In-Memory Cache](./F-LOOKUP-004-foyer-memory-cache.md) -- Ring 0 cache backend
- [F-LOOKUP-005: foyer Hybrid Cache](./F-LOOKUP-005-foyer-hybrid-cache.md) -- Ring 1 cache backend
- [F-LOOKUP-009: Partitioned Lookup](./F-LOOKUP-009-partitioned-lookup.md) -- partitioned strategy
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model design
- [foyer documentation](https://github.com/foyer-rs/foyer) -- Cache library
