# F-CONN-002C: PARTIAL Cache Mode & Xor Filter

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-002C |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-CONN-002B (Table Source Population), F020 (Lookup Joins) |
| **Crate** | laminar-core, laminar-db |
| **Created** | 2026-02-06 |
| **ADR** | [ADR-005: Tiered Reference Tables](../../adr/ADR-005-tiered-reference-tables.md) |

## Summary

Add a PARTIAL cache mode for reference tables where only hot keys are kept in
Ring 0 memory (LRU cache) and cache misses are served asynchronously from the
backing store (Ring 1). Add xor filters for negative lookup short-circuiting to
eliminate most cache misses for keys that don't exist in the reference table.
This enables reference tables larger than available memory without sacrificing
hot-path latency.

## Motivation

F-CONN-002 and F-CONN-002B provide FULL cache mode — the entire table
materialized in memory. This works for tables up to ~10M rows, but production
workloads often have:

- **Large dimension tables**: Customer profiles (50M+), product catalogs (100M+),
  IP geolocation (500M+)
- **Skewed access patterns**: 80-90% of lookups hit < 5% of keys (Zipf distribution)
- **Memory constraints**: Embedded deployment where total RAM is limited

PARTIAL mode exploits access skew: keep the hot working set in Ring 0 FxHashMap,
serve cold lookups asynchronously from RocksDB (F-CONN-002D) or the backing
connector source. For joins where many stream keys have no match in the reference
table (sparse joins), xor filters avoid ~60-90% of unnecessary cache misses.

## Current State

| Component | Status |
|-----------|--------|
| `TableStore` FULL mode (FxHashMap) | Done (F-CONN-002) |
| `LookupJoinOperator` with pending-key pattern | Done (F020) |
| `TableLoader` async trait | Done (laminar-connectors) |
| LRU cache in Ring 0 | **NOT DONE** |
| Xor filter for negative lookup | **NOT DONE** |
| Cache mode DDL syntax | **NOT DONE** |
| Cache miss SPSC path to Ring 1 | **NOT DONE** |
| Cache hit/miss metrics | **NOT DONE** |

## Design

### Cache Modes

```rust
/// How reference table data is cached in Ring 0.
#[derive(Debug, Clone)]
pub enum TableCacheMode {
    /// Entire table in Ring 0 FxHashMap.
    /// Best for tables < 1M rows.
    Full,
    /// LRU cache with max entries. Cache miss → async Ring 1 lookup.
    /// Best for large tables with skewed access.
    Partial {
        /// Maximum entries in the LRU cache.
        max_entries: usize,
    },
    /// No caching. Every lookup goes through Ring 1.
    /// Only for testing or very low-throughput paths.
    None,
}

impl Default for TableCacheMode {
    fn default() -> Self {
        Self::Full
    }
}
```

### LRU Cache

A cache-line-aligned LRU cache for Ring 0 with O(1) get/insert:

```rust
/// Ring 0 LRU cache for reference table lookups.
/// Cache-line aligned to avoid false sharing.
#[repr(align(64))]
pub struct TableLruCache {
    /// Hash map for O(1) lookup by PK string.
    map: FxHashMap<String, LruEntry>,
    /// Doubly-linked list for LRU eviction ordering.
    order: VecDeque<String>,
    /// Maximum entries before eviction.
    max_entries: usize,
    /// Hit count for metrics.
    hits: u64,
    /// Miss count for metrics.
    misses: u64,
}

struct LruEntry {
    batch: RecordBatch,     // Single-row batch
    access_order: usize,    // Position in VecDeque
}
```

**Operations**:
- `get(key) -> Option<&RecordBatch>`: O(1) lookup, moves key to front of LRU
- `insert(key, batch)`: O(1) insert, evicts LRU entry if at capacity
- `invalidate(key)`: Remove specific key (for CDC delete events)
- `clear()`: Reset cache (for full table refresh)
- `hit_rate() -> f64`: `hits / (hits + misses)` for metrics

### Xor Filter

Xor filters provide faster and more space-efficient membership testing than
Bloom filters. A 64-bit xor filter uses ~9.84 bits/key with < 0.4% false
positive rate. xor16 uses ~19.7 bits/key with < 0.002% FPR.

```rust
/// Xor filter for fast negative-lookup short-circuiting.
pub struct TableXorFilter {
    /// The filter — rebuilt on each table refresh.
    filter: Option<xorf::Xor8>,  // or Xor16 for lower FPR
    /// Number of keys in the filter.
    key_count: usize,
    /// Number of negative lookups avoided.
    short_circuits: u64,
}
```

**Integration with lookup path**:
```rust
fn lookup(&self, table: &str, key: &str) -> LookupDecision {
    // Step 1: Xor filter check (< 50ns)
    if let Some(filter) = &self.xor_filter {
        if !filter.contains(key) {
            self.metrics.short_circuits += 1;
            return LookupDecision::NotFound;  // Definite miss
        }
    }

    // Step 2: LRU cache check (< 200ns)
    if let Some(batch) = self.lru_cache.get(key) {
        self.metrics.cache_hits += 1;
        return LookupDecision::Found(batch.clone());
    }

    // Step 3: Cache miss — queue async Ring 1 lookup
    self.metrics.cache_misses += 1;
    LookupDecision::Pending  // Will be resolved via provide_lookup()
}
```

**Rebuild strategy**: The xor filter is rebuilt from scratch on each table
refresh (snapshot or CDC batch). For a 10M key table, xor8 filter build
takes ~200ms and uses ~12MB. This happens in Ring 1 and is swapped into
Ring 0 atomically via `Arc::swap()`.

### DDL Syntax

```sql
-- Explicit PARTIAL mode
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR,
    segment VARCHAR
) WITH (
    connector = 'postgres',
    host = 'localhost',
    database = 'mydb',
    table = 'customers',
    cache_mode = 'partial',
    cache_max_entries = 500000
);

-- Auto-detect: FULL for tables < 1M rows (from row count hint or config)
CREATE TABLE products (
    sku VARCHAR PRIMARY KEY,
    name VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'products',
    brokers = 'localhost:9092'
);
```

### Cache Miss Path (SPSC to Ring 1)

When the LRU cache misses in Ring 0:

1. Ring 0 enqueues a `LookupRequest { table, key, request_id }` to SPSC queue
2. Ring 0 buffers the stream event in `LookupJoinOperator.pending_events`
   (existing F020 infrastructure)
3. Ring 1 receives the request, reads from `TableStore` (RocksDB or in-memory)
4. Ring 1 enqueues `LookupResponse { request_id, result }` back via SPSC
5. Ring 0 processes the response, inserts into LRU cache, completes the join

This reuses the existing `pending_keys` / `provide_lookup()` pattern from
F020 `LookupJoinOperator`. The enhancement is connecting it to the table
store backend instead of an external async source.

### Metrics

```rust
pub struct TableCacheMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub xor_short_circuits: u64,
    pub evictions: u64,
    pub cache_size: usize,
    pub xor_filter_keys: usize,
    pub hit_rate: f64,
}
```

Exposed via `SHOW TABLE STATUS` or the observability API.

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `table_lru_cache` | 12 | Insert/get, LRU eviction, capacity enforcement, invalidate, clear, hit/miss counts, cache-line alignment, concurrent-safe (single-thread Ring 0) |
| `table_xor_filter` | 8 | Build from keys, membership true positive, membership true negative, false positive rate < 1%, rebuild on refresh, empty filter, large key set (1M), filter size |
| `partial_cache_mode` | 6 | PARTIAL mode DDL parsing, cache miss triggers async, cache hit returns immediately, xor short-circuit returns NotFound, eviction under load, mixed FULL + PARTIAL tables |
| `cache_miss_path` | 5 | SPSC enqueue on miss, Ring 1 lookup response, LRU populated after response, pending event completed, multiple concurrent misses |
| `metrics` | 3 | Hit rate calculation, short-circuit counting, eviction counting |
| **Total** | **~34** | |

## Files

### New Files
- `crates/laminar-core/src/operator/table_cache.rs` — `TableLruCache`, `TableXorFilter`
- `crates/laminar-db/src/table_cache_mode.rs` — `TableCacheMode`, DDL parsing

### Modified Files
- `crates/laminar-db/src/table_store.rs` — Add `cache_mode` to `TableState`, cache mode selection
- `crates/laminar-db/src/db.rs` — Parse `cache_mode`/`cache_max_entries` from WITH options
- `crates/laminar-db/src/connector_manager.rs` — Store `TableCacheMode` in `TableRegistration`
- `crates/laminar-core/src/operator/lookup_join.rs` — Wire xor filter + LRU into lookup path

### Dependencies
- `xorf` crate (xor filter implementation, pure Rust, no-std compatible)
