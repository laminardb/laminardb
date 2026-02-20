# F-LOOKUP-002: LookupSource Trait (Unified Query Method)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-002 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LOOKUP-001 (LookupTable trait), F-LOOKUP-003 (Predicate types) |
| **Blocks** | F-LOOKUP-007 (PostgresLookupSource), F-LOOKUP-008 (ParquetLookupSource) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/source.rs` |

## Summary

Unified `LookupSource` trait providing a single `query()` method for all external data source interactions in the lookup table system. This design is an explicit **anti-pattern correction**: earlier drafts had separate `get_batch()` and `query()` methods, which created ambiguity about when to use each, duplicated error handling, and made source implementations unnecessarily complex.

The unified `query()` method accepts keys, predicates, and projections. When predicates and projections are empty, it degrades to a simple batch key lookup. When keys are empty but predicates are provided, it performs a filtered scan. This single method covers all access patterns while the `SourceCapabilities` struct declares what the source can actually handle, enabling the engine to plan accordingly.

## Goals

- Define a single `query()` method replacing the anti-pattern `get_batch()` + `query()` split
- Define `SourceCapabilities` struct declaring source-level pushdown support
- Support predicate pushdown (filter at source, not in engine)
- Support projection pushdown (select only needed columns)
- Support batch key lookup via `keys` parameter
- Graceful degradation: if source does not support a capability, engine handles it
- Async interface (sources involve I/O: network, disk)
- Pluggable for any data source (Postgres, Parquet, Redis, HTTP, etc.)

## Non-Goals

- Implementing specific source connectors (see F-LOOKUP-007, F-LOOKUP-008)
- Query planning or optimization (handled at SQL layer)
- Schema management or DDL for source tables
- Write operations (lookup sources are read-only)
- Streaming/incremental reads (that is CDC, see F-LOOKUP-006)
- Connection pool management (source-specific, see F-LOOKUP-007)

## Technical Design

### Architecture

**Ring**: Ring 1 / Ring 2 (async, off hot path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/source.rs`

The `LookupSource` sits at the bottom of the lookup table cache hierarchy. When both foyer in-memory (Tier 0) and foyer HybridCache disk (Tier 1) miss, the lookup table implementation calls `source.query()` to fetch from the external system (Tier 2). The source is always async because it involves I/O (network or disk).

```
Cache Miss Flow:
┌─────────────────────┐
│ LookupTable.get()   │
│   → foyer::Cache    │  Tier 0: sync, < 500ns
│   → MISS            │
├─────────────────────┤
│   → HybridCache     │  Tier 1: async, 10-100μs
│   → MISS            │
├─────────────────────┤
│   → LookupSource    │  Tier 2: async, 1-100ms
│     .query(         │
│       keys,         │  ← batch of missing keys
│       predicates,   │  ← pushdown filters
│       projection,   │  ← pushdown columns
│     )               │
└─────────────────────┘
         │
         ▼
   ┌───────────┐
   │ Postgres   │  or Parquet, Redis, HTTP, etc.
   └───────────┘
```

### API/Interface

```rust
use crate::lookup::predicate::{ColumnId, Predicate};
use crate::lookup::LookupError;

/// Unique identifier for a column in a lookup table schema.
///
/// Used for projection pushdown (selecting specific columns)
/// and in predicates (filtering on specific columns).
pub type ColumnId = u32;

/// Declares what query capabilities a source implementation supports.
///
/// The engine uses this to decide what to push down to the source
/// versus what to evaluate locally. Sources should be conservative:
/// only declare capabilities they can handle efficiently.
///
/// # Design Rationale
///
/// A capabilities struct (rather than feature flags on the trait) allows
/// runtime capability discovery. This matters when the same source type
/// may have different capabilities depending on configuration (e.g.,
/// Postgres with/without certain indexes).
#[derive(Debug, Clone)]
pub struct SourceCapabilities {
    /// Source can evaluate predicates server-side (WHERE clause pushdown).
    /// If false, engine fetches all rows and filters locally.
    pub supports_predicate_pushdown: bool,

    /// Source can return a subset of columns (SELECT column pushdown).
    /// If false, engine fetches all columns and projects locally.
    pub supports_projection_pushdown: bool,

    /// Source can look up multiple keys in a single round-trip.
    /// If false, engine issues one query per key (sequential).
    pub supports_batch_lookup: bool,

    /// Source can perform ordered range scans.
    /// If false, range queries are converted to point lookups.
    pub supports_range_scan: bool,

    /// Maximum number of keys in a single batch lookup.
    /// Only meaningful if `supports_batch_lookup` is true.
    /// Engine will chunk larger batches to this size.
    pub max_batch_size: usize,

    /// Maximum number of predicates the source can handle per query.
    /// Excess predicates are evaluated locally after fetch.
    pub max_predicate_count: usize,

    /// Estimated latency for a single-key lookup (for cost modeling).
    pub estimated_single_lookup_ms: f64,

    /// Estimated latency for a batch lookup (for cost modeling).
    pub estimated_batch_lookup_ms: f64,
}

impl Default for SourceCapabilities {
    fn default() -> Self {
        Self {
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            supports_batch_lookup: false,
            supports_range_scan: false,
            max_batch_size: 1,
            max_predicate_count: 0,
            estimated_single_lookup_ms: 5.0,
            estimated_batch_lookup_ms: 10.0,
        }
    }
}

/// Unified trait for querying external lookup data sources.
///
/// Replaces the earlier anti-pattern of separate `get_batch()` and
/// `query()` methods. A single `query()` method handles all access
/// patterns:
///
/// - **Batch key lookup**: `keys` populated, predicates/projection empty
/// - **Filtered lookup**: `keys` populated, predicates filter results
/// - **Scan with pushdown**: `keys` empty, predicates filter source-side
/// - **Full scan**: all parameters empty (returns all rows)
///
/// # Anti-Pattern Correction
///
/// The original design had:
/// ```rust,ignore
/// // DON'T: Two separate methods create ambiguity
/// async fn get_batch(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>>;
/// async fn query(&self, predicates: &[Predicate], proj: &[ColumnId]) -> Vec<Vec<u8>>;
/// ```
///
/// Problems with the split approach:
/// 1. Unclear when to use which method
/// 2. Duplicated error handling and retry logic
/// 3. No way to combine key lookup with predicate filtering
/// 4. Source implementations must implement two similar methods
///
/// The unified `query()` method solves all of these by making keys,
/// predicates, and projections orthogonal parameters that compose freely.
///
/// # Implementation Guide
///
/// Sources should:
/// 1. Check if keys are provided → use them for primary key lookup
/// 2. Check predicates → push down if capable, ignore if not
/// 3. Check projection → select only requested columns if capable
/// 4. Return `Vec<Option<Vec<u8>>>` aligned with input keys
///    (None = key not found, Some = serialized row)
///
/// When keys are empty, return all matching rows (scan mode).
pub trait LookupSource: Send + Sync {
    /// Query the source for lookup data.
    ///
    /// # Parameters
    ///
    /// - `keys`: Primary key values to look up. When non-empty, results
    ///   are aligned 1:1 with input keys (None = not found). When empty,
    ///   the query is a scan returning all matching rows.
    /// - `predicates`: Filter conditions to apply. Implicitly ANDed.
    ///   Sources that support pushdown evaluate these server-side.
    ///   Sources that don't ignore them (engine filters locally).
    /// - `projection`: Column IDs to include in results. Empty = all columns.
    ///   Sources that support pushdown select only these columns.
    ///
    /// # Returns
    ///
    /// When `keys` is non-empty: `Vec<Option<Vec<u8>>>` of length `keys.len()`,
    /// where each entry corresponds to the key at the same index.
    ///
    /// When `keys` is empty (scan mode): `Vec<Option<Vec<u8>>>` containing
    /// all matching rows (all entries are `Some`).
    ///
    /// # Errors
    ///
    /// Returns `LookupError::SourceError` for network/IO failures.
    /// Returns `LookupError::Timeout` if the query exceeds the deadline.
    fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> impl std::future::Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send;

    /// Declare this source's capabilities.
    ///
    /// Called once during lookup table initialization. The engine caches
    /// the result and uses it for pushdown planning. Implementations
    /// should return conservative (understated) capabilities.
    fn capabilities(&self) -> SourceCapabilities;

    /// Human-readable name for logging and diagnostics.
    fn source_name(&self) -> &str;

    /// Estimated row count (for cost modeling). Returns None if unknown.
    fn estimated_row_count(&self) -> Option<u64> {
        None
    }

    /// Health check: verify the source is reachable.
    ///
    /// Called periodically by the health monitor. Default implementation
    /// issues a minimal query.
    fn health_check(
        &self,
    ) -> impl std::future::Future<Output = Result<(), LookupError>> + Send {
        async { Ok(()) }
    }
}
```

### Data Structures

```rust
/// Metadata about a query result for monitoring and optimization.
#[derive(Debug, Clone)]
pub struct QueryMetrics {
    /// Number of keys requested.
    pub keys_requested: usize,
    /// Number of keys found (non-None results).
    pub keys_found: usize,
    /// Number of predicates pushed to source.
    pub predicates_pushed: usize,
    /// Number of predicates evaluated locally.
    pub predicates_local: usize,
    /// Number of columns projected at source.
    pub columns_projected: usize,
    /// Total bytes returned from source.
    pub bytes_returned: usize,
    /// Wall-clock time for the query.
    pub duration: std::time::Duration,
}

/// Wraps a LookupSource to add predicate/projection handling
/// for sources that don't support pushdown natively.
///
/// When a source declares `supports_predicate_pushdown: false`,
/// this wrapper fetches all rows and filters locally. Similarly
/// for projection pushdown.
pub struct PushdownAdapter<S: LookupSource> {
    inner: S,
    capabilities: SourceCapabilities,
}

impl<S: LookupSource> PushdownAdapter<S> {
    /// Create a new adapter wrapping the given source.
    pub fn new(inner: S) -> Self {
        let capabilities = inner.capabilities();
        Self { inner, capabilities }
    }

    /// Split predicates into pushable (sent to source) and local
    /// (evaluated after fetch) based on source capabilities.
    fn split_predicates<'a>(
        &self,
        predicates: &'a [Predicate],
    ) -> (Vec<&'a Predicate>, Vec<&'a Predicate>) {
        if self.capabilities.supports_predicate_pushdown {
            let max = self.capabilities.max_predicate_count;
            if predicates.len() <= max {
                (predicates.iter().collect(), Vec::new())
            } else {
                // Push first `max` predicates, evaluate rest locally
                let (push, local) = predicates.split_at(max);
                (push.iter().collect(), local.iter().collect())
            }
        } else {
            // Source cannot push down: all predicates are local
            (Vec::new(), predicates.iter().collect())
        }
    }

    /// Apply local predicates to filter results.
    fn apply_local_predicates(
        &self,
        results: Vec<Option<Vec<u8>>>,
        predicates: &[&Predicate],
    ) -> Vec<Option<Vec<u8>>> {
        if predicates.is_empty() {
            return results;
        }
        results
            .into_iter()
            .map(|opt| {
                opt.and_then(|row| {
                    if self.evaluate_predicates(&row, predicates) {
                        Some(row)
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Evaluate a set of predicates against a serialized row.
    fn evaluate_predicates(&self, _row: &[u8], _predicates: &[&Predicate]) -> bool {
        // Deserialization and predicate evaluation logic
        // Implementation depends on row format (Arrow, rkyv, etc.)
        todo!("Row deserialization + predicate evaluation")
    }
}
```

### Algorithm/Flow

#### Unified Query Flow

```
1. LookupTable receives cache miss for key(s)
2. Collect missed keys into batch (up to max_batch_size)
3. Retrieve source capabilities
4. Split predicates into pushable vs. local
5. Call source.query(keys, pushable_predicates, projection)
6. If source does not support batch: chunk keys, issue parallel queries
7. Apply local predicates to results (if any)
8. Apply local projection to results (if source doesn't support it)
9. Insert results into foyer caches (hot + hybrid)
10. Return results to caller
```

#### Capability-Based Planning

```
Given: keys=[k1,k2,...,kN], predicates=[p1,p2,...,pM], projection=[c1,c2,...,cP]

Step 1: Check supports_batch_lookup
  - true: send all keys in one query (up to max_batch_size)
  - false: issue N individual queries

Step 2: Check supports_predicate_pushdown
  - true: include predicates in query (up to max_predicate_count)
  - false: omit predicates, filter results locally

Step 3: Check supports_projection_pushdown
  - true: include projection in query
  - false: omit projection, project results locally

Step 4: Execute query with resolved parameters
Step 5: Post-process with any remaining local predicates/projections
```

#### Batch Chunking

```
If keys.len() > source.capabilities().max_batch_size:
  1. Split keys into chunks of max_batch_size
  2. Execute chunks in parallel (up to concurrency limit)
  3. Merge results maintaining original key order
  4. Return combined results
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `LookupError::SourceError` | Network failure, connection refused | Retry with exponential backoff (100ms, 200ms, 400ms, max 5s) |
| `LookupError::SourceError` | Authentication failure | Log error, alert, do not retry (permanent failure) |
| `LookupError::Timeout` | Query exceeds deadline | Return cached stale value if available, else propagate |
| `LookupError::SourceError` | Source returned invalid data | Log, skip entry, return None for affected keys |
| `LookupError::Internal` | Batch size exceeded max | Chunk batch automatically (should not reach here) |
| `LookupError::SourceError` | Connection pool exhausted | Backpressure: queue request, retry when connection available |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| `query()` single key (Postgres) | 1-5ms | Network + query | `bench_source_postgres_single` |
| `query()` batch 100 keys (Postgres) | 5-15ms | WHERE IN clause | `bench_source_postgres_batch_100` |
| `query()` single key (Parquet) | 10-50ms | S3 fetch + decode | `bench_source_parquet_single` |
| `query()` batch 100 keys (Parquet) | 20-100ms | Row group scan | `bench_source_parquet_batch_100` |
| Predicate split overhead | < 1μs | Capability check + split | `bench_predicate_split` |
| Local predicate evaluation | < 10μs per row | In-memory filter | `bench_local_predicate_eval` |
| Batch chunking overhead | < 5μs | 10K keys into 10 chunks | `bench_batch_chunking` |

## Test Plan

### Unit Tests

- [ ] `test_source_capabilities_default_values` -- verify conservative defaults
- [ ] `test_source_capabilities_custom_values` -- verify override works
- [ ] `test_query_with_keys_only` -- predicates and projection empty
- [ ] `test_query_with_predicates_only` -- keys empty, scan mode
- [ ] `test_query_with_keys_and_predicates` -- combined lookup + filter
- [ ] `test_query_with_projection` -- column subset returned
- [ ] `test_query_result_aligned_with_keys` -- result[i] corresponds to keys[i]
- [ ] `test_query_empty_keys_returns_scan_results` -- all Some entries
- [ ] `test_pushdown_adapter_splits_predicates` -- push vs. local
- [ ] `test_pushdown_adapter_all_predicates_local` -- source has no pushdown
- [ ] `test_pushdown_adapter_all_predicates_pushed` -- source has full pushdown
- [ ] `test_pushdown_adapter_excess_predicates_overflow` -- max_predicate_count exceeded
- [ ] `test_query_metrics_populated` -- verify all fields set

### Integration Tests

- [ ] `test_mock_source_batch_lookup` -- mock source, batch of 100 keys
- [ ] `test_mock_source_predicate_pushdown` -- mock source, verify predicates forwarded
- [ ] `test_mock_source_projection_pushdown` -- mock source, verify columns selected
- [ ] `test_mock_source_no_pushdown_local_filter` -- mock source, engine filters locally
- [ ] `test_mock_source_batch_chunking` -- batch exceeds max_batch_size, auto-chunked
- [ ] `test_mock_source_health_check` -- health check passes/fails
- [ ] `test_source_error_retry_backoff` -- verify retry on transient failure

### Benchmarks

- [ ] `bench_predicate_split` -- Target: < 1μs for 10 predicates
- [ ] `bench_local_predicate_eval` -- Target: < 10μs per row
- [ ] `bench_batch_chunking` -- Target: < 5μs for 10K keys
- [ ] `bench_query_result_assembly` -- Target: < 1μs for 100 results

## Rollout Plan

1. **Step 1**: Define `SourceCapabilities` struct in `laminar-core/src/lookup/source.rs`
2. **Step 2**: Define `LookupSource` trait with `query()` and `capabilities()`
3. **Step 3**: Implement `PushdownAdapter` for local predicate/projection handling
4. **Step 4**: Implement `MockLookupSource` for testing
5. **Step 5**: Unit tests for trait, capabilities, and adapter
6. **Step 6**: Integration tests with mock source
7. **Step 7**: Benchmarks for overhead measurements
8. **Step 8**: Code review and merge

## Open Questions

- [ ] Should `query()` return `Vec<Option<Vec<u8>>>` (byte-oriented) or `Vec<Option<RecordBatch>>` (Arrow-oriented)? Bytes are simpler and cache-friendly; Arrow is more ergonomic for SQL integration. May need a `TypedLookupSource<T>` wrapper.
- [ ] Should the trait use `async fn` (RPITIT) or return `Pin<Box<dyn Future>>`? RPITIT is cleaner but requires Rust 1.75+. Given our MSRV policy, RPITIT should be fine.
- [ ] How should pagination work for scan queries that return millions of rows? Should `query()` return a `Stream` instead of a `Vec` for scan mode?
- [ ] Should `SourceCapabilities` include cost estimates (e.g., estimated latency per operation) to help the query planner choose between strategies?

## Completion Checklist

- [ ] `LookupSource` trait defined with unified `query()` method
- [ ] `SourceCapabilities` struct with all capability flags
- [ ] `PushdownAdapter` wrapper for local evaluation
- [ ] `QueryMetrics` struct for monitoring
- [ ] `MockLookupSource` for testing
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

- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- parent abstraction
- [F-LOOKUP-003: Predicate Types](./F-LOOKUP-003-predicate-types.md) -- predicate enum
- [F-LOOKUP-007: PostgresLookupSource](./F-LOOKUP-007-postgres-lookup-source.md) -- Postgres implementation
- [F-LOOKUP-008: ParquetLookupSource](./F-LOOKUP-008-parquet-lookup-source.md) -- Parquet implementation
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model design
