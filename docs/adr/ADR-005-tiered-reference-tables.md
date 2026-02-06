# ADR-005: Tiered Reference Table Architecture

## Status
Proposed

## Context

Production streaming pipelines frequently join streams against dimension/reference
tables (e.g., enrich trades with instrument metadata, join orders with customer
profiles). F-CONN-002 delivered the foundational `TableStore` — an in-memory
primary-key-keyed store for reference tables. However, production workloads present
challenges the in-memory-only design cannot handle:

1. **Large dimension tables** (10M+ rows) exceed available memory
2. **Multiple external sources** (PostgreSQL, MySQL, MongoDB, Redis, SQL Server)
   need to populate tables, not just INSERT statements
3. **Freshness requirements** vary from "load once" to "real-time CDC"
4. **Lookup performance** must remain sub-microsecond on the hot path (Ring 0)

Industry research (January 2026) shows all major streaming systems converge on
a tiered model: hot data in memory, warm data on local disk, cold data in external
systems. No production system uses federated query pushdown for stream-table joins.

## Decision Drivers

1. **Ring 0 Constraints**
   - Lookup latency must stay < 500ns for cached keys
   - No network I/O on the hot path
   - No locks — SPSC communication between rings
   - No heap allocations in the lookup path

2. **Scalability**
   - Reference tables from 1K rows to 100M+ rows
   - Must gracefully degrade rather than OOM-crash
   - Memory budget should be configurable per table

3. **Source Diversity**
   - RDBMS: PostgreSQL, MySQL, SQL Server (CDC or periodic snapshot)
   - Document stores: MongoDB (change streams)
   - Key-value: Redis (direct lookup, no materialization needed)
   - Files: Parquet, CSV (one-time or periodic)
   - Message queues: Kafka compacted topics (snapshot + incremental)

4. **Freshness**
   - CDC-based refresh (sub-second) for RDBMS sources
   - Compacted topic consumption for Kafka sources
   - Periodic snapshot for REST/file sources
   - Cycle-consistent: no partial updates visible within a processing cycle

## Considered Options

### Option 1: In-Memory Only (Current F-CONN-002)

**Approach**: All reference table data in `HashMap<String, RecordBatch>`.

**Pros:**
- Simplest implementation (already done)
- Lowest lookup latency (~100ns)
- No additional dependencies

**Cons:**
- Cannot handle tables exceeding memory
- No disk persistence (data lost on restart without connector re-read)
- Memory pressure from large tables impacts stream processing

### Option 2: Federated Query Pushdown

**Approach**: Route lookups to external source (e.g., SELECT from PostgreSQL)
during stream-table joins via DataFusion `TableProvider`.

**Pros:**
- Zero memory usage for reference data
- Always-fresh data
- Leverages existing DataFusion federation crate

**Cons:**
- Network round-trip per lookup (1-100ms) — **incompatible with Ring 0 latency**
- External system becomes bottleneck under high throughput
- No production streaming system uses this for stream-table joins
- External system failure blocks the entire pipeline

### Option 3: RocksDB-Only Storage

**Approach**: Store all reference data in RocksDB. Always read from disk.

**Pros:**
- Handles any table size (limited by disk, not memory)
- Existing F022 infrastructure uses RocksDB
- Persistent across restarts

**Cons:**
- Every lookup incurs SSD read (~10-100us) — too slow for Ring 0
- No benefit for small tables that fit in memory
- RocksDB write amplification under update-heavy workloads

### Option 4: Three-Tier Hybrid (Recommended)

**Approach**: In-memory cache (Ring 0) + RocksDB persistent store (Ring 1) +
external source refresh (Ring 1), with configurable cache mode per table.

```
                     Ring 0 (Hot Path)
  ┌──────────────────────────────────────────────────┐
  │  Stream Event ──► LookupJoinOperator             │
  │                       │                          │
  │              ┌────────┼────────┐                  │
  │              ▼        ▼        ▼                  │
  │         [FxHashMap] [Xor     [LRU Cache]          │
  │          (FULL)    Filter]   (PARTIAL)            │
  │              │      (neg     miss? ──► Pending    │
  │              ▼      lookup)         Queue         │
  │          Join result                 │ SPSC       │
  └──────────────────────────────────────┼────────────┘
                     Ring 1 (Background) │
  ┌──────────────────────────────────────┼────────────┐
  │  CDC/Snapshot    TableStore          │            │
  │  Refresh ──────► (RocksDB) ◄─ Async Lookup       │
  │  (Kafka/PG/     Persistent                       │
  │   MySQL/etc)     Store                           │
  └───────────────────────────────────────────────────┘
```

**Pros:**
- Ring 0 latency maintained (< 500ns for cached keys)
- Graceful degradation for large tables (LRU eviction, disk fallback)
- Persistent across restarts (no re-snapshot from source)
- Reuses existing infrastructure (F022 RocksDB, F025/F027/F028 connectors)
- Industry-validated (Flink, Kafka Streams, RisingWave all use this model)

**Cons:**
- Most complex implementation
- RocksDB tuning required for lookup workload
- Cache miss path adds latency (still async, doesn't block Ring 0)

### Option 5: mmap-Based Store

**Approach**: Memory-map reference table data files. Let the OS manage caching.

**Pros:**
- Simple implementation
- Zero-copy reads when data is in page cache
- Automatic memory management by OS

**Cons:**
- Unpredictable eviction (OS decides, not application)
- Poor tail latency when data exceeds page cache
- Industry trend is away from mmap (Qdrant, ScyllaDB migrated to io_uring)
- No control over I/O scheduling or prioritization

## Decision

**Implement Three-Tier Hybrid Architecture (Option 4)** with three cache modes
per table:

```rust
pub enum TableCacheMode {
    /// Entire table in FxHashMap. Best for small tables (< 1M rows).
    Full,
    /// LRU cache with configurable max entries.
    /// Cache miss triggers async Ring 1 lookup from RocksDB.
    Partial { max_entries: usize },
    /// No caching. Every lookup queued to Ring 1.
    /// Only for testing or very low-throughput scenarios.
    None,
}
```

Default: `Full` for tables < 1M rows, `Partial { max_entries: 100_000 }` for
larger tables. The mode is configurable via DDL:

```sql
CREATE TABLE instruments (
    symbol VARCHAR PRIMARY KEY, ...
) WITH (
    connector = 'postgres',
    cache_mode = 'partial',
    cache_max_entries = 500000
);
```

### Refresh Models

```rust
pub enum RefreshMode {
    /// One-time snapshot at startup.
    SnapshotOnly,
    /// Snapshot at startup, then CDC for incremental updates.
    SnapshotPlusCdc,
    /// Periodic full re-read at configured interval.
    Periodic { interval: Duration },
    /// Manual only — populated via INSERT statements.
    Manual,
}
```

### Source Abstraction

```rust
pub trait ReferenceTableSource: Send + Sync {
    /// Load initial snapshot into the table store.
    async fn snapshot(&mut self) -> Result<Vec<RecordBatch>, ConnectorError>;
    /// Poll for incremental changes (CDC, compacted topic, etc.).
    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;
    /// Return the current checkpoint for recovery.
    fn checkpoint(&self) -> SourceCheckpoint;
    /// Restore from a previous checkpoint.
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;
}
```

This trait is implemented by adapting existing connectors:
- `KafkaSource` in snapshot mode → `KafkaTableSource`
- `PostgresCdcSource` → `PostgresTableSource` (snapshot query + WAL CDC)
- `MySqlCdcSource` → `MySqlTableSource` (snapshot query + binlog CDC)

### Negative Lookup Optimization

Use xor filters (not Bloom) for PARTIAL mode to short-circuit lookups for keys
known to not exist in the reference table:

- 2026 research shows xor filters are faster to build and query than Bloom
  filters with better space efficiency (RisingWave Hummock validates this)
- For sparse joins (many stream keys have no match), this avoids 60-90% of
  cache misses and Ring 1 round-trips
- Rebuild xor filter on table refresh (incremental rebuild not needed — filter
  build is ~100ns/key)

### Cycle-Consistent Updates

Table updates from CDC/polling are applied **between processing cycles**, never
during. This ensures lookup joins within a single cycle always see a consistent
snapshot. The existing `TableStore` design already supports this via the
`parking_lot::Mutex` boundary — updates are batched and applied atomically.

## Consequences

### Positive

- Sub-microsecond lookups for hot keys (Ring 0 FxHashMap/LRU)
- Scales to 100M+ row tables via RocksDB spill
- Persistent across restarts (RocksDB + checkpoint integration)
- Reuses existing connector infrastructure (no new connectors needed)
- CDC refresh provides sub-second freshness
- Xor filter eliminates most negative lookups in sparse joins

### Negative

- Three code paths (FULL/PARTIAL/NONE) to maintain and test
- RocksDB tuning for point-lookup workload (bloom filter per SST, direct I/O)
- Async cache-miss path adds complexity to LookupJoinOperator
- XOR filter rebuild cost on large table refresh (~100ms for 10M keys)

### Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| RocksDB lookup too slow | Low | Medium | Point lookup bloom filter, block cache tuning |
| LRU thrashing under uniform access | Medium | Medium | Adaptive cache sizing, metrics-driven tuning |
| CDC lag causes stale lookups | Low | Low | Staleness metrics, configurable max-lag alert |
| Xor filter false positives | Very Low | Low | xor16 gives < 0.002% FPR |

## Implementation Plan

### Phase A: Connector-Backed Table Population (F-CONN-002B)
- Wire `ReferenceTableSource` into pipeline loop
- Kafka compacted topic snapshot + incremental
- PostgreSQL/MySQL CDC refresh via existing connectors
- Table readiness gating (block pipeline until tables loaded)
- ~1-2 weeks

### Phase B: PARTIAL Cache Mode + Xor Filter (F-CONN-002C)
- LRU cache in Ring 0 with configurable max entries
- Async cache-miss path via SPSC to Ring 1
- Xor filter for negative lookup short-circuit
- Cache mode DDL syntax (`cache_mode`, `cache_max_entries`)
- ~1-2 weeks

### Phase C: RocksDB-Backed Persistent Store (F-CONN-002D)
- RocksDB column family per reference table
- Point-lookup optimized configuration (bloom filter, direct I/O)
- Checkpoint integration (RocksDB checkpoint = table checkpoint)
- io_uring-based reads from Ring 0 via Three-Ring I/O (F069)
- ~2-3 weeks

### Phase D: Extended Sources (future)
- MongoDB change streams
- SQL Server CDC
- Redis direct lookup (no materialization)
- Parquet/CSV file sources
- REST API with polling

## Metrics & Validation

| Metric | Target | Validation |
|--------|--------|------------|
| FULL mode lookup | < 500ns | Benchmark with 1M row table |
| PARTIAL mode hit | < 1us | Benchmark with LRU cache |
| PARTIAL mode miss (RocksDB) | < 100us | Benchmark with SSD |
| Xor filter check | < 50ns | Microbenchmark |
| CDC refresh lag | < 1s | Integration test with PG CDC |
| Table snapshot load (1M rows) | < 5s | Integration test |
| Memory per 1M rows (FULL) | < 500MB | Monitor with metrics |

## References

- [ADR-004: Checkpoint Strategy](ADR-004-checkpoint-strategy.md)
- [F-CONN-002: Reference Table Support](../features/phase-3/F-CONN-002-reference-tables.md)
- [Flink Delta Join (FLIP-486)](https://cwiki.apache.org/confluence/display/FLINK/FLIP-486)
- [Flink Lookup Join Cache Modes](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/hints/)
- [RisingWave Hummock Storage Engine](https://risingwave.com/blog/hummock-a-storage-engine-designed-for-stream-processing/)
- [Apache Paimon Lookup Joins](https://paimon.apache.org/docs/master/flink/sql-lookup/)
- [Xor Filters: Faster and Smaller Than Bloom](https://arxiv.org/abs/1912.08258)
- [Kafka Streams GlobalKTable](https://developer.confluent.io/courses/kafka-streams/joins/)
- [Materialize Delta Joins](https://materialize.com/blog/delta-joins/)
- [io_uring vs mmap (Qdrant)](https://qdrant.tech/articles/io_uring/)
