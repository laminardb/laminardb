# F-LOOKUP-006: CDC-to-Cache Adapter (Replicated Strategy)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-006 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | S (2-3 days) |
| **Dependencies** | F-LOOKUP-004 (foyer In-Memory Cache), F027 (PostgreSQL CDC connector) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/lookup/cdc_adapter.rs` |

## Summary

Thin adapter (~30 lines of core logic) that feeds CDC (Change Data Capture) events from Postgres logical replication into the foyer lookup cache. This is the primary cache population mechanism for the **Replicated** lookup table strategy: every node independently subscribes to the CDC stream and maintains a full local copy of the lookup table in its foyer cache.

The adapter translates CDC operations directly to cache operations:
- CDC **Insert** -> `cache.insert(key, row)`
- CDC **Update** -> `cache.insert(key, row)` (upsert)
- CDC **Delete** -> `cache.remove(key)`

This intentionally reuses the existing Postgres CDC source connector (F027) rather than building a new one. The adapter is a consumer of CDC events, not a CDC implementation.

Three deployment fan-out strategies handle different delta sizes:

1. **Embedded** (1 node): `Postgres -> CDC -> foyer` (direct connection)
2. **Delta (3-5 nodes)**: `Postgres -> CDC -> foyer` per node (independent connections)
3. **Delta (50+ nodes)**: `Postgres -> CDC -> Kafka compact topic -> foyer` (Kafka fan-out to avoid N connections to Postgres)

## Goals

- Translate CDC events to cache insert/remove operations (core: ~30 lines)
- Support all three CDC operation types: Insert, Update, Delete
- Handle initial snapshot loading (full table scan before CDC stream)
- Monitor CDC lag (gap between source change and cache update)
- Integrate with existing Postgres CDC connector (F027)
- Support Kafka compact topic fan-out for large deltas
- Provide metrics: events processed, lag, cache population count

## Non-Goals

- Implementing CDC protocol (that is F027)
- Kafka producer/consumer implementation (existing connector)
- Cache implementation (that is F-LOOKUP-004/005)
- Schema evolution handling (handled at CDC connector level)
- Multi-master conflict resolution (single-source assumption)
- Exactly-once semantics for cache population (at-least-once is sufficient; idempotent upserts)
- Transactions spanning multiple CDC events (each event is independent)

## Technical Design

### Architecture

**Ring**: Ring 1 (Async) -- CDC events arrive asynchronously
**Crate**: `laminar-connectors`
**Module**: `laminar-connectors/src/lookup/cdc_adapter.rs`

The CDC-to-Cache adapter sits between the CDC source and the foyer cache. It is a simple mapping layer that does not maintain its own state beyond a CDC position marker for resume.

```
Embedded (1 node):
┌────────────┐     CDC Stream      ┌──────────────┐     insert/remove    ┌─────────────┐
│  Postgres   │ ──────────────────> │ CdcCache     │ ──────────────────> │ foyer::Cache │
│  (source)   │     Logical         │ Adapter       │                     │ (Ring 0)     │
└────────────┘     Replication      └──────────────┘                     └─────────────┘

Delta (3-5 nodes):
┌────────────┐     CDC Stream      ┌──────────────┐     insert/remove    ┌─────────────┐
│  Postgres   │ ──────────────────> │ CdcAdapter   │ ──────────────────> │ Node 1 cache │
│  (source)   │ ──────────────────> │ CdcAdapter   │ ──────────────────> │ Node 2 cache │
└────────────┘ ──────────────────> │ CdcAdapter   │ ──────────────────> │ Node 3 cache │
               (N connections)      └──────────────┘                     └─────────────┘

Delta (50+ nodes):
┌────────────┐  CDC  ┌─────────┐  Kafka  ┌──────────────┐  insert  ┌─────────────┐
│  Postgres   │ ───> │  Kafka   │ ──────> │ CdcAdapter   │ ───────> │ Node 1..N   │
│  (source)   │      │ compact  │         │ (per node)   │          │ foyer cache │
└────────────┘      │ topic    │         └──────────────┘          └─────────────┘
                     └─────────┘
  1 CDC connection    fan-out     N Kafka consumers    N cache instances
```

### API/Interface

```rust
use crate::lookup::foyer_cache::FoyerMemoryCache;
use crate::lookup::LookupError;
use std::sync::Arc;

/// CDC operation types from the source database.
///
/// Maps directly to Postgres logical replication message types.
/// Reuses the existing CDC connector's event model (F027).
#[derive(Debug, Clone)]
pub enum CdcOperation {
    /// New row inserted. Contains the full row data.
    Insert {
        /// Primary key bytes.
        key: Vec<u8>,
        /// Full row serialized as bytes.
        value: Vec<u8>,
    },
    /// Existing row updated. Contains the full new row data.
    /// Old row data is not needed (cache is overwritten).
    Update {
        /// Primary key bytes.
        key: Vec<u8>,
        /// Full updated row serialized as bytes.
        value: Vec<u8>,
    },
    /// Row deleted. Contains only the primary key.
    Delete {
        /// Primary key bytes.
        key: Vec<u8>,
    },
}

/// Metrics for CDC-to-cache processing.
#[derive(Debug, Clone, Default)]
pub struct CdcAdapterMetrics {
    /// Total CDC events processed (all types).
    pub events_processed: u64,
    /// Insert events processed.
    pub inserts: u64,
    /// Update events processed.
    pub updates: u64,
    /// Delete events processed.
    pub deletes: u64,
    /// Current CDC lag in milliseconds.
    /// (wall clock - event timestamp)
    pub lag_ms: u64,
    /// Number of entries currently in cache.
    pub cache_entries: usize,
    /// Whether initial snapshot is complete.
    pub snapshot_complete: bool,
    /// Errors encountered.
    pub errors: u64,
}

/// Configuration for the CDC-to-Cache adapter.
#[derive(Debug, Clone)]
pub struct CdcCacheAdapterConfig {
    /// Lookup table ID for cache key prefix.
    pub table_id: u32,

    /// Whether to load an initial snapshot before starting CDC.
    /// Default: true (ensures complete cache on startup).
    pub load_initial_snapshot: bool,

    /// Batch size for initial snapshot loading.
    /// Larger batches are faster but use more memory.
    /// Default: 10_000 rows.
    pub snapshot_batch_size: usize,

    /// Fan-out strategy for delta mode.
    pub fan_out: CdcFanOutStrategy,

    /// Maximum acceptable CDC lag before alerting.
    /// Default: 5 seconds.
    pub max_lag_threshold: std::time::Duration,
}

/// Fan-out strategy for distributing CDC events to nodes.
#[derive(Debug, Clone)]
pub enum CdcFanOutStrategy {
    /// Each node connects directly to the CDC source.
    /// Best for 1-5 nodes.
    Direct,
    /// CDC events flow through a Kafka compact topic.
    /// Best for 6+ nodes to avoid N Postgres connections.
    KafkaFanOut {
        /// Kafka topic name for CDC events.
        topic: String,
        /// Kafka broker addresses.
        brokers: Vec<String>,
    },
}

impl Default for CdcCacheAdapterConfig {
    fn default() -> Self {
        Self {
            table_id: 0,
            load_initial_snapshot: true,
            snapshot_batch_size: 10_000,
            fan_out: CdcFanOutStrategy::Direct,
            max_lag_threshold: std::time::Duration::from_secs(5),
        }
    }
}

/// CDC-to-Cache Adapter.
///
/// Thin translation layer that maps CDC events to cache operations.
/// The core logic is intentionally minimal (~30 lines) because the
/// complexity lives in the CDC connector (F027) and the cache (F-LOOKUP-004).
///
/// This adapter is the primary cache population mechanism for the
/// Replicated lookup table strategy. Each node runs its own adapter
/// instance, receiving CDC events either directly from Postgres or
/// via a Kafka compact topic.
pub struct CdcCacheAdapter {
    /// Target cache to populate.
    cache: Arc<FoyerMemoryCache>,
    /// Configuration.
    config: CdcCacheAdapterConfig,
    /// Runtime metrics.
    metrics: std::sync::Mutex<CdcAdapterMetrics>,
    /// Last known CDC LSN (Log Sequence Number) for resume.
    last_lsn: std::sync::atomic::AtomicU64,
}

impl CdcCacheAdapter {
    /// Create a new CDC-to-Cache adapter.
    pub fn new(cache: Arc<FoyerMemoryCache>, config: CdcCacheAdapterConfig) -> Self {
        Self {
            cache,
            config,
            metrics: std::sync::Mutex::new(CdcAdapterMetrics::default()),
            last_lsn: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Process a single CDC event.
    ///
    /// This is the core logic (~30 lines). Maps CDC operations directly
    /// to cache operations. Insert and Update both call cache.insert()
    /// (upsert semantics). Delete calls cache.remove().
    ///
    /// Returns Ok(()) on success. Errors are logged and counted in metrics
    /// but do NOT stop CDC processing (at-least-once, idempotent).
    pub fn process_event(&self, event: CdcOperation) -> Result<(), LookupError> {
        match event {
            CdcOperation::Insert { key, value } => {
                self.cache.insert(&key, value);
                let mut m = self.metrics.lock().unwrap();
                m.inserts += 1;
                m.events_processed += 1;
            }
            CdcOperation::Update { key, value } => {
                self.cache.insert(&key, value);
                let mut m = self.metrics.lock().unwrap();
                m.updates += 1;
                m.events_processed += 1;
            }
            CdcOperation::Delete { key } => {
                self.cache.remove(&key);
                let mut m = self.metrics.lock().unwrap();
                m.deletes += 1;
                m.events_processed += 1;
            }
        }
        Ok(())
    }

    /// Process a batch of CDC events.
    ///
    /// More efficient than processing one-by-one because the metrics
    /// lock is acquired once per batch instead of per event.
    pub fn process_batch(&self, events: Vec<CdcOperation>) -> Result<usize, LookupError> {
        let count = events.len();
        let mut inserts = 0u64;
        let mut updates = 0u64;
        let mut deletes = 0u64;

        for event in events {
            match event {
                CdcOperation::Insert { key, value } => {
                    self.cache.insert(&key, value);
                    inserts += 1;
                }
                CdcOperation::Update { key, value } => {
                    self.cache.insert(&key, value);
                    updates += 1;
                }
                CdcOperation::Delete { key } => {
                    self.cache.remove(&key);
                    deletes += 1;
                }
            }
        }

        let mut m = self.metrics.lock().unwrap();
        m.inserts += inserts;
        m.updates += updates;
        m.deletes += deletes;
        m.events_processed += count as u64;
        m.cache_entries = self.cache.len();

        Ok(count)
    }

    /// Load initial snapshot from source.
    ///
    /// Called on startup before CDC streaming begins. Issues a full
    /// table scan (or Parquet snapshot read) and populates the cache.
    /// After snapshot completes, CDC starts from the snapshot LSN.
    pub async fn load_snapshot(
        &self,
        source: &dyn crate::lookup::source::LookupSource,
    ) -> Result<usize, LookupError> {
        if !self.config.load_initial_snapshot {
            return Ok(0);
        }

        // Scan the full table with no predicates and no projection
        let rows = source.query(&[], &[], &[]).await?;

        let mut count = 0;
        for row in rows.into_iter().flatten() {
            // Extract primary key from row (source-specific deserialization)
            // For now, use the full row bytes as the key
            // Real implementation will extract PK columns
            self.cache.insert(&row, row.clone());
            count += 1;
        }

        let mut m = self.metrics.lock().unwrap();
        m.snapshot_complete = true;
        m.cache_entries = self.cache.len();

        Ok(count)
    }

    /// Update CDC lag metric.
    ///
    /// Called by the CDC connector with the event timestamp.
    /// Lag = wall_clock_now - event_timestamp.
    pub fn update_lag(&self, event_timestamp_ms: u64) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let lag = now_ms.saturating_sub(event_timestamp_ms);
        if let Ok(mut m) = self.metrics.lock() {
            m.lag_ms = lag;
        }
    }

    /// Check if CDC lag exceeds the configured threshold.
    pub fn is_lagging(&self) -> bool {
        let m = self.metrics.lock().unwrap();
        m.lag_ms > self.config.max_lag_threshold.as_millis() as u64
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> CdcAdapterMetrics {
        self.metrics.lock().unwrap().clone()
    }

    /// Update the last processed LSN for CDC resume.
    pub fn update_lsn(&self, lsn: u64) {
        self.last_lsn.store(lsn, std::sync::atomic::Ordering::Release);
    }

    /// Get the last processed LSN for CDC resume.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(std::sync::atomic::Ordering::Acquire)
    }
}
```

### Data Structures

```rust
/// CDC position marker for resume after restart.
///
/// Stored as part of the checkpoint to enable CDC resume
/// from the exact position after a failure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CdcPosition {
    /// Postgres WAL Log Sequence Number.
    pub lsn: u64,
    /// Kafka offset (if using Kafka fan-out).
    pub kafka_offset: Option<i64>,
    /// Timestamp of the last processed event.
    pub last_event_timestamp_ms: u64,
}
```

### Algorithm/Flow

#### Startup Flow (Replicated Strategy)

```
1. Create FoyerMemoryCache (Ring 0)
2. Create CdcCacheAdapter with config
3. If load_initial_snapshot:
   a. Connect to source (Postgres or Parquet)
   b. Full table scan → populate cache
   c. Record snapshot LSN
4. Start CDC stream from snapshot LSN:
   a. Direct mode: open Postgres replication slot
   b. Kafka mode: subscribe to Kafka compact topic
5. CDC event loop:
   a. Receive CdcOperation
   b. Call adapter.process_event(op)
   c. Update LSN
   d. Update lag metric
   e. If is_lagging(): log warning, emit metric
6. On checkpoint: save CdcPosition for resume
```

#### Fan-Out Decision Tree

```
How many nodes in the delta?

1 node (embedded):
  → Direct: Postgres → CDC → foyer
  → 1 replication slot, minimal overhead

2-5 nodes:
  → Direct: each node has its own CDC connection
  → N replication slots, N connections
  → Acceptable: Postgres handles 5 slots fine

6-50 nodes:
  → Kafka fan-out recommended
  → 1 CDC connection → Kafka compact topic
  → N Kafka consumers (cheap, scalable)
  → Kafka compaction ensures late joiners get full state

50+ nodes:
  → Kafka fan-out required
  → 1 CDC connection → Kafka compact topic
  → Kafka handles fan-out natively
  → Compact topic acts as persistent cache itself
```

#### CDC Event Translation (Core Logic)

```rust
// This is the ~30 lines of core logic:
fn translate(event: CdcOperation, cache: &FoyerMemoryCache) {
    match event {
        CdcOperation::Insert { key, value } => cache.insert(&key, value),
        CdcOperation::Update { key, value } => cache.insert(&key, value),
        CdcOperation::Delete { key }        => cache.remove(&key),
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| CDC connection lost | Postgres restart, network issue | Reconnect with exponential backoff, resume from LSN |
| Snapshot load failure | Source unreachable during startup | Retry snapshot, fail after N attempts |
| Cache insert failure | Cache capacity issue | Log warning, skip entry (will be fetched on-demand) |
| CDC lag exceeded | Source producing faster than consumer | Alert, consider Kafka fan-out upgrade |
| Kafka consumer error | Kafka broker down | Reconnect, resume from last committed offset |
| Corrupt CDC event | Source encoding issue | Log error, skip event, continue |
| LSN gap detected | Missed events due to slot drop | Full re-snapshot from source |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| CDC event processing | < 10μs per event | Single cache insert | `bench_cdc_process_event` |
| Batch processing | < 5μs per event | Batch of 1000 events | `bench_cdc_process_batch_1000` |
| Initial snapshot load | > 50K rows/sec | 256-byte rows | `bench_cdc_snapshot_load` |
| CDC lag (Direct) | < 100ms | Postgres → cache | Runtime metric |
| CDC lag (Kafka) | < 500ms | Postgres → Kafka → cache | Runtime metric |
| Metrics overhead | < 100ns per event | Lock + atomic increment | `bench_cdc_metrics_overhead` |

## Test Plan

### Unit Tests

- [ ] `test_cdc_adapter_process_insert` -- insert populates cache
- [ ] `test_cdc_adapter_process_update` -- update overwrites cache
- [ ] `test_cdc_adapter_process_delete` -- delete removes from cache
- [ ] `test_cdc_adapter_process_batch` -- batch of mixed events
- [ ] `test_cdc_adapter_process_batch_returns_count` -- correct count
- [ ] `test_cdc_adapter_metrics_inserts` -- insert count incremented
- [ ] `test_cdc_adapter_metrics_updates` -- update count incremented
- [ ] `test_cdc_adapter_metrics_deletes` -- delete count incremented
- [ ] `test_cdc_adapter_metrics_events_processed` -- total count
- [ ] `test_cdc_adapter_update_lag` -- lag computed correctly
- [ ] `test_cdc_adapter_is_lagging_below_threshold` -- false when under
- [ ] `test_cdc_adapter_is_lagging_above_threshold` -- true when over
- [ ] `test_cdc_adapter_update_lsn` -- LSN stored and retrieved
- [ ] `test_cdc_adapter_config_default` -- verify defaults
- [ ] `test_cdc_position_serialization` -- serde roundtrip

### Integration Tests

- [ ] `test_cdc_adapter_snapshot_then_stream` -- full startup flow
- [ ] `test_cdc_adapter_resume_from_lsn` -- restart, resume from saved position
- [ ] `test_cdc_adapter_concurrent_cdc_and_reads` -- cache reads during CDC writes
- [ ] `test_cdc_adapter_with_lookup_table` -- end-to-end with LookupTable trait
- [ ] `test_cdc_adapter_kafka_fanout` -- CDC through Kafka compact topic
- [ ] `test_cdc_adapter_replication_consistency` -- all nodes have same data

### Benchmarks

- [ ] `bench_cdc_process_event` -- Target: < 10μs per event
- [ ] `bench_cdc_process_batch_1000` -- Target: < 5μs per event amortized
- [ ] `bench_cdc_snapshot_load` -- Target: > 50K rows/sec
- [ ] `bench_cdc_metrics_overhead` -- Target: < 100ns per event

## Rollout Plan

1. **Step 1**: Define `CdcOperation` enum and `CdcCacheAdapterConfig` in `laminar-connectors/src/lookup/cdc_adapter.rs`
2. **Step 2**: Implement `CdcCacheAdapter` with `process_event()` and `process_batch()`
3. **Step 3**: Implement `load_snapshot()` for initial cache population
4. **Step 4**: Implement lag monitoring and LSN tracking
5. **Step 5**: Wire to existing Postgres CDC connector (F027)
6. **Step 6**: Unit tests
7. **Step 7**: Integration tests with Postgres testcontainer
8. **Step 8**: Benchmarks
9. **Step 9**: Code review and merge

## Open Questions

- [ ] Should the adapter handle schema changes (column addition/removal) or should CDC schema evolution be handled upstream in F027?
- [ ] Should we implement a "catch-up" mode where the adapter processes CDC events faster (larger batches, skip metrics) when lagging?
- [ ] For Kafka fan-out, should we use a dedicated Kafka topic per lookup table or multiplex multiple tables on one topic with key-based partitioning?
- [ ] Should the adapter support filtering CDC events (e.g., only replicate rows matching certain criteria)? This would reduce cache size but adds complexity.

## Completion Checklist

- [ ] `CdcOperation` enum defined
- [ ] `CdcCacheAdapterConfig` with defaults
- [ ] `CdcFanOutStrategy` enum
- [ ] `CdcCacheAdapter` implementation
- [ ] `process_event()` and `process_batch()` working
- [ ] `load_snapshot()` for initial population
- [ ] Lag monitoring and LSN tracking
- [ ] `CdcAdapterMetrics` struct
- [ ] `CdcPosition` for checkpoint/resume
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

- [F-LOOKUP-004: foyer In-Memory Cache](./F-LOOKUP-004-foyer-memory-cache.md) -- target cache
- [F027: PostgreSQL CDC Connector](../../phase-1/F027-postgresql-cdc.md) -- CDC source
- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- parent abstraction
- [Postgres Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html) -- CDC protocol
- [Kafka Log Compaction](https://kafka.apache.org/documentation/#compaction) -- fan-out mechanism
