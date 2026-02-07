# F-CONN-002B: Connector-Backed Table Population

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-002B |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-CONN-002 (Reference Tables), F025 (Kafka Source), F027 (PostgreSQL CDC), F028 (MySQL CDC) |
| **Crate** | laminar-db, laminar-connectors |
| **Created** | 2026-02-06 |
| **ADR** | [ADR-005: Tiered Reference Tables](../../adr/ADR-005-tiered-reference-tables.md) |

## Summary

Wire connector-backed table population into the pipeline loop so that
`CREATE TABLE ... WITH (connector = 'kafka')` (and PostgreSQL, MySQL) actually
loads data from the source into the `TableStore`. Currently F-CONN-002 registers
the connector config but the `_table_regs` parameter in `start_connector_pipeline()`
is unused. This feature closes that gap — snapshot loading, incremental CDC refresh,
table readiness gating, and checkpoint integration.

## Motivation

Without connector-backed population:
- `CREATE TABLE ... WITH (connector = 'kafka')` registers metadata but loads no data
- Tables can only be populated via INSERT statements (impractical for production)
- No snapshot-mode consumption from Kafka compacted topics
- No CDC refresh from PostgreSQL/MySQL for dimension tables
- Pipeline starts with empty tables, causing silent join misses

With connector-backed population:
- Kafka compacted topics are consumed to high water mark (snapshot), then incrementally
- PostgreSQL tables are snapshot-queried then kept fresh via CDC
- MySQL tables are snapshot-queried then kept fresh via binlog CDC
- Pipeline startup blocks until all reference tables are ready
- Lookup joins return correct results from the first event

## Current State

| Component | Status |
|-----------|--------|
| `TableStore` (PK upsert/delete/lookup) | Done (F-CONN-002) |
| `TableRegistration` in `ConnectorManager` | Done (F-CONN-002) |
| `ConnectorRegistry.create_source()` | Done (existing) |
| `KafkaSource` with snapshot-mode awareness | **NOT DONE** |
| `ReferenceTableSource` trait | **NOT DONE** |
| Pipeline loop table polling | **NOT DONE** |
| Table readiness gating | **NOT DONE** |
| Table checkpoint integration | **NOT DONE** |
| `sync_table_to_datafusion()` from pipeline | Done (F-CONN-002) |

## Design

### `ReferenceTableSource` Trait

New trait in `laminar-connectors` for table-specific source behavior:

```rust
/// A source that provides snapshot + incremental data for reference tables.
#[async_trait]
pub trait ReferenceTableSource: Send + Sync {
    /// Load the initial snapshot. Returns batches until the source is caught up.
    /// Returns `None` when the snapshot is complete.
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Whether the initial snapshot is complete.
    fn is_snapshot_complete(&self) -> bool;

    /// Poll for incremental changes after snapshot.
    /// Returns `None` when no changes are available (non-blocking).
    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Return the current source position for checkpointing.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Restore from a previous checkpoint (skip re-snapshot if possible).
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;

    /// Close the source connection.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}
```

### Kafka Table Source

Adapts existing `KafkaSource` for snapshot-mode consumption:

```rust
pub struct KafkaTableSource {
    inner: KafkaSource,
    snapshot_complete: bool,
    high_water_marks: HashMap<i32, i64>,  // partition → HWM offset
    current_offsets: HashMap<i32, i64>,   // partition → current offset
}
```

**Snapshot behavior**:
1. On `open()`: fetch high water marks for all partitions via `KafkaSource`
2. `poll_snapshot()`: consume messages, upsert by PK (last-writer-wins for
   compacted topics), return `None` when all partitions reach HWM
3. `poll_changes()`: continue consuming new messages (upserts + tombstone deletes)
4. **Tombstone handling**: Kafka null-value message = delete the PK from `TableStore`

### PostgreSQL Table Source

Adapts existing `PostgresCdcSource` with an initial `SELECT *` snapshot:

```rust
pub struct PostgresTableSource {
    snapshot_query: String,         // "SELECT * FROM {table}"
    cdc_source: Option<PostgresCdcSource>,  // WAL-based incremental after snapshot
    snapshot_complete: bool,
    snapshot_cursor: Option<...>,   // Cursor for batched snapshot reads
}
```

**Snapshot behavior**:
1. On `open()`: execute `SELECT * FROM {table}` with cursor-based batching
   (default 10,000 rows per batch to avoid memory spikes)
2. `poll_snapshot()`: fetch next batch from cursor until exhausted
3. After snapshot: create replication slot from the LSN captured before
   snapshot, start CDC via `PostgresCdcSource`
4. `poll_changes()`: delegate to `PostgresCdcSource.poll_batch()`

**SQL DDL**:
```sql
CREATE TABLE instruments (
    symbol VARCHAR PRIMARY KEY,
    company_name VARCHAR
) WITH (
    connector = 'postgres',
    host = 'localhost',
    port = '5432',
    database = 'mydb',
    table = 'instruments',
    refresh = 'cdc'           -- 'snapshot_only' | 'cdc' | 'periodic:60s'
);
```

### MySQL Table Source

Same pattern as PostgreSQL, using `MySqlCdcSource` for binlog-based refresh:

```rust
pub struct MySqlTableSource {
    snapshot_query: String,
    cdc_source: Option<MySqlCdcSource>,
    snapshot_complete: bool,
}
```

Uses `SELECT * FROM {table}` for snapshot, then binlog replication for CDC.
GTID-based positioning ensures no data loss between snapshot and CDC start.

### Pipeline Integration

In `start_connector_pipeline()`:

```rust
// 1. Build table sources from ConnectorManager registrations
let mut table_sources: Vec<(String, Box<dyn ReferenceTableSource>)> = Vec::new();
for (name, reg) in table_regs {
    if let Some(ref connector_type) = reg.connector_type {
        let source = registry.create_table_source(connector_type, &config)?;
        table_sources.push((name.clone(), source));
    }
}

// 2. Snapshot phase — block until all tables are ready
for (name, source) in &mut table_sources {
    while let Some(batch) = source.poll_snapshot().await? {
        let mut ts = self.table_store.lock();
        ts.upsert(name, &batch)?;
    }
    self.sync_table_to_datafusion(name)?;
    self.table_store.lock().set_ready(name, true);
}

// 3. Start stream sources (only after tables are ready)
// ... existing stream source startup ...

// 4. In the main loop, poll table sources for incremental changes
loop {
    // Poll stream sources (existing)
    for (name, source, _) in &mut stream_sources { ... }

    // Poll table sources (new)
    for (name, source) in &mut table_sources {
        if let Some(batch) = source.poll_changes().await? {
            let mut ts = self.table_store.lock();
            ts.upsert(name, &batch)?;
            drop(ts);
            self.sync_table_to_datafusion(name)?;
        }
    }

    // Process pipeline cycle (existing)
    ...
}
```

### Startup Sequencing

```
1. Parse DDL, register tables in ConnectorManager
2. Open table source connections
3. Run snapshot phase (block until all tables ready)
4. Open stream source connections
5. Start pipeline processing loop
6. Poll table sources for incremental changes alongside stream processing
```

This ensures lookup joins have complete data before the first stream event.

### Refresh Modes

```rust
pub enum RefreshMode {
    /// One-time snapshot at startup. No CDC.
    SnapshotOnly,
    /// Snapshot at startup, then CDC for incremental updates.
    SnapshotPlusCdc,
    /// Periodic full re-read at configured interval.
    Periodic { interval: Duration },
    /// Manual only — populated via INSERT statements.
    Manual,
}
```

Parsed from `WITH (refresh = 'cdc')` in DDL. Default: `SnapshotPlusCdc` for
connectors that support CDC (Kafka, PostgreSQL, MySQL), `SnapshotOnly` otherwise.

### Checkpoint Integration

Extend `PipelineCheckpoint` to include table source offsets:

```rust
pub struct PipelineCheckpoint {
    // ... existing fields ...
    pub table_offsets: HashMap<String, SerializableSourceCheckpoint>,
}
```

On recovery:
1. If table checkpoint exists, restore source to checkpointed offset
2. Resume CDC from that offset (skip full re-snapshot)
3. If no checkpoint, perform full snapshot

### ConnectorRegistry Extension

Add `create_table_source()` method to `ConnectorRegistry`:

```rust
impl ConnectorRegistry {
    pub fn create_table_source(
        &self,
        connector_type: &str,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn ReferenceTableSource>, ConnectorError> {
        match connector_type {
            "kafka" => Ok(Box::new(KafkaTableSource::new(config)?)),
            "postgres" => Ok(Box::new(PostgresTableSource::new(config)?)),
            "mysql" => Ok(Box::new(MySqlTableSource::new(config)?)),
            _ => Err(ConnectorError::UnsupportedConnector(connector_type.into())),
        }
    }
}
```

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `reference_table_source` | 3 | Trait default impls, checkpoint round-trip, close idempotent |
| `kafka_table_source` | 8 | Snapshot to HWM, incremental after snapshot, tombstone delete, checkpoint/restore, empty topic, multi-partition |
| `postgres_table_source` | 6 | Snapshot cursor batching, CDC after snapshot, LSN positioning, checkpoint/restore, empty table |
| `mysql_table_source` | 6 | Snapshot, binlog CDC, GTID positioning, checkpoint/restore, empty table |
| `pipeline_integration` | 7 | Startup sequencing, table readiness blocking, incremental refresh in loop, checkpoint with table offsets, recovery skip re-snapshot, multiple tables, table + stream together |
| `refresh_modes` | 4 | SnapshotOnly stops after snapshot, SnapshotPlusCdc continues, Periodic re-reads, Manual no auto-load |
| **Total** | **~34** | |

## Files

### New Files
- `crates/laminar-connectors/src/reference.rs` — `ReferenceTableSource` trait
- `crates/laminar-connectors/src/kafka/table_source.rs` — `KafkaTableSource`
- `crates/laminar-connectors/src/postgres/table_source.rs` — `PostgresTableSource`
- `crates/laminar-connectors/src/cdc/mysql/table_source.rs` — `MySqlTableSource`

### Modified Files
- `crates/laminar-connectors/src/registry.rs` — Add `create_table_source()`
- `crates/laminar-connectors/src/mod.rs` — Export `reference` module
- `crates/laminar-db/src/db.rs` — Wire table sources into pipeline loop
- `crates/laminar-db/src/pipeline_checkpoint.rs` — Add `table_offsets` field
- `crates/laminar-db/src/connector_manager.rs` — Parse `RefreshMode` from WITH options
