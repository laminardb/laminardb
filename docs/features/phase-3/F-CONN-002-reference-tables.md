# F-CONN-002: Reference Table Support

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-002 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (3-5 days) |
| **Dependencies** | F020 (Lookup Joins), F025 (Kafka Source) |

## Summary

Add first-class support for reference/dimension tables that can be loaded from
Kafka compacted topics or in-memory data, materialized as key-value state, and
used in lookup JOINs. Introduces `CREATE TABLE` DDL with `FROM KAFKA` connector
support and snapshot-mode consumption semantics.

## Motivation

Production streaming pipelines need dimension data for enrichment:

```sql
-- Enrich trades with instrument metadata
CREATE TABLE instruments (
    symbol VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    sector VARCHAR,
    exchange VARCHAR,
    lot_size INT
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic = 'instruments',
    format = 'avro',
    schema_registry_url = 'http://localhost:8081'
);

SELECT t.symbol, t.price, t.volume, i.sector, i.exchange
FROM trades t
JOIN instruments i ON t.symbol = i.symbol;
```

Currently, reference data can only be consumed as a stream source. There is no
snapshot-mode consumption, no key-based deduplication, and no materialized state
for lookup joins. The `LookupJoinOperator` exists but has no integrated table
loader.

## Design

### DDL: CREATE TABLE

```sql
CREATE TABLE <name> (
    <column> <type> [PRIMARY KEY],
    ...
) FROM KAFKA (
    brokers = '...',
    topic = '...',
    format = 'json' | 'avro',
    [schema_registry_url = '...']
);

-- Or: in-memory table populated via INSERT
CREATE TABLE <name> (
    <column> <type> [PRIMARY KEY],
    ...
);

INSERT INTO <name> VALUES (...), (...);
```

### Parser Changes (`streaming_parser.rs`)

Add `StreamingStatement::CreateTable` variant:

```rust
pub struct CreateTableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<String>,
    pub connector: Option<ConnectorDef>,  // FROM KAFKA (...)
    pub if_not_exists: bool,
}
```

Parse `CREATE TABLE` with optional `FROM <connector>` clause. If no connector,
it's an in-memory table populated via INSERT.

### Table Store

New `TableStore` in laminar-db for materialized reference tables:

```rust
pub struct TableStore {
    /// In-memory key-value state per table
    tables: HashMap<String, TableState>,
}

struct TableState {
    /// Schema of the table
    schema: SchemaRef,
    /// Primary key column name
    primary_key: String,
    /// Current state: primary_key → row data
    rows: FxHashMap<Vec<u8>, RecordBatch>,
    /// Total row count
    row_count: usize,
    /// Whether initial load is complete
    ready: bool,
}
```

### Kafka Snapshot Mode

For `CREATE TABLE ... FROM KAFKA`, implement snapshot consumption:

```rust
pub enum KafkaConsumptionMode {
    /// Existing: continuous stream, emit each message
    Stream,
    /// New: read to high water mark, then switch to incremental
    Snapshot,
}
```

**Snapshot mode behavior**:

1. **Initial load phase**: Consume from earliest offset until reaching the
   high water mark (end of each partition). Deduplicate by primary key (last
   writer wins). Mark table as `ready = true` when all partitions caught up.
2. **Incremental phase**: Continue consuming new messages. Apply upserts
   (insert or update by primary key) and deletes (tombstone = null value).
3. **Pipeline startup**: Block `db.start()` until all reference tables are
   `ready`. This ensures lookup joins have complete data before processing
   stream events.

**Startup sequencing**:
```
1. Open Kafka consumers for all TABLE sources
2. Consume to high water mark (snapshot phase)
3. Mark tables as ready
4. Open Kafka consumers for STREAM sources
5. Start pipeline processing
```

### Lookup JOIN Integration

Wire `TableStore` into the lookup join operator:

```rust
impl TableLoader for TableStore {
    fn lookup(&self, table: &str, key: &[u8]) -> Option<RecordBatch> {
        self.tables.get(table)?.rows.get(key).cloned()
    }
}
```

When the planner detects a JOIN between a stream and a table (not a stream),
configure `LookupJoinOperator` with `TableStore` as the lookup source instead
of the async pending-lookup pattern.

### Compacted Topic Handling

- **Kafka log compaction** ensures only the latest value per key is retained
- **Tombstones** (null value): delete the key from `TableState.rows`
- **Schema evolution**: If Avro schema changes, the new schema applies to
  subsequent rows. Existing rows retain their original schema (no backfill).

### Background Refresh

After initial snapshot, continue consuming in the background:

```rust
// In the pipeline loop, poll table sources alongside stream sources
for (name, table_source) in &mut table_sources {
    if let Some(batch) = table_source.poll_batch(100).await? {
        table_store.apply_upserts(name, &batch)?;
    }
}
```

Table updates are applied between pipeline cycles, ensuring lookup joins
always see a consistent snapshot within a single cycle.

### Checkpoint Integration

Include table state in pipeline checkpoints:

```rust
pub struct PipelineCheckpoint {
    // ...existing fields...
    pub table_offsets: HashMap<String, SourceCheckpoint>,  // Kafka offsets for tables
}
```

On recovery, tables resume from checkpointed offsets (not from earliest),
avoiding full re-snapshot.

### SHOW / DESCRIBE Support

```sql
SHOW TABLES;
DESCRIBE instruments;
DROP TABLE instruments;
```

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `streaming_parser` | 6 | CREATE TABLE parsing, PRIMARY KEY, FROM KAFKA, IF NOT EXISTS |
| `table_store` | 8 | Insert, upsert, delete (tombstone), lookup, schema, row count |
| `snapshot_mode` | 5 | Consume to HWM, ready flag, incremental after snapshot |
| `lookup_integration` | 6 | Stream JOIN table, multi-table lookups, table update during processing |
| `checkpoint` | 4 | Table offset persistence, recovery without re-snapshot |
| `ddl` | 3 | SHOW TABLES, DESCRIBE, DROP TABLE |

## Files

- `crates/laminar-sql/src/parser/streaming_parser.rs` — CREATE TABLE parsing
- `crates/laminar-db/src/table_store.rs` — NEW: TableStore, TableState
- `crates/laminar-db/src/db.rs` — Table lifecycle, snapshot startup, pipeline integration
- `crates/laminar-db/src/connector_manager.rs` — Table source management
- `crates/laminar-connectors/src/kafka/config.rs` — KafkaConsumptionMode::Snapshot
- `crates/laminar-connectors/src/kafka/source.rs` — Snapshot mode high water mark detection
