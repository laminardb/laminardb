# F-E2E-003: Idempotent Sink (Layer 3: Engine to Sink, Effectively-Once)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-E2E-003 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6c |
| **Effort** | L (5-10 days) |
| **Dependencies** | F-DCKP-001 (Barrier Protocol), F027B (Postgres Sink) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/sink/idempotent.rs` |

## Summary

Implements an idempotent sink strategy for effectively-once delivery without the overhead of distributed transactions. Instead of wrapping writes in a transaction (as in F-E2E-002), this approach uses deterministic deduplication keys so that duplicate writes overwrite identical data. If the engine replays events after a failure, the sink writes the same records with the same keys, resulting in an idempotent upsert that produces the same final state. This strategy works with any downstream system that supports upsert semantics (INSERT ON CONFLICT UPDATE in Postgres, ON DUPLICATE KEY UPDATE in MySQL, or key-based dedup in Kafka via log compaction). It is simpler and lower-latency than the transactional approach, but requires deterministic key generation and downstream upsert support.

## Goals

- Define `IdempotentSink` trait extending `SinkConnector` with dedup key derivation and upsert methods
- Implement deterministic dedup key derivation from event content and epoch
- Generate correct upsert SQL for Postgres (INSERT ... ON CONFLICT DO UPDATE)
- Generate correct upsert SQL for MySQL (INSERT ... ON DUPLICATE KEY UPDATE)
- Support dedup table schema for Kafka (topic + partition + offset as dedup key)
- Achieve lower latency than transactional sinks (no commit overhead per epoch)
- Support configurable dedup key strategies (content-based, epoch-based, composite)
- Provide clear comparison and guidance on when to use idempotent vs transactional

## Non-Goals

- Full exactly-once guarantees for sinks that do not support upsert (use F-E2E-002 for those)
- Dedup key generation for non-deterministic outputs (requires deterministic operators)
- Cross-sink dedup coordination (each sink instance handles its own dedup independently)
- Automatic dedup table creation (DDL is user responsibility; we generate SQL suggestions)
- Dedup key garbage collection in the downstream system

## Technical Design

### Architecture

**Ring**: Ring 1 (Background) -- upsert writes are async I/O, but with no transaction overhead.

**Crate**: `laminar-connectors`

**Module**: `laminar-connectors/src/sink/idempotent.rs`

The idempotent sink computes a deterministic key for each output event and writes it using upsert semantics. If the same event is written twice (due to replay after failure), the upsert overwrites the existing row with identical data, resulting in no net change. This eliminates the need for coordinated transactions while still preventing duplicate output.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  IDEMPOTENT WRITE FLOW                                                    │
│                                                                           │
│  Event → DedupKeyDerivation → Upsert                                    │
│                                                                           │
│  First write:                                                            │
│    INSERT INTO orders (dedup_key, amount, status)                        │
│    VALUES ('epoch7_order123', 99.99, 'filled')                           │
│    ON CONFLICT (dedup_key) DO UPDATE SET                                 │
│      amount = EXCLUDED.amount, status = EXCLUDED.status;                 │
│    → Row created                                                         │
│                                                                           │
│  Replay (duplicate) write:                                               │
│    INSERT INTO orders (dedup_key, amount, status)                        │
│    VALUES ('epoch7_order123', 99.99, 'filled')                           │
│    ON CONFLICT (dedup_key) DO UPDATE SET                                 │
│      amount = EXCLUDED.amount, status = EXCLUDED.status;                 │
│    → Row updated with IDENTICAL data (idempotent)                        │
│                                                                           │
│  Net result: exactly one visible copy of each event                      │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use async_trait::async_trait;

/// Trait for sink connectors using idempotent upsert for deduplication.
///
/// Instead of transactions, this approach relies on deterministic keys
/// and upsert semantics to achieve effectively-once delivery.
///
/// Implementations must ensure:
/// 1. Each output event has a unique, deterministic dedup key.
/// 2. Writing the same event twice with the same key produces the same result.
/// 3. The downstream system supports atomic upsert (INSERT ON CONFLICT or equivalent).
#[async_trait]
pub trait IdempotentSink: SinkConnector {
    /// Derive a dedup key for the given event and epoch.
    ///
    /// The key must be:
    /// - **Deterministic**: same event + same epoch = same key (always)
    /// - **Unique**: no two different events produce the same key
    /// - **Stable**: key does not depend on wall-clock time or random values
    ///
    /// Common strategies:
    /// - Content-based: hash of event payload
    /// - Epoch-based: epoch + sequence number within epoch
    /// - Composite: natural key from event fields + epoch
    fn derive_dedup_key(&self, event: &OutputEvent, epoch: u64) -> DedupKey;

    /// Write an event using upsert semantics with the derived dedup key.
    ///
    /// If the key already exists in the downstream system, the row/record
    /// is updated with the new values. If it does not exist, a new
    /// row/record is created.
    ///
    /// # Performance
    ///
    /// No transaction commit overhead. Write latency is the same as
    /// a regular INSERT/UPDATE (typically 1-5ms for database, < 1ms for Kafka).
    async fn upsert(&self, key: &DedupKey, event: &OutputEvent) -> Result<(), SinkError>;

    /// Write a batch of events using upsert semantics.
    ///
    /// More efficient than individual upserts for high-throughput scenarios.
    async fn upsert_batch(
        &self,
        events: &[(DedupKey, OutputEvent)],
    ) -> Result<(), SinkError>;

    /// Get the dedup key strategy configuration.
    fn dedup_strategy(&self) -> &DedupStrategy;
}

/// A deduplication key derived from event content and epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey {
    /// The key value as a string (for SQL compatibility).
    pub value: String,
    /// Components used to derive the key (for debugging).
    pub components: DedupKeyComponents,
}

/// Components that contributed to the dedup key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DedupKeyComponents {
    /// Key derived from event fields.
    ContentBased {
        /// Names of the event fields used.
        fields: Vec<String>,
    },
    /// Key derived from epoch and sequence.
    EpochBased {
        epoch: u64,
        sequence: u64,
    },
    /// Key derived from source offset.
    SourceOffset {
        source_id: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Key derived from composite natural key + epoch.
    Composite {
        natural_key: String,
        epoch: u64,
    },
}

/// Strategy for deriving dedup keys.
#[derive(Debug, Clone)]
pub enum DedupStrategy {
    /// Use specified event fields as the natural key.
    /// Suitable when events have a business primary key.
    ///
    /// Example: order_id is the natural key for order events.
    ContentFields {
        /// Field names to use as the dedup key.
        fields: Vec<String>,
        /// Whether to include epoch in the key.
        /// If true: key = hash(fields) + epoch (allows same key in different epochs)
        /// If false: key = hash(fields) (latest value always wins)
        include_epoch: bool,
    },

    /// Use epoch + sequence number within epoch.
    /// Suitable when events do not have a natural key.
    ///
    /// The sequence number is deterministic because it is derived
    /// from the event's position in the epoch's output stream.
    EpochSequence,

    /// Use the source offset as the dedup key.
    /// Suitable for pass-through pipelines where the source offset
    /// uniquely identifies each output event.
    SourceOffset,

    /// Custom key derivation function.
    Custom {
        /// Name of the custom strategy (for logging).
        name: String,
    },
}
```

### Data Structures

```rust
/// Postgres idempotent sink using INSERT ... ON CONFLICT DO UPDATE.
pub struct PostgresIdempotentSink {
    /// Database connection pool.
    pool: sqlx::PgPool,
    /// Target table name.
    table: String,
    /// Column mapping from event fields to table columns.
    columns: Vec<ColumnMapping>,
    /// Dedup key column name in the target table.
    dedup_column: String,
    /// Dedup strategy.
    strategy: DedupStrategy,
    /// Prepared upsert SQL template.
    upsert_sql: String,
    /// Metrics.
    metrics: IdempotentSinkMetrics,
}

impl PostgresIdempotentSink {
    /// Generate the upsert SQL for Postgres.
    ///
    /// Example output:
    /// ```sql
    /// INSERT INTO orders (dedup_key, order_id, amount, status)
    /// VALUES ($1, $2, $3, $4)
    /// ON CONFLICT (dedup_key) DO UPDATE SET
    ///   order_id = EXCLUDED.order_id,
    ///   amount = EXCLUDED.amount,
    ///   status = EXCLUDED.status
    /// ```
    fn generate_upsert_sql(
        table: &str,
        dedup_column: &str,
        columns: &[ColumnMapping],
    ) -> String {
        let all_columns: Vec<String> = std::iter::once(dedup_column.to_string())
            .chain(columns.iter().map(|c| c.column_name.clone()))
            .collect();

        let placeholders: Vec<String> = (1..=all_columns.len())
            .map(|i| format!("${}", i))
            .collect();

        let update_set: Vec<String> = columns
            .iter()
            .map(|c| format!("{} = EXCLUDED.{}", c.column_name, c.column_name))
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table,
            all_columns.join(", "),
            placeholders.join(", "),
            dedup_column,
            update_set.join(", "),
        )
    }

    /// Generate batch upsert SQL using UNNEST for Postgres.
    ///
    /// Example output:
    /// ```sql
    /// INSERT INTO orders (dedup_key, order_id, amount, status)
    /// SELECT * FROM UNNEST($1::text[], $2::text[], $3::float8[], $4::text[])
    /// ON CONFLICT (dedup_key) DO UPDATE SET
    ///   order_id = EXCLUDED.order_id,
    ///   amount = EXCLUDED.amount,
    ///   status = EXCLUDED.status
    /// ```
    fn generate_batch_upsert_sql(
        table: &str,
        dedup_column: &str,
        columns: &[ColumnMapping],
    ) -> String {
        let all_columns: Vec<String> = std::iter::once(dedup_column.to_string())
            .chain(columns.iter().map(|c| c.column_name.clone()))
            .collect();

        let unnest_params: Vec<String> = std::iter::once(format!("$1::text[]"))
            .chain(
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("${}::{}", i + 2, c.sql_type)),
            )
            .collect();

        let update_set: Vec<String> = columns
            .iter()
            .map(|c| format!("{} = EXCLUDED.{}", c.column_name, c.column_name))
            .collect();

        format!(
            "INSERT INTO {} ({}) SELECT * FROM UNNEST({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table,
            all_columns.join(", "),
            unnest_params.join(", "),
            dedup_column,
            update_set.join(", "),
        )
    }
}

/// MySQL idempotent sink using INSERT ... ON DUPLICATE KEY UPDATE.
pub struct MysqlIdempotentSink {
    /// Database connection pool.
    pool: sqlx::MySqlPool,
    /// Target table name.
    table: String,
    /// Column mapping.
    columns: Vec<ColumnMapping>,
    /// Dedup key column name.
    dedup_column: String,
    /// Dedup strategy.
    strategy: DedupStrategy,
    /// Prepared upsert SQL template.
    upsert_sql: String,
    /// Metrics.
    metrics: IdempotentSinkMetrics,
}

impl MysqlIdempotentSink {
    /// Generate the upsert SQL for MySQL.
    ///
    /// Example output:
    /// ```sql
    /// INSERT INTO orders (dedup_key, order_id, amount, status)
    /// VALUES (?, ?, ?, ?)
    /// ON DUPLICATE KEY UPDATE
    ///   order_id = VALUES(order_id),
    ///   amount = VALUES(amount),
    ///   status = VALUES(status)
    /// ```
    fn generate_upsert_sql(
        table: &str,
        dedup_column: &str,
        columns: &[ColumnMapping],
    ) -> String {
        let all_columns: Vec<String> = std::iter::once(dedup_column.to_string())
            .chain(columns.iter().map(|c| c.column_name.clone()))
            .collect();

        let placeholders: Vec<String> = all_columns.iter().map(|_| "?".to_string()).collect();

        let update_set: Vec<String> = columns
            .iter()
            .map(|c| format!("{} = VALUES({})", c.column_name, c.column_name))
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}",
            table,
            all_columns.join(", "),
            placeholders.join(", "),
            update_set.join(", "),
        )
    }
}

/// Kafka idempotent sink using dedup via key-based compaction.
///
/// For Kafka, the "upsert" is achieved by producing records with
/// the same Kafka record key. With log compaction enabled on the
/// target topic, only the latest value for each key is retained.
///
/// Dedup key: topic + partition + offset (from source) or content hash.
pub struct KafkaIdempotentSink {
    /// Kafka producer (idempotent, not transactional).
    producer: rdkafka::producer::FutureProducer,
    /// Target topic.
    topic: String,
    /// Dedup strategy.
    strategy: DedupStrategy,
    /// Metrics.
    metrics: IdempotentSinkMetrics,
}

/// Dedup table schema for Kafka deduplication via external table.
///
/// When Kafka log compaction is insufficient (e.g., downstream consumer
/// reads before compaction runs), a separate dedup table can be maintained.
///
/// Schema:
/// ```sql
/// CREATE TABLE kafka_dedup (
///     topic      VARCHAR NOT NULL,
///     partition  INT NOT NULL,
///     offset     BIGINT NOT NULL,
///     processed  BOOLEAN DEFAULT TRUE,
///     created_at TIMESTAMP DEFAULT NOW(),
///     PRIMARY KEY (topic, partition, offset)
/// );
/// ```
pub struct KafkaDedupTable {
    /// Database connection pool for the dedup table.
    pool: sqlx::PgPool,
    /// Table name.
    table: String,
}

impl KafkaDedupTable {
    /// Check if an offset has already been processed.
    pub async fn is_processed(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<bool, SinkError> {
        let row = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM kafka_dedup WHERE topic = $1 AND partition = $2 AND offset = $3)"
        )
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SinkError::DedupCheckFailed(e.to_string()))?;
        Ok(row)
    }

    /// Mark an offset as processed.
    pub async fn mark_processed(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), SinkError> {
        sqlx::query(
            "INSERT INTO kafka_dedup (topic, partition, offset) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
        )
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .execute(&self.pool)
        .await
        .map_err(|e| SinkError::DedupMarkFailed(e.to_string()))?;
        Ok(())
    }
}

/// Metrics for idempotent sink operations.
#[derive(Debug, Default)]
pub struct IdempotentSinkMetrics {
    /// Total upserts executed.
    pub upserts: u64,
    /// Upserts that were actual inserts (new rows).
    pub inserts: u64,
    /// Upserts that were updates (duplicate keys, idempotent overwrites).
    pub updates: u64,
    /// Total upsert failures.
    pub failures: u64,
    /// Average upsert latency in microseconds.
    pub avg_upsert_latency_us: f64,
}
```

### Algorithm/Flow

#### Dedup Key Derivation

```
Given: event E, epoch N, DedupStrategy S

ContentFields { fields: ["order_id"], include_epoch: false }:
  key = sha256(E.order_id)                    → "abc123"
  If order 123 replayed, same key, same upsert, same result.

ContentFields { fields: ["order_id"], include_epoch: true }:
  key = sha256(E.order_id + epoch)            → "abc123_epoch7"
  Allows same order in different epochs to coexist.

EpochSequence:
  key = f"{epoch}_{sequence_in_epoch}"        → "7_42"
  Deterministic because sequence is derived from processing order,
  and processing order is deterministic (same input + same state).

SourceOffset:
  key = f"{source_id}_{topic}_{partition}_{offset}" → "kafka_orders_0_12345"
  Most natural for pass-through pipelines.
```

#### Upsert Write Flow

```
1. Event arrives at idempotent sink
2. Derive dedup key: key = derive_dedup_key(event, epoch)
3. Build upsert SQL:
   INSERT INTO table (dedup_key, col1, col2, ...)
   VALUES (key, val1, val2, ...)
   ON CONFLICT (dedup_key) DO UPDATE SET col1=EXCLUDED.col1, ...
4. Execute upsert against downstream database
5. Result:
   - If key is new: INSERT (new row created)
   - If key exists: UPDATE (identical data overwrites, idempotent)
6. No transaction overhead, no commit latency
```

#### Comparison: Idempotent vs Transactional

```
| Aspect                | Transactional (F-E2E-002)    | Idempotent (F-E2E-003)       |
|-----------------------|------------------------------|------------------------------|
| Guarantee             | Exactly-once (strict)        | Effectively-once             |
| Mechanism             | 2PC / BEGIN+COMMIT           | Upsert / ON CONFLICT         |
| Commit latency        | 50-200ms per epoch           | 0ms (no transaction)         |
| Write latency         | Same (buffered in txn)       | Same (direct upsert)         |
| Downstream support    | Any (txn is transparent)     | Must support upsert          |
| Complexity            | High (txn lifecycle)         | Medium (key derivation)      |
| Failure recovery      | Abort uncommitted txn        | Replay produces same upserts |
| Key generation        | Not needed                   | Required (deterministic)     |
| Visibility            | All-or-nothing per epoch     | Each upsert immediately visible |
| Use when              | Strict exactly-once needed   | Upsert available, latency matters |
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SinkError::UpsertFailed` | Database rejected the upsert (constraint violation other than dedup key) | Log error, retry; if persistent, skip event and alert |
| `SinkError::DedupKeyCollision` | Two different events generated the same dedup key | Review key derivation strategy; increase key specificity |
| `SinkError::ConnectionLost` | Database connection dropped during upsert | Reconnect and retry; idempotent writes are safe to retry |
| `SinkError::DedupCheckFailed` | Dedup table query failed (for Kafka dedup table) | Retry; if persistent, fall back to at-least-once |
| `SinkError::DedupMarkFailed` | Failed to mark offset as processed in dedup table | Retry; may result in duplicate processing on next restart |
| `SinkError::NonDeterministicKey` | Key derivation produced different key for replayed event | Fatal: operator is non-deterministic; cannot use idempotent sink |
| `SinkError::SchemaEvolution` | Target table schema changed, upsert SQL no longer valid | Regenerate upsert SQL; may require migration |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Single upsert latency (Postgres, local) | < 2ms | `bench_postgres_upsert_single` |
| Batch upsert latency (Postgres, 1000 rows) | < 20ms | `bench_postgres_upsert_batch` |
| Single upsert latency (MySQL, local) | < 2ms | `bench_mysql_upsert_single` |
| Dedup key derivation | < 200ns | `bench_dedup_key_derivation` |
| Throughput (Postgres upsert) | > 50K rows/sec | `bench_postgres_upsert_throughput` |
| Throughput (Kafka idempotent) | > 200K records/sec | `bench_kafka_idempotent_throughput` |
| Overhead vs non-dedup writes | < 10% | `bench_upsert_vs_plain_insert` |
| Kafka dedup table check | < 1ms | `bench_kafka_dedup_table_check` |

## Test Plan

### Unit Tests

- [ ] `test_dedup_key_content_fields_deterministic` - Same event + same epoch = same key
- [ ] `test_dedup_key_content_fields_different_events_differ` - Different events produce different keys
- [ ] `test_dedup_key_epoch_sequence_deterministic` - Same epoch + sequence = same key
- [ ] `test_dedup_key_source_offset_deterministic` - Same source offset = same key
- [ ] `test_dedup_key_with_epoch_includes_epoch` - Epoch is part of key when configured
- [ ] `test_dedup_key_without_epoch_excludes_epoch` - Epoch not part of key when configured
- [ ] `test_postgres_upsert_sql_generation` - Correct ON CONFLICT DO UPDATE syntax
- [ ] `test_postgres_batch_upsert_sql_generation` - Correct UNNEST syntax
- [ ] `test_mysql_upsert_sql_generation` - Correct ON DUPLICATE KEY UPDATE syntax
- [ ] `test_kafka_dedup_table_schema` - Table schema matches expected DDL
- [ ] `test_idempotent_sink_metrics_tracking` - Upsert and failure counters
- [ ] `test_dedup_strategy_content_fields_config` - Strategy configuration parsing
- [ ] `test_dedup_strategy_epoch_sequence_config` - Strategy configuration parsing

### Integration Tests

- [ ] `test_postgres_idempotent_upsert_creates_row` - First write creates new row
- [ ] `test_postgres_idempotent_upsert_overwrites_identical` - Duplicate write is idempotent
- [ ] `test_postgres_idempotent_batch_upsert` - Batch of 1000 upserts
- [ ] `test_postgres_idempotent_replay_produces_same_result` - Simulate failure + replay, verify row count
- [ ] `test_mysql_idempotent_upsert_creates_row` - MySQL upsert creates row
- [ ] `test_mysql_idempotent_upsert_overwrites_identical` - MySQL duplicate is idempotent
- [ ] `test_kafka_idempotent_sink_key_based` - Kafka produce with same key
- [ ] `test_kafka_dedup_table_prevents_double_processing` - Offset checked before processing
- [ ] `test_idempotent_sink_with_checkpoint_recovery` - Full pipeline: events, checkpoint, crash, replay, verify no duplicates
- [ ] `test_idempotent_vs_transactional_equivalence` - Same input produces same output count

### Benchmarks

- [ ] `bench_dedup_key_derivation` - Target: < 200ns
- [ ] `bench_postgres_upsert_single` - Target: < 2ms
- [ ] `bench_postgres_upsert_batch` - Target: < 20ms (1000 rows)
- [ ] `bench_postgres_upsert_throughput` - Target: > 50K rows/sec
- [ ] `bench_mysql_upsert_single` - Target: < 2ms
- [ ] `bench_kafka_idempotent_throughput` - Target: > 200K records/sec
- [ ] `bench_upsert_vs_plain_insert` - Target: < 10% overhead
- [ ] `bench_kafka_dedup_table_check` - Target: < 1ms

## Rollout Plan

1. **Phase 1**: Define `IdempotentSink` trait, `DedupKey`, `DedupStrategy` types
2. **Phase 2**: Implement dedup key derivation for all strategies
3. **Phase 3**: Implement `PostgresIdempotentSink` with upsert SQL generation
4. **Phase 4**: Implement `MysqlIdempotentSink` with upsert SQL generation
5. **Phase 5**: Implement `KafkaIdempotentSink` with key-based dedup
6. **Phase 6**: Implement `KafkaDedupTable` for external dedup tracking
7. **Phase 7**: Unit tests for key derivation and SQL generation
8. **Phase 8**: Integration tests with real databases
9. **Phase 9**: Benchmarks and throughput validation
10. **Phase 10**: Documentation comparing idempotent vs transactional approaches

## Open Questions

- [ ] Should the dedup key include a hash of the output schema version to handle schema evolution? If the schema changes, the same event might produce different column values.
- [ ] Should there be a TTL-based cleanup mechanism for old dedup keys in the downstream table? Without cleanup, the dedup table grows unboundedly.
- [ ] For the Kafka dedup table approach, should the dedup check and mark be wrapped in a single SQL transaction? This adds latency but prevents race conditions.
- [ ] Should idempotent sinks support "soft deletes" (tombstone records) for delete events, or should deletes be handled differently?
- [ ] How to handle the case where an event's natural key is NULL? Should we generate a synthetic key or reject the event?

## Completion Checklist

- [ ] `IdempotentSink` trait defined
- [ ] `DedupKey` and `DedupStrategy` types implemented
- [ ] Dedup key derivation for ContentFields, EpochSequence, SourceOffset
- [ ] `PostgresIdempotentSink` with ON CONFLICT DO UPDATE
- [ ] `MysqlIdempotentSink` with ON DUPLICATE KEY UPDATE
- [ ] `KafkaIdempotentSink` with key-based dedup
- [ ] `KafkaDedupTable` for external dedup tracking
- [ ] Batch upsert support for Postgres (UNNEST)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with real databases
- [ ] Benchmarks meet throughput targets
- [ ] Comparison documentation (idempotent vs transactional)
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Postgres INSERT ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html) -- Upsert syntax
- [MySQL INSERT ON DUPLICATE KEY](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) -- MySQL upsert syntax
- [Kafka Log Compaction](https://kafka.apache.org/documentation/#compaction) -- Key-based dedup in Kafka
- [F-E2E-001: Source Offset Checkpoint](./F-E2E-001-source-offset-checkpoint.md) -- Source-side exactly-once
- [F-E2E-002: Transactional Sink](./F-E2E-002-transactional-sink.md) -- Transaction-based exactly-once
- [F027B: Postgres Sink](../../phase-3/F027B-postgres-sink.md) -- Base Postgres sink connector
- [F026: Kafka Sink](../../phase-3/F026-kafka-sink.md) -- Base Kafka sink connector
