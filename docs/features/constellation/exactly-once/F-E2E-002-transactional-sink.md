# F-E2E-002: Transactional Sink (Layer 3: Engine to Sink, Exactly-Once)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-E2E-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6c |
| **Effort** | XL (10-15 days) |
| **Dependencies** | F-DCKP-001 (Barrier Protocol), F026 (Kafka Sink), F027B (Postgres Sink) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/sink/transactional.rs` |

## Summary

Implements the third layer of LaminarDB's exactly-once semantics using Two-Phase Commit (2PC) for transactional sinks. When a checkpoint barrier arrives, the sink begins a transaction. All output events produced during the epoch are buffered within the transaction. When the checkpoint coordinator confirms that all operators have successfully snapshotted, the transaction is committed. If any failure occurs, the transaction is aborted. This guarantees that external consumers see each output event exactly once -- no duplicates, no gaps. Two concrete implementations are provided: `KafkaTransactionalSink` using Kafka's transaction API, and `JdbcTransactionalSink` using database BEGIN/COMMIT semantics. The trade-off is higher latency: each checkpoint adds 50-200ms of commit overhead.

## Goals

- Define `TransactionalSink` trait extending `SinkConnector` with transaction lifecycle methods
- Implement `KafkaTransactionalSink` using Kafka's producer transaction API (init, begin, send_offsets, commit, abort)
- Implement `JdbcTransactionalSink` using SQL BEGIN/INSERT/COMMIT/ROLLBACK
- Map epochs to transactions: one transaction per checkpoint epoch
- Buffer output events within a transaction during the epoch
- Commit transaction only after checkpoint coordinator confirms epoch completion
- Abort uncommitted transactions on failure or restart
- Support concurrent sink instances (one per partition) with independent transactions

## Non-Goals

- Idempotent/upsert-based deduplication (covered by F-E2E-003)
- At-least-once delivery (the default, no transactional overhead needed)
- Cross-sink distributed transactions (each sink manages its own transactions independently)
- Custom transaction protocols beyond Kafka and JDBC
- Sub-millisecond sink latency (transactional commit adds 50-200ms per epoch)

## Technical Design

### Architecture

**Ring**: Ring 1 (Background) -- transaction commit is async I/O. Event buffering happens in the boundary between Ring 0 output and Ring 1 I/O.

**Crate**: `laminar-connectors`

**Module**: `laminar-connectors/src/sink/transactional.rs`

The transactional sink wraps the regular sink connector with transaction lifecycle management. During normal processing, events are written to the sink within an open transaction. When a checkpoint barrier arrives, the sink stops accepting new events for the current epoch, waits for the checkpoint coordinator to signal success, and then commits. If the coordinator signals failure (or a timeout occurs), the transaction is aborted and all buffered events are discarded.

```
┌──────────────────────────────────────────────────────────────────────┐
│  EPOCH N                                                              │
│                                                                       │
│  Events: ──[e1]──[e2]──[e3]──│BARRIER│──[e4]──[e5]──│BARRIER│──     │
│                                                                       │
│  Transaction Timeline:                                                │
│                                                                       │
│  ┌────────────── Epoch N ──────────────┐┌──── Epoch N+1 ────────┐   │
│  │ begin_transaction()                  ││ begin_transaction()    │   │
│  │ write(e1)                            ││ write(e4)              │   │
│  │ write(e2)                            ││ write(e5)              │   │
│  │ write(e3)                            ││ ...                    │   │
│  │ <barrier received>                   ││                        │   │
│  │ <await checkpoint confirmation>      ││                        │   │
│  │ commit_transaction() ────────── 50-200ms latency               │   │
│  └──────────────────────────────────────┘└────────────────────────┘   │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use async_trait::async_trait;
use std::time::Duration;

/// Trait for sink connectors that support transactional exactly-once delivery.
///
/// **Existing SinkConnector methods**: The existing `SinkConnector` trait
/// (`laminar_connectors::connector`) already has `begin_epoch(epoch)`,
/// `pre_commit(epoch)`, and `commit_epoch(epoch)` methods with default
/// no-op implementations. `TransactionalSink` extends these with
/// stronger transaction semantics: `begin_transaction` replaces
/// `begin_epoch` with an actual transaction open, and `commit_transaction`
/// replaces `commit_epoch` with an atomic commit. The existing methods
/// are kept for at-least-once sinks; `TransactionalSink` overrides them
/// for exactly-once sinks.
///
/// Extends `SinkConnector` with transaction lifecycle methods. Each
/// checkpoint epoch maps to a single transaction: begin on epoch start,
/// write events during the epoch, commit on checkpoint success, abort
/// on failure.
///
/// Implementations must guarantee:
/// 1. Events written within a transaction are invisible to consumers
///    until commit.
/// 2. Abort discards all events written since begin.
/// 3. Commit is atomic -- either all events are visible or none.
/// 4. On restart, any uncommitted transactions from previous runs
///    are automatically aborted (fencing).
#[async_trait]
pub trait TransactionalSink: SinkConnector {
    /// Initialize the transactional producer.
    ///
    /// Called once at startup. For Kafka, this calls `init_transactions()`
    /// which fences out any previous instance with the same transactional.id.
    /// For JDBC, this may set up connection pool configuration.
    ///
    /// # Errors
    ///
    /// Returns `SinkError::TransactionInitFailed` if initialization fails.
    async fn init_transactions(&mut self) -> Result<(), SinkError>;

    /// Begin a new transaction for the given epoch.
    ///
    /// Called when a new epoch begins (after the previous epoch's barrier
    /// has been processed). All subsequent `write()` calls are part of
    /// this transaction until `commit_transaction()` or `abort_transaction()`.
    ///
    /// # Errors
    ///
    /// Returns `SinkError::TransactionBeginFailed` if a transaction
    /// is already in progress or the underlying system rejects the begin.
    async fn begin_transaction(&mut self, epoch: u64) -> Result<(), SinkError>;

    /// Commit the current transaction, making all buffered events visible.
    ///
    /// Called after the checkpoint coordinator confirms that all operators
    /// have successfully snapshotted for this epoch. This is the "Phase 2"
    /// of the 2PC protocol.
    ///
    /// For Kafka: calls `commit_transaction()` on the producer.
    /// For JDBC: issues `COMMIT` on the database connection.
    ///
    /// # Performance
    ///
    /// Expected latency: 50-200ms depending on the downstream system.
    ///
    /// # Errors
    ///
    /// Returns `SinkError::TransactionCommitFailed` if the commit fails.
    /// On commit failure, the epoch is considered failed and the engine
    /// must roll back to the previous checkpoint.
    async fn commit_transaction(&mut self, epoch: u64) -> Result<(), SinkError>;

    /// Abort the current transaction, discarding all buffered events.
    ///
    /// Called when the checkpoint coordinator signals failure, or when
    /// the sink detects an unrecoverable error during the epoch.
    ///
    /// # Errors
    ///
    /// Returns `SinkError::TransactionAbortFailed` if the abort itself fails.
    /// In this case, the fencing mechanism (on next restart) will clean up.
    async fn abort_transaction(&mut self, epoch: u64) -> Result<(), SinkError>;

    /// Get the current transaction state.
    fn transaction_state(&self) -> TransactionState;
}

/// Current state of the sink's transaction lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No transaction in progress. Ready to begin.
    Idle,
    /// Transaction is open. Events are being buffered.
    Active { epoch: u64 },
    /// Transaction is being committed (awaiting downstream confirmation).
    Committing { epoch: u64 },
    /// Transaction is being aborted.
    Aborting { epoch: u64 },
    /// Transaction commit failed; recovery required.
    Failed { epoch: u64 },
}

/// Configuration for transactional sink behavior.
#[derive(Debug, Clone)]
pub struct TransactionalSinkConfig {
    /// Timeout for transaction commit operations.
    pub commit_timeout: Duration,
    /// Timeout for transaction abort operations.
    pub abort_timeout: Duration,
    /// Maximum number of events to buffer per transaction before flushing.
    pub max_buffer_size: usize,
    /// Whether to automatically abort on startup (fence previous instances).
    pub auto_fence_on_startup: bool,
}

impl Default for TransactionalSinkConfig {
    fn default() -> Self {
        Self {
            commit_timeout: Duration::from_secs(30),
            abort_timeout: Duration::from_secs(10),
            max_buffer_size: 100_000,
            auto_fence_on_startup: true,
        }
    }
}
```

### Data Structures

```rust
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;

/// Kafka transactional sink using Kafka's exactly-once producer API.
///
/// Uses the Kafka transactions protocol:
/// 1. `init_transactions()` -- register transactional.id, fence zombies
/// 2. `begin_transaction()` -- start a new transaction
/// 3. `send()` -- produce records within the transaction
/// 4. `send_offsets_to_transaction()` -- commit consumer offsets atomically
/// 5. `commit_transaction()` -- commit all records and offsets
/// 6. `abort_transaction()` -- discard all records
///
/// The `transactional.id` must be unique per sink instance and stable
/// across restarts to enable fencing of zombie producers.
pub struct KafkaTransactionalSink {
    /// Kafka producer configured with transactional.id.
    producer: FutureProducer,
    /// Target topic for output events.
    topic: String,
    /// Current transaction state.
    state: TransactionState,
    /// Configuration.
    config: KafkaTransactionalConfig,
    /// Events buffered in the current transaction.
    buffer_count: usize,
    /// Metrics.
    metrics: TransactionalSinkMetrics,
}

/// Kafka-specific transactional configuration.
#[derive(Debug, Clone)]
pub struct KafkaTransactionalConfig {
    /// Kafka broker addresses.
    pub brokers: String,
    /// Transactional ID (must be unique per sink instance, stable across restarts).
    pub transactional_id: String,
    /// Transaction timeout (Kafka server-side).
    pub transaction_timeout: Duration,
    /// Generic transactional sink config.
    pub base: TransactionalSinkConfig,
}

impl KafkaTransactionalSink {
    /// Create a new Kafka transactional sink.
    pub fn new(config: KafkaTransactionalConfig, topic: String) -> Result<Self, SinkError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("transactional.id", &config.transactional_id)
            .set("transaction.timeout.ms", &config.transaction_timeout.as_millis().to_string())
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .map_err(|e| SinkError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            producer,
            topic,
            state: TransactionState::Idle,
            config,
            buffer_count: 0,
            metrics: TransactionalSinkMetrics::default(),
        })
    }
}

/// JDBC transactional sink using SQL BEGIN/COMMIT/ROLLBACK.
///
/// Supports Postgres and MySQL. Events are written as INSERT statements
/// within a database transaction. On commit, all inserts become visible
/// atomically. On abort, all inserts are rolled back.
pub struct JdbcTransactionalSink {
    /// Database connection pool.
    pool: sqlx::PgPool,
    /// Current database transaction handle.
    active_transaction: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
    /// Target table name.
    table: String,
    /// Column mapping from event fields to table columns.
    column_mapping: Vec<ColumnMapping>,
    /// Current transaction state.
    state: TransactionState,
    /// Configuration.
    config: JdbcTransactionalConfig,
    /// Events buffered in the current transaction.
    buffer_count: usize,
    /// Metrics.
    metrics: TransactionalSinkMetrics,
}

/// JDBC-specific transactional configuration.
#[derive(Debug, Clone)]
pub struct JdbcTransactionalConfig {
    /// Database connection string.
    pub connection_url: String,
    /// Maximum batch size for INSERT statements.
    pub batch_size: usize,
    /// Generic transactional sink config.
    pub base: TransactionalSinkConfig,
}

/// Mapping from event field to database column.
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Event field name or path.
    pub event_field: String,
    /// Database column name.
    pub column_name: String,
    /// SQL type for parameterized queries.
    pub sql_type: String,
}

/// Epoch-to-transaction mapping tracker.
///
/// Tracks the state of each epoch's transaction for monitoring
/// and recovery purposes.
pub struct EpochTransactionTracker {
    /// Map from epoch to transaction state.
    epochs: std::collections::HashMap<u64, EpochTransactionRecord>,
}

/// Record of a single epoch's transaction.
#[derive(Debug, Clone)]
pub struct EpochTransactionRecord {
    /// Epoch number.
    pub epoch: u64,
    /// Transaction state.
    pub state: TransactionState,
    /// Number of events written in this transaction.
    pub events_written: usize,
    /// Timestamp when transaction was opened.
    pub opened_at: std::time::Instant,
    /// Timestamp when transaction was committed/aborted (if completed).
    pub completed_at: Option<std::time::Instant>,
}

/// Metrics for transactional sink operations.
#[derive(Debug, Default)]
pub struct TransactionalSinkMetrics {
    /// Total transactions committed.
    pub commits: u64,
    /// Total transactions aborted.
    pub aborts: u64,
    /// Total events written across all transactions.
    pub events_written: u64,
    /// Average commit latency in microseconds.
    pub avg_commit_latency_us: f64,
    /// Maximum commit latency in microseconds.
    pub max_commit_latency_us: u64,
    /// Total commit failures.
    pub commit_failures: u64,
}
```

### Algorithm/Flow

#### Kafka Transactional Sink Flow

```
STARTUP:
1. Create FutureProducer with transactional.id
2. Call producer.init_transactions()
   - Registers this producer with the Kafka transaction coordinator
   - Fences out any previous zombie producer with the same transactional.id
   - Any uncommitted transactions from the zombie are aborted

EPOCH N:
1. Receive "epoch started" signal
2. Call producer.begin_transaction()
3. For each output event in epoch N:
   a. Serialize event to Kafka record
   b. Call producer.send(FutureRecord::to(&topic).payload(&bytes))
   c. Record is buffered in Kafka producer (not yet visible to consumers)
4. Receive checkpoint barrier for epoch N
5. Wait for checkpoint coordinator confirmation
6. On SUCCESS:
   a. Optionally: producer.send_offsets_to_transaction(consumer_offsets)
      (commits consumer offsets atomically with produced records)
   b. Call producer.commit_transaction()
   c. All records become visible to consumers atomically
   d. Transition to Idle state
7. On FAILURE:
   a. Call producer.abort_transaction()
   b. All records discarded
   c. Engine rolls back to previous checkpoint

COMMIT LATENCY BREAKDOWN:
- producer.commit_transaction() ~50-100ms
  - Kafka transaction coordinator writes commit marker
  - All partitions involved in the transaction are notified
  - Consumers see records only after all markers are written
```

#### JDBC Transactional Sink Flow

```
STARTUP:
1. Create connection pool
2. Verify table exists and column mapping is valid

EPOCH N:
1. Acquire connection from pool
2. Issue BEGIN (or use sqlx::Transaction::begin())
3. For each output event in epoch N:
   a. Map event fields to columns via ColumnMapping
   b. Build parameterized INSERT statement
   c. Execute INSERT within the transaction
   d. Batch inserts for efficiency (batch_size events per statement)
4. Receive checkpoint barrier for epoch N
5. Wait for checkpoint coordinator confirmation
6. On SUCCESS:
   a. Issue COMMIT on the transaction
   b. All rows become visible atomically
   c. Return connection to pool
7. On FAILURE:
   a. Issue ROLLBACK on the transaction
   b. All rows discarded
   c. Return connection to pool

COMMIT LATENCY BREAKDOWN:
- COMMIT ~10-50ms (local Postgres)
- COMMIT ~50-200ms (remote Postgres with fsync)
```

#### Failure Recovery

```
On restart after failure:

1. Kafka: init_transactions() automatically fences the zombie producer
   and aborts any uncommitted transactions from the previous instance.
   No manual cleanup needed.

2. JDBC: On startup, check for any abandoned transactions:
   a. Query pg_stat_activity for long-running transactions from our session
   b. Any found are from the previous failed instance
   c. They will be automatically rolled back by Postgres when the
      connection is cleaned up (connection timeout or explicit kill)
   d. No data from uncommitted transactions is visible

3. Resume from last committed checkpoint epoch:
   a. Epoch N was committed → consumers have seen all events up to N
   b. Epoch N+1 was in-progress → aborted, no events visible
   c. Engine replays from epoch N+1, sink writes events again
   d. Result: exactly-once delivery maintained
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SinkError::TransactionInitFailed` | Kafka transactional.id conflict or broker unreachable | Retry with backoff; check transactional.id uniqueness |
| `SinkError::TransactionBeginFailed` | Previous transaction not committed/aborted, or DB connection lost | Abort previous transaction first; reconnect |
| `SinkError::TransactionCommitFailed` | Kafka transaction coordinator timeout, DB commit failure | Abort transaction, notify coordinator of epoch failure; engine rolls back |
| `SinkError::TransactionAbortFailed` | Network failure during abort | Log error; fencing on next restart will clean up |
| `SinkError::WriteFailed` | Individual record/insert failed within transaction | Abort entire transaction; engine rolls back epoch |
| `SinkError::BufferOverflow` | More events in epoch than max_buffer_size | Flush batch within transaction (does not commit); continue |
| `SinkError::ConnectionLost` | Database or Kafka broker connection dropped | Attempt reconnect; abort current transaction; engine rolls back |
| `SinkError::FencedOut` | Another producer instance took over the transactional.id | Fatal: this instance is a zombie; shut down immediately |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Kafka transaction commit latency | 50-100ms | `bench_kafka_txn_commit` |
| JDBC transaction commit latency | 10-50ms (local) | `bench_jdbc_txn_commit` |
| Event write throughput (within txn) | > 100K events/sec | `bench_txn_write_throughput` |
| Transaction begin overhead | < 1ms | `bench_txn_begin` |
| Transaction abort latency | < 50ms | `bench_txn_abort` |
| Checkpoint overhead vs non-transactional | < 200ms per epoch | `bench_txn_vs_nontxn` |
| Memory per buffered event | < 1KB | Static analysis |

## Test Plan

### Unit Tests

- [ ] `test_transaction_state_transitions_idle_to_active` - Begin moves from Idle to Active
- [ ] `test_transaction_state_transitions_active_to_committing` - Commit moves from Active to Committing
- [ ] `test_transaction_state_transitions_active_to_aborting` - Abort moves from Active to Aborting
- [ ] `test_transaction_state_begin_while_active_returns_error` - Double begin rejected
- [ ] `test_transaction_state_commit_while_idle_returns_error` - Commit without begin rejected
- [ ] `test_epoch_tracker_records_transaction_lifecycle` - Track open/commit/abort per epoch
- [ ] `test_kafka_config_sets_transactional_id` - Kafka producer configured correctly
- [ ] `test_kafka_config_enables_idempotence` - enable.idempotence = true
- [ ] `test_jdbc_column_mapping_produces_correct_insert` - SQL INSERT generated from mapping
- [ ] `test_jdbc_batch_insert_groups_events` - Batch size respected
- [ ] `test_transactional_sink_config_defaults` - Default values sensible
- [ ] `test_metrics_increment_on_commit` - Commit counter incremented
- [ ] `test_metrics_increment_on_abort` - Abort counter incremented
- [ ] `test_metrics_commit_latency_tracked` - Latency histogram updated

### Integration Tests

- [ ] `test_kafka_transactional_sink_exactly_once` - Write events in transaction, commit, verify consumer sees all events exactly once
- [ ] `test_kafka_transactional_sink_abort_discards_events` - Write events, abort, verify consumer sees nothing
- [ ] `test_kafka_transactional_sink_fencing` - Start two producers with same transactional.id, verify fencing
- [ ] `test_kafka_transactional_sink_recovery_after_crash` - Kill producer mid-epoch, restart, verify no duplicates
- [ ] `test_jdbc_transactional_sink_commit_visible` - INSERT in transaction, COMMIT, verify rows visible
- [ ] `test_jdbc_transactional_sink_rollback_invisible` - INSERT in transaction, ROLLBACK, verify rows absent
- [ ] `test_jdbc_transactional_sink_recovery_after_crash` - Kill connection mid-epoch, verify rollback
- [ ] `test_transactional_sink_with_checkpoint_barrier` - Full pipeline: source -> operators -> transactional sink, checkpoint, verify
- [ ] `test_transactional_sink_multi_epoch` - Multiple epochs committed sequentially

### Benchmarks

- [ ] `bench_kafka_txn_commit` - Target: 50-100ms
- [ ] `bench_jdbc_txn_commit` - Target: 10-50ms
- [ ] `bench_txn_write_throughput` - Target: > 100K events/sec within transaction
- [ ] `bench_txn_begin` - Target: < 1ms
- [ ] `bench_txn_abort` - Target: < 50ms
- [ ] `bench_txn_vs_nontxn` - Measure exactly-once overhead vs at-least-once

## Rollout Plan

1. **Phase 1**: Define `TransactionalSink` trait and `TransactionState` enum
2. **Phase 2**: Implement `KafkaTransactionalSink` with rdkafka transactions
3. **Phase 3**: Implement `JdbcTransactionalSink` with sqlx transactions
4. **Phase 4**: Implement `EpochTransactionTracker` for monitoring
5. **Phase 5**: Wire transactional sink to checkpoint barrier protocol
6. **Phase 6**: Unit tests for state transitions and configuration
7. **Phase 7**: Integration tests with real Kafka and Postgres
8. **Phase 8**: Benchmarks and latency profiling
9. **Phase 9**: Fencing and recovery tests
10. **Phase 10**: Documentation and code review

## Open Questions

- [ ] Should the transactional sink support "pre-commit" hooks (e.g., custom validation before commit)? This would allow users to verify output before it becomes visible.
- [ ] Should we support nested transactions (savepoints) within an epoch for partial retry of failed batches? Or is all-or-nothing per epoch sufficient?
- [ ] How to handle the case where the Kafka transaction coordinator is temporarily unavailable? Should the engine buffer events and retry, or fail the epoch immediately?
- [ ] Should there be a configurable "commit delay" to batch multiple checkpoints into a single transaction (reducing commit overhead at the cost of higher latency)?
- [ ] Should the JDBC sink use prepared statements for INSERT performance, and if so, how to handle schema evolution?

## Completion Checklist

- [ ] `TransactionalSink` trait defined with full lifecycle methods
- [ ] `TransactionState` enum with all transitions
- [ ] `KafkaTransactionalSink` implemented with rdkafka
- [ ] `JdbcTransactionalSink` implemented with sqlx
- [ ] `EpochTransactionTracker` for monitoring
- [ ] `TransactionalSinkMetrics` integrated with laminar-observe
- [ ] Fencing logic (zombie detection) on startup
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with Kafka and Postgres
- [ ] Benchmarks meet latency targets
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Kafka Transactions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) -- KIP-98 Exactly Once Delivery
- [rdkafka Transactions](https://docs.rs/rdkafka/latest/rdkafka/producer/struct.BaseProducer.html) -- Rust Kafka client transaction API
- [sqlx Transactions](https://docs.rs/sqlx/latest/sqlx/struct.Transaction.html) -- Rust SQL transaction API
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Checkpoint barrier lifecycle
- [F-E2E-001: Source Offset Checkpoint](./F-E2E-001-source-offset-checkpoint.md) -- Source-side exactly-once
- [F-E2E-003: Idempotent Sink](./F-E2E-003-idempotent-sink.md) -- Lower-overhead alternative
- [F026: Kafka Sink](../../phase-3/F026-kafka-sink.md) -- Base Kafka sink connector
- [F027B: Postgres Sink](../../phase-3/F027B-postgres-sink.md) -- Base Postgres sink connector
