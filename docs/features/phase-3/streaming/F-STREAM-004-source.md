# F-STREAM-004: Source

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-004 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-STREAM-001, F-STREAM-002, F-STREAM-003 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

The Source is the user-facing API for pushing data into LaminarDB. It provides a type-safe, ergonomic interface for data ingestion with automatic channel management, watermark emission, and integration with the streaming SQL engine.

**Key Design Principle**: Source configuration has NO channel field - channel type (SPSC/MPSC) is automatically derived from usage.

## Goals

- Zero-allocation push path for hot path performance
- Automatic SPSC → MPSC upgrade on clone
- Arrow RecordBatch integration for zero-copy batch ingestion
- Configurable backpressure and wait strategies
- Watermark emission for event-time processing
- Integration with CREATE SOURCE SQL DDL

## Non-Goals

- User-specified channel types (always automatic)
- Serialization/deserialization (handled externally)
- Schema evolution (handled by schema registry)
- Remote/distributed sources (Phase 4+)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/source.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                        Source Architecture                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    USER APPLICATION                       │   │
│  │                                                           │   │
│  │  let source = db.source::<Trade>("trades")?;              │   │
│  │  source.push(trade)?;                                     │   │
│  │                                                           │   │
│  │  // Multi-producer: just clone                            │   │
│  │  let src2 = source.clone();  // Auto MPSC upgrade         │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                       Source<T>                           │   │
│  │                                                           │   │
│  │  • push(record) ─────────────┐                            │   │
│  │  • try_push(record) ─────────┤                            │   │
│  │  • push_batch(records) ──────┤                            │   │
│  │  • push_arrow(batch) ────────┼──► UpgradableChannel<T>    │   │
│  │  • watermark(ts) ────────────┘                            │   │
│  │                                                           │   │
│  │  Clone triggers SPSC → MPSC                               │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  STREAMING SQL ENGINE                     │   │
│  │                                                           │   │
│  │  SELECT symbol, SUM(qty) FROM trades                      │   │
│  │  GROUP BY TUMBLE(ts, INTERVAL '1 minute'), symbol         │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;

/// Marker trait for types that can be used as records.
///
/// Implement this for your data types to enable Source<YourType>.
pub trait Record: Send + Sized + 'static {
    /// Convert to Arrow RecordBatch (single row).
    fn to_record_batch(&self) -> RecordBatch;

    /// Get the Arrow schema for this record type.
    fn schema() -> arrow::datatypes::SchemaRef;

    /// Extract event timestamp (for event-time processing).
    /// Returns None for processing-time semantics.
    fn event_time(&self) -> Option<i64> {
        None
    }
}

/// Source configuration.
///
/// **NOTE**: No `channel` field - channel type is automatically derived:
/// - Single Source → SPSC
/// - source.clone() → Auto-upgrade to MPSC
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Buffer size (must be power of 2). Default: 65536
    pub buffer_size: usize,

    /// Backpressure strategy when buffer is full. Default: Block
    pub backpressure: Backpressure,

    /// Wait strategy for blocking operations. Default: SpinYield(100)
    pub wait_strategy: WaitStrategy,

    /// Checkpoint interval override. None = use database default
    pub checkpoint_interval: Option<Duration>,

    /// WAL mode override. None = use database default
    pub wal_mode: Option<WalMode>,

    /// Watermark configuration for event-time processing
    pub watermark: Option<WatermarkConfig>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            buffer_size: 65536,
            backpressure: Backpressure::Block,
            wait_strategy: WaitStrategy::SpinYield(100),
            checkpoint_interval: None,
            wal_mode: None,
            watermark: None,
        }
    }
}

/// Watermark configuration.
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Column name for event timestamp
    pub column: String,
    /// Maximum out-of-order delay
    pub delay: Duration,
}

/// WAL mode for durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    /// Background flush (lower latency, small window for data loss)
    Async,
    /// Synchronous flush (higher latency, no data loss)
    Sync,
}

/// Source handle for pushing data into LaminarDB.
///
/// # Cloning and Multi-Producer
///
/// Cloning a Source automatically upgrades the underlying channel
/// from SPSC to MPSC. This is transparent to user code:
///
/// ```rust
/// let source = db.source::<Trade>("trades")?;
/// let src2 = source.clone();  // Now MPSC
///
/// // Both can push concurrently
/// std::thread::spawn(move || src1.push(trade1));
/// std::thread::spawn(move || src2.push(trade2));
/// ```
pub struct Source<T: Record> {
    inner: Arc<SourceInner<T>>,
}

struct SourceInner<T: Record> {
    /// Name of the source (matches SQL CREATE SOURCE)
    name: String,

    /// Configuration
    config: SourceConfig,

    /// The underlying upgradable channel
    channel: UpgradableChannel<SourceMessage<T>>,

    /// Current watermark value
    watermark: AtomicI64,

    /// Schema for this source
    schema: arrow::datatypes::SchemaRef,
}

/// Messages sent through the source channel.
enum SourceMessage<T> {
    /// A single record
    Record(T),
    /// A batch of records
    Batch(Vec<T>),
    /// An Arrow RecordBatch (zero-copy)
    Arrow(RecordBatch),
    /// Watermark update
    Watermark(i64),
}
```

### API/Interface

```rust
impl<T: Record> Source<T> {
    /// Push a single record into the source.
    ///
    /// This is the hot path for data ingestion. In SPSC mode,
    /// this is a simple sequence update with ~20-50ns latency.
    ///
    /// # Backpressure
    ///
    /// Behavior when buffer is full depends on configuration:
    /// - `Block`: Wait until space is available (default)
    /// - `DropOldest`: Overwrite oldest unread record
    /// - `Reject`: Return error immediately
    ///
    /// # Example
    ///
    /// ```rust
    /// let source = db.source::<Trade>("trades")?;
    ///
    /// for trade in trades {
    ///     source.push(trade)?;
    /// }
    /// ```
    pub fn push(&self, record: T) -> Result<(), SourceError> {
        self.inner.channel.producer().push(SourceMessage::Record(record))
            .map_err(|e| match e {
                ChannelError::Closed(_) => SourceError::Closed,
                ChannelError::Full(_) => SourceError::Full,
            })
    }

    /// Try to push a record without blocking.
    ///
    /// Returns the record if the buffer is full.
    ///
    /// # Example
    ///
    /// ```rust
    /// match source.try_push(trade) {
    ///     Ok(()) => println!("Pushed"),
    ///     Err(TryPushError::Full(trade)) => {
    ///         // Handle backpressure
    ///         backlog.push(trade);
    ///     }
    /// }
    /// ```
    pub fn try_push(&self, record: T) -> Result<(), TryPushError<T>> {
        self.inner.channel.producer().try_push(SourceMessage::Record(record))
            .map_err(|e| match e {
                TryPushError::Full(SourceMessage::Record(r)) => TryPushError::Full(r),
                _ => unreachable!(),
            })
    }

    /// Push a batch of records.
    ///
    /// More efficient than individual pushes due to reduced
    /// synchronization overhead. Useful for micro-batched ingestion.
    ///
    /// # Example
    ///
    /// ```rust
    /// let batch: Vec<Trade> = receive_trades();
    /// source.push_batch(&batch)?;
    /// ```
    pub fn push_batch(&self, records: &[T]) -> Result<(), SourceError>
    where
        T: Clone,
    {
        if records.is_empty() {
            return Ok(());
        }

        // For small batches, push individually
        if records.len() <= 4 {
            for record in records {
                self.push(record.clone())?;
            }
            return Ok(());
        }

        // For larger batches, send as batch message
        self.inner.channel.producer()
            .push(SourceMessage::Batch(records.to_vec()))
            .map_err(|e| match e {
                ChannelError::Closed(_) => SourceError::Closed,
                ChannelError::Full(_) => SourceError::Full,
            })
    }

    /// Push an Arrow RecordBatch directly (zero-copy).
    ///
    /// This is the most efficient path for bulk ingestion when
    /// data is already in Arrow format.
    ///
    /// # Example
    ///
    /// ```rust
    /// let batch: RecordBatch = read_parquet_batch()?;
    /// source.push_arrow(batch)?;
    /// ```
    pub fn push_arrow(&self, batch: RecordBatch) -> Result<(), SourceError> {
        // Validate schema matches
        if batch.schema() != self.inner.schema {
            return Err(SourceError::SchemaMismatch {
                expected: self.inner.schema.clone(),
                got: batch.schema(),
            });
        }

        self.inner.channel.producer()
            .push(SourceMessage::Arrow(batch))
            .map_err(|e| match e {
                ChannelError::Closed(_) => SourceError::Closed,
                ChannelError::Full(_) => SourceError::Full,
            })
    }

    /// Emit a watermark for event-time processing.
    ///
    /// Watermarks indicate that no events with timestamp < watermark
    /// will arrive. This triggers window computations.
    ///
    /// # Example
    ///
    /// ```rust
    /// // After processing events up to time T
    /// source.watermark(T - allowed_lateness);
    /// ```
    pub fn watermark(&self, timestamp: i64) {
        // Update local watermark (monotonic)
        let current = self.inner.watermark.load(Ordering::Acquire);
        if timestamp > current {
            self.inner.watermark.store(timestamp, Ordering::Release);

            // Send watermark through channel
            let _ = self.inner.channel.producer()
                .try_push(SourceMessage::Watermark(timestamp));
        }
    }

    /// Check if there's space available for pushing.
    ///
    /// Useful for implementing custom backpressure logic.
    #[inline]
    pub fn has_capacity(&self) -> bool {
        !self.inner.channel.is_full()
    }

    /// Get the current watermark value.
    #[inline]
    pub fn current_watermark(&self) -> i64 {
        self.inner.watermark.load(Ordering::Acquire)
    }

    /// Get the source name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Get the source schema.
    #[inline]
    pub fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.inner.schema
    }

    /// Get the number of active producers.
    #[inline]
    pub fn producer_count(&self) -> usize {
        self.inner.channel.producer_count()
    }

    /// Check if the source is in MPSC mode.
    #[inline]
    pub fn is_mpsc(&self) -> bool {
        self.inner.channel.is_mpsc()
    }

    /// Close the source.
    ///
    /// After closing, all producers will receive errors on push.
    /// The consumer will drain remaining items.
    pub fn close(&self) {
        self.inner.channel.close();
    }
}

/// Clone triggers automatic SPSC → MPSC upgrade.
impl<T: Record> Clone for Source<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Source errors.
#[derive(Debug)]
pub enum SourceError {
    /// Source is closed
    Closed,
    /// Buffer is full (with Reject backpressure)
    Full,
    /// Schema mismatch for Arrow batch
    SchemaMismatch {
        expected: arrow::datatypes::SchemaRef,
        got: arrow::datatypes::SchemaRef,
    },
}
```

### LaminarDB Integration

```rust
impl LaminarDB {
    /// Get a source handle for pushing data.
    ///
    /// The source must have been created via CREATE SOURCE SQL.
    ///
    /// # Example
    ///
    /// ```rust
    /// db.execute("CREATE SOURCE trades (
    ///     symbol VARCHAR,
    ///     price DOUBLE,
    ///     qty BIGINT,
    ///     ts TIMESTAMP
    /// )")?;
    ///
    /// let source = db.source::<Trade>("trades")?;
    /// source.push(trade)?;
    /// ```
    pub fn source<T: Record>(&self, name: &str) -> Result<Source<T>, Error> {
        self.source_with_config(name, SourceConfig::default())
    }

    /// Get a source handle with custom configuration.
    pub fn source_with_config<T: Record>(
        &self,
        name: &str,
        config: SourceConfig,
    ) -> Result<Source<T>, Error> {
        // Validate source exists
        let source_meta = self.catalog.get_source(name)
            .ok_or(Error::SourceNotFound(name.to_string()))?;

        // Validate schema matches
        let expected_schema = T::schema();
        if source_meta.schema != expected_schema {
            return Err(Error::SchemaMismatch {
                expected: expected_schema,
                got: source_meta.schema.clone(),
            });
        }

        // Create or get existing source
        self.sources.get_or_create(name, config)
    }
}
```

### SQL DDL Integration

```sql
-- CREATE SOURCE syntax (no channel specification)
CREATE SOURCE trades (
    symbol VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    qty BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    WATERMARK FOR ts AS ts - INTERVAL '100 milliseconds'
) WITH (
    buffer_size = 131072,
    backpressure = 'block',
    wait_strategy = 'spin_yield',
    checkpoint_interval = '10 seconds'
);

-- Minimal (all defaults)
CREATE SOURCE simple_events (
    id BIGINT,
    data VARCHAR
);
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SourceError::Closed` | Source was closed | None, recreate source |
| `SourceError::Full` | Buffer full with Reject backpressure | Retry or drop |
| `SourceError::SchemaMismatch` | Arrow batch schema doesn't match | Fix schema |
| `Error::SourceNotFound` | CREATE SOURCE not executed | Execute DDL |

## Test Plan

### Unit Tests

- [ ] `test_source_push_single`
- [ ] `test_source_try_push_when_empty`
- [ ] `test_source_try_push_when_full`
- [ ] `test_source_push_batch`
- [ ] `test_source_push_arrow`
- [ ] `test_source_watermark`
- [ ] `test_source_clone_upgrades_to_mpsc`
- [ ] `test_source_close`
- [ ] `test_source_schema_validation`
- [ ] `test_backpressure_block`
- [ ] `test_backpressure_drop_oldest`
- [ ] `test_backpressure_reject`

### Integration Tests

- [ ] Source created via SQL DDL
- [ ] Push data through Source, read via Sink
- [ ] Multi-producer stress test
- [ ] Watermark propagation to window operators

### Property Tests

- [ ] `prop_all_pushed_records_consumed`
- [ ] `prop_watermarks_monotonic`
- [ ] `prop_clone_preserves_data`

### Benchmarks

- [ ] `bench_source_push_single` - Target: < 100ns
- [ ] `bench_source_push_batch_1000` - Target: < 10ns/item
- [ ] `bench_source_push_arrow` - Target: < 1μs/batch
- [ ] `bench_source_mpsc_2_producers` - Target: < 200ns

## Rollout Plan

1. **Phase 1**: Core Source implementation
2. **Phase 2**: Arrow RecordBatch support
3. **Phase 3**: Watermark emission
4. **Phase 4**: SQL DDL integration
5. **Phase 5**: Benchmarks + optimization

## Open Questions

- [x] **Batch semantics**: Should batch push be atomic?
  - Decision: No, partial success is acceptable for performance
- [ ] **Arrow schema validation**: Validate once or per-batch?
  - Leaning: Once at source creation, per-batch is optional

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/source.rs`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] SQL DDL parser integration
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

**Zero-Config Channel**: The Source API intentionally has no channel configuration. Channel type is derived:
- Initial Source → SPSC (fastest path)
- `source.clone()` → Automatic MPSC upgrade
- This removes a decision from users while optimizing for the common case

**Record Trait**: The `Record` trait enables type-safe sources. Users implement it for their types to get compile-time schema validation.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-002: SPSC Channel](F-STREAM-002-spsc-channel.md)
- [F-STREAM-003: MPSC Auto-Upgrade](F-STREAM-003-mpsc-upgrade.md)
- [F-STREAM-007: SQL DDL](F-STREAM-007-sql-ddl.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
