# F-STREAM-005: Sink

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-005 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001, F-STREAM-006 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

The Sink is the output endpoint of a streaming query. It receives processed results from materialized views and allows users to subscribe for consumption. Sink channel type is automatically derived from the query plan - never user-specified.

**Key Design Principle**: Sink channel type is derived from topology:
- 1 subscriber → SPSC
- N subscribers → Broadcast

## Goals

- Type-safe output subscription
- Automatic channel type derivation from query plan
- Arrow RecordBatch output for zero-copy consumption
- Multiple subscribers via Broadcast (when needed)
- Integration with CREATE SINK SQL DDL

## Non-Goals

- User-specified channel types (always automatic)
- External sink connectors (Phase 4+: Kafka, Delta Lake)
- Transactional sink semantics (covered by F023)
- Schema transformation (output matches MV schema)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) for data flow, Ring 1 for subscriber management
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/sink.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                        Sink Architecture                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  STREAMING SQL ENGINE                     │   │
│  │                                                           │   │
│  │  SELECT symbol, SUM(qty) as volume                        │   │
│  │  FROM trades                                              │   │
│  │  GROUP BY TUMBLE(ts, INTERVAL '1 minute'), symbol         │   │
│  │                                                           │   │
│  │  Query Plan Analysis:                                     │   │
│  │  └─ 1 sink consumer → SPSC                                │   │
│  │  └─ N sink consumers → Broadcast                          │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼ RecordBatch output                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                        Sink<T>                            │   │
│  │                                                           │   │
│  │  Channel type: Auto-derived from plan                     │   │
│  │  • 1 consumer → SPSC                                      │   │
│  │  • N consumers → Broadcast                                │   │
│  │                                                           │   │
│  │  subscribe() ──► Subscription<T>                          │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    USER APPLICATION                       │   │
│  │                                                           │   │
│  │  let sink = db.sink::<OHLC>("ohlc_output")?;              │   │
│  │  for batch in sink.subscribe() {                          │   │
│  │      process(batch);                                      │   │
│  │  }                                                        │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Sink channel mode - derived from query plan, not user-specified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    /// Single consumer (default, fastest)
    Spsc,
    /// Multiple consumers (broadcast to all)
    Broadcast,
}

/// Sink configuration.
///
/// **NOTE**: No `channel` field - channel type is automatically derived
/// from the query plan based on the number of consumers.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Buffer size per consumer. Default: 65536
    pub buffer_size: usize,

    /// Wait strategy for subscriber blocking. Default: SpinYield(100)
    pub wait_strategy: WaitStrategy,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            buffer_size: 65536,
            wait_strategy: WaitStrategy::SpinYield(100),
        }
    }
}

/// Sink handle for consuming query results.
///
/// Sinks receive output from materialized views and allow
/// subscription for consumption.
///
/// # Channel Type Derivation
///
/// The sink channel type is derived from the query plan:
/// - Single consumer (1 subscription) → SPSC
/// - Multiple consumers (N subscriptions) → Broadcast
///
/// This is transparent to users - just call `subscribe()`.
pub struct Sink<T: Record> {
    inner: Arc<SinkInner<T>>,
}

struct SinkInner<T: Record> {
    /// Name of the sink (matches SQL CREATE SINK)
    name: String,

    /// Source materialized view name
    source_view: String,

    /// Configuration
    config: SinkConfig,

    /// Current mode (auto-derived)
    mode: AtomicU8,

    /// Number of active subscribers
    subscriber_count: AtomicUsize,

    /// For SPSC mode: single consumer channel
    spsc_channel: Option<SpscConsumer<RecordBatch>>,

    /// For Broadcast mode: broadcast channel
    broadcast_channel: Option<BroadcastChannel<RecordBatch>>,

    /// Schema for this sink
    schema: arrow::datatypes::SchemaRef,
}

/// Broadcast channel for multi-consumer scenarios.
struct BroadcastChannel<T> {
    /// Single producer writes here
    buffer: RingBuffer<T>,

    /// Per-subscriber read cursors
    cursors: Vec<CachePadded<AtomicU64>>,

    /// Maximum number of subscribers
    max_subscribers: usize,
}
```

### API/Interface

```rust
impl<T: Record> Sink<T> {
    /// Create a new subscription to this sink.
    ///
    /// The first subscription uses SPSC mode (fastest).
    /// Additional subscriptions trigger upgrade to Broadcast.
    ///
    /// # Example
    ///
    /// ```rust
    /// let sink = db.sink::<OHLC>("ohlc_output")?;
    ///
    /// for batch in sink.subscribe() {
    ///     for row in batch.iter() {
    ///         println!("{:?}", row);
    ///     }
    /// }
    /// ```
    pub fn subscribe(&self) -> Subscription<T> {
        let prev_count = self.inner.subscriber_count.fetch_add(1, Ordering::SeqCst);

        // First subscriber: use SPSC
        if prev_count == 0 {
            self.inner.mode.store(SinkMode::Spsc as u8, Ordering::Release);
            return Subscription::new_spsc(self.inner.clone());
        }

        // Additional subscribers: ensure Broadcast mode
        let current_mode = SinkMode::from(self.inner.mode.load(Ordering::Acquire));
        if current_mode == SinkMode::Spsc {
            self.upgrade_to_broadcast();
        }

        // Create broadcast subscription with unique cursor
        Subscription::new_broadcast(self.inner.clone(), prev_count)
    }

    /// Get the number of active subscribers.
    #[inline]
    pub fn subscriber_count(&self) -> usize {
        self.inner.subscriber_count.load(Ordering::Acquire)
    }

    /// Get the current channel mode.
    #[inline]
    pub fn mode(&self) -> SinkMode {
        SinkMode::from(self.inner.mode.load(Ordering::Acquire))
    }

    /// Get the sink name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Get the schema.
    #[inline]
    pub fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.inner.schema
    }

    /// Get the source materialized view name.
    #[inline]
    pub fn source_view(&self) -> &str {
        &self.inner.source_view
    }

    /// Upgrade from SPSC to Broadcast mode.
    fn upgrade_to_broadcast(&self) {
        // Similar to MPSC upgrade, but for multi-consumer
        // This migrates the existing consumer to a broadcast cursor
        std::sync::atomic::fence(Ordering::SeqCst);
        self.inner.mode.store(SinkMode::Broadcast as u8, Ordering::SeqCst);
    }
}

// Sink is Clone for convenience, shares the same inner
impl<T: Record> Clone for Sink<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
```

### LaminarDB Integration

```rust
impl LaminarDB {
    /// Get a sink handle for consuming query results.
    ///
    /// The sink must have been created via CREATE SINK SQL.
    ///
    /// # Example
    ///
    /// ```rust
    /// db.execute("CREATE SINK ohlc_output FROM ohlc_1min")?;
    ///
    /// let sink = db.sink::<OHLC>("ohlc_output")?;
    /// for batch in sink.subscribe() {
    ///     process(batch);
    /// }
    /// ```
    pub fn sink<T: Record>(&self, name: &str) -> Result<Sink<T>, Error> {
        self.sink_with_config(name, SinkConfig::default())
    }

    /// Get a sink handle with custom configuration.
    pub fn sink_with_config<T: Record>(
        &self,
        name: &str,
        config: SinkConfig,
    ) -> Result<Sink<T>, Error> {
        // Validate sink exists
        let sink_meta = self.catalog.get_sink(name)
            .ok_or(Error::SinkNotFound(name.to_string()))?;

        // Validate schema matches
        let expected_schema = T::schema();
        if sink_meta.schema != expected_schema {
            return Err(Error::SchemaMismatch {
                expected: expected_schema,
                got: sink_meta.schema.clone(),
            });
        }

        self.sinks.get_or_create(name, config)
    }
}
```

### Internal Writer Interface

```rust
/// Internal interface for the streaming engine to write to sinks.
/// This is NOT part of the public API.
impl<T: Record> SinkInner<T> {
    /// Write a RecordBatch to the sink.
    ///
    /// Called by the streaming engine when a window closes or
    /// a materialized view emits results.
    pub(crate) fn write(&self, batch: RecordBatch) -> Result<(), SinkError> {
        match self.mode() {
            SinkMode::Spsc => {
                self.spsc_channel.as_ref()
                    .ok_or(SinkError::NotInitialized)?
                    .push(batch)
                    .map_err(|_| SinkError::Full)
            }
            SinkMode::Broadcast => {
                self.broadcast_channel.as_ref()
                    .ok_or(SinkError::NotInitialized)?
                    .broadcast(batch)
                    .map_err(|_| SinkError::Full)
            }
        }
    }

    /// Write with watermark.
    pub(crate) fn write_with_watermark(
        &self,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<(), SinkError> {
        self.write(batch)?;
        self.update_watermark(watermark);
        Ok(())
    }
}
```

### SQL DDL Integration

```sql
-- CREATE SINK syntax (no channel specification - always automatic)
CREATE SINK ohlc_output FROM ohlc_1min;

-- With custom buffer size
CREATE SINK ohlc_output FROM ohlc_1min WITH (
    buffer_size = 131072
);

-- Sink channel type is derived from topology:
-- This query has 1 sink (ohlc_output) → SPSC
-- If we later add another sink from the same view → Broadcast
```

### Broadcast Channel Design

```rust
/// Broadcast channel for multi-consumer scenarios.
///
/// Unlike MPSC (multi-producer), Broadcast has:
/// - Single producer (the streaming engine)
/// - Multiple consumers (subscribers)
/// - Each consumer has its own cursor
/// - Slowest consumer determines retention
pub struct BroadcastChannel<T> {
    /// Shared ring buffer
    buffer: RingBuffer<T>,

    /// Per-subscriber read cursors
    cursors: RwLock<Vec<Arc<AtomicU64>>>,

    /// Write sequence (producer's position)
    write_seq: AtomicU64,

    /// Maximum allowed subscribers
    max_subscribers: usize,
}

impl<T: Clone> BroadcastChannel<T> {
    /// Broadcast a value to all subscribers.
    ///
    /// The value is cloned for each subscriber's cursor.
    pub fn broadcast(&self, value: T) -> Result<(), BroadcastError> {
        // Check if any subscriber is too slow
        let min_cursor = self.slowest_cursor();
        let write = self.write_seq.load(Ordering::Acquire);

        if write - min_cursor >= self.buffer.capacity() as u64 {
            return Err(BroadcastError::SlowSubscriber);
        }

        // Write value
        unsafe {
            self.buffer.write_slot(write, value);
            self.write_seq.store(write + 1, Ordering::Release);
        }

        Ok(())
    }

    /// Get the slowest subscriber's cursor.
    fn slowest_cursor(&self) -> u64 {
        self.cursors.read().unwrap()
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Add a new subscriber cursor.
    pub fn add_subscriber(&self) -> usize {
        let mut cursors = self.cursors.write().unwrap();
        let id = cursors.len();

        if id >= self.max_subscribers {
            panic!("Maximum subscribers exceeded");
        }

        let current_write = self.write_seq.load(Ordering::Acquire);
        cursors.push(Arc::new(AtomicU64::new(current_write)));
        id
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `Error::SinkNotFound` | CREATE SINK not executed | Execute DDL |
| `Error::SchemaMismatch` | Type schema doesn't match sink | Fix type |
| `SinkError::Full` | Buffer full (slow subscriber) | Subscriber catches up |
| `SinkError::Closed` | Sink was closed | None |
| `BroadcastError::SlowSubscriber` | A subscriber is too slow | Drop slow subscriber or increase buffer |

## Test Plan

### Unit Tests

- [ ] `test_sink_single_subscriber_spsc`
- [ ] `test_sink_multiple_subscribers_broadcast`
- [ ] `test_sink_subscribe_returns_subscription`
- [ ] `test_sink_schema_validation`
- [ ] `test_broadcast_to_all_subscribers`
- [ ] `test_slow_subscriber_backpressure`
- [ ] `test_subscriber_drop_cleanup`

### Integration Tests

- [ ] Create sink via SQL DDL
- [ ] Push data through source, receive via sink
- [ ] Multiple sinks from same MV (broadcast)
- [ ] End-to-end streaming query

### Property Tests

- [ ] `prop_all_subscribers_receive_all_data`
- [ ] `prop_data_order_preserved`

### Benchmarks

- [ ] `bench_sink_spsc_throughput` - Target: > 10M/sec
- [ ] `bench_sink_broadcast_2_subscribers` - Target: > 5M/sec
- [ ] `bench_sink_broadcast_4_subscribers` - Target: > 2M/sec

## Rollout Plan

1. **Phase 1**: Core Sink with SPSC
2. **Phase 2**: Broadcast channel implementation
3. **Phase 3**: SQL DDL integration
4. **Phase 4**: Subscription (F-STREAM-006)
5. **Phase 5**: Integration tests + benchmarks

## Open Questions

- [x] **Broadcast ordering**: Guarantee FIFO for all subscribers?
  - Decision: Yes, all subscribers see same order
- [ ] **Slow subscriber policy**: Drop messages or block?
  - Leaning: Configurable, default is block with timeout

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/sink.rs`
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

**Auto-Derived Channel Type**: Users never specify SPSC vs Broadcast. The query plan analysis determines:
- 1 MV reading source → SPSC sink
- N MVs reading same source → Broadcast sink

This removes a decision from users while optimizing for common cases.

**Broadcast Semantics**: In broadcast mode, each subscriber has an independent cursor. A slow subscriber doesn't block others, but may cause backpressure if it falls too far behind.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-006: Subscription](F-STREAM-006-subscription.md)
- [F-STREAM-007: SQL DDL](F-STREAM-007-sql-ddl.md)
- [F-STREAM-010: Broadcast Channel](F-STREAM-010-broadcast-channel.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
