# F-STREAM-006: Subscription

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-006 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | S (1-3 days) |
| **Dependencies** | F-STREAM-001, F-STREAM-005 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

The Subscription is the consumer endpoint returned by `Sink.subscribe()`. It provides multiple consumption patterns: polling, blocking receive, timeout-based receive, and Iterator integration for idiomatic Rust usage.

## Goals

- Multiple consumption patterns (poll, recv, timeout, iterator)
- Zero-copy Arrow RecordBatch delivery
- Efficient blocking with configurable wait strategy
- Clean Iterator trait integration
- Type-safe output matching sink schema

## Non-Goals

- Acknowledgment/offset management (handled by exactly-once semantics)
- Filtering at subscription level (use SQL WHERE clause)
- Multiple outputs per subscription (one output stream per subscription)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/subscription.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                    Subscription Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                        Sink<T>                            │   │
│  │                                                           │   │
│  │  subscribe() ───────────────────────────────────────►     │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Subscription<T>                        │   │
│  │                                                           │   │
│  │  Consumption Patterns:                                    │   │
│  │                                                           │   │
│  │  ┌─────────────────────────────────────────────────┐     │   │
│  │  │ poll()          → Option<RecordBatch>            │     │   │
│  │  │                   Non-blocking, returns None     │     │   │
│  │  │                   if no data available           │     │   │
│  │  └─────────────────────────────────────────────────┘     │   │
│  │                                                           │   │
│  │  ┌─────────────────────────────────────────────────┐     │   │
│  │  │ recv()          → Result<RecordBatch>            │     │   │
│  │  │                   Blocking, waits for data       │     │   │
│  │  │                   Error on closed sink           │     │   │
│  │  └─────────────────────────────────────────────────┘     │   │
│  │                                                           │   │
│  │  ┌─────────────────────────────────────────────────┐     │   │
│  │  │ recv_timeout()  → Result<RecordBatch>            │     │   │
│  │  │                   Blocking with timeout          │     │   │
│  │  │                   Error on timeout or closed     │     │   │
│  │  └─────────────────────────────────────────────────┘     │   │
│  │                                                           │   │
│  │  ┌─────────────────────────────────────────────────┐     │   │
│  │  │ Iterator        → impl Iterator<Item=RecordBatch>│     │   │
│  │  │                   Idiomatic for loop usage       │     │   │
│  │  │                   Terminates on sink close       │     │   │
│  │  └─────────────────────────────────────────────────┘     │   │
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

/// Subscription to a sink for consuming query results.
///
/// Created by calling `sink.subscribe()`. Provides multiple
/// consumption patterns for different use cases.
///
/// # Iterator Usage
///
/// ```rust
/// let sink = db.sink::<OHLC>("ohlc_output")?;
///
/// for batch in sink.subscribe() {
///     for row in 0..batch.num_rows() {
///         process_row(&batch, row);
///     }
/// }
/// ```
///
/// # Manual Control
///
/// ```rust
/// let mut sub = sink.subscribe();
///
/// loop {
///     match sub.recv_timeout(Duration::from_secs(1)) {
///         Ok(batch) => process(batch),
///         Err(RecvError::Timeout) => check_for_shutdown(),
///         Err(RecvError::Closed) => break,
///     }
/// }
/// ```
pub struct Subscription<T: Record> {
    /// Reference to the sink
    sink: Arc<SinkInner<T>>,

    /// Subscription ID (for broadcast mode)
    id: usize,

    /// Read cursor (for broadcast mode, unique per subscription)
    read_cursor: u64,

    /// Wait strategy
    wait_strategy: WaitStrategy,

    /// Cached write sequence
    cached_write_seq: u64,

    /// Stats
    stats: SubscriptionStats,
}

/// Subscription statistics.
#[derive(Debug, Default, Clone)]
pub struct SubscriptionStats {
    /// Total batches received
    pub batches_received: u64,
    /// Total rows received
    pub rows_received: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Poll misses (no data available)
    pub poll_misses: u64,
}

/// Receive errors.
#[derive(Debug)]
pub enum RecvError {
    /// Timeout expired without data
    Timeout,
    /// Sink was closed
    Closed,
}
```

### API/Interface

```rust
impl<T: Record> Subscription<T> {
    /// Create a new subscription for SPSC mode.
    pub(crate) fn new_spsc(sink: Arc<SinkInner<T>>) -> Self {
        Self {
            sink,
            id: 0,
            read_cursor: 0,
            wait_strategy: WaitStrategy::SpinYield(100),
            cached_write_seq: 0,
            stats: SubscriptionStats::default(),
        }
    }

    /// Create a new subscription for Broadcast mode.
    pub(crate) fn new_broadcast(sink: Arc<SinkInner<T>>, id: usize) -> Self {
        let initial_cursor = sink.broadcast_channel.as_ref()
            .map(|bc| bc.write_seq.load(Ordering::Acquire))
            .unwrap_or(0);

        Self {
            sink,
            id,
            read_cursor: initial_cursor,
            wait_strategy: WaitStrategy::SpinYield(100),
            cached_write_seq: initial_cursor,
            stats: SubscriptionStats::default(),
        }
    }

    /// Poll for the next batch without blocking.
    ///
    /// Returns `None` if no data is currently available.
    /// This is the lowest-latency consumption method.
    ///
    /// # Example
    ///
    /// ```rust
    /// loop {
    ///     if let Some(batch) = sub.poll() {
    ///         process(batch);
    ///     } else {
    ///         do_other_work();
    ///     }
    /// }
    /// ```
    pub fn poll(&mut self) -> Option<RecordBatch> {
        match self.sink.mode() {
            SinkMode::Spsc => self.poll_spsc(),
            SinkMode::Broadcast => self.poll_broadcast(),
        }
    }

    fn poll_spsc(&mut self) -> Option<RecordBatch> {
        // Refresh write sequence
        self.cached_write_seq = self.sink.spsc_channel.as_ref()?.write_seq();

        if self.read_cursor >= self.cached_write_seq {
            self.stats.poll_misses += 1;
            return None;
        }

        // Read batch
        let batch = unsafe {
            self.sink.spsc_channel.as_ref()?.read_slot(self.read_cursor)
        };
        self.read_cursor += 1;

        // Update read cursor in channel
        unsafe {
            self.sink.spsc_channel.as_ref()?.advance_read_seq(self.read_cursor);
        }

        self.update_stats(&batch);
        Some(batch)
    }

    fn poll_broadcast(&mut self) -> Option<RecordBatch> {
        let bc = self.sink.broadcast_channel.as_ref()?;

        // Refresh write sequence
        self.cached_write_seq = bc.write_seq.load(Ordering::Acquire);

        if self.read_cursor >= self.cached_write_seq {
            self.stats.poll_misses += 1;
            return None;
        }

        // Read batch (cloned in broadcast mode)
        let batch = unsafe {
            bc.buffer.read_slot_cloned(self.read_cursor)
        };
        self.read_cursor += 1;

        // Update our cursor
        bc.cursors.read().unwrap()[self.id]
            .store(self.read_cursor, Ordering::Release);

        self.update_stats(&batch);
        Some(batch)
    }

    /// Receive the next batch, blocking if necessary.
    ///
    /// Blocks until a batch is available or the sink is closed.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Closed` if the sink is closed and drained.
    ///
    /// # Example
    ///
    /// ```rust
    /// loop {
    ///     match sub.recv() {
    ///         Ok(batch) => process(batch),
    ///         Err(RecvError::Closed) => break,
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub fn recv(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            // Try poll first
            if let Some(batch) = self.poll() {
                return Ok(batch);
            }

            // Check if closed
            if self.sink.is_closed() {
                // Double-check for any remaining data
                if let Some(batch) = self.poll() {
                    return Ok(batch);
                }
                return Err(RecvError::Closed);
            }

            // Wait
            self.wait();
        }
    }

    /// Receive with timeout.
    ///
    /// Blocks until a batch is available, the timeout expires,
    /// or the sink is closed.
    ///
    /// # Errors
    ///
    /// - `RecvError::Timeout` if timeout expires
    /// - `RecvError::Closed` if sink is closed
    ///
    /// # Example
    ///
    /// ```rust
    /// match sub.recv_timeout(Duration::from_secs(5)) {
    ///     Ok(batch) => process(batch),
    ///     Err(RecvError::Timeout) => println!("No data in 5 seconds"),
    ///     Err(RecvError::Closed) => println!("Sink closed"),
    /// }
    /// ```
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<RecordBatch, RecvError> {
        let start = std::time::Instant::now();

        loop {
            // Try poll first
            if let Some(batch) = self.poll() {
                return Ok(batch);
            }

            // Check if closed
            if self.sink.is_closed() {
                if let Some(batch) = self.poll() {
                    return Ok(batch);
                }
                return Err(RecvError::Closed);
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Err(RecvError::Timeout);
            }

            // Wait
            self.wait();
        }
    }

    /// Receive a batch of results.
    ///
    /// Collects up to `max` batches without blocking between them.
    /// Returns at least 1 batch (blocks for the first).
    pub fn recv_batch(&mut self, max: usize) -> Result<Vec<RecordBatch>, RecvError> {
        let mut results = Vec::with_capacity(max);

        // Block for first batch
        results.push(self.recv()?);

        // Collect remaining without blocking
        while results.len() < max {
            match self.poll() {
                Some(batch) => results.push(batch),
                None => break,
            }
        }

        Ok(results)
    }

    /// Check if there's data available without consuming it.
    #[inline]
    pub fn has_data(&self) -> bool {
        match self.sink.mode() {
            SinkMode::Spsc => {
                self.sink.spsc_channel.as_ref()
                    .map(|c| c.write_seq() > self.read_cursor)
                    .unwrap_or(false)
            }
            SinkMode::Broadcast => {
                self.sink.broadcast_channel.as_ref()
                    .map(|bc| bc.write_seq.load(Ordering::Acquire) > self.read_cursor)
                    .unwrap_or(false)
            }
        }
    }

    /// Get the current lag (number of unread batches).
    #[inline]
    pub fn lag(&self) -> u64 {
        let write_seq = match self.sink.mode() {
            SinkMode::Spsc => self.sink.spsc_channel.as_ref()
                .map(|c| c.write_seq())
                .unwrap_or(0),
            SinkMode::Broadcast => self.sink.broadcast_channel.as_ref()
                .map(|bc| bc.write_seq.load(Ordering::Acquire))
                .unwrap_or(0),
        };

        write_seq.saturating_sub(self.read_cursor)
    }

    /// Get subscription statistics.
    #[inline]
    pub fn stats(&self) -> &SubscriptionStats {
        &self.stats
    }

    /// Set the wait strategy.
    pub fn set_wait_strategy(&mut self, strategy: WaitStrategy) {
        self.wait_strategy = strategy;
    }

    fn wait(&self) {
        match self.wait_strategy {
            WaitStrategy::Spin => {
                std::hint::spin_loop();
            }
            WaitStrategy::SpinYield(iterations) => {
                for _ in 0..iterations {
                    std::hint::spin_loop();
                }
                std::thread::yield_now();
            }
            WaitStrategy::Park => {
                std::thread::park_timeout(Duration::from_micros(1));
            }
        }
    }

    fn update_stats(&mut self, batch: &RecordBatch) {
        self.stats.batches_received += 1;
        self.stats.rows_received += batch.num_rows() as u64;
        // Approximate byte size
        self.stats.bytes_received += batch.get_array_memory_size() as u64;
    }
}

/// Iterator implementation for idiomatic Rust usage.
impl<T: Record> Iterator for Subscription<T> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

/// Drop decrements subscriber count.
impl<T: Record> Drop for Subscription<T> {
    fn drop(&mut self) {
        self.sink.subscriber_count.fetch_sub(1, Ordering::SeqCst);
    }
}
```

### Usage Patterns

#### Pattern 1: Iterator (Recommended for Simple Cases)

```rust
let sink = db.sink::<OHLC>("ohlc_output")?;

// Simple, idiomatic
for batch in sink.subscribe() {
    for row in 0..batch.num_rows() {
        let symbol: &str = batch.column(0).as_string::<i32>().value(row);
        let close: f64 = batch.column(1).as_primitive::<Float64Type>().value(row);
        println!("{}: {}", symbol, close);
    }
}
```

#### Pattern 2: Polling (Low-Latency)

```rust
let mut sub = sink.subscribe();

loop {
    // Non-blocking poll
    if let Some(batch) = sub.poll() {
        process(batch);
    } else {
        // Do other work while waiting
        process_other_events();
    }
}
```

#### Pattern 3: Timeout-Based (Graceful Shutdown)

```rust
let mut sub = sink.subscribe();
let shutdown = Arc::new(AtomicBool::new(false));

while !shutdown.load(Ordering::Relaxed) {
    match sub.recv_timeout(Duration::from_millis(100)) {
        Ok(batch) => process(batch),
        Err(RecvError::Timeout) => continue, // Check shutdown
        Err(RecvError::Closed) => break,
    }
}
```

#### Pattern 4: Batch Collection

```rust
let mut sub = sink.subscribe();

// Collect up to 100 batches at a time
while let Ok(batches) = sub.recv_batch(100) {
    // Process in bulk
    for batch in batches {
        bulk_insert(batch);
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `RecvError::Timeout` | Timeout expired | Continue or adjust timeout |
| `RecvError::Closed` | Sink closed | Exit loop, subscription done |

## Test Plan

### Unit Tests

- [ ] `test_subscription_poll_empty`
- [ ] `test_subscription_poll_with_data`
- [ ] `test_subscription_recv_blocks`
- [ ] `test_subscription_recv_timeout`
- [ ] `test_subscription_iterator`
- [ ] `test_subscription_recv_batch`
- [ ] `test_subscription_has_data`
- [ ] `test_subscription_lag`
- [ ] `test_subscription_stats`
- [ ] `test_subscription_drop_decrements_count`
- [ ] `test_subscription_wait_strategies`

### Integration Tests

- [ ] End-to-end: push source → query → subscribe sink
- [ ] Multiple subscribers (broadcast)
- [ ] Subscription after data already in sink
- [ ] Graceful shutdown with timeout

### Property Tests

- [ ] `prop_all_pushed_data_received`
- [ ] `prop_iterator_terminates_on_close`
- [ ] `prop_stats_accurate`

### Benchmarks

- [ ] `bench_subscription_poll` - Target: < 50ns
- [ ] `bench_subscription_recv` - Target: < 100ns with data
- [ ] `bench_subscription_iterator_throughput` - Target: > 10M rows/sec

## Rollout Plan

1. **Phase 1**: Core Subscription with poll/recv
2. **Phase 2**: Timeout and batch operations
3. **Phase 3**: Iterator implementation
4. **Phase 4**: Statistics tracking
5. **Phase 5**: Integration tests

## Open Questions

- [x] **Batch vs Row access**: Should we provide row-level API?
  - Decision: No, batch-level only. Row access via Arrow APIs.
- [ ] **Async support**: Add async recv for tokio integration?
  - Leaning: Phase 4, not Tier 1

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/subscription.rs`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

**Iterator Semantics**: The Iterator implementation uses `recv()` internally, which means it blocks on empty. The iterator terminates when the sink is closed and drained.

**Statistics**: The stats are per-subscription, useful for monitoring and debugging. They don't impact hot path performance.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-005: Sink](F-STREAM-005-sink.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
