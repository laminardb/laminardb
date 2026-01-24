# F058: Async State Access

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F058 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | XL (3-4 weeks) |
| **Dependencies** | F013, F019, F003 |
| **Owner** | TBD |
| **Research** | [Stream Joins Research 2026](../../research/laminardb-stream-joins-research-review-2026.md) |

## Summary

Implement asynchronous state access patterns inspired by Apache Flink 2.0's disaggregated state management. This enables out-of-order record processing while waiting for state operations, dramatically improving throughput for state-heavy operators.

## Motivation

From Flink 2.0 VLDB 2025 paper:
- **94% reduction** in checkpoint duration
- **49x faster recovery**
- **50% cost savings** in cloud deployments

Current LaminarDB state access is synchronous, which blocks the hot path during state operations. For operators like joins that require multiple state lookups per event, this significantly limits throughput.

## Current State

```rust
// Current synchronous pattern (blocking)
impl Operator for StreamJoinOperator {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        let key = extract_key(&event.data);

        // BLOCKS - hot path is blocked during state access
        let left_matches = ctx.state.prefix_scan(&left_prefix);

        // BLOCKS again
        ctx.state.put_typed(&state_key, &join_row)?;

        // Process matches...
    }
}
```

## Goals

1. Async state API for non-blocking state operations
2. Out-of-order record processing with eventual reordering
3. Batch state operations for efficiency
4. Maintain exactly-once semantics
5. Optional disaggregated state backend (ForSt-style)

## Technical Design

### Async State API

```rust
/// Asynchronous state store interface.
#[async_trait]
pub trait AsyncStateStore: Send + Sync {
    /// Asynchronous get operation.
    async fn get_async(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError>;

    /// Asynchronous put operation.
    async fn put_async(&self, key: &[u8], value: &[u8]) -> Result<(), StateError>;

    /// Asynchronous delete operation.
    async fn delete_async(&self, key: &[u8]) -> Result<(), StateError>;

    /// Asynchronous prefix scan.
    async fn prefix_scan_async(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StateError>;

    /// Batch get for efficiency.
    async fn get_batch(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StateError>;

    /// Batch put for efficiency.
    async fn put_batch(&self, entries: &[(&[u8], &[u8])]) -> Result<(), StateError>;
}

/// Typed async state access extension.
pub trait AsyncStateStoreExt: AsyncStateStore {
    async fn get_typed_async<T: Archive>(&self, key: &[u8]) -> Result<Option<T>, StateError>
    where
        T::Archived: Deserialize<T, rkyv::rancor::Strategy<(), rkyv::rancor::Error>>;

    async fn put_typed_async<T: Serialize<AllocSerializer<256>>>(&self, key: &[u8], value: &T)
        -> Result<(), StateError>;
}
```

### Async Operator Trait

```rust
/// Operator trait with async state access support.
#[async_trait]
pub trait AsyncOperator: Send {
    /// Process an event with async state access.
    ///
    /// Returns a future that resolves to outputs when state operations complete.
    async fn process_async(
        &mut self,
        event: &Event,
        ctx: &mut AsyncOperatorContext,
    ) -> OutputVec;

    /// Handle timer with async state access.
    async fn on_timer_async(
        &mut self,
        timer: Timer,
        ctx: &mut AsyncOperatorContext,
    ) -> OutputVec;

    // Synchronous methods remain for compatibility
    fn checkpoint(&self) -> OperatorState;
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError>;
}

/// Context for async operators.
pub struct AsyncOperatorContext<'a> {
    pub event_time: i64,
    pub processing_time: i64,
    pub timers: &'a mut TimerService,
    pub state: &'a dyn AsyncStateStore,
    pub watermark_generator: &'a mut dyn WatermarkGenerator,
    pub operator_index: usize,
}
```

### Out-of-Order Processing with Reordering

```rust
/// Manages out-of-order processing and result reordering.
pub struct AsyncExecutionController {
    /// Pending operations keyed by sequence number.
    pending: BTreeMap<u64, PendingOperation>,

    /// Next sequence number to emit (for ordering).
    next_emit_seq: u64,

    /// Maximum concurrent operations.
    max_concurrent: usize,

    /// Current concurrent operations.
    active_count: usize,
}

struct PendingOperation {
    /// Sequence number for ordering.
    seq: u64,
    /// The event being processed.
    event: Event,
    /// Future for the state operation.
    state_future: BoxFuture<'static, Result<StateResult, StateError>>,
    /// Result once available.
    result: Option<OutputVec>,
}

impl AsyncExecutionController {
    /// Submit an operation for out-of-order processing.
    pub fn submit(&mut self, seq: u64, event: Event, state_op: impl Future<Output = StateResult>) {
        if self.active_count >= self.max_concurrent {
            // Backpressure: wait for some operations to complete
            self.drain_completed();
        }

        self.pending.insert(seq, PendingOperation {
            seq,
            event,
            state_future: Box::pin(state_op),
            result: None,
        });
        self.active_count += 1;
    }

    /// Poll completed operations and emit in order.
    pub fn poll_ordered(&mut self, cx: &mut Context<'_>) -> Vec<OutputVec> {
        // Poll all pending futures
        for (_, op) in self.pending.iter_mut() {
            if op.result.is_none() {
                if let Poll::Ready(result) = op.state_future.poll_unpin(cx) {
                    op.result = Some(self.process_result(result));
                    self.active_count -= 1;
                }
            }
        }

        // Emit completed operations in sequence order
        let mut outputs = Vec::new();
        while let Some((&seq, op)) = self.pending.iter().next() {
            if seq != self.next_emit_seq {
                break; // Gap in sequence - wait for earlier operations
            }
            if op.result.is_none() {
                break; // Not yet complete
            }

            let op = self.pending.remove(&seq).unwrap();
            outputs.push(op.result.unwrap());
            self.next_emit_seq += 1;
        }

        outputs
    }
}
```

### Async Stream Join Implementation

```rust
#[async_trait]
impl AsyncOperator for AsyncStreamJoinOperator {
    async fn process_async(
        &mut self,
        event: &Event,
        ctx: &mut AsyncOperatorContext,
    ) -> OutputVec {
        let key = self.extract_key(&event.data);
        let timestamp = event.timestamp;
        let side = self.current_side;

        // Start state operations concurrently
        let (store_future, probe_future) = join!(
            // Store this event
            self.store_event_async(ctx.state, side, &key, event),
            // Probe the other side
            self.probe_other_side_async(ctx.state, side, &key, timestamp),
        );

        // Process results
        let stored = store_future?;
        let matches = probe_future?;

        // Create outputs from matches
        let mut output = OutputVec::new();
        for other_row in matches {
            if let Some(joined) = self.create_joined_event(event, &other_row) {
                output.push(Output::Event(joined));
            }
        }

        output
    }

    async fn store_event_async(
        &self,
        state: &dyn AsyncStateStore,
        side: JoinSide,
        key: &[u8],
        event: &Event,
    ) -> Result<(), OperatorError> {
        let state_key = self.make_state_key(side, key, event.timestamp);
        let join_row = JoinRow::new(event)?;
        state.put_typed_async(&state_key, &join_row).await?;
        Ok(())
    }

    async fn probe_other_side_async(
        &self,
        state: &dyn AsyncStateStore,
        current_side: JoinSide,
        key: &[u8],
        timestamp: i64,
    ) -> Result<Vec<JoinRow>, OperatorError> {
        let prefix = self.make_probe_prefix(current_side.opposite(), key);
        let entries = state.prefix_scan_async(&prefix).await?;

        // Filter by time bound
        let matches: Vec<_> = entries
            .into_iter()
            .filter_map(|(_, value)| {
                let row: JoinRow = rkyv::from_bytes(&value).ok()?;
                if (timestamp - row.timestamp).abs() <= self.time_bound_ms {
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        Ok(matches)
    }
}
```

### Disaggregated State Backend (ForSt-Style)

```rust
/// Tiered state backend with local cache and remote storage.
pub struct DisaggregatedStateStore {
    /// Local in-memory cache (L1).
    local_cache: Arc<RwLock<LruCache<Vec<u8>, CachedEntry>>>,

    /// Local SSD cache (L2).
    ssd_cache: Option<SsdCache>,

    /// Remote storage (S3, HDFS, etc.) (L3).
    remote_storage: Arc<dyn RemoteStorage>,

    /// Background prefetch task.
    prefetch_handle: JoinHandle<()>,

    /// Pending writes for batching.
    write_buffer: Arc<Mutex<WriteBuffer>>,

    /// Configuration.
    config: DisaggregatedConfig,
}

#[async_trait]
impl AsyncStateStore for DisaggregatedStateStore {
    async fn get_async(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        // L1: Check local cache
        {
            let cache = self.local_cache.read().await;
            if let Some(entry) = cache.get(key) {
                return Ok(Some(entry.value.clone()));
            }
        }

        // L2: Check SSD cache
        if let Some(ref ssd) = self.ssd_cache {
            if let Some(value) = ssd.get(key).await? {
                // Promote to L1
                self.local_cache.write().await.put(key.to_vec(), CachedEntry::new(value.clone()));
                return Ok(Some(value));
            }
        }

        // L3: Fetch from remote
        let value = self.remote_storage.get(key).await?;
        if let Some(ref v) = value {
            // Populate caches
            self.local_cache.write().await.put(key.to_vec(), CachedEntry::new(v.clone()));
            if let Some(ref ssd) = self.ssd_cache {
                ssd.put(key, v).await?;
            }
        }

        Ok(value)
    }

    async fn put_async(&self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
        // Update local cache immediately
        self.local_cache.write().await.put(key.to_vec(), CachedEntry::new(value.to_vec()));

        // Buffer write for batching
        self.write_buffer.lock().await.add(key.to_vec(), value.to_vec());

        // Flush if buffer is full
        if self.write_buffer.lock().await.should_flush() {
            self.flush_writes().await?;
        }

        Ok(())
    }
}
```

## Implementation Phases

### Phase 1: Async State API (5-7 days)

1. Define `AsyncStateStore` trait
2. Implement async wrapper for `InMemoryStore`
3. Add batching support
4. Unit tests for async operations

### Phase 2: Out-of-Order Controller (5-7 days)

1. Implement `AsyncExecutionController`
2. Sequence tracking and reordering
3. Backpressure handling
4. Correctness tests

### Phase 3: Async Operators (5-7 days)

1. Define `AsyncOperator` trait
2. Implement `AsyncStreamJoinOperator`
3. Integrate with reactor loop
4. Performance benchmarks

### Phase 4: Disaggregated Backend (Optional, 7-10 days)

1. Design tiered storage architecture
2. Implement local caching layer
3. Remote storage integration (S3)
4. Checkpoint optimization

## Configuration

```rust
pub struct AsyncStateConfig {
    /// Maximum concurrent async operations per operator.
    pub max_concurrent_ops: usize,

    /// Buffer size for write batching.
    pub write_buffer_size: usize,

    /// Flush interval for write buffer.
    pub flush_interval: Duration,

    /// Enable out-of-order processing.
    pub out_of_order_processing: bool,

    /// Maximum reorder buffer size.
    pub max_reorder_buffer: usize,
}

impl Default for AsyncStateConfig {
    fn default() -> Self {
        Self {
            max_concurrent_ops: 1000,
            write_buffer_size: 64 * 1024, // 64KB
            flush_interval: Duration::from_millis(10),
            out_of_order_processing: true,
            max_reorder_buffer: 10000,
        }
    }
}
```

## Metrics

```rust
pub struct AsyncStateMetrics {
    /// Total async operations initiated.
    pub ops_initiated: u64,
    /// Operations completed successfully.
    pub ops_completed: u64,
    /// Operations failed.
    pub ops_failed: u64,
    /// Average latency for async operations.
    pub avg_latency_us: u64,
    /// P99 latency for async operations.
    pub p99_latency_us: u64,
    /// Reorder buffer high watermark.
    pub reorder_buffer_hwm: usize,
    /// Cache hit rate (for disaggregated).
    pub cache_hit_rate: f64,
}
```

## Acceptance Criteria

- [ ] AsyncStateStore trait defined and documented
- [ ] InMemoryStore async wrapper implemented
- [ ] AsyncExecutionController handles out-of-order correctly
- [ ] AsyncStreamJoinOperator maintains join semantics
- [ ] Checkpoints work correctly with async state
- [ ] 30%+ throughput improvement for join workloads
- [ ] Metrics and observability
- [ ] 20+ unit and integration tests

## Performance Targets

| Metric | Sync Baseline | Async Target | Improvement |
|--------|---------------|--------------|-------------|
| Join throughput | 200K/s | 300K/s | 50% |
| State access latency p99 | 100μs | 50μs | 50% |
| Checkpoint duration | 10s | 1s | 90% |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Complexity in reordering | Extensive testing, formal verification |
| Memory overhead for buffers | Configurable limits, backpressure |
| Consistency during failures | Careful checkpoint/recovery design |
| Integration with existing operators | Gradual migration, dual-path support |

## References

- Apache Flink 2.0 VLDB 2025 Paper: Disaggregated State Management
- ForSt State Backend Architecture
- RocksDB Async API Design
