# Sink-Pipeline Integration Fix Plan

**Date:** 2026-02-27
**Based on:** [Sink-Pipeline Integration Audit](sink-pipeline-integration-audit.md)

---

## Priority Matrix

| ID | Severity | Issue | Effort | Timeline |
|----|----------|-------|--------|----------|
| CRITICAL-1 | CRITICAL | Subscription `recv()` unbounded spin | S | Day 1 |
| CRITICAL-2 | CRITICAL | CreditGate `acquire_blocking()` unbounded spin | S | Day 1 |
| HIGH-1 | HIGH | No periodic sink flush | S | Day 1 |
| HIGH-4 | HIGH | HashMap allocated every cycle | S | Day 1 |
| MEDIUM-1 | MEDIUM | Dispatcher spin iterations too aggressive | S | Day 1 |
| MEDIUM-4 | MEDIUM | PostgreSQL flush interval 1s default | S | Day 1 |
| HIGH-2 | HIGH | Sequential sink writes | M | Day 2 |
| HIGH-3 | HIGH | All sinks receive all results (no routing) | M | Day 2-3 |
| CRITICAL-3 | CRITICAL | Arc<Mutex> sink contention with checkpoint | L | Day 3-5 |
| MEDIUM-2 | MEDIUM | WebSocket double serialization | M | Week 2 |
| MEDIUM-3 | MEDIUM | DagSinkBridge unused | L | Week 2+ |

---

## Immediate Fixes (Day 1)

### Fix 1: Subscription `recv()` Progressive Backoff [CRITICAL-1]

**File:** `crates/laminar-core/src/streaming/subscription.rs`

```rust
// BEFORE (lines 116-129)
pub fn recv(&self) -> Result<RecordBatch, RecvError> {
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        std::hint::spin_loop();
    }
}

// AFTER
pub fn recv(&self) -> Result<RecordBatch, RecvError> {
    let mut spins = 0u32;
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        // Progressive backoff: spin → yield → park
        if spins < 64 {
            std::hint::spin_loop();
        } else if spins < 128 {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(std::time::Duration::from_micros(100));
        }
        spins = spins.saturating_add(1);
    }
}
```

Also apply same pattern to `recv_timeout()` (lines 137-155):

```rust
// BEFORE (lines 137-155)
pub fn recv_timeout(&self, timeout: Duration) -> Result<RecordBatch, RecvError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        if Instant::now() >= deadline {
            return Err(RecvError::Timeout);
        }
        std::hint::spin_loop();
    }
}

// AFTER
pub fn recv_timeout(&self, timeout: Duration) -> Result<RecordBatch, RecvError> {
    let deadline = Instant::now() + timeout;
    let mut spins = 0u32;
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        if Instant::now() >= deadline {
            return Err(RecvError::Timeout);
        }
        if spins < 64 {
            std::hint::spin_loop();
        } else if spins < 128 {
            std::thread::yield_now();
        } else {
            // Sleep for shorter periods as deadline approaches
            let remaining = deadline.saturating_duration_since(Instant::now());
            let sleep_dur = remaining.min(std::time::Duration::from_micros(100));
            std::thread::park_timeout(sleep_dur);
        }
        spins = spins.saturating_add(1);
    }
}
```

**Tests:** Existing `recv()` and `recv_timeout()` tests should pass. Add a benchmark to verify latency impact is < 1μs for the spin→yield transition.

---

### Fix 2: CreditGate `acquire_blocking()` Progressive Backoff [CRITICAL-2]

**File:** `crates/laminar-core/src/tpc/backpressure.rs`

```rust
// BEFORE (lines 247-258)
pub fn acquire_blocking(&self, n: usize) {
    loop {
        match self.try_acquire_n(n) {
            CreditAcquireResult::Acquired | CreditAcquireResult::Dropped => return,
            CreditAcquireResult::WouldBlock => {
                std::hint::spin_loop();
            }
        }
    }
}

// AFTER
pub fn acquire_blocking(&self, n: usize) {
    let mut attempt = 0u32;
    loop {
        match self.try_acquire_n(n) {
            CreditAcquireResult::Acquired | CreditAcquireResult::Dropped => return,
            CreditAcquireResult::WouldBlock => {
                self.metrics.record_blocked();
                if attempt < 64 {
                    std::hint::spin_loop();
                } else if attempt < 128 {
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(std::time::Duration::from_micros(50));
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}
```

**Tests:** Existing backpressure tests. Add test for contention scenario verifying CPU doesn't stay at 100%.

---

### Fix 3: Add Periodic Sink Flush [HIGH-1]

**File:** `crates/laminar-db/src/db.rs`

Add a flush arm to the main pipeline `tokio::select!` loop.

```rust
// BEFORE (lines 2730-2738)
loop {
    tokio::select! {
        () = shutdown.notified() => {
            tracing::info!("Pipeline shutdown signal received");
            break;
        }
        () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    // ... poll/execute/write cycle ...
}

// AFTER
let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(5));
flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
loop {
    tokio::select! {
        () = shutdown.notified() => {
            tracing::info!("Pipeline shutdown signal received");
            break;
        }
        () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }

    // ... existing poll/execute/write cycle ...

    // Periodic sink flush (every 5s or configurable)
    if flush_interval.poll_tick(&mut std::task::Context::from_waker(
        futures::task::noop_waker_ref()
    )).is_ready() {
        for (name, sink_arc, _, _) in &sinks {
            if let Err(e) = sink_arc.lock().await.flush().await {
                tracing::warn!(sink = %name, error = %e, "Periodic flush error");
            }
        }
    }
}
```

Alternative simpler approach — use cycle count:

```rust
// After the existing sink write loop
cycle_count += 1;
if cycle_count % 50 == 0 {  // Every 50 cycles (~5s at 100ms intervals)
    for (name, sink_arc, _, _) in &sinks {
        if let Err(e) = sink_arc.lock().await.flush().await {
            tracing::warn!(sink = %name, error = %e, "Periodic flush error");
        }
    }
}
```

---

### Fix 4: Hoist HashMap Outside Pipeline Loop [HIGH-4]

**File:** `crates/laminar-db/src/db.rs`

```rust
// BEFORE (line 2766, inside loop)
let mut source_batches = HashMap::new();

// AFTER (hoist before loop, ~line 2729)
let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
// ... inside loop:
source_batches.clear();
```

**Note:** `HashMap::clear()` retains capacity but drops values. `Vec::new()` inside `or_insert_with` still allocates. For full optimization, pre-populate with known source names:

```rust
let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
for (name, _, _) in &sources {
    source_batches.insert(name.clone(), Vec::new());
}

// Inside loop:
for (_, v) in source_batches.iter_mut() {
    v.clear();
}
```

---

### Fix 5: Reduce Dispatcher Spin Iterations [MEDIUM-1]

**File:** `crates/laminar-core/src/subscription/dispatcher.rs`

```rust
// BEFORE (line ~75 in config defaults)
spin_iterations: 100,
idle_sleep: Duration::from_micros(10),

// AFTER
spin_iterations: 16,
idle_sleep: Duration::from_micros(50),
```

16 spin iterations (PAUSE instructions) takes ~200-400ns on modern x86, which is a reasonable spin budget before yielding to the tokio scheduler. Increasing idle_sleep from 10μs to 50μs reduces unnecessary wakeups by 5x while keeping latency under 100μs.

---

### Fix 6: Reduce PostgreSQL Default Flush Interval [MEDIUM-4]

**File:** `crates/laminar-connectors/src/postgres/sink_config.rs`

```rust
// BEFORE (line 84)
flush_interval: Duration::from_secs(1),

// AFTER
flush_interval: Duration::from_millis(250),
```

---

## Short-Term Fixes (Day 2-3)

### Fix 7: Parallelize Sink Writes [HIGH-2]

**File:** `crates/laminar-db/src/db.rs`

```rust
// BEFORE (lines 2880-2916) — sequential
for (sink_name, sink_arc, _, filter_expr) in &sinks {
    for (stream_name, batches) in &results {
        for batch in batches {
            let filtered = /* ... */;
            if filtered.num_rows() > 0 {
                if let Err(e) = sink_arc.lock().await.write_batch(&filtered).await {
                    tracing::warn!(sink = %sink_name, error = %e, "Sink write error");
                }
            }
        }
    }
}

// AFTER — concurrent
use futures::future::join_all;

let write_futures: Vec<_> = sinks
    .iter()
    .map(|(sink_name, sink_arc, _, filter_expr)| {
        let results = &results;
        let sink_name = sink_name.clone();
        let sink_arc = Arc::clone(sink_arc);
        let filter_expr = filter_expr.clone();
        async move {
            for (stream_name, batches) in results.iter() {
                for batch in batches {
                    let filtered = if let Some(ref filter_sql) = filter_expr {
                        match apply_filter(batch, filter_sql).await {
                            Ok(Some(fb)) => fb,
                            Ok(None) => continue,
                            Err(e) => {
                                tracing::warn!(
                                    sink = %sink_name,
                                    filter = %filter_sql,
                                    error = %e,
                                    "Sink filter error"
                                );
                                continue;
                            }
                        }
                    } else {
                        batch.clone()
                    };
                    if filtered.num_rows() > 0 {
                        if let Err(e) =
                            sink_arc.lock().await.write_batch(&filtered).await
                        {
                            tracing::warn!(
                                sink = %sink_name,
                                stream = %stream_name,
                                error = %e,
                                "Sink write error"
                            );
                        }
                    }
                }
            }
        }
    })
    .collect();
join_all(write_futures).await;
```

**Dependency:** Requires `futures` crate (already in workspace).

---

### Fix 8: Route Sink Results by FROM Clause [HIGH-3]

**File:** `crates/laminar-db/src/db.rs`

```rust
// Build a sink-to-input mapping during setup
let sink_input_map: HashMap<String, String> = sink_regs
    .iter()
    .map(|(name, reg)| (name.clone(), reg.input.clone()))
    .collect();

// BEFORE (line 2881)
for (stream_name, batches) in &results {  // ALL results to ALL sinks

// AFTER — filter by sink's registered input
for (sink_name, sink_arc, _, filter_expr) in &sinks {
    let sink_input = &sink_input_map[sink_name];
    let Some(batches) = results.get(sink_input) else {
        continue;  // No results for this sink's input stream
    };
    for batch in batches {
        // ... existing filter and write logic ...
    }
}
```

**Requires:** Passing `sink_input_map` into the pipeline task. Add it to the `sinks` tuple or as a separate HashMap.

---

## Medium-Term Fixes (Week 2)

### Fix 9: Decouple Sinks from Pipeline via Command Channels [CRITICAL-3]

This is the most impactful structural change.

**Architecture:**
```
Pipeline Task                    Sink Task (per sink)
────────────                    ──────────────────
execute_cycle()                 loop {
  → results                       select! {
  → tx.send(WriteBatch(batch))      cmd = rx.recv() => {
                                       match cmd {
Checkpoint Coordinator                  WriteBatch(b) => sink.write_batch(b),
──────────────────────                  Flush => sink.flush(),
pre_commit_sinks()                      PreCommit(e) => sink.pre_commit(e),
  → tx.send(PreCommit(epoch))          CommitEpoch(e) => sink.commit_epoch(e),
commit_sinks()                          Close => { sink.close(); break; }
  → tx.send(CommitEpoch(epoch))       }
                                     }
                                     _ = flush_interval.tick() => {
                                       sink.flush();
                                     }
                                   }
                                 }
```

**New types:**

```rust
/// Command sent to a sink's dedicated task.
enum SinkCommand {
    WriteBatch {
        batch: RecordBatch,
        /// Optional oneshot for backpressure / completion signal.
        ack: Option<oneshot::Sender<Result<WriteResult, ConnectorError>>>,
    },
    Flush {
        ack: Option<oneshot::Sender<Result<(), ConnectorError>>>,
    },
    PreCommit {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    CommitEpoch {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    RollbackEpoch {
        epoch: u64,
    },
    Close,
}

/// Handle for sending commands to a sink task.
struct SinkTaskHandle {
    name: String,
    tx: mpsc::Sender<SinkCommand>,
    task: JoinHandle<()>,
}

impl SinkTaskHandle {
    /// Non-blocking write — returns immediately.
    /// Backpressure: bounded channel will apply send().await pressure.
    async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.tx
            .send(SinkCommand::WriteBatch { batch, ack: None })
            .await
            .map_err(|_| ConnectorError::ConnectionFailed("sink task closed".into()))
    }

    /// Blocking pre-commit — waits for sink to acknowledge.
    async fn pre_commit(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::PreCommit { epoch, ack: tx })
            .await
            .map_err(|_| ConnectorError::ConnectionFailed("sink task closed".into()))?;
        rx.await
            .map_err(|_| ConnectorError::ConnectionFailed("sink task dropped ack".into()))?
    }
}
```

**Sink task function:**

```rust
async fn run_sink_task(
    name: String,
    mut sink: Box<dyn SinkConnector>,
    mut rx: mpsc::Receiver<SinkCommand>,
    flush_interval: Duration,
) {
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { break };
                match cmd {
                    SinkCommand::WriteBatch { batch, ack } => {
                        let result = sink.write_batch(&batch).await;
                        if let Some(ack) = ack {
                            let _ = ack.send(result);
                        }
                    }
                    SinkCommand::Flush { ack } => {
                        let result = sink.flush().await;
                        if let Some(ack) = ack {
                            let _ = ack.send(result);
                        }
                    }
                    SinkCommand::PreCommit { epoch, ack } => {
                        let result = sink.pre_commit(epoch).await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::CommitEpoch { epoch, ack } => {
                        let result = sink.commit_epoch(epoch).await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::RollbackEpoch { epoch } => {
                        let _ = sink.rollback_epoch(epoch).await;
                    }
                    SinkCommand::Close => {
                        let _ = sink.flush().await;
                        let _ = sink.close().await;
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                if let Err(e) = sink.flush().await {
                    tracing::warn!(sink = %name, error = %e, "Periodic flush error");
                }
            }
        }
    }
}
```

**Benefits:**
1. Eliminates `Arc<Mutex>` entirely — each sink owned by its task
2. Bounded channel provides natural backpressure (configurable capacity)
3. Pipeline writes are non-blocking (just channel send)
4. Checkpoint coordinator communicates via same channel (ordered operations)
5. Built-in periodic flush via `tokio::select!`
6. Each sink runs independently — slow sink doesn't block others

**Channel capacity recommendation:** 64-256 batches. At 1024 rows/batch, this buffers 64K-256K rows.

**Migration:**
1. Replace `Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>` with `SinkTaskHandle`
2. Spawn one task per sink during `start_connector_pipeline()`
3. Pipeline loop sends `WriteBatch` commands (non-blocking)
4. Checkpoint coordinator sends `PreCommit`/`CommitEpoch` with ack channels
5. Shutdown sends `Close` and joins task handle

---

### Fix 10: WebSocket Sink Single-Pass Serialization [MEDIUM-2]

**File:** `crates/laminar-connectors/src/websocket/sink.rs`

```rust
// BEFORE (lines 324-338)
let json = self.serializer.serialize_to_json(batch)?;
let msg = ServerMessage::Data { data: json, ... };
let serialized = serde_json::to_string(&msg)?;
let data = Bytes::from(serialized);

// AFTER — single allocation
let mut buf = Vec::with_capacity(batch.num_rows() * 128); // estimate
buf.extend_from_slice(b"{\"type\":\"data\",\"subscription_id\":\"");
buf.extend_from_slice(self.subscription_id.as_bytes());
buf.extend_from_slice(b"\",\"sequence\":");
// ... write sequence, watermark ...
buf.extend_from_slice(b",\"data\":");
self.serializer.serialize_to_writer(batch, &mut buf)?;
buf.push(b'}');
let data = Bytes::from(buf);
```

Or more practically, use `serde_json::to_writer` with a `Vec<u8>`:

```rust
let mut buf = Vec::with_capacity(4096);
let msg = ServerMessage::Data { ... };
serde_json::to_writer(&mut buf, &msg)?;
let data = Bytes::from(buf);
```

---

## Architecture Recommendations

### A. Wire DagSinkBridge into Pipeline [MEDIUM-3]

The `DagSinkBridge` + `ConnectorBridgeRuntime` infrastructure provides:
- Proper DAG-integrated sink nodes
- Explicit epoch lifecycle (begin/commit/rollback per sink)
- `flush_outputs()` that drains accumulated outputs
- Per-sink metrics via `SinkBridgeMetrics`

Currently, the pipeline bypasses all of this. Long-term, the `DagSinkBridge` should be the canonical path for external sink delivery. This would also enable:
- Sink nodes in the DAG visualization
- Per-sink backpressure that feeds back into DAG scheduling
- Standardized epoch lifecycle management

**Recommendation:** After Fix 9 (command channels) stabilizes, evaluate migrating to `DagSinkBridge` as the command handler within each sink task.

### B. Consider Async Sink Trait Without `async_trait`

For future Ring 0 sink support (in-memory materialized views that need sub-microsecond latency), consider a non-async sink trait variant:

```rust
pub trait SyncSink: Send {
    fn write_batch_sync(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;
}
```

This avoids the `Box::pin()` allocation from `#[async_trait]`. Not needed today since all sink writes are Ring 1+.

### C. Add Sink Metrics to Pipeline Counters

Currently, the pipeline tracks `events_ingested`, `events_emitted`, `total_batches` but has no sink-specific counters visible at the pipeline level. Add:
- `sink_writes_total` per sink
- `sink_write_errors_total` per sink
- `sink_write_latency_us` histogram per sink
- `sink_backpressure_events` per sink (when channel-based)

---

## Testing Plan

| Fix | Test Type | Validation |
|-----|-----------|------------|
| Fix 1, 2 | Unit test | Verify recv/acquire don't burn 100% CPU; benchmark latency |
| Fix 3, 6 | Integration test | Verify data appears in sink within configured flush interval |
| Fix 4, 5 | Benchmark | Measure allocation reduction via `cargo bench` |
| Fix 7 | Integration test | Verify N sinks complete in time ≈ max(sink_latencies), not sum |
| Fix 8 | Unit test | Verify sink only receives results from its FROM clause |
| Fix 9 | Integration test | Full pipeline with Kafka sink, verify no checkpoint contention |
| Fix 10 | Benchmark | Measure serialization throughput improvement |

---

## Rollout Order

```
Day 1: Fix 1 + Fix 2 (spin loops)
        Fix 3 (periodic flush)
        Fix 4 + Fix 5 + Fix 6 (quick wins)
        → cargo test --all --lib
        → cargo clippy --all -- -D warnings
        → cargo bench --bench latency

Day 2: Fix 7 (parallel sink writes)
        Fix 8 (sink routing)
        → Integration tests with mock sinks

Day 3-5: Fix 9 (command channel architecture)
          → Full integration test suite
          → Kafka sink end-to-end test
          → Checkpoint recovery test with sinks

Week 2: Fix 10 (WebSocket serialization)
         Architecture recommendations evaluation
```
