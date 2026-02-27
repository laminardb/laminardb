# Sink-Pipeline Integration Audit

**Date:** 2026-02-27
**Scope:** Sink connector implementation, pipeline→sink wiring, spin loops, CPU waste, performance anti-patterns
**Codebase Version:** v0.16.0 (commit b7f2c64)

---

## Executive Summary

The sink subsystem is architecturally sound—trait design is clean, checkpoint 2PC protocol is correct, and emit strategies are well-implemented. However, the **pipeline→sink wiring** has several efficiency issues: sinks are wrapped in `Arc<tokio::sync::Mutex>`, there is no periodic flush during normal operation, sink writes are sequential across all sinks per cycle, and the `DagSinkBridge` infrastructure is fully implemented but unused. The spin loop analysis found **3 critical** and **3 medium** issues, primarily in backpressure acquisition and subscription receive paths.

---

## Phase 1: Sink Trait & API Surface

### 1.1 SinkConnector Trait

**File:** `crates/laminar-connectors/src/connector.rs:303-439`

| Check | Status | Notes |
|-------|--------|-------|
| `write_batch()` exists | PASS | `async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult>` |
| `flush()` exists | PASS | `async fn flush(&mut self)` — default no-op |
| `close()` exists | PASS | `async fn close(&mut self)` |
| Uses Arrow RecordBatch | PASS | `&RecordBatch` reference (zero-copy to caller) |
| Uses `async_trait` | PASS | `#[async_trait]` — adds vtable + Box<Future> allocation per call |
| Factory method exists | PASS | Via `ConnectorRegistry::create_sink(&config)` |
| Error types well-defined | PASS | `ConnectorError` with typed variants (not anyhow passthrough) |
| 2PC protocol methods | PASS | `begin_epoch`, `pre_commit`, `commit_epoch`, `rollback_epoch` |
| Capabilities advertised | PASS | `SinkConnectorCapabilities` bitmask |

**Overhead note:** `#[async_trait]` adds a `Box::pin()` allocation per `write_batch()` call. For Ring 2 external I/O this is acceptable. For a future in-memory Ring 0 sink path, manual Future impl would be needed.

### 1.2 Concrete Sink Implementations

| Sink | File | Data Receive | Blocking? | Buffering | Shutdown |
|------|------|-------------|-----------|-----------|----------|
| **KafkaSink** | `kafka/sink.rs` | Direct `write_batch()` call | Async (rdkafka queue) | rdkafka internal queue | Flush + abort txn |
| **PostgresSink** | `postgres/sink.rs` | Direct `write_batch()` call | Async (buffered locally) | Time (1s) + size (4096 rows) | Flush buffer + close pool |
| **WebSocketSink** | `websocket/sink.rs` | Direct `write_batch()` call | Non-blocking (fanout) | Per-client circular buffer | Shutdown signal + join |
| **MockSink** | `testing.rs:168-280` | Direct `write_batch()` call | Non-blocking (Vec push) | None | No-op |

**NOT IMPLEMENTED:** RabbitMQ, FileSink, RingBufferSink, S3/Delta Lake sink

### 1.3 Pipeline → Sink Wiring

**File:** `crates/laminar-db/src/db.rs:2880-2916`

This is the **most critical finding** of the audit.

**Current architecture:**
```
tokio::select! { sleep(100ms) }     ← Timer-poll at 100ms intervals
  → poll_batch() on each source      ← Sequential, under Arc<Mutex>
  → executor.execute_cycle()          ← Stateful streaming SQL execution
  → for each sink:                    ← SEQUENTIAL iteration
      → sink_arc.lock().await          ← tokio::sync::Mutex lock acquisition
      → write_batch(&filtered).await   ← Async write (may do I/O)
```

| Check | Status | Notes |
|-------|--------|-------|
| Data flows via bounded channel | **FAIL** | No channel — direct `write_batch()` call in same task |
| Channel type appropriate | N/A | No channel exists |
| No Arc<Mutex> on hot path | **FAIL** | `Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>` at line 2451 |
| Ring 0 not blocked by sink | PASS | Ring 0 (core_handle.rs) does not interact with sinks |
| Sink writes in Ring 1/2 | PASS | Writes happen in tokio task (Ring 1) |
| Periodic flush during operation | **FAIL** | `flush()` only called at shutdown (line 3143) |
| DagSinkBridge used | **FAIL** | Fully implemented but not wired into pipeline |

**Sink type declaration (line 2449-2454):**
```rust
let mut sinks: Vec<(
    String,
    Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>,
    ConnectorConfig,
    Option<String>,  // filter expression
)> = Vec::new();
```

### 1.4 CREATE SINK DDL Path

**File:** `crates/laminar-db/src/db.rs:604-689`

| Check | Status | Notes |
|-------|--------|-------|
| WITH options reach connector | PASS | `build_sink_config(reg)` at line 2459 constructs `ConnectorConfig` from registration |
| FROM clause resolves correctly | PASS | Both `SinkFrom::Table` and `SinkFrom::Query` handled |
| Catalog registration correct | PASS | `catalog.register_sink(&name, &input)` |
| DROP SINK cleans up | **PARTIAL** | Catalog drop exists but running pipeline not notified mid-flight |
| Two syntax forms supported | PASS | `INTO KAFKA (...)` and `WITH ('connector'='kafka', ...)` both work |

**Config flow:**
```
SQL WITH options → CreateSinkStatement → SinkRegistration → build_sink_config() → ConnectorConfig → registry.create_sink(&config)
```

Both `connector_options` (from INTO clause) and `with_options` (from WITH clause) are merged into the final `ConnectorConfig`. Verified at lines 639-656.

---

## Phase 2: Spin Loop & CPU Waste Audit

### 2.1 Loop Classification

| Location | Pattern | Verdict | Ring |
|----------|---------|---------|------|
| `core_handle.rs:593-683` | `loop { drain inbox → poll reactor → push outbox → spin_loop() }` | **OK** — Ring 0 owns CPU | Ring 0 |
| `backpressure.rs:247-258` | `loop { try_acquire → spin_loop() }` | **CRITICAL** — unbounded spin, no backoff | Ring 1→0 boundary |
| `subscription.rs:116-129` | `loop { poll → spin_loop() }` | **CRITICAL** — unbounded spin, no timeout | Ring 1 |
| `subscription.rs:137-155` | `loop { poll → spin_loop() until deadline }` | **HIGH** — spin with deadline but no backoff | Ring 1 |
| `dispatcher.rs:199-216` | `loop { drain → 100x spin_loop() → sleep(10μs) }` | **MEDIUM** — aggressive default spin count | Ring 1 |
| `channel.rs:399-425` | `wait_for_space()` — configurable Spin/SpinYield/Park | **CONFIGURABLE** — depends on WaitStrategy | Ring 0/1 |
| `db.rs:2730-2738` | `loop { tokio::select! { shutdown ∣ sleep(100ms) } }` | **OK** — event-driven with timer | Ring 1 |

### 2.2 WaitStrategy Audit

**File:** `crates/laminar-core/src/streaming/channel.rs:399-425`

| Strategy | Implementation | Verdict |
|----------|---------------|---------|
| `Spin` | `while is_full() { spin_loop() }` | Pure spin — Ring 0 only |
| `SpinYield` | 100x `spin_loop()` then `yield_now()`, repeat | Spin with kernel yield — acceptable for Ring 0/1 |
| `Park` | `park_timeout(100μs)` while full | Syscall-based sleep — Ring 1/2 recommended |

- Default WaitStrategy for streaming channels: Not explicitly set in audit scope; depends on `ChannelConfig`.
- `SpinYield` correctly yields after 100 iterations (line 409-415).
- `Park` uses `thread::park_timeout(100μs)` (line 421) — reasonable for non-hot-path.

### 2.3 Backpressure Path Audit

**CreditGate:** `crates/laminar-core/src/tpc/backpressure.rs:175-258`

| Check | Status | Notes |
|-------|--------|-------|
| Bounded channels between pipeline and sink | **FAIL** | No channel — direct call |
| Backpressure behavior explicit | PASS | `OverflowStrategy::Block/Drop/Error` |
| Blocking doesn't stall Ring 0 | PASS | CreditGate guards Ring 1→0 inbox, not sink path |
| Slow sink can't block Ring 0 | **PARTIAL** | Ring 0 doesn't call sinks, but outbox drop is silent |
| Backpressure metrics exist | PASS | `CreditMetrics` tracks acquired/blocked/dropped |

**Streaming channel backpressure** (`channel.rs:237-246`):
- `BackpressureStrategy::Block` → `push_blocking()` → `wait_for_space()` → spin/yield/park
- `BackpressureStrategy::DropOldest` → evict oldest entry
- `BackpressureStrategy::Reject` → return `ChannelFull` error

These channels are used for in-memory stream subscriptions, NOT for external sink delivery.

### 2.4 Tokio Runtime Misuse

| Check | Status | Notes |
|-------|--------|-------|
| No `block_on()` in async | PASS | Not found in sink/pipeline paths |
| `spawn_blocking` used appropriately | N/A | Not used in sink path (sinks are all async) |
| Ring 0 not on Tokio runtime | PASS | `std::thread::spawn` with CPU pinning at `core_handle.rs:490` |
| External I/O on separate task | **PARTIAL** | Sink writes happen in pipeline task, not spawned separately |
| No work stealing across cores | PASS | Thread-per-core with dedicated OS threads |

---

## Phase 3: Performance Anti-Pattern Detection

### 3.1 Allocation in Hot Path

**Pipeline → Sink handoff (`db.rs:2880-2916`):**

| Location | Allocation | Severity | Notes |
|----------|-----------|----------|-------|
| `db.rs:2899` | `batch.clone()` | LOW | Arrow RecordBatch clone is Arc bump (cheap) |
| `db.rs:2815-2816` | `or_insert_with(Vec::new)` | LOW | Per-source batch collection; grows once |
| `db.rs:2847-2854` | `format!`, `collect()` | NONE | Debug logging only (≤3 batches) |
| `db.rs:2766` | `HashMap::new()` per cycle | MEDIUM | `source_batches` recreated every 100ms cycle |

**Ring 0 core loop (`core_handle.rs:590-694`):**

| Location | Allocation | Severity |
|----------|-----------|----------|
| Line 590 | `Vec::with_capacity(256)` | NONE — allocated once, reused |
| Line 663 | `poll_buffer.clear()` | NONE — reuse pattern |

**WebSocket sink (`websocket/sink.rs:324-338`):**

| Location | Allocation | Severity |
|----------|-----------|----------|
| Line 324 | `serialize_to_json(batch)` | MEDIUM — allocates String |
| Line 326 | `serde_json::to_string(&msg)` | MEDIUM — second String allocation |
| Line 328 | `Bytes::from(serialized)` | LOW — moves ownership (no copy) |

### 3.2 Lock Contention

| Lock | Location | Ring | Verdict |
|------|----------|------|---------|
| `Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>` | `db.rs:2451` | Ring 1 | **HIGH** — held across async write_batch() |
| `Arc<tokio::sync::Mutex<DynSourceHandle>>` | `db.rs:2440` | Ring 1 | **HIGH** — held across async poll_batch() |
| `parking_lot::Mutex<TableStore>` | `db.rs:2933` | Ring 1 | MEDIUM — synchronous, short-held |
| `RwLock` on subscriber list | `streaming/sink.rs:217` | Ring 1 | LOW — write-rare, read-frequent |

**Critical contention point:** Sink Mutex is held for the entire duration of `write_batch().await`. If Kafka broker is slow, the lock is held for seconds, blocking checkpoint coordinator's `pre_commit()` call which also needs to lock the same sink.

### 3.3 Serialization Overhead

| Sink | Serialization Location | Ring | Issue |
|------|----------------------|------|-------|
| KafkaSink | Per-row serialization in `write_batch()` | Ring 1 | Acceptable — unavoidable for Kafka produce |
| PostgresSink | Batch-level COPY/UNNEST construction | Ring 1 | Acceptable — deferred to flush |
| WebSocketSink | Double serialization (batch→JSON, then message→JSON) | Ring 1 | **MEDIUM** — two allocations per write |
| MockSink | `batch.clone()` into Vec | Ring 1 | Acceptable — test only |

In-memory sinks (stream subscriptions) do NOT serialize — they pass `RecordBatch` directly via SPSC ring buffer. This is correct.

### 3.4 Batch Formation

| Sink | Batch Size | Batch Timeout | Strategy |
|------|-----------|--------------|----------|
| KafkaSink | 16KB / 1000 records | 5ms (linger_ms) | rdkafka internal |
| PostgresSink | 4096 rows | 1s | Time OR size, whichever first |
| WebSocketSink | None | None (immediate) | Per-batch broadcast |
| Pipeline level | None | 100ms (poll interval) | Timer-driven micro-batch |

| Check | Status | Notes |
|-------|--------|-------|
| Configurable batch size AND timeout | **PARTIAL** | Kafka and Postgres yes; WebSocket and pipeline-level no |
| No sleep-in-loop for timeout | PASS | `tokio::select!` with `tokio::time::sleep` |
| Empty batches not flushed | PASS | `if filtered.num_rows() > 0` guard at line 2902 |
| Pre-allocated batch buffers | **PARTIAL** | Postgres buffers `Vec<RecordBatch>` but doesn't pre-allocate |

---

## Phase 4: Pipeline Integration Correctness

### 4.1 End-to-End Data Flow

```
Step 1: tokio::select! { sleep(100ms) } wakes pipeline task              [Ring 1]
Step 2: source_arc.lock().await.poll_batch() for each source              [Ring 1, sequential]
Step 3: Watermark extraction and late-row filtering                        [Ring 1]
Step 4: executor.execute_cycle(&source_batches, watermark)                 [Ring 1, DataFusion]
Step 5: Push results to stream_sources via src.push_arrow(batch.clone())   [Ring 1→0 via SPSC]
Step 6: For each sink, lock → filter → write_batch() → release lock       [Ring 1, sequential]
```

| Step | Unnecessary Copy? | Thread Boundary? | Allocation? | Await Point? |
|------|-------------------|------------------|-------------|-------------|
| 1 | No | No | No | Yes (sleep) |
| 2 | No | No | HashMap entry | Yes (poll_batch) |
| 3 | Yes — `filter_late_rows` creates new RecordBatch | No | Yes | No |
| 4 | Depends on DataFusion plan | No | Yes (DataFusion internal) | Yes |
| 5 | Yes — `batch.clone()` (cheap Arc bump) | Yes (SPSC to Ring 0) | No | No |
| 6 | Yes — `batch.clone()` when no filter (line 2899) | No | Mutex lock | Yes (write_batch) |

### 4.2 Emit Semantics → Sink Interaction

**File:** `crates/laminar-core/src/operator/window.rs:255-443`

| Check | Status | Notes |
|-------|--------|-------|
| EMIT ON WINDOW CLOSE: one final result | PASS | Timer fires once, state purged immediately (line 3851) |
| EMIT ON UPDATE: incremental with retractions | PASS | Emits on every `state_updated=true` (line 3777) |
| Watermark triggers emission | PASS | Timer registered at `window_end + allowed_lateness` (line 3557) |
| Ordered output within key | PASS | Event timestamp set to `window_id.end` (line 3872) |
| Late data handling correct | PASS | OnWindowClose drops late data (line 3714); configurable side output |

**Emit strategies implemented:** OnWatermark, Periodic, OnUpdate, OnWindowClose, Changelog, Final — all 6 are complete.

**Session windows:** NOT IMPLEMENTED (line 9 comment: "future"). Only TumblingWindowAssigner exists.

### 4.3 Checkpoint ↔ Sink Coordination

**File:** `crates/laminar-db/src/checkpoint_coordinator.rs:390-510`

| Check | Status | Notes |
|-------|--------|-------|
| Sink flushes before checkpoint ACK | PASS | Step 4: `pre_commit_sinks(epoch)` before manifest persist |
| Exactly-once: checkpoint includes sink commit | PASS | Step 6: `commit_sinks_tracked(epoch)` after manifest |
| Sink position in checkpoint state | PASS | `manifest.sink_epochs` and `manifest.sink_commit_statuses` |
| Recovery resumes from checkpoint | PASS | Source offsets + operator states restored |
| At-least-once: async flush OK | PASS | Non-exactly-once sinks skip pre_commit/commit |
| Manifest saved twice (before + after commit) | PASS | Line 465 (Pending) and line 493 (final statuses) |
| Rollback on manifest persist failure | PASS | Line 472: `rollback_sinks(epoch)` |

**2PC protocol is correctly implemented.** The seven-phase cycle (barrier → snapshot → source checkpoint → sink pre-commit → manifest persist → sink commit → cleanup) follows standard Chandy-Lamport with exactly-once extension.

### 4.4 Multi-Sink Fan-Out

**File:** `crates/laminar-core/src/streaming/sink.rs:36-250`

| Check | Status | Notes |
|-------|--------|-------|
| Multiple sinks share computation | **PARTIAL** | In-memory subscriptions use broadcast; external sinks iterate same result HashMap |
| Broadcast channel used | PASS | For in-memory subscribers (SinkMode::Broadcast) |
| Slowest sink doesn't block others | **FAIL** | External sinks iterated sequentially in pipeline loop |
| Failure isolation | PASS | Sink write errors logged but don't halt pipeline (line 2905-2911) |

**External sink fan-out architecture (db.rs:2880-2916):**
```rust
for (sink_name, sink_arc, _, filter_expr) in &sinks {
    for (stream_name, batches) in &results {  // All sinks get all results
        for batch in batches {
            sink_arc.lock().await.write_batch(&filtered).await  // SEQUENTIAL
        }
    }
}
```

All sinks receive ALL query results regardless of their FROM clause. Filtering is only by the sink's WHERE expression. There is no routing of specific query outputs to specific sinks.

---

## Phase 5: Gap Analysis

### CRITICAL Issues

### [CRITICAL-1] Subscription `recv()` Unbounded Spin Loop

**Location:** `crates/laminar-core/src/streaming/subscription.rs:116-129`
**Category:** Spin Loop
**Ring Violation:** Ring 1
**Impact:** CPU Waste, potential thread starvation

**Current Behavior:**
```rust
pub fn recv(&self) -> Result<RecordBatch, RecvError> {
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        std::hint::spin_loop();  // Burns CPU indefinitely
    }
}
```

**Expected Behavior:** Should use exponential backoff or async notification. A consumer calling `recv()` on an empty subscription will burn 100% CPU on one core until data arrives.

**Fix:**
```rust
pub fn recv(&self) -> Result<RecordBatch, RecvError> {
    let mut spins = 0u32;
    loop {
        if let Some(batch) = self.poll() {
            return Ok(batch);
        }
        if self.is_disconnected() {
            return Err(RecvError::Disconnected);
        }
        if spins < 64 {
            std::hint::spin_loop();
            spins += 1;
        } else if spins < 128 {
            std::thread::yield_now();
            spins += 1;
        } else {
            std::thread::park_timeout(Duration::from_micros(100));
        }
    }
}
```

**Effort:** S (< 1 hour)

---

### [CRITICAL-2] CreditGate `acquire_blocking()` Unbounded Spin

**Location:** `crates/laminar-core/src/tpc/backpressure.rs:247-258`
**Category:** Spin Loop
**Ring Violation:** Ring 1 (caller is pipeline coordination thread)
**Impact:** CPU Waste, main thread blocked

**Current Behavior:**
```rust
pub fn acquire_blocking(&self, n: usize) {
    loop {
        match self.try_acquire_n(n) {
            CreditAcquireResult::Acquired | CreditAcquireResult::Dropped => return,
            CreditAcquireResult::WouldBlock => {
                std::hint::spin_loop();  // No backoff, no timeout
            }
        }
    }
}
```

Called from `core_handle.rs:261` when `OverflowStrategy::Block` is configured (which is the default). The sending thread (Ring 1 tokio task or coordination thread) spins with only a PAUSE hint until credits become available.

**Expected Behavior:** Progressive backoff: spin → yield → park.

**Fix:**
```rust
pub fn acquire_blocking(&self, n: usize) {
    let mut attempt = 0u32;
    loop {
        match self.try_acquire_n(n) {
            CreditAcquireResult::Acquired | CreditAcquireResult::Dropped => return,
            CreditAcquireResult::WouldBlock => {
                if attempt < 64 {
                    std::hint::spin_loop();
                } else if attempt < 128 {
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(Duration::from_micros(50));
                }
                attempt += 1;
            }
        }
    }
}
```

**Effort:** S (< 1 hour)

---

### [CRITICAL-3] Sinks Wrapped in `Arc<tokio::sync::Mutex>` — Contention with Checkpoint

**Location:** `crates/laminar-db/src/db.rs:2451, 2904`
**Category:** Lock Contention
**Ring Violation:** None (both Ring 1)
**Impact:** Latency, checkpoint stall

**Current Behavior:**
```rust
// Line 2451: Declaration
Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>

// Line 2904: Pipeline loop holds lock across write_batch()
sink_arc.lock().await.write_batch(&filtered).await

// Checkpoint coordinator also needs to lock for pre_commit/commit
```

If a sink's `write_batch()` takes long (e.g., Kafka broker slow), the Mutex is held for that duration. The checkpoint coordinator's `pre_commit_sinks()` must acquire the same lock, causing checkpoint delay.

**Expected Behavior:** Sink write and checkpoint operations should not contend on the same lock. Either use a dedicated channel to serialize operations, or give the checkpoint coordinator exclusive access via a separate synchronization mechanism.

**Fix:** Decouple pipeline writes from checkpoint operations by giving each sink its own write channel:
```rust
// Replace Arc<Mutex<Sink>> with a channel-based approach:
struct SinkHandle {
    tx: mpsc::Sender<SinkCommand>,
}

enum SinkCommand {
    WriteBatch(RecordBatch),
    Flush,
    PreCommit(u64),
    CommitEpoch(u64),
    Close,
}

// Sink runs in its own tokio task, processing commands sequentially
```

**Effort:** L (4+ hours)

---

### HIGH Issues

### [HIGH-1] No Periodic Sink Flush During Operation

**Location:** `crates/laminar-db/src/db.rs:3141-3145`
**Category:** Missing Feature
**Ring Violation:** None
**Impact:** Data loss risk on crash

**Current Behavior:** `flush()` is only called at shutdown (line 3143). During normal operation, sinks that buffer data (PostgresSink with 1s flush interval) rely on their internal timers. But since `write_batch()` is called synchronously in the pipeline loop, sinks don't have their own timer tasks — they only flush when `write_batch()` triggers their internal threshold.

If the pipeline crashes between writes, buffered data in PostgresSink is lost (up to `batch_size` rows or `flush_interval` duration of data).

**Fix:** Add periodic flush to the pipeline loop:
```rust
// In the main pipeline loop, add a flush interval
let mut flush_interval = tokio::time::interval(Duration::from_secs(5));

loop {
    tokio::select! {
        () = shutdown.notified() => break,
        () = tokio::time::sleep(Duration::from_millis(100)) => {
            // ... existing poll/execute/write cycle ...
        }
        _ = flush_interval.tick() => {
            for (name, sink_arc, _, _) in &sinks {
                if let Err(e) = sink_arc.lock().await.flush().await {
                    tracing::warn!(sink = %name, error = %e, "Periodic flush error");
                }
            }
        }
    }
}
```

**Effort:** S (< 1 hour)

---

### [HIGH-2] Sequential Sink Writes Block Pipeline

**Location:** `crates/laminar-db/src/db.rs:2880-2916`
**Category:** Performance
**Ring Violation:** None
**Impact:** Latency (slowest sink determines cycle time)

**Current Behavior:** All sinks are written to sequentially in a nested loop. If sink A takes 50ms and sink B takes 5ms, every cycle takes at least 55ms for sink writes alone. With 100ms poll interval, sink writes consume >50% of cycle budget.

**Expected Behavior:** Sink writes should be parallelized. Since each sink has its own Mutex, they can be written concurrently.

**Fix:**
```rust
// Replace sequential loop with concurrent writes
let sink_futures: Vec<_> = sinks.iter().map(|(sink_name, sink_arc, _, filter_expr)| {
    let results = &results;
    async move {
        for (stream_name, batches) in results {
            for batch in batches {
                let filtered = /* apply filter */;
                if filtered.num_rows() > 0 {
                    if let Err(e) = sink_arc.lock().await.write_batch(&filtered).await {
                        tracing::warn!(sink = %sink_name, error = %e, "Sink write error");
                    }
                }
            }
        }
    }
}).collect();
futures::future::join_all(sink_futures).await;
```

**Effort:** M (1-4 hours)

---

### [HIGH-3] All Sinks Receive All Query Results (No Routing)

**Location:** `crates/laminar-db/src/db.rs:2881`
**Category:** Correctness / Performance
**Ring Violation:** None
**Impact:** Wasted sink writes

**Current Behavior:**
```rust
for (sink_name, sink_arc, _, filter_expr) in &sinks {
    for (stream_name, batches) in &results {  // ALL results to ALL sinks
```

Every external sink receives results from ALL streaming queries, not just the query its FROM clause references. A sink created with `FROM suspicious_sessions` also receives results from every other stream. The only guard is the optional WHERE filter.

**Expected Behavior:** Each sink should only receive results from its registered input stream/view.

**Fix:** Filter results by sink's input stream name:
```rust
for (sink_name, sink_arc, _, filter_expr) in &sinks {
    let sink_input = &sink_input_map[sink_name]; // FROM clause target
    if let Some(batches) = results.get(sink_input) {
        for batch in batches {
            // ... apply filter and write
        }
    }
}
```

**Effort:** M (1-4 hours)

---

### [HIGH-4] `HashMap::new()` Allocated Every Pipeline Cycle

**Location:** `crates/laminar-db/src/db.rs:2766`
**Category:** Allocation
**Ring Violation:** None (Ring 1)
**Impact:** Unnecessary allocation every 100ms

**Current Behavior:**
```rust
let mut source_batches = HashMap::new();  // Allocated every cycle
```

**Fix:** Hoist outside loop and reuse:
```rust
let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
loop {
    source_batches.clear();
    // ... use source_batches ...
}
```

**Effort:** S (< 1 hour)

---

### MEDIUM Issues

### [MEDIUM-1] Dispatcher Spin Iterations Default Too Aggressive

**Location:** `crates/laminar-core/src/subscription/dispatcher.rs:211-214`
**Category:** Spin Loop
**Ring Violation:** Ring 1
**Impact:** CPU Waste on idle, tokio task starvation

**Current Behavior:**
```rust
for _ in 0..self.config.spin_iterations {  // Default: 100
    std::hint::spin_loop();
}
tokio::time::sleep(self.config.idle_sleep).await;  // Default: 10μs
```

100 spin iterations in a tokio async context prevents the runtime from scheduling other tasks during the spin phase.

**Fix:** Reduce default `spin_iterations` to 10-20 for Ring 1 async context. Consider using `tokio::sync::Notify` instead of polling.

**Effort:** S (< 1 hour)

---

### [MEDIUM-2] WebSocket Sink Double Serialization

**Location:** `crates/laminar-connectors/src/websocket/sink.rs:324-338`
**Category:** Allocation
**Ring Violation:** None (Ring 2)
**Impact:** Extra allocation per write

**Current Behavior:**
1. `serialize_to_json(batch)` → String (allocation 1)
2. `serde_json::to_string(&msg)` → String (allocation 2)
3. `Bytes::from(serialized)` → Bytes (move, no copy)

**Fix:** Serialize directly into a reusable `Vec<u8>` buffer:
```rust
let mut buf = Vec::with_capacity(4096);
self.serializer.serialize_to_writer(batch, &mut buf)?;
// Wrap in ServerMessage and serialize once to final Bytes
```

**Effort:** M (1-4 hours)

---

### [MEDIUM-3] DagSinkBridge Infrastructure Unused

**Location:** `crates/laminar-connectors/src/bridge/sink_bridge.rs` (entire file)
**Category:** Dead Code / Missing Integration
**Ring Violation:** None
**Impact:** Wasted implementation effort, missing features (epoch lifecycle not wired)

**Current Behavior:** `DagSinkBridge` and `ConnectorBridgeRuntime` are fully implemented with:
- DAG executor integration via `flush_outputs()`
- `take_sink_outputs()` draining pattern
- Epoch lifecycle management (begin/commit/rollback)
- Metrics tracking

But the actual pipeline in `db.rs` bypasses all of this and calls `write_batch()` directly on the raw connector.

**Expected Behavior:** Pipeline should use `DagSinkBridge` for proper epoch lifecycle management and decoupled sink writes.

**Effort:** L (4+ hours)

---

### [MEDIUM-4] PostgreSQL Default Flush Interval of 1 Second

**Location:** `crates/laminar-connectors/src/postgres/sink_config.rs:84`
**Category:** Configuration
**Ring Violation:** None
**Impact:** Up to 1s latency for append-mode writes

**Current Behavior:**
```rust
flush_interval: Duration::from_secs(1),
```

For streaming use cases, 1 second is a long delay. Combined with the fact that pipeline-level periodic flush doesn't exist (HIGH-1), data may sit in PostgresSink's buffer for up to 1 second plus the time until the next `write_batch()` call triggers the internal timer check.

**Fix:** Reduce default to 100-250ms.

**Effort:** S (< 1 hour)

---

### LOW Issues

### [LOW-1] Subscription `recv_timeout()` Spin with Deadline

**Location:** `crates/laminar-core/src/streaming/subscription.rs:137-155`
**Category:** Spin Loop
**Ring Violation:** Ring 1
**Impact:** CPU waste during timeout wait

Same pattern as CRITICAL-1 but with a deadline. Still burns CPU until deadline or data arrival. Same fix applies (progressive backoff).

**Effort:** S (< 1 hour)

---

### [LOW-2] `async_trait` Overhead on SinkConnector

**Location:** `crates/laminar-connectors/src/connector.rs:303`
**Category:** Allocation
**Ring Violation:** None (all Ring 1+)
**Impact:** Box::pin() allocation per write_batch() call

For current Ring 1/2 usage, acceptable. If sink writes move to Ring 0 in the future, manual Future impl needed.

**Effort:** N/A (future consideration)

---

## Phase 6: Known Issues Investigation

### 6.1 Config Pipeline Breakage

**Status: NOT BROKEN** (for current implementation)

Traced the full path:
```
SQL "WITH ('topic'='events')"
  → parser/sink_parser.rs extracts into CreateSinkStatement.connector_options
  → db.rs:639-656 resolves connector_type and options (supports both INTO and WITH syntax)
  → connector_manager::build_sink_config(reg) constructs ConnectorConfig
  → registry.create_sink(&config) instantiates connector with full config
```

Both `connector_options` (from INTO clause) and `with_options` (from WITH clause) are properly merged. The `extract_connector_from_with_options()` helper correctly splits connector-specific and format-specific options.

### 6.2 Session Window Partial Emission

**Status: NOT APPLICABLE** — Session windows are not implemented. Only tumbling windows exist (`TumblingWindowAssigner` at `window.rs:741`). Comment at line 9 indicates "Session: future".

### 6.3 DataFusion Stateless Execution

**Status: CORRECTLY HANDLED**

DataFusion's native aggregation operators are stateless between batches. The streaming validator at `streaming_optimizer.rs:122-138` explicitly rejects unbounded final aggregation:

```rust
"Final aggregation on unbounded input will never emit results.
 Use a window function (TUMBLE/HOP/SESSION) or add an EMIT clause."
```

Windowed aggregation state is maintained by LaminarDB's custom `TumblingWindowOperator` in its state store (`window.rs:3530-3533`), not by DataFusion. State persists across micro-batches via the operator's internal HashMap and is included in checkpoint snapshots.

### 6.4 Sink Backpressure → Source Stall

**Status: POTENTIAL ISSUE**

Analysis of the backpressure chain when a sink is slow:

1. `sink_arc.lock().await.write_batch(&filtered).await` blocks the pipeline tokio task
2. While blocked, no new `poll_batch()` calls occur — sources effectively stall
3. Ring 0 continues independently (own thread, SPSC inbox/outbox)
4. Ring 0 outbox may fill up → outputs dropped silently (`outputs_dropped` counter incremented)

**Verdict:** Ring 0 does NOT block (correct). But the pipeline task stalls entirely — no source polling, no query execution, no other sink writes. This is because everything runs in a single tokio task with sequential operations.

**Mitigation:** Separate sink writes into their own tasks (see CRITICAL-3 fix). Add bounded channel between pipeline output and sink task with configurable backpressure (drop vs block).

---

## Quality Gate Checklist

- [x] Every Rust file related to sinks, pipeline output, and emit has been read
- [x] Every `loop {}` in the pipeline+sink path has been classified
- [x] Every channel between pipeline and sink verified (finding: no channels exist — direct calls)
- [x] Every allocation in Ring 0 → sink handoff identified
- [x] Gap report has zero TBD entries
- [x] Fix plan has concrete code snippets for every CRITICAL and HIGH issue
- [x] No fix introduces a new Ring 0 constraint violation
