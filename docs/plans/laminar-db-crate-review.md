# laminar-db Crate Review & Remediation Plan

**Date**: 2026-03-25
**Scope**: Correctness, safety, and production-readiness of `crates/laminar-db/`
**Method**: Line-by-line review of checkpoint, recovery, execution, sink, and lifecycle code

---

## Executive Summary

Review of the laminar-db crate found 2 CRITICAL, 3 HIGH, 5 MEDIUM, and 3 LOW
issues. The critical issues are both in the checkpoint path and can cause data
loss or permanent operational degradation in exactly-once pipelines.

Phase 1 (8 fixes) and Phase 2 (H2, H3) are complete and passing tests.
Phase 3 (dead code audit, API cleanup) is deferred.

---

## Phase 1: Immediate Fixes — COMPLETE

All fixes applied, clippy clean, 570 tests passing.

### C1. `sink_timed_out` permanently disables checkpointing under exactly-once

**File**: `pipeline_callback.rs:537-543`
**Bug**: Under `ExactlyOnce`, `maybe_checkpoint()` returns early (line 373) before
clearing `sink_timed_out` (line 394). `checkpoint_with_barrier()` checks the flag
but never clears it. A single sink timeout permanently disables all barrier
checkpoints. The pipeline runs without checkpointing; state grows unboundedly.
**Fix**: Clear `sink_timed_out` in `checkpoint_with_barrier()` after skipping.
**Verification**: The two callback paths (`maybe_checkpoint` and
`checkpoint_with_barrier`) are mutually exclusive — the coordinator calls
`callback.maybe_checkpoint` only when `source_handles.is_empty()` (line 640),
and `checkpoint_with_barrier` only when barriers align (requires sources).

### C2. Sinks left in pre-committed state after checkpoint size rejection

**File**: `checkpoint_coordinator.rs:982-1000`
**Bug**: `pre_commit_sinks(epoch)` succeeds (line 929), then size check rejects
(line 982) without calling `rollback_sinks()`. Exactly-once sinks (Kafka
transactions) left with open transactions that eventually timeout or fence.
**Fix**: Call `rollback_sinks(epoch)` before returning from size rejection.
Mirrors existing rollback on manifest persist failure (line 1046).

### H1. `start()` state transition is non-atomic (TOCTOU race)

**File**: `pipeline_lifecycle.rs:83-96`
**Bug**: Separate `load()` + `store()` allows two concurrent `start()` calls to
both read `STATE_CREATED` and both proceed to initialize the pipeline.
**Fix**: `compare_exchange(STATE_CREATED, STATE_STARTING, AcqRel, Acquire)` with
explicit handling of all state variants including `STATE_SHUTTING_DOWN`.
**Known residual**: If `start_connector_pipeline` fails after CAS, state stays
`STATE_STARTING` permanently. This is pre-existing (old code had the same bug).
Fix requires error recovery path through all `start()` failure modes — separate PR.

### M1. `shutdown` flag uses `Relaxed` ordering

**File**: `pipeline_lifecycle.rs:50-57`
**Fix**: `close()` → `Release`, `is_closed()` → `Acquire`.

### M2. Periodic flush timeout equals flush interval

**File**: `sink_task.rs:364-365`
**Fix**: `flush_interval * FLUSH_TIMEOUT_MULTIPLIER` (2x). Distinguishes
slow-but-healthy flushes from stuck flushes.

### M4. No size bound on `post_barrier_buf`

**File**: `pipeline/streaming_coordinator.rs:528`
**Fix**: `MAX_POST_BARRIER_BUF = 10_000` cap. Sheds batches with WARN log when
checkpoint is stuck. Post-barrier data belongs to the next epoch; shed data is
re-delivered on recovery from the prior checkpoint.

### M5. HashMap non-determinism in sidecar packing

**File**: `checkpoint_coordinator.rs:665-675`
**Fix**: Sort operator names before iterating. Deterministic sidecar layout.

### L1. `url_to_checkpoint_prefix` naive string parsing

**File**: `pipeline_lifecycle.rs:23-45`
**Fix**: Strip query parameters (`?`) and fragments (`#`) before path extraction.

---

## Phase 2: Design-Complete, Ready for Implementation

### H2. Operator starvation under budget pressure

**File**: `operator_graph.rs:787-797`

#### Problem

`execute_cycle` processes operators in topological order with a global time
budget (`query_budget_ns`, default 8ms). When budget is exceeded, `break` skips
every remaining operator. Topo order is deterministic — the same tail operators
starve every cycle. Their input buffers accumulate data until
`max_input_buf_batches` (256) sheds them. This is silent data loss.

#### Why naive fixes fail

| Approach | Problem |
|----------|---------|
| Round-robin start position | Breaks topo ordering — Q2 runs before dependency Q1 |
| Per-operator budget slicing | Expensive operators blow their slice; cheap ones waste theirs |
| Warn-only logging | Reports the problem, doesn't fix it, spams under legitimate workloads |

#### Production system comparison

- **Flink**: Per-operator threads + bounded network buffers = natural backpressure
- **Arroyo**: Operator chaining, entire chain is the scheduling unit
- **RisingWave**: Work-stealing scheduler with actor-based operators
- All rely on backpressure propagation, not budget-based skipping

#### Design: Post-budget deferred execution (one operator per cycle, round-robin)

After the budget `break`, execute at most ONE deferred operator with non-empty
input. A round-robin offset (`deferred_scan_offset`) rotates the starting
position across cycles, ensuring fair scheduling when all deferred operators
continuously receive input. Without rotation, the first operator in topo order
would monopolize the deferred slot.

#### Properties

- Topo ordering preserved (deferred ops execute in topo order)
- Bounded budget overrun: at most one extra operator per cycle
- Every operator runs at most N cycles late (N = deferred count)
- One internal field (`deferred_scan_offset: usize`) — not checkpointed
- Zero overhead when no operators are deferred (no budget exhaustion)

#### Scaling: embedded → single-node → distributed

| Mode | Behavior |
|------|----------|
| **Embedded** | Single thread, single graph. Works as-is. |
| **Single-node server** | Same graph, HTTP wrapper. No change. |
| **Distributed** | Each node runs local graph over its vnodes. Deferred execution is local, no cross-node coordination. Checkpoint barriers flow through sources (not the graph), so deferred operators' stale state is correct at snapshot time — recovery replays from checkpoint offset. |

When distributed shuffle operators arrive (Phase 7+), upgrade to full deferred
pass (run ALL deferred operators at start of next cycle) because cross-node
latency makes one-per-cycle too slow. The code change is localized to
`execute_cycle`.

#### Constraint

The deferred state is transient — it MUST NOT be checkpointed. It's execution
bookkeeping, not durable state. On recovery, there are no deferred operators;
everything starts fresh. This is already guaranteed since the mechanism uses
local variables or `Vec<usize>` on the struct (not in `GraphCheckpoint`).

#### Implementation tasks — COMPLETE

1. ~~Extract the operator execution body into `execute_single_operator` helper~~
2. ~~Add post-budget round-robin deferred execution after the `break`~~
3. ~~Add `test_og_budget_deferred_forward_progress`: 5 operators, 1ns budget,
   all produce output within 5 cycles~~
4. ~~Existing `test_og_budget_exhaustion` passes~~

---

### H3. WriteBatch fire-and-forget — no error visibility

**File**: `sink_task.rs:133-143`, `pipeline_callback.rs:249-257`

#### Problem

`SinkTaskHandle::write_batch()` returns `Ok(())` as soon as the channel send
succeeds. The actual write error happens in the task loop, where it:
- Increments `write_errors` (Arc<AtomicU64>) — never read outside the task
- Sets `epoch_poisoned` (Arc<AtomicBool>) — correctly blocks checkpoint pre-commit

The pipeline callback sees `Ok(())` even for failed writes. Under at-least-once,
this is correct (checkpoint fails, source replays). Under exactly-once, this is
also correct (`epoch_poisoned` → pre-commit fails → checkpoint fails → no offset
advance → source replays). The mechanism works.

The real issue is **observability**: the pipeline looks healthy (cycles running,
events ingested) while a sink is dead and the replay window grows unboundedly.

#### Production system comparison

- **Flink**: Sink failure → task fails → TaskManager restarts job from checkpoint
- **Kafka Streams**: `ProductionExceptionHandler` — user provides policy (CONTINUE/FAIL)
- **Arroyo**: Per-sink error handling config, default FAIL for persistent errors
- Common pattern: engine provides the signal, application decides the policy

#### Design: Expose sink errors through existing PipelineCounters

LaminarDB is an embedded database. The application owns the pipeline lifecycle.
The existing `PipelineCounters` struct already has `events_dropped` for
subscription backpressure — same pattern.

```rust
// In PipelineCounters (metrics.rs):
/// Total sink write errors across all sinks.
pub sink_write_errors: AtomicU64,

// In write_to_sinks (pipeline_callback.rs), on error:
if let Some(ref counters) = self.counters {
    counters.sink_write_errors.fetch_add(1, Ordering::Relaxed);
}

// In CounterSnapshot / PipelineMetrics:
pub sink_write_errors: u64,
```

#### What this does NOT include (by design)

- **No accessor methods on SinkTaskHandle** — the per-task `write_errors` is
  internal bookkeeping for `epoch_poisoned`. Pipeline-level counter in
  `PipelineCounters` is the right aggregation level.
- **No circuit breaker** — application-level concern. The application polls
  metrics and decides policy (halt, retry, alert).
- **No retry logic** — connector-level concern. Some sinks already retry
  internally.
- **No new error return paths from `write_to_sinks`** — the fire-and-forget
  pattern is correct for the async channel architecture.

#### Scaling: embedded → single-node → distributed

| Mode | Behavior |
|------|----------|
| **Embedded** | Application polls `PipelineMetrics`, sees counter, decides policy |
| **Single-node server** | Counter flows through existing `GET /metrics` endpoint. Prometheus scrapes `laminar_sink_write_errors`. Alerting rules trigger restarts. |
| **Distributed** | Each node has per-node counter. `sum(laminar_sink_write_errors) by (cluster)` in Prometheus. Per-node `epoch_poisoned` → local checkpoint failure → global barrier failure → all nodes stay at old checkpoint. Identical to Flink's behavior. |

#### Future extension points (Phase 7+)

- Per-sink error counters (when multi-sink pipelines are common)
- Circuit breaker config (when server has a restart manager)
- Distributed sink health aggregation (when cluster coordinator exists)

#### Implementation tasks — COMPLETE

1. ~~Add `sink_write_errors: AtomicU64` to `PipelineCounters` (Ring 2 group)~~
2. ~~Update `PipelineCounters::new()` and `snapshot()`~~
3. ~~Update `CounterSnapshot` struct~~
4. ~~In `pipeline_callback.rs:write_to_sinks`, increment counter on
   `Ok(Err(e))` and timeout~~
5. ~~All existing metrics tests pass (12/12)~~
6. ~~`#[repr(C)]` padding unaffected (Ring 2 group, no padding change)~~
7. ~~Server `GET /metrics` exposes `laminardb_sink_write_errors_total`~~

---

## Phase 3: Deferred

### M3. `stream_executor.rs` blanket `#![allow(dead_code)]`

**File**: `stream_executor.rs:14`
**Status**: 6,026 lines with blanket dead code suppression.
**Approach**: Remove `#![allow(dead_code)]`, compile, triage warnings into:
- `#[cfg(test)]` for test-only code
- Targeted `#[allow(dead_code)]` for intentionally retained utilities
- Deletion for genuinely dead code
**Estimated LOC change**: 200+ (mostly deletions)
**Dependency**: None, but large and mechanical — own PR.

### L3. `CheckpointResult.success` boolean footgun

**Status**: Doc comment added (Phase 1). Proper fix is replacing with an enum:
```rust
pub enum CheckpointOutcome {
    Success { checkpoint_id: u64, epoch: u64, duration: Duration },
    Failure { checkpoint_id: u64, epoch: u64, duration: Duration, error: String },
}
```
**Dependency**: Changes public API surface. Touches every checkpoint call site.
Defer until next breaking change window.

### Pre-existing: `start()` stuck at STATE_STARTING on failure

**Status**: H1 fix (CAS) prevents the race but doesn't fix the stuck-state
issue when `start_connector_pipeline` fails. Needs error recovery path that
resets state to `STATE_CREATED` on all failure branches in `start()`.
**Risk**: Low — `start()` failure is uncommon (config errors, missing
connectors). But a stuck `STATE_STARTING` makes the instance unrecoverable
without restart.

---

## Issue Tracker

| ID | Severity | Status | Summary |
|----|----------|--------|---------|
| C1 | CRITICAL | **DONE** | `sink_timed_out` permanent checkpoint disable |
| C2 | CRITICAL | **DONE** | Missing sink rollback on size rejection |
| H1 | HIGH | **DONE** | Non-atomic state transition in `start()` |
| H2 | HIGH | **DONE** | Operator starvation under budget pressure |
| H3 | HIGH | **DONE** | WriteBatch fire-and-forget, no error visibility |
| M1 | MEDIUM | **DONE** | Relaxed ordering on shutdown flag |
| M2 | MEDIUM | **DONE** | Flush timeout = flush interval |
| M3 | MEDIUM | DEFERRED | stream_executor.rs blanket dead_code allow |
| M4 | MEDIUM | **DONE** | Unbounded post_barrier_buf |
| M5 | MEDIUM | **DONE** | Non-deterministic sidecar packing |
| L1 | LOW | **DONE** | URL parsing ignores query/fragment |
| L2 | LOW | WONTFIX | JSON for checkpoint serialization (known tradeoff) |
| L3 | LOW | DEFERRED | CheckpointResult boolean footgun |

---

## Validation

Phase 1 changes:
- `cargo clippy -p laminar-db -- -D warnings`: clean
- `cargo test -p laminar-db --lib`: 570 passed, 0 failed
- `cargo test -p laminar-core -p laminar-sql -p laminar-storage --lib`: 196 passed, 0 failed
- Files changed: 5, +89 / -29 lines

Phase 2 changes:
- `cargo clippy -p laminar-db -- -D warnings`: clean
- `cargo test -p laminar-db --lib`: 571 passed, 0 failed (+1 new test)
- `cargo test -p laminar-core -p laminar-sql -p laminar-storage --lib`: 196 passed, 0 failed
- Files changed: 4 (`operator_graph.rs`, `metrics.rs`, `pipeline_callback.rs`, `http.rs`)
