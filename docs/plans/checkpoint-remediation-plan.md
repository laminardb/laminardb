# LaminarDB Checkpoint Remediation Plan

> **Created:** 2026-02-28
> **Status:** Phase 1 Complete
> **Audit Score:** 72/100 → Target: 92/100
> **Tracking:** Update status fields as work progresses

---

## Table of Contents

1. [Overview](#overview)
2. [Phase 1: Quick Wins (Critical + Small)](#phase-1-quick-wins)
3. [Phase 2: End-to-End Barrier Wiring](#phase-2-end-to-end-barrier-wiring)
4. [Phase 3: Operational Maturity](#phase-3-operational-maturity)
5. [Phase 4: Performance & Advanced](#phase-4-performance--advanced)
6. [Architecture Concerns Resolution](#architecture-concerns-resolution)
7. [Research Notes](#research-notes)

---

## Overview

### Problem Statement

LaminarDB has a well-designed checkpoint architecture with correct building blocks
(barriers, alignment, two-phase commit, incremental snapshots) but critical wiring
gaps prevent end-to-end exactly-once semantics. The main issue: **barriers exist but
don't flow through the pipeline**, so checkpoint snapshots are taken at arbitrary
points rather than barrier-consistent points.

### Severity Summary

| Severity | Count | Phase |
|----------|-------|-------|
| Critical | 3 | Phase 1-2 |
| High | 5 | Phase 2-3 |
| Medium | 6 | Phase 3-4 |
| Low | 4 | Phase 4 |

### Success Criteria

- All critical gaps resolved
- End-to-end checkpoint test: source → operator → sink with verified exactly-once
- Recovery test: crash mid-checkpoint, verify fallback to previous checkpoint
- Benchmark: checkpoint cycle < 500ms for 100MB state
- Audit score: 92+/100

---

## Phase 1: Quick Wins

**Goal:** Fix critical safety issues that are small in scope.
**Estimated Effort:** 2-3 days total
**Priority:** Do first — these are high-impact, low-effort fixes.

---

### Task 1.1: Recovery Manager Fallback to Previous Checkpoints

**Status:** `[x] Complete`
**Severity:** Critical | **Effort:** S (2-3 hours)
**Ring Impact:** Ring 2 (control plane)

#### Problem

`crates/laminar-db/src/recovery_manager.rs` only calls `store.load_latest()`.
If the latest checkpoint is corrupt (e.g., crash during manifest write), recovery
fails entirely instead of trying previous checkpoints.

The storage-layer `RecoveryManager` (`crates/laminar-storage/src/checkpoint/recovery.rs`)
already has `max_fallback_attempts` — but the db-layer doesn't use this capability.

#### Solution

Update `RecoveryManager::recover()` to iterate through available checkpoints on failure:

```rust
// In crates/laminar-db/src/recovery_manager.rs

pub(crate) async fn recover(
    &self,
    sources: &[RegisteredSource],
    sinks: &[RegisteredSink],
    table_sources: &[RegisteredSource],
) -> Result<Option<RecoveredState>, DbError> {
    // Step 1: Try load_latest() first (fast path)
    match self.store.load_latest() {
        Ok(Some(manifest)) => {
            match self.try_recover_from(manifest, sources, sinks, table_sources).await {
                Ok(state) => return Ok(Some(state)),
                Err(e) => {
                    warn!(error = %e, "latest checkpoint recovery failed, trying fallback");
                }
            }
        }
        Ok(None) => {
            info!("no checkpoint found, starting fresh");
            return Ok(None);
        }
        Err(e) => {
            warn!(error = %e, "failed to load latest checkpoint, trying fallback");
        }
    }

    // Step 2: Fallback — iterate through all checkpoints in reverse order
    let checkpoints = self.store.list()
        .map_err(|e| DbError::Checkpoint(format!("failed to list checkpoints: {e}")))?;

    for (checkpoint_id, _epoch) in checkpoints.iter().rev() {
        match self.store.load_by_id(*checkpoint_id) {
            Ok(Some(manifest)) => {
                match self.try_recover_from(manifest, sources, sinks, table_sources).await {
                    Ok(state) => {
                        info!(checkpoint_id, "recovered from fallback checkpoint");
                        return Ok(Some(state));
                    }
                    Err(e) => {
                        warn!(checkpoint_id, error = %e, "fallback checkpoint recovery failed");
                    }
                }
            }
            Ok(None) | Err(_) => continue,
        }
    }

    // Step 3: All checkpoints exhausted — start fresh with warning
    warn!("all checkpoints failed recovery, starting fresh");
    Ok(None)
}
```

#### Files to Modify

- `crates/laminar-db/src/recovery_manager.rs` — Add fallback loop
- Extract current `recover()` body into `try_recover_from()` private method

#### Tests to Add

- `test_recover_fallback_to_previous` — corrupt latest, verify previous loads
- `test_recover_all_corrupt_starts_fresh` — all corrupt, verify fresh start

#### Acceptance Criteria

- [x] `load_latest()` failure triggers fallback
- [x] Iterates checkpoints in reverse chronological order
- [x] Logs warning for each failed fallback attempt
- [x] Falls through to fresh start if all checkpoints fail
- [x] Unit tests pass

---

### Task 1.2: Non-Replayable Source Validation

**Status:** `[x] Complete`
**Severity:** Critical | **Effort:** S (2-3 hours)
**Ring Impact:** Ring 1 (connector layer)

#### Problem

Sources like WebSocket cannot replay data after a crash, but the system silently
enables checkpointing for them, giving users a false sense of exactly-once guarantees.

#### Solution

**Step A:** Add `supports_replay()` to `SourceConnector` trait.

```rust
// In crates/laminar-connectors/src/connector.rs
pub trait SourceConnector: Send {
    // ... existing methods ...

    /// Whether this source supports replay from a checkpointed position.
    ///
    /// Sources that return `false` (e.g., WebSocket, raw TCP) cannot guarantee
    /// exactly-once semantics. The system will log a warning when checkpointing
    /// is enabled with non-replayable sources.
    fn supports_replay(&self) -> bool {
        true // Default: most sources support replay
    }
}
```

**Step B:** Override in non-replayable sources.

```rust
// In crates/laminar-connectors/src/websocket/source.rs
impl SourceConnector for WebSocketSource {
    fn supports_replay(&self) -> bool {
        false
    }
    // ...
}
```

**Step C:** Validate in `CheckpointCoordinator::register_source()`.

```rust
// In crates/laminar-db/src/checkpoint_coordinator.rs
pub fn register_source(
    &mut self,
    name: impl Into<String>,
    connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    supports_replay: bool,
) {
    let name = name.into();
    if !supports_replay {
        warn!(
            source = %name,
            "source does not support replay — exactly-once semantics \
             are degraded to at-most-once for this source"
        );
    }
    self.sources.push(RegisteredSource {
        name,
        connector,
        supports_replay,
    });
}
```

#### Files to Modify

- `crates/laminar-connectors/src/connector.rs` — Add trait method
- `crates/laminar-connectors/src/websocket/source.rs` — Override to `false`
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Add validation
- Any source that isn't replayable needs override

#### Tests to Add

- `test_non_replayable_source_warning` — verify warning is logged

#### Acceptance Criteria

- [x] `supports_replay()` default returns `true`
- [x] WebSocket source returns `false`
- [x] Warning logged when non-replayable source registered with checkpointing
- [x] No behavior change for replayable sources

---

### Task 1.3: Pipeline Topology Validation on Restore

**Status:** `[x] Complete`
**Severity:** High | **Effort:** S (2-3 hours)
**Ring Impact:** Ring 2

#### Problem

Can restore a checkpoint into a pipeline with different sources/sinks without
detection. Only per-operator schema fingerprinting catches some incompatibilities.

#### Solution

Add topology metadata to `CheckpointManifest`:

```rust
// In crates/laminar-storage/src/checkpoint_manifest.rs
pub struct CheckpointManifest {
    // ... existing fields ...

    /// Names of registered sources at checkpoint time.
    #[serde(default)]
    pub source_names: Vec<String>,

    /// Names of registered sinks at checkpoint time.
    #[serde(default)]
    pub sink_names: Vec<String>,

    /// Hash of the pipeline query/topology for compatibility detection.
    #[serde(default)]
    pub pipeline_hash: Option<u64>,
}
```

Add validation in `CheckpointManifest::validate()`:

```rust
pub fn validate_topology(
    &self,
    current_sources: &[String],
    current_sinks: &[String],
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    if !self.source_names.is_empty() {
        let checkpoint_set: HashSet<_> = self.source_names.iter().collect();
        let current_set: HashSet<_> = current_sources.iter().collect();

        let added: Vec<_> = current_set.difference(&checkpoint_set).collect();
        let removed: Vec<_> = checkpoint_set.difference(&current_set).collect();

        if !added.is_empty() {
            errors.push(ManifestValidationError {
                message: format!("sources added since checkpoint: {added:?}"),
            });
        }
        if !removed.is_empty() {
            errors.push(ManifestValidationError {
                message: format!("sources removed since checkpoint: {removed:?}"),
            });
        }
    }
    // Same for sinks...
    errors
}
```

#### Files to Modify

- `crates/laminar-storage/src/checkpoint_manifest.rs` — Add fields + validation
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Populate fields during checkpoint
- `crates/laminar-db/src/recovery_manager.rs` — Call validation, warn on mismatch

#### Acceptance Criteria

- [x] Source/sink names persisted in manifest
- [x] Warning logged on topology mismatch during restore
- [x] Recovery still proceeds (warn, don't block — user may intentionally change topology)

---

### Task 1.4: Per-Source Watermark Accuracy

**Status:** `[x] Complete`
**Severity:** Medium | **Effort:** S (30 min)
**Ring Impact:** Ring 2

#### Problem

`collect_source_watermarks()` uses the global watermark as a conservative lower
bound for all sources. A fast source's watermark is underreported.

#### Solution

Capture actual per-source watermarks from the source connectors or watermark tracker
rather than using the global watermark for all sources.

#### Files to Modify

- `crates/laminar-db/src/checkpoint_coordinator.rs` — `collect_source_watermarks()`

---

### Task 1.5: Pipeline Hash in Manifest

**Status:** `[x] Complete`
**Severity:** Medium | **Effort:** S (1 hour)
**Ring Impact:** Ring 2

#### Problem

No query identity in manifest — cross-query checkpoint restoration is theoretically
possible if schemas happen to match.

#### Solution

Add `pipeline_hash: Option<u64>` to `CheckpointManifest`. Compute as hash of
query SQL text + operator names. Warn on restore if mismatched.

#### Files to Modify

- `crates/laminar-storage/src/checkpoint_manifest.rs` — Add field
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Compute and set hash
- `crates/laminar-db/src/recovery_manager.rs` — Validate on restore

---

## Phase 2: End-to-End Barrier Wiring

**Goal:** Wire the barrier protocol end-to-end so checkpoints capture
globally consistent snapshots.
**Estimated Effort:** 7-10 days total
**Priority:** Most important phase — this is the single biggest gap.
**Dependency:** Phase 1 should be done first.

### Context

The core problem is a **7-layer integration gap**:

```
Layer 1: CheckpointCoordinator triggers checkpoint
Layer 2: Barrier injected at sources (BarrierPollHandle)
Layer 3: Source tasks pause and emit barrier event
Layer 4: Barrier flows through pipeline as SourceEvent::Barrier
Layer 5: Fan-in operators align barriers (BarrierAligner)
Layer 6: Operators snapshot state at barrier
Layer 7: Coordinator collects all snapshots, persists manifest
```

All 7 layers have building blocks implemented. The wiring between them is missing.

---

### Task 2.1: Add Barrier Infrastructure to Source Tasks

**Status:** `[ ] Not Started`
**Severity:** Critical (part of C2) | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1

#### Problem

`SourceTaskHandle` and source tasks have no mechanism to receive or inject barriers.
The `SourceEvent` enum lacks a `Barrier` variant.

#### Solution

**Step A:** Add `CheckpointBarrier` variant to `SourceEvent` enum.

```rust
// In crates/laminar-db/src/pipeline/source_event.rs
pub enum SourceEvent {
    Batch(SourceBatch),
    Barrier(CheckpointBarrier),
    // ... existing variants ...
}
```

**Step B:** Add `BarrierPollHandle` to `SourceTaskHandle`.

```rust
// In crates/laminar-db/src/pipeline/source_task.rs
pub struct SourceTaskHandle {
    // ... existing fields ...
    barrier_injector: CheckpointBarrierInjector,
}

impl SourceTaskHandle {
    pub fn inject_barrier(&self, checkpoint_id: u64, flags: u64) {
        self.barrier_injector.trigger(checkpoint_id, flags);
    }
}
```

**Step C:** Modify source task loop to poll for barriers.

In the source task's main loop (the `tokio::spawn` task), after each `poll_batch()`:

```rust
// Check for pending barrier before next poll
let poll_handle = barrier_injector.handle();
if let Some(barrier) = poll_handle.poll(current_epoch) {
    // 1. Capture source checkpoint BEFORE emitting barrier
    let checkpoint = connector.checkpoint();
    let _ = cp_tx.send(checkpoint);

    // 2. Emit barrier event downstream
    let _ = event_tx.send(SourceEvent::Barrier(barrier)).await;

    // 3. Do NOT poll_batch() until barrier is consumed
    //    (barrier acts as a "pause" point)
}
```

#### Files to Modify

- `crates/laminar-db/src/pipeline/source_event.rs` — Add `Barrier` variant
- `crates/laminar-db/src/pipeline/source_task.rs` — Add barrier polling to task loop
- `crates/laminar-db/src/pipeline/mod.rs` — Re-export barrier types if needed

#### Tests to Add

- `test_source_task_emits_barrier` — inject barrier, verify SourceEvent::Barrier received
- `test_source_task_checkpoints_before_barrier` — verify checkpoint captured before barrier emission
- `test_source_task_multiple_barriers` — verify sequential barriers work

#### Acceptance Criteria

- [x] Source tasks poll for barriers on each iteration
- [x] Barrier injection from coordinator triggers `SourceEvent::Barrier`
- [x] Source checkpoint is captured before barrier emission
- [x] Source pauses polling until barrier is consumed by coordinator

---

### Task 2.2: Wire Barrier Injection in Pipeline Coordinator

**Status:** `[ ] Not Started`
**Severity:** Critical (part of C2) | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1, Ring 2

#### Problem

`PipelineCoordinator` calls `callback.maybe_checkpoint(force)` but never injects
barriers into source tasks. The `maybe_checkpoint` callback in `db.rs` captures
state at an arbitrary point.

#### Solution

**Step A:** Add barrier injection to `PipelineCoordinator`.

The coordinator needs access to `SourceTaskHandle` instances for all sources.
When a checkpoint is triggered, it must:

1. Inject barriers on ALL source tasks
2. Wait for all source tasks to emit `SourceEvent::Barrier`
3. Only THEN proceed with state snapshot

```rust
// In crates/laminar-db/src/pipeline/coordinator.rs

/// Triggers a checkpoint by injecting barriers into all source tasks.
///
/// Returns when all barriers have been acknowledged or timeout expires.
async fn trigger_barrier_checkpoint(
    &mut self,
    checkpoint_id: u64,
    epoch: u64,
) -> Result<(), PipelineError> {
    // Step 1: Inject barriers on all source tasks
    for handle in &self.source_handles {
        handle.inject_barrier(checkpoint_id, flags::NONE);
    }

    // Step 2: Wait for all barriers to arrive (with timeout)
    // Barriers arrive as SourceEvent::Barrier in the event channel
    // The coordinator main loop handles them and tracks alignment
    self.pending_barrier = Some(PendingBarrier {
        checkpoint_id,
        epoch,
        sources_pending: self.source_handles.len(),
        sources_aligned: 0,
        started_at: Instant::now(),
    });

    Ok(())
}
```

**Step B:** Handle barrier events in the coordinator's main loop.

When a `SourceEvent::Barrier` arrives from source `i`, mark that source as aligned.
When ALL sources are aligned, proceed with checkpoint.

**Step C:** Modify `PipelineCallback` trait to support barrier-based checkpointing.

```rust
pub trait PipelineCallback: Send + Sync {
    /// Called when all barriers are aligned and checkpoint can proceed.
    async fn checkpoint_aligned(
        &self,
        checkpoint_id: u64,
        epoch: u64,
        source_checkpoints: HashMap<String, SourceCheckpoint>,
    ) -> bool;
}
```

#### Files to Modify

- `crates/laminar-db/src/pipeline/coordinator.rs` — Barrier injection + alignment tracking
- `crates/laminar-db/src/pipeline/config.rs` — Add alignment timeout config
- `crates/laminar-db/src/db.rs` — Update `PipelineCallback` impl

#### Tests to Add

- `test_coordinator_barrier_injection` — verify barriers injected on all sources
- `test_coordinator_barrier_alignment` — verify waiting for all sources
- `test_coordinator_barrier_timeout` — verify timeout handling

---

### Task 2.3: Barrier Propagation Through Operators

**Status:** `[ ] Not Started`
**Severity:** Critical (part of C2) | **Effort:** M (2-3 days)
**Ring Impact:** Ring 0, Ring 1

#### Problem

Operators receive events via `Event` type, not `StreamMessage<Event>`. Barriers
have no path through operator chains. Fan-in operators (joins) don't have
`BarrierAligner` instances.

#### Solution

This is the most architecturally significant change. Two approaches:

**Approach A: StreamMessage envelope (recommended)**

Change the operator processing path to use `StreamMessage<T>` as the envelope:
- Events: `StreamMessage::Event(batch)`
- Barriers: `StreamMessage::Barrier(barrier)`
- Watermarks: `StreamMessage::Watermark(ts)`

Operators that don't care about barriers pass them through unchanged.
Fan-in operators (joins, unions) use `BarrierAligner`.

**Approach B: Out-of-band barrier tracking (simpler, less correct)**

Keep the event path unchanged. Track barrier alignment in the coordinator via
side-channel signals. Less correct because events and barriers don't share ordering.

**Recommendation:** Approach A for the DAG path (already uses `StreamMessage`).
Approach B acceptable as an interim solution for the pipeline path if the DAG
path isn't used yet.

For the pipeline path (stream executor), the operator chain is:
`source → stream_executor → sink`

The stream executor is a single operator, so alignment is trivial (single input).
The barrier just needs to:
1. Trigger `stream_executor.checkpoint_state()`
2. Pass through to the sink
3. Trigger `sink.pre_commit(epoch)`

#### Files to Modify

- `crates/laminar-core/src/dag/executor.rs` — Handle `StreamMessage::Barrier` in event loop
- `crates/laminar-core/src/dag/mod.rs` — Wire `BarrierAligner` into fan-in nodes
- `crates/laminar-db/src/stream_executor.rs` — Handle barrier in execute cycle
- `crates/laminar-db/src/db.rs` — Propagate barrier through pipeline

#### Tests to Add

- `test_barrier_flows_through_single_operator` — source → operator → sink
- `test_barrier_alignment_at_join` — two sources → join → sink
- `test_barrier_triggers_state_snapshot` — verify operator state captured at barrier

---

### Task 2.4: Connect Checkpoint Coordinator to Barrier Flow

**Status:** `[ ] Not Started`
**Severity:** Critical (part of C2) | **Effort:** M (1-2 days)
**Ring Impact:** Ring 2

#### Problem

`CheckpointCoordinator::checkpoint()` receives `operator_states` as a parameter
but has no connection to the barrier flow. It doesn't know when barriers are
aligned. The caller (`db.rs`) passes operator states captured at an arbitrary point.

#### Solution

Restructure the checkpoint flow so the coordinator drives the entire cycle:

```
1. Coordinator: trigger barriers on all sources
2. Sources: emit barrier, capture offset
3. Pipeline: propagate barrier through operators
4. Operators: snapshot state when barrier arrives
5. Sinks: pre-commit when barrier arrives
6. Coordinator: collect all state, build manifest, persist
7. Coordinator: commit sinks
```

The key change: `checkpoint()` should orchestrate steps 1-7 rather than
receiving pre-captured state.

```rust
// New method on CheckpointCoordinator
pub async fn run_checkpoint_cycle(
    &mut self,
    barrier_injectors: &[CheckpointBarrierInjector],
    operator_snapshotter: &dyn OperatorSnapshotter,
) -> Result<CheckpointResult, DbError> {
    let checkpoint_id = self.next_checkpoint_id;
    let epoch = self.epoch;

    // Step 1: Inject barriers
    for injector in barrier_injectors {
        injector.trigger(checkpoint_id, flags::NONE);
    }

    // Step 2-5: Wait for alignment (driven by pipeline coordinator)
    // The pipeline coordinator notifies us when all barriers are aligned
    // and provides collected state

    // Step 6: Build manifest from collected state
    // Step 7: Persist and commit
    // ... (existing checkpoint() logic)
}
```

#### Files to Modify

- `crates/laminar-db/src/checkpoint_coordinator.rs` — Add `run_checkpoint_cycle()`
- `crates/laminar-db/src/db.rs` — Wire new method into pipeline loop

---

### Task 2.5: Join State in Stream Executor Checkpoint

**Status:** `[ ] Not Started`
**Severity:** Medium | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1

#### Problem

`stream_executor.checkpoint_state()` only serializes aggregate and window states.
Join buffers in the non-DAG path are not captured.

#### Solution

Add join state serialization to `StreamExecutorCheckpoint`:

```rust
pub struct StreamExecutorCheckpoint {
    pub version: u32,
    pub agg_states: HashMap<String, AggStateCheckpoint>,
    pub eowc_states: HashMap<String, EowcStateCheckpoint>,
    pub core_window_states: HashMap<String, CoreWindowCheckpoint>,
    // NEW:
    pub join_states: HashMap<String, JoinStateCheckpoint>,
}
```

#### Files to Modify

- `crates/laminar-db/src/stream_executor.rs` — Extend checkpoint/restore
- `crates/laminar-db/src/aggregate_state.rs` — Add `JoinStateCheckpoint` type

---

### Task 2.6: End-to-End Integration Test

**Status:** `[ ] Not Started`
**Severity:** Critical | **Effort:** M (1-2 days)
**Ring Impact:** All

#### Problem

No test validates the full checkpoint → crash → recover → verify-exactly-once cycle.

#### Solution

Create `crates/laminar-db/tests/checkpoint_exactly_once.rs`:

```rust
/// End-to-end exactly-once test:
/// 1. Create pipeline: in-memory source → COUNT(*) aggregate → in-memory sink
/// 2. Insert 1000 events
/// 3. Trigger checkpoint
/// 4. Insert 500 more events
/// 5. Simulate crash (drop pipeline)
/// 6. Restart from checkpoint
/// 7. Verify: aggregate state is exactly at event 1000
/// 8. Insert remaining 500 events
/// 9. Verify: final count is 1500 (not 2000 from double-counting)
```

#### Files to Create

- `crates/laminar-db/tests/checkpoint_exactly_once.rs`

---

## Phase 3: Operational Maturity

**Goal:** Production-grade observability and configuration.
**Estimated Effort:** 5-7 days total
**Dependency:** Phase 2 should be substantially done.

---

### Task 3.1: Checkpoint REST API / CLI

**Status:** `[ ] Not Started`
**Severity:** High | **Effort:** M (2-3 days)
**Ring Impact:** Ring 2

#### Problem

No user-facing way to list checkpoints, inspect sizes, view durations, or
diagnose failures.

#### Solution

Add REST endpoints in `laminar-admin`:

```
GET  /api/v1/checkpoints              → list checkpoints
GET  /api/v1/checkpoints/:id          → get checkpoint details
GET  /api/v1/checkpoints/latest       → get latest checkpoint
GET  /api/v1/checkpoints/stats        → get checkpoint stats
POST /api/v1/checkpoints/trigger      → manual checkpoint trigger
```

Response format:
```json
{
  "checkpoint_id": 42,
  "epoch": 42,
  "timestamp_ms": 1709136000000,
  "duration_ms": 234,
  "state_size_bytes": 1048576,
  "sources": ["kafka_orders", "kafka_users"],
  "sinks": ["postgres_output"],
  "sink_statuses": {"postgres_output": "Committed"},
  "is_incremental": false
}
```

#### Files to Modify

- `crates/laminar-admin/src/api/` — New checkpoint endpoints
- `crates/laminar-db/src/api/` — Expose checkpoint store access

---

### Task 3.2: SQL DDL for Checkpoint Configuration

**Status:** `[ ] Not Started`
**Severity:** High | **Effort:** M (2-3 days)
**Ring Impact:** Ring 2

#### Problem

All checkpoint config is programmatic. For a SQL-first database, this should
be SQL-accessible.

#### Solution

Add SQL commands:

```sql
-- Set checkpoint interval
SET checkpoint_interval = '5s';

-- Disable checkpointing
SET checkpoint_interval = 'off';

-- View checkpoint status
SHOW CHECKPOINT STATUS;
-- Returns: epoch, last_checkpoint_time, last_duration_ms, completed, failed

-- Trigger manual checkpoint
CHECKPOINT;

-- Restore from specific checkpoint
RESTORE FROM CHECKPOINT 42;
```

#### Files to Modify

- `crates/laminar-sql/src/parser/` — Parse new commands
- `crates/laminar-sql/src/planner/` — Plan execution
- `crates/laminar-db/src/api/connection.rs` — Execute commands

---

### Task 3.3: Checkpoint Duration Histogram

**Status:** `[x] Complete`
**Severity:** Medium | **Effort:** S (half day)
**Ring Impact:** Ring 2

#### Problem

Only `last_checkpoint_duration_ms` is tracked. No p50/p95/p99.

#### Solution

Add a simple rolling histogram to `CheckpointStats`:

```rust
pub struct CheckpointStats {
    pub completed: u64,
    pub failed: u64,
    pub last_duration: Option<Duration>,
    pub duration_p50_ms: u64,
    pub duration_p95_ms: u64,
    pub duration_p99_ms: u64,
    pub total_bytes_written: u64,
    pub current_phase: CheckpointPhase,
    pub current_epoch: u64,
}
```

Use a fixed-size ring buffer (e.g., last 100 durations) with sorted percentile
extraction.

#### Files to Modify

- `crates/laminar-db/src/checkpoint_coordinator.rs` — Add histogram tracking

---

### Task 3.4: Recovery Benchmark in CI

**Status:** `[ ] Not Started`
**Severity:** High | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1

#### Problem

The `<60s for 10GB` recovery target is not validated in automated benchmarks.

#### Solution

Add a criterion benchmark:

```rust
// crates/laminar-db/benches/recovery_bench.rs
fn bench_recovery_1gb(c: &mut Criterion) {
    // 1. Create 1GB state (synthetic key-value pairs)
    // 2. Save checkpoint
    // 3. Benchmark: load checkpoint + restore state
    // Target: < 6 seconds for 1GB (scales to 60s for 10GB)
}
```

#### Files to Create

- `crates/laminar-db/benches/recovery_bench.rs`

---

### Task 3.5: State Sidecar Atomicity Fix

**Status:** `[x] Complete`
**Severity:** Medium | **Effort:** S (half day)
**Ring Impact:** Ring 1

#### Problem

In `FileSystemCheckpointStore`, manifest and state sidecar are written separately.
If state write fails after manifest succeeds, recovery reads a manifest pointing
to nonexistent state.

#### Solution

Two options:

**Option A: Write state before manifest (recommended)**

Write `state.bin` first (if needed), then manifest. If state write fails,
manifest is never written. On recovery, if `state.bin` exists without manifest,
it's just an orphan (ignored).

**Option B: Marker file**

Write `state.bin`, then `manifest.json`, then `.complete` marker. Only consider
a checkpoint valid if `.complete` exists.

#### Files to Modify

- `crates/laminar-storage/src/checkpoint_store.rs` — Reorder writes

---

### Task 3.6: Checkpoint on Kafka Rebalance

**Status:** `[ ] Not Started`
**Severity:** Medium | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1

#### Problem

Kafka Streams triggers checkpoint on partition rebalance. LaminarDB doesn't.
On rebalance, partition ownership changes, so checkpointed offsets may become
stale.

#### Solution

Wire rdkafka's rebalance callback to trigger an immediate checkpoint:

```rust
// In crates/laminar-connectors/src/kafka/source.rs
// When rdkafka signals revoke_partitions:
// 1. Trigger immediate checkpoint via coordinator
// 2. Save partition → offset mapping for the partitions being revoked
// 3. On assign_partitions, check if we have checkpoint state for new partitions
```

#### Files to Modify

- `crates/laminar-connectors/src/kafka/source.rs` — Rebalance callback
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Support rebalance-triggered checkpoint

---

## Phase 4: Performance & Advanced

**Goal:** Unaligned checkpoints, io_uring optimization, adaptive intervals.
**Estimated Effort:** 10-14 days total
**Dependency:** Phase 2 must be complete.

---

### Task 4.1: Unaligned Checkpoints (F-DCKP-006)

**Status:** `[ ] Not Started`
**Severity:** High | **Effort:** L (5-7 days)
**Ring Impact:** Ring 0, Ring 1

#### Problem

Under back-pressure, aligned checkpoints can take minutes because slow operators
block barrier alignment. This can cause checkpoint timeouts and cascading failures.

#### Background (2026 Research)

Flink 2.0 disabled unaligned checkpoints for sink expansion topology (committer
and pre/post commit) because committables need to be at the respective operators
on `notifyCheckpointComplete`. This is relevant — LaminarDB's sink two-phase
commit has the same constraint.

RisingWave implemented "unaligned joins" in 2025 where barriers pass through
immediately instead of waiting for slow downstream operators.

#### Solution

Implement the Flink 1.11+ unaligned checkpoint protocol:

1. When a barrier arrives at a backpressured operator, instead of waiting for
   alignment, store the in-flight data (events between barriers from different
   inputs) as part of the checkpoint.

2. On recovery, replay the in-flight data before processing new events.

3. **Exception:** Disable unaligned mode for sink operators (same as Flink 2.0).

```rust
pub struct UnalignedCheckpointData<T> {
    /// In-flight events from inputs that have received the barrier
    /// but the operator hasn't aligned yet.
    pub in_flight: Vec<Vec<T>>,
    /// The operator state at the moment the first barrier arrived.
    pub operator_state: Vec<u8>,
}
```

#### Spec Reference

- `docs/features/delta/checkpoint/F-DCKP-006-unaligned-checkpoints.md`

#### Files to Modify

- `crates/laminar-core/src/checkpoint/alignment.rs` — Add unaligned mode
- `crates/laminar-core/src/checkpoint/barrier.rs` — Add UNALIGNED flag
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Support both modes
- `crates/laminar-storage/src/checkpoint_manifest.rs` — Store in-flight data

---

### Task 4.2: io_uring for Checkpoint Persistence

**Status:** `[ ] Not Started`
**Severity:** Medium | **Effort:** L (3-5 days)
**Ring Impact:** Ring 1

#### Background (2026 Research)

Key findings from Jasny et al. (December 2025) and Apache Iggy's production
migration (February 2026):

- **fsync is blocking in io_uring** — falls back to worker threads. For
  checkpoint durability, this is the bottleneck.
- **Registered buffers** eliminate per-request page pinning (~11% improvement).
- **NVMe passthrough** with explicit flush commands offers truly async durability
  but requires raw device access (not applicable to embedded deployment).
- **IOSQE_IO_LINK** flag is essential for ordered checkpoint writes.
- **compio** runtime selected over monoio/glommio in Apache Iggy for active
  maintenance and broad io_uring feature coverage.

LaminarDB already has `CoreRingManager` for io_uring storage I/O. The question
is whether to extend this for checkpoint persistence.

#### Solution

**Phase A (Quick Win):** Use registered buffers for WAL writes.

Already have `CoreRingManager` with SQPOLL. Add registered buffer support
for WAL segment writes to reduce per-write overhead.

**Phase B (Later):** Evaluate moving checkpoint manifest writes to io_uring.

Since fsync is blocking in io_uring, this only helps if we can batch checkpoint
writes with fdatasync. The current `tokio::task::spawn_blocking` approach for
manifest writes may be adequate for embedded use.

#### Files to Modify

- `crates/laminar-storage/src/per_core_wal/writer.rs` — Registered buffers
- `crates/laminar-core/src/tpc/io_uring.rs` — Extend for storage ops

---

### Task 4.3: Adaptive Checkpoint Intervals

**Status:** `[ ] Not Started`
**Severity:** Low | **Effort:** M (2-3 days)
**Ring Impact:** Ring 2

#### Background (2026 Research)

2025 research (Wiley) proposes linear regression-based checkpoint interval
prediction. The idea: monitor checkpoint duration and state growth rate,
dynamically adjust the interval to minimize recovery time while keeping
checkpoint overhead below a threshold.

#### Solution

```rust
pub struct AdaptiveCheckpointConfig {
    /// Minimum interval between checkpoints.
    pub min_interval: Duration,
    /// Maximum interval between checkpoints.
    pub max_interval: Duration,
    /// Target checkpoint duration as fraction of interval (e.g., 0.1 = 10%).
    pub target_overhead_ratio: f64,
    /// State growth rate smoothing factor (EMA alpha).
    pub growth_rate_alpha: f64,
}
```

Monitor:
- Rolling average checkpoint duration
- State size growth rate
- Back-pressure indicators

Adjust interval: if last checkpoint took 5s and target overhead is 10%,
set interval to max(50s, min_interval).

#### Files to Modify

- `crates/laminar-db/src/checkpoint_coordinator.rs` — Add adaptive logic

---

### Task 4.4: Large State Sidecar Threshold

**Status:** `[ ] Not Started`
**Severity:** Medium | **Effort:** M (1-2 days)
**Ring Impact:** Ring 1

#### Problem

Operator state is base64-encoded in JSON manifest. For large state (>10MB), this
is slow and wasteful.

#### Solution

Add threshold-based split: inline small state (<1MB), sidecar large state.

```rust
impl OperatorCheckpoint {
    pub fn from_bytes(data: &[u8], threshold: usize) -> Self {
        if data.len() <= threshold {
            Self::inline(data)
        } else {
            Self::external(format!("state_{}.bin", hash(data)))
        }
    }
}
```

Write external files alongside manifest. Load them back during recovery.

#### Files to Modify

- `crates/laminar-storage/src/checkpoint_manifest.rs` — Threshold logic
- `crates/laminar-storage/src/checkpoint_store.rs` — Sidecar read/write
- `crates/laminar-db/src/checkpoint_coordinator.rs` — Use threshold

---

## Architecture Concerns Resolution

### Concern A: Two Parallel Checkpoint Paths

**Problem:** The `DagCheckpointCoordinator` (barrier-based, correct) and the
`db.rs` pipeline checkpoint (arbitrary-point) are not unified.

**Resolution:** Phase 2 (Tasks 2.1-2.4) addresses this by wiring barrier
injection into the pipeline path. After Phase 2, both paths use barriers.

**Long-term:** Consider deprecating the arbitrary-point path entirely once
barrier wiring is stable.

### Concern B: ObjectStoreCheckpointer vs FileSystemCheckpointStore

**Problem:** Two separate checkpoint persistence abstractions that don't share
a trait.

**Resolution:** This is acceptable for now. `FileSystemCheckpointStore` implements
`CheckpointStore` trait for embedded use. `ObjectStoreCheckpointer` implements
`Checkpointer` trait for cloud use. They serve different deployment models.

**Long-term:** Unify under a single trait when constellation mode (multi-node)
ships. The `ObjectStoreCheckpointer` is the natural choice for distributed
deployments.

### Concern C: Serialization Format Diversity

**Problem:** Manifests are JSON, WAL entries use rkyv, operator state uses
serde_json.

**Resolution (from 2026 research):**

- **JSON for manifests:** Keep. Human-readable, debuggable. Manifests are small.
- **rkyv for WAL and state:** Keep. Zero-copy, mmap-compatible, ideal for
  performance-critical paths. rkyv 0.8 is stable.
- **serde_json for operator state checkpoints:** Replace with rkyv long-term.
  Currently operator state (aggregations, windows) uses serde_json for
  `StreamExecutorCheckpoint`. This is fine for now but should migrate to rkyv
  for zero-copy recovery (Cloudflare's mmap-sync pattern).

**Action:** Add as Task 4.5 (Low priority, future work).

---

## Research Notes

### Flink 2.0 ForSt (2025): Key Insight for LaminarDB

Flink 2.0 introduced ForSt (For Streaming), which co-locates working state and
checkpoint directories with hard-linked file sharing. This achieves 94% reduction
in checkpoint duration because most state files are already durably replicated.

**Applicability to LaminarDB:** The mmap state store could adopt a similar
pattern. Instead of serializing state for checkpoints, create a hard-linked
copy of the mmap file. This would make checkpoints near-instant for large state.
However, this only works for file-backed state (not the in-memory arena mode).

### Scabbard (VLDB 2022): Selective Persistence

The most relevant academic work for single-node embedded streaming. Key insight:
instead of persisting all streams, place persistence points **after operators
that discard data** (high-selectivity operators like filters, aggregations).
This dramatically reduces I/O bandwidth.

**Applicability to LaminarDB:** Currently, LaminarDB checkpoints all operator
state uniformly. An optimization would be to analyze the operator graph and
skip checkpointing for operators that can be cheaply recomputed from upstream
state. E.g., a filter operator's state is empty — no need to checkpoint it.

### Apache Iggy (2026): Thread-Per-Core + io_uring Validation

Iggy's migration from tokio to compio (completion-based runtime with io_uring)
achieved 57-60% tail latency improvements. Key lesson: `io_uring` must be
integrated architecturally, not bolted on.

**Applicability to LaminarDB:** LaminarDB already has the right architecture
(thread-per-core, `CoreRingManager`). The main opportunity is using registered
buffers for WAL writes and exploring compio for the tokio-based Ring 1 tasks.

### CheckMate (ICDE 2024): Coordinated vs Uncoordinated

For single-node, coordinated checkpointing (Chandy-Lamport) is straightforward
since there's no network coordination. However, for skewed workloads (one slow
operator), uncoordinated checkpointing (each operator checkpoints independently)
outperforms by ~10%. Unaligned checkpoints (Task 4.1) are the practical middle
ground.

---

## Progress Tracking

### Phase 1 Checklist

- [x] Task 1.1: Recovery manager fallback
- [x] Task 1.2: Non-replayable source validation
- [x] Task 1.3: Pipeline topology validation
- [x] Task 1.4: Per-source watermark accuracy
- [x] Task 1.5: Pipeline hash in manifest

### Phase 2 Checklist

- [x] Task 2.1: Barrier infrastructure in source tasks
- [x] Task 2.2: Barrier injection in pipeline coordinator
- [x] Task 2.3: Barrier propagation through operators (out-of-band approach — operators don't need barriers since pipeline serializes execution)
- [x] Task 2.4: Connect checkpoint coordinator to barrier flow
- [x] Task 2.5: Join state in stream executor (join_states field + backward-compat serde; ASOF/temporal stateless per-cycle)
- [x] Task 2.6: End-to-end integration test (checkpoint_exactly_once.rs — 4 tests)

### Phase 3 Checklist

- [ ] Task 3.1: Checkpoint REST API / CLI
- [ ] Task 3.2: SQL DDL for checkpoint config
- [x] Task 3.3: Checkpoint duration histogram
- [ ] Task 3.4: Recovery benchmark in CI
- [x] Task 3.5: State sidecar atomicity fix
- [ ] Task 3.6: Checkpoint on Kafka rebalance

### Phase 4 Checklist

- [ ] Task 4.1: Unaligned checkpoints
- [ ] Task 4.2: io_uring for checkpoint persistence
- [ ] Task 4.3: Adaptive checkpoint intervals
- [ ] Task 4.4: Large state sidecar threshold
