# F-SSQL-002: StreamExecutor State Checkpoint Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SSQL-002 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M |
| **Dependencies** | F-SSQL-001, F-CKP-003 |
| **Owner** | TBD |
| **Created** | 2026-02-25 |
| **Updated** | 2026-02-25 |

## Summary

Wire `StreamExecutor`'s aggregation state (both `IncrementalAggState` from the Gap 1 fix and `IncrementalEowcState` from F-SSQL-001) into the unified checkpoint/recovery infrastructure. Currently, a process restart loses all accumulated aggregation state because `StreamExecutor` does not participate in the checkpoint protocol. This feature adds `checkpoint_state() -> Vec<u8>` and `restore_state(bytes)` methods using DataFusion's `Accumulator::state()` / `Accumulator::merge_batch()` for serialization, integrated with `CheckpointCoordinator` under the key `"stream_executor"`.

## Goals

- Serialize all `StreamExecutor` aggregation state (non-EOWC and EOWC) to bytes on checkpoint
- Restore aggregation state from checkpoint bytes on recovery
- Backward-compatible: missing `"stream_executor"` key in checkpoint manifest means no state to restore (fresh start)
- Zero allocation on the hot path (serialization only happens during checkpoint)
- Support schema evolution: if aggregate expressions change between restarts, discard stale state and log a warning

## Non-Goals

- Checkpointing non-aggregate StreamExecutor state (projection/filter queries are stateless)
- Changes to the checkpoint protocol itself (F-CKP-003 is already complete)
- Changes to Ring 0 operator checkpointing (already works via `StateStore`)

## Technical Design

### Architecture

**Ring**: Ring 1 (checkpoint I/O)
**Crate**: `laminar-db`
**Integration point**: `CheckpointCoordinator::trigger_checkpoint()` calls `StreamExecutor::checkpoint_state()`, and `CheckpointCoordinator::restore()` calls `StreamExecutor::restore_state()`.

### Serialization Format

```rust
/// Serialized StreamExecutor state (rkyv-compatible).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct StreamExecutorCheckpoint {
    /// Version tag for forward compatibility.
    version: u32,
    /// Per-query non-EOWC aggregate state.
    /// Key: query name, Value: serialized IncrementalAggState.
    incremental_states: HashMap<String, IncrementalAggCheckpoint>,
    /// Per-query EOWC aggregate state.
    /// Key: query name, Value: serialized IncrementalEowcState.
    eowc_states: HashMap<String, EowcAggCheckpoint>,
}

/// Checkpoint for one IncrementalAggState.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IncrementalAggCheckpoint {
    /// Fingerprint of the aggregate query (for schema evolution detection).
    query_fingerprint: u64,
    /// Per-group accumulator state.
    /// Key: group key as Vec<ScalarValue bytes>.
    /// Value: Vec of accumulator states (one per aggregate function).
    groups: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
}

/// Checkpoint for one IncrementalEowcState.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct EowcAggCheckpoint {
    /// Fingerprint of the aggregate query.
    query_fingerprint: u64,
    /// Per-window accumulator state.
    /// Key: window_start_ms, Value: per-group state (same as IncrementalAggCheckpoint).
    windows: Vec<(i64, Vec<(Vec<u8>, Vec<Vec<u8>>)>)>,
}
```

### API/Interface

```rust
impl StreamExecutor {
    /// Serialize all aggregation state for checkpoint.
    ///
    /// Returns `None` if there is no stateful aggregation state to checkpoint.
    pub fn checkpoint_state(&self) -> Result<Option<Vec<u8>>, DbError> {
        // 1. For each IncrementalAggState: call accumulator.state() for each group
        // 2. For each IncrementalEowcState: same, but nested per-window
        // 3. Serialize via rkyv
        // 4. Return bytes
    }

    /// Restore aggregation state from checkpoint bytes.
    ///
    /// Silently skips queries whose fingerprint doesn't match (schema changed).
    /// Returns the number of queries successfully restored.
    pub fn restore_state(&mut self, bytes: &[u8]) -> Result<usize, DbError> {
        // 1. Deserialize via rkyv
        // 2. For each query: compare fingerprint
        // 3. If match: create accumulators, call merge_batch(state) for each group
        // 4. If mismatch: log warning, skip (fresh start for that query)
    }
}
```

### Algorithm/Flow

**Checkpoint (triggered by `CheckpointCoordinator`):**

1. `CheckpointCoordinator` calls `stream_executor.checkpoint_state()`
2. For each query with `IncrementalAggState`:
   a. Compute query fingerprint (hash of normalized SQL + schema)
   b. For each group key: call `accumulator.state()` → `Vec<ScalarValue>`
   c. Serialize `ScalarValue` vectors to bytes via rkyv
3. For each query with `IncrementalEowcState`:
   a. Same as above, but iterate over `BTreeMap<window_start, groups>`
4. Wrap in `StreamExecutorCheckpoint`, serialize to `Vec<u8>`
5. Return to coordinator, which stores under key `"stream_executor"` in the checkpoint manifest

**Restore (on startup):**

1. `CheckpointCoordinator::restore()` retrieves bytes for key `"stream_executor"`
2. If missing: no-op (fresh start, backward compatible)
3. If present: calls `stream_executor.restore_state(bytes)`
4. For each query in the checkpoint:
   a. Look up the query by name in current `StreamExecutor` state
   b. Compare `query_fingerprint` — if mismatch, skip with warning
   c. Create fresh accumulators via `AggFuncSpec::create_accumulator()`
   d. Call `accumulator.merge_batch()` with deserialized `ScalarValue` state
   e. Insert into the appropriate `IncrementalAggState` or `IncrementalEowcState`

### ScalarValue Serialization

DataFusion's `Accumulator::state()` returns `Vec<ScalarValue>`. Since `ScalarValue` is not directly rkyv-serializable, convert to an intermediate representation:

```rust
/// Serialize a ScalarValue to bytes for checkpoint.
fn scalar_to_bytes(sv: &ScalarValue) -> Vec<u8> {
    // Use DataFusion's built-in protobuf serialization
    // (datafusion_proto::bytes::scalar_value_to_proto)
    // or manual encoding for the common types (Int64, Float64, Utf8, Boolean, Null)
}

/// Deserialize bytes back to ScalarValue.
fn bytes_to_scalar(bytes: &[u8]) -> Result<ScalarValue, DbError> {
    // Inverse of scalar_to_bytes
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DbError::Checkpoint` | rkyv serialization fails | Log error, skip checkpoint for this executor |
| `DbError::Checkpoint` | rkyv deserialization fails (corrupt data) | Log warning, start fresh |
| `DbError::Internal` | Query fingerprint mismatch on restore | Log warning, skip that query (fresh start) |
| `DbError::Execution` | `merge_batch()` fails (incompatible state) | Log warning, discard state for that group |

## Test Plan

### Unit Tests

- [ ] `test_checkpoint_roundtrip_single_group_sum`
- [ ] `test_checkpoint_roundtrip_multi_group_avg`
- [ ] `test_checkpoint_roundtrip_eowc_tumbling_window`
- [ ] `test_checkpoint_roundtrip_eowc_multiple_windows`
- [ ] `test_restore_missing_key_is_noop`
- [ ] `test_restore_fingerprint_mismatch_skips_query`
- [ ] `test_restore_corrupt_bytes_returns_error`
- [ ] `test_checkpoint_empty_state_returns_none`
- [ ] `test_scalar_value_roundtrip_all_types`

### Integration Tests

- [ ] End-to-end: checkpoint → restart → restore → verify running totals continue
- [ ] End-to-end: checkpoint with EOWC → restart mid-window → window close produces correct result
- [ ] Schema evolution: change query between restarts → old state discarded, new state starts fresh

### Property Tests

- [ ] For any sequence of batches, `checkpoint_state()` + `restore_state()` produces the same accumulator state as processing all batches from scratch

## Rollout Plan

1. **Phase 1**: `ScalarValue` serialization helpers + unit tests
2. **Phase 2**: `StreamExecutor::checkpoint_state()` + `restore_state()` + unit tests
3. **Phase 3**: Wire into `CheckpointCoordinator` + integration tests
4. **Phase 4**: Backward-compatibility verification (no checkpoint key = fresh start)
5. **Phase 5**: Code review + merge

## Open Questions

- [ ] Should we use `datafusion_proto` for `ScalarValue` serialization, or a custom compact encoding?
- [ ] Should checkpoint frequency for StreamExecutor state be independent of operator checkpoint frequency?
- [ ] Should we expose a metric for checkpoint size (bytes) per query?

## Completion Checklist

- [ ] Code implemented
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

- The version field in `StreamExecutorCheckpoint` allows future format changes without breaking restore from older checkpoints.
- Query fingerprinting uses a hash of the normalized SQL text plus the output schema. This catches cases where the user changes `COUNT(*)` to `SUM(amount)` between restarts.
- The `merge_batch()` approach (instead of directly setting internal state) ensures compatibility across DataFusion version upgrades where accumulator internal state format might change.

## References

- [F-CKP-003: Checkpoint Coordinator](../F-CKP-003-checkpoint-coordinator.md)
- [F-SSQL-001: EOWC Incremental Accumulators](F-SSQL-001-eowc-incremental-accumulators.md)
- [aggregate_state.rs — IncrementalAggState](../../../../crates/laminar-db/src/aggregate_state.rs)
- [checkpoint_coordinator.rs](../../../../crates/laminar-db/src/checkpoint_coordinator.rs)
- [F-DCKP-005: Recovery Manager](../../delta/checkpoint/F-DCKP-005-recovery-manager.md)
