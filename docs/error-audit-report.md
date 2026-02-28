# LaminarDB Error Handling Audit Report

**Date**: 2026-02-28
**Version**: 0.16.0
**Scope**: All 8 crates in the workspace

---

## Executive Summary

LaminarDB has a **mixed-quality error handling story**. The SQL/DataFusion error translation layer is genuinely good — structured LDB-NNNN codes, typo suggestions, hint messages. However, the rest of the codebase has significant gaps: **~6,100+ `.unwrap()` calls** (many in non-test code), **~200+ `let _ =` silent drops** (including critical checkpoint paths), and several serialization paths that silently swallow corruption.

### Key Numbers

| Pattern | Total | Non-Test Estimate | Severity |
|---------|-------|-------------------|----------|
| `.unwrap()` | ~6,100 | ~1,500-2,000 | HIGH |
| `let _ =` on Results | ~200 | ~80-100 | CRITICAL (some) |
| `.ok()` on Results | ~140 | ~50 | MEDIUM |
| `.unwrap_or_default()` | ~60 | ~30 | HIGH (some) |
| `.expect()` with vague msg | ~150 | ~50 | MEDIUM |
| `map_err(\|_\| ...)` | ~60 | ~40 | MEDIUM |

### Critical Findings (P0)

1. **Checkpoint rollback failure silently dropped** — `checkpoint_coordinator.rs:628`
2. **Kafka group_id defaults to empty string on missing metadata** — `source_offsets.rs:219`
3. **WAL reader allocates `usize::MAX` bytes on negative length** — `wal.rs:441`
4. **Base64 decode error in operator checkpoint silently returns None** — `checkpoint_manifest.rs:359`
5. **Known wrong-result bugs in aggregation** — DISTINCT/FILTER/HAVING silently broken (not a double-aggregation issue)

---

## 1. Error Type Inventory

### 1.1 Error Types by Crate

#### laminar-core (17 error types)

| Error Type | File | Library | Variants |
|-----------|------|---------|----------|
| `Error` (top-level) | `lib.rs:74` | thiserror | 11 — aggregates all submodule errors |
| `StreamingError` | `streaming/error.rs:10` | Custom Display | 7: ChannelFull, ChannelClosed, Disconnected, InvalidConfig, SchemaMismatch, Timeout, Internal |
| `DagError` | `dag/error.rs:4` | thiserror | 13: CycleDetected, DisconnectedNode, NodeNotFound, CheckpointInProgress, etc. |
| `CompileError` | `compiler/error.rs:12` | Custom Display | 6: UnsupportedExpr, ColumnNotFound, Cranelift, etc. |
| `SinkError` | `sink/error.rs:6` | thiserror | 15: TransactionAlreadyActive, CommitFailed, WriteFailed, etc. |
| `IoUringError` | `io_uring/error.rs:6` | thiserror | 15: RingCreation, BufferPoolExhausted, SubmissionQueueFull, etc. |
| `NumaError` | `numa/error.rs:6` | thiserror | 7: AllocationFailed, BindFailed, SyscallFailed, etc. |
| `MvError` | `mv/error.rs:6` | thiserror | 10: DuplicateName, CycleDetected, ViewNotFound, etc. |
| `XdpError` | `xdp/error.rs:6` | thiserror | 12: NotAvailable, LoadFailed, PermissionDenied, etc. |
| `OperatorError` | `operator/mod.rs:161` | thiserror | 4: StateAccessFailed, SerializationFailed, ProcessingFailed, ConfigError |
| `ReactorError` | `reactor/mod.rs:557` | thiserror | 4: InitializationFailed, EventProcessingFailed, ShutdownFailed, QueueFull |
| `StateError` | `state/mod.rs:687` | thiserror | 5: Io, Serialization, Deserialization, Corruption, NotSupported |
| `TimeError` | `time/mod.rs:431` | thiserror | 3: InvalidTimestamp, TimerNotFound, WatermarkRegression |
| `TpcError` | `tpc/mod.rs:85` | thiserror | 10: SpawnFailed, QueueFull, Backpressure, etc. |
| `RouterError` | `tpc/router.rs:35` | thiserror | Zero-allocation (static strings only) |
| `TryPushError<T>` | `streaming/error.rs` | Custom | Channel push failure |
| `RecvError` | `streaming/error.rs` | Custom | Channel receive failure |

#### laminar-db (3 error types)

| Error Type | File | Library | Notes |
|-----------|------|---------|-------|
| `DbError` | `error.rs:4` | thiserror | 24 variants — main facade error |
| `ApiError` | `api/error.rs:64` | thiserror | 6 categories with numeric codes (100-901) |
| FFI error codes | `ffi/error.rs` | Manual | Thread-local storage, numeric codes |

#### laminar-sql (3 error types + translation layer)

| Error Type | File | Library | Notes |
|-----------|------|---------|-------|
| `Error` (top-level) | `lib.rs:61` | thiserror | 4: ParseError, PlanningError, DataFusionError, UnsupportedFeature |
| `ParseError` | `parser/mod.rs:799` | thiserror | 4: SqlParseError, StreamingError, WindowError, ValidationError |
| `PlanningError` | `planner/mod.rs:605` | thiserror | 5: UnsupportedSql, InvalidQuery, SourceNotFound, etc. |
| `TranslatedError` | `error/mod.rs:62` | Manual | Structured LDB-NNNN codes with hints |

#### laminar-storage (3 error types)

| Error Type | File | Library | Notes |
|-----------|------|---------|-------|
| `CheckpointStoreError` | `checkpoint_store.rs:49` | thiserror | 3: Io, Serde, NotFound |
| `IncrementalCheckpointError` | `incremental/error.rs:6` | thiserror | 12 variants, has `is_transient()`/`is_corruption()` |
| `PerCoreWalError` | `per_core_wal/error.rs:6` | thiserror | 12 variants |

#### laminar-connectors (3 error types)

| Error Type | File | Library | Notes |
|-----------|------|---------|-------|
| `ConnectorError` | `error.rs:10` | thiserror | 15 variants |
| `SerdeError` | `error.rs:79` | thiserror | 11 variants (rich context for Avro/JSON) |
| `SchemaError` | `schema/error.rs:14` | thiserror | 13 variants |

#### laminar-auth, laminar-admin, laminar-observe

No custom error types. Delegate to other crates.

#### laminar-server

Uses `anyhow::Result<T>` (binary pattern — appropriate).

### 1.2 Error Library Usage

- **All library crates**: `thiserror` (workspace dep `thiserror = "2.0"`)
- **Binary crate (server)**: `anyhow` (workspace dep `anyhow = "1.0"`)
- **No usage of**: `snafu`, `error-stack`, `eyre`
- **Assessment**: Consistent and appropriate. No change needed.

### 1.3 Error Code System (Existing)

LaminarDB already has a partial error code system in `laminar-sql/src/error/mod.rs`:

| Range | Category | Status |
|-------|----------|--------|
| `LDB-1001`..`LDB-1099` | SQL syntax | Implemented |
| `LDB-1100`..`LDB-1199` | Schema/column | Implemented |
| `LDB-1200`..`LDB-1299` | Type errors | Implemented |
| `LDB-2000`..`LDB-2099` | Window/watermark | Implemented |
| `LDB-3000`..`LDB-3099` | Join errors | Implemented |
| `LDB-9000`..`LDB-9099` | Internal | Implemented |
| `LDB-0xxx` | General/config | **Missing** |
| `LDB-4xxx` | Serialization/state | **Missing** |
| `LDB-5xxx` | Connector/I-O | **Missing** |
| `LDB-6xxx` | Checkpoint/recovery | **Missing** |
| `LDB-7xxx` | DataFusion interop | **Missing** (currently uses LDB-9000 fallback) |
| `LDB-8xxx` | Internal/should-not-happen | **Missing** |

---

## 2. Error Propagation Graph

```
                        ┌─────────────────────────────────┐
                        │         User / FFI Client        │
                        └───────────────┬─────────────────┘
                                        │
                                  ApiError (numeric codes)
                                        │
                        ┌───────────────┴─────────────────┐
                        │           DbError                │
                        │  (24 variants, custom Display)   │
                        └───┬──────┬──────┬────────┬──────┘
                            │      │      │        │
                   From     │      │      │        │ From
               ┌────────────┘      │      │        └───────────┐
               │                   │      │                    │
     laminar_sql::Error    Core::Error  StreamingError    DataFusionError
       │        │              │                          (translated on
    ParseError  PlanningError  ├── ReactorError            Display via
       │           │           ├── StateError              TranslatedError)
   ParserError  DataFusion     ├── OperatorError
                Error          ├── TpcError
                               ├── SinkError
                               ├── IoUringError
                               ├── NumaError
                               ├── MvError
                               ├── XdpError
                               └── DagError

     laminar_connectors            laminar_storage
     ┌──────────────┐              ┌──────────────────────┐
     │ConnectorError │              │CheckpointStoreError   │
     │  ↕ SchemaError│              │IncrementalCheckpoint  │
     │  SerdeError──┘│              │  Error                │
     └──────────────┘              │PerCoreWalError ───────┘
                                   └──────────────────────┘
```

### 2.1 Context Loss Points

| Location | Pattern | What's Lost |
|----------|---------|-------------|
| `DbError::Connector(String)` | String wrapping | Typed ConnectorError variants |
| `DbError::Pipeline(String)` | String wrapping | Typed error, source chain |
| `DbError::Checkpoint(String)` | String wrapping | CheckpointStoreError/IncrementalCheckpointError details |
| `DbError::Storage(String)` | String wrapping | Typed storage errors |
| `DbError::MaterializedView(String)` | String wrapping | MvError variants |
| `ApiError::From<DbError>` | Intentional flattening | Variant-specific info (designed for FFI) |
| `ConnectorError::Serde(#[from] SerdeError)` | OK | Preserved via #[from] |

**Key insight**: 6 of DbError's 24 variants are `String` wrappers that lose the original typed error. These should be `#[from]` conversions or carry the source error.

---

## 3. Silent Failure Analysis

### 3.1 CRITICAL: Checkpoint Path Silent Drops

**`checkpoint_coordinator.rs:628`** — Rollback failure silently dropped during error recovery:
```rust
let _ = self.rollback_sinks(epoch).await;
```
If `save_manifest()` fails AND `rollback_sinks()` also fails, the second failure is completely invisible. Sinks may be left in an inconsistent committed/uncommitted state.

**`checkpoint_store.rs:255`** — Prune failure silently ignored:
```rust
let _ = self.prune(self.max_retained);
```
Disk can fill up with old checkpoints.

### 3.2 CRITICAL: Wrong Defaults on Missing Metadata

**`source_offsets.rs:219,244`** — Kafka group_id/Postgres slot_name default to empty string:
```rust
let group_id = cp.metadata.get("group_id").cloned().unwrap_or_default();
```
Empty group_id causes Kafka offset recovery to use the wrong consumer group — silent data loss.

### 3.3 CRITICAL: Memory Safety in WAL Reader

**`wal.rs:441`** — Negative length causes `usize::MAX` allocation:
```rust
let mut data = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
```
A corrupted WAL entry with a negative length field will attempt to allocate ~18 exabytes, causing OOM.

### 3.4 HIGH: Sink Task Silent Drops

**`sink_task.rs:237-252`** — Multiple sink operations have errors dropped:
```rust
SinkCommand::Flush { ack } => {
    let result = sink.flush().await;
    let _ = ack.send(result);  // If receiver dropped, flush error is lost
}
SinkCommand::RollbackEpoch { epoch } => {
    let _ = sink.rollback_epoch(epoch).await;  // Rollback failure invisible
}
SinkCommand::Close => {
    let _ = sink.flush().await;   // Close-time flush failure invisible
    let _ = sink.close().await;   // Close failure invisible
}
```

**Assessment**: The `ack.send()` drops are acceptable (oneshot channel — receiver may have timed out). But `rollback_epoch`, `flush`, and `close` failures during shutdown should at minimum be logged.

### 3.5 HIGH: Base64 Decode Error Swallowed

**`checkpoint_manifest.rs:355-360`** — `decode_inline()` silently drops base64 errors:
```rust
pub fn decode_inline(&self) -> Option<Vec<u8>> {
    self.state_b64
        .as_ref()
        .and_then(|b64| base64::engine::general_purpose::STANDARD.decode(b64).ok())
}
```
If operator checkpoint state has corrupted base64, this returns `None` and the operator starts from scratch — no warning, no error.

### 3.6 MEDIUM: Stream Executor Table Deregistration

**`stream_executor.rs`** — 8 instances of `let _ = self.ctx.deregister_table(...)`:
```rust
let _ = self.ctx.deregister_table("_pre_agg");
let _ = self.ctx.deregister_table("_input");
```
DataFusion context can leak table registrations. Not data-affecting but can cause memory growth.

### 3.7 MEDIUM: Schema Registry Response Body

**`kafka/schema_registry.rs:372,429,488,582`** — HTTP response body defaults to empty string:
```rust
let text = resp.text().await.unwrap_or_default();
```
If the HTTP response body read fails, schema parsing gets an empty string and produces a confusing error downstream instead of reporting the actual HTTP failure.

---

## 4. DataFusion Error Boundary Assessment

### 4.1 Translation Layer — STRONG

The existing `laminar-sql/src/error/mod.rs` is well-designed:
- Pattern-matches ~15 known DataFusion error formats
- Strips internal prefixes (`DataFusion error:`, `Arrow error:`, etc.)
- Provides structured codes (LDB-1001 through LDB-9001)
- Includes actionable hints (e.g., "Did you mean 'price'?" for typos)
- 30+ unit tests covering translation paths
- Edit-distance suggestions via `suggest` submodule

### 4.2 Error Bridge — GOOD with Gaps

All critical SQL execution paths use `DbError::query_pipeline()` which triggers translation:
- `stream_executor.rs:321,325,525,617,639,709,764,821` — all caught and wrapped
- `db.rs` — SQL execution methods use `?` which auto-converts via `From<DataFusionError>`

**Gap**: The `DbError::DataFusion(#[from])` variant allows DataFusion errors to bypass translation when using `?` operator instead of explicit `query_pipeline()`. The `Display` impl catches this and translates on formatting, but the typed error information is the raw DataFusion type.

### 4.3 Missing Error Code Ranges

The translation layer only covers SQL-related DataFusion errors. Other error categories lack structured codes:
- Connector errors (Kafka, CDC, WebSocket) — no LDB codes
- Checkpoint/recovery errors — no LDB codes
- Serialization errors — no LDB codes
- State store errors — no LDB codes

---

## 5. Double Aggregation Investigation

### 5.1 Verdict: No Double Aggregation Detected

The architecture prevents double aggregation through a clean separation:

1. **Query routing**: `stream_executor.rs:558` — `detect_aggregate_query()` routes aggregate queries to `IncrementalAggState`, NOT through DataFusion's aggregate executor.

2. **Pre-aggregation SQL**: `aggregate_state.rs:75-148` — Builds a **projection-only** SQL query (no `GROUP BY`, no aggregate functions). DataFusion executes only the projection; all aggregation happens in LaminarDB's user-space accumulators.

3. **Plan validation**: `streaming_optimizer.rs:122-138` — `StreamingPhysicalValidator` rejects any plan with `AggregateExec(Final)` on unbounded input.

4. **Accumulator ownership**: `aggregate_state.rs:103` — LaminarDB maintains its own `HashMap<GroupKey, Vec<Accumulator>>` using DataFusion's `Accumulator` trait but managing the state lifecycle itself.

### 5.2 Actual Aggregation Bugs (Not Double-Aggregation)

The audit found known wrong-result bugs that are often confused with "double aggregation":

| ID | Bug | Impact | Root Cause |
|----|-----|--------|------------|
| H-01 | `COUNT(DISTINCT x)` counts duplicates | Wrong results | `distinct` flag hardcoded to `false` in `aggregate_bridge.rs` |
| H-02 | `FILTER` clause ignored | Wrong results | `filter_expr` not extracted from aggregate function |
| H-03 | `HAVING` clause silently dropped | Wrong results | Post-emission filter not applied |
| H-04 | Same as H-01 | Wrong results | Duplicate of H-01 in a different path |
| H-05 | `.expect()` in accumulator creation | Panic | Unsupported type hits expect |
| H-06 | `.expect()` in `compute::take()` | Panic | Invalid indices hit expect |
| H-08 | `clone_box()` panic in adapter | Crash | Accumulator cloning unsupported |
| H-09 | Silent error in `add_event` | Wrong results | Failed updates silently ignored |

**These are the likely root cause of issues attributed to "double aggregation."** The `DISTINCT` bug (H-01/H-04) is particularly deceptive — it makes `COUNT(DISTINCT col)` return the same value as `COUNT(col)`, which looks like the count was applied twice.

### 5.3 `aggregate_state.rs:604` — Unwrap After Insert

```rust
let accs = self.groups.get_mut(&empty_key).unwrap();
```

This is technically safe (insert just happened 2 lines above), but the pattern is fragile. If future refactoring moves the insert, this panics in the hot path.

---

## 6. Serialization Error Handling

### 6.1 Checkpoint Manifest (JSON) — GOOD

- `checkpoint_store.rs:233` — `serde_json::to_string_pretty(manifest)?` propagates correctly
- `checkpoint_store.rs:287` — `serde_json::from_str::<CheckpointManifest>(&json)?` propagates correctly
- Both wrapped in `CheckpointStoreError::Serde(#[from] serde_json::Error)`

### 6.2 WAL Serialization (rkyv) — GOOD

- `wal.rs:214` — `high::to_bytes_in::<_, RkyvError>(entry, taken).map_err(|e| WalError::Serialization(e.to_string()))?`
- Error message preserved, context via WalError variant

### 6.3 Operator State Recovery — PROBLEMATIC

**`recovery_manager.rs:245-270`** — Sidecar recovery silently skips corrupted operators:
```rust
if end <= state_data.len() {
    let data = &state_data[start..end];
    *op = OperatorCheckpoint::inline(data);
} else {
    warn!(...);  // But recovery continues with incomplete state
}
```
Recovery proceeds with missing operator state — the operator starts from scratch without any indication to the user that data was lost.

### 6.4 CDC JSON Parsing — DANGEROUS

**`cdc/postgres/source.rs:1020`** — Production code uses `.unwrap()` on JSON parse:
```rust
let after_json: serde_json::Value = serde_json::from_str(after_col.value(0)).unwrap();
```
A malformed CDC event crashes the entire source pipeline.

**`cdc/mysql/changelog.rs:329`** — Silent fallback:
```rust
serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!(s))
```
No logging when JSON parsing fails — data corruption is invisible.

### 6.5 rkyv Validation Gap

**`checkpoint/mod.rs:295-300`** — `rkyv::access()` performs zero-copy access without explicit validation:
```rust
let archived = rkyv::access::<rkyv::Archived<CheckpointMetadataInternal>, Error>(&aligned_bytes)?;
```
While `rkyv::deserialize()` (called next) does validate, a corrupted buffer could cause undefined behavior in the `access()` step if using an unchecked path. The codebase imports `bytecheck::CheckBytes` but should verify it's actually being used in the access step.

### 6.6 Missing: `serde_path_to_error`

No crate in the workspace uses `serde_path_to_error`. All JSON parse errors report the serde error message without indicating which field or path caused the failure. For complex structures like `CheckpointManifest` or connector configs, this makes debugging difficult.

---

## 7. Error Logging Patterns

### 7.1 Structured Logging — MIXED

**Good examples** (checkpoint coordinator):
```rust
error!(checkpoint_id, epoch, error = %e, "manifest persist failed");
```

**Bad examples** (scattered across codebase):
```rust
eprintln!("io_uring WAL not available: {e}");  // io_uring_wal.rs:346
```

### 7.2 Error Metrics — ABSENT

No error counters exist. There is no Prometheus/metrics integration for error tracking. Errors are only visible in logs — no dashboards, no alerting.

---

## 8. Recommendations (Prioritized)

### P0 — Fix Now (Data Loss / Corruption Risk)

1. **Log checkpoint rollback failures** — `checkpoint_coordinator.rs:628`
   - Replace `let _ =` with `if let Err(e) = ... { error!(...) }`

2. **Return error on missing Kafka group_id** — `source_offsets.rs:219`
   - Don't default to empty string; return error or log critical warning

3. **Validate WAL entry length before allocation** — `wal.rs:441`
   - Check `len` fits in reasonable bounds before `vec![0u8; len]`

4. **Return Result from decode_inline()** — `checkpoint_manifest.rs:355`
   - Change `.ok()` to proper error propagation

5. **Fix CDC JSON unwrap** — `cdc/postgres/source.rs:1020`
   - Replace `.unwrap()` with proper error handling via ConnectorError

### P1 — Fix Soon (Wrong Results / Poor Debuggability)

6. **Fix DISTINCT aggregation bug** — `aggregate_bridge.rs`
   - Read `agg_func.params.distinct` and pass to accumulator

7. **Fix FILTER clause handling** — `aggregate_state.rs`
   - Extract `filter_expr` and apply via CASE WHEN

8. **Fix HAVING clause** — `stream_executor.rs`
   - Apply HAVING as post-emission filter

9. **Add logging to sink_task silent drops** — `sink_task.rs:248-252`
   - Log rollback/flush/close failures at warn level

10. **Add `serde_path_to_error`** to JSON deserialization of complex types

### P2 — Improve (Error System Maturity)

11. **Extend error code registry** — Add LDB-4xxx through LDB-8xxx ranges
12. **Replace String-wrapping DbError variants** — Use typed sources
13. **Add error metrics** — Prometheus counters per error code
14. **Add Clippy lints** — `unwrap_used = "deny"` (after migration)
15. **Ban `eprintln!`** — Replace with `tracing::warn!`

### P3 — Polish (World-Class)

16. **Ring 0 zero-alloc error type** — For hot path error reporting
17. **Error snapshot debug mode** — Dump pipeline state on error
18. **CI error pattern checker** — Grep for banned patterns
19. **rkyv validation hardening** — Explicit `check_archived_root()` calls

---

## Appendix A: Error Propagation Per Crate

### laminar-core → laminar-db

All 11 submodule errors flow through `laminar_core::Error` via `#[from]`, then to `DbError::Engine(#[from] laminar_core::Error)`. Context preserved at each level via thiserror.

### laminar-sql → laminar-db

- `ParseError` → `laminar_sql::Error::ParseError` → `DbError::SqlParse` (via #[from])
- `PlanningError` → `laminar_sql::Error::PlanningError` → `DbError::Sql` (via #[from])
- `DataFusionError` → `laminar_sql::Error::DataFusionError` → `DbError::Sql` (via #[from])
- Additionally: `DataFusionError` → `DbError::DataFusion` (direct #[from]) — translated on Display

### laminar-connectors → laminar-db

**Gap**: No `#[from]` conversion exists. Connector errors are manually wrapped as `DbError::Connector(format!("{e}"))` — typed information lost.

### laminar-storage → laminar-db

**Gap**: No `#[from]` conversion exists. Storage errors are manually wrapped as `DbError::Storage(format!("{e}"))` or `DbError::Checkpoint(format!("{e}"))` — typed information lost.

## Appendix B: Files with Most Silent Failures (Non-Test)

| File | `.unwrap()` | `let _ =` | `.ok()` | Total |
|------|-------------|-----------|---------|-------|
| `aggregate_state.rs` | ~30 (non-test) | 3 | 0 | ~33 |
| `stream_executor.rs` | ~15 | 12 | 0 | ~27 |
| `db.rs` | ~20 | 3 | 5 | ~28 |
| `sink_task.rs` | 0 | 6 | 0 | 6 |
| `checkpoint_coordinator.rs` | 2 | 1 | 0 | 3 |
| `websocket/source.rs` | ~5 | 7 | 0 | ~12 |
| `kafka/schema_registry.rs` | ~5 | 1 | 0 | ~6 |
| `wal.rs` | ~10 | 0 | 0 | ~10 |
| `source_offsets.rs` | ~5 | 0 | 4 | ~9 |

## Appendix C: Existing Error Helper Methods

Several error types already have useful discrimination methods:

- `IoUringError::is_fatal()` / `is_transient()` — for retry decisions
- `IncrementalCheckpointError::is_transient()` / `is_corruption()` — for recovery decisions
- `XdpError::is_permission_error()` / `is_not_available()` — for graceful degradation
- `DbError::query_pipeline()` / `query_pipeline_with_columns()` / `query_pipeline_arrow()` — for DataFusion error translation

These should be preserved and expanded to other error types.
