# LaminarDB Audit Fix Plan

**Created:** 2026-03-07
**Source:** Unified audit report from 5 agents (Performance, Checkpoint, TPC, State, Lookup)
**Total findings:** 60 (1 CRITICAL, 13 HIGH, 24 MEDIUM, 22 LOW)

---

## Principles

1. **Every phase ends with an automated review agent + human sign-off** — no merges without both
2. **No backward compatibility** — breaking changes are fine; delete old code paths, bump format versions, change trait signatures freely
3. **Phases ordered by risk x impact** — critical/correctness first, performance second, cleanup last
4. **Each phase is a single PR** — small, reviewable, independently testable
5. **No AI slop** — review agent specifically checks for dead code, duplicated logic, unused abstractions, and over-engineering

---

## Review Agent Protocol

After each phase's code changes are complete, run the **Review Agent** with the following mandate. The review agent MUST:

### 1. AI Slop Detection
- Grep for functions/methods/structs added in this phase that are never called outside tests
- Flag any "just in case" error variants, config fields, or trait methods that have zero production callers
- Flag any doc comments that restate what the code obviously does
- Flag helper functions that are called exactly once — inline them
- Flag any `todo!()`, `unimplemented!()`, `FIXME`, or placeholder code
- Flag any new abstractions (traits, enums, wrapper types) that exist for a single implementation

### 2. Duplicate Code Detection
- Diff the changed files against the rest of the codebase — flag any logic that duplicates existing utilities
- Check for copy-pasted blocks within the same file
- Flag any new utility functions that duplicate what `arrow::compute`, `bytes`, or existing crate helpers already provide

### 3. Dead Code in Production
- Run `cargo build --release 2>&1 | grep "warning.*dead_code"` and verify zero new warnings
- Grep for any `#[allow(dead_code)]` added in this phase — each must be justified
- Verify every new `pub` item has at least one non-test caller
- Check that removed/replaced code paths are fully deleted, not commented out or gated behind `#[cfg(never)]`

### 4. Performance Validation
- For hot-path changes: verify no new allocations via `HotPathGuard` or manual inspection
- Check that no `clone()`, `to_vec()`, `to_string()`, `format!()` were added on Ring 0 paths
- Verify no new `Mutex`, `RwLock`, or blocking I/O on hot paths
- Run relevant benchmarks and compare against baseline

### 5. Test Quality
- Flag tests that only test the happy path without edge cases
- Flag tests with hardcoded magic numbers without explanation
- Verify tests would actually fail if the fix were reverted

The review agent outputs a **PASS/FAIL** verdict with specific line-number citations for every flag. Human reviewer then decides whether to accept, fix, or defer each flag.

---

## Phase 1: Critical Exactly-Once Fix

**Branch:** `fix/audit-phase1-checkpoint-putmode`
**Scope:** ~50 lines changed, 2 files
**Risk:** HIGH (touches checkpoint persistence)

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| C1 | ObjectStore manifest re-save fails silently | `checkpoint_store.rs:822` | Add `update_manifest()` method with overwrite semantics |

### Implementation

1. **`CheckpointStore` trait** — add method:
   ```rust
   fn update_manifest(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointError>;
   ```

2. **`ObjectStoreCheckpointStore`** — implement with `PutOptions::default()` (no `PutMode::Create`):
   ```rust
   fn update_manifest(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointError> {
       let path = self.manifest_path(manifest.checkpoint_id);
       let payload = PutPayload::from(serde_json::to_vec(manifest)?);
       self.runtime.block_on(self.store.put_opts(&path, payload, PutOptions::default()))?;
       Ok(())
   }
   ```

3. **`FileCheckpointStore`** — delegate to `save()` (write-temp-rename is already idempotent).

4. **`InMemoryCheckpointStore`** — delegate to `save()` (HashMap insert overwrites).

5. **`checkpoint_coordinator.rs:1133`** — Step 6b calls `update_manifest()`:
   ```rust
   self.checkpoint_store.update_manifest(&updated_manifest)?;
   ```

### Tests

- `test_object_store_manifest_resave` — save, then update with new sink statuses, verify overwrite succeeds
- `test_recovery_sees_committed_sinks` — full cycle: checkpoint -> commit -> update -> recover -> sinks are `Committed`

### Review Agent Gate 1

Run review agent. Additionally verify:
- [ ] `PutMode::Create` preserved for initial save (split-brain protection)
- [ ] `update_manifest` has exactly 3 implementations, none is dead code
- [ ] No new error variants added that are never constructed
- [ ] `save()` and `update_manifest()` don't share duplicated serialization logic — extract if so

---

## Phase 2: Lookup Join Correctness

**Branch:** `fix/audit-phase2-lookup-correctness`
**Scope:** ~150 lines changed, 4 files
**Risk:** HIGH (SQL semantics)

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H1 | NULL join keys match incorrectly | `lookup_join_exec.rs` | Add null-key filter before probe |
| H2 | CDC deletes not handled in partial cache | `pipeline_callback.rs` | Inspect op column, invalidate on delete |
| H3 | Timestamp predicates drop rows | `source.rs` | Add Timestamp arm to `compare_column_scalar` |

### Implementation

#### H1: NULL key filter

In both `probe_batch()` and `probe_partial_batch_with_fallback()`, before `RowConverter` encoding:

```rust
let null_mask = stream_key_arrays.iter().fold(
    None,
    |mask: Option<BooleanArray>, col| {
        let col_nulls = arrow::compute::is_null(col).unwrap();
        match mask {
            None => Some(col_nulls),
            Some(m) => Some(arrow::compute::or(&m, &col_nulls).unwrap()),
        }
    },
);
```

- **Inner join:** skip null-key rows entirely (don't probe, don't emit)
- **Left outer:** emit null-key rows with NULL lookup columns (no probe)

No special-casing for "what if both sides are NULL" — NULL keys never reach the probe.

#### H2: CDC delete handling

In `update_partial_cache_from_batch()`:

```rust
let op_col = batch.schema().fields().iter()
    .position(|f| matches!(f.name().as_str(), "__op" | "__operation" | "op"));

for row in 0..batch.num_rows() {
    let key = rows.row(row);
    let is_delete = op_col.map_or(false, |idx| {
        batch.column(idx).as_any()
            .downcast_ref::<StringArray>()
            .map_or(false, |a| matches!(a.value(row), "d" | "D" | "delete" | "DELETE"))
    });

    if is_delete {
        partial.foyer_cache.remove(key.as_ref());
    } else {
        partial.foyer_cache.insert(key.as_ref().to_vec(), batch.slice(row, 1));
    }
}
```

#### H3: Timestamp predicate

Add to `compare_column_scalar`:

```rust
ScalarValue::Timestamp(us) => {
    if let Some(arr) = column.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let scalar = TimestampMicrosecondArray::new_scalar(*us);
        Some(apply_cmp(op, arr, &scalar))
    } else if let Some(arr) = column.as_any().downcast_ref::<TimestampMillisecondArray>() {
        let scalar = TimestampMillisecondArray::new_scalar(*us / 1000);
        Some(apply_cmp(op, arr, &scalar))
    } else {
        None
    }
}
```

No fallback to casting — if the column type doesn't match, return `None` (predicate not applied locally, stays pushed to source or skipped). Don't build a "universal cast chain."

### Tests

- `test_null_key_inner_join` — NULL stream key + NULL lookup key -> 0 rows
- `test_null_key_left_outer` — NULL stream key -> row with NULL lookup columns
- `test_null_key_mixed` — batch with NULL and valid keys -> only valid keys matched
- `test_cdc_delete_invalidates_cache` — insert then CDC delete -> cache miss
- `test_cdc_no_op_column` — no `__op` column -> all rows treated as upsert
- `test_timestamp_predicate_eval` — Timestamp predicate filters correctly
- `test_timestamp_predicate_microsecond_and_millisecond` — both variants handled

### Review Agent Gate 2

Run review agent. Additionally verify:
- [ ] `null_mask` computation is not duplicated between `probe_batch` and `probe_partial_batch_with_fallback` — extract shared function if identical
- [ ] No unused `ScalarValue` arms added "for completeness"
- [ ] CDC op column detection doesn't create a new constant/enum that's used once — keep inline
- [ ] Timestamp handling has exactly 2 arms (microsecond, millisecond), not a sprawling match

---

## Phase 3: Recovery & Durability Hardening

**Branch:** `fix/audit-phase3-recovery-durability`
**Scope:** ~140 lines changed, 5 files
**Risk:** MEDIUM-HIGH

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H7 | Default lenient recovery | `recovery_manager.rs` | Default to strict; remove lenient as default |
| H8 | StreamCheckpoint no integrity check | `checkpoint.rs` | Replace v1 format with CRC32C format |
| H10 | Per-core WAL ignores positions | `per_core_wal/recovery.rs` | Accept positions parameter; wire from manifest |
| M3 | WAL truncated to 0 | `checkpoint_coordinator.rs` | Keep previous checkpoint's WAL data |
| M4 | epoch=0 manifests valid | `checkpoint_store.rs` | Reject epoch=0/checkpoint_id=0 as invalid |

### Implementation

#### H7: Strict recovery default

```rust
pub fn new(store: Arc<dyn CheckpointStore>) -> Self {
    Self { store, strict: true }  // was: false
}
```

Remove `new_lenient()` — if someone needs lenient, they construct with `strict: false` directly. Don't add a builder or a separate constructor for a single bool.

At call sites, add guard:
```rust
let recovered = recovery_manager.recover(&manifest)?;
if recovered.has_errors() {
    return Err(DbError::RecoveryIncomplete(recovered.error_summary()));
}
```

#### H8: StreamCheckpoint CRC32C (no backward compat)

Replace the format entirely. Bump version to 2. Remove v1 deserialization code — no backward compatibility needed.

`to_bytes()`:
```rust
buf.push(2u8);
// ... existing serialization of fields ...
let crc = crc32c::crc32c(&buf[1..]);
buf.extend_from_slice(&crc.to_le_bytes());
```

`from_bytes()`:
```rust
if data[0] != 2 {
    return Err(CheckpointError::UnsupportedVersion(data[0]));
}
let payload = &data[1..data.len() - 4];
let stored = u32::from_le_bytes(data[data.len()-4..].try_into()?);
let computed = crc32c::crc32c(payload);
if stored != computed {
    return Err(CheckpointError::Corrupted { stored, computed });
}
// ... deserialize payload ...
```

Delete any v1 parsing code.

#### H10: Per-core WAL recovery from positions

Change signature:
```rust
// Before:
pub fn recover(&self) -> Result<RecoveredState>

// After:
pub fn recover(&self, starting_positions: &[u64]) -> Result<RecoveredState>
```

Remove the hardcoded `vec![0u64; num_cores]`. Callers pass `manifest.per_core_wal_positions`. If length mismatches, log warning and use `vec![0; num_cores]`.

#### M3: WAL truncation keeps safety buffer

```rust
fn truncate_wal_after_checkpoint(&mut self) {
    let truncate_to = self.previous_wal_positions.take().unwrap_or_default();
    for (core_id, pos) in truncate_to.iter().enumerate() {
        self.wal.truncate_to(core_id, *pos);
    }
    self.previous_wal_positions = Some(self.current_wal_positions.clone());
}
```

#### M4: Reject epoch=0 manifests

```rust
if manifest.epoch == 0 || manifest.checkpoint_id == 0 {
    return ValidationResult { valid: false, issues };
}
```

### Tests

- `test_strict_recovery_aborts_on_failure` — source fails restore -> error returned
- `test_checkpoint_crc_roundtrip` — serialize -> deserialize -> equal
- `test_checkpoint_crc_corruption` — flip byte -> `Corrupted` error
- `test_checkpoint_v1_rejected` — v1 data -> `UnsupportedVersion`
- `test_per_core_wal_recovery_from_positions` — positions from manifest used, not 0
- `test_wal_truncation_keeps_buffer` — previous checkpoint's WAL data preserved
- `test_epoch_zero_manifest_invalid` — epoch=0 -> validation fails

### Review Agent Gate 3

Run review agent. Additionally verify:
- [ ] v1 parsing code is fully deleted — no `from_bytes_v1` dead code remaining
- [ ] `CheckpointError::Corrupted` variant is constructed in exactly one place (the CRC check)
- [ ] `previous_wal_positions` field doesn't duplicate state already tracked elsewhere
- [ ] `strict: true` default doesn't break any existing integration tests — if tests assumed lenient, they had a bug
- [ ] Run `cargo bench --bench latency` — CRC32C add <100ns per checkpoint

---

## Phase 4: State Store Correctness

**Branch:** `fix/audit-phase4-state-store`
**Scope:** ~100 lines changed, 4 files
**Risk:** MEDIUM

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H13 | ChangelogAwareStore missing `get_ref` | `changelog_aware.rs` | Forward to inner |
| M17 | SPSC buffer `Sync` unsoundness | `changelog.rs` | Remove `Sync` impl |
| M18 | `Storage::get()` panics on bad index | `mmap.rs` | Return `Option`, bounds-check |
| M15 | Mmap unbounded fragmentation | `mmap.rs` | Auto-compact at threshold |

### Implementation

#### H13: One-line fix

```rust
fn get_ref(&self, key: &[u8]) -> Option<&[u8]> {
    self.inner.get_ref(key)
}
```

#### M17: Remove `Sync`

Delete the `unsafe impl Sync for StateChangelogBuffer` block. Keep `Send`. Update callers that share across threads to use `Arc<parking_lot::Mutex<StateChangelogBuffer>>` for the drain (consumer) side — this is the cold checkpoint path, not Ring 0.

If any caller holds `&StateChangelogBuffer` across threads for `push()`, it must be restructured so only one thread calls `push()` via owned or `&mut` access.

#### M18: Bounds-checked storage access

```rust
fn get(&self, offset: usize, len: usize) -> Option<&[u8]> {
    let end = offset.checked_add(len)?;
    match self {
        Storage::Arena { data, .. } => data.get(offset..end),
        Storage::Mmap { mmap, .. } => {
            let start = MMAP_HEADER_SIZE.checked_add(offset)?;
            mmap.get(start..start.checked_add(len)?)
        }
    }
}
```

Change all callers from `.get(o, l)` to `.get(o, l).ok_or(StateError::CorruptedIndex)?`.

#### M15: Auto-compact

In `put()`, after successful insertion:
```rust
if self.fragmentation() > 0.5 && self.len() > 100 {
    self.compact()?;
}
```

No config field — hardcode 50% threshold. Don't over-engineer with a `CompactionPolicy` trait.

### Tests

- `test_changelog_aware_get_ref` — wraps AHashMapStore, `get_ref` returns data
- `test_storage_get_bounds_check` — out-of-range offset returns `None`
- `test_storage_get_overflow` — `offset + len` overflows usize -> `None`
- `test_mmap_auto_compact` — 200 updates to same key -> compact fires, fragmentation < 50%
- `test_mmap_compact_preserves_data` — all live keys survive compaction

### Review Agent Gate 4

Run review agent. Additionally verify:
- [ ] `get_ref` forwarding doesn't record to changelog (reads should NOT be tracked — confirm)
- [ ] Removing `Sync` — grep for `Arc<StateChangelogBuffer>` shared across threads, verify all callers adapted
- [ ] `Storage::get` returning `Option` — no caller silently swallows `None` (all propagate as error)
- [ ] Auto-compact threshold is just a constant, not a config struct with a builder
- [ ] No new `#[allow(dead_code)]` added

---

## Phase 5: Stream Join Hot-Path Performance

**Branch:** `fix/audit-phase5-join-perf`
**Scope:** ~150 lines changed, 2 files
**Risk:** MEDIUM

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H4 | `probe_opposite_side` heap allocs per event | `stream_join.rs` | SmallVec + fixed-size key |
| H5 | `process_matches` 3N serde roundtrips | `stream_join.rs` | Separate matched flag storage |
| H6 | `AHashMapStore::get()` copies | `ahash_store.rs` | Store `Bytes` internally |
| M5 | `key_value.to_vec()` per event | `stream_join.rs` | Accept `&[u8]` in `JoinRow` |

### Implementation

#### H4: SmallVec probe matches

```rust
fn probe_opposite_side(...) -> SmallVec<[([u8; 28], JoinRow); 4]> {
    let mut matches = SmallVec::new();
    // ...
    let mut key_buf = [0u8; 28];
    key_buf.copy_from_slice(state_key);
    matches.push((key_buf, row));
}
```

Zero heap allocations for 0-4 matches (the common case).

#### H5: Separate matched flag

```rust
const MATCHED_PREFIX: u8 = 0x03;

fn matched_key(state_key: &[u8; 28]) -> [u8; 29] {
    let mut k = [0u8; 29];
    k[0] = MATCHED_PREFIX;
    k[1..].copy_from_slice(state_key);
    k
}

fn mark_matched(state: &mut dyn StateStore, key: &[u8; 28]) {
    state.put(&matched_key(key), &[1]).ok();
}

fn is_matched(state: &dyn StateStore, key: &[u8; 28]) -> bool {
    state.get_ref(&matched_key(key)).is_some()
}
```

Remove `matched` field from `JoinRow`. Replace all `get_typed -> modify matched -> put_typed` sequences with `mark_matched()`.

#### H6: AHashMapStore stores Bytes

```rust
struct AHashMapStore {
    data: AHashMap<Vec<u8>, Bytes>,  // was: AHashMap<Vec<u8>, Vec<u8>>
    // ...
}

fn get(&self, key: &[u8]) -> Option<Bytes> {
    self.data.get(key).cloned()  // Arc bump ~2ns
}

fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError> {
    self.data.insert(key.to_vec(), Bytes::copy_from_slice(value));
    // ...
}
```

#### M5: JoinRow takes &[u8]

Change `JoinRow::with_encoding(ts, key_value: &[u8], data, encoding)` — copy directly into rkyv buffer, no intermediate `Vec<u8>`.

### Tests

- `test_probe_zero_alloc` — use `HotPathGuard`, 1-match probe -> zero heap allocs
- `test_matched_flag_mark_and_check` — mark -> is_matched returns true
- `test_matched_flag_unset` — unmatched -> is_matched returns false
- `test_anti_join_with_separated_flag` — anti join semantics preserved
- `test_semi_join_with_separated_flag` — semi join semantics preserved
- `test_ahash_get_no_alloc` — get returns Bytes clone (not copy)
- All existing `stream_join` tests — regression

### Review Agent Gate 5

Run review agent. Additionally verify:
- [ ] `MATCHED_PREFIX` (0x03) confirmed not conflicting with LEFT (0x01) / RIGHT (0x02) prefixes
- [ ] `matched` field fully removed from `JoinRow` struct — no vestigial field
- [ ] `mark_matched`/`is_matched` are the only callers of the matched key pattern — no duplication
- [ ] `AHashMapStore::put` allocation moved to write side — confirm write path is not Ring 0 hot
- [ ] Run `cargo bench --bench latency` and `cargo bench --bench state_bench` — quantify improvement

---

## Phase 6: TPC Architecture Fixes

**Branch:** `fix/audit-phase6-tpc`
**Scope:** ~100 lines changed, 4 files
**Risk:** MEDIUM

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H11 | SPSC multi-producer risk | `tpc_runtime.rs` | Assert in `attach_source()` |
| H12 | CPU pin panics at cpu_id >= 64 | `core_handle.rs` | Validate before shift |
| M12 | No cpu bounds validation | `tpc_runtime.rs` | Check in `TpcConfig::validate()` |
| M14 | Core threads not joined on shutdown | `tpc_runtime.rs` | Join after signal |
| M13 | Core checkpoint state discarded | `tpc_coordinator.rs` | Persist operator states |

### Implementation

#### H11: SPSC guard

Add `core_has_source: Vec<bool>` to `TpcRuntime`. In `attach_source()`:
```rust
let core_id = self.next_core % self.cores.len();
assert!(!self.core_has_source[core_id], "SPSC violation: core {core_id} already has a source");
self.core_has_source[core_id] = true;
```

Use `assert!` not `Result` — this is a programming error, not a runtime condition.

#### H12: CPU pin bounds

```rust
fn pin_to_cpu(cpu_id: usize) {
    assert!(cpu_id < 64, "cpu_id {cpu_id} >= 64: Windows processor groups not yet supported");
    // ... existing SetThreadAffinityMask code
}
```

#### M12: Config validation

```rust
if self.cpu_pinning {
    let available = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    assert!(
        self.cpu_start + self.num_cores <= available,
        "cpu_start({}) + num_cores({}) exceeds available CPUs ({available})",
        self.cpu_start, self.num_cores
    );
}
```

#### M14: Join on shutdown

```rust
// After signaling all cores to stop:
for core in &mut self.cores {
    core.join();
}
```

#### M13: Persist core operator states

```rust
Output::CheckpointComplete(data) => {
    for (op_id, state_bytes) in &data.operator_states {
        checkpoint_state.insert(
            format!("core_{}_op_{}", data.core_id, op_id),
            state_bytes.clone(),
        );
    }
}
```

### Tests

- `test_attach_source_spsc_guard` — attach 2 sources with 1 core -> panic
- `test_cpu_pin_rejects_high_id` — cpu_id=128 -> panic
- `test_config_validate_cpu_bounds` — cpu_start=60, cores=8, available=64 -> panic
- `test_shutdown_joins_all` — after shutdown, thread handles consumed
- `test_checkpoint_preserves_core_state` — operator states appear in checkpoint

### Review Agent Gate 6

Run review agent. Additionally verify:
- [ ] `core_has_source` is a simple `Vec<bool>`, not an over-engineered tracker
- [ ] Assert messages are descriptive enough for debugging
- [ ] Core state key format `core_{id}_op_{id}` doesn't collide with any existing checkpoint keys
- [ ] `join()` on shutdown has no timeout — confirm threads are guaranteed to exit (shutdown signal + unpark)
- [ ] No new `pub` methods added that are only called from tests

---

## Phase 7: Checkpoint Edge Cases

**Branch:** `fix/audit-phase7-checkpoint-edges`
**Scope:** ~80 lines changed, 3 files
**Risk:** LOW-MEDIUM

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| H9 | WAL durability gap | `wal.rs` | Add `sync_on_commit` option; document guarantee |
| M1 | Object store sidecar not atomic | `checkpoint_store.rs` | Content-hash sidecar path |
| M2 | Epoch advances on partial sink failure | `checkpoint_coordinator.rs` | Don't advance on failure |

### Implementation

#### H9: WAL sync-on-commit

Add `sync_on_commit: bool` to `WalConfig` (default `false`). In `append()`:
```rust
if self.config.sync_on_commit || force_sync || self.last_sync.elapsed() >= self.sync_interval {
    self.sync()?;
}
```

Add doc comment on `WriteAheadLog`:
```rust
/// Durability: with group commit (default), up to `sync_interval` data may be lost on crash.
/// WAL is always synced before checkpoint. Set `sync_on_commit = true` for per-write durability.
```

#### M1: Content-hash sidecar

```rust
fn save_state_data(&self, checkpoint_id: u64, data: &[u8]) -> Result<()> {
    let hash = crc32c::crc32c(data);
    let path = format!("{}/state-{}-{:08x}.bin", self.prefix, checkpoint_id, hash);
    self.runtime.block_on(self.store.put_opts(&path.into(), PutPayload::from(data), PutOptions::default()))?;
    Ok(())
}
```

Store the full path (with hash) in the manifest. Partial uploads produce dangling objects with wrong names.

#### M2: No epoch advance on failure

```rust
let all_ok = sink_statuses.iter().all(|(_, s)| matches!(s, SinkCommitStatus::Committed));
if all_ok {
    self.next_checkpoint_id += 1;
    self.epoch += 1;
} else {
    tracing::error!("Partial sink failure — epoch NOT advanced, will retry");
}
```

### Tests

- `test_wal_sync_on_commit_syncs_every_append` — with flag, sync called per append
- `test_sidecar_path_includes_hash` — verify path format
- `test_epoch_stays_on_sink_failure` — partial failure -> epoch unchanged

### Review Agent Gate 7

Run review agent. Additionally verify:
- [ ] `sync_on_commit` is a simple bool field, not wrapped in an enum/policy type
- [ ] Sidecar hash uses crc32c (already in deps), not a new dependency
- [ ] Epoch non-advance: verify sinks are idempotent for same-epoch retry (check trait docs)
- [ ] No "retry counter" or "max retries" abstraction added — keep it simple

---

## Phase 8: Lookup Join Hardening

**Branch:** `fix/audit-phase8-lookup-hardening`
**Scope:** ~100 lines changed, 4 files
**Risk:** LOW-MEDIUM

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| M21 | Column names not quoted in SQL | `predicate.rs` | Double-quote column names |
| M22 | No cache stampede dedup | `lookup_join_exec.rs` | Dedup miss keys before query |
| M23 | No join key type validation | `lookup_join_exec.rs` | Type check at plan time |
| M24 | Per-row `take()` in CDC update | `pipeline_callback.rs` | Use `batch.slice()` |
| M16 | `record_put` loses value | `changelog_aware.rs` | Add value param |

### Implementation

#### M21: Quote columns

```rust
format!("\"{}\" {} {}", column.replace('"', "\"\""), op_str, value)
```

Apply across all `predicate_to_sql` arms. Single change point.

#### M22: Dedup miss keys

Before source query:
```rust
let mut unique_keys: Vec<&[u8]> = Vec::new();
let mut seen: FxHashSet<&[u8]> = FxHashSet::default();
for key in &miss_keys {
    if seen.insert(key.as_ref()) {
        unique_keys.push(key.as_ref());
    }
}
let results = source.query_batch(&unique_keys).await?;
// Build key -> result map, fan out to all rows
```

#### M23: Type check at plan time

```rust
for (si, li) in stream_key_indices.iter().zip(&lookup_key_indices) {
    let st = stream_schema.field(*si).data_type();
    let lt = lookup_schema.field(*li).data_type();
    if st != lt {
        return Err(DataFusionError::Plan(format!(
            "Lookup join key type mismatch: {st:?} vs {lt:?}"
        )));
    }
}
```

Strict equality — no implicit promotion. Users must cast explicitly in SQL.

#### M24: slice() not take()

```rust
// Replace: arrow::compute::take per row
// With:
let row_batch = batch.slice(row, 1);
```

#### M16: record_put with value

Change trait:
```rust
fn record_put(&self, key: &[u8], value: &[u8]) -> bool;
```

Update call site in `ChangelogAwareStore::put` and all implementations.

### Tests

- `test_sql_reserved_word_column_quoted` — column `order` -> `"order" = 1`
- `test_dedup_miss_keys` — 50 identical misses -> 1 source query
- `test_type_mismatch_plan_error` — Int64 vs Utf8 -> planning error
- `test_cdc_slice_identical_to_take` — functional equivalence test

### Review Agent Gate 8

Run review agent. Additionally verify:
- [ ] Column quoting applied uniformly to ALL predicate arms (Eq, Lt, Gt, In, etc.) — not just Eq
- [ ] Dedup uses `FxHashSet` (not `HashSet` which is banned)
- [ ] Type check is strict equality, no new `types_compatible()` helper with special cases
- [ ] `record_put` signature change — grep for all implementors, verify all updated
- [ ] `batch.slice()` replaces `take()` everywhere in CDC path — no leftover `take()` calls

---

## Phase 9: Performance Polish

**Branch:** `fix/audit-phase9-perf-polish`
**Scope:** ~60 lines changed, 5 files
**Risk:** LOW

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| M6 | `Arc::from()` per late event | `window.rs` | Cache Arc in operator struct |
| M7 | `std::sync::Mutex` not banned | `clippy.toml` | Add to disallowed-types |
| M8 | No AHashMapStore benchmark | `state_bench.rs` | Add bench |
| M19 | Per-row RecordBatch in TableStore | `table_store.rs` | Dedup within batch |
| M20 | No max open windows | `eowc_state.rs` | Add limit, force-close oldest |

### Implementation

#### M6: Cache Arc

Add `side_output_arc: Option<Arc<str>>` field. On first late event:
```rust
let arc = self.side_output_arc.get_or_insert_with(|| Arc::from(name));
```

#### M7: clippy.toml

```toml
{ path = "std::sync::Mutex", reason = "use parking_lot::Mutex or lock-free" },
```

Fix any violations this surfaces.

#### M8: Benchmark

```rust
fn bench_ahash_store_get(c: &mut Criterion) {
    let mut store = AHashMapStore::new();
    for i in 0..10_000u64 {
        store.put(&i.to_le_bytes(), &[0u8; 64]).unwrap();
    }
    c.bench_function("ahash_get_64B", |b| {
        let mut i = 0u64;
        b.iter(|| {
            store.get(&(i % 10_000).to_le_bytes());
            i += 1;
        });
    });
}
```

#### M19: TableStore dedup

```rust
let mut pk_rows: FxHashMap<String, usize> = FxHashMap::default();
for i in 0..batch.num_rows() {
    pk_rows.insert(extract_pk_string(pk_col, i)?, i);
}
for (key, row) in pk_rows {
    self.backend.put(&key, batch.slice(row, 1));
}
```

#### M20: Window limit

Hardcode `MAX_OPEN_WINDOWS = 10_000`. In `update_batch`, after window assignment:
```rust
if self.windows.len() > MAX_OPEN_WINDOWS {
    tracing::warn!(count = self.windows.len(), "Force-closing oldest windows");
    while self.windows.len() > MAX_OPEN_WINDOWS {
        if let Some((&oldest_key, _)) = self.windows.iter().next() {
            self.close_window(oldest_key);
        }
    }
}
```

Force-closed windows emit their current results (not silently dropped).

### Tests

- `test_side_output_arc_reused` — 2 late events, same Arc pointer
- `bench_ahash_get_64B` — validates < 200ns
- `test_table_store_dedup_last_wins` — duplicate PKs -> last value kept
- `test_max_open_windows_closes_oldest` — exceed 10K -> oldest closed, results emitted

### Review Agent Gate 9

Run review agent. Additionally verify:
- [ ] `side_output_arc` field doesn't change checkpoint serialization (it's transient)
- [ ] `std::sync::Mutex` ban — run `cargo clippy` and verify results. List any files that need migration
- [ ] `MAX_OPEN_WINDOWS` is a constant, not a config field with serde
- [ ] Force-close emits results before dropping state — verify `close_window` path
- [ ] Benchmark is simple — no setup abstractions, no parameterized variants

---

## Phase 10: Low-Priority Cleanup

**Branch:** `fix/audit-phase10-cleanup`
**Scope:** ~80 lines changed, 6 files
**Risk:** LOW

### Fixes

| ID | Finding | File | Change |
|----|---------|------|--------|
| M9/M10 | PartitionedRouter RoundRobin broken | `partitioned_router.rs` | Fix to actual round-robin |
| M11 | Missing Timestamp/Date types in router | `partitioned_router.rs` | Add type arms |
| L3 | `Instant::now()` per reactor iter | `reactor/mod.rs` | Check every 64 events |
| L4 | No `#[cold]` on error paths | `operator/*.rs` | Add annotations |
| L11 | `size_bytes` underreports | `ahash_store.rs` | Count index overhead |
| L22 | Registry std RwLock | `lookup_join_exec.rs` | Switch to parking_lot |

### Implementation

#### M10: Fix RoundRobin

```rust
KeySpec::RoundRobin => {
    let core = self.rr_counter.fetch_add(1, Relaxed) % num_cores;
    Ok(vec![(core, batch)])
}
```

Add `rr_counter: AtomicUsize` field.

#### M11: Add types

Add arms for `TimestampMillisecondType`, `TimestampMicrosecondType`, `Date32Type`, `Date64Type`, `Decimal128Type` in `hash_array_value`. Each follows the existing pattern — downcast, hash the native value.

#### L3: Periodic time check

```rust
if self.iteration_count % 64 == 0 {
    if let Some(start) = self.iteration_start {
        if start.elapsed() > self.max_iteration_time { break; }
    } else {
        self.iteration_start = Some(Instant::now());
    }
}
self.iteration_count += 1;
```

#### L4: Cold annotations

Add `#[cold] #[inline(never)]` to error-logging functions in operator modules. Don't annotate individual match arms — only extract-and-annotate actual functions.

#### L11: Accurate size_bytes

```rust
// Account for key in both data HashMap and index BTreeSet
self.size_bytes += 2 * key.len() + value.len();
```

#### L22: parking_lot RwLock

Replace `std::sync::RwLock` with `parking_lot::RwLock`. Remove all `.expect("lock poisoned")` calls.

### Tests

- `test_round_robin_distributes` — 4 batches, 2 cores -> each core gets 2
- `test_hash_timestamp_column` — Timestamp column routes consistently
- `test_hash_decimal_column` — Decimal128 routes consistently
- Existing tests cover remaining changes

### Review Agent Gate 10

Run review agent. Additionally verify:
- [ ] `AtomicUsize` for round-robin is `Relaxed` ordering (sufficient for counter, no need for stronger)
- [ ] New `hash_array_value` arms follow existing pattern exactly — no new helper functions
- [ ] `iteration_count` is a simple `u64`, reset when `Instant::now()` is taken
- [ ] `#[cold]` applied only to functions, not to match arms or closures
- [ ] `parking_lot::RwLock` — verify it's already in Cargo.toml deps (no new dep)
- [ ] No remaining `std::sync::Mutex` or `std::sync::RwLock` in codebase (the clippy ban catches this)

---

## Summary

| Phase | Branch | Fixes | LOC | Risk |
|-------|--------|-------|-----|------|
| 1 | `fix/audit-phase1-checkpoint-putmode` | C1 | ~50 | HIGH |
| 2 | `fix/audit-phase2-lookup-correctness` | H1, H2, H3 | ~150 | HIGH |
| 3 | `fix/audit-phase3-recovery-durability` | H7, H8, H10, M3, M4 | ~140 | MED-HIGH |
| 4 | `fix/audit-phase4-state-store` | H13, M15, M17, M18 | ~100 | MEDIUM |
| 5 | `fix/audit-phase5-join-perf` | H4, H5, H6, M5 | ~150 | MEDIUM |
| 6 | `fix/audit-phase6-tpc` | H11, H12, M12, M13, M14 | ~100 | MEDIUM |
| 7 | `fix/audit-phase7-checkpoint-edges` | H9, M1, M2 | ~80 | LOW-MED |
| 8 | `fix/audit-phase8-lookup-hardening` | M16, M21, M22, M23, M24 | ~100 | LOW-MED |
| 9 | `fix/audit-phase9-perf-polish` | M6, M7, M8, M19, M20 | ~60 | LOW |
| 10 | `fix/audit-phase10-cleanup` | M9-M11, L3, L4, L11, L22 | ~80 | LOW |

**Total:** ~1,010 lines across 10 PRs, fixing 48 findings. Remaining 12 LOW items deferred to regular development.

**After all phases**, final validation:

```bash
cargo test                           # all pass
cargo clippy -- -D warnings          # zero warnings
cargo bench --bench state_bench      # AHashMapStore < 200ns
cargo bench --bench throughput       # > 500K events/sec/core
cargo bench --bench latency          # p99 < 10us
```
