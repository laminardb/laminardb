# Plan: Remove per_core_wal Module

**Status:** Pending
**Impact:** ~3,265 LOC deleted from per_core_wal + ~300 LOC removed across consumers
**Risk:** Low — module is fully dead code (writes happen but are never read in production)

## Background

The `per_core_wal` module implements a WAL-replay recovery strategy: write
Put/Delete/Commit/EpochBarrier entries to per-core log files during
checkpointing, then replay them on recovery to reconstruct state.

**This recovery path is never used.** The production recovery path
(`RecoveryManager` in laminar-db) loads operator state snapshots directly
from `CheckpointManifest` — full state is captured by each operator's
`checkpoint()` method. Sources are restored to their checkpointed offsets
(Kafka partition offsets, PG LSN, MySQL GTID, etc.) and resume from there.

The per_core_wal module:
- **Writes** on every checkpoint cycle (changelog drain → WAL Put/Delete, epoch barriers, fdatasync)
- **Never reads** in production (no caller to `recover_per_core()` or `PerCoreRecoveryManager` from laminar-db)
- Costs real I/O latency on the checkpoint path (fdatasync per cycle)

The single-writer `wal` module (`laminar-storage/src/wal/`) is a **separate
system** used by the `incremental` checkpoint subsystem — it is NOT affected
by this removal.

## Removal Steps

### Step 1: Delete per_core_wal directory

Delete the entire directory `crates/laminar-storage/src/per_core_wal/` (8 files):

- `mod.rs` (~91 lines)
- `manager.rs` (~583 lines)
- `writer.rs` (~573 lines)
- `reader.rs` (~512 lines)
- `coordinator.rs` (~445 lines)
- `recovery.rs` (~509 lines)
- `entry.rs` (~453 lines)
- `error.rs` (~100 lines)

### Step 2: Remove from laminar-storage lib.rs

**File:** `crates/laminar-storage/src/lib.rs`

- **Line 10:** Remove `- [`per_core_wal`]: Per-core WAL segments for thread-per-core architecture` from doc comment
- **Lines 40-41:** Remove `pub mod per_core_wal;` and its doc comment
- **Lines 74-79:** Remove entire re-export block:
  ```rust
  // Re-export per-core WAL types
  pub use per_core_wal::{
      recover_per_core, CoreWalWriter, PerCoreCheckpointCoordinator, PerCoreRecoveredState,
      PerCoreRecoveryManager, PerCoreWalConfig, PerCoreWalEntry, PerCoreWalError, PerCoreWalManager,
      PerCoreWalReader, SegmentStats, WalOperation,
  };
  ```

### Step 3: Remove WAL fields from CheckpointManifest

**File:** `crates/laminar-storage/src/checkpoint_manifest.rs`

- **Lines 87-96:** Remove both fields:
  ```rust
  /// WAL position for single-writer mode.
  #[serde(default)]
  pub wal_position: u64,
  /// Per-core WAL positions at the time of operator snapshot.
  /// ...
  #[serde(default)]
  pub per_core_wal_positions: Vec<u64>,
  ```
- **Lines 309-310:** Remove from `new()` constructor:
  ```rust
  wal_position: 0,
  per_core_wal_positions: Vec::new(),
  ```
- **Lines 530, 540:** Remove `wal_position` assignment and assertion in test
- **Line 565:** Remove `per_core_wal_positions` assertion in test
- **Lines 623-629:** Delete entire `test_manifest_per_core_wal_positions` test

**File:** `crates/laminar-storage/src/checkpoint_store.rs`

- **Lines 1237-1238:** Remove test assignments:
  ```rust
  m.wal_position = 4096;
  m.per_core_wal_positions = vec![100, 200];
  ```
- **Lines 1246-1247:** Remove test assertions:
  ```rust
  assert_eq!(loaded.wal_position, 4096);
  assert_eq!(loaded.per_core_wal_positions, vec![100, 200]);
  ```

> **Note on deserialization:** Both fields use `#[serde(default)]`, so any
> existing checkpoint JSON files that contain these fields will still
> deserialize cleanly — serde ignores unknown fields by default. No
> migration needed.

### Step 4: Remove WAL integration from CheckpointCoordinator

**File:** `crates/laminar-db/src/checkpoint_coordinator.rs`

**Import (line 36):** Remove:
```rust
use laminar_storage::per_core_wal::PerCoreWalManager;
```

**Struct `WalPrepareResult` (lines 232-242):** Delete entire struct. Rewrite
`prepare_wal_for_checkpoint()` to return just `entries_drained: u64`.

**Fields on `CheckpointCoordinator` (lines 261-275):** Remove:
```rust
wal_manager: Option<PerCoreWalManager>,       // line 262
previous_wal_positions: Option<Vec<u64>>,      // line 275
```

**Constructor `new()` (lines 300, 306):** Remove:
```rust
wal_manager: None,             // line 300
previous_wal_positions: None,  // line 306
```

**Methods to delete entirely (lines 393-504):**
- `register_wal_manager()` (lines 401-403)
- `prepare_wal_for_checkpoint()` — rewrite to **only** drain changelogs (lines 427-463):
  ```rust
  // AFTER: just drain changelogs, no WAL involvement
  pub fn prepare_for_checkpoint(&mut self) -> u64 {
      let mut total_drained: u64 = 0;
      for drainer in &mut self.changelog_drainers {
          let count = drainer.drain();
          total_drained += count as u64;
          debug!(drained = count, pending = drainer.pending_count(), "changelog drainer flushed");
      }
      total_drained
  }
  ```
- `truncate_wal_after_checkpoint()` (lines 473-493) — delete entirely
- `wal_manager()` (lines 497-499) — delete
- `wal_manager_mut()` (lines 502-504) — delete

**Usage in `checkpoint_inner()` (lines 1032-1078):**
- **Lines 1032-1037:** Replace WAL prep call:
  ```rust
  // BEFORE:
  let wal_result = self.prepare_wal_for_checkpoint()?;
  let per_core_wal_positions = wal_result.per_core_wal_positions;

  // AFTER:
  let _entries_drained = self.prepare_for_checkpoint();
  ```
- **Lines 1077-1078:** Remove:
  ```rust
  // wal_position is legacy (single-writer mode); per-core positions are authoritative.
  manifest.per_core_wal_positions = per_core_wal_positions;
  ```

**Post-checkpoint truncation:** Search for any call to
`truncate_wal_after_checkpoint` in `checkpoint_inner` or its callers and
remove.

**Debug impl (line 1328):** Remove:
```rust
.field("has_wal_manager", &self.wal_manager.is_some())
```

**Tests to delete (lines 1680-1903):** Remove all 8 WAL-related tests:
- `test_prepare_wal_no_wal_manager` (1682-1691)
- `test_prepare_wal_with_manager` (1693-1717)
- `test_truncate_wal_no_manager` (1719-1726)
- `test_truncate_wal_with_manager` (1728-1752)
- `test_truncate_wal_safety_buffer` (1754-1794)
- `test_prepare_wal_with_changelog_drainer` (1796-1822) — **keep but update:** remove the `per_core_wal_positions` assertion (line 1818), keep the changelog drainer portion
- `test_full_checkpoint_with_wal_coordination` (1824-1868) — **keep but update:** remove WAL setup (lines 1833-1838), remove WAL position assertion (line 1863), remove WAL truncation (lines 1865-1867)
- `test_wal_manager_accessors` (1870-1887) — delete
- `test_coordinator_debug_with_wal` (1889-1903) — delete

### Step 5: Remove WAL from pipeline_lifecycle.rs

**File:** `crates/laminar-db/src/pipeline_lifecycle.rs`

**Lines 163-196:** Remove entire WAL initialization block:
```rust
// Wire per-core WAL for crash recovery between checkpoints.
// ... (lines 163-196)
```

This removes:
- WAL directory calculation (lines 165-170)
- num_cores calculation (lines 173-176)
- `PerCoreWalManager::new()` call (lines 178-179)
- `coord.register_wal_manager(wal)` call (line 187)
- WAL failure warning (lines 189-196)

### Step 6: Remove WAL accessors from recovery_manager.rs

**File:** `crates/laminar-db/src/recovery_manager.rs`

- **Lines 82-92:** Remove both accessor methods:
  ```rust
  pub fn wal_position(&self) -> u64 { ... }
  pub fn per_core_wal_positions(&self) -> &[u64] { ... }
  ```
- **Lines 778-786:** Remove test assignments and assertions:
  ```rust
  manifest.wal_position = 4096;
  ...
  assert_eq!(result.wal_position(), 4096);
  assert_eq!(result.per_core_wal_positions(), &[100, 200, 300]);
  ```

### Step 7: Clean up test files and benches

**File:** `crates/laminar-db/tests/checkpoint_recovery.rs`

- **Line 55-56:** Remove assertion:
  ```rust
  assert!(manifest.per_core_wal_positions.is_empty());
  ```
- **Lines 174-198:** Delete entire `test_wal_positions_recovery` test
- **Line 196:** (covered by deleting the above test)
- **Lines 343-344:** Remove from round-trip test:
  ```rust
  manifest.wal_position = 4096;
  manifest.per_core_wal_positions = vec![10, 20, 30, 40];
  ```
- **Lines 372-373:** Remove from round-trip assertions:
  ```rust
  assert_eq!(restored.wal_position, 4096);
  assert_eq!(restored.per_core_wal_positions, vec![10, 20, 30, 40]);
  ```

**File:** `crates/laminar-db/benches/recovery_bench.rs`

- **Lines 38-39:** Remove:
  ```rust
  m.wal_position = 4096 * id;
  m.per_core_wal_positions = vec![100, 200, 300, 400];
  ```

### Step 8: Documentation updates

**File:** `docs/ARCHITECTURE.md`

- **Line ~76:** Remove reference to per_core_wal in WAL Writer description

**File:** `crates/laminar-storage/README.md`

- **Line ~16:** Remove per_core_wal from module listing
- **Line ~28:** Remove PerCoreWalManager/CoreWalWriter component references

### Step 9: Build and verify

```bash
cargo build --all 2>&1 | head -50           # compile check
cargo clippy --all -- -D warnings           # lint
cargo test --all --lib                       # unit tests
cargo test --all --test checkpoint_recovery  # integration test
```

## What stays

- **`crate::wal`** (single-writer WAL) — used by `incremental` module, separate system
- **`ChangelogDrainer`** — still useful for flushing Ring 1 → Ring 0 state; the drain-only version of `prepare_for_checkpoint()` keeps this
- **`CheckpointManifest`** with operator_states, source_offsets, etc. — the actual recovery mechanism
- **`RecoveryManager`** in laminar-db — loads manifest and restores sources/operators

## What does NOT need migration

Since `wal_position` and `per_core_wal_positions` are `#[serde(default)]`,
existing checkpoint JSON files will deserialize cleanly with serde's default
`deny_unknown_fields = false` behavior. The fields are simply ignored on
load. No checkpoint migration or versioning needed.
