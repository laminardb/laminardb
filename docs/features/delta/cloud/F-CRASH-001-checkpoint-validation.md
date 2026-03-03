# F-CRASH-001: Checkpoint Validation & Crash Recovery

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CRASH-001 |
| **Status** | Done |
| **Priority** | P0 |
| **Phase** | 7b |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-S3-001 |
| **Blocks** | F-DISAGG-001 |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage` |
| **Module** | `crates/laminar-storage/src/checkpoint_store.rs`, `checkpoint_manifest.rs` |

## Summary

Detect incomplete checkpoints, orphaned state files, and corrupt manifests. Recover to the last known-good state by walking backward from latest and validating each checkpoint. Add SHA-256 checksums to manifests for state blob integrity. Manifest format versioning already existed (`version` field).

## Goals

- `validate_checkpoint(id)`: verify manifest + state blob exist and checksum matches
- `recover_latest_validated()`: walk backward, validate each checkpoint, return first valid one
- SHA-256 checksum in manifest for sidecar state blob: `state_checksum: Option<String>`
- `--validate-checkpoints` CLI flag for offline validation
- Orphan cleanup: `cleanup_orphans()` deletes state blobs with no matching manifest
- `RecoveryReport`: which checkpoint chosen, which skipped, total recovery time

## Non-Goals

- Automatic checkpoint repair (corrupt = skip, don't fix)
- Cross-node checkpoint validation in delta mode

## Technical Design

### Manifest Schema Changes

```rust
pub struct CheckpointManifest {
    // ... existing fields ...
    #[serde(default)]
    pub state_checksum: Option<String>,  // SHA-256 hex digest of state.bin
}
```

Backward compatible via `#[serde(default)]` — older manifests without this field deserialize as `None`.

### Recovery Algorithm

```
1. Enumerate all checkpoint IDs via list_ids() (includes corrupt ones)
2. Sort descending (newest first)
3. For each candidate:
   a. validate_checkpoint(id):
      - Try loading manifest JSON (corrupt JSON → invalid, not error)
      - Run manifest.validate() for structural issues
      - If state_checksum present: load state.bin, compute SHA-256, compare
   b. If valid → return this checkpoint
   c. If invalid → log warning, add to skipped list, try next
4. If none found → return RecoveryReport with chosen_id = None (fresh start)
```

### Crash Scenarios

| Crash Point | Symptom | Recovery |
|-------------|---------|----------|
| After state write, before manifest | Orphaned state.bin | Skip (no manifest), cleanup_orphans() |
| During manifest write | Partial/corrupt JSON | validate_checkpoint catches Serde error |
| After manifest, before latest.txt | Latest points to old checkpoint | list_ids() finds all, walks backward |
| State blob corrupt on S3 | Checksum mismatch | Skip, try previous checkpoint |

### API

```rust
// On CheckpointStore trait
fn validate_checkpoint(&self, id: u64) -> Result<ValidationResult, CheckpointStoreError>;
fn recover_latest_validated(&self) -> Result<RecoveryReport, CheckpointStoreError>;
fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError>;
fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError>;

// Structs
pub struct ValidationResult { pub checkpoint_id: u64, pub valid: bool, pub issues: Vec<String> }
pub struct RecoveryReport { pub chosen_id: Option<u64>, pub skipped: Vec<(u64, String)>, pub examined: usize, pub elapsed: Duration }
```

### Files Modified

| File | Change |
|------|--------|
| `crates/laminar-storage/src/checkpoint_manifest.rs` | Add `state_checksum: Option<String>` field |
| `crates/laminar-storage/src/checkpoint_store.rs` | Add `validate_checkpoint()`, `recover_latest_validated()`, `cleanup_orphans()`, `list_ids()`, `ValidationResult`, `RecoveryReport` |
| `crates/laminar-storage/src/lib.rs` | Export `RecoveryReport`, `ValidationResult` |
| `crates/laminar-server/src/main.rs` | Add `--validate-checkpoints` CLI flag with `build_checkpoint_store()` |

## Test Plan

- Validate a correct checkpoint → valid
- Validate a missing checkpoint → invalid (not found)
- Validate a corrupt manifest → invalid (Serde error caught)
- Validate with correct state checksum → valid
- Validate with corrupt state.bin → checksum mismatch → invalid
- Validate with missing state.bin when checksum expected → invalid
- Recovery walk: corrupt latest → skips to valid predecessor
- Recovery walk: empty store → fresh start
- Recovery walk: all corrupt → fresh start (chosen_id = None)
- Cleanup orphans: removes state.bin dirs without manifests
- Cleanup orphans: no-op when clean
- save_with_state() writes SHA-256 checksum automatically
- Backward compat: older manifests without state_checksum deserialize fine
- ObjectStore variants: validate, recover, cleanup all work with InMemory backend

## Completion Checklist

- [x] `validate_checkpoint()` implemented on `CheckpointStore` trait
- [x] `recover_latest_validated()` with backward walk using `list_ids()`
- [x] SHA-256 `state_checksum` field on `CheckpointManifest` (with `#[serde(default)]`)
- [x] `save_with_state()` automatically computes and stores checksum
- [x] `format_version` field already exists (`version: u32` on `CheckpointManifest`)
- [x] `RecoveryReport` struct with chosen_id, skipped, examined, elapsed
- [x] `ValidationResult` struct with checkpoint_id, valid, issues
- [x] `cleanup_orphans()` for `FileSystemCheckpointStore` (scans dirs) and `ObjectStoreCheckpointStore` (scans state files)
- [x] `list_ids()` on trait + both implementations for ID-only enumeration
- [x] Corrupt manifest JSON caught as validation failure (not propagated error)
- [x] CLI `--validate-checkpoints` flag with `build_checkpoint_store()` in server main
- [x] 20 crash scenario tests: FS (14) + ObjectStore (6)
- [x] Backward compat test: manifests without `state_checksum` deserialize fine
- [x] Feature INDEX.md updated
