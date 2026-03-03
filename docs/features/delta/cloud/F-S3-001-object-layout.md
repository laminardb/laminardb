# F-S3-001: S3 Object Layout

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-S3-001 |
| **Status** | Done |
| **Priority** | P0 |
| **Phase** | 7b |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-CFG-001 |
| **Blocks** | F-CRASH-001, F-DISAGG-001 |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage` |
| **Module** | `crates/laminar-storage/src/checkpoint_store.rs` |

## Summary

Migrate `ObjectStoreCheckpointStore` from flat `checkpoints/checkpoint_NNNNNN/` layout to a hierarchical, production-ready S3 object layout. All files are immutable (write-once, never modify). Manifests use epoch-based naming. Per-partition state directories enable future disaggregated mode. Backward-compatible: reads old layout, writes new layout.

## Goals

- Hierarchical object layout: `manifests/`, `checkpoints/`, `output/`
- Epoch-based manifest naming: `manifests/manifest-{epoch}.json`
- Per-partition checkpoint paths: `checkpoints/cp-{epoch}-{shard-id}.rkyv`
- Conditional PUT on manifest writes (split-brain prevention via `PutMode::Create`)
- Backward compatibility: detect old layout on `load_latest()`, read from it
- Parquet output paths for window results and materialized views

## Non-Goals

- Implementing Parquet writers (deferred)
- Storage class tiering (F-S3-003)
- Checkpoint batching (F-S3-002)

## Technical Design

### Object Layout

```
s3://{bucket}/{instance-id}/
  manifests/
    manifest-{epoch}.json              # Atomic pointer to valid state
    latest.json                        # Points to current epoch
  checkpoints/
    cp-{epoch}-{shard-id}.rkyv         # rkyv-serialized operator state
  output/
    windows/{table}/{window_type}/{date}/
      result-{partition}.parquet       # Window results
    materialized_views/{view_name}/
      snapshot-{epoch}.parquet         # MV snapshots
```

### Design Principles

- All files immutable (write-once, never modify) — fits S3's object model
- Manifest is the atomic pointer (conditional PUT prevents split-brain)
- Hierarchical keys for efficient S3 LIST and lifecycle rules
- Parquet outputs directly queryable by external engines

### Files to Modify

| File | Change |
|------|--------|
| `crates/laminar-storage/src/checkpoint_store.rs` | Update `ObjectStoreCheckpointStore` path methods; add migration detection |

## Test Plan

- Write checkpoint with new layout, verify paths match spec
- Read checkpoint from old layout (backward compat)
- Write 100 checkpoints, verify hierarchical structure
- Conditional PUT prevents duplicate epoch manifest

## Completion Checklist

- [x] New v2 path methods: `manifest_path()`, `latest_pointer_path()`, `state_path()`
- [x] Legacy v1 path methods for backward-compat reads
- [x] Conditional PUT on manifest via `PutMode::Create` (with `NotImplemented` fallback)
- [x] `LatestPointer` JSON struct for `manifests/latest.json`
- [x] `get_bytes()` / `load_manifest_at()` helpers to reduce duplication
- [x] `load_latest()` / `load_by_id()` / `load_state_data()` check v2 then v1
- [x] `list_checkpoint_ids()` scans both `manifests/` and `checkpoints/` prefixes
- [x] `prune()` cleans both v2 and v1 layouts
- [x] 20 tests: 12 existing (updated) + 8 new (layout paths, backward compat, mixed, conditional PUT, state compat, prune both, latest.json format)
- [x] Feature INDEX.md updated
