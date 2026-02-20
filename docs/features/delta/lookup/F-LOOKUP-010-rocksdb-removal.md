# F-LOOKUP-010: Remove RocksDB Dependency

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-010 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | S (1-2 days) |
| **Dependencies** | F-LOOKUP-004 (foyer Memory Cache), F-LOOKUP-005 (foyer Hybrid Cache) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core`, `laminar-db` |
| **Module** | Multiple — removal across codebase |

## Summary

Remove the `rocksdb` crate dependency, the `rocksdb` feature flag, and all RocksDB-related code (`RocksDbTableStore`, `Arc<Mutex<DB>>` wrapper, Windows PlainTable workarounds). RocksDB's roles are split between two pure-Rust replacements: **foyer** replaces RocksDB for lookup table caching (in-memory + disk tiers), and **redb** (F-IDX-001) replaces RocksDB for secondary index storage and ad-hoc query persistence. No migration path — tables re-populate from source connectors on restart.

## Goals

- Delete `RocksDbTableStore` and all RocksDB-related modules
- Remove `rocksdb` feature flag from `laminar-db/Cargo.toml`
- Remove `rocksdb` crate from dependency tree entirely
- Eliminate C++ FFI from the build (no C++ compiler required)
- Fix Windows build simplification (PlainTable workaround removed)
- Remove `Arc<Mutex<DB>>` pattern and associated clippy suppressions
- Clean up all `#[cfg(feature = "rocksdb")]` gates

## Non-Goals

- Providing any migration tooling or data export
- Keeping RocksDB behind a feature flag
- Supporting RocksDB as an alternative backend

## Technical Design

### Removal Scope

Files to delete:
- `laminar-core/src/table/rocksdb_store.rs` (or equivalent)
- Any RocksDB-related code in `laminar-storage/src/` (incremental checkpoint module uses RocksDB)
- `laminar-db/src/table_backend.rs` — remove the `Persistent` variant (RocksDB column-family backend)
- Any RocksDB-specific test files
- Any RocksDB-specific benchmark files

Code to remove:
- `rocksdb` feature flag in `laminar-db/Cargo.toml`
- `rocksdb` feature flag AND default feature entry in `laminar-storage/Cargo.toml`
- All `#[cfg(feature = "rocksdb")]` conditional compilation blocks
- `#[allow(clippy::unnecessary_wraps)]` suppressions added for RocksDB dual-feature compatibility
- `Arc<Mutex<DB>>` wrapper (replaced by lock-free foyer)
- Windows-specific `BlockBasedTable` fallback for unavailable `PlainTable`

Dependencies to remove from Cargo.toml:
- `laminar-db/Cargo.toml`: `rocksdb = { version = "0.24", optional = true }`
- `laminar-storage/Cargo.toml`: `rocksdb = { version = "0.24", optional = true }` — **NOTE**: `laminar-storage` has `default = ["rocksdb"]`, this must also be removed from the default feature list

### Replacement

RocksDB's roles are split across two pure-Rust crates:

| RocksDB role | Replacement | Crate |
|---|---|---|
| Lookup table cache (memory tier) | foyer in-memory Cache (Ring 0) | `foyer` (F-LOOKUP-004) |
| Lookup table cache (disk tier) | foyer HybridCache (memory + disk) | `foyer` (F-LOOKUP-005) |
| `create_cf()` per table | Table ID prefix in cache key | `foyer` |
| Block cache for reads | foyer in-memory Cache (Ring 0) | `foyer` |
| Compaction for space reclaim | foyer disk eviction policy | `foyer` |
| Secondary indexes for ad-hoc queries | redb B-tree tables | `redb` (F-IDX-001) |
| Persistent key-value store (general) | redb for indexes, foyer for caching | `redb` + `foyer` |

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Build failure (missing rocksdb) | Users still enabling `rocksdb` feature | Compile error with clear message |

## Test Plan

### Unit Tests

- [ ] All existing `TableStore` trait tests pass without `rocksdb` feature
- [ ] `cargo build` succeeds without C++ compiler installed
- [ ] `cargo build --all-features` succeeds without `rocksdb` in feature list
- [ ] No `#[cfg(feature = "rocksdb")]` blocks remain in codebase

### Integration Tests

- [ ] Reference table tests pass with foyer backend
- [ ] Windows CI passes (no PlainTable workaround needed)

## Completion Checklist

- [ ] All RocksDB code deleted
- [ ] `rocksdb` removed from `laminar-db/Cargo.toml`
- [ ] `rocksdb` removed from `laminar-storage/Cargo.toml` (including `default = ["rocksdb"]`)
- [ ] All `#[cfg(feature = "rocksdb")]` removed
- [ ] All clippy dual-feature suppressions removed
- [ ] Tests pass on Linux and Windows
- [ ] No C++ compiler needed for build
- [ ] Feature INDEX.md updated
- [ ] MEMORY.md RocksDB notes removed

---

## References

- [F-CONN-002D: RocksDB-Backed Persistent Table Store](../../phase-3/F-CONN-002D-rocksdb-table-store.md) (to be superseded)
- [F-LOOKUP-004: foyer In-Memory Cache](F-LOOKUP-004-foyer-memory-cache.md)
- [F-LOOKUP-005: foyer Hybrid Cache](F-LOOKUP-005-foyer-hybrid-cache.md)
- [F-IDX-001: redb Secondary Indexes](../indexes/F-IDX-001-redb-secondary-indexes.md)
- [Storage Architecture Research](../../../research/storage-architecture.md)
