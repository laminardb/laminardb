# F-DISAGG-001: Disaggregated State Backend

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DISAGG-001 |
| **Status** | Done |
| **Priority** | P1 |
| **Phase** | 7c |
| **Effort** | XL (8-13 days) |
| **Dependencies** | F-CRASH-001, F-S3-001, F-GATE-001 |
| **Blocks** | F-MIGRATE-001 |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage`, `laminar-server` |
| **Module** | `crates/laminar-storage/src/disaggregated.rs` |

## Summary

S3 as source of truth for partition state, with foyer in-memory cache for fast recovery reads. Ring 0 never touches S3. Ring 1 does async flush. Ring 2 manages lifecycle. Follows RisingWave Hummock pattern: "write to S3, never read from it during queries."

## Goals

- `DisaggregatedStateBackend`: wraps `Arc<dyn ObjectStore>` + `foyer::Cache`
- Cache hit → return. Cache miss → S3 GET → decompress → populate cache (recovery only)
- `put_batch()`: serialize + LZ4 compress → single S3 PUT → populate cache
- Per-partition layout: `partitions/{id}/state/epoch-{n}.lz4`
- All writes immutable (write-once, never modify)
- Zombie fencing: epoch-based, reject writes from stale partition owners

## Non-Goals

- SlateDB integration (deferred evaluation after this phase)
- Point lookups against S3 during normal operation (cache must absorb all hot reads)
- `laminar-core` state store variant (server-only integration — library users configure directly)

## Technical Design

### Architecture (Ring Model)

```
Ring 0 (hot path, <500ns)
  FxHashMap (partition-local state, zero alloc)
    |
    v (changelog, async, Ring 1)
Ring 1 (background, microseconds)
  foyer in-memory cache (recovery reads)
    |
    v (batched flush, async, Ring 1)
Ring 2 (control plane, milliseconds)
  S3 (source of truth, immutable blobs)
    LZ4-compressed state blobs
```

### Ring Invariants (enforced in code)

- Ring 0: NEVER calls ObjectStore methods. NEVER allocates. NEVER locks.
- Ring 1: Async only. foyer::Cache for reads. Batched S3 PUT for writes.
- Ring 2: Manifest management. S3 lifecycle tiering. Partition assignment.

### Blob Format (LDS1)

```text
[magic: 4 bytes "LDS1"]
[version: u32 LE = 1]
[entry_count: u32 LE]
[uncompressed_size: u32 LE]
[compressed_body: LZ4 block]
  → decoded body: [key_len: u32 LE][key][value_len: u32 LE][value]...
```

### S3 Object Layout

```
{prefix}/partitions/{partition_id:06}/state/epoch-{epoch:06}.lz4
```

### API

```rust
pub struct DisaggregatedConfig {
    pub prefix: String,
    pub cache_capacity: usize,   // default 1024
    pub cache_shards: usize,     // default 16
}

pub struct DisaggregatedStateBackend { /* ... */ }

impl DisaggregatedStateBackend {
    pub fn new(store: Arc<dyn ObjectStore>, config: DisaggregatedConfig) -> Self;
    pub fn put_batch(&self, partition_id: u64, epoch: u64, entries: &[StateEntry]) -> Result<(), DisaggregatedError>;
    pub fn get(&self, partition_id: u64, epoch: u64) -> Result<Option<Vec<StateEntry>>, DisaggregatedError>;
    pub fn load_latest(&self, partition_id: u64) -> Result<Option<(u64, Vec<StateEntry>)>, DisaggregatedError>;
    pub fn advance_epoch(&self, new_epoch: u64);
    pub fn current_epoch(&self) -> u64;
    pub fn list_epochs(&self, partition_id: u64) -> Result<Vec<u64>, DisaggregatedError>;
    pub fn prune(&self, partition_id: u64, keep_count: usize) -> Result<usize, DisaggregatedError>;
}

pub struct StateEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
```

### Files Modified

| File | Change |
|------|--------|
| `crates/laminar-storage/src/disaggregated.rs` | New: `DisaggregatedStateBackend`, `DisaggregatedConfig`, `DisaggregatedError`, `StateEntry`, LDS1 blob codec, 30 tests |
| `crates/laminar-storage/src/lib.rs` | Add `pub mod disaggregated`, re-export key types |
| `crates/laminar-storage/Cargo.toml` | Add `foyer` dependency |
| `crates/laminar-server/src/config.rs` | Add `cache_capacity`, `cache_shards` fields to `StateSection` |

## Test Plan

- Blob encode/decode roundtrip (empty, normal, large batch)
- Invalid blob detection (bad magic, unsupported version, truncated)
- Truncated body detection (key length, key data, value length, value data)
- put + get roundtrip
- get nonexistent returns None
- Cache hit path (repeated get)
- Cache miss path (cross-backend S3 fetch populates cache)
- Zombie fencing: stale epoch rejected, current/future epoch allowed
- Monotonic epoch advancement
- list_epochs sorted, per-partition isolation
- load_latest picks highest epoch
- Prune keeps recent, noop when under limit, evicts cache
- Per-partition state isolation
- Large batch (1000 entries) roundtrip
- Path epoch parsing (valid, invalid prefix, invalid suffix)
- Default config values
- Error display formatting

## Completion Checklist

- [x] `DisaggregatedStateBackend` implemented with dedicated tokio runtime
- [x] foyer in-memory cache integration (cache on put, cache on get miss)
- [x] Per-partition S3 layout: `partitions/{id}/state/epoch-{n}.lz4`
- [x] Zombie fencing: `advance_epoch()` + stale epoch rejection in `put_batch()`
- [x] LDS1 blob format: LZ4-compressed key-value entries with header
- [x] `load_latest()`: scan epochs, load highest
- [x] `prune()`: delete old epochs, evict from cache
- [x] `list_epochs()`: list available epochs per partition
- [x] Server config: `cache_capacity`, `cache_shards` in `[state]` section
- [x] 30 unit tests + 1 doc-test (all passing)
- [x] 33 existing server config tests still pass
- [x] Clippy clean (`-D warnings`) on both `laminar-storage` and `laminar-server`
- [x] Feature INDEX.md updated
