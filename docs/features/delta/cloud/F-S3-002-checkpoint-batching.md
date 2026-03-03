# F-S3-002: Checkpoint Batching

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-S3-002 |
| **Status** | Done |
| **Priority** | P1 |
| **Phase** | 7b |
| **Effort** | M (3-4 days) |
| **Dependencies** | F-S3-001 |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-storage` |
| **Module** | `crates/laminar-storage/src/checkpoint_batcher.rs` |

## Summary

Accumulate state blobs from multiple partitions/operators into batched objects before flushing to S3. Targets 4-32MB object sizes to minimize PUT costs ($0.005/1000 PUTs). Compresses with LZ4 before upload.

## Goals

- `CheckpointBatcher`: accumulates state blobs until flush threshold (default 8MB) or interval
- LZ4 compression before upload (fastest decompression for recovery)
- Metrics: `BatchMetrics` with batches_flushed, entries_flushed, bytes before/after compression, put_count
- Configurable flush threshold and interval

## Non-Goals

- Zstd compression (archival only, deferred to F-S3-003 tiering)
- Multi-part uploads (objects should stay under 32MB)

## Technical Design

### Cost Model

Every S3 PUT costs $0.005/1000 requests. At 50 PUTs/s: ~$18/day. Batching reduces to ~5 PUTs/s: ~$1.80/day.

Object size sweet spot: 4MB-32MB. Too small = high per-request cost. Too large = wasted bandwidth on partial reads during recovery.

### Batch Format

```text
[magic: 4 bytes "LCB1"]
[version: u32 LE = 1]
[entry_count: u32 LE]
[uncompressed_size: u32 LE]
[compressed_body: LZ4 block (compress_prepend_size)]
  → decoded body is a sequence of frames:
    [key_len: u32 LE][key: UTF-8][data_len: u32 LE][data: bytes]
```

### API

```rust
pub struct CheckpointBatcher { /* ... */ }

impl CheckpointBatcher {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, flush_threshold: Option<usize>) -> Self;
    pub fn add(&mut self, key: String, data: Vec<u8>);
    pub fn should_flush(&self) -> bool;
    pub fn flush(&mut self, epoch: u64) -> Result<(), CheckpointStoreError>;
    pub fn metrics(&self) -> &Arc<BatchMetrics>;
}

pub fn decode_batch(raw: &[u8]) -> Result<Vec<(String, Vec<u8>)>, CheckpointStoreError>;
```

### Files Modified

| File | Change |
|------|--------|
| `Cargo.toml` | Add `lz4_flex = "0.11"` to workspace deps |
| `crates/laminar-storage/Cargo.toml` | Add `lz4_flex` dependency |
| `crates/laminar-storage/src/checkpoint_batcher.rs` | New: `CheckpointBatcher`, `BatchMetrics`, `decode_batch` |
| `crates/laminar-storage/src/lib.rs` | Export new module and types |

## Test Plan

- Batching accumulates until threshold, then flushes single object
- LZ4 roundtrip: compress -> store -> load -> decompress matches original
- Metrics emitted on flush
- Flush on interval even if threshold not reached

## Completion Checklist

- [x] `CheckpointBatcher` implemented with `add()`, `should_flush()`, `flush()`, `len()`, `is_empty()`, `buffer_size()`
- [x] LZ4 compression via `lz4_flex::compress_prepend_size` / `decompress_size_prepended`
- [x] `BatchMetrics` with `AtomicU64` counters + `snapshot()` (project pattern)
- [x] `decode_batch()` public function for reading batches during recovery
- [x] Binary batch format with magic, version, entry count, and framed entries
- [x] 12 unit tests with `InMemory` backend: size tracking, threshold, flush, LZ4 roundtrip, metrics accumulation, compression ratio, decode errors
- [x] Feature INDEX.md updated
