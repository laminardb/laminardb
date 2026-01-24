# F002: Memory-Mapped State Store

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F002 |
| **Status** | ✅ Done |
| **Priority** | P0 (Critical) |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F001, F003 |
| **Owner** | TBD |
| **Created** | 2026-01-20 |
| **Updated** | 2026-01-22 |

## Summary

A high-performance key-value state store implementation using memory-mapped files for persistence and `FxHashMap` for fast lookups. This provides the primary state backend for streaming operators, optimized for sub-microsecond access times.

## Goals

- Achieve < 500ns p99 lookup latency
- Support both in-memory and persistent modes
- Provide efficient prefix scanning for window state
- Enable zero-copy reads where possible

## Non-Goals

- Distributed state (single-node only)
- Transactions across multiple keys
- Secondary indexes
- Index persistence (deferred to F007 - WAL will handle durable recovery)

## Technical Design

### Architecture

Located in `crates/laminar-core/src/state/mmap.rs`, implements the `StateStore` trait.

```rust
pub struct MmapStateStore {
    // Index mapping keys to value entries
    index: FxHashMap<Vec<u8>, ValueEntry>,
    // Storage backend (arena or mmap)
    storage: Storage,
    // Total size tracking
    size_bytes: usize,
    // Next version number
    next_version: u64,
}

struct ValueEntry {
    offset: usize,  // Offset in storage
    len: usize,
    version: u64,
}

enum Storage {
    Arena { data: Vec<u8>, write_pos: usize },
    Mmap { mmap: MmapMut, file: File, path: PathBuf, write_pos: usize, capacity: usize },
}
```

### API/Interface

```rust
impl MmapStateStore {
    // In-memory mode (fastest, not persistent)
    pub fn in_memory(capacity: usize) -> Self;

    // Persistent mode (file-backed)
    pub fn persistent(path: &Path, initial_capacity: usize) -> Result<Self, StateError>;

    // Compaction support
    pub fn compact(&mut self) -> Result<(), StateError>;
    pub fn fragmentation(&self) -> f64;
}

impl StateStore for MmapStateStore {
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateError>;
    fn delete(&mut self, key: &[u8]) -> Result<(), StateError>;
    fn prefix_scan<'a>(&'a self, prefix: &'a [u8]) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a>;
    // ... full StateStore trait
}
```

### Performance Results

| Metric | Target | Achieved |
|--------|--------|----------|
| Get (10K entries) | < 500ns | ~39ns |
| Contains (10K entries) | < 500ns | ~5ns |

**12x better than target** - The two-tier architecture (FxHashMap index + data storage) provides consistent sub-40ns lookups.

## Completion Checklist

- [x] Code implemented (`crates/laminar-core/src/state/mmap.rs`)
- [x] Unit tests passing (13 mmap-specific tests)
- [x] Benchmarks meet targets (12x better than 500ns)
- [x] Documentation updated
- [x] Compaction support for fragmentation management
- [x] Both in-memory and persistent modes working
