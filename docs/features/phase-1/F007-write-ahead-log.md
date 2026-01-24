# F007: Write-Ahead Log

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F007 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F001, F002 |
| **Owner** | TBD |

## Summary

Implement a write-ahead log (WAL) for durability. All state mutations are logged before being applied, enabling recovery after crashes.

## Goals

- Append-only log with group commit
- Configurable sync intervals
- Log rotation and cleanup
- Recovery replay support
- MmapStateStore index reconstruction on recovery (deferred from F002)

## Technical Design

```rust
pub struct WriteAheadLog {
    writer: BufWriter<File>,
    sync_interval: Duration,
    position: u64,
}

pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Checkpoint { id: u64 },
    Commit { offsets: HashMap<String, u64> },
}

impl WriteAheadLog {
    pub fn append(&mut self, entry: WalEntry) -> Result<u64>;
    pub fn sync(&mut self) -> Result<()>;
    pub fn read_from(&self, position: u64) -> impl Iterator<Item = WalEntry>;
}
```

## Benchmarks

- [x] `bench_wal_append` - Target: < 1μs (buffered) - **Result: ~215ns ✓**
- [x] `bench_wal_sync` - Target: < 10ms (fsync) - **Result: ~2.3ms ✓**

## Completion Checklist

- [x] WAL writing implemented
- [x] Group commit working
- [x] Recovery replay tested
- [x] Benchmarks passing
