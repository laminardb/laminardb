# F022: Incremental Checkpointing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F022 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F008, F013 |
| **Owner** | TBD |
| **Research** | [Checkpoint Implementation Prompt](../../research/checkpoint-implementation-prompt.md) |

## Summary

Implement a three-tier incremental checkpoint architecture that maintains Ring 0 latency (<500ns) while providing durable, incremental state snapshots. Uses a zero-alloc changelog buffer in Ring 0, async WAL drain in Ring 1, and RocksDB incremental checkpoints with hard-linked SSTables.

## Motivation

From research review - **Current limitations**:

1. **F008 blocks Ring 0**: Current checkpoint creates full snapshot synchronously, blocking the reactor
2. **Full snapshots only**: No incremental mechanism, checkpoint size = full state size
3. **No delta tracking**: Cannot identify changed keys since last checkpoint
4. **Recovery time**: Large state requires reading entire snapshot

**Core Invariant**:
```
Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State
```

**Why WAL + Checkpoint (not just one)?**

| Approach | Durability | Recovery Time | Space |
|----------|------------|---------------|-------|
| WAL only | ✅ Per-commit | ❌ O(all events) | ❌ Unbounded |
| Checkpoint only | ❌ Periodic loss | ✅ O(state) | ✅ Bounded |
| **WAL + Checkpoint** | ✅ Per-commit | ✅ O(state + delta) | ✅ WAL truncated |

## Goals

1. Maintain Ring 0 latency (<500ns) during checkpoint operations
2. Incremental checkpoints (only changed state)
3. Async checkpoint creation in Ring 1
4. RocksDB-based checkpoint with hard-linked SSTables
5. Resume from partial checkpoint after crash
6. WAL truncation after successful checkpoint
7. Source offset tracking for exactly-once replay

## Non-Goals

- Object storage tier (Ring 2) - future Phase 3 scope
- Distributed checkpointing across nodes
- Unaligned checkpoints (Flink 1.11+ feature)

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ WRITE PATH                                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Ring 0 (Hot, <500ns):                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Event ──▶ mmap_state.put() ──▶ changelog.push(offset_ref)  │ │
│  │                                 (zero-alloc)               │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                  │
│                              ▼ async drain when idle            │
│  Ring 1 (Background):                                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ changelog.drain() ──▶ wal.append() ──▶ wal.sync()          │ │
│  │                        (group commit, fdatasync)           │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                  │
│                              ▼ periodic checkpoint              │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ wal.replay(last_ckpt..now) ──▶ rocksdb.write_batch()       │ │
│  │ rocksdb.create_checkpoint() ──▶ hard-link SSTables         │ │
│  │ wal.truncate(checkpoint_epoch)                             │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│ RECOVERY PATH                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Find latest valid checkpoint (metadata.json exists)         │
│  2. Open RocksDB from checkpoint directory                      │
│  3. Full scan RocksDB ──▶ rebuild mmap state store              │
│  4. Replay WAL entries after checkpoint.wal_position            │
│  5. Return source_offsets for upstream replay                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
/// Ring 0: Zero-alloc changelog entry (offset reference, not data copy).
///
/// This structure is 32 bytes and fits in half a cache line.
/// Uses offset references into mmap to avoid copying data.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ChangelogRef {
    /// Epoch when this change occurred
    pub epoch: u64,
    /// Hash of the key (for fast lookup)
    pub key_hash: u64,
    /// Offset into mmap where key-value is stored
    pub mmap_offset: usize,
    /// Length of the value
    pub value_len: u32,
    /// Operation type
    pub op: Op,
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Op {
    Put = 0,
    Delete = 1,
}

/// Ring 0: Lock-free SPSC changelog buffer.
///
/// Uses the same SPSC queue design from F014.
pub struct ChangelogBuffer {
    /// Ring buffer of changelog entries
    queue: SpscQueue<ChangelogRef>,
    /// Current epoch (atomic for lock-free access)
    current_epoch: AtomicU64,
}

impl ChangelogBuffer {
    /// Push a changelog entry (Ring 0, ~50ns).
    ///
    /// Zero-alloc: stores offset reference, not data copy.
    #[inline]
    pub fn push(&self, entry: ChangelogRef) -> Result<(), ChangelogFull> {
        self.queue.push(entry)
    }

    /// Drain all entries (Ring 1, background).
    pub fn drain(&self) -> impl Iterator<Item = ChangelogRef> {
        std::iter::from_fn(|| self.queue.pop())
    }

    /// Advance epoch (Ring 0, <10ns).
    #[inline]
    pub fn advance_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::Release)
    }
}
```

### Checkpoint Metadata

```rust
/// Checkpoint metadata (JSON for debuggability).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckpointMetadata {
    /// Monotonically increasing checkpoint ID
    pub id: u64,
    /// Epoch at checkpoint time
    pub epoch: u64,
    /// WAL byte position for replay starting point
    pub wal_position: u64,
    /// Source offsets for exactly-once replay
    /// Map: source_name -> partition -> offset
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,
    /// List of SSTable files in this checkpoint
    pub sst_files: Vec<String>,
    /// Watermark at checkpoint time
    pub watermark: Option<i64>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Total state size in bytes
    pub state_size_bytes: u64,
}

/// Recovery result returned after checkpoint + WAL replay.
pub struct RecoveredState {
    /// Rebuilt mmap state store
    pub state_store: MmapStateStore,
    /// Epoch to resume from
    pub epoch: u64,
    /// Source offsets for exactly-once replay
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,
    /// WAL position after recovery
    pub wal_position: u64,
    /// Restored watermark
    pub watermark: Option<i64>,
}
```

### Incremental Checkpoint Manager

```rust
/// Manages incremental checkpoints using RocksDB.
pub struct IncrementalCheckpointManager {
    /// Base directory for checkpoints
    checkpoint_dir: PathBuf,
    /// RocksDB instance for checkpoint state
    rocksdb: DB,
    /// WAL reader for replay
    wal: WriteAheadLog,
    /// Last checkpoint epoch
    last_epoch: u64,
    /// Maximum checkpoints to retain
    max_retained: usize,
}

impl IncrementalCheckpointManager {
    /// Create incremental checkpoint.
    ///
    /// 1. Replay WAL to RocksDB (incremental)
    /// 2. Create RocksDB checkpoint (hard-links, O(1))
    /// 3. Write metadata
    /// 4. Truncate WAL
    /// 5. Cleanup old checkpoints
    pub fn create_checkpoint(&mut self, epoch: u64) -> Result<CheckpointMetadata> {
        // 1. Replay WAL to RocksDB
        let entries = self.wal.read_range(self.last_epoch, epoch)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            match entry.operation {
                WalOperation::Put { key, value } => batch.put(&key, &value),
                WalOperation::Delete { key } => batch.delete(&key),
                _ => {}
            }
        }
        self.rocksdb.write(batch)?;

        // 2. Create RocksDB checkpoint (hard-links SSTables)
        let path = self.checkpoint_dir.join(format!("checkpoint-{:020}", epoch));
        let checkpoint = Checkpoint::new(&self.rocksdb)?;
        checkpoint.create_checkpoint(&path)?;

        // 3. Write metadata
        let metadata = CheckpointMetadata {
            id: epoch,
            epoch,
            wal_position: self.wal.position(),
            // ... other fields
        };
        fs::write(
            path.join("metadata.json"),
            serde_json::to_vec_pretty(&metadata)?,
        )?;

        // 4. Truncate WAL
        self.wal.truncate_before(epoch)?;

        // 5. Cleanup old checkpoints
        self.cleanup_old_checkpoints()?;

        self.last_epoch = epoch;
        Ok(metadata)
    }
}
```

### MmapStateStore Integration

```rust
impl MmapStateStore {
    /// Put with changelog tracking (Ring 0, <500ns).
    #[inline]
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        // Write to mmap (~100ns)
        let offset = self.mmap_write(key, value);

        // Update index (~50ns)
        let key_hash = fxhash::hash64(key);
        self.index.insert(key_hash, Entry { offset, len: value.len() });

        // Push to changelog (~50ns, zero-alloc)
        self.changelog.push(ChangelogRef {
            epoch: self.current_epoch.load(Relaxed),
            key_hash,
            mmap_offset: offset,
            value_len: value.len() as u32,
            op: Op::Put,
        });
    }
}
```

### WAL Flush (Ring 1)

```rust
impl WalWriter {
    /// Flush changelog to WAL (Ring 1, async).
    ///
    /// Reads from mmap using offset references, appends to WAL.
    pub fn flush_changelog(
        &mut self,
        changelog: &ChangelogBuffer,
        mmap: &Mmap,
    ) -> Result<u64> {
        for entry in changelog.drain() {
            let (key, value) = mmap.read_at(entry.mmap_offset, entry.value_len);
            self.append(WalOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
        }
        self.sync()?;  // group commit with fdatasync
        Ok(self.position)
    }
}
```

## Implementation Phases

### Phase 1: ChangelogBuffer (2-3 days)

1. Create `ChangelogBuffer` using existing `SpscQueue`
2. Define `ChangelogRef` struct (32 bytes)
3. Integrate with `MmapStateStore::put()`
4. Unit tests for changelog operations
5. Benchmark: changelog.push() < 50ns

### Phase 2: Async WAL Drain (2-3 days)

1. Create background drain task
2. Implement `WalWriter::flush_changelog()`
3. Coordinate with group commit
4. Tests for async drain
5. Verify Ring 0 not blocked

### Phase 3: RocksDB Integration (3-4 days)

1. Enable RocksDB feature (already optional dependency)
2. Implement `IncrementalCheckpointManager`
3. WAL replay to RocksDB write batch
4. Hard-linked checkpoint creation
5. Integration tests

### Phase 4: Recovery & Cleanup (2-3 days)

1. Implement `RecoveryManager` with RocksDB
2. Rebuild mmap from RocksDB scan
3. WAL truncation after checkpoint
4. Checkpoint retention/cleanup
5. End-to-end recovery tests

## Test Cases

```rust
#[test]
fn test_changelog_zero_alloc() {
    // Verify no heap allocations on put
    let mut store = MmapStateStore::new();
    let before = ALLOCATOR.allocated();
    store.put(b"key", b"value");
    let after = ALLOCATOR.allocated();
    assert_eq!(before, after, "put() should not allocate");
}

#[test]
fn test_incremental_checkpoint_size() {
    // Incremental should be smaller than full snapshot
    let mut manager = IncrementalCheckpointManager::new();

    // Create base checkpoint with 1M keys
    populate_state(1_000_000);
    let base = manager.create_checkpoint(0)?;

    // Modify 1% of keys
    modify_random_keys(10_000);
    let incr = manager.create_checkpoint(1)?;

    // Incremental should be ~1% of base size
    assert!(incr.state_size_bytes < base.state_size_bytes / 50);
}

#[test]
fn test_ring0_latency_during_checkpoint() {
    // Ring 0 operations should not be affected by checkpoint
    let runtime = ThreadPerCoreRuntime::new()?;

    // Start async checkpoint
    runtime.trigger_checkpoint();

    // Measure Ring 0 latency during checkpoint
    let start = Instant::now();
    for _ in 0..10_000 {
        runtime.put(b"key", b"value");
    }
    let elapsed = start.elapsed();

    // p99 should still be < 1μs
    assert!(elapsed.as_nanos() / 10_000 < 1000);
}

#[test]
fn test_recovery_from_checkpoint_plus_wal() {
    // Recovery should restore exact state
    let mut store = setup_store();

    // Create checkpoint at epoch 100
    store.checkpoint()?;

    // More mutations (will be in WAL)
    store.put(b"after_checkpoint", b"value");

    // Simulate crash and recover
    let recovered = RecoveryManager::recover()?;

    // Should have both checkpoint state and WAL mutations
    assert_eq!(recovered.get(b"after_checkpoint"), Some(b"value"));
}

#[test]
fn test_source_offset_tracking() {
    // Source offsets should enable exactly-once replay
    let mut store = setup_store();

    store.commit_offsets(hashmap! {
        "kafka-topic" => hashmap! { 0 => 1000, 1 => 2000 }
    });
    store.checkpoint()?;

    let recovered = RecoveryManager::recover()?;
    assert_eq!(
        recovered.source_offsets["kafka-topic"][&0],
        1000
    );
}

#[test]
fn test_wal_truncation() {
    // WAL should be truncated after successful checkpoint
    let mut manager = IncrementalCheckpointManager::new();

    // Write 1000 entries
    for i in 0..1000 {
        manager.wal.append(/* ... */)?;
    }
    let wal_size_before = manager.wal.size();

    // Create checkpoint
    manager.create_checkpoint(100)?;

    // WAL should be truncated
    let wal_size_after = manager.wal.size();
    assert!(wal_size_after < wal_size_before / 10);
}
```

## Acceptance Criteria

- [ ] ChangelogBuffer with zero-alloc push (<50ns)
- [ ] Async WAL drain (Ring 1, doesn't block Ring 0)
- [ ] RocksDB checkpoint with hard-linked SSTables
- [ ] Incremental size < 10% of full state for 1% changes
- [ ] WAL truncation after checkpoint
- [ ] Source offset tracking for exactly-once
- [ ] Recovery from checkpoint + WAL replay
- [ ] Ring 0 p99 < 1μs during checkpoint
- [ ] 10+ unit tests passing
- [ ] Integration test with F013 (thread-per-core)

## Performance Targets

| Operation | Ring | Target | Notes |
|-----------|------|--------|-------|
| `state.put()` + changelog | 0 | <500ns | Zero-alloc offset reference |
| `epoch.advance()` | 0 | <10ns | Atomic increment |
| `wal.sync()` (group commit) | 1 | <100μs | fdatasync, batched |
| `checkpoint.create()` | 1 | <500ms | Hard-linked SSTables |
| `recovery.full()` | 1 | <60s | For 10GB state |
| Incremental size | - | <10% | Of full state for 1% changes |

## Migration from F008

F008 (Basic Checkpointing) remains for:
- Simple use cases without RocksDB
- Testing and development
- Fallback if RocksDB unavailable

F022 adds:
- Zero-alloc changelog
- Async checkpoint (non-blocking)
- RocksDB incremental
- WAL truncation

## References

- [Checkpoint Implementation Prompt](../../research/checkpoint-implementation-prompt.md)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/)
- [RocksDB Checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints)
- [LMDB Copy-on-Write](https://www.symas.com/lmdb)
