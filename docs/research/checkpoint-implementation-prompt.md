# LaminarDB Checkpointing Implementation

## When to Use This

Run this prompt in Claude Code **after** you have implemented:
- [ ] F001: Core Reactor
- [ ] F002: mmap State Store  
- [ ] F003: State Store Interface
- [ ] F004: WAL Writer

## Research Summary (January 2026)

### Best Practices from Industry

1. **Asynchronous Barrier Snapshotting (ABS)** - Flink/RisingWave approach, Chandy-Lamport variant
2. **Generic Log-based Incremental Checkpoints (GIC)** - Flink 1.16+, decouples materialization from checkpoint timing
3. **Unaligned Checkpoints** - Barriers overtake in-flight data, eliminates backpressure impact
4. **Copy-on-Write mmap** - LMDB approach, zero hot-path overhead
5. **RocksDB Incremental Checkpoints** - Hard-linked SSTables, reference counting

### Recommended Architecture for LaminarDB

**Three-tier hybrid** optimized for sub-500ns Ring 0:

```
Ring 0: mmap State Store ──▶ Changelog Buffer (lock-free, zero-alloc)
                │
                ▼ async drain
Ring 1: WAL Writer ──▶ RocksDB ──▶ Incremental Checkpoint
                │
                ▼ periodic
Ring 2: Object Storage (disaster recovery)
```

### Core Invariant

**Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State**

### Why WAL + Checkpoint (not just one)?

| Approach | Durability | Recovery Time | Space |
|----------|------------|---------------|-------|
| WAL only | ✅ Per-commit | ❌ O(all events) | ❌ Unbounded |
| Checkpoint only | ❌ Periodic loss | ✅ O(state) | ✅ Bounded |
| **WAL + Checkpoint** | ✅ Per-commit | ✅ O(state + delta) | ✅ WAL truncated |

---

## Claude Code Prompt

Copy everything below this line into your Claude Code session:

---

Implement checkpointing for LaminarDB. Generate feature specs, ADRs, skills, and code following the existing project conventions.

### Context

LaminarDB uses:
- Thread-per-core with Ring 0 (<500ns), Ring 1 (background), Ring 2 (control)
- mmap state store in Ring 0
- WAL in Ring 1 with group commit
- RocksDB in Ring 1 for checkpoints
- rkyv for zero-copy serialization

### Requirements

**Functional:**
- Checkpoint must add <10ns to Ring 0 processing (epoch increment only)
- Incremental checkpointing via WAL replay to RocksDB
- Configurable intervals (100ms - 10min)
- Exact state recovery at checkpoint epoch
- WAL truncation after checkpoint
- N retained checkpoints with cleanup
- Source offset tracking for exactly-once replay

**Non-Functional:**
- Ring 0 p99 <1μs during checkpoint
- Recovery <60s for 10GB state
- Incremental size <10% of full state

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
│  │                        (group commit, fsync)               │ │
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
// Ring 0: Zero-alloc changelog entry (offset reference, not data copy)
#[derive(Clone, Copy)]
pub struct ChangelogRef {
    pub epoch: u64,
    pub key_hash: u64,
    pub mmap_offset: usize,
    pub value_len: u32,
    pub op: Op,
}

#[derive(Clone, Copy, Debug)]
pub enum Op { Put, Delete }

// Ring 1: WAL entry (rkyv serialized)
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp_ns: i64,
    pub operation: WalOperation,
    pub checksum: u32,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum WalOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    EpochComplete { epoch: u64 },
    CheckpointStart { checkpoint_id: u64 },
    CheckpointComplete { checkpoint_id: u64, wal_position: u64 },
    SourceOffset { source_id: u32, partition: u32, offset: u64 },
}

// Checkpoint metadata (JSON for debuggability)
#[derive(Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub epoch: u64,
    pub wal_position: u64,
    pub source_offsets: HashMap<String, HashMap<u32, u64>>, // source -> partition -> offset
    pub sst_files: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub state_size_bytes: u64,
}

// Recovery result
pub struct RecoveredState {
    pub state_store: MmapStateStore,
    pub epoch: u64,
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,
    pub wal_position: u64,
}
```

### Components

| Component | Location | Ring | Purpose |
|-----------|----------|------|---------|
| `ChangelogBuffer` | `laminar-storage/src/changelog/` | 0 | Lock-free SPSC ring buffer |
| `WalWriter` | `laminar-storage/src/wal/writer.rs` | 1 | Append + group commit |
| `WalReader` | `laminar-storage/src/wal/reader.rs` | 1 | Sequential + range reads |
| `CheckpointManager` | `laminar-storage/src/checkpoint/manager.rs` | 1 | Orchestration |
| `RecoveryManager` | `laminar-storage/src/checkpoint/recovery.rs` | 1 | Startup recovery |

### Key Implementation Details

**1. Changelog (Ring 0, zero-alloc):**
```rust
impl MmapStateStore {
    #[inline]
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let offset = self.mmap_write(key, value);  // ~100ns
        let key_hash = fxhash::hash64(key);        // ~20ns
        self.index.insert(key_hash, Entry { offset, len: value.len() });
        
        // Zero-alloc: push offset reference, not data
        self.changelog.push(ChangelogRef {
            epoch: self.current_epoch.load(Relaxed),
            key_hash,
            mmap_offset: offset,
            value_len: value.len() as u32,
            op: Op::Put,
        });  // ~50ns
    }
}
```

**2. WAL Flush (Ring 1, async):**
```rust
impl WalWriter {
    pub fn flush_changelog(&mut self, changelog: &ChangelogBuffer, mmap: &Mmap) -> Result<u64> {
        for entry in changelog.drain() {
            let (key, value) = mmap.read_at(entry.mmap_offset, entry.value_len);
            self.append(WalOperation::Put { key: key.to_vec(), value: value.to_vec() })?;
        }
        self.sync()?;  // group commit
        Ok(self.position)
    }
}
```

**3. Checkpoint (Ring 1, periodic):**
```rust
impl CheckpointManager {
    pub fn create_checkpoint(&mut self, epoch: u64) -> Result<CheckpointMetadata> {
        // 1. Replay WAL to RocksDB
        let entries = self.wal.read_range(self.last_epoch, epoch)?;
        let mut batch = WriteBatch::default();
        for e in entries {
            match e.operation {
                WalOperation::Put { key, value } => batch.put(&key, &value),
                WalOperation::Delete { key } => batch.delete(&key),
                _ => {}
            }
        }
        self.rocksdb.write(batch)?;
        
        // 2. Create RocksDB checkpoint (hard-links)
        let path = self.checkpoint_dir.join(format!("checkpoint-{}", epoch));
        Checkpoint::new(&self.rocksdb)?.create_checkpoint(&path)?;
        
        // 3. Write metadata
        let metadata = CheckpointMetadata { epoch, wal_position: self.wal.position(), ... };
        fs::write(path.join("metadata.json"), serde_json::to_vec_pretty(&metadata)?)?;
        
        // 4. Truncate WAL
        self.wal.truncate_before(epoch)?;
        
        // 5. Cleanup old checkpoints
        self.cleanup_old_checkpoints()?;
        
        Ok(metadata)
    }
}
```

**4. Recovery:**
```rust
impl RecoveryManager {
    pub fn recover(&self) -> Result<RecoveredState> {
        let checkpoint = self.find_latest_checkpoint()?;
        
        // Load from RocksDB checkpoint
        let db = DB::open_for_read_only(&opts, &checkpoint.path, true)?;
        
        // Rebuild mmap store
        let mut store = MmapStateStore::new()?;
        for (k, v) in db.iterator(IteratorMode::Start) {
            store.put_raw(&k?, &v?);
        }
        
        // Replay WAL after checkpoint
        for entry in self.wal.read_from(checkpoint.wal_position)? {
            match entry.operation {
                WalOperation::Put { key, value } => store.put_raw(&key, &value),
                WalOperation::Delete { key } => store.delete_raw(&key),
                _ => {}
            }
        }
        
        store.set_epoch(checkpoint.epoch + 1);
        Ok(RecoveredState { state_store: store, epoch: checkpoint.epoch, ... })
    }
}
```

### Latency Targets

| Operation | Ring | Target |
|-----------|------|--------|
| `state.put()` including changelog | 0 | <500ns |
| `epoch.advance()` | 0 | <10ns |
| `wal.sync()` (group commit) | 1 | <100μs |
| `checkpoint.create()` | 1 | <500ms |
| `recovery.full()` | 1 | <60s |

### Testing Requirements

```rust
#[test] fn test_checkpoint_recovery_correctness();      // State matches after recovery
#[test] fn test_wal_truncation_after_checkpoint();      // WAL bounded
#[test] fn test_ring0_latency_during_checkpoint();      // p99 <1μs
#[test] fn test_incremental_checkpoint_efficiency();    // SSTable reuse
#[test] fn test_recovery_from_partial_checkpoint();     // Crash during checkpoint
#[test] fn test_source_offset_tracking();               // Exactly-once replay
```

### Generate These Artifacts

1. **Feature Spec:** `docs/features/phase-1/F052-state-checkpointing.md`
2. **ADR:** `docs/adr/ADR-XXX-checkpoint-strategy.md` (WAL + incremental RocksDB vs alternatives)
3. **Code:** Module structure in `crates/laminar-storage/src/checkpoint/` with traits, structs, tests
4. **Integration:** Changelog hooks in `crates/laminar-core/src/state/mmap.rs`

First, examine existing feature specs and code style, then generate consistent artifacts.
