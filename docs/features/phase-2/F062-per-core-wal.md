# F062: Per-Core WAL Segments

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F062 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F007, F013, F014 |
| **Owner** | TBD |
| **Research** | [Checkpoint Implementation Prompt](../../research/checkpoint-implementation-prompt.md) |

## Summary

Implement per-core WAL segments to support thread-per-core architecture (F013). Each core writes to its own WAL file without contention, and segments are merged during checkpoint for global recovery.

## Motivation

From research review and STEERING.md:

> "Per-core WAL segments - Required for F013 (thread-per-core)"

**Current Problem**:
- F007 implements a single WAL shared across all cores
- Thread-per-core (F013) routes events by key to dedicated cores
- Shared WAL creates contention when multiple cores write concurrently
- Single WAL write path violates Ring 0 lock-free constraint

**Solution**:
- Each core has its own WAL segment file
- No cross-core synchronization on write path
- Segments merged during checkpoint for unified recovery

## Goals

1. Eliminate WAL write contention in thread-per-core mode
2. Maintain exactly-once semantics across core boundaries
3. Support ordered recovery from multiple segments
4. Enable parallel WAL writes (one per core)
5. Checkpoint merges all segments coherently

## Non-Goals

- Distributed WAL across nodes (single-node only)
- Cross-core transactions (events are partitioned by key)
- Real-time WAL merge (only on checkpoint/recovery)

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Thread-Per-Core with Per-Core WAL            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Core 0     │  │   Core 1     │  │   Core 2     │          │
│  │              │  │              │  │              │          │
│  │  Changelog   │  │  Changelog   │  │  Changelog   │          │
│  │      │       │  │      │       │  │      │       │          │
│  │      ▼       │  │      ▼       │  │      ▼       │          │
│  │  wal-0.log   │  │  wal-1.log   │  │  wal-2.log   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│         │                 │                 │                   │
│         └─────────────────┼─────────────────┘                   │
│                           │                                     │
│                           ▼                                     │
│                  ┌─────────────────┐                            │
│                  │ Checkpoint      │                            │
│                  │ Coordinator     │                            │
│                  │                 │                            │
│                  │ Merge segments  │                            │
│                  │ by epoch order  │                            │
│                  └─────────────────┘                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
/// Per-core WAL configuration.
pub struct PerCoreWalConfig {
    /// Base directory for WAL segments
    pub base_dir: PathBuf,
    /// Number of cores (determines number of segments)
    pub num_cores: usize,
    /// Segment file name pattern: wal-{core_id}.log
    pub segment_pattern: String,
}

/// Per-core WAL writer (owned by single core, no locking).
pub struct CoreWalWriter {
    /// Core ID this writer belongs to
    core_id: usize,
    /// WAL file for this core
    file: BufWriter<File>,
    /// Current write position
    position: u64,
    /// Current epoch
    epoch: u64,
}

impl CoreWalWriter {
    /// Append entry to core's WAL segment (lock-free).
    #[inline]
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        // Same format as F007: [length: 4][crc32: 4][data: length]
        let data = rkyv::to_bytes::<_, 256>(entry)?;
        let crc = crc32c::crc32c(&data);

        self.file.write_all(&(data.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&data)?;

        self.position += 8 + data.len() as u64;
        Ok(self.position)
    }

    /// Sync this core's WAL segment.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;  // fdatasync
        Ok(())
    }
}

/// Coordinates per-core WAL segments.
pub struct PerCoreWalManager {
    /// Configuration
    config: PerCoreWalConfig,
    /// Per-core writers (index = core_id)
    writers: Vec<CoreWalWriter>,
    /// Global epoch counter (shared, atomic)
    global_epoch: Arc<AtomicU64>,
}

impl PerCoreWalManager {
    /// Get writer for specific core (no locking).
    #[inline]
    pub fn writer(&mut self, core_id: usize) -> &mut CoreWalWriter {
        &mut self.writers[core_id]
    }

    /// Advance global epoch (called during checkpoint).
    pub fn advance_epoch(&self) -> u64 {
        self.global_epoch.fetch_add(1, Ordering::SeqCst)
    }
}
```

### Epoch Ordering

```rust
/// WAL entry with epoch for cross-core ordering.
#[derive(Archive, Serialize, Deserialize)]
pub struct PerCoreWalEntry {
    /// Global epoch (for ordering during recovery)
    pub epoch: u64,
    /// Core-local sequence number
    pub sequence: u64,
    /// Core ID that wrote this entry
    pub core_id: u16,
    /// Timestamp in nanoseconds
    pub timestamp_ns: i64,
    /// The actual operation
    pub operation: WalOperation,
    /// CRC32C checksum
    pub checksum: u32,
}

impl Ord for PerCoreWalEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by epoch first, then by timestamp within epoch
        match self.epoch.cmp(&other.epoch) {
            Ordering::Equal => self.timestamp_ns.cmp(&other.timestamp_ns),
            other => other,
        }
    }
}
```

### Checkpoint Coordination

```rust
/// Coordinates checkpoint across all cores.
pub struct CheckpointCoordinator {
    /// Per-core WAL manager
    wal_manager: PerCoreWalManager,
    /// Checkpoint manager
    checkpoint_manager: IncrementalCheckpointManager,
}

impl CheckpointCoordinator {
    /// Create checkpoint by merging all core segments.
    pub async fn create_checkpoint(&mut self) -> Result<CheckpointMetadata> {
        let epoch = self.wal_manager.advance_epoch();

        // 1. Signal all cores to flush to their WAL segments
        self.signal_flush_all_cores(epoch).await?;

        // 2. Wait for all cores to complete flush
        self.await_flush_completion(epoch).await?;

        // 3. Merge WAL segments in epoch order
        let merged = self.merge_segments(epoch)?;

        // 4. Create incremental checkpoint
        let metadata = self.checkpoint_manager
            .create_checkpoint_from_merged(merged, epoch)?;

        // 5. Truncate all segments
        self.truncate_all_segments(epoch)?;

        Ok(metadata)
    }

    /// Merge WAL segments from all cores.
    fn merge_segments(&self, up_to_epoch: u64) -> Result<Vec<PerCoreWalEntry>> {
        let mut entries = Vec::new();

        // Read all segments
        for core_id in 0..self.wal_manager.config.num_cores {
            let segment_path = self.segment_path(core_id);
            let reader = PerCoreWalReader::open(&segment_path)?;

            for entry in reader.entries() {
                let entry = entry?;
                if entry.epoch <= up_to_epoch {
                    entries.push(entry);
                }
            }
        }

        // Sort by epoch, then timestamp for deterministic recovery
        entries.sort();

        Ok(entries)
    }
}
```

### Recovery

```rust
/// Recovery from per-core WAL segments.
pub struct PerCoreRecoveryManager {
    config: PerCoreWalConfig,
}

impl PerCoreRecoveryManager {
    /// Recover state from checkpoint + all WAL segments.
    pub fn recover(&self) -> Result<RecoveredState> {
        // 1. Load latest checkpoint
        let checkpoint = self.find_latest_checkpoint()?;

        // 2. Open RocksDB from checkpoint
        let db = DB::open_for_read_only(&opts, &checkpoint.path, true)?;

        // 3. Rebuild mmap store from RocksDB
        let mut store = MmapStateStore::new()?;
        for (k, v) in db.iterator(IteratorMode::Start) {
            store.put_raw(&k?, &v?)?;
        }

        // 4. Merge and replay WAL segments after checkpoint
        let mut entries = Vec::new();
        for core_id in 0..self.config.num_cores {
            let reader = PerCoreWalReader::open(self.segment_path(core_id))?;
            for entry in reader.entries_from(checkpoint.wal_positions[core_id]) {
                entries.push(entry?);
            }
        }
        entries.sort();  // Order by epoch, then timestamp

        // 5. Apply merged entries
        for entry in entries {
            match entry.operation {
                WalOperation::Put { key, value } => store.put_raw(&key, &value)?,
                WalOperation::Delete { key } => store.delete_raw(&key)?,
                _ => {}
            }
        }

        Ok(RecoveredState {
            state_store: store,
            epoch: entries.last().map(|e| e.epoch).unwrap_or(checkpoint.epoch),
            source_offsets: checkpoint.source_offsets.clone(),
            ..
        })
    }
}
```

### Integration with F013 (Thread-Per-Core)

```rust
impl CoreHandle {
    /// Create with per-core WAL writer.
    pub fn new_with_wal(
        core_id: usize,
        config: CoreConfig,
        wal_writer: CoreWalWriter,
    ) -> Self {
        Self {
            core_id,
            config,
            wal_writer: Some(wal_writer),
            changelog: ChangelogBuffer::new(config.changelog_size),
            ..
        }
    }

    /// Flush changelog to this core's WAL (Ring 1, called periodically).
    pub fn flush_changelog(&mut self) -> Result<u64> {
        if let Some(ref mut wal) = self.wal_writer {
            for entry in self.changelog.drain() {
                let (key, value) = self.mmap.read_at(entry.mmap_offset, entry.value_len);
                wal.append(&WalEntry::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                })?;
            }
            wal.sync()?;
            Ok(wal.position)
        } else {
            Ok(0)
        }
    }
}
```

## Implementation Phases

### Phase 1: Core WAL Writer (1-2 days)

1. Create `CoreWalWriter` struct
2. Segment file creation/opening
3. Same record format as F007 (compatible)
4. Unit tests for single-core writes

### Phase 2: PerCoreWalManager (1-2 days)

1. Manage multiple `CoreWalWriter` instances
2. Global epoch counter
3. Segment path management
4. Integration with `ThreadPerCoreRuntime`

### Phase 3: Checkpoint Coordination (1-2 days)

1. Implement `CheckpointCoordinator`
2. Epoch-based flush signaling
3. Segment merging with ordering
4. Integration with F022 incremental checkpoints

### Phase 4: Recovery (1 day)

1. Multi-segment WAL reading
2. Merge and order by epoch
3. Per-core position tracking in checkpoint metadata
4. End-to-end recovery tests

## Test Cases

```rust
#[test]
fn test_per_core_wal_no_contention() {
    // Parallel writes to different cores should not block
    let manager = PerCoreWalManager::new(4);

    let handles: Vec<_> = (0..4).map(|core_id| {
        thread::spawn(move || {
            for i in 0..10_000 {
                manager.writer(core_id).append(&entry)?;
            }
        })
    }).collect();

    // All should complete without contention
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_segment_merge_ordering() {
    // Entries should be ordered by epoch after merge
    let mut entries = vec![
        PerCoreWalEntry { epoch: 2, core_id: 0, .. },
        PerCoreWalEntry { epoch: 1, core_id: 1, .. },
        PerCoreWalEntry { epoch: 2, core_id: 1, .. },
        PerCoreWalEntry { epoch: 1, core_id: 0, .. },
    ];
    entries.sort();

    assert_eq!(entries[0].epoch, 1);
    assert_eq!(entries[1].epoch, 1);
    assert_eq!(entries[2].epoch, 2);
    assert_eq!(entries[3].epoch, 2);
}

#[test]
fn test_recovery_from_multiple_segments() {
    // Recovery should merge all segments correctly
    let manager = setup_per_core_wal(4);

    // Write to different cores
    manager.writer(0).append(&entry_a)?;
    manager.writer(1).append(&entry_b)?;
    manager.writer(2).append(&entry_c)?;

    // Create checkpoint
    let checkpoint = manager.create_checkpoint()?;

    // Simulate crash and recover
    let recovered = PerCoreRecoveryManager::recover()?;

    // All entries should be present
    assert!(recovered.contains(&entry_a));
    assert!(recovered.contains(&entry_b));
    assert!(recovered.contains(&entry_c));
}

#[test]
fn test_checkpoint_truncates_all_segments() {
    // Checkpoint should truncate all segment files
    let manager = setup_per_core_wal(4);

    // Write many entries
    for core_id in 0..4 {
        for _ in 0..1000 {
            manager.writer(core_id).append(&entry)?;
        }
    }

    let sizes_before: Vec<_> = (0..4)
        .map(|i| fs::metadata(manager.segment_path(i)).unwrap().len())
        .collect();

    // Create checkpoint
    manager.create_checkpoint()?;

    let sizes_after: Vec<_> = (0..4)
        .map(|i| fs::metadata(manager.segment_path(i)).unwrap().len())
        .collect();

    // All segments should be smaller
    for (before, after) in sizes_before.iter().zip(sizes_after.iter()) {
        assert!(after < before);
    }
}
```

## Acceptance Criteria

- [ ] Per-core WAL writers (no cross-core locking)
- [ ] Epoch-based ordering for deterministic recovery
- [ ] Segment merge during checkpoint
- [ ] Per-core position tracking in checkpoint metadata
- [ ] Recovery from multiple segments
- [ ] WAL truncation of all segments after checkpoint
- [ ] Integration with F013 ThreadPerCoreRuntime
- [ ] Integration with F022 IncrementalCheckpointManager
- [ ] 8+ unit tests passing
- [ ] No contention benchmarks (4+ cores)

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Per-core WAL append | <100ns | No locking, direct write |
| Segment merge | O(n log n) | Sort by epoch |
| Checkpoint coordination | <10ms | Parallel segment flush |
| Recovery (4 segments) | <70s | For 10GB state |

## References

- [Checkpoint Implementation Prompt](../../research/checkpoint-implementation-prompt.md)
- [F007: Write-Ahead Log](../phase-1/F007-write-ahead-log.md)
- [F013: Thread-Per-Core Architecture](F013-thread-per-core.md)
- [F022: Incremental Checkpointing](F022-incremental-checkpointing.md)
