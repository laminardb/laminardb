# F-STREAM-013: Checkpointing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-013 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-STREAM-001, F-STREAM-004, F022 (Incremental Checkpointing) |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

Checkpointing for the streaming API. **OPTIONAL by default** - disabled unless explicitly enabled. Zero overhead when disabled, <1% overhead when enabled with async mode.

**Key Design Principle**: Checkpointing is opt-in. Pure in-memory streaming has zero durability overhead.

## Goals

- Zero overhead when disabled (default)
- <1% overhead with async checkpointing
- Non-blocking Ring 0 operation
- Consistent snapshots across sources/sinks
- Integration with existing F022 infrastructure

## Non-Goals

- Synchronous checkpointing on Ring 0 (blocks hot path)
- Transactional semantics (covered by F023)
- Distributed checkpointing (Phase 4+)

## Technical Design

### Architecture

**Ring**: Ring 1 (Background), triggered from Ring 2
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/checkpoint.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpointing Architecture                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Configuration:                                                  │
│  checkpoint_interval: None    → Pure in-memory (0% overhead)    │
│  checkpoint_interval: 10s     → Async snapshots (<1% overhead)  │
│  checkpoint_interval: 10s     → + WAL (1-2% overhead)           │
│       + wal_mode: Async                                         │
│                                                                  │
│  ═══════════════════════════════════════════════════════════   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                     RING 0: HOT PATH                     │   │
│  │                                                          │   │
│  │  Source ──► Operators ──► State ──► Sink                │   │
│  │     │                       │                            │   │
│  │     │ (if checkpoint enabled)                            │   │
│  │     ▼                       ▼                            │   │
│  │  ┌─────────────────────────────────────────────┐        │   │
│  │  │  Changelog Buffer (zero-alloc references)   │        │   │
│  │  │  Appends offset/delta info, never blocks    │        │   │
│  │  └─────────────────────────────────────────────┘        │   │
│  │                         │                                │   │
│  └─────────────────────────┼────────────────────────────────┘   │
│                            │ SPSC drain                         │
│                            ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   RING 1: BACKGROUND                     │   │
│  │                                                          │   │
│  │  ┌───────────────────────────────────────────────┐      │   │
│  │  │            Checkpoint Manager                  │      │   │
│  │  │                                                │      │   │
│  │  │  • Async snapshot creation                     │      │   │
│  │  │  • WAL writing (if enabled)                    │      │   │
│  │  │  • RocksDB state persistence                   │      │   │
│  │  │  • No Ring 0 blocking                          │      │   │
│  │  └───────────────────────────────────────────────┘      │   │
│  │                                                          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                    │
│                            ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   RING 2: CONTROL                        │   │
│  │                                                          │   │
│  │  Timer triggers checkpoint  ──► Manual db.checkpoint()   │   │
│  │                                                          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::time::Duration;
use std::path::PathBuf;

/// Checkpointing is OPTIONAL - disabled by default.
///
/// Enable via configuration:
/// ```rust
/// Config {
///     checkpoint_interval: Some(Duration::from_secs(10)),
///     wal_mode: Some(WalMode::Async),
///     data_dir: Some("/var/lib/laminar".into()),
///     ..Default::default()
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Checkpoint interval. None = disabled (default)
    pub interval: Option<Duration>,

    /// WAL mode. None = disabled
    pub wal_mode: Option<WalMode>,

    /// Data directory (required if checkpointing enabled)
    pub data_dir: Option<PathBuf>,

    /// Maximum checkpoint size before compaction
    pub max_checkpoint_size: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: None,           // Disabled by default!
            wal_mode: None,           // Disabled by default!
            data_dir: None,
            max_checkpoint_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// WAL mode options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    /// Background flush (lower latency, small data loss window)
    Async,
    /// Synchronous flush (higher latency, no data loss)
    Sync,
}

/// Checkpoint contents.
#[derive(Debug)]
pub struct Checkpoint {
    /// Unique checkpoint ID
    pub id: u64,

    /// Epoch/timestamp when checkpoint was taken
    pub epoch: i64,

    /// Source sequence numbers at checkpoint time
    pub source_sequences: HashMap<String, u64>,

    /// Sink acknowledgment positions
    pub sink_positions: HashMap<String, u64>,

    /// Operator state snapshots
    pub operator_states: HashMap<String, OperatorSnapshot>,

    /// Watermark values
    pub watermarks: HashMap<String, i64>,
}

/// Operator state snapshot.
#[derive(Debug)]
pub struct OperatorSnapshot {
    /// Operator ID
    pub operator_id: String,

    /// State type (window, aggregation, join, etc.)
    pub state_type: OperatorStateType,

    /// Serialized state data (RocksDB SST reference or inline)
    pub data: SnapshotData,
}

/// Snapshot data storage.
#[derive(Debug)]
pub enum SnapshotData {
    /// Small state, stored inline
    Inline(Vec<u8>),
    /// Large state, stored in RocksDB SST file
    SstFile(PathBuf),
}

/// Checkpoint manager for async checkpoint coordination.
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,

    /// Last completed checkpoint
    last_checkpoint: AtomicU64,

    /// In-progress checkpoint (if any)
    in_progress: RwLock<Option<InProgressCheckpoint>>,

    /// RocksDB for state persistence
    state_store: Arc<RocksDbStore>,

    /// WAL writer (if enabled)
    wal: Option<WalWriter>,
}

/// Changelog buffer for Ring 0 (zero-allocation on hot path).
///
/// This is a bounded buffer that Ring 0 appends to without blocking.
/// Ring 1 drains this buffer asynchronously.
pub struct ChangelogBuffer {
    /// Pre-allocated buffer of change records
    buffer: RingBuffer<ChangeRecord>,

    /// Overflow policy
    overflow_policy: OverflowPolicy,
}

/// A change record (minimal allocation).
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    /// Source of the change
    pub source: u16,  // Index, not String

    /// Sequence number
    pub sequence: u64,

    /// Operation type
    pub op_type: OpType,

    /// Key (reference to shared key buffer)
    pub key_offset: u32,
    pub key_len: u16,

    /// Value (reference to shared value buffer)
    pub value_offset: u32,
    pub value_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum OpType {
    Insert,
    Update,
    Delete,
}
```

### API/Interface

```rust
impl CheckpointManager {
    /// Create a new checkpoint manager.
    ///
    /// # Validation
    ///
    /// Returns error if checkpointing is enabled but data_dir is None.
    pub fn new(config: CheckpointConfig) -> Result<Self, CheckpointError> {
        config.validate()?;

        let state_store = if config.interval.is_some() {
            Some(Arc::new(RocksDbStore::open(&config.data_dir.as_ref().unwrap())?))
        } else {
            None
        };

        let wal = config.wal_mode.map(|mode| {
            WalWriter::new(&config.data_dir.as_ref().unwrap(), mode)
        }).transpose()?;

        Ok(Self {
            config,
            last_checkpoint: AtomicU64::new(0),
            in_progress: RwLock::new(None),
            state_store: state_store.unwrap_or_else(|| Arc::new(RocksDbStore::noop())),
            wal,
        })
    }

    /// Trigger a checkpoint (non-blocking from Ring 0 perspective).
    ///
    /// This initiates a checkpoint sequence:
    /// 1. Snapshot current sequences
    /// 2. Drain changelog buffer
    /// 3. Write to RocksDB
    /// 4. Update last_checkpoint
    ///
    /// Returns checkpoint ID or None if checkpointing is disabled.
    pub fn trigger(&self) -> Option<u64> {
        if self.config.interval.is_none() {
            return None;
        }

        let checkpoint_id = self.last_checkpoint.load(Ordering::Acquire) + 1;

        // Spawn async checkpoint task on Ring 1
        self.spawn_checkpoint_task(checkpoint_id);

        Some(checkpoint_id)
    }

    /// Manual checkpoint (called by user or timer).
    pub fn checkpoint(&self) -> Result<Option<u64>, CheckpointError> {
        if self.config.interval.is_none() {
            return Ok(None);
        }

        let checkpoint_id = self.trigger()
            .ok_or(CheckpointError::Disabled)?;

        // Wait for completion
        self.wait_for_checkpoint(checkpoint_id)?;

        Ok(Some(checkpoint_id))
    }

    /// Restore from latest checkpoint.
    pub fn restore(&self) -> Result<Checkpoint, CheckpointError> {
        if self.config.interval.is_none() {
            return Err(CheckpointError::Disabled);
        }

        let checkpoint_id = self.last_checkpoint.load(Ordering::Acquire);
        if checkpoint_id == 0 {
            return Err(CheckpointError::NoCheckpoint);
        }

        self.load_checkpoint(checkpoint_id)
    }

    /// Get last completed checkpoint ID.
    pub fn last_checkpoint_id(&self) -> u64 {
        self.last_checkpoint.load(Ordering::Acquire)
    }

    /// Check if checkpointing is enabled.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.config.interval.is_some()
    }

    fn spawn_checkpoint_task(&self, checkpoint_id: u64) {
        // Implementation spawns on Ring 1 thread pool
        // Does NOT block Ring 0
    }

    fn wait_for_checkpoint(&self, checkpoint_id: u64) -> Result<(), CheckpointError> {
        // Wait with timeout
        let timeout = Duration::from_secs(60);
        let start = std::time::Instant::now();

        loop {
            if self.last_checkpoint.load(Ordering::Acquire) >= checkpoint_id {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(CheckpointError::Timeout);
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn load_checkpoint(&self, id: u64) -> Result<Checkpoint, CheckpointError> {
        // Load from RocksDB
        self.state_store.load_checkpoint(id)
    }
}

impl CheckpointConfig {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        // data_dir required if checkpointing enabled
        if self.interval.is_some() && self.data_dir.is_none() {
            return Err(CheckpointError::DataDirRequired);
        }

        // WAL requires checkpointing
        if self.wal_mode.is_some() && self.interval.is_none() {
            return Err(CheckpointError::WalRequiresCheckpoint);
        }

        Ok(())
    }
}
```

### LaminarDB Integration

```rust
impl LaminarDB {
    /// Manual checkpoint (no-op if disabled).
    ///
    /// # Example
    ///
    /// ```rust
    /// // With checkpointing enabled:
    /// let config = Config {
    ///     checkpoint_interval: Some(Duration::from_secs(10)),
    ///     data_dir: Some("/var/lib/laminar".into()),
    ///     ..Default::default()
    /// };
    /// let db = LaminarDB::open_with_config(config)?;
    ///
    /// // Manual checkpoint
    /// if let Some(id) = db.checkpoint()? {
    ///     println!("Checkpoint {} complete", id);
    /// }
    /// ```
    pub fn checkpoint(&self) -> Result<Option<u64>, Error> {
        self.checkpoint_manager.checkpoint()
            .map_err(Error::Checkpoint)
    }

    /// Restore from latest checkpoint.
    ///
    /// Called automatically on startup if checkpointing is enabled.
    pub fn restore(&self) -> Result<(), Error> {
        if !self.checkpoint_manager.is_enabled() {
            return Ok(());
        }

        let checkpoint = self.checkpoint_manager.restore()?;
        self.apply_checkpoint(checkpoint)?;
        Ok(())
    }
}
```

### Overhead Analysis

| Configuration | Hot Path Overhead | Storage | Recovery Time |
|---------------|-------------------|---------|---------------|
| `checkpoint_interval: None` | **0%** | None | N/A |
| `checkpoint_interval: Some(10s)` | <1% | Snapshots | Fast |
| `+ wal_mode: Async` | 1-2% | Snapshots + WAL | Very fast |
| `+ wal_mode: Sync` | 5-10% | Snapshots + WAL | Very fast |

### Recovery Flow

```
Startup with Checkpoint Enabled:
1. Find latest checkpoint in data_dir
2. Load checkpoint metadata
3. Restore source sequences
4. Restore operator states
5. Restore watermarks
6. If WAL exists: replay from checkpoint epoch
7. Resume processing
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `CheckpointError::Disabled` | Checkpoint called but disabled | None needed |
| `CheckpointError::DataDirRequired` | No data_dir configured | Set data_dir |
| `CheckpointError::WalRequiresCheckpoint` | WAL without checkpoint | Enable checkpoint first |
| `CheckpointError::Timeout` | Checkpoint took too long | Retry or investigate |
| `CheckpointError::NoCheckpoint` | Restore with no checkpoints | Start fresh |

## Test Plan

### Unit Tests

- [ ] `test_checkpoint_disabled_by_default`
- [ ] `test_checkpoint_no_op_when_disabled`
- [ ] `test_checkpoint_requires_data_dir`
- [ ] `test_wal_requires_checkpoint`
- [ ] `test_trigger_checkpoint`
- [ ] `test_manual_checkpoint`
- [ ] `test_restore_from_checkpoint`
- [ ] `test_checkpoint_contains_sequences`
- [ ] `test_checkpoint_contains_watermarks`

### Integration Tests

- [ ] Full cycle: push → checkpoint → shutdown → restore → verify
- [ ] Checkpoint during active processing
- [ ] Recovery with WAL replay

### Benchmarks

- [ ] `bench_hot_path_no_checkpoint` - Baseline
- [ ] `bench_hot_path_with_checkpoint` - Target: <1% overhead
- [ ] `bench_hot_path_with_wal_async` - Target: <2% overhead
- [ ] `bench_recovery_time` - Target: <10s for 1GB state

## Rollout Plan

1. **Phase 1**: Configuration and validation
2. **Phase 2**: Changelog buffer (Ring 0)
3. **Phase 3**: Checkpoint manager (Ring 1)
4. **Phase 4**: Recovery flow
5. **Phase 5**: Integration tests + benchmarks

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks confirm <1% overhead
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] Merged to main

---

## Notes

**Default Disabled**: Checkpointing is explicitly opt-in. The default configuration has zero durability overhead, optimizing for pure in-memory streaming use cases.

**Three-Ring Separation**: Ring 0 only appends to the changelog buffer (zero-alloc). Ring 1 handles actual persistence. Ring 2 triggers checkpoints.

## References

- [F022: Incremental Checkpointing](../../phase-2/F022-incremental-checkpointing.md)
- [F007: Write-Ahead Log](../../phase-1/F007-write-ahead-log.md)
- [ADR-004: Checkpoint Strategy](../../../adr/ADR-004-checkpoint-strategy.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
