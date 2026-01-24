---
name: checkpoint-recovery
description: WAL, checkpointing, and recovery patterns for exactly-once semantics in LaminarDB. Use when implementing durability, state snapshots, or recovery mechanisms.
---

# Checkpoint & Recovery Skill

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpoint & Recovery                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Events ──▶ [Operator] ──▶ [State Store] ──▶ [Sink]           │
│                  │               │                              │
│                  │               ▼                              │
│                  │         ┌─────────┐                          │
│                  │         │   WAL   │ ◀── Append-only log      │
│                  │         └────┬────┘                          │
│                  │              │                               │
│                  │              ▼                               │
│                  │    ┌──────────────────┐                      │
│                  └──▶ │   Checkpoint     │ ◀── Periodic snapshot│
│                       │  ┌────────────┐  │                      │
│                       │  │   State    │  │                      │
│                       │  │  Snapshot  │  │                      │
│                       │  └────────────┘  │                      │
│                       │  ┌────────────┐  │                      │
│                       │  │   Source   │  │                      │
│                       │  │  Offsets   │  │                      │
│                       │  └────────────┘  │                      │
│                       └──────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Write-Ahead Log (WAL)

```rust
use std::io::{BufWriter, Write};
use std::fs::{File, OpenOptions};
use rkyv::{Archive, Deserialize, Serialize, rancor::Error};
use rkyv::util::AlignedVec;

pub struct WriteAheadLog {
    writer: BufWriter<File>,
    sync_interval: Duration,
    last_sync: Instant,
    position: u64,
}

/// WAL entry with rkyv for zero-copy deserialization during recovery.
#[derive(Archive, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: u64,
    pub operation: WalOperation,
    pub checksum: u32,
}

#[derive(Archive, Serialize, Deserialize)]
pub enum WalOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Checkpoint { id: u64, position: u64 },
    Commit { source_offsets: HashMap<String, u64> },
}

impl WriteAheadLog {
    pub fn append(&mut self, op: WalOperation) -> Result<u64, WalError> {
        let entry = WalEntry {
            sequence: self.next_sequence(),
            timestamp: current_time_ms(),
            operation: op,
            checksum: 0,  // Calculated below
        };

        // Use rkyv for zero-copy serialization
        let bytes: AlignedVec = rkyv::to_bytes::<Error>(&entry)?;
        let checksum = crc32fast::hash(&bytes);

        // Write length-prefixed entry
        self.writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.write_all(&checksum.to_le_bytes())?;

        // Group commit for efficiency
        if self.last_sync.elapsed() >= self.sync_interval {
            self.writer.flush()?;
            self.writer.get_ref().sync_data()?;
            self.last_sync = Instant::now();
        }

        self.position += bytes.len() as u64 + 8;
        Ok(self.position)
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}
```

## Checkpointing

```rust
use rkyv::{Archive, Deserialize, Serialize};

pub struct CheckpointManager {
    state_store: Arc<StateStore>,
    wal: Arc<Mutex<WriteAheadLog>>,
    checkpoint_interval: Duration,
    checkpoint_dir: PathBuf,
}

/// Checkpoint metadata with rkyv for fast serialization.
#[derive(Archive, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: u64,
    pub timestamp: u64,
    pub wal_position: u64,
    pub source_offsets: HashMap<String, u64>,
    pub state_snapshot_path: PathBuf,
}

impl CheckpointManager {
    pub async fn create_checkpoint(&self) -> Result<Checkpoint, CheckpointError> {
        let checkpoint_id = self.next_checkpoint_id();
        
        // 1. Pause processing (or use copy-on-write)
        let snapshot = self.state_store.create_snapshot()?;
        
        // 2. Get current WAL position
        let wal_position = self.wal.lock().await.position();
        
        // 3. Get source offsets
        let source_offsets = self.get_source_offsets().await?;
        
        // 4. Write state snapshot
        let snapshot_path = self.checkpoint_dir.join(format!("checkpoint-{}", checkpoint_id));
        snapshot.write_to_file(&snapshot_path)?;
        
        // 5. Write checkpoint metadata
        let checkpoint = Checkpoint {
            id: checkpoint_id,
            timestamp: current_time_ms(),
            wal_position,
            source_offsets,
            state_snapshot_path: snapshot_path,
        };
        
        let meta_path = self.checkpoint_dir.join(format!("checkpoint-{}.meta", checkpoint_id));
        std::fs::write(&meta_path, serde_json::to_vec(&checkpoint)?)?;
        
        // 6. Mark checkpoint in WAL
        self.wal.lock().await.append(WalOperation::Checkpoint {
            id: checkpoint_id,
            position: wal_position,
        })?;
        
        // 7. Clean up old checkpoints
        self.cleanup_old_checkpoints(checkpoint_id)?;
        
        Ok(checkpoint)
    }
}
```

## Recovery

```rust
pub struct RecoveryManager {
    checkpoint_dir: PathBuf,
    wal_dir: PathBuf,
}

impl RecoveryManager {
    pub async fn recover(&self) -> Result<RecoveredState, RecoveryError> {
        // 1. Find latest valid checkpoint
        let checkpoint = self.find_latest_checkpoint()?;
        
        // 2. Load state snapshot
        let mut state = StateStore::load_from_file(&checkpoint.state_snapshot_path)?;
        
        // 3. Replay WAL entries after checkpoint
        let wal_reader = WalReader::open(&self.wal_dir)?;
        let entries = wal_reader.read_from(checkpoint.wal_position)?;
        
        for entry in entries {
            match entry.operation {
                WalOperation::Put { key, value } => {
                    state.put(&key, &value)?;
                }
                WalOperation::Delete { key } => {
                    state.delete(&key)?;
                }
                WalOperation::Commit { source_offsets } => {
                    // Update offsets for exactly-once
                }
                WalOperation::Checkpoint { .. } => {
                    // Skip checkpoint markers
                }
            }
        }
        
        // 4. Return recovered state with source offsets
        Ok(RecoveredState {
            state,
            source_offsets: checkpoint.source_offsets,
        })
    }
    
    fn find_latest_checkpoint(&self) -> Result<Checkpoint, RecoveryError> {
        let mut checkpoints: Vec<_> = std::fs::read_dir(&self.checkpoint_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension() == Some("meta".as_ref()))
            .collect();
        
        checkpoints.sort_by(|a, b| b.path().cmp(&a.path()));
        
        for entry in checkpoints {
            if let Ok(checkpoint) = self.validate_checkpoint(&entry.path()) {
                return Ok(checkpoint);
            }
        }
        
        Err(RecoveryError::NoValidCheckpoint)
    }
}
```

## Exactly-Once Semantics

```rust
/// Sink that tracks committed offsets to prevent duplicates
pub struct ExactlyOnceSink {
    inner: Box<dyn Sink>,
    committed_offsets: HashMap<String, u64>,
}

impl ExactlyOnceSink {
    pub async fn write_batch(
        &mut self,
        batch: RecordBatch,
        source_offsets: &HashMap<String, u64>,
    ) -> Result<()> {
        // 1. Check for duplicates
        for (source, offset) in source_offsets {
            if let Some(&committed) = self.committed_offsets.get(source) {
                if *offset <= committed {
                    // Already processed - skip
                    return Ok(());
                }
            }
        }
        
        // 2. Write data with transaction
        let tx_id = self.inner.begin_transaction().await?;
        self.inner.write(batch).await?;
        
        // 3. Commit offsets atomically with data
        self.inner.commit_with_offsets(tx_id, source_offsets).await?;
        
        // 4. Update local offset tracking
        self.committed_offsets.extend(source_offsets.clone());
        
        Ok(())
    }
}

/// Two-phase commit for distributed exactly-once
pub struct TwoPhaseCommit {
    coordinator: Box<dyn Coordinator>,
    participants: Vec<Box<dyn Participant>>,
}

impl TwoPhaseCommit {
    pub async fn commit(&mut self, transaction: Transaction) -> Result<()> {
        // Phase 1: Prepare
        for participant in &mut self.participants {
            participant.prepare(&transaction).await?;
        }
        
        // Phase 2: Commit (or rollback on failure)
        let result = self.coordinator.decide(&transaction).await;
        
        match result {
            Decision::Commit => {
                for participant in &mut self.participants {
                    participant.commit(&transaction).await?;
                }
            }
            Decision::Abort => {
                for participant in &mut self.participants {
                    participant.rollback(&transaction).await?;
                }
            }
        }
        
        Ok(())
    }
}
```

## Configuration

```toml
[checkpoint]
interval = "10s"           # Checkpoint every 10 seconds
min_pause_between = "1s"   # Minimum time between checkpoints
timeout = "5m"             # Checkpoint must complete within this
max_retained = 3           # Keep last N checkpoints

[wal]
sync_interval = "100ms"    # Group commit interval
buffer_size = "16MB"       # WAL buffer before flush
max_size = "1GB"           # Rotate WAL at this size
compression = "lz4"        # Compress WAL entries

[recovery]
parallelism = 4            # Parallel state loading
max_wal_replay = "1h"      # Fail if WAL replay exceeds this
verify_checksums = true    # Verify all checksums on recovery
```
