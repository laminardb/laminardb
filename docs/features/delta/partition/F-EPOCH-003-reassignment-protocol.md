# F-EPOCH-003: Partition Reassignment Protocol

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-EPOCH-003 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F-EPOCH-001 (PartitionGuard), F-EPOCH-002 (Assignment Algorithm), F-DISC-002 (Gossip Discovery), F-CKP-001 (Checkpoint Manifest) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/delta/partition/migration.rs` |

## Summary

The partition reassignment protocol defines the complete, safe sequence for moving a partition from one node (Star) to another. This is the most critical protocol in the Delta architecture because it must guarantee zero data loss and zero split-brain: at no point can two nodes simultaneously believe they own the same partition. The protocol is a two-phase handoff with explicit acknowledgment, mediated by checkpoint state and epoch fencing.

**Phase 1 (Setting - Old Owner):** Stop processing on the partition, flush all in-flight events, create a final checkpoint at epoch N, persist the checkpoint to shared storage, release the partition guard, and announce "released, epoch N, checkpoint X" via gossip.

**Phase 2 (Rising - New Owner):** Wait for the "released" announcement, download checkpoint X, restore state from the checkpoint, increment epoch to N+1 via Raft, seek the source to the checkpointed offset, begin processing, and announce "active, epoch N+1" via gossip.

The invariant is absolute: the new owner never starts until the old owner has confirmed release with a committed checkpoint.

## Goals

- Define the complete two-phase partition handoff protocol
- Guarantee no split-brain: at most one active owner at any time
- Guarantee no data loss: final checkpoint captures all processed state
- Minimize downtime: partition unavailable for the duration of checkpoint + transfer + restore
- Support both planned (graceful) and unplanned (failure) reassignment
- Provide observable state machine for monitoring migration progress
- Handle edge cases: old owner crash mid-release, new owner crash mid-restore, network partition

## Non-Goals

- Partition assignment decisions (F-EPOCH-002 decides what moves where)
- Epoch fencing implementation (F-EPOCH-001)
- Checkpoint format or storage (uses existing checkpoint infrastructure)
- Live migration with zero downtime (partitions will have a brief unavailability window)
- Parallel migration of multiple partitions (each partition migrates independently)

## Technical Design

### Architecture

The migration protocol operates across **Ring 2 (Control Plane)** for coordination and **Ring 1 (Background)** for checkpoint I/O. The hot path (Ring 0) is stopped during migration. The protocol is orchestrated by the `DeltaManager` which drives state transitions on both the old and new owner.

```
Timeline:
=========

  Old Owner (Setting)              Raft Leader            New Owner (Rising)
  ====================            ===========            ====================

  1. STOP processing
     (drain Ring 0)
         |
  2. FLUSH final
     checkpoint (epoch N)
         |
  3. UPLOAD checkpoint X
     to shared storage
         |
  4. RELEASE partition
     guard
         |
  5. Commit to Raft:       -----> 6. Commit log entry:
     "Released P,                     partition P released
      epoch N, ckpt X"               at epoch N
         |                                |
  7. Gossip: "Setting,            8. Raft committed  -----> 9. Observe: partition P
     partition P,                                              is released
     epoch N"                                                     |
                                                           10. DOWNLOAD checkpoint X
                                                               from shared storage
                                                                  |
                                                           11. RESTORE state
                                                               from checkpoint
                                                                  |
                                                           12. Commit to Raft: ----> 13. Commit log entry:
                                                               "Acquired P,              partition P assigned
                                                                epoch N+1"               to new owner, epoch N+1
                                                                  |
                                                           14. SEEK source to
                                                               checkpointed offset
                                                                  |
                                                           15. BEGIN processing
                                                               (start Ring 0)
                                                                  |
                                                           16. Gossip: "Rising,
                                                               partition P,
                                                               epoch N+1"
```

### API/Interface

```rust
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// State machine for a single partition migration.
///
/// Tracks the current phase of the migration and provides methods to
/// drive each transition. The state machine is linear and non-reversible:
/// once a phase completes, it cannot go back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Migration has been planned but not started.
    Planned,

    /// Old owner is draining Ring 0 and flushing state.
    Draining,

    /// Old owner is creating the final checkpoint.
    Checkpointing,

    /// Old owner is uploading the checkpoint to shared storage.
    Uploading,

    /// Old owner has released the partition; waiting for Raft commit.
    Released,

    /// New owner is downloading the checkpoint from shared storage.
    Downloading,

    /// New owner is restoring state from the checkpoint.
    Restoring,

    /// New owner is seeking sources to checkpointed offsets.
    Seeking,

    /// New owner is actively processing; migration complete.
    Active,

    /// Migration failed at some phase.
    Failed {
        phase: Box<MigrationPhase>,
        error: String,
    },

    /// Migration was cancelled before completion.
    Cancelled,
}

/// A migration task for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTask {
    /// Unique migration ID.
    pub migration_id: u64,

    /// The partition being migrated.
    pub partition_id: PartitionId,

    /// The node releasing the partition.
    pub from_node: NodeId,

    /// The node acquiring the partition.
    pub to_node: NodeId,

    /// The epoch at which the old owner holds the partition.
    pub from_epoch: u64,

    /// The epoch the new owner will acquire (from_epoch + 1).
    pub to_epoch: u64,

    /// Current phase of the migration.
    pub phase: MigrationPhase,

    /// Checkpoint ID produced by the old owner (set during Checkpointing phase).
    pub checkpoint_id: Option<CheckpointId>,

    /// Source offsets at the checkpoint (for exactly-once resumption).
    pub source_offsets: Option<HashMap<String, HashMap<u32, u64>>>,

    /// Timestamp when migration started.
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// Timestamp when migration completed or failed.
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Maximum allowed duration for the entire migration.
    pub timeout: Duration,
}

/// Executes partition migrations on a node.
///
/// Each node runs a `MigrationExecutor` that handles both the "Setting"
/// (release) and "Rising" (acquire) sides of partition migrations.
pub struct MigrationExecutor {
    /// Node identity.
    node_id: NodeId,

    /// Reference to the delta metadata (Raft state).
    metadata: Arc<DeltaMetadata>,

    /// Partition guard set for this node.
    guard_set: Arc<RwLock<PartitionGuardSet>>,

    /// Checkpoint manager for creating/restoring checkpoints.
    checkpoint_manager: Arc<CheckpointManager>,

    /// Shared storage client for checkpoint uploads/downloads.
    storage: Arc<dyn CheckpointStorage>,

    /// Discovery for gossip announcements.
    discovery: Arc<dyn Discovery>,

    /// Active migrations on this node.
    active_migrations: RwLock<HashMap<PartitionId, MigrationTask>>,

    /// Channel for partition lifecycle events.
    event_tx: mpsc::Sender<PartitionEvent>,
}

impl MigrationExecutor {
    /// Execute the "Setting" (release) side of a partition migration.
    ///
    /// This method blocks until the partition is fully released or fails.
    /// Called on the OLD owner of the partition.
    pub async fn execute_release(
        &self,
        task: &mut MigrationTask,
    ) -> Result<(), MigrationError> {
        // Phase 1: Drain Ring 0
        task.phase = MigrationPhase::Draining;
        self.drain_partition(task.partition_id).await?;

        // Phase 2: Create final checkpoint
        task.phase = MigrationPhase::Checkpointing;
        let checkpoint = self.checkpoint_manager
            .create_partition_checkpoint(task.partition_id, task.from_epoch)
            .await?;
        task.checkpoint_id = Some(checkpoint.id);
        task.source_offsets = Some(checkpoint.source_offsets.clone());

        // Phase 3: Upload checkpoint to shared storage
        task.phase = MigrationPhase::Uploading;
        let guard = self.guard_set.read().await
            .get(task.partition_id)
            .ok_or(MigrationError::NotOwned(task.partition_id))?;
        self.storage.upload_checkpoint(
            &checkpoint,
            &guard.conditional_put_precondition(),
        ).await?;

        // Phase 4: Release partition guard
        task.phase = MigrationPhase::Released;
        self.guard_set.write().await.remove(task.partition_id);

        // Phase 5: Commit release to Raft
        self.metadata.commit_partition_release(
            task.partition_id,
            task.from_epoch,
            checkpoint.id,
        ).await?;

        // Phase 6: Announce via gossip
        self.discovery.announce(NodeMetadata::PartitionReleased {
            partition: task.partition_id,
            epoch: task.from_epoch,
            checkpoint: checkpoint.id,
        }).await?;

        task.completed_at = Some(chrono::Utc::now());
        Ok(())
    }

    /// Execute the "Rising" (acquire) side of a partition migration.
    ///
    /// This method blocks until the partition is fully acquired or fails.
    /// Called on the NEW owner of the partition.
    pub async fn execute_acquire(
        &self,
        task: &mut MigrationTask,
    ) -> Result<(), MigrationError> {
        // Wait for release confirmation from Raft
        let release_info = self.wait_for_release(
            task.partition_id,
            task.from_epoch,
            task.timeout,
        ).await?;
        task.checkpoint_id = Some(release_info.checkpoint_id);
        task.source_offsets = Some(release_info.source_offsets.clone());

        // Phase 1: Download checkpoint from shared storage
        task.phase = MigrationPhase::Downloading;
        let checkpoint_data = self.storage
            .download_checkpoint(release_info.checkpoint_id)
            .await?;

        // Phase 2: Restore state from checkpoint
        task.phase = MigrationPhase::Restoring;
        self.checkpoint_manager
            .restore_partition_checkpoint(task.partition_id, &checkpoint_data)
            .await?;

        // Phase 3: Commit acquisition to Raft (epoch N+1)
        self.metadata.commit_partition_acquisition(
            task.partition_id,
            self.node_id,
            task.to_epoch,
        ).await?;

        // Phase 4: Create partition guard with new epoch
        let guard = PartitionGuard::new(
            task.partition_id,
            task.to_epoch,
            self.node_id,
        );
        self.guard_set.write().await.insert(guard);

        // Phase 5: Seek sources to checkpointed offsets
        task.phase = MigrationPhase::Seeking;
        self.seek_sources(task.partition_id, &release_info.source_offsets).await?;

        // Phase 6: Begin processing
        task.phase = MigrationPhase::Active;
        self.start_partition_processing(task.partition_id).await?;

        // Phase 7: Announce via gossip
        self.discovery.announce(NodeMetadata::PartitionAcquired {
            partition: task.partition_id,
            epoch: task.to_epoch,
        }).await?;

        task.completed_at = Some(chrono::Utc::now());
        Ok(())
    }

    /// Wait for the old owner to release the partition.
    ///
    /// Watches Raft committed state for the partition release entry.
    /// Times out after the configured duration.
    async fn wait_for_release(
        &self,
        partition: PartitionId,
        expected_epoch: u64,
        timeout: Duration,
    ) -> Result<ReleaseInfo, MigrationError> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if let Some(info) = self.metadata.get_release_info(partition, expected_epoch) {
                return Ok(info);
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(MigrationError::Timeout {
                    partition,
                    phase: MigrationPhase::Downloading,
                    elapsed: timeout,
                });
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Drain all in-flight events for a partition on Ring 0.
    async fn drain_partition(&self, partition: PartitionId) -> Result<(), MigrationError> {
        self.event_tx
            .send(PartitionEvent::DrainRequested(partition))
            .await
            .map_err(|_| MigrationError::ChannelClosed)?;
        // Wait for drain confirmation
        // Implementation: reactor stops accepting new events for this partition
        // and processes all queued events before signaling completion
        Ok(())
    }

    /// Seek all sources for a partition to their checkpointed offsets.
    async fn seek_sources(
        &self,
        partition: PartitionId,
        offsets: &HashMap<String, HashMap<u32, u64>>,
    ) -> Result<(), MigrationError> {
        for (source_name, partition_offsets) in offsets {
            for (source_partition, offset) in partition_offsets {
                self.event_tx
                    .send(PartitionEvent::SeekSource {
                        partition,
                        source: source_name.clone(),
                        source_partition: *source_partition,
                        offset: *offset,
                    })
                    .await
                    .map_err(|_| MigrationError::ChannelClosed)?;
            }
        }
        Ok(())
    }

    /// Start processing events for a newly acquired partition.
    async fn start_partition_processing(
        &self,
        partition: PartitionId,
    ) -> Result<(), MigrationError> {
        self.event_tx
            .send(PartitionEvent::StartProcessing(partition))
            .await
            .map_err(|_| MigrationError::ChannelClosed)?;
        Ok(())
    }
}

/// Release information committed to Raft by the old owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseInfo {
    pub partition_id: PartitionId,
    pub epoch: u64,
    pub checkpoint_id: CheckpointId,
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,
    pub released_at: chrono::DateTime<chrono::Utc>,
}
```

### Data Structures

```rust
/// Events emitted during partition lifecycle changes.
#[derive(Debug, Clone)]
pub enum PartitionEvent {
    /// Request to drain all in-flight events for a partition.
    DrainRequested(PartitionId),

    /// Seek a source to a specific offset for a partition.
    SeekSource {
        partition: PartitionId,
        source: String,
        source_partition: u32,
        offset: u64,
    },

    /// Start processing events for a partition.
    StartProcessing(PartitionId),

    /// Stop processing events for a partition.
    StopProcessing(PartitionId),
}

/// Errors during partition migration.
#[derive(Debug, Clone)]
pub enum MigrationError {
    /// The partition is not owned by this node.
    NotOwned(PartitionId),

    /// The migration timed out waiting for a phase to complete.
    Timeout {
        partition: PartitionId,
        phase: MigrationPhase,
        elapsed: Duration,
    },

    /// Checkpoint creation failed.
    CheckpointFailed {
        partition: PartitionId,
        source: String,
    },

    /// Checkpoint upload to shared storage failed.
    UploadFailed {
        partition: PartitionId,
        source: String,
    },

    /// Checkpoint download from shared storage failed.
    DownloadFailed {
        partition: PartitionId,
        checkpoint: CheckpointId,
        source: String,
    },

    /// State restoration from checkpoint failed.
    RestoreFailed {
        partition: PartitionId,
        source: String,
    },

    /// Raft commit failed (leadership lost, network issue).
    RaftCommitFailed {
        partition: PartitionId,
        source: String,
    },

    /// The old owner crashed before completing the release.
    /// Triggers forced recovery: the new owner waits for the old owner's
    /// last known checkpoint and uses that as the starting point.
    OldOwnerCrashed {
        partition: PartitionId,
        last_known_epoch: u64,
    },

    /// Internal channel closed unexpectedly.
    ChannelClosed,

    /// Epoch conflict: another migration is already in progress.
    EpochConflict {
        partition: PartitionId,
        expected: u64,
        actual: u64,
    },

    /// Source seek failed.
    SeekFailed {
        partition: PartitionId,
        source: String,
    },
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::NotOwned(p) => write!(f, "partition {:?} not owned", p),
            MigrationError::Timeout { partition, phase, elapsed } => {
                write!(f, "migration timeout for {:?} at {:?} after {:?}", partition, phase, elapsed)
            }
            MigrationError::CheckpointFailed { partition, source } => {
                write!(f, "checkpoint failed for {:?}: {}", partition, source)
            }
            MigrationError::UploadFailed { partition, source } => {
                write!(f, "upload failed for {:?}: {}", partition, source)
            }
            MigrationError::DownloadFailed { partition, checkpoint, source } => {
                write!(f, "download failed for {:?} (ckpt {:?}): {}", partition, checkpoint, source)
            }
            MigrationError::RestoreFailed { partition, source } => {
                write!(f, "restore failed for {:?}: {}", partition, source)
            }
            MigrationError::RaftCommitFailed { partition, source } => {
                write!(f, "raft commit failed for {:?}: {}", partition, source)
            }
            MigrationError::OldOwnerCrashed { partition, last_known_epoch } => {
                write!(f, "old owner crashed for {:?} at epoch {}", partition, last_known_epoch)
            }
            MigrationError::ChannelClosed => write!(f, "internal channel closed"),
            MigrationError::EpochConflict { partition, expected, actual } => {
                write!(f, "epoch conflict for {:?}: expected={}, actual={}", partition, expected, actual)
            }
            MigrationError::SeekFailed { partition, source } => {
                write!(f, "source seek failed for {:?}: {}", partition, source)
            }
        }
    }
}

/// Configuration for the migration executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum time for a single partition migration.
    /// Default: 30 seconds.
    pub migration_timeout: Duration,

    /// Maximum time to wait for the old owner to release.
    /// Default: 15 seconds.
    pub release_wait_timeout: Duration,

    /// Maximum time for checkpoint upload/download.
    /// Default: 60 seconds.
    pub checkpoint_transfer_timeout: Duration,

    /// Maximum concurrent migrations per node.
    /// Default: 2.
    pub max_concurrent_migrations: usize,

    /// Retry count for failed migrations before giving up.
    /// Default: 3.
    pub max_retries: u32,

    /// Delay between retries.
    /// Default: 5 seconds.
    pub retry_delay: Duration,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            migration_timeout: Duration::from_secs(30),
            release_wait_timeout: Duration::from_secs(15),
            checkpoint_transfer_timeout: Duration::from_secs(60),
            max_concurrent_migrations: 2,
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        }
    }
}
```

### Algorithm/Flow

**Graceful Migration (planned):**

1. Raft leader commits `AssignmentPlan` with partition moves.
2. For each move, the `DeltaManager` on the old owner node starts `execute_release()`.
3. Old owner drains Ring 0 for the partition (processes all queued events).
4. Old owner creates a final checkpoint with epoch N.
5. Old owner uploads checkpoint to shared storage with conditional PUT (epoch N).
6. Old owner removes the partition guard and commits release to Raft.
7. Old owner gossips "Setting, partition P, epoch N".
8. New owner's `DeltaManager` observes the release via Raft.
9. New owner downloads checkpoint from shared storage.
10. New owner restores state and commits acquisition at epoch N+1 to Raft.
11. New owner creates partition guard, seeks sources, starts processing.
12. New owner gossips "Rising, partition P, epoch N+1".

**Forced Migration (old owner crashed):**

1. Failure detector determines old owner is unreachable (phi-accrual via gossip).
2. Raft leader commits `NodeFailed(old_node)` entry.
3. Assignment algorithm produces new plan with crashed node's partitions reassigned.
4. For each orphaned partition, the new owner looks up the last committed checkpoint from Raft metadata.
5. If the checkpoint exists in shared storage, the new owner downloads and restores it.
6. If no checkpoint exists, the partition starts from scratch (data loss for uncommitted state).
7. New owner increments epoch and commits acquisition to Raft.
8. Processing resumes from the last checkpointed offset. Events between the checkpoint and the crash are replayed from the source (exactly-once via deduplication).

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `MigrationError::Timeout` | Migration phase took too long | Retry with backoff; if repeated, alert operator |
| `MigrationError::CheckpointFailed` | State too large, disk full, or corruption | Retry; if persistent, skip checkpoint and accept data loss |
| `MigrationError::UploadFailed` | Network issue to shared storage | Retry with exponential backoff |
| `MigrationError::DownloadFailed` | Checkpoint missing or corrupted in storage | Check if old owner still has it; re-request release |
| `MigrationError::RaftCommitFailed` | Leadership change during migration | Retry on new leader |
| `MigrationError::OldOwnerCrashed` | Node failure mid-release | Fall back to forced migration path |
| `MigrationError::EpochConflict` | Concurrent migration for same partition | Abort one migration; the other proceeds |
| `MigrationError::SeekFailed` | Source unavailable at checkpointed offset | Retry source connection; fall back to earliest offset |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Graceful migration (small state, < 100MB) | 1-5 seconds | Dominated by checkpoint create + upload |
| Graceful migration (large state, 1-10GB) | 10-30 seconds | Dominated by checkpoint transfer |
| Forced migration (from last checkpoint) | 5-15 seconds | Download + restore + source seek |
| Partition unavailability window | < migration time | No dual-write period |
| Checkpoint upload throughput | > 100 MB/s | Depends on storage backend |
| Checkpoint download throughput | > 200 MB/s | Typically faster than upload |
| Raft commit for release/acquire | < 10ms | Single log entry |
| Gossip propagation of migration event | < 2s | Via chitchat |

## Test Plan

### Unit Tests

- [ ] `test_migration_phase_transitions_linear` - Phases progress in order
- [ ] `test_migration_task_creation` - Task initializes with correct fields
- [ ] `test_release_info_serialization` - ReleaseInfo round-trips through serde
- [ ] `test_migration_config_defaults` - Default config values are sensible
- [ ] `test_migration_error_display` - All error variants format correctly
- [ ] `test_migration_phase_failed_captures_context` - Failed phase includes original phase

### Integration Tests

- [ ] Graceful migration: Partition moves from node A to node B; B has correct state
- [ ] Epoch increment: After migration, new owner has epoch = old + 1
- [ ] No split-brain: During migration, at most one node processes events
- [ ] Source replay: New owner starts processing from checkpointed offset
- [ ] Forced migration: Old owner killed; new owner recovers from checkpoint
- [ ] Concurrent migrations: Two partitions migrate simultaneously without interference
- [ ] Migration timeout: Slow checkpoint triggers timeout and retry
- [ ] Checkpoint conditional PUT: Stale owner's upload rejected by storage
- [ ] Rolling restart: All nodes restart one at a time; all partitions survive
- [ ] Network partition: Raft leader isolated; migration stalls and retries after partition heals

### Benchmarks

- [ ] `bench_migration_small_state_100mb` - Target: < 5 seconds end-to-end
- [ ] `bench_migration_large_state_1gb` - Target: < 30 seconds end-to-end
- [ ] `bench_checkpoint_upload_throughput` - Target: > 100 MB/s
- [ ] `bench_checkpoint_download_throughput` - Target: > 200 MB/s
- [ ] `bench_drain_ring0` - Target: < 100ms for 10K queued events

## Rollout Plan

1. **Phase 1**: Define `MigrationPhase`, `MigrationTask`, `MigrationError`, and `MigrationConfig`
2. **Phase 2**: Implement `execute_release()` with drain, checkpoint, upload, and Raft commit
3. **Phase 3**: Implement `execute_acquire()` with wait, download, restore, and Raft commit
4. **Phase 4**: Implement forced migration path for node failure
5. **Phase 5**: Add retry logic, timeout handling, and concurrent migration support
6. **Phase 6**: Integration tests with full delta setup
7. **Phase 7**: Benchmarks, observability metrics, documentation

## Open Questions

- [ ] Should we support "warm standby" where the new owner pre-downloads the checkpoint before the old owner releases, to reduce the unavailability window?
- [ ] For very large state (>10GB), should we support incremental checkpoint transfer (only deltas since last full checkpoint)?
- [ ] Should forced migration wait a grace period before starting, in case the old owner is temporarily unreachable but will come back?
- [ ] How should we handle the case where shared storage is unavailable during migration? Fall back to direct node-to-node transfer?
- [ ] Should migration progress be exposed via the admin API for monitoring?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-EPOCH-001: PartitionGuard & Epoch Fencing](F-EPOCH-001-partition-guard.md)
- [F-EPOCH-002: Partition Assignment Algorithm](F-EPOCH-002-assignment-algorithm.md)
- [F-COORD-001: DeltaMetadata & Raft Integration](../coordination/F-COORD-001-raft-metadata.md)
- [F-DISC-002: Gossip Discovery](../discovery/F-DISC-002-gossip-discovery.md)
- [F-CKP-001: Checkpoint Manifest & Store](../../phase-3/F-CKP-001-checkpoint-manifest.md)
- [Apache Flink Task Failover Strategies](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery/)
- [Kafka Consumer Group Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429)
