# F-DCKP-005: Recovery Manager

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-005 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DCKP-004 (Object Store Checkpointer), F-STATE-001 (State Store Interface) |
| **Blocks** | F-DCKP-008 (Distributed Coordination) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-storage` |
| **Module** | `laminar-storage/src/checkpoint/recovery.rs` |

## Summary

Orchestrates full recovery from an object store checkpoint after a failure or restart. The Recovery Manager reads the latest valid checkpoint manifest, downloads operator state snapshots for the assigned partitions, restores each operator's state store, seeks all sources to their checkpointed offsets, and resumes processing. Events between the last checkpoint and the failure point are automatically replayed from sources, providing exactly-once semantics when combined with idempotent sinks. The Recovery Manager handles corrupted snapshots, partial downloads, manifest version mismatches, and partition reassignment gracefully.

## Goals

- Orchestrate end-to-end recovery from the latest valid checkpoint
- Download and restore operator state snapshots for assigned partitions
- Seek sources to checkpointed offsets for replay
- Handle partition reassignment (different node count after recovery)
- Detect and recover from corrupted snapshots with fallback to previous checkpoint
- Provide progress reporting and estimated time to recovery
- Support partial recovery (restore subset of operators for testing/debugging)
- Validate manifest version compatibility before attempting recovery

## Non-Goals

- Checkpoint creation (covered by F-DCKP-004)
- Barrier protocol and injection (covered by F-DCKP-001)
- Cross-node coordination during recovery (covered by F-DCKP-008)
- Automatic failover detection (covered by health check / coordinator)
- State store implementation (covered by F-STATE-001)

## Technical Design

### Architecture

**Ring**: Ring 2 (Async I/O) for checkpoint download; Ring 1 for state restoration orchestration.

**Crate**: `laminar-storage`

**Module**: `laminar-storage/src/checkpoint/recovery.rs`

The Recovery Manager is invoked during node startup or after a failure detection. It coordinates with the `Checkpointer` (F-DCKP-004) to download checkpoint data and with individual operator state stores to restore state. After restoration, it provides source offsets so that source readers can seek to the correct position for replay.

> **AUDIT FIX (C9)**: Recovery Manager uses a dual-source checkpoint discovery strategy:
> Raft metadata is the primary index, but object storage listing is used as
> a fallback. This handles the case where `save()` completed but the Raft
> `commit_checkpoint()` crashed before persisting. See F-DCKP-004 §Checkpoint
> Commit Protocol for the full crash recovery analysis.
>
> **AUDIT FIX (C1)**: Recovery now supports incremental (delta) snapshots.
> After restoring the base full snapshot, all subsequent `.delta` files are
> replayed in epoch order to reach the latest consistent state. See
> F-STATE-001 `IncrementalSnapshot` and F-DCKP-003 `.delta` layout.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Recovery Flow                                     │
│                                                                      │
│  1. Read Manifest    2. Download Snapshots    3. Restore State       │
│  ┌──────────┐       ┌──────────────────┐     ┌──────────────────┐   │
│  │ Object   │──────>│  Download pool   │────>│ State Stores     │   │
│  │ Store    │       │  (parallel)      │     │ .restore(bytes)  │   │
│  │ manifest │       │                  │     │                  │   │
│  └──────────┘       └──────────────────┘     └──────────────────┘   │
│                                                                      │
│  4. Seek Sources     5. Resume Processing                            │
│  ┌──────────────┐   ┌──────────────────┐                            │
│  │ Source readers│   │ Operator DAG     │                            │
│  │ .seek(offset)│──>│ resumes from     │                            │
│  │              │   │ consistent state  │                            │
│  └──────────────┘   └──────────────────┘                            │
└─────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::checkpoint::checkpointer::{Checkpointer, CheckpointData, SnapshotData, CheckpointError};
use crate::checkpoint::manifest::*;
use crate::state::IncrementalSnapshot;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for the Recovery Manager.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum number of previous checkpoints to try if the latest is corrupt.
    pub max_fallback_attempts: usize,

    /// Maximum time allowed for the entire recovery process.
    pub recovery_timeout: std::time::Duration,

    /// Whether to verify SHA-256 integrity of downloaded snapshots.
    pub verify_integrity: bool,

    /// Partitions assigned to this node (for distributed recovery).
    /// If None, recover all partitions (single-node mode).
    pub assigned_partitions: Option<Vec<PartitionAssignment>>,

    /// Whether to allow recovery with a different operator set than
    /// the checkpoint (e.g., after a schema migration).
    pub allow_operator_mismatch: bool,

    /// Progress reporting callback interval.
    pub progress_report_interval: std::time::Duration,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_fallback_attempts: 3,
            recovery_timeout: std::time::Duration::from_secs(300),
            verify_integrity: true,
            assigned_partitions: None,
            allow_operator_mismatch: false,
            progress_report_interval: std::time::Duration::from_secs(5),
        }
    }
}

/// Describes which partitions are assigned to this node.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Operator whose partitions we are restoring.
    pub operator_id: OperatorId,
    /// Specific partition IDs assigned to this node.
    pub partition_ids: Vec<PartitionId>,
}

/// The result of a successful recovery.
#[derive(Debug)]
pub struct RecoveryResult {
    /// The checkpoint that was restored.
    pub checkpoint_id: CheckpointId,

    /// The epoch that was restored.
    pub epoch: u64,

    /// Source offsets to seek to for replay.
    pub source_offsets: HashMap<SourceId, SourceOffset>,

    /// Per-operator restoration results.
    pub operator_results: Vec<OperatorRecoveryResult>,

    /// Total time taken for recovery.
    pub recovery_duration: std::time::Duration,

    /// Total bytes downloaded.
    pub bytes_downloaded: u64,

    /// Number of fallback attempts before successful recovery.
    pub fallback_attempts: usize,
}

/// Result of restoring a single operator's state.
#[derive(Debug)]
pub struct OperatorRecoveryResult {
    /// Operator that was restored.
    pub operator_id: OperatorId,

    /// Partitions that were restored.
    pub partitions_restored: Vec<PartitionId>,

    /// Total bytes restored for this operator.
    pub bytes_restored: u64,

    /// Time taken to restore this operator.
    pub restore_duration: std::time::Duration,
}

/// Progress report during recovery.
#[derive(Debug, Clone)]
pub struct RecoveryProgress {
    /// Current phase of recovery.
    pub phase: RecoveryPhase,

    /// Overall progress as a fraction (0.0 to 1.0).
    pub progress: f64,

    /// Estimated time remaining.
    pub estimated_remaining: Option<std::time::Duration>,

    /// Bytes downloaded so far.
    pub bytes_downloaded: u64,

    /// Total bytes to download.
    pub total_bytes: u64,

    /// Operators restored so far.
    pub operators_restored: usize,

    /// Total operators to restore.
    pub total_operators: usize,
}

/// Phases of the recovery process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// Reading the checkpoint manifest.
    ReadingManifest,
    /// Downloading operator snapshots.
    DownloadingSnapshots,
    /// Restoring operator state stores.
    RestoringState,
    /// Seeking sources to checkpoint offsets.
    SeekingSources,
    /// Recovery complete.
    Complete,
}

/// Trait for objects that can have their state restored from a snapshot.
pub trait Restorable: Send + Sync {
    /// Restore state from a full serialized snapshot.
    ///
    /// The data is rkyv-serialized bytes. The implementation should
    /// deserialize and replace its current state.
    fn restore(&mut self, data: &[u8]) -> Result<(), RestoreError>;

    /// Apply an incremental delta on top of the current state.
    ///
    /// **AUDIT FIX (C1)**: Called after `restore()` to replay mutations
    /// recorded between checkpoint epochs. The delta is applied in order:
    /// Put entries overwrite, Delete entries remove.
    fn apply_delta(&mut self, delta: &IncrementalSnapshot) -> Result<(), RestoreError> {
        for entry in &delta.changes {
            match entry {
                crate::state::ChangeEntry::Put { key, value } => {
                    self.restore_put(key, value)?;
                }
                crate::state::ChangeEntry::Delete { key } => {
                    self.restore_delete(key)?;
                }
            }
        }
        Ok(())
    }

    /// Insert a single key-value pair during incremental restore.
    /// Default implementation calls `restore()` — overridden by stores
    /// that support granular mutation.
    fn restore_put(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), RestoreError> {
        Err(RestoreError::Rejected {
            operator_id: self.operator_id().0.clone(),
            reason: "incremental restore not supported".into(),
        })
    }

    /// Delete a single key during incremental restore.
    fn restore_delete(&mut self, _key: &[u8]) -> Result<(), RestoreError> {
        Err(RestoreError::Rejected {
            operator_id: self.operator_id().0.clone(),
            reason: "incremental restore not supported".into(),
        })
    }

    /// Returns the operator ID for this restorable.
    fn operator_id(&self) -> &OperatorId;
}

/// Errors during state restoration.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// Deserialization failed.
    #[error("failed to deserialize snapshot for {operator_id}: {reason}")]
    DeserializationFailed {
        operator_id: String,
        reason: String,
    },

    /// State store rejected the snapshot (e.g., schema mismatch).
    #[error("state store rejected snapshot for {operator_id}: {reason}")]
    Rejected {
        operator_id: String,
        reason: String,
    },

    /// Snapshot data is corrupt.
    #[error("corrupt snapshot for {operator_id}, partition {partition_id}")]
    CorruptSnapshot {
        operator_id: String,
        partition_id: u32,
    },
}

/// Trait for sources that can seek to a specific offset.
pub trait Seekable: Send + Sync {
    /// Seek the source to the given offset for replay.
    fn seek(&mut self, offset: &SourceOffset) -> Result<(), SeekError>;

    /// Returns the source ID.
    fn source_id(&self) -> &SourceId;
}

/// Errors during source seeking.
#[derive(Debug, thiserror::Error)]
pub enum SeekError {
    /// The offset is no longer available (e.g., Kafka retention expired).
    #[error("offset no longer available for {source_id}: {reason}")]
    OffsetExpired {
        source_id: String,
        reason: String,
    },

    /// Source connection failed.
    #[error("failed to connect to source {source_id}: {reason}")]
    ConnectionFailed {
        source_id: String,
        reason: String,
    },
}
```

### Data Structures

```rust
/// The Recovery Manager.
pub struct RecoveryManager {
    /// The checkpointer for reading checkpoint data.
    checkpointer: Arc<dyn Checkpointer>,

    /// Configuration.
    config: RecoveryConfig,
}

impl RecoveryManager {
    /// Create a new Recovery Manager.
    pub fn new(
        checkpointer: Arc<dyn Checkpointer>,
        config: RecoveryConfig,
    ) -> Self {
        Self {
            checkpointer,
            config,
        }
    }

    /// Execute full recovery from the latest valid checkpoint.
    ///
    /// This is the main entry point for recovery. It:
    /// 1. Finds the latest valid checkpoint
    /// 2. Downloads operator snapshots
    /// 3. Restores operator state stores
    /// 4. Returns source offsets for replay
    pub async fn recover(
        &self,
        operators: &mut HashMap<OperatorId, Box<dyn Restorable>>,
        sources: &mut HashMap<SourceId, Box<dyn Seekable>>,
        progress_tx: Option<tokio::sync::watch::Sender<RecoveryProgress>>,
    ) -> Result<RecoveryResult, RecoveryError> {
        let start = std::time::Instant::now();
        let mut fallback_attempts = 0;

        // Phase 1: Find valid checkpoint with fallback
        let (checkpoint_data, manifest) = self
            .find_valid_checkpoint(&mut fallback_attempts)
            .await?;

        self.report_progress(&progress_tx, RecoveryPhase::DownloadingSnapshots, 0.1, &manifest);

        // Phase 2: Validate operator compatibility
        self.validate_operator_compatibility(&manifest, operators)?;

        // Phase 3: Restore operator state
        let operator_results = self
            .restore_operators(&checkpoint_data, operators, &progress_tx, &manifest)
            .await?;

        self.report_progress(&progress_tx, RecoveryPhase::SeekingSources, 0.8, &manifest);

        // Phase 4: Seek sources to checkpoint offsets
        self.seek_sources(&checkpoint_data.source_offsets, sources)?;

        self.report_progress(&progress_tx, RecoveryPhase::Complete, 1.0, &manifest);

        let bytes_downloaded: u64 = operator_results.iter()
            .map(|r| r.bytes_restored)
            .sum();

        Ok(RecoveryResult {
            checkpoint_id: checkpoint_data.id,
            epoch: checkpoint_data.epoch,
            source_offsets: checkpoint_data.source_offsets,
            operator_results,
            recovery_duration: start.elapsed(),
            bytes_downloaded,
            fallback_attempts,
        })
    }

    /// Find a valid checkpoint, falling back to older ones if needed.
    ///
    /// **AUDIT FIX (C9)**: Uses dual-source checkpoint discovery:
    /// 1. Primary: Raft metadata checkpoint index (fast, authoritative)
    /// 2. Fallback: Object storage listing (catches commits that completed
    ///    save() but crashed before Raft commit_checkpoint())
    ///
    /// See F-DCKP-004 §Checkpoint Commit Protocol for crash recovery analysis.
    async fn find_valid_checkpoint(
        &self,
        fallback_attempts: &mut usize,
    ) -> Result<(CheckpointData, CheckpointManifest), RecoveryError> {
        // Primary source: Raft metadata index
        let mut checkpoints = self.checkpointer.list().await
            .map_err(|e| RecoveryError::CheckpointListFailed(e.to_string()))?;

        // Fallback source: object storage listing (C9 fix)
        // If Raft index is empty or stale, scan object storage directly.
        // This handles the case where save() completed but commit_checkpoint()
        // crashed before Raft persistence.
        if checkpoints.is_empty() {
            tracing::info!("no checkpoints in Raft metadata, scanning object storage");
            checkpoints = self.checkpointer.list_from_storage().await
                .unwrap_or_default();
        }

        if checkpoints.is_empty() {
            return Err(RecoveryError::NoCheckpointAvailable);
        }

        for (i, manifest) in checkpoints.iter().enumerate() {
            if i >= self.config.max_fallback_attempts + 1 {
                break;
            }

            match self.checkpointer.load(&manifest.checkpoint_id).await {
                Ok(data) => {
                    if i > 0 {
                        tracing::warn!(
                            checkpoint_id = %manifest.checkpoint_id.to_path_string(),
                            fallback_attempt = i,
                            "using fallback checkpoint after primary failed"
                        );
                    }
                    *fallback_attempts = i;
                    return Ok((data, manifest.clone()));
                }
                Err(e) => {
                    tracing::error!(
                        checkpoint_id = %manifest.checkpoint_id.to_path_string(),
                        error = %e,
                        "checkpoint load failed, trying fallback"
                    );
                    *fallback_attempts = i + 1;
                    continue;
                }
            }
        }

        Err(RecoveryError::AllCheckpointsCorrupt {
            attempted: (*fallback_attempts).min(checkpoints.len()),
        })
    }

    /// Validate that the checkpoint operators match the current deployment.
    fn validate_operator_compatibility(
        &self,
        manifest: &CheckpointManifest,
        operators: &HashMap<OperatorId, Box<dyn Restorable>>,
    ) -> Result<(), RecoveryError> {
        if self.config.allow_operator_mismatch {
            return Ok(());
        }

        let checkpoint_ops: std::collections::HashSet<_> = manifest.operators.iter()
            .map(|op| &op.operator_id)
            .collect();

        let current_ops: std::collections::HashSet<_> = operators.keys().collect();

        let missing_in_current: Vec<_> = checkpoint_ops.difference(&current_ops).collect();
        let missing_in_checkpoint: Vec<_> = current_ops.difference(&checkpoint_ops).collect();

        if !missing_in_current.is_empty() || !missing_in_checkpoint.is_empty() {
            return Err(RecoveryError::OperatorMismatch {
                missing_in_current: missing_in_current.into_iter()
                    .map(|id| id.0.clone())
                    .collect(),
                missing_in_checkpoint: missing_in_checkpoint.into_iter()
                    .map(|id| id.0.clone())
                    .collect(),
            });
        }

        Ok(())
    }

    /// Restore operator state from checkpoint data.
    ///
    /// **AUDIT FIX (C1)**: Handles both full and incremental (delta) snapshots.
    /// For each partition:
    /// 1. If snapshot is `SnapshotData::Full(bytes)` → call `operator.restore(bytes)`
    /// 2. If snapshot is `SnapshotData::Delta { bytes, base_epoch }` →
    ///    the base full snapshot must already be restored; call
    ///    `operator.apply_delta(incremental_snapshot)` to replay mutations.
    ///
    /// Recovery sequence for incremental checkpoints:
    /// - Load the most recent full checkpoint → `restore()` each partition
    /// - Load all subsequent delta checkpoints in epoch order → `apply_delta()` each
    /// - Result: state is reconstructed at the latest epoch
    async fn restore_operators(
        &self,
        data: &CheckpointData,
        operators: &mut HashMap<OperatorId, Box<dyn Restorable>>,
        progress_tx: &Option<tokio::sync::watch::Sender<RecoveryProgress>>,
        manifest: &CheckpointManifest,
    ) -> Result<Vec<OperatorRecoveryResult>, RecoveryError> {
        let mut results = Vec::new();
        let total_operators = data.operator_snapshots.len();

        for (idx, (operator_id, partitions)) in data.operator_snapshots.iter().enumerate() {
            let op_start = std::time::Instant::now();

            let operator = operators.get_mut(operator_id)
                .ok_or_else(|| RecoveryError::OperatorNotFound(operator_id.0.clone()))?;

            let mut bytes_restored = 0u64;
            let mut partitions_restored = Vec::new();

            for (partition_id, snapshot_data) in partitions {
                // Filter by assigned partitions if configured
                if let Some(ref assignments) = self.config.assigned_partitions {
                    let assigned = assignments.iter().any(|a| {
                        a.operator_id == *operator_id
                            && a.partition_ids.contains(partition_id)
                    });
                    if !assigned {
                        continue;
                    }
                }

                // AUDIT FIX (C1): Handle full vs incremental snapshots
                match snapshot_data {
                    SnapshotData::Full(snapshot_bytes) => {
                        operator.restore(snapshot_bytes).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator_id: operator_id.0.clone(),
                                partition_id: partition_id.0,
                                reason: e.to_string(),
                            }
                        })?;
                        bytes_restored += snapshot_bytes.len() as u64;
                    }
                    SnapshotData::Delta { bytes, base_epoch } => {
                        let delta = IncrementalSnapshot::from_bytes(bytes)
                            .map_err(|e| RecoveryError::RestoreFailed {
                                operator_id: operator_id.0.clone(),
                                partition_id: partition_id.0,
                                reason: format!(
                                    "failed to deserialize delta (base_epoch={}): {}",
                                    base_epoch, e
                                ),
                            })?;
                        operator.apply_delta(&delta).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator_id: operator_id.0.clone(),
                                partition_id: partition_id.0,
                                reason: format!("delta apply failed: {}", e),
                            }
                        })?;
                        bytes_restored += bytes.len() as u64;
                    }
                }

                partitions_restored.push(*partition_id);
            }

            results.push(OperatorRecoveryResult {
                operator_id: operator_id.clone(),
                partitions_restored,
                bytes_restored,
                restore_duration: op_start.elapsed(),
            });

            // Report progress
            let progress = 0.1 + 0.7 * ((idx + 1) as f64 / total_operators as f64);
            self.report_progress(progress_tx, RecoveryPhase::RestoringState, progress, manifest);
        }

        Ok(results)
    }

    /// Seek all sources to their checkpointed offsets.
    fn seek_sources(
        &self,
        offsets: &HashMap<SourceId, SourceOffset>,
        sources: &mut HashMap<SourceId, Box<dyn Seekable>>,
    ) -> Result<(), RecoveryError> {
        for (source_id, offset) in offsets {
            let source = sources.get_mut(source_id)
                .ok_or_else(|| RecoveryError::SourceNotFound(source_id.0.clone()))?;

            source.seek(offset).map_err(|e| RecoveryError::SeekFailed {
                source_id: source_id.0.clone(),
                reason: e.to_string(),
            })?;

            tracing::info!(
                source_id = %source_id.0,
                "source seeked to checkpoint offset"
            );
        }

        Ok(())
    }

    fn report_progress(
        &self,
        tx: &Option<tokio::sync::watch::Sender<RecoveryProgress>>,
        phase: RecoveryPhase,
        progress: f64,
        manifest: &CheckpointManifest,
    ) {
        if let Some(tx) = tx {
            let _ = tx.send(RecoveryProgress {
                phase,
                progress,
                estimated_remaining: None, // TODO: calculate based on download speed
                bytes_downloaded: 0,
                total_bytes: manifest.total_size_bytes,
                operators_restored: 0,
                total_operators: manifest.operators.len(),
            });
        }
    }
}

/// Errors during recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// No checkpoint is available for recovery.
    #[error("no checkpoint available for recovery")]
    NoCheckpointAvailable,

    /// Failed to list checkpoints.
    #[error("failed to list checkpoints: {0}")]
    CheckpointListFailed(String),

    /// All attempted checkpoints are corrupt.
    #[error("all {attempted} checkpoint(s) are corrupt or unreadable")]
    AllCheckpointsCorrupt { attempted: usize },

    /// Operator set mismatch between checkpoint and current deployment.
    #[error("operator mismatch: missing in current deployment: {missing_in_current:?}, \
             missing in checkpoint: {missing_in_checkpoint:?}")]
    OperatorMismatch {
        missing_in_current: Vec<String>,
        missing_in_checkpoint: Vec<String>,
    },

    /// Operator not found in current deployment.
    #[error("operator not found: {0}")]
    OperatorNotFound(String),

    /// Source not found for seeking.
    #[error("source not found: {0}")]
    SourceNotFound(String),

    /// State restoration failed.
    #[error("failed to restore operator {operator_id} partition {partition_id}: {reason}")]
    RestoreFailed {
        operator_id: String,
        partition_id: u32,
        reason: String,
    },

    /// Source seek failed.
    #[error("failed to seek source {source_id}: {reason}")]
    SeekFailed {
        source_id: String,
        reason: String,
    },

    /// Recovery timed out.
    #[error("recovery timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Manifest version is incompatible.
    #[error("manifest version {found} is not compatible (expected {expected})")]
    VersionMismatch { expected: u32, found: u32 },
}
```

### Algorithm/Flow

#### Full Recovery Flow

```
RecoveryManager.recover():

1. DISCOVER CHECKPOINT (AUDIT FIX C9: dual-source discovery)
   a. Primary: call checkpointer.list() → Raft metadata checkpoint index
   b. If empty: fallback to checkpointer.list_from_storage() → object
      storage listing (catches saves that completed but missed Raft commit)
   c. If both empty → return NoCheckpointAvailable
   d. Try to load the newest checkpoint
   e. If corrupt → try next oldest (up to max_fallback_attempts)
   f. If all corrupt → return AllCheckpointsCorrupt

2. VALIDATE COMPATIBILITY
   a. Compare checkpoint operator set with current operator set
   b. If mismatch and !allow_operator_mismatch → return OperatorMismatch
   c. Log warnings for any differences

3. RESTORE OPERATOR STATE (AUDIT FIX C1: full + incremental)
   a. Filter partitions by assigned_partitions (distributed mode)
   b. For each assigned partition, inspect SnapshotData variant:
      i.   SnapshotData::Full(bytes) → call operator.restore(bytes)
      ii.  SnapshotData::Delta { bytes, base_epoch } →
           deserialize IncrementalSnapshot, call operator.apply_delta(delta)
   c. If both full and delta snapshots exist for one partition:
      - First restore the full snapshot (base)
      - Then apply all deltas in epoch order
   d. Track bytes restored and duration per operator

4. SEEK SOURCES
   a. For each source in checkpoint offsets:
      i.  Find matching source reader
      ii. Call source.seek(offset)
      iii. If offset expired (Kafka retention) → return SeekFailed
   b. Log each successful seek

5. RESUME PROCESSING
   a. Return RecoveryResult with source offsets
   b. Caller resumes the operator DAG
   c. Sources replay events from checkpoint offset to current
   d. Operators process replayed events (idempotent with exactly-once sinks)
```

#### Partition Reassignment During Recovery

```
Scenario: Cluster resized from 3 nodes to 2 nodes after failure.

Original assignment:
  Node 0: partitions [0, 3, 6]
  Node 1: partitions [1, 4, 7]
  Node 2: partitions [2, 5, 8]

After recovery (2 nodes):
  Node 0: partitions [0, 2, 4, 6, 8]   ← picks up Node 2's partitions
  Node 1: partitions [1, 3, 5, 7]       ← picks up Node 2's partitions

Recovery flow:
1. Coordinator computes new partition assignment
2. Each node sets assigned_partitions in RecoveryConfig
3. Each node downloads only its assigned partition snapshots
4. State is restored for assigned partitions only
5. Sources are seeked based on the minimum offset across all restored partitions
```

#### Fallback Chain

```
AUDIT FIX (C9): Dual-source discovery precedes fallback chain.

Discovery:
  1. Raft metadata: checkpointer.list() → ordered checkpoint index
  2. If empty: checkpointer.list_from_storage() → scan object store
     (catches checkpoints saved but not Raft-committed due to crash)

Fallback (once checkpoint list is obtained):

Attempt 0: Load checkpoint-id-latest (newest)
  → Success: restore from this checkpoint
  → Failure: corrupt manifest or missing snapshots

Attempt 1: Load checkpoint-id-previous
  → Success: restore (losing events between previous and latest)
  → Failure: also corrupt

Attempt 2: Load checkpoint-id-previous-previous
  → Success: restore (losing more events, but consistent)
  → Failure: return AllCheckpointsCorrupt

Each fallback loses events between the fallback checkpoint and the
most recent one. These events will be replayed from sources if the
source retention period covers them.
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `NoCheckpointAvailable` | Fresh deployment or all checkpoints GC'd | Start from scratch (no state to restore) |
| `AllCheckpointsCorrupt` | Storage corruption or version incompatibility | Manual intervention required; alert operator |
| `OperatorMismatch` | Schema change between checkpoint and current code | Set `allow_operator_mismatch: true` or restart from scratch |
| `RestoreFailed` | rkyv deserialization failure or state store rejection | Fall back to previous checkpoint; if persistent, manual intervention |
| `SeekFailed` | Kafka offset expired past retention | Alert operator; restart from scratch for affected sources |
| `Timeout` | Network issues or very large checkpoint | Increase timeout; check network connectivity |
| `VersionMismatch` | Binary upgrade changed manifest format | Implement migration path or restart from scratch |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Recovery (10 MB state, local) | < 1s | Integration benchmark |
| Recovery (100 MB state, S3) | < 10s | Integration benchmark |
| Recovery (1 GB state, S3) | < 30s | Integration benchmark |
| Partition reassignment overhead | < 5s additional | Benchmark with reassignment |
| Manifest read + validation | < 100ms | Unit benchmark |
| Source seek (Kafka) | < 1s per source | Integration test |
| Progress reporting overhead | < 1% of recovery time | Benchmark comparison |

## Test Plan

### Unit Tests

- [ ] `test_recovery_config_defaults` - Verify default configuration
- [ ] `test_validate_operator_compatibility_match` - Matching operators pass
- [ ] `test_validate_operator_compatibility_mismatch` - Mismatched operators fail
- [ ] `test_validate_operator_compatibility_allow_mismatch` - Flag bypasses check
- [ ] `test_partition_assignment_filtering` - Only assigned partitions restored
- [ ] `test_fallback_to_previous_checkpoint` - Primary corrupt, secondary works
- [ ] `test_all_checkpoints_corrupt` - All fallbacks exhausted
- [ ] `test_no_checkpoint_available` - Empty checkpoint list

### Integration Tests

- [ ] `test_full_recovery_single_operator` - Save and recover single operator
- [ ] `test_full_recovery_multiple_operators` - Save and recover multiple operators
- [ ] `test_full_recovery_with_source_seek` - Restore state and seek sources
- [ ] `test_recovery_with_partition_reassignment` - Different partition assignment
- [ ] `test_recovery_from_second_oldest_checkpoint` - Fallback works
- [ ] `test_recovery_with_corrupt_snapshot` - One snapshot corrupt, fallback
- [ ] `test_recovery_progress_reporting` - Progress callbacks fire correctly
- [ ] `test_recovery_timeout` - Recovery aborted after timeout
- [ ] `test_recovery_idempotent` - Running recovery twice produces same state
- [ ] `test_recovery_dual_source_raft_empty_storage_fallback` - (C9) Raft index empty, object storage has checkpoint, recovery succeeds
- [ ] `test_recovery_dual_source_raft_and_storage_both_populated` - (C9) Raft is primary, storage fallback not invoked
- [ ] `test_recovery_incremental_full_plus_deltas` - (C1) Restore full snapshot then apply 3 deltas in epoch order, verify final state
- [ ] `test_recovery_incremental_delta_only_fails` - (C1) Delta without base full snapshot returns error
- [ ] `test_recovery_incremental_apply_delta_put_and_delete` - (C1) Delta with Put and Delete entries applied correctly

### Benchmarks

- [ ] `bench_recovery_10mb_local` - Target: < 1s
- [ ] `bench_recovery_100mb_local` - Target: < 5s
- [ ] `bench_manifest_validation` - Target: < 100ms
- [ ] `bench_state_restore_throughput` - Target: > 500 MB/s (rkyv deserialize)

## Rollout Plan

1. **Phase 1**: `RecoveryManager` struct and `recover()` skeleton
2. **Phase 2**: Manifest loading with fallback chain
3. **Phase 3**: Operator state restoration with partition filtering
4. **Phase 4**: Source seeking integration
5. **Phase 5**: Progress reporting via watch channel
6. **Phase 6**: Integration tests with local filesystem
7. **Phase 7**: Benchmarks and optimization
8. **Phase 8**: Documentation and code review

## Open Questions

- [ ] Should recovery be a blocking operation or run in the background while the system starts accepting connections? Leaning toward blocking (no queries until recovered).
- [ ] How should we handle sources whose offsets have expired (e.g., Kafka topic retention)? Options: (a) fail recovery, (b) start from earliest available, (c) start from latest.
- [ ] Should we support partial recovery (restore some operators, skip others that no longer exist)? The `allow_operator_mismatch` flag partially addresses this.
- [ ] Should recovery automatically trigger a fresh checkpoint after completion to establish a new baseline?

## Completion Checklist

- [ ] `RecoveryManager` implemented with `recover()` method
- [ ] Fallback chain for corrupt checkpoints
- [ ] Operator state restoration with partition filtering
- [ ] Source offset seeking
- [ ] `Restorable` and `Seekable` traits defined
- [ ] Progress reporting via watch channel
- [ ] Error types defined with actionable messages
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Apache Flink Recovery](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/#checkpoint-and-recovery) - Recovery procedure reference
- [F-DCKP-004: Object Store Checkpointer](F-DCKP-004-object-store-checkpointer.md) - Checkpoint data source, §Checkpoint Commit Protocol (C9)
- [F-DCKP-003: Object Store Layout](F-DCKP-003-object-store-layout.md) - Manifest schema, `.delta` layout (C1)
- [F-STATE-001: State Store Interface](../state/F-STATE-001-state-store-trait.md) - `IncrementalSnapshot` (C1), `StateStore` trait
- [F-DCKP-008: Distributed Coordination](F-DCKP-008-distributed-coordination.md) - Multi-node recovery
- [F-COORD-001: Raft Metadata](../coordination/F-COORD-001-raft-metadata.md) - Checkpoint commit linkage (C9)
