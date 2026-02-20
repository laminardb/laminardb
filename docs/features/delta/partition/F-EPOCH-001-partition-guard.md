# F-EPOCH-001: PartitionGuard & Epoch Fencing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-EPOCH-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-COORD-001 (Raft Metadata) |
| **Blocks** | F-EPOCH-002 (Assignment Algorithm), F-EPOCH-003 (Reassignment Protocol) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/delta/partition/mod.rs`, `laminar-core/src/delta/partition/guard.rs` |

## Summary

Every partition in a LaminarDB Delta has a monotonically increasing epoch that establishes exclusive ownership. The `PartitionGuard` struct encapsulates this ownership proof: it carries the partition ID, the current epoch, and the owning node ID. Every checkpoint upload, source commit, and state mutation must pass through epoch validation. If a stale node attempts a write with an old epoch, the operation is rejected immediately. This prevents split-brain scenarios where two nodes believe they own the same partition after a network partition or delayed failover. On object storage backends that support conditional writes (S3 `If-Match`, GCS `ifGenerationMatch`), the epoch is also used as a precondition to guarantee that only the rightful owner can persist checkpoint data.

## Goals

- Define `PartitionGuard` struct as the exclusive ownership proof for a partition
- Implement `validate()` method that checks the local epoch against the authoritative `DeltaMetadata`
- Define `EpochError` with structured error variants for stale epoch, revoked ownership, and unknown partition
- Provide `PartitionGuardSet` for efficient bulk validation of all partitions owned by a node
- Integrate with checkpoint uploads via conditional PUT operations on object storage
- Ensure epoch fencing adds < 50ns overhead per operation on the hot path
- Gate all state-mutating operations behind epoch validation

## Non-Goals

- Partition assignment logic (F-EPOCH-002)
- Partition migration/reassignment protocol (F-EPOCH-003)
- Raft consensus implementation (F-COORD-001)
- Object storage client implementation (uses existing storage crate)
- Cross-datacenter epoch synchronization

## Technical Design

### Architecture

PartitionGuard operates in **Ring 2 (Control Plane)** for initial acquisition and validation against Raft metadata, but the cached epoch check on the hot path runs in **Ring 0** with a single atomic load. The flow:

1. Node acquires partition ownership via Raft consensus (Ring 2)
2. `PartitionGuard` is created with the assigned epoch
3. On every state mutation, the guard's epoch is compared against a local atomic cache (Ring 0, < 50ns)
4. Periodically, the cached epoch is reconciled with Raft metadata (Ring 2)
5. On checkpoint upload, the epoch is included as a conditional write precondition

```
Ring 0 (Hot Path):                Ring 2 (Control Plane):
+------------------+              +------------------------+
| Event Processing |              | DeltaMetadata  |
|                  |              | (Raft State Machine)   |
| guard.check()   |              |                        |
| (atomic load,   |    refresh   | partition_epoch()      |
|  <50ns)         | <----------> | assign_partition()     |
|                  |              | release_partition()    |
+------------------+              +------------------------+
        |
        v  (on checkpoint)
+------------------+
| Object Storage   |
| Conditional PUT  |
| If-Match: epoch  |
+------------------+
```

### API/Interface

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::fmt;

/// Unique identifier for a partition within the delta.
///
/// NOTE: The existing codebase has `PartitionId { source_id: usize,
/// partition_number: usize }` at `laminar_core::time::partitioned_watermark`.
/// This delta `PartitionId(u32)` is a DIFFERENT type — it identifies
/// a partition in the delta's hash ring, not a source partition.
/// A mapping layer bridges the two: delta PartitionId → set of
/// source PartitionIds that route to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

/// Unique identifier for a node (Star) within the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Proof of exclusive ownership for a single partition.
///
/// A `PartitionGuard` is created when a node is assigned a partition via Raft
/// consensus. It must be validated before any state mutation, checkpoint upload,
/// or source offset commit. The guard carries a cached epoch for fast hot-path
/// checks and can be refreshed against the authoritative Raft metadata.
///
/// # Epoch Semantics
///
/// - Epochs are monotonically increasing per partition
/// - A new epoch is minted on every ownership transfer
/// - Stale epochs are always rejected (no grace period)
/// - Epoch 0 is reserved and never assigned
/// - `NodeId(0)` is the "unassigned" sentinel (AUDIT FIX C6) — partitions
///   with `NodeId(0)` owner are awaiting reassignment by PartitionAssigner
pub struct PartitionGuard {
    /// The partition this guard protects.
    partition_id: PartitionId,

    /// The epoch at which ownership was granted.
    epoch: u64,

    /// The node that owns this partition at this epoch.
    node_id: NodeId,

    /// Cached epoch for fast Ring 0 checks. Updated atomically when
    /// refreshed from Raft metadata. If the cached value exceeds our
    /// epoch, ownership has been revoked.
    cached_current_epoch: AtomicU64,
}

impl PartitionGuard {
    /// Create a new partition guard.
    ///
    /// Called when Raft assigns a partition to this node. The epoch must
    /// be > 0 and must match the value committed in DeltaMetadata.
    pub fn new(partition_id: PartitionId, epoch: u64, node_id: NodeId) -> Self {
        assert!(epoch > 0, "epoch 0 is reserved");
        Self {
            partition_id,
            epoch,
            node_id,
            cached_current_epoch: AtomicU64::new(epoch),
        }
    }

    /// Fast epoch check for the hot path (Ring 0).
    ///
    /// Compares our epoch against the cached current epoch using a single
    /// atomic load. Returns `Ok(())` if we are still the owner, or
    /// `Err(EpochError::StaleEpoch)` if ownership has been revoked.
    ///
    /// # Performance
    ///
    /// Single `AtomicU64::load(Acquire)` — typically < 10ns.
    #[inline]
    pub fn check(&self) -> Result<(), EpochError> {
        let current = self.cached_current_epoch.load(Ordering::Acquire);
        if current > self.epoch {
            Err(EpochError::StaleEpoch {
                partition: self.partition_id,
                local_epoch: self.epoch,
                current_epoch: current,
            })
        } else {
            Ok(())
        }
    }

    /// Full validation against the authoritative Raft metadata (Ring 2).
    ///
    /// This performs a read from DeltaMetadata, which may involve
    /// a Raft read-index operation for linearizable consistency. Use this
    /// for checkpoint uploads and periodic reconciliation, not on the hot path.
    ///
    /// AUDIT FIX (C5): Now validates BOTH epoch AND owner node_id. The
    /// previous version only checked `epoch > self.epoch`, which would miss
    /// cases where ownership transferred to a different node at the same
    /// epoch (shouldn't happen normally, but defense-in-depth). Also
    /// handles the `NodeId(0)` unassigned sentinel from DeregisterNode (C6).
    pub fn validate(&self, metadata: &DeltaMetadata) -> Result<(), EpochError> {
        let ownership = metadata.partition_ownership(self.partition_id)
            .ok_or(EpochError::UnknownPartition {
                partition: self.partition_id,
            })?;

        // Always update cached epoch for fast hot-path rejection
        self.cached_current_epoch.store(ownership.epoch, Ordering::Release);

        if ownership.epoch > self.epoch {
            return Err(EpochError::StaleEpoch {
                partition: self.partition_id,
                local_epoch: self.epoch,
                current_epoch: ownership.epoch,
            });
        }

        // Verify this node is still the owner (defense-in-depth).
        // NodeId(0) is the unassigned sentinel from DeregisterNode.
        if ownership.node_id != self.node_id {
            return Err(EpochError::NotOwned {
                partition: self.partition_id,
            });
        }

        Ok(())
    }

    /// Refresh the cached epoch from Raft metadata without failing.
    ///
    /// Returns `true` if we are still the owner, `false` if revoked.
    pub fn refresh(&self, metadata: &DeltaMetadata) -> bool {
        match metadata.partition_epoch(self.partition_id) {
            Some(current) => {
                self.cached_current_epoch.store(current, Ordering::Release);
                current <= self.epoch
            }
            None => false,
        }
    }

    /// Build a conditional PUT precondition for object storage uploads.
    ///
    /// Returns the epoch as a string suitable for `If-Match` (S3) or
    /// `ifGenerationMatch` (GCS) headers.
    pub fn conditional_put_precondition(&self) -> ConditionalPut {
        ConditionalPut {
            partition_id: self.partition_id,
            epoch: self.epoch,
            node_id: self.node_id,
        }
    }

    /// Get the partition ID protected by this guard.
    #[inline]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Get the epoch of this guard.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Get the node that owns this partition.
    #[inline]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}
```

### Data Structures

```rust
/// Conditional PUT precondition for object storage.
///
/// Used to ensure that only the rightful epoch owner can persist
/// checkpoint data to object storage.
#[derive(Debug, Clone)]
pub struct ConditionalPut {
    pub partition_id: PartitionId,
    pub epoch: u64,
    pub node_id: NodeId,
}

impl ConditionalPut {
    /// Format as an object storage metadata key-value pair.
    pub fn as_metadata(&self) -> Vec<(String, String)> {
        vec![
            ("x-laminar-partition".into(), self.partition_id.0.to_string()),
            ("x-laminar-epoch".into(), self.epoch.to_string()),
            ("x-laminar-node".into(), self.node_id.0.to_string()),
        ]
    }

    /// Format as an S3 conditional write condition.
    /// Uses the epoch as the expected ETag for If-Match semantics.
    pub fn s3_condition(&self) -> String {
        format!("epoch-{}-partition-{}", self.epoch, self.partition_id.0)
    }
}

/// Set of partition guards for all partitions owned by a node.
///
/// Provides efficient bulk operations: validate all, refresh all,
/// and lookup by partition ID.
pub struct PartitionGuardSet {
    /// Guards indexed by partition ID for O(1) lookup.
    guards: HashMap<PartitionId, PartitionGuard>,

    /// The node that owns all these partitions.
    node_id: NodeId,
}

impl PartitionGuardSet {
    /// Create a new empty guard set for a node.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            guards: HashMap::new(),
            node_id,
        }
    }

    /// Add a partition guard to the set.
    pub fn insert(&mut self, guard: PartitionGuard) {
        assert_eq!(guard.node_id, self.node_id, "guard node must match set node");
        self.guards.insert(guard.partition_id, guard);
    }

    /// Remove a partition guard (on ownership release).
    pub fn remove(&mut self, partition_id: PartitionId) -> Option<PartitionGuard> {
        self.guards.remove(&partition_id)
    }

    /// Fast check for a single partition (Ring 0).
    #[inline]
    pub fn check(&self, partition_id: PartitionId) -> Result<(), EpochError> {
        self.guards
            .get(&partition_id)
            .ok_or(EpochError::NotOwned { partition: partition_id })?
            .check()
    }

    /// Validate all guards against Raft metadata (Ring 2).
    ///
    /// Returns a list of partitions whose ownership has been revoked.
    pub fn validate_all(
        &self,
        metadata: &DeltaMetadata,
    ) -> Vec<(PartitionId, EpochError)> {
        let mut revoked = Vec::new();
        for (pid, guard) in &self.guards {
            if let Err(e) = guard.validate(metadata) {
                revoked.push((*pid, e));
            }
        }
        revoked
    }

    /// Refresh all cached epochs from Raft metadata.
    ///
    /// Returns the list of partitions that are no longer owned.
    pub fn refresh_all(&self, metadata: &DeltaMetadata) -> Vec<PartitionId> {
        self.guards
            .iter()
            .filter(|(_, guard)| !guard.refresh(metadata))
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// Number of partitions in this guard set.
    pub fn len(&self) -> usize {
        self.guards.len()
    }

    /// Check if the guard set is empty.
    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    /// Iterate over all guarded partition IDs.
    pub fn partition_ids(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.guards.keys().copied()
    }
}

/// Errors from epoch validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochError {
    /// The local epoch is behind the current epoch. Another node has taken
    /// ownership of this partition. All in-flight operations must be aborted.
    StaleEpoch {
        partition: PartitionId,
        local_epoch: u64,
        current_epoch: u64,
    },

    /// The partition is not known to the delta metadata.
    /// This can happen if the partition was removed or never existed.
    UnknownPartition {
        partition: PartitionId,
    },

    /// This node does not own the requested partition.
    /// The partition exists but is assigned to a different node.
    NotOwned {
        partition: PartitionId,
    },

    /// Ownership was revoked during a checkpoint upload.
    /// The conditional PUT to object storage failed because the epoch changed.
    ConditionalPutFailed {
        partition: PartitionId,
        expected_epoch: u64,
        actual_epoch: u64,
    },
}

impl fmt::Display for EpochError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EpochError::StaleEpoch { partition, local_epoch, current_epoch } => {
                write!(
                    f,
                    "stale epoch for partition {:?}: local={}, current={}",
                    partition, local_epoch, current_epoch
                )
            }
            EpochError::UnknownPartition { partition } => {
                write!(f, "unknown partition: {:?}", partition)
            }
            EpochError::NotOwned { partition } => {
                write!(f, "partition {:?} not owned by this node", partition)
            }
            EpochError::ConditionalPutFailed { partition, expected_epoch, actual_epoch } => {
                write!(
                    f,
                    "conditional PUT failed for partition {:?}: expected epoch={}, actual={}",
                    partition, expected_epoch, actual_epoch
                )
            }
        }
    }
}

impl std::error::Error for EpochError {}
```

### Algorithm/Flow

1. **Guard Acquisition**: When Raft assigns partition P to node N at epoch E, the `DeltaManager` creates `PartitionGuard::new(P, E, N)` and inserts it into the node's `PartitionGuardSet`.

2. **Hot-Path Check (Ring 0)**: Before every state mutation on partition P, the reactor calls `guard_set.check(P)`. This performs a single `AtomicU64::load(Acquire)` against the cached epoch. If the cached epoch exceeds the guard's epoch, the operation is rejected with `EpochError::StaleEpoch`.

3. **Periodic Refresh (Ring 2)**: A background task calls `guard_set.refresh_all(metadata)` every 1-5 seconds. This reads the authoritative epoch from Raft metadata and updates the atomic cache. If any partition's epoch has advanced, the guard is marked as stale and the node begins the partition release protocol (F-EPOCH-003).

4. **Checkpoint Upload**: Before uploading a checkpoint, `guard.validate(metadata)` performs a full Raft read to ensure ownership is current. The checkpoint is then uploaded with `conditional_put_precondition()` metadata. If the object storage supports conditional writes, a concurrent upload from a stale node will be rejected by the storage backend.

5. **Guard Release**: When a partition is being migrated away, the guard is removed from the set via `guard_set.remove(P)`. Any subsequent hot-path checks for P will return `EpochError::NotOwned`.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `EpochError::StaleEpoch` | Another node was assigned this partition at a higher epoch | Stop processing this partition immediately; begin release protocol |
| `EpochError::UnknownPartition` | Partition ID not found in delta metadata | Log error; partition may have been deleted or never configured |
| `EpochError::NotOwned` | Node attempted operation on unowned partition | Route to correct node or reject the request |
| `EpochError::ConditionalPutFailed` | Object storage rejected checkpoint upload due to epoch mismatch | Discard checkpoint; the new owner will upload its own |

## Performance Targets

| Operation | Ring | Target | Notes |
|-----------|------|--------|-------|
| `guard.check()` | 0 | < 10ns | Single atomic load |
| `guard_set.check(partition)` | 0 | < 50ns | HashMap lookup + atomic load |
| `guard.validate(metadata)` | 2 | < 5ms | Raft read-index + atomic store |
| `guard_set.refresh_all()` | 2 | < 10ms | Batch read of all partition epochs |
| `guard_set.validate_all()` | 2 | < 10ms | Batch validation against Raft |
| Conditional PUT overhead | 2 | < 1ms | Metadata header addition only |
| Memory per guard | - | 40 bytes | PartitionId + epoch + NodeId + AtomicU64 |

## Test Plan

### Unit Tests

- [ ] `test_guard_new_valid_epoch` - Creating guard with epoch > 0 succeeds
- [ ] `test_guard_new_epoch_zero_panics` - Creating guard with epoch 0 panics
- [ ] `test_guard_check_valid_returns_ok` - Fresh guard check returns Ok
- [ ] `test_guard_check_stale_returns_error` - Guard with outdated epoch returns StaleEpoch
- [ ] `test_guard_validate_against_metadata_ok` - Validation passes when epoch and node match
- [ ] `test_guard_validate_stale_updates_cache` - Validation failure updates cached epoch
- [ ] `test_guard_validate_unknown_partition` - Validation with missing partition returns error
- [ ] `test_guard_validate_wrong_node_returns_not_owned` - Same epoch but different node_id returns NotOwned (C5)
- [ ] `test_guard_validate_unassigned_sentinel_returns_not_owned` - NodeId(0) sentinel returns NotOwned (C6)
- [ ] `test_guard_refresh_still_owner` - Refresh returns true when still owner
- [ ] `test_guard_refresh_revoked` - Refresh returns false when epoch advanced
- [ ] `test_guard_conditional_put_metadata` - Conditional PUT produces correct metadata
- [ ] `test_guard_set_insert_and_check` - Insert guard and verify check works
- [ ] `test_guard_set_check_not_owned` - Check for unowned partition returns NotOwned
- [ ] `test_guard_set_remove` - Removing guard makes subsequent checks fail
- [ ] `test_guard_set_validate_all_mixed` - Batch validation with some valid, some stale
- [ ] `test_guard_set_refresh_all_returns_revoked` - Refresh returns list of revoked partitions
- [ ] `test_epoch_error_display` - All error variants produce readable messages

### Integration Tests

- [ ] End-to-end: Assign partition via Raft, create guard, validate, release
- [ ] Split-brain prevention: Two nodes attempt to own same partition; only one succeeds
- [ ] Epoch monotonicity: Verify epochs only increase across multiple ownership transfers
- [ ] Checkpoint rejection: Stale node's checkpoint upload rejected via conditional PUT
- [ ] Guard refresh under load: Hot-path checks continue while background refresh runs

### Benchmarks

- [ ] `bench_guard_check_hot_path` - Target: < 10ns single check
- [ ] `bench_guard_set_check` - Target: < 50ns with 1000 partitions
- [ ] `bench_guard_validate_raft` - Target: < 5ms with Raft read-index
- [ ] `bench_guard_set_refresh_all` - Target: < 10ms for 1000 partitions

## Rollout Plan

1. **Phase 1**: Implement `PartitionGuard`, `EpochError`, and `PartitionGuardSet` with unit tests
2. **Phase 2**: Integrate with `DeltaMetadata` (depends on F-COORD-001)
3. **Phase 3**: Add conditional PUT support for object storage backends
4. **Phase 4**: Integration tests with Raft and partition migration
5. **Phase 5**: Benchmarks and hot-path optimization
6. **Phase 6**: Documentation and code review

## Open Questions

- [ ] Should the epoch refresh interval be configurable, or fixed at a sensible default (e.g., 2 seconds)?
- [ ] Should `PartitionGuard` implement `Drop` to automatically notify the delta manager when a guard is released?
- [ ] For object storage backends that do not support conditional writes, should we use a distributed lock or accept the (rare) risk of duplicate checkpoint uploads?
- [ ] Should the `PartitionGuardSet` use a `Vec` indexed by partition ID instead of `HashMap` for faster lookup when partition IDs are dense?

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

- [F-COORD-001: DeltaMetadata & Raft Integration](../coordination/F-COORD-001-raft-metadata.md)
- [F-EPOCH-002: Partition Assignment Algorithm](F-EPOCH-002-assignment-algorithm.md)
- [F-EPOCH-003: Partition Reassignment Protocol](F-EPOCH-003-reassignment-protocol.md)
- [F022: Incremental Checkpointing](../../phase-2/F022-incremental-checkpointing.md)
- [Apache Flink Epoch-Based Ownership](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/stateful-stream-processing/)
- [S3 Conditional Writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
