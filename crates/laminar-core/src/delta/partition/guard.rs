//! Epoch-fenced partition guards for split-brain prevention.
//!
//! A `PartitionGuard` validates that this node still owns a partition
//! at the current epoch before processing events. The hot-path `check()`
//! is a single `AtomicU64::load(Acquire)` — target < 10ns.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::delta::discovery::NodeId;

/// Error returned when an epoch fence check fails.
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    /// This node no longer owns the partition.
    #[error("partition {partition_id} epoch fenced: expected {expected}, got {actual}")]
    Fenced {
        /// The partition that was fenced.
        partition_id: u32,
        /// The epoch this node expected.
        expected: u64,
        /// The actual current epoch.
        actual: u64,
    },

    /// The partition is not assigned to any node.
    #[error("partition {0} is unassigned")]
    Unassigned(u32),

    /// The partition is not in the guard set.
    #[error("partition {0} not in guard set")]
    NotTracked(u32),
}

/// An epoch-fenced guard for a single partition.
///
/// The hot-path `check()` is a single atomic load, designed for
/// sub-10ns latency in the event processing loop.
#[derive(Debug)]
pub struct PartitionGuard {
    /// The partition this guard protects.
    pub partition_id: u32,
    /// The epoch at which this node acquired ownership.
    pub epoch: u64,
    /// The owning node.
    pub node_id: NodeId,
    /// Cached current epoch — updated periodically from Raft.
    /// When this diverges from `self.epoch`, the guard is fenced.
    cached_current_epoch: Arc<AtomicU64>,
}

impl PartitionGuard {
    /// Create a new guard for a partition at the given epoch.
    #[must_use]
    pub fn new(partition_id: u32, epoch: u64, node_id: NodeId) -> Self {
        Self {
            partition_id,
            epoch,
            node_id,
            cached_current_epoch: Arc::new(AtomicU64::new(epoch)),
        }
    }

    /// Fast-path epoch check (single atomic load).
    ///
    /// Returns `Ok(())` if this node still owns the partition at the
    /// expected epoch. Returns `Err(EpochError::Fenced)` if the epoch
    /// has advanced (meaning ownership has changed).
    ///
    /// # Errors
    ///
    /// Returns [`EpochError::Fenced`] if the partition's epoch has changed.
    #[inline]
    pub fn check(&self) -> Result<(), EpochError> {
        let current = self.cached_current_epoch.load(Ordering::Acquire);
        if current == self.epoch {
            Ok(())
        } else {
            Err(EpochError::Fenced {
                partition_id: self.partition_id,
                expected: self.epoch,
                actual: current,
            })
        }
    }

    /// Update the cached epoch from an external source (e.g., Raft read).
    ///
    /// Called periodically by the health-check task to propagate epoch
    /// changes from the Raft state machine.
    pub fn update_cached_epoch(&self, new_epoch: u64) {
        self.cached_current_epoch
            .store(new_epoch, Ordering::Release);
    }

    /// Get a handle to the cached epoch atomic for external updates.
    #[must_use]
    pub fn cached_epoch_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.cached_current_epoch)
    }
}

/// A set of partition guards managed by a single node.
#[derive(Debug)]
pub struct PartitionGuardSet {
    guards: HashMap<u32, PartitionGuard>,
    node_id: NodeId,
}

impl PartitionGuardSet {
    /// Create a new empty guard set for the given node.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            guards: HashMap::new(),
            node_id,
        }
    }

    /// Add a guard for a partition.
    pub fn insert(&mut self, partition_id: u32, epoch: u64) {
        self.guards.insert(
            partition_id,
            PartitionGuard::new(partition_id, epoch, self.node_id),
        );
    }

    /// Remove a guard for a partition.
    pub fn remove(&mut self, partition_id: u32) -> Option<PartitionGuard> {
        self.guards.remove(&partition_id)
    }

    /// Check that a partition is still owned at the expected epoch.
    ///
    /// # Errors
    ///
    /// Returns [`EpochError::NotTracked`] if the partition is not in the set,
    /// or [`EpochError::Fenced`] if the epoch has changed.
    pub fn check(&self, partition_id: u32) -> Result<(), EpochError> {
        self.guards
            .get(&partition_id)
            .ok_or(EpochError::NotTracked(partition_id))?
            .check()
    }

    /// Get a guard by partition ID.
    #[must_use]
    pub fn get(&self, partition_id: u32) -> Option<&PartitionGuard> {
        self.guards.get(&partition_id)
    }

    /// Get the number of tracked partitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.guards.len()
    }

    /// Returns `true` if no partitions are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    /// Iterate over all guards.
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &PartitionGuard)> {
        self.guards.iter()
    }

    /// Update the cached epoch for a partition from Raft state.
    pub fn update_epoch(&self, partition_id: u32, new_epoch: u64) {
        if let Some(guard) = self.guards.get(&partition_id) {
            guard.update_cached_epoch(new_epoch);
        }
    }
}

/// Conditional PUT parameters for object storage epoch fencing.
///
/// Used when writing checkpoint data to S3/GCS/Azure to ensure that
/// only the current epoch owner can write.
#[derive(Debug, Clone)]
pub struct ConditionalPut {
    /// The partition being checkpointed.
    pub partition_id: u32,
    /// The epoch that must match for the write to succeed.
    pub expected_epoch: u64,
    /// The node that is writing.
    pub writer_node: NodeId,
}

impl ConditionalPut {
    /// Create conditional PUT parameters.
    #[must_use]
    pub const fn new(partition_id: u32, expected_epoch: u64, writer_node: NodeId) -> Self {
        Self {
            partition_id,
            expected_epoch,
            writer_node,
        }
    }

    /// Convert to HTTP header metadata for S3 conditional PUT.
    #[must_use]
    pub fn to_metadata(&self) -> Vec<(String, String)> {
        vec![
            ("x-amz-meta-epoch".into(), self.expected_epoch.to_string()),
            (
                "x-amz-meta-writer-node".into(),
                self.writer_node.0.to_string(),
            ),
            (
                "x-amz-meta-partition-id".into(),
                self.partition_id.to_string(),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_check_ok() {
        let guard = PartitionGuard::new(0, 1, NodeId(1));
        assert!(guard.check().is_ok());
    }

    #[test]
    fn test_guard_check_fenced() {
        let guard = PartitionGuard::new(0, 1, NodeId(1));
        guard.update_cached_epoch(2);
        let err = guard.check().unwrap_err();
        match err {
            EpochError::Fenced {
                partition_id,
                expected,
                actual,
            } => {
                assert_eq!(partition_id, 0);
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("expected Fenced error"),
        }
    }

    #[test]
    fn test_guard_set_insert_check() {
        let mut set = PartitionGuardSet::new(NodeId(1));
        set.insert(0, 1);
        set.insert(1, 1);
        assert_eq!(set.len(), 2);
        assert!(set.check(0).is_ok());
        assert!(set.check(1).is_ok());
    }

    #[test]
    fn test_guard_set_not_tracked() {
        let set = PartitionGuardSet::new(NodeId(1));
        let err = set.check(99).unwrap_err();
        assert!(matches!(err, EpochError::NotTracked(99)));
    }

    #[test]
    fn test_guard_set_remove() {
        let mut set = PartitionGuardSet::new(NodeId(1));
        set.insert(0, 1);
        assert_eq!(set.len(), 1);
        let removed = set.remove(0);
        assert!(removed.is_some());
        assert!(set.is_empty());
    }

    #[test]
    fn test_guard_set_update_epoch() {
        let mut set = PartitionGuardSet::new(NodeId(1));
        set.insert(0, 1);
        assert!(set.check(0).is_ok());

        set.update_epoch(0, 2);
        assert!(set.check(0).is_err());
    }

    #[test]
    fn test_conditional_put_metadata() {
        let cp = ConditionalPut::new(5, 10, NodeId(3));
        let meta = cp.to_metadata();
        assert_eq!(meta.len(), 3);
        assert!(meta
            .iter()
            .any(|(k, v)| k == "x-amz-meta-epoch" && v == "10"));
        assert!(meta
            .iter()
            .any(|(k, v)| k == "x-amz-meta-writer-node" && v == "3"));
    }

    #[test]
    fn test_guard_atomic_handle() {
        let guard = PartitionGuard::new(0, 5, NodeId(1));
        let handle = guard.cached_epoch_handle();
        assert_eq!(handle.load(Ordering::Relaxed), 5);

        // External update via handle
        handle.store(6, Ordering::Release);
        assert!(guard.check().is_err());
    }

    #[test]
    fn test_guard_set_iter() {
        let mut set = PartitionGuardSet::new(NodeId(1));
        set.insert(0, 1);
        set.insert(1, 2);

        let partitions: Vec<u32> = set.iter().map(|(id, _)| *id).collect();
        assert_eq!(partitions.len(), 2);
    }
}
