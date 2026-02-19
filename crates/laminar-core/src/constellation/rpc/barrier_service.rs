//! Barrier forwarding service for distributed checkpoints.
//!
//! Receives `InjectBarrier` RPCs from the checkpoint coordinator and
//! injects them into local source operators. Also reports completed
//! snapshots back via `ReportSnapshot`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::checkpoint::CheckpointBarrier;
use crate::constellation::discovery::NodeId;

/// A registered injection point for barrier forwarding.
#[derive(Debug)]
pub struct BarrierInjectionPoint {
    /// Operator ID that owns this injection point.
    pub operator_id: String,
    /// Channel ID within the operator.
    pub channel_id: u32,
    /// Sender for injecting barriers.
    pub sender: mpsc::Sender<CheckpointBarrier>,
}

/// Configuration for barrier forwarding.
#[derive(Debug, Clone)]
pub struct BarrierForwarderConfig {
    /// Initial retry delay.
    pub initial_retry_delay: Duration,
    /// Maximum retry delay.
    pub max_retry_delay: Duration,
    /// Maximum number of retries.
    pub max_retries: u32,
    /// Timeout for individual barrier injection RPCs.
    pub inject_timeout: Duration,
}

impl Default for BarrierForwarderConfig {
    fn default() -> Self {
        Self {
            initial_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_secs(5),
            max_retries: 5,
            inject_timeout: Duration::from_secs(5),
        }
    }
}

/// Routes checkpoint barriers to remote nodes with exponential backoff retry.
#[derive(Debug)]
pub struct BarrierForwarder {
    config: BarrierForwarderConfig,
    /// Injection points keyed by partition ID.
    injection_points: Arc<RwLock<HashMap<u32, Vec<BarrierInjectionPoint>>>>,
}

impl BarrierForwarder {
    /// Create a new barrier forwarder.
    #[must_use]
    pub fn new(config: BarrierForwarderConfig) -> Self {
        Self {
            config,
            injection_points: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register an injection point for a partition.
    pub fn register_injection_point(
        &self,
        partition_id: u32,
        point: BarrierInjectionPoint,
    ) {
        self.injection_points
            .write()
            .entry(partition_id)
            .or_default()
            .push(point);
    }

    /// Unregister all injection points for a partition.
    pub fn unregister_partition(&self, partition_id: u32) {
        self.injection_points.write().remove(&partition_id);
    }

    /// Inject a barrier into all registered points for the given partitions.
    ///
    /// Returns the list of partitions that were successfully injected.
    pub async fn inject_local(
        &self,
        barrier: CheckpointBarrier,
        target_partitions: &[u32],
    ) -> Vec<u32> {
        // Collect senders under the lock, then send outside the lock
        // to avoid holding RwLockReadGuard across await points.
        let targets: Vec<(u32, Vec<mpsc::Sender<CheckpointBarrier>>)> = {
            let points = self.injection_points.read();
            target_partitions
                .iter()
                .filter_map(|&pid| {
                    points.get(&pid).map(|partition_points| {
                        let senders: Vec<_> =
                            partition_points.iter().map(|p| p.sender.clone()).collect();
                        (pid, senders)
                    })
                })
                .collect()
        };

        let mut injected = Vec::new();
        for (pid, senders) in targets {
            let mut all_ok = true;
            for sender in &senders {
                if sender
                    .send_timeout(barrier, self.config.inject_timeout)
                    .await
                    .is_err()
                {
                    all_ok = false;
                }
            }
            if all_ok {
                injected.push(pid);
            }
        }

        injected
    }

    /// Get the configuration.
    #[must_use]
    pub fn config(&self) -> &BarrierForwarderConfig {
        &self.config
    }

    /// Get the number of registered partitions.
    #[must_use]
    pub fn registered_partition_count(&self) -> usize {
        self.injection_points.read().len()
    }
}

impl Default for BarrierForwarder {
    fn default() -> Self {
        Self::new(BarrierForwarderConfig::default())
    }
}

/// Tracks per-checkpoint snapshot completion across partitions.
#[derive(Debug)]
pub struct SnapshotCollector {
    /// `checkpoint_id` → (expected partitions, received partitions).
    tracking: HashMap<u64, SnapshotProgress>,
}

/// Progress of a single distributed checkpoint.
#[derive(Debug)]
pub struct SnapshotProgress {
    /// Expected partition IDs.
    pub expected: Vec<u32>,
    /// Received snapshot reports: `partition_id` → (`node_id`, `manifest_path`).
    pub received: HashMap<u32, (NodeId, String)>,
    /// Timestamp when tracking started.
    pub started_at_ms: i64,
}

impl SnapshotCollector {
    /// Create a new snapshot collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tracking: HashMap::new(),
        }
    }

    /// Start tracking a new checkpoint.
    pub fn start_tracking(&mut self, checkpoint_id: u64, expected_partitions: Vec<u32>) {
        self.tracking.insert(
            checkpoint_id,
            SnapshotProgress {
                expected: expected_partitions,
                received: HashMap::new(),
                started_at_ms: chrono::Utc::now().timestamp_millis(),
            },
        );
    }

    /// Record a snapshot completion for a partition.
    pub fn record_snapshot(
        &mut self,
        checkpoint_id: u64,
        partition_id: u32,
        node_id: NodeId,
        manifest_path: String,
    ) -> bool {
        if let Some(progress) = self.tracking.get_mut(&checkpoint_id) {
            progress
                .received
                .insert(partition_id, (node_id, manifest_path));
            self.is_complete(checkpoint_id)
        } else {
            false
        }
    }

    /// Check if all expected snapshots have been received for a checkpoint.
    #[must_use]
    pub fn is_complete(&self, checkpoint_id: u64) -> bool {
        self.tracking
            .get(&checkpoint_id)
            .is_some_and(|p| {
                p.expected
                    .iter()
                    .all(|pid| p.received.contains_key(pid))
            })
    }

    /// Get the progress for a checkpoint.
    #[must_use]
    pub fn progress(&self, checkpoint_id: u64) -> Option<&SnapshotProgress> {
        self.tracking.get(&checkpoint_id)
    }

    /// Remove completed checkpoints.
    pub fn cleanup_completed(&mut self) {
        let completed: Vec<u64> = self
            .tracking
            .iter()
            .filter(|(id, _)| self.is_complete(**id))
            .map(|(id, _)| *id)
            .collect();
        for id in completed {
            self.tracking.remove(&id);
        }
    }

    /// Remove a specific checkpoint from tracking.
    pub fn remove(&mut self, checkpoint_id: u64) {
        self.tracking.remove(&checkpoint_id);
    }
}

impl Default for SnapshotCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_barrier_forwarder_config_default() {
        let config = BarrierForwarderConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_retry_delay, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_inject_local_no_points() {
        let forwarder = BarrierForwarder::default();
        let barrier = CheckpointBarrier::new(1, 1);
        let injected = forwarder.inject_local(barrier, &[0, 1, 2]).await;
        assert!(injected.is_empty());
    }

    #[tokio::test]
    async fn test_inject_local_with_point() {
        let forwarder = BarrierForwarder::default();
        let (tx, mut rx) = mpsc::channel(10);

        forwarder.register_injection_point(
            0,
            BarrierInjectionPoint {
                operator_id: "op1".into(),
                channel_id: 0,
                sender: tx,
            },
        );

        let barrier = CheckpointBarrier::new(42, 1);
        let injected = forwarder.inject_local(barrier, &[0]).await;
        assert_eq!(injected, vec![0]);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.checkpoint_id, 42);
    }

    #[test]
    fn test_forwarder_register_unregister() {
        let forwarder = BarrierForwarder::default();
        let (tx, _rx) = mpsc::channel(10);

        forwarder.register_injection_point(
            0,
            BarrierInjectionPoint {
                operator_id: "op1".into(),
                channel_id: 0,
                sender: tx,
            },
        );
        assert_eq!(forwarder.registered_partition_count(), 1);

        forwarder.unregister_partition(0);
        assert_eq!(forwarder.registered_partition_count(), 0);
    }

    #[test]
    fn test_snapshot_collector_basic() {
        let mut collector = SnapshotCollector::new();
        collector.start_tracking(1, vec![0, 1, 2]);

        assert!(!collector.is_complete(1));

        collector.record_snapshot(1, 0, NodeId(1), "/ckp/0".into());
        assert!(!collector.is_complete(1));

        collector.record_snapshot(1, 1, NodeId(1), "/ckp/1".into());
        assert!(!collector.is_complete(1));

        let done = collector.record_snapshot(1, 2, NodeId(2), "/ckp/2".into());
        assert!(done);
        assert!(collector.is_complete(1));
    }

    #[test]
    fn test_snapshot_collector_unknown_checkpoint() {
        let mut collector = SnapshotCollector::new();
        assert!(!collector.record_snapshot(99, 0, NodeId(1), "/x".into()));
        assert!(!collector.is_complete(99));
    }

    #[test]
    fn test_snapshot_collector_cleanup() {
        let mut collector = SnapshotCollector::new();
        collector.start_tracking(1, vec![0]);
        collector.start_tracking(2, vec![0, 1]);

        collector.record_snapshot(1, 0, NodeId(1), "/ckp/0".into());
        // checkpoint 1 is complete, checkpoint 2 is not

        collector.cleanup_completed();
        assert!(collector.progress(1).is_none());
        assert!(collector.progress(2).is_some());
    }

    #[test]
    fn test_snapshot_collector_remove() {
        let mut collector = SnapshotCollector::new();
        collector.start_tracking(1, vec![0]);
        collector.remove(1);
        assert!(collector.progress(1).is_none());
    }

    #[tokio::test]
    async fn test_inject_dropped_receiver() {
        let forwarder = BarrierForwarder::new(BarrierForwarderConfig {
            inject_timeout: Duration::from_millis(100),
            ..BarrierForwarderConfig::default()
        });
        let (tx, _rx) = mpsc::channel(1);

        forwarder.register_injection_point(
            0,
            BarrierInjectionPoint {
                operator_id: "op1".into(),
                channel_id: 0,
                sender: tx,
            },
        );

        // Drop the receiver
        drop(_rx);

        let barrier = CheckpointBarrier::new(1, 1);
        let injected = forwarder.inject_local(barrier, &[0]).await;
        // Should fail since receiver is dropped
        assert!(injected.is_empty());
    }
}
