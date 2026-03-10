//! Kafka consumer group rebalance state tracking.
//!
//! [`RebalanceState`] tracks which topic-partitions are currently
//! assigned to this consumer and counts rebalance events.
//!
//! [`LaminarConsumerContext`] is an rdkafka `ConsumerContext` that
//! signals a checkpoint request on partition revocation, enabling
//! the pipeline to persist offsets before ownership changes.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use tracing::{info, warn};

/// Tracks partition assignments across consumer group rebalances.
#[derive(Debug, Clone, Default)]
pub struct RebalanceState {
    /// Currently assigned (topic, partition) pairs.
    assigned: HashSet<(String, i32)>,
    /// Total number of rebalance events.
    rebalance_count: u64,
}

impl RebalanceState {
    /// Starts with no partitions assigned.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Handles a partition assignment event.
    ///
    /// Replaces the current assignment set and increments the rebalance counter.
    pub fn on_assign(&mut self, partitions: &[(String, i32)]) {
        self.assigned.clear();
        for (topic, partition) in partitions {
            self.assigned.insert((topic.clone(), *partition));
        }
        self.rebalance_count += 1;
    }

    /// Handles a partition revocation event.
    ///
    /// Removes the specified partitions from the assignment set.
    pub fn on_revoke(&mut self, partitions: &[(String, i32)]) {
        for (topic, partition) in partitions {
            self.assigned.remove(&(topic.clone(), *partition));
        }
    }

    /// Returns the set of currently assigned partitions.
    #[must_use]
    pub fn assigned_partitions(&self) -> &HashSet<(String, i32)> {
        &self.assigned
    }

    /// Returns the total number of rebalance events.
    #[must_use]
    pub fn rebalance_count(&self) -> u64 {
        self.rebalance_count
    }

    /// Returns `true` if the given topic-partition is currently assigned.
    #[must_use]
    pub fn is_assigned(&self, topic: &str, partition: i32) -> bool {
        self.assigned.contains(&(topic.to_string(), partition))
    }
}

/// rdkafka consumer context that signals a checkpoint on partition revocation.
///
/// When a consumer group rebalance revokes partitions from this consumer,
/// the context notifies the pipeline coordinator to trigger an immediate
/// checkpoint before the partitions are reassigned. This prevents offset
/// loss during rebalance.
///
/// Rebalance callbacks run on rdkafka's background thread, so all shared
/// state uses `Arc` + atomic types for thread safety.
pub struct LaminarConsumerContext {
    checkpoint_requested: Arc<AtomicBool>,
    rebalance_count: AtomicU64,
    /// Shared rebalance state updated on Assign/Revoke events.
    rebalance_state: Arc<Mutex<RebalanceState>>,
    /// Shared rebalance event counter for source-level metrics.
    rebalance_metric: Arc<AtomicU64>,
}

impl LaminarConsumerContext {
    /// Wires checkpoint signaling, partition tracking, and rebalance metrics.
    #[must_use]
    pub fn new(
        checkpoint_requested: Arc<AtomicBool>,
        rebalance_state: Arc<Mutex<RebalanceState>>,
        rebalance_metric: Arc<AtomicU64>,
    ) -> Self {
        Self {
            checkpoint_requested,
            rebalance_count: AtomicU64::new(0),
            rebalance_state,
            rebalance_metric,
        }
    }

    /// Total rebalance events observed.
    #[must_use]
    pub fn rebalance_count(&self) -> u64 {
        self.rebalance_count.load(Ordering::Relaxed)
    }
}

impl ClientContext for LaminarConsumerContext {}

impl ConsumerContext for LaminarConsumerContext {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        use rdkafka::consumer::Rebalance;

        match rebalance {
            Rebalance::Revoke(tpl) => {
                let count = tpl.count();
                info!(
                    partitions_revoked = count,
                    "kafka rebalance: partitions being revoked, requesting checkpoint"
                );
                // Update shared rebalance state.
                let partitions: Vec<(String, i32)> = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic().to_string(), e.partition()))
                    .collect();
                if let Ok(mut state) = self.rebalance_state.lock() {
                    state.on_revoke(&partitions);
                }
                self.rebalance_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.rebalance_metric
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.checkpoint_requested.store(true, Ordering::Release);
            }
            Rebalance::Assign(tpl) => {
                let count = tpl.count();
                info!(
                    partitions_assigned = count,
                    "kafka rebalance: new partitions assigned"
                );
                // Update shared rebalance state.
                let partitions: Vec<(String, i32)> = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic().to_string(), e.partition()))
                    .collect();
                if let Ok(mut state) = self.rebalance_state.lock() {
                    state.on_assign(&partitions);
                }
                self.rebalance_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.rebalance_metric
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Rebalance::Error(msg) => {
                warn!(error = %msg, "kafka rebalance error");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign() {
        let mut state = RebalanceState::new();
        state.on_assign(&[
            ("events".into(), 0),
            ("events".into(), 1),
            ("events".into(), 2),
        ]);

        assert_eq!(state.assigned_partitions().len(), 3);
        assert!(state.is_assigned("events", 0));
        assert!(state.is_assigned("events", 1));
        assert!(state.is_assigned("events", 2));
        assert!(!state.is_assigned("events", 3));
        assert_eq!(state.rebalance_count(), 1);
    }

    #[test]
    fn test_revoke() {
        let mut state = RebalanceState::new();
        state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
        state.on_revoke(&[("events".into(), 1)]);

        assert_eq!(state.assigned_partitions().len(), 1);
        assert!(state.is_assigned("events", 0));
        assert!(!state.is_assigned("events", 1));
    }

    #[test]
    fn test_reassign() {
        let mut state = RebalanceState::new();
        state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
        // New assignment replaces old
        state.on_assign(&[("events".into(), 2), ("events".into(), 3)]);

        assert_eq!(state.assigned_partitions().len(), 2);
        assert!(!state.is_assigned("events", 0));
        assert!(state.is_assigned("events", 2));
        assert_eq!(state.rebalance_count(), 2);
    }

    #[test]
    fn test_empty_state() {
        let state = RebalanceState::new();
        assert_eq!(state.assigned_partitions().len(), 0);
        assert_eq!(state.rebalance_count(), 0);
        assert!(!state.is_assigned("events", 0));
    }

    fn make_context() -> (Arc<AtomicBool>, LaminarConsumerContext) {
        let flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(RebalanceState::new()));
        let metric = Arc::new(AtomicU64::new(0));
        let ctx = LaminarConsumerContext::new(
            Arc::clone(&flag),
            state,
            metric,
        );
        (flag, ctx)
    }

    #[test]
    fn test_consumer_context_initial_state() {
        let (flag, ctx) = make_context();
        assert!(!flag.load(Ordering::Relaxed));
        assert_eq!(ctx.rebalance_count(), 0);
    }

    #[test]
    fn test_consumer_context_shared_flag() {
        let (flag, _ctx) = make_context();

        // Simulate what pre_rebalance(Revoke) does.
        flag.store(true, Ordering::Relaxed);
        assert!(flag.load(Ordering::Relaxed));

        // Coordinator would swap-clear the flag.
        assert!(flag.swap(false, Ordering::Relaxed));
        assert!(!flag.load(Ordering::Relaxed));
    }
}
