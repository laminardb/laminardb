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

use rdkafka::consumer::{Consumer, ConsumerContext};
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
    /// Monotonically increasing generation bumped on each Revoke event.
    ///
    /// Allows lock-free detection of revoke events from the hot path
    /// (`poll_batch`) — the source compares its cached generation against
    /// this value using `Relaxed` ordering, and only locks the mutex when
    /// a change is detected.
    revoke_generation: Arc<AtomicU64>,
    /// Shared flag indicating whether the reader task has paused Kafka
    /// partitions for backpressure. On `Assign`, newly assigned partitions
    /// must be re-paused if this flag is true.
    reader_paused: Arc<AtomicBool>,
    /// Set by `commit_callback` on broker rejection; reader task escalates
    /// to `CommitMode::Sync` on the next timer tick.
    commit_retry_needed: Arc<AtomicBool>,
    /// Snapshot of consumed offsets, updated once per `poll_batch()` cycle.
    /// Read on Assign to seek newly assigned partitions to last-consumed
    /// offset + 1, preventing duplicates after broker failures.
    offset_snapshot: Arc<Mutex<super::offsets::OffsetTracker>>,
}

impl LaminarConsumerContext {
    /// Wires checkpoint signaling, partition tracking, and rebalance metrics.
    #[must_use]
    pub fn new(
        checkpoint_requested: Arc<AtomicBool>,
        rebalance_state: Arc<Mutex<RebalanceState>>,
        rebalance_metric: Arc<AtomicU64>,
        revoke_generation: Arc<AtomicU64>,
        reader_paused: Arc<AtomicBool>,
        commit_retry_needed: Arc<AtomicBool>,
        offset_snapshot: Arc<Mutex<super::offsets::OffsetTracker>>,
    ) -> Self {
        Self {
            checkpoint_requested,
            rebalance_count: AtomicU64::new(0),
            rebalance_state,
            rebalance_metric,
            revoke_generation,
            reader_paused,
            commit_retry_needed,
            offset_snapshot,
        }
    }

    /// Total rebalance events observed.
    #[must_use]
    pub fn rebalance_count(&self) -> u64 {
        self.rebalance_count.load(Ordering::Relaxed)
    }

    /// Returns the shared revoke generation counter.
    #[must_use]
    pub fn revoke_generation(&self) -> &Arc<AtomicU64> {
        &self.revoke_generation
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
                match self.rebalance_state.lock() {
                    Ok(mut state) => state.on_revoke(&partitions),
                    Err(poisoned) => {
                        warn!("rebalance_state mutex poisoned, recovering");
                        poisoned.into_inner().on_revoke(&partitions);
                    }
                }
                self.revoke_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                match self.rebalance_state.lock() {
                    Ok(mut state) => state.on_assign(&partitions),
                    Err(poisoned) => {
                        warn!("rebalance_state mutex poisoned, recovering");
                        poisoned.into_inner().on_assign(&partitions);
                    }
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

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(()) => {
                tracing::debug!(
                    partitions = offsets.count(),
                    "broker offset commit confirmed"
                );
            }
            Err(e) => {
                self.commit_retry_needed.store(true, Ordering::Release);
                warn!(
                    error = %e,
                    partitions = offsets.count(),
                    "broker offset commit failed — scheduling sync retry"
                );
            }
        }
    }

    fn post_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        use rdkafka::consumer::Rebalance;

        if let Rebalance::Assign(tpl) = rebalance {
            // Seek assigned partitions to tracked offsets so we don't fall
            // back to broker-stored group offsets (which may be stale or
            // reset to earliest after a broker failure).
            //
            // Uses seek_partitions() instead of assign() to avoid clobbering
            // the partition set under cooperative rebalancing, where the tpl
            // contains only NEWLY assigned partitions (not the full set).
            let assigned: Vec<(String, i32)> = tpl
                .elements()
                .iter()
                .map(|e| (e.topic().to_string(), e.partition()))
                .collect();

            let seek_tpl = match self.offset_snapshot.lock() {
                Ok(tracker) => tracker.to_seek_tpl(&assigned),
                Err(poisoned) => poisoned.into_inner().to_seek_tpl(&assigned),
            };

            if seek_tpl.count() > 0 {
                match base_consumer.seek_partitions(seek_tpl, std::time::Duration::ZERO) {
                    Ok(result) => {
                        let errors: Vec<_> = result
                            .elements()
                            .iter()
                            .filter(|e| e.error().is_some())
                            .map(|e| format!("{}-{}: {:?}", e.topic(), e.partition(), e.error()))
                            .collect();
                        if errors.is_empty() {
                            info!(
                                partitions = result.count(),
                                "seeked assigned partitions to tracked offsets"
                            );
                        } else {
                            warn!(?errors, "some partitions failed to seek to tracked offsets");
                        }
                    }
                    Err(e) => warn!(
                        error = %e,
                        "failed to seek assigned partitions to tracked offsets"
                    ),
                }
            }

            // Re-pause newly assigned partitions if backpressure is active.
            if self.reader_paused.load(Ordering::Relaxed) {
                if let Err(e) = base_consumer.pause(tpl) {
                    warn!(error = %e, "failed to re-pause newly assigned partitions");
                } else {
                    info!(
                        partitions = tpl.count(),
                        "re-paused newly assigned partitions (reader backpressure active)"
                    );
                }
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
        let revoke_gen = Arc::new(AtomicU64::new(0));
        let reader_paused = Arc::new(AtomicBool::new(false));
        let commit_retry = Arc::new(AtomicBool::new(false));
        let offset_snapshot = Arc::new(Mutex::new(super::offsets::OffsetTracker::new()));
        let ctx = LaminarConsumerContext::new(
            Arc::clone(&flag),
            state,
            metric,
            revoke_gen,
            reader_paused,
            commit_retry,
            offset_snapshot,
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
