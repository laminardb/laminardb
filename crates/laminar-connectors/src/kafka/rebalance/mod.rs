//! Kafka consumer group rebalance state tracking.
//!
//! [`RebalanceState`] tracks which topic-partitions are currently
//! assigned to this consumer.
//!
//! [`LaminarConsumerContext`] is an rdkafka `ConsumerContext` that tracks
//! assignment changes and broker offset-commit outcomes.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use prometheus::IntCounter;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::ClientContext;
use tracing::{info, warn};

/// Tracks partition assignments across consumer group rebalances.
#[derive(Debug, Clone, Default)]
pub struct RebalanceState {
    /// Currently assigned (topic, partition) pairs.
    assigned: Arc<HashSet<(String, i32)>>,
}

impl RebalanceState {
    /// Starts with no partitions assigned.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Handles a partition assignment event.
    ///
    /// Additive: inserts new partitions without clearing existing ones.
    /// This is correct for both eager and cooperative rebalance protocols:
    /// - Eager: the preceding `on_revoke(all)` already clears the set.
    /// - Cooperative: `Assign` only contains newly assigned partitions,
    ///   so clearing would lose existing assignments.
    pub fn on_assign(&mut self, partitions: &[(String, i32)]) {
        let assigned = Arc::make_mut(&mut self.assigned);
        for (topic, partition) in partitions {
            assigned.insert((topic.clone(), *partition));
        }
    }

    /// Handles a partition revocation event.
    ///
    /// Removes the specified partitions from the assignment set.
    pub fn on_revoke(&mut self, partitions: &[(String, i32)]) {
        let assigned = Arc::make_mut(&mut self.assigned);
        for (topic, partition) in partitions {
            assigned.remove(&(topic.clone(), *partition));
        }
    }

    /// Returns the set of currently assigned partitions.
    #[must_use]
    pub fn assigned_partitions(&self) -> &HashSet<(String, i32)> {
        &self.assigned
    }

    /// Immutable assignment snapshot for checkpoint and revoke processing.
    #[must_use]
    pub fn assignment_snapshot(&self) -> Arc<HashSet<(String, i32)>> {
        Arc::clone(&self.assigned)
    }
}

/// rdkafka consumer context that tracks partition assignment changes.
///
/// The callback does not request an asynchronous engine checkpoint: Kafka may revoke ownership as
/// soon as this callback returns, so a later checkpoint cannot certify the revoked cut. Guaranteed
/// delivery uses engine-owned manual assignment instead; this state is for dynamic best-effort
/// subscriptions and observability.
///
/// Rebalance callbacks run on rdkafka's background thread, so all shared
/// state uses `Arc` + atomic types for thread safety.
pub struct LaminarConsumerContext {
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
    /// Bumped on each Assign; the reader task seeks the newly-assigned partitions
    /// from the poll loop (see the `KafkaSource` reader loop).
    assign_generation: Arc<AtomicU64>,
    /// Counter bumped on every broker-confirmed advisory progress commit.
    /// Engine recovery uses checkpoint state, so asynchronous broker commit
    /// failures affect monitoring lag rather than the recovery guarantee.
    commits_counter: IntCounter,
    /// Counter bumped when the broker rejects a commit.
    commit_failures_counter: IntCounter,
}

impl LaminarConsumerContext {
    /// Wires partition tracking, commit outcomes, and rebalance metrics.
    #[must_use]
    pub fn new(
        rebalance_state: Arc<Mutex<RebalanceState>>,
        rebalance_metric: Arc<AtomicU64>,
        revoke_generation: Arc<AtomicU64>,
        assign_generation: Arc<AtomicU64>,
        commits_counter: IntCounter,
        commit_failures_counter: IntCounter,
    ) -> Self {
        Self {
            rebalance_state,
            rebalance_metric,
            revoke_generation,
            assign_generation,
            commits_counter,
            commit_failures_counter,
        }
    }

    /// Locks the rebalance state, recovering from poison.
    fn lock_rebalance_state(&self) -> std::sync::MutexGuard<'_, RebalanceState> {
        self.rebalance_state.lock().unwrap_or_else(|poisoned| {
            warn!("rebalance_state mutex poisoned, recovering");
            poisoned.into_inner()
        })
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
                    "kafka rebalance: partitions being revoked"
                );
                // Update shared rebalance state.
                let partitions: Vec<(String, i32)> = tpl
                    .elements()
                    .iter()
                    .map(|e| (e.topic().to_string(), e.partition()))
                    .collect();
                self.lock_rebalance_state().on_revoke(&partitions);
                self.revoke_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                self.rebalance_metric
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                self.lock_rebalance_state().on_assign(&partitions);
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
                self.commits_counter.inc();
                tracing::debug!(
                    partition_count = offsets.count(),
                    "broker offset commit confirmed"
                );
            }
            Err(e) => {
                self.commit_failures_counter.inc();
                warn!(
                    error = %e,
                    partition_count = offsets.count(),
                    "broker offset commit failed (callback)"
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
            // Pause the newly-assigned partitions so librdkafka can't fetch from
            // the reset position before the reader loop seeks them to their
            // checkpointed offsets; the reader resumes them after the seek. (Seeking
            // here fails — the partitions aren't fetch-ready yet in the callback.)
            if let Err(e) = base_consumer.pause(tpl) {
                warn!(error = %e, "failed to pause newly assigned partitions for seek");
            }
            // Signal the reader loop to seek + resume from the live poll loop.
            self.assign_generation.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests;
