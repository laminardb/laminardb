//! Kafka consumer group rebalance state tracking.
//!
//! [`RebalanceState`] tracks which topic-partitions are currently
//! assigned to this consumer and counts rebalance events.

use std::collections::HashSet;

/// Tracks partition assignments across consumer group rebalances.
#[derive(Debug, Clone, Default)]
pub struct RebalanceState {
    /// Currently assigned (topic, partition) pairs.
    assigned: HashSet<(String, i32)>,
    /// Total number of rebalance events.
    rebalance_count: u64,
}

impl RebalanceState {
    /// Creates a new empty rebalance state.
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
}
