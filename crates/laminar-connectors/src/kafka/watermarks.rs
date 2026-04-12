//! Kafka per-partition watermark tracking.

use std::time::{Duration, Instant};

/// Kafka-specific watermark tracker wrapping `PartitionedWatermarkTracker`.
///
/// Tracks watermarks per Kafka partition and computes a combined watermark
/// as the minimum across all active (non-idle) partitions.
#[derive(Debug)]
pub struct KafkaWatermarkTracker {
    /// Source ID for this tracker.
    source_id: usize,
    /// Per-partition watermarks (indexed by partition number).
    partition_watermarks: Vec<PartitionState>,
    /// Combined watermark (minimum of active partitions).
    combined_watermark: i64,
    /// Idle timeout for automatic idle detection.
    idle_timeout: Duration,
    /// Maximum out-of-orderness for watermark generation.
    max_out_of_orderness: Duration,
    /// Metrics.
    metrics: WatermarkMetrics,
}

/// Per-partition watermark state.
#[derive(Debug, Clone)]
struct PartitionState {
    /// Current watermark (event time minus out-of-orderness bound).
    watermark: i64,
    /// Maximum event time seen.
    max_event_time: i64,
    /// Last activity timestamp.
    last_activity: Instant,
    /// Whether marked as idle.
    is_idle: bool,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            watermark: i64::MIN,
            max_event_time: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
        }
    }
}

/// Metrics for watermark tracking.
///
/// The tracker is single-threaded (owned by `KafkaSource`), so plain
/// `u64` counters are used instead of atomics.
#[derive(Debug, Clone, Copy, Default)]
pub struct WatermarkMetrics {
    /// Total watermark updates.
    pub updates: u64,
    /// Watermark advances.
    pub advances: u64,
    /// Partitions marked idle.
    pub idle_transitions: u64,
    /// Partitions resumed from idle.
    pub active_transitions: u64,
}

impl KafkaWatermarkTracker {
    /// Creates a new Kafka watermark tracker.
    ///
    /// # Arguments
    ///
    /// * `source_id` - Unique identifier for this source
    /// * `idle_timeout` - Duration after which an inactive partition is marked idle
    #[must_use]
    pub fn new(source_id: usize, idle_timeout: Duration) -> Self {
        Self {
            source_id,
            partition_watermarks: Vec::new(),
            combined_watermark: i64::MIN,
            idle_timeout,
            max_out_of_orderness: Duration::from_secs(5),
            metrics: WatermarkMetrics::default(),
        }
    }

    /// Creates a tracker with custom out-of-orderness bound.
    #[must_use]
    pub fn with_max_out_of_orderness(mut self, max_out_of_orderness: Duration) -> Self {
        self.max_out_of_orderness = max_out_of_orderness;
        self
    }

    /// Returns the source ID.
    #[must_use]
    pub fn source_id(&self) -> usize {
        self.source_id
    }

    /// Registers partitions for tracking.
    ///
    /// Call this when partitions are assigned during Kafka rebalance.
    pub fn register_partitions(&mut self, num_partitions: usize) {
        self.partition_watermarks
            .resize_with(num_partitions, PartitionState::new);
    }

    /// Adds a partition (e.g., during Kafka rebalance).
    pub fn add_partition(&mut self, partition: i32) {
        let Some(idx) = usize::try_from(partition).ok() else {
            return; // Ignore negative partitions
        };
        if idx >= self.partition_watermarks.len() {
            self.partition_watermarks
                .resize_with(idx + 1, PartitionState::new);
        }
    }

    /// Removes a partition (e.g., during Kafka rebalance).
    pub fn remove_partition(&mut self, partition: i32) {
        let Some(idx) = usize::try_from(partition).ok() else {
            return; // Ignore negative partitions
        };
        if idx < self.partition_watermarks.len() {
            // Mark as idle rather than removing to maintain indices
            self.partition_watermarks[idx].is_idle = true;
            self.partition_watermarks[idx].watermark = i64::MAX; // Exclude from min
        }
        // Truncate trailing idle entries so the Vec doesn't retain
        // capacity for revoked high-numbered partitions.
        while self.partition_watermarks.last().is_some_and(|s| s.is_idle) {
            self.partition_watermarks.pop();
        }
        self.recompute_combined();
    }

    /// Updates the watermark for a partition.
    ///
    /// # Arguments
    ///
    /// * `partition` - Kafka partition number
    /// * `event_time` - Event timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// `true` if the combined watermark advanced.
    pub fn update_partition(&mut self, partition: i32, event_time: i64) -> bool {
        let Some(idx) = usize::try_from(partition).ok() else {
            return false; // Ignore negative partitions
        };
        if idx >= self.partition_watermarks.len() {
            self.partition_watermarks
                .resize_with(idx + 1, PartitionState::new);
        }

        let state = &mut self.partition_watermarks[idx];
        state.last_activity = Instant::now();
        self.metrics.updates += 1;

        // Resume if was idle
        if state.is_idle {
            state.is_idle = false;
            self.metrics.active_transitions += 1;
        }

        // Update max event time
        if event_time > state.max_event_time {
            state.max_event_time = event_time;
            // Watermark = max_event_time - max_out_of_orderness
            // Saturate to i64::MAX if duration is too large (extremely unlikely in practice)
            let out_of_order_ms =
                i64::try_from(self.max_out_of_orderness.as_millis()).unwrap_or(i64::MAX);
            let new_watermark = event_time.saturating_sub(out_of_order_ms);
            if new_watermark > state.watermark {
                state.watermark = new_watermark;
            }
        }

        self.recompute_combined()
    }

    /// Marks a partition as idle.
    ///
    /// Idle partitions are excluded from watermark computation.
    pub fn mark_idle(&mut self, partition: i32) {
        let Some(idx) = usize::try_from(partition).ok() else {
            return; // Ignore negative partitions
        };
        if idx < self.partition_watermarks.len() && !self.partition_watermarks[idx].is_idle {
            self.partition_watermarks[idx].is_idle = true;
            self.metrics.idle_transitions += 1;
            self.recompute_combined();
        }
    }

    /// Checks for idle partitions based on timeout and marks them.
    ///
    /// Call this periodically (e.g., every poll cycle).
    pub fn check_idle_partitions(&mut self) {
        let now = Instant::now();
        let mut any_changed = false;

        for state in &mut self.partition_watermarks {
            if !state.is_idle && now.duration_since(state.last_activity) > self.idle_timeout {
                state.is_idle = true;
                self.metrics.idle_transitions += 1;
                any_changed = true;
            }
        }

        if any_changed {
            self.recompute_combined();
        }
    }

    /// Returns the current combined watermark.
    ///
    /// Returns `None` if no partitions are registered or all are idle.
    #[must_use]
    pub fn current_watermark(&self) -> Option<i64> {
        if self.combined_watermark == i64::MIN {
            None
        } else {
            Some(self.combined_watermark)
        }
    }

    /// Returns the number of active (non-idle) partitions.
    #[must_use]
    pub fn active_partition_count(&self) -> usize {
        self.partition_watermarks
            .iter()
            .filter(|s| !s.is_idle)
            .count()
    }

    /// Returns the number of idle partitions.
    #[must_use]
    pub fn idle_partition_count(&self) -> usize {
        self.partition_watermarks
            .iter()
            .filter(|s| s.is_idle)
            .count()
    }

    /// Returns the total number of registered partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partition_watermarks.len()
    }

    /// Returns metrics for this tracker.
    #[must_use]
    pub fn metrics(&self) -> &WatermarkMetrics {
        &self.metrics
    }

    /// Returns the watermark for a specific partition.
    #[must_use]
    pub fn partition_watermark(&self, partition: i32) -> Option<i64> {
        let idx = usize::try_from(partition).ok()?;
        self.partition_watermarks.get(idx).and_then(|s| {
            if s.watermark == i64::MIN {
                None
            } else {
                Some(s.watermark)
            }
        })
    }

    /// Returns whether a partition is idle.
    #[must_use]
    pub fn is_partition_idle(&self, partition: i32) -> bool {
        let Some(idx) = usize::try_from(partition).ok() else {
            return false;
        };
        self.partition_watermarks
            .get(idx)
            .is_some_and(|s| s.is_idle)
    }

    /// Recomputes the combined watermark from all active partitions.
    fn recompute_combined(&mut self) -> bool {
        let old = self.combined_watermark;

        // Minimum watermark across active partitions
        let min = self
            .partition_watermarks
            .iter()
            .filter(|s| !s.is_idle && s.watermark != i64::MIN)
            .map(|s| s.watermark)
            .min();

        self.combined_watermark = min.unwrap_or(i64::MIN);

        let advanced = self.combined_watermark > old && old != i64::MIN;
        if advanced {
            self.metrics.advances += 1;
        }
        advanced
    }
}

impl Default for KafkaWatermarkTracker {
    fn default() -> Self {
        Self::new(0, Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_new() {
        let tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
        assert_eq!(tracker.source_id(), 0);
        assert_eq!(tracker.partition_count(), 0);
        assert!(tracker.current_watermark().is_none());
    }

    #[test]
    fn test_register_partitions() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
        tracker.register_partitions(4);
        assert_eq!(tracker.partition_count(), 4);
        assert_eq!(tracker.active_partition_count(), 4);
        assert_eq!(tracker.idle_partition_count(), 0);
    }

    #[test]
    fn test_update_partition() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);

        // Watermark = min(5000-1000, 3000-1000) = 2000
        assert_eq!(tracker.current_watermark(), Some(2000));
    }

    #[test]
    fn test_idle_partition() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);

        // Mark slow partition as idle
        tracker.mark_idle(1);

        // Watermark now advances (only considers partition 0)
        assert_eq!(tracker.current_watermark(), Some(4000));
        assert_eq!(tracker.active_partition_count(), 1);
        assert_eq!(tracker.idle_partition_count(), 1);
    }

    #[test]
    fn test_resume_from_idle() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.mark_idle(1);
        assert_eq!(tracker.active_partition_count(), 1);

        // Update idle partition - should resume
        tracker.update_partition(1, 4000);
        assert_eq!(tracker.active_partition_count(), 2);
        // Watermark = min(4000, 3000) = 3000
        assert_eq!(tracker.current_watermark(), Some(3000));
    }

    #[test]
    fn test_add_partition_dynamically() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));

        tracker.update_partition(0, 5000);
        tracker.add_partition(5);
        tracker.update_partition(5, 3000);

        assert_eq!(tracker.partition_count(), 6); // 0-5
        assert_eq!(tracker.current_watermark(), Some(2000));
    }

    #[test]
    fn test_remove_partition() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);

        tracker.remove_partition(1);

        // Watermark advances (partition 1 excluded)
        assert_eq!(tracker.current_watermark(), Some(4000));
    }

    #[test]
    fn test_partition_watermark() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30))
            .with_max_out_of_orderness(Duration::from_millis(1000));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);

        assert_eq!(tracker.partition_watermark(0), Some(4000));
        assert_eq!(tracker.partition_watermark(1), Some(2000));
        assert!(tracker.partition_watermark(99).is_none());
    }

    #[test]
    fn test_metrics() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);
        tracker.mark_idle(1);
        tracker.update_partition(1, 4000); // resume

        let m = tracker.metrics();
        assert_eq!(m.updates, 3);
        assert_eq!(m.idle_transitions, 1);
        assert_eq!(m.active_transitions, 1);
    }

    #[test]
    fn test_all_partitions_idle() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
        tracker.register_partitions(2);

        tracker.update_partition(0, 5000);
        tracker.update_partition(1, 3000);
        tracker.mark_idle(0);
        tracker.mark_idle(1);

        // No active partitions - no watermark
        assert!(tracker.current_watermark().is_none());
    }

    #[test]
    fn test_remove_partition_truncates_trailing() {
        let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
        tracker.register_partitions(4); // 0, 1, 2, 3
        tracker.update_partition(0, 1000);
        tracker.update_partition(1, 1000);
        tracker.update_partition(3, 2000);

        // Remove trailing partitions — Vec should shrink.
        // Partition 2 was never updated but is still active (idle=false).
        // Removing partition 3 (last, now idle) truncates to len=3.
        // Removing partition 2 marks it idle — but it's not trailing
        // because partition 2 was registered as active (idle=false) by
        // register_partitions. We need to remove it explicitly.
        tracker.remove_partition(3);
        assert_eq!(tracker.partition_count(), 3, "trailing idle p3 truncated");
        tracker.remove_partition(2);
        assert_eq!(tracker.partition_count(), 2, "trailing idle p2 truncated");

        // Remove middle partition — Vec should NOT shrink past active.
        tracker.register_partitions(4);
        tracker.update_partition(3, 2000);
        tracker.remove_partition(1);
        assert_eq!(
            tracker.partition_count(),
            4,
            "middle idle does not truncate"
        );
    }
}
