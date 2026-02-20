//! Kafka watermark integration.
//!
//! Integrates `laminar-core` per-partition watermarks and watermark
//! alignment groups with the Kafka source connector.
//!
//! # Per-Partition Watermarks
//!
//! Tracks watermarks at Kafka partition granularity, allowing progress even
//! when some partitions are slow or idle:
//!
//! ```rust,ignore
//! use laminar_connectors::kafka::watermarks::KafkaWatermarkTracker;
//! use std::time::Duration;
//!
//! let mut tracker = KafkaWatermarkTracker::new(0, Duration::from_secs(30));
//! tracker.register_partitions(4);
//!
//! // Update individual partitions
//! tracker.update_partition(0, 5000);
//! tracker.update_partition(1, 3000); // slow partition
//!
//! // Combined watermark is minimum (3000)
//! assert_eq!(tracker.current_watermark(), Some(3000));
//!
//! // Mark slow partition as idle
//! tracker.mark_idle(1);
//!
//! // Now watermark advances (5000)
//! assert_eq!(tracker.current_watermark(), Some(5000));
//! ```
//!
//! # Watermark Alignment
//!
//! For multi-topic joins, alignment prevents fast topics from building
//! unbounded state while waiting for slow topics:
//!
//! ```rust,ignore
//! use laminar_connectors::kafka::watermarks::{KafkaAlignmentConfig, KafkaAlignmentMode};
//! use std::time::Duration;
//!
//! let config = KafkaAlignmentConfig {
//!     group_id: "orders-payments".to_string(),
//!     max_drift: Duration::from_secs(300), // 5 minutes
//!     mode: KafkaAlignmentMode::Pause,
//! };
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
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
#[derive(Debug, Default)]
pub struct WatermarkMetrics {
    /// Total watermark updates.
    pub updates: AtomicU64,
    /// Watermark advances.
    pub advances: AtomicU64,
    /// Partitions marked idle.
    pub idle_transitions: AtomicU64,
    /// Partitions resumed from idle.
    pub active_transitions: AtomicU64,
}

impl WatermarkMetrics {
    fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of current metrics.
    #[must_use]
    pub fn snapshot(&self) -> WatermarkMetricsSnapshot {
        WatermarkMetricsSnapshot {
            updates: self.updates.load(Ordering::Relaxed),
            advances: self.advances.load(Ordering::Relaxed),
            idle_transitions: self.idle_transitions.load(Ordering::Relaxed),
            active_transitions: self.active_transitions.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of watermark metrics.
#[derive(Debug, Clone, Default)]
pub struct WatermarkMetricsSnapshot {
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
            metrics: WatermarkMetrics::new(),
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
        self.metrics.updates.fetch_add(1, Ordering::Relaxed);

        // Resume if was idle
        if state.is_idle {
            state.is_idle = false;
            self.metrics
                .active_transitions
                .fetch_add(1, Ordering::Relaxed);
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
            self.metrics
                .idle_transitions
                .fetch_add(1, Ordering::Relaxed);
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
                self.metrics
                    .idle_transitions
                    .fetch_add(1, Ordering::Relaxed);
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
            self.metrics.advances.fetch_add(1, Ordering::Relaxed);
        }
        advanced
    }
}

impl Default for KafkaWatermarkTracker {
    fn default() -> Self {
        Self::new(0, Duration::from_secs(30))
    }
}

/// Alignment mode for multi-source watermark coordination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaAlignmentMode {
    /// Pause fast sources until slow sources catch up.
    #[default]
    Pause,
    /// Emit warnings but don't pause.
    WarnOnly,
    /// Drop events from fast sources that exceed drift.
    DropExcess,
}

impl std::fmt::Display for KafkaAlignmentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pause => write!(f, "pause"),
            Self::WarnOnly => write!(f, "warn-only"),
            Self::DropExcess => write!(f, "drop-excess"),
        }
    }
}

impl std::str::FromStr for KafkaAlignmentMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('_', "-").as_str() {
            "pause" => Ok(Self::Pause),
            "warn-only" | "warnonly" | "warn" => Ok(Self::WarnOnly),
            "drop-excess" | "dropexcess" | "drop" => Ok(Self::DropExcess),
            other => Err(format!("invalid alignment mode: '{other}'")),
        }
    }
}

/// Configuration for Kafka source watermark alignment.
///
/// When multiple Kafka sources are used in a join, alignment ensures
/// fast sources don't get too far ahead of slow sources.
#[derive(Debug, Clone)]
pub struct KafkaAlignmentConfig {
    /// Alignment group identifier (sources with same ID coordinate).
    pub group_id: String,
    /// Maximum allowed drift between fastest and slowest source.
    pub max_drift: Duration,
    /// Enforcement mode.
    pub mode: KafkaAlignmentMode,
}

impl KafkaAlignmentConfig {
    /// Creates a new alignment config with defaults.
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            max_drift: Duration::from_secs(300), // 5 minutes
            mode: KafkaAlignmentMode::Pause,
        }
    }

    /// Sets the maximum drift.
    #[must_use]
    pub fn with_max_drift(mut self, max_drift: Duration) -> Self {
        self.max_drift = max_drift;
        self
    }

    /// Sets the enforcement mode.
    #[must_use]
    pub fn with_mode(mut self, mode: KafkaAlignmentMode) -> Self {
        self.mode = mode;
        self
    }
}

impl Default for KafkaAlignmentConfig {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Result of an alignment check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentCheckResult {
    /// Continue processing normally.
    Continue,
    /// Source should pause (too far ahead).
    Pause,
    /// Source can resume (caught up).
    Resume,
    /// Event should be dropped (`DropExcess` mode).
    Drop,
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

        let snapshot = tracker.metrics().snapshot();
        assert_eq!(snapshot.updates, 3);
        assert_eq!(snapshot.idle_transitions, 1);
        assert_eq!(snapshot.active_transitions, 1);
    }

    #[test]
    fn test_alignment_mode_parsing() {
        assert_eq!(
            "pause".parse::<KafkaAlignmentMode>().unwrap(),
            KafkaAlignmentMode::Pause
        );
        assert_eq!(
            "warn-only".parse::<KafkaAlignmentMode>().unwrap(),
            KafkaAlignmentMode::WarnOnly
        );
        assert_eq!(
            "drop-excess".parse::<KafkaAlignmentMode>().unwrap(),
            KafkaAlignmentMode::DropExcess
        );
        assert!("invalid".parse::<KafkaAlignmentMode>().is_err());
    }

    #[test]
    fn test_alignment_config() {
        let config = KafkaAlignmentConfig::new("test-group")
            .with_max_drift(Duration::from_secs(60))
            .with_mode(KafkaAlignmentMode::WarnOnly);

        assert_eq!(config.group_id, "test-group");
        assert_eq!(config.max_drift, Duration::from_secs(60));
        assert_eq!(config.mode, KafkaAlignmentMode::WarnOnly);
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
}
