//! # Per-Partition Watermark Tracking
//!
//! Extends the watermark system to track watermarks per source partition rather
//! than per source. This is critical for Kafka integration where each partition
//! may have different event-time progress.
//!
//! ## Problem
//!
//! With per-source watermarks, a single slow partition blocks the entire source:
//!
//! ```text
//! Source "orders" (Kafka topic with 4 partitions):
//!   Partition 0: ████████████████ (active, ts: 10:05)
//!   Partition 1: ████░░░░░░░░░░░░ (idle since 10:01)  ← BLOCKS ENTIRE SOURCE
//!   Partition 2: ████████████████ (active, ts: 10:06)
//!   Partition 3: ████████████████ (active, ts: 10:04)
//!
//! Source Watermark: stuck at 10:01 (because of Partition 1)
//! ```
//!
//! ## Solution
//!
//! Track watermarks at partition granularity with per-partition idle detection:
//!
//! ```rust
//! use laminar_core::time::{PartitionedWatermarkTracker, PartitionId, Watermark};
//!
//! let mut tracker = PartitionedWatermarkTracker::new();
//!
//! // Register Kafka source with 4 partitions
//! tracker.register_source(0, 4);
//!
//! // Update individual partitions
//! tracker.update_partition(PartitionId::new(0, 0), 5000);
//! tracker.update_partition(PartitionId::new(0, 1), 3000);
//! tracker.update_partition(PartitionId::new(0, 2), 4000);
//! tracker.update_partition(PartitionId::new(0, 3), 4500);
//!
//! // Combined watermark is minimum (3000)
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
//!
//! // Mark partition 1 as idle
//! tracker.mark_partition_idle(PartitionId::new(0, 1));
//!
//! // Now combined watermark advances to 4000 (min of active partitions)
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::Watermark;

/// Partition identifier within a source.
///
/// Uniquely identifies a partition by combining source ID and partition number.
/// For Kafka sources, the partition number corresponds to the Kafka partition.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PartitionId {
    /// Source identifier (index in the source registry)
    pub source_id: usize,
    /// Partition number within the source
    pub partition: u32,
}

impl PartitionId {
    /// Creates a new partition identifier.
    #[inline]
    #[must_use]
    pub const fn new(source_id: usize, partition: u32) -> Self {
        Self {
            source_id,
            partition,
        }
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.source_id, self.partition)
    }
}

/// Per-partition watermark state.
///
/// Tracks the watermark and activity status for a single partition.
#[derive(Debug, Clone)]
pub struct PartitionWatermarkState {
    /// Current watermark for this partition
    pub watermark: i64,
    /// Last event time seen
    pub last_event_time: i64,
    /// Last activity timestamp (wall clock)
    pub last_activity: Instant,
    /// Whether this partition is marked idle
    pub is_idle: bool,
    /// Core assignment (for thread-per-core routing)
    pub assigned_core: Option<usize>,
}

impl PartitionWatermarkState {
    /// Creates new partition state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watermark: i64::MIN,
            last_event_time: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
            assigned_core: None,
        }
    }

    /// Creates partition state with a specific core assignment.
    #[must_use]
    pub fn with_core(core_id: usize) -> Self {
        Self {
            watermark: i64::MIN,
            last_event_time: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
            assigned_core: Some(core_id),
        }
    }
}

impl Default for PartitionWatermarkState {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics for partitioned watermark tracking.
#[derive(Debug, Clone, Default)]
pub struct PartitionedWatermarkMetrics {
    /// Total partitions tracked
    pub total_partitions: usize,
    /// Currently active (non-idle) partitions
    pub active_partitions: usize,
    /// Idle partitions
    pub idle_partitions: usize,
    /// Watermark advancements
    pub watermark_advances: u64,
    /// Partition rebalances (adds/removes)
    pub rebalances: u64,
}

impl PartitionedWatermarkMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Errors that can occur in partitioned watermark operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WatermarkError {
    /// Partition not registered
    #[error("Unknown partition: {0}")]
    UnknownPartition(PartitionId),

    /// Source not found
    #[error("Source not found: {0}")]
    SourceNotFound(usize),

    /// Invalid partition number
    #[error("Invalid partition {partition} for source {source_id} (max: {max_partition})")]
    InvalidPartition {
        /// Source ID
        source_id: usize,
        /// Requested partition
        partition: u32,
        /// Maximum valid partition
        max_partition: u32,
    },

    /// Partition already exists
    #[error("Partition already exists: {0}")]
    PartitionExists(PartitionId),
}

/// Tracks watermarks across partitions within sources.
///
/// Extends `WatermarkTracker` to support partition-level granularity.
/// The combined watermark is the minimum across all active partitions.
///
/// # Thread Safety
///
/// This tracker is NOT thread-safe. For thread-per-core architectures,
/// use [`CoreWatermarkState`] for per-core tracking and coordinate
/// via the runtime.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{PartitionedWatermarkTracker, PartitionId, Watermark};
///
/// let mut tracker = PartitionedWatermarkTracker::new();
///
/// // Register Kafka source with 4 partitions
/// tracker.register_source(0, 4);
///
/// // Update individual partitions
/// tracker.update_partition(PartitionId::new(0, 0), 5000);
/// tracker.update_partition(PartitionId::new(0, 1), 3000);
/// tracker.update_partition(PartitionId::new(0, 2), 4000);
/// tracker.update_partition(PartitionId::new(0, 3), 4500);
///
/// // Combined watermark is minimum (3000)
/// assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
///
/// // Mark partition 1 as idle
/// tracker.mark_partition_idle(PartitionId::new(0, 1));
///
/// // Now combined watermark advances to 4000 (min of active partitions)
/// assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
/// ```
#[derive(Debug)]
pub struct PartitionedWatermarkTracker {
    /// Per-partition state, keyed by `PartitionId`
    partitions: HashMap<PartitionId, PartitionWatermarkState>,

    /// Number of partitions per source (for bounds checking)
    source_partition_counts: Vec<usize>,

    /// Combined watermark across all active partitions
    combined_watermark: i64,

    /// Idle timeout for automatic idle detection
    idle_timeout: Duration,

    /// Metrics
    metrics: PartitionedWatermarkMetrics,
}

impl PartitionedWatermarkTracker {
    /// Default idle timeout (30 seconds).
    pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Creates a new partitioned watermark tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            source_partition_counts: Vec::new(),
            combined_watermark: i64::MIN,
            idle_timeout: Self::DEFAULT_IDLE_TIMEOUT,
            metrics: PartitionedWatermarkMetrics::new(),
        }
    }

    /// Creates a new tracker with custom idle timeout.
    #[must_use]
    pub fn with_idle_timeout(idle_timeout: Duration) -> Self {
        Self {
            partitions: HashMap::new(),
            source_partition_counts: Vec::new(),
            combined_watermark: i64::MIN,
            idle_timeout,
            metrics: PartitionedWatermarkMetrics::new(),
        }
    }

    /// Registers a source with the specified number of partitions.
    ///
    /// Creates partition state for each partition in the source.
    /// Call this when a source is created or when Kafka partitions are assigned.
    pub fn register_source(&mut self, source_id: usize, num_partitions: usize) {
        // Expand source_partition_counts if needed
        while self.source_partition_counts.len() <= source_id {
            self.source_partition_counts.push(0);
        }

        self.source_partition_counts[source_id] = num_partitions;

        // Create partition state for each partition
        #[allow(clippy::cast_possible_truncation)]
        // Partition count bounded by Kafka max (< u32::MAX)
        for partition in 0..num_partitions {
            let pid = PartitionId::new(source_id, partition as u32);
            self.partitions.entry(pid).or_default();
        }

        self.update_metrics();
    }

    /// Adds a partition to a source (for Kafka rebalancing).
    ///
    /// # Errors
    ///
    /// Returns an error if the partition already exists.
    pub fn add_partition(&mut self, partition: PartitionId) -> Result<(), WatermarkError> {
        if self.partitions.contains_key(&partition) {
            return Err(WatermarkError::PartitionExists(partition));
        }

        // Expand source_partition_counts if needed
        while self.source_partition_counts.len() <= partition.source_id {
            self.source_partition_counts.push(0);
        }

        // Update partition count
        let current_count = self.source_partition_counts[partition.source_id];
        if partition.partition as usize >= current_count {
            self.source_partition_counts[partition.source_id] = partition.partition as usize + 1;
        }

        self.partitions
            .insert(partition, PartitionWatermarkState::new());
        self.metrics.rebalances += 1;
        self.update_metrics();

        Ok(())
    }

    /// Removes a partition from tracking (for Kafka rebalancing).
    ///
    /// Returns the partition state if it existed.
    pub fn remove_partition(&mut self, partition: PartitionId) -> Option<PartitionWatermarkState> {
        let state = self.partitions.remove(&partition);
        if state.is_some() {
            self.metrics.rebalances += 1;
            self.update_metrics();
            // Recalculate combined watermark
            self.recalculate_combined();
        }
        state
    }

    /// Updates the watermark for a specific partition.
    ///
    /// This also marks the partition as active and updates its last activity time.
    ///
    /// # Returns
    ///
    /// `Some(Watermark)` if the combined watermark advances.
    #[inline]
    pub fn update_partition(
        &mut self,
        partition: PartitionId,
        watermark: i64,
    ) -> Option<Watermark> {
        if let Some(state) = self.partitions.get_mut(&partition) {
            // Mark as active
            if state.is_idle {
                state.is_idle = false;
                self.metrics.active_partitions += 1;
                self.metrics.idle_partitions = self.metrics.idle_partitions.saturating_sub(1);
            }
            state.last_activity = Instant::now();

            // Update watermark if it advances
            if watermark > state.watermark {
                state.watermark = watermark;
                state.last_event_time = watermark;
                return self.try_advance_combined();
            }
        }
        None
    }

    /// Updates watermark from an event timestamp (applies bounded lateness).
    ///
    /// Convenience method that subtracts the configured lateness.
    #[inline]
    pub fn update_partition_from_event(
        &mut self,
        partition: PartitionId,
        event_time: i64,
        max_lateness: i64,
    ) -> Option<Watermark> {
        let watermark = event_time.saturating_sub(max_lateness);
        self.update_partition(partition, watermark)
    }

    /// Marks a partition as idle, excluding it from watermark calculation.
    ///
    /// # Returns
    ///
    /// `Some(Watermark)` if marking idle causes the combined watermark to advance.
    pub fn mark_partition_idle(&mut self, partition: PartitionId) -> Option<Watermark> {
        if let Some(state) = self.partitions.get_mut(&partition) {
            if !state.is_idle {
                state.is_idle = true;
                self.metrics.idle_partitions += 1;
                self.metrics.active_partitions = self.metrics.active_partitions.saturating_sub(1);
                return self.try_advance_combined();
            }
        }
        None
    }

    /// Marks a partition as active again.
    ///
    /// Called automatically when `update_partition` is called, but can be
    /// called explicitly to reactivate a partition before receiving events.
    pub fn mark_partition_active(&mut self, partition: PartitionId) {
        if let Some(state) = self.partitions.get_mut(&partition) {
            if state.is_idle {
                state.is_idle = false;
                state.last_activity = Instant::now();
                self.metrics.active_partitions += 1;
                self.metrics.idle_partitions = self.metrics.idle_partitions.saturating_sub(1);
            }
        }
    }

    /// Checks for partitions that have been idle longer than the timeout.
    ///
    /// Should be called periodically from Ring 1.
    ///
    /// # Returns
    ///
    /// `Some(Watermark)` if marking idle partitions causes the combined watermark to advance.
    pub fn check_idle_partitions(&mut self) -> Option<Watermark> {
        let mut any_marked = false;

        for state in self.partitions.values_mut() {
            if !state.is_idle && state.last_activity.elapsed() >= self.idle_timeout {
                state.is_idle = true;
                any_marked = true;
            }
        }

        if any_marked {
            self.update_metrics();
            self.try_advance_combined()
        } else {
            None
        }
    }

    /// Returns the current combined watermark.
    #[inline]
    #[must_use]
    pub fn current_watermark(&self) -> Option<Watermark> {
        if self.combined_watermark == i64::MIN {
            None
        } else {
            Some(Watermark::new(self.combined_watermark))
        }
    }

    /// Returns the watermark for a specific partition.
    #[must_use]
    pub fn partition_watermark(&self, partition: PartitionId) -> Option<i64> {
        self.partitions.get(&partition).map(|s| s.watermark)
    }

    /// Returns the watermark for a source (minimum across its partitions).
    #[must_use]
    pub fn source_watermark(&self, source_id: usize) -> Option<i64> {
        let mut min_watermark = i64::MAX;
        let mut found = false;

        for (pid, state) in &self.partitions {
            if pid.source_id == source_id && !state.is_idle {
                found = true;
                min_watermark = min_watermark.min(state.watermark);
            }
        }

        if found && min_watermark != i64::MAX {
            Some(min_watermark)
        } else {
            None
        }
    }

    /// Returns whether a partition is idle.
    #[must_use]
    pub fn is_partition_idle(&self, partition: PartitionId) -> bool {
        self.partitions.get(&partition).is_some_and(|s| s.is_idle)
    }

    /// Returns the number of active partitions for a source.
    #[must_use]
    pub fn active_partition_count(&self, source_id: usize) -> usize {
        self.partitions
            .iter()
            .filter(|(pid, state)| pid.source_id == source_id && !state.is_idle)
            .count()
    }

    /// Returns the total number of partitions for a source.
    #[must_use]
    pub fn partition_count(&self, source_id: usize) -> usize {
        self.source_partition_counts
            .get(source_id)
            .copied()
            .unwrap_or(0)
    }

    /// Returns metrics.
    #[must_use]
    pub fn metrics(&self) -> &PartitionedWatermarkMetrics {
        &self.metrics
    }

    /// Returns the number of sources registered.
    #[must_use]
    pub fn num_sources(&self) -> usize {
        self.source_partition_counts.len()
    }

    /// Assigns a partition to a core (for thread-per-core routing).
    pub fn assign_partition_to_core(&mut self, partition: PartitionId, core_id: usize) {
        if let Some(state) = self.partitions.get_mut(&partition) {
            state.assigned_core = Some(core_id);
        }
    }

    /// Returns the core assignment for a partition.
    #[must_use]
    pub fn partition_core(&self, partition: PartitionId) -> Option<usize> {
        self.partitions
            .get(&partition)
            .and_then(|s| s.assigned_core)
    }

    /// Returns all partitions assigned to a specific core.
    #[must_use]
    pub fn partitions_for_core(&self, core_id: usize) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .filter_map(|(pid, state)| {
                if state.assigned_core == Some(core_id) {
                    Some(*pid)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the idle timeout.
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Sets the idle timeout.
    pub fn set_idle_timeout(&mut self, timeout: Duration) {
        self.idle_timeout = timeout;
    }

    /// Returns partition state for inspection.
    #[must_use]
    pub fn partition_state(&self, partition: PartitionId) -> Option<&PartitionWatermarkState> {
        self.partitions.get(&partition)
    }

    /// Tries to advance the combined watermark.
    fn try_advance_combined(&mut self) -> Option<Watermark> {
        let new_combined = self.calculate_combined();

        if new_combined > self.combined_watermark && new_combined != i64::MAX {
            self.combined_watermark = new_combined;
            self.metrics.watermark_advances += 1;
            Some(Watermark::new(new_combined))
        } else {
            None
        }
    }

    /// Recalculates the combined watermark from scratch.
    fn recalculate_combined(&mut self) {
        let new_combined = self.calculate_combined();
        if new_combined != i64::MAX {
            self.combined_watermark = new_combined;
        }
    }

    /// Calculates the combined watermark.
    fn calculate_combined(&self) -> i64 {
        let mut min_watermark = i64::MAX;
        let mut has_active = false;

        for state in self.partitions.values() {
            if !state.is_idle {
                has_active = true;
                min_watermark = min_watermark.min(state.watermark);
            }
        }

        // If all partitions are idle, use the max watermark to allow progress
        if !has_active {
            min_watermark = self
                .partitions
                .values()
                .map(|s| s.watermark)
                .max()
                .unwrap_or(i64::MIN);
        }

        min_watermark
    }

    /// Updates metrics counts.
    fn update_metrics(&mut self) {
        self.metrics.total_partitions = self.partitions.len();
        self.metrics.idle_partitions = self.partitions.values().filter(|s| s.is_idle).count();
        self.metrics.active_partitions =
            self.metrics.total_partitions - self.metrics.idle_partitions;
    }
}

impl Default for PartitionedWatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-core partition watermark aggregator.
///
/// Each core tracks watermarks for its assigned partitions.
/// The global tracker aggregates across cores.
///
/// This is used in thread-per-core architectures where each core
/// processes a subset of partitions.
#[derive(Debug)]
pub struct CoreWatermarkState {
    /// Partitions assigned to this core
    assigned_partitions: Vec<PartitionId>,

    /// Per-partition watermarks (parallel to `assigned_partitions`)
    partition_watermarks: Vec<i64>,

    /// Local watermark (minimum across assigned partitions)
    local_watermark: i64,

    /// Idle status for each partition (parallel to `assigned_partitions`)
    idle_status: Vec<bool>,

    /// Core ID
    core_id: usize,
}

impl CoreWatermarkState {
    /// Creates a new per-core watermark state.
    #[must_use]
    pub fn new(core_id: usize) -> Self {
        Self {
            assigned_partitions: Vec::new(),
            partition_watermarks: Vec::new(),
            local_watermark: i64::MIN,
            idle_status: Vec::new(),
            core_id,
        }
    }

    /// Creates with pre-assigned partitions.
    #[must_use]
    pub fn with_partitions(core_id: usize, partitions: Vec<PartitionId>) -> Self {
        let count = partitions.len();
        Self {
            assigned_partitions: partitions,
            partition_watermarks: vec![i64::MIN; count],
            local_watermark: i64::MIN,
            idle_status: vec![false; count],
            core_id,
        }
    }

    /// Assigns a partition to this core.
    pub fn assign_partition(&mut self, partition: PartitionId) {
        if !self.assigned_partitions.contains(&partition) {
            self.assigned_partitions.push(partition);
            self.partition_watermarks.push(i64::MIN);
            self.idle_status.push(false);
        }
    }

    /// Removes a partition from this core.
    pub fn remove_partition(&mut self, partition: PartitionId) -> bool {
        if let Some(idx) = self
            .assigned_partitions
            .iter()
            .position(|p| *p == partition)
        {
            self.assigned_partitions.swap_remove(idx);
            self.partition_watermarks.swap_remove(idx);
            self.idle_status.swap_remove(idx);
            self.recalculate_local();
            true
        } else {
            false
        }
    }

    /// Updates a partition watermark on this core.
    ///
    /// # Returns
    ///
    /// `Some(i64)` with the new local watermark if it advances.
    #[inline]
    pub fn update_partition(&mut self, partition: PartitionId, watermark: i64) -> Option<i64> {
        if let Some(idx) = self
            .assigned_partitions
            .iter()
            .position(|p| *p == partition)
        {
            if watermark > self.partition_watermarks[idx] {
                self.partition_watermarks[idx] = watermark;
                self.idle_status[idx] = false;

                // Check if local watermark advances
                let new_local = self.calculate_local();
                if new_local > self.local_watermark {
                    self.local_watermark = new_local;
                    return Some(new_local);
                }
            }
        }
        None
    }

    /// Marks a partition as idle on this core.
    pub fn mark_idle(&mut self, partition: PartitionId) -> Option<i64> {
        if let Some(idx) = self
            .assigned_partitions
            .iter()
            .position(|p| *p == partition)
        {
            if !self.idle_status[idx] {
                self.idle_status[idx] = true;

                // Recalculate local watermark
                let new_local = self.calculate_local();
                if new_local > self.local_watermark {
                    self.local_watermark = new_local;
                    return Some(new_local);
                }
            }
        }
        None
    }

    /// Returns the local (per-core) watermark.
    #[inline]
    #[must_use]
    pub fn local_watermark(&self) -> i64 {
        self.local_watermark
    }

    /// Returns the core ID.
    #[must_use]
    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Returns the assigned partitions.
    #[must_use]
    pub fn assigned_partitions(&self) -> &[PartitionId] {
        &self.assigned_partitions
    }

    /// Returns the number of assigned partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.assigned_partitions.len()
    }

    /// Calculates the local watermark.
    fn calculate_local(&self) -> i64 {
        let mut min = i64::MAX;
        let mut has_active = false;

        for (idx, &wm) in self.partition_watermarks.iter().enumerate() {
            if !self.idle_status[idx] {
                has_active = true;
                min = min.min(wm);
            }
        }

        if !has_active {
            // All idle - use max
            self.partition_watermarks
                .iter()
                .copied()
                .max()
                .unwrap_or(i64::MIN)
        } else if min == i64::MAX {
            i64::MIN
        } else {
            min
        }
    }

    /// Recalculates the local watermark from scratch.
    fn recalculate_local(&mut self) {
        self.local_watermark = self.calculate_local();
    }
}

/// Collects watermarks from multiple cores and computes the global watermark.
///
/// This is used by the thread-per-core runtime coordinator to aggregate
/// watermarks across all cores.
#[derive(Debug)]
pub struct GlobalWatermarkCollector {
    /// Per-core watermarks
    core_watermarks: Vec<i64>,

    /// Global watermark (minimum across all cores)
    global_watermark: i64,
}

impl GlobalWatermarkCollector {
    /// Creates a new collector for the given number of cores.
    #[must_use]
    pub fn new(num_cores: usize) -> Self {
        Self {
            core_watermarks: vec![i64::MIN; num_cores],
            global_watermark: i64::MIN,
        }
    }

    /// Updates the watermark for a specific core.
    ///
    /// # Returns
    ///
    /// `Some(Watermark)` if the global watermark advances.
    #[inline]
    pub fn update_core(&mut self, core_id: usize, watermark: i64) -> Option<Watermark> {
        if core_id < self.core_watermarks.len() {
            self.core_watermarks[core_id] = watermark;

            // Calculate new global minimum
            let new_global = self
                .core_watermarks
                .iter()
                .copied()
                .min()
                .unwrap_or(i64::MIN);

            if new_global > self.global_watermark && new_global != i64::MIN {
                self.global_watermark = new_global;
                return Some(Watermark::new(new_global));
            }
        }
        None
    }

    /// Returns the current global watermark.
    #[must_use]
    pub fn global_watermark(&self) -> Option<Watermark> {
        if self.global_watermark == i64::MIN {
            None
        } else {
            Some(Watermark::new(self.global_watermark))
        }
    }

    /// Returns the watermark for a specific core.
    #[must_use]
    pub fn core_watermark(&self, core_id: usize) -> Option<i64> {
        self.core_watermarks.get(core_id).copied()
    }

    /// Returns the number of cores.
    #[must_use]
    pub fn num_cores(&self) -> usize {
        self.core_watermarks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_id_creation() {
        let pid = PartitionId::new(1, 3);
        assert_eq!(pid.source_id, 1);
        assert_eq!(pid.partition, 3);
    }

    #[test]
    fn test_partition_id_equality() {
        let p1 = PartitionId::new(1, 2);
        let p2 = PartitionId::new(1, 2);
        let p3 = PartitionId::new(1, 3);

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_partition_id_display() {
        let pid = PartitionId::new(2, 5);
        assert_eq!(format!("{pid}"), "2:5");
    }

    #[test]
    fn test_partitioned_tracker_single_partition_updates_watermark() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 1);

        let wm = tracker.update_partition(PartitionId::new(0, 0), 1000);
        assert_eq!(wm, Some(Watermark::new(1000)));
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(1000)));
    }

    #[test]
    fn test_partitioned_tracker_multiple_partitions_uses_minimum() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 4);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 3000);
        tracker.update_partition(PartitionId::new(0, 2), 4000);
        tracker.update_partition(PartitionId::new(0, 3), 4500);

        assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
    }

    #[test]
    fn test_partitioned_tracker_idle_partition_excluded_from_min() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 4);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 1000); // Slow partition
        tracker.update_partition(PartitionId::new(0, 2), 4000);
        tracker.update_partition(PartitionId::new(0, 3), 4500);

        assert_eq!(tracker.current_watermark(), Some(Watermark::new(1000)));

        // Mark slow partition as idle
        let wm = tracker.mark_partition_idle(PartitionId::new(0, 1));
        assert_eq!(wm, Some(Watermark::new(4000)));
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
    }

    #[test]
    fn test_partitioned_tracker_all_idle_uses_max() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 3000);

        tracker.mark_partition_idle(PartitionId::new(0, 0));
        let wm = tracker.mark_partition_idle(PartitionId::new(0, 1));

        // When all idle, use max to allow progress
        assert_eq!(wm, Some(Watermark::new(5000)));
    }

    #[test]
    fn test_partitioned_tracker_partition_reactivated_on_update() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 3000);

        // Mark partition 1 idle
        tracker.mark_partition_idle(PartitionId::new(0, 1));
        assert!(tracker.is_partition_idle(PartitionId::new(0, 1)));
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(5000)));

        // Update partition 1 - should reactivate it
        tracker.update_partition(PartitionId::new(0, 1), 4000);
        assert!(!tracker.is_partition_idle(PartitionId::new(0, 1)));

        // Watermark should now be min of both (4000)
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(5000))); // Still 5000 because combined can't regress
    }

    #[test]
    fn test_partitioned_tracker_add_partition_during_operation() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 4000);

        // Add a new partition (Kafka rebalance)
        tracker.add_partition(PartitionId::new(0, 2)).unwrap();

        // New partition starts at MIN, so watermark shouldn't advance yet
        assert_eq!(tracker.partition_count(0), 3);

        // Update new partition
        tracker.update_partition(PartitionId::new(0, 2), 3000);
        // Watermark is now 3000 (min of all)
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000))); // Can't regress
    }

    #[test]
    fn test_partitioned_tracker_remove_partition_recalculates_watermark() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 3);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 2000); // Slowest
        tracker.update_partition(PartitionId::new(0, 2), 4000);

        assert_eq!(tracker.current_watermark(), Some(Watermark::new(2000)));

        // Remove slowest partition (e.g., Kafka rebalance)
        let state = tracker.remove_partition(PartitionId::new(0, 1));
        assert!(state.is_some());
        assert_eq!(state.unwrap().watermark, 2000);

        // Watermark advances to 4000 (min of remaining: 5000, 4000)
        // This is correct - removing a slow partition allows progress
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
    }

    #[test]
    fn test_partitioned_tracker_check_idle_marks_stale_partitions() {
        let mut tracker = PartitionedWatermarkTracker::with_idle_timeout(Duration::from_millis(10));
        tracker.register_source(0, 2);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 3000);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(20));

        // Only update partition 0
        tracker.update_partition(PartitionId::new(0, 0), 6000);

        // Check for idle - partition 1 should be marked idle
        let wm = tracker.check_idle_partitions();

        assert!(tracker.is_partition_idle(PartitionId::new(0, 1)));
        // Watermark should advance since idle partition is excluded
        assert!(wm.is_some() || tracker.current_watermark() == Some(Watermark::new(6000)));
    }

    #[test]
    fn test_partitioned_tracker_source_watermark_aggregates_partitions() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);
        tracker.register_source(1, 2);

        tracker.update_partition(PartitionId::new(0, 0), 5000);
        tracker.update_partition(PartitionId::new(0, 1), 3000);
        tracker.update_partition(PartitionId::new(1, 0), 4000);
        tracker.update_partition(PartitionId::new(1, 1), 6000);

        assert_eq!(tracker.source_watermark(0), Some(3000));
        assert_eq!(tracker.source_watermark(1), Some(4000));
    }

    #[test]
    fn test_partitioned_tracker_metrics_accurate() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 4);

        tracker.update_partition(PartitionId::new(0, 0), 1000);
        tracker.update_partition(PartitionId::new(0, 1), 2000);

        let metrics = tracker.metrics();
        assert_eq!(metrics.total_partitions, 4);
        assert_eq!(metrics.active_partitions, 4);
        assert_eq!(metrics.idle_partitions, 0);

        tracker.mark_partition_idle(PartitionId::new(0, 2));

        let metrics = tracker.metrics();
        assert_eq!(metrics.idle_partitions, 1);
        assert_eq!(metrics.active_partitions, 3);
    }

    #[test]
    fn test_partitioned_tracker_core_assignment() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 4);

        tracker.assign_partition_to_core(PartitionId::new(0, 0), 0);
        tracker.assign_partition_to_core(PartitionId::new(0, 1), 0);
        tracker.assign_partition_to_core(PartitionId::new(0, 2), 1);
        tracker.assign_partition_to_core(PartitionId::new(0, 3), 1);

        assert_eq!(tracker.partition_core(PartitionId::new(0, 0)), Some(0));
        assert_eq!(tracker.partition_core(PartitionId::new(0, 2)), Some(1));

        let core0_partitions = tracker.partitions_for_core(0);
        assert_eq!(core0_partitions.len(), 2);
    }

    #[test]
    fn test_partitioned_tracker_multiple_sources() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);
        tracker.register_source(1, 3);

        assert_eq!(tracker.num_sources(), 2);
        assert_eq!(tracker.partition_count(0), 2);
        assert_eq!(tracker.partition_count(1), 3);
    }

    #[test]
    fn test_partitioned_tracker_update_from_event() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 1);

        // Event time 5000, max lateness 1000 -> watermark 4000
        let wm = tracker.update_partition_from_event(PartitionId::new(0, 0), 5000, 1000);
        assert_eq!(wm, Some(Watermark::new(4000)));
    }

    #[test]
    fn test_partitioned_tracker_add_partition_error() {
        let mut tracker = PartitionedWatermarkTracker::new();
        tracker.register_source(0, 2);

        // Adding existing partition should fail
        let result = tracker.add_partition(PartitionId::new(0, 0));
        assert!(matches!(result, Err(WatermarkError::PartitionExists(_))));
    }

    #[test]
    fn test_core_watermark_state_creation() {
        let state = CoreWatermarkState::new(0);
        assert_eq!(state.core_id(), 0);
        assert_eq!(state.partition_count(), 0);
        assert_eq!(state.local_watermark(), i64::MIN);
    }

    #[test]
    fn test_core_watermark_state_with_partitions() {
        let partitions = vec![PartitionId::new(0, 0), PartitionId::new(0, 1)];
        let state = CoreWatermarkState::with_partitions(1, partitions);

        assert_eq!(state.core_id(), 1);
        assert_eq!(state.partition_count(), 2);
    }

    #[test]
    fn test_core_watermark_state_update() {
        let mut state = CoreWatermarkState::with_partitions(
            0,
            vec![PartitionId::new(0, 0), PartitionId::new(0, 1)],
        );

        // First update - local watermark is still MIN because partition 1 is at MIN
        let wm = state.update_partition(PartitionId::new(0, 0), 5000);
        assert!(wm.is_none()); // Can't advance - other partition still at MIN
        assert_eq!(state.local_watermark(), i64::MIN);

        // Second update - now both partitions have values
        let wm = state.update_partition(PartitionId::new(0, 1), 3000);
        assert_eq!(wm, Some(3000)); // Local watermark advances to min of both
        assert_eq!(state.local_watermark(), 3000);

        // Update partition 0 again - no change to local (still 3000)
        let wm = state.update_partition(PartitionId::new(0, 0), 6000);
        assert!(wm.is_none());
        assert_eq!(state.local_watermark(), 3000);

        // Update partition 1 - local advances
        let wm = state.update_partition(PartitionId::new(0, 1), 4000);
        assert_eq!(wm, Some(4000));
        assert_eq!(state.local_watermark(), 4000);
    }

    #[test]
    fn test_core_watermark_state_idle() {
        let mut state = CoreWatermarkState::with_partitions(
            0,
            vec![PartitionId::new(0, 0), PartitionId::new(0, 1)],
        );

        state.update_partition(PartitionId::new(0, 0), 5000);
        state.update_partition(PartitionId::new(0, 1), 2000);

        assert_eq!(state.local_watermark(), 2000);

        // Mark slow partition idle
        let wm = state.mark_idle(PartitionId::new(0, 1));
        assert_eq!(wm, Some(5000));
        assert_eq!(state.local_watermark(), 5000);
    }

    #[test]
    fn test_core_watermark_state_assign_remove() {
        let mut state = CoreWatermarkState::new(0);

        state.assign_partition(PartitionId::new(0, 0));
        state.assign_partition(PartitionId::new(0, 1));
        assert_eq!(state.partition_count(), 2);

        state.update_partition(PartitionId::new(0, 0), 5000);
        state.update_partition(PartitionId::new(0, 1), 3000);
        assert_eq!(state.local_watermark(), 3000);

        // Remove slow partition
        let removed = state.remove_partition(PartitionId::new(0, 1));
        assert!(removed);
        assert_eq!(state.partition_count(), 1);
        assert_eq!(state.local_watermark(), 5000);
    }

    #[test]
    fn test_global_collector_creation() {
        let collector = GlobalWatermarkCollector::new(4);
        assert_eq!(collector.num_cores(), 4);
        assert_eq!(collector.global_watermark(), None);
    }

    #[test]
    fn test_global_collector_update() {
        let mut collector = GlobalWatermarkCollector::new(3);

        collector.update_core(0, 5000);
        collector.update_core(1, 3000);
        let wm = collector.update_core(2, 4000);

        // Global is min of all (3000)
        assert_eq!(wm, Some(Watermark::new(3000)));
        assert_eq!(collector.global_watermark(), Some(Watermark::new(3000)));
    }

    #[test]
    fn test_global_collector_advancement() {
        let mut collector = GlobalWatermarkCollector::new(2);

        collector.update_core(0, 5000);
        collector.update_core(1, 3000);

        assert_eq!(collector.global_watermark(), Some(Watermark::new(3000)));

        // Advance the slower core
        let wm = collector.update_core(1, 4000);
        assert_eq!(wm, Some(Watermark::new(4000)));
    }

    #[test]
    fn test_global_collector_no_regression() {
        let mut collector = GlobalWatermarkCollector::new(2);

        collector.update_core(0, 5000);
        collector.update_core(1, 4000);

        // Try to go backwards (should not regress)
        let wm = collector.update_core(1, 3000);
        assert!(wm.is_none());
        // The core watermark is updated, but global doesn't regress
        assert_eq!(collector.core_watermark(1), Some(3000));
    }
}
