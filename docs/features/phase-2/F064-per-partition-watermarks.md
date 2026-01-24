# F064: Per-Partition Watermarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F064 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F010, F013 |
| **Blocks** | F025 (Kafka Source) |
| **Owner** | TBD |
| **Created** | 2026-01-24 |
| **Research** | [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) |

## Summary

Extend the watermark system to track watermarks per source partition rather than per source. This is critical for Kafka integration where each partition may have different event-time progress, and one slow partition should not block all others.

## Motivation

**From Research Section 4 (Idle Source Detection):**

> When a source/partition stops producing events, its watermark stalls, blocking the entire pipeline's progress.

**Current State:**
- `WatermarkTracker` tracks watermarks per *source* (e.g., source 0, source 1)
- Kafka sources have multiple *partitions* within a single source
- A single slow/idle partition blocks the entire source's watermark

**Problem Scenario:**
```
Source "orders" (Kafka topic with 4 partitions):
  Partition 0: ████████████████ (active, ts: 10:05)
  Partition 1: ████░░░░░░░░░░░░ (idle since 10:01)  ← BLOCKS ENTIRE SOURCE
  Partition 2: ████████████████ (active, ts: 10:06)
  Partition 3: ████████████████ (active, ts: 10:04)

Source Watermark: stuck at 10:01 (because of Partition 1)
```

**Solution:** Track watermarks at partition granularity with per-partition idle detection.

## Goals

1. Track watermarks per partition within a source
2. Per-partition idle detection and marking
3. Integrate with thread-per-core (F013) - partitions can be assigned to different cores
4. Support Kafka partition rebalancing (partitions added/removed)
5. Maintain backward compatibility with existing `WatermarkTracker` API

## Non-Goals

- Keyed watermarks (within partitions) - covered by F065
- Watermark alignment groups across sources - covered by F066
- Adaptive watermark delay - future enhancement

## Technical Design

### Architecture

**Ring:** Ring 0 (hot path) for watermark updates, Ring 1 for idle checking

**Crate:** `laminar-core/src/time/`

### Data Structures

```rust
/// Partition identifier within a source.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct PartitionId {
    pub source_id: usize,
    pub partition: u32,
}

impl PartitionId {
    pub fn new(source_id: usize, partition: u32) -> Self {
        Self { source_id, partition }
    }
}

/// Per-partition watermark state.
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
    /// Core assignment (for thread-per-core)
    pub assigned_core: Option<usize>,
}

impl PartitionWatermarkState {
    pub fn new() -> Self {
        Self {
            watermark: i64::MIN,
            last_event_time: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
            assigned_core: None,
        }
    }
}

/// Tracks watermarks across partitions within sources.
///
/// Extends `WatermarkTracker` to support partition-level granularity.
/// The combined watermark is the minimum across all active partitions.
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
pub struct PartitionedWatermarkTracker {
    /// Per-partition state, keyed by (source_id, partition)
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
```

### API/Interface

```rust
impl PartitionedWatermarkTracker {
    /// Creates a new partitioned watermark tracker.
    pub fn new() -> Self;

    /// Creates with custom idle timeout.
    pub fn with_idle_timeout(idle_timeout: Duration) -> Self;

    /// Registers a source with the specified number of partitions.
    ///
    /// Call this when a source is created or when Kafka partitions are assigned.
    pub fn register_source(&mut self, source_id: usize, num_partitions: usize);

    /// Adds a partition to a source (for Kafka rebalancing).
    pub fn add_partition(&mut self, partition: PartitionId) -> Result<(), WatermarkError>;

    /// Removes a partition from tracking (for Kafka rebalancing).
    pub fn remove_partition(&mut self, partition: PartitionId) -> Option<PartitionWatermarkState>;

    /// Updates the watermark for a specific partition.
    ///
    /// # Returns
    /// `Some(Watermark)` if the combined watermark advances.
    #[inline]
    pub fn update_partition(&mut self, partition: PartitionId, watermark: i64) -> Option<Watermark>;

    /// Updates watermark from an event timestamp (applies bounded lateness).
    ///
    /// Convenience method that subtracts the configured lateness.
    #[inline]
    pub fn update_partition_from_event(
        &mut self,
        partition: PartitionId,
        event_time: i64,
        max_lateness: i64,
    ) -> Option<Watermark>;

    /// Marks a partition as idle, excluding it from watermark calculation.
    pub fn mark_partition_idle(&mut self, partition: PartitionId) -> Option<Watermark>;

    /// Marks a partition as active again.
    pub fn mark_partition_active(&mut self, partition: PartitionId);

    /// Checks for partitions that have been idle longer than the timeout.
    ///
    /// Should be called periodically from Ring 1.
    pub fn check_idle_partitions(&mut self) -> Option<Watermark>;

    /// Returns the current combined watermark.
    pub fn current_watermark(&self) -> Option<Watermark>;

    /// Returns the watermark for a specific partition.
    pub fn partition_watermark(&self, partition: PartitionId) -> Option<i64>;

    /// Returns the watermark for a source (minimum across its partitions).
    pub fn source_watermark(&self, source_id: usize) -> Option<i64>;

    /// Returns whether a partition is idle.
    pub fn is_partition_idle(&self, partition: PartitionId) -> bool;

    /// Returns the number of active partitions for a source.
    pub fn active_partition_count(&self, source_id: usize) -> usize;

    /// Returns metrics.
    pub fn metrics(&self) -> &PartitionedWatermarkMetrics;

    /// Assigns a partition to a core (for thread-per-core routing).
    pub fn assign_partition_to_core(&mut self, partition: PartitionId, core_id: usize);

    /// Returns the core assignment for a partition.
    pub fn partition_core(&self, partition: PartitionId) -> Option<usize>;
}
```

### Thread-Per-Core Integration (F013)

```rust
/// Per-core partition watermark aggregator.
///
/// Each core tracks watermarks for its assigned partitions.
/// The global tracker aggregates across cores.
pub struct CoreWatermarkState {
    /// Partitions assigned to this core
    assigned_partitions: Vec<PartitionId>,

    /// Local watermark (minimum across assigned partitions)
    local_watermark: i64,

    /// Core ID
    core_id: usize,
}

impl CoreWatermarkState {
    /// Updates a partition watermark on this core.
    #[inline]
    pub fn update_partition(&mut self, partition: PartitionId, watermark: i64) -> Option<i64>;

    /// Returns the local (per-core) watermark.
    pub fn local_watermark(&self) -> i64;
}

/// Extension to ThreadPerCoreRuntime for watermark coordination.
impl ThreadPerCoreRuntime {
    /// Collects watermarks from all cores and computes global watermark.
    ///
    /// Called periodically from the coordinator.
    pub fn collect_watermarks(&self) -> Option<Watermark>;

    /// Broadcasts global watermark to all cores.
    ///
    /// Cores use this for window triggering.
    pub fn broadcast_watermark(&self, watermark: Watermark);
}
```

### Algorithm: Watermark Calculation

```
function calculate_combined_watermark():
    min_watermark = MAX_VALUE
    has_active = false

    for each partition in tracked_partitions:
        if not partition.is_idle:
            has_active = true
            min_watermark = min(min_watermark, partition.watermark)

    if not has_active:
        # All partitions idle - use max to allow progress
        min_watermark = max(all_partition_watermarks)

    if min_watermark > combined_watermark:
        combined_watermark = min_watermark
        return Some(Watermark(min_watermark))
    else:
        return None
```

### Idle Detection Algorithm

```
function check_idle_partitions():
    any_marked = false

    for each partition in tracked_partitions:
        if not partition.is_idle:
            if now() - partition.last_activity > idle_timeout:
                partition.is_idle = true
                any_marked = true

    if any_marked:
        return calculate_combined_watermark()
    else:
        return None
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `UnknownPartition` | Partition not registered | Register partition first |
| `SourceNotFound` | Source ID not registered | Register source first |
| `InvalidPartition` | Partition ID out of range | Check partition count |

## Test Plan

### Unit Tests

- [ ] `test_partitioned_tracker_single_partition_updates_watermark`
- [ ] `test_partitioned_tracker_multiple_partitions_uses_minimum`
- [ ] `test_partitioned_tracker_idle_partition_excluded_from_min`
- [ ] `test_partitioned_tracker_all_idle_uses_max`
- [ ] `test_partitioned_tracker_partition_reactivated_on_update`
- [ ] `test_partitioned_tracker_add_partition_during_operation`
- [ ] `test_partitioned_tracker_remove_partition_recalculates_watermark`
- [ ] `test_partitioned_tracker_check_idle_marks_stale_partitions`
- [ ] `test_partitioned_tracker_source_watermark_aggregates_partitions`
- [ ] `test_partitioned_tracker_metrics_accurate`

### Integration Tests

- [ ] Kafka source with multiple partitions advancing at different rates
- [ ] Partition rebalancing during stream processing
- [ ] Thread-per-core with partition assignment

### Benchmarks

- [ ] `bench_partition_watermark_update` - Target: <50ns per update
- [ ] `bench_partition_idle_check_100_partitions` - Target: <1μs
- [ ] `bench_combined_watermark_calculation_100_partitions` - Target: <500ns

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Partition update | < 50ns | Hot path, inline |
| Combined watermark calc | < 500ns | Called per update |
| Idle check (100 partitions) | < 1μs | Ring 1 periodic |
| Memory per partition | < 64 bytes | Fits cache line |

## Completion Checklist

- [ ] `PartitionedWatermarkTracker` implemented
- [ ] Per-partition idle detection working
- [ ] Thread-per-core integration (CoreWatermarkState)
- [ ] Partition rebalancing support
- [ ] Unit tests passing (10+ tests)
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] Integration with F025 (Kafka Source)

## Open Questions

- [ ] Should we support dynamic partition count changes, or require re-registration?
- [ ] How should we handle partitions that were never active (no events since registration)?

## References

- [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) - Section 4
- [F010: Watermarks](../phase-1/F010-watermarks.md)
- [F013: Thread-Per-Core Architecture](F013-thread-per-core.md)
- [F025: Kafka Source Connector](../phase-3/F025-kafka-source.md)
- [Apache Flink: Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/)
