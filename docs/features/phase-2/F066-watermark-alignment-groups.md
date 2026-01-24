# F066: Watermark Alignment Groups

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F066 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F010, F064 |
| **Owner** | TBD |
| **Created** | 2026-01-24 |
| **Research** | [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) |

## Summary

Implement watermark alignment groups to coordinate watermark progress across multiple sources, preventing unbounded state growth when sources have different processing speeds. Fast sources are paused when they get too far ahead of slow sources within the same alignment group.

## Motivation

**From Research Section 8 (Multi-Source Watermark Alignment):**

> When sources have different processing speeds, fast sources can cause:
> - Excessive state growth (buffering data waiting for slow sources)
> - Memory pressure on downstream operators
> - Uneven resource utilization

**The Problem:**

```
Without Alignment Groups:
┌─────────────────────────────────────────────────────────────────┐
│ Source A (fast): watermark = 10:30  ─┐                         │
│ Source B (slow): watermark = 10:00  ─┼─► Join Operator         │
│                                      │                         │
│ Drift: 30 minutes!                   │                         │
│                                      │                         │
│ Join must buffer ALL of Source A's   │                         │
│ events from 10:00-10:30 waiting      │                         │
│ for Source B to catch up             │                         │
│                                      │                         │
│ Result: Unbounded memory growth!     │                         │
└─────────────────────────────────────────────────────────────────┘
```

**With Alignment Groups:**

```
With Alignment Groups (max_drift = 5 minutes):
┌─────────────────────────────────────────────────────────────────┐
│ Source A: watermark = 10:05 ──► PAUSED (drift > max_drift)     │
│ Source B: watermark = 10:00 ──► Processing...                  │
│                                                                 │
│ When Source B reaches 10:00:                                    │
│   Source A resumes                                              │
│   Max buffered data: 5 minutes worth                           │
│                                                                 │
│ Result: Bounded memory, predictable resource usage             │
└─────────────────────────────────────────────────────────────────┘
```

## Goals

1. Group sources that need coordinated watermark progress
2. Enforce maximum watermark drift within a group
3. Pause fast sources when they exceed drift limit
4. Resume paused sources when slow sources catch up
5. Integrate with backpressure system (F013/F014)

## Non-Goals

- Cross-region alignment (requires distributed coordination)
- Automatic grouping (groups must be explicitly configured)
- Priority-based scheduling within groups

## Technical Design

### Architecture

**Ring:** Ring 1 for alignment coordination, Ring 0 for pause/resume signals

**Crate:** `laminar-core/src/time/`

### Data Structures

```rust
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Identifier for an alignment group.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AlignmentGroupId(pub String);

/// Configuration for a watermark alignment group.
#[derive(Debug, Clone)]
pub struct AlignmentGroupConfig {
    /// Group identifier
    pub group_id: AlignmentGroupId,
    /// Maximum allowed drift between fastest and slowest source
    pub max_drift: Duration,
    /// How often to check alignment (wall clock)
    pub update_interval: Duration,
    /// Whether to pause sources or just emit warnings
    pub enforcement_mode: EnforcementMode,
}

/// Enforcement mode for alignment groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementMode {
    /// Pause fast sources (recommended for production)
    Pause,
    /// Emit warnings but don't pause (for monitoring)
    WarnOnly,
    /// Drop events from fast sources that exceed drift
    DropExcess,
}

/// State for a source within an alignment group.
#[derive(Debug, Clone)]
pub struct AlignmentSourceState {
    /// Source identifier
    pub source_id: usize,
    /// Current watermark
    pub watermark: i64,
    /// Whether this source is currently paused
    pub is_paused: bool,
    /// Time when pause started (for metrics)
    pub pause_start: Option<Instant>,
    /// Total time spent paused
    pub total_pause_time: Duration,
    /// Events processed while paused (for DropExcess mode)
    pub events_dropped_while_paused: u64,
}

/// Manages watermark alignment across sources in a group.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{
///     WatermarkAlignmentGroup, AlignmentGroupConfig, AlignmentGroupId,
///     EnforcementMode, Watermark,
/// };
/// use std::time::Duration;
///
/// let config = AlignmentGroupConfig {
///     group_id: AlignmentGroupId("orders-payments".to_string()),
///     max_drift: Duration::from_secs(300), // 5 minutes
///     update_interval: Duration::from_secs(1),
///     enforcement_mode: EnforcementMode::Pause,
/// };
///
/// let mut group = WatermarkAlignmentGroup::new(config);
///
/// // Register sources
/// group.register_source(0); // orders stream
/// group.register_source(1); // payments stream
///
/// // Report watermarks
/// let action = group.report_watermark(0, 10_000); // orders at 10:00
/// assert_eq!(action, AlignmentAction::Continue);
///
/// let action = group.report_watermark(0, 310_000); // orders jumps to 10:05:10
/// assert_eq!(action, AlignmentAction::Pause); // Too far ahead!
///
/// // Source 0 should pause until source 1 catches up
/// let action = group.report_watermark(1, 100_000); // payments at 10:01:40
/// // Now drift is 210 seconds, still > 300, source 0 stays paused
///
/// let action = group.report_watermark(1, 200_000); // payments at 10:03:20
/// // Now drift is 110 seconds, < 300, source 0 can resume
/// assert!(group.should_resume(0));
/// ```
pub struct WatermarkAlignmentGroup {
    /// Configuration
    config: AlignmentGroupConfig,

    /// Per-source state
    sources: HashMap<usize, AlignmentSourceState>,

    /// Current minimum watermark in the group
    min_watermark: i64,

    /// Current maximum watermark in the group
    max_watermark: i64,

    /// Last alignment check time
    last_check: Instant,

    /// Metrics
    metrics: AlignmentGroupMetrics,
}

/// Metrics for an alignment group.
#[derive(Debug, Clone, Default)]
pub struct AlignmentGroupMetrics {
    /// Number of times sources were paused
    pub pause_events: u64,
    /// Number of times sources were resumed
    pub resume_events: u64,
    /// Total pause time across all sources
    pub total_pause_time: Duration,
    /// Maximum observed drift
    pub max_observed_drift: Duration,
    /// Current drift
    pub current_drift: Duration,
    /// Events dropped (in DropExcess mode)
    pub events_dropped: u64,
}

/// Action to take for a source based on alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentAction {
    /// Continue processing normally
    Continue,
    /// Pause this source (too far ahead)
    Pause,
    /// Resume this source (caught up)
    Resume,
    /// Drop this event (DropExcess mode)
    Drop,
    /// Warning only (WarnOnly mode)
    Warn { drift: Duration },
}
```

### API/Interface

```rust
impl WatermarkAlignmentGroup {
    /// Creates a new alignment group.
    pub fn new(config: AlignmentGroupConfig) -> Self;

    /// Registers a source with this alignment group.
    pub fn register_source(&mut self, source_id: usize);

    /// Removes a source from this alignment group.
    pub fn unregister_source(&mut self, source_id: usize);

    /// Reports a watermark update from a source.
    ///
    /// Returns the action the source should take.
    pub fn report_watermark(&mut self, source_id: usize, watermark: i64) -> AlignmentAction;

    /// Checks if a paused source should resume.
    pub fn should_resume(&self, source_id: usize) -> bool;

    /// Returns the current drift (max - min watermark).
    pub fn current_drift(&self) -> Duration;

    /// Returns whether a specific source is paused.
    pub fn is_paused(&self, source_id: usize) -> bool;

    /// Returns the minimum watermark in the group.
    pub fn min_watermark(&self) -> i64;

    /// Returns the maximum watermark in the group.
    pub fn max_watermark(&self) -> i64;

    /// Returns metrics for this group.
    pub fn metrics(&self) -> &AlignmentGroupMetrics;

    /// Performs periodic alignment check.
    ///
    /// Should be called from Ring 1 at the configured update_interval.
    /// Returns list of (source_id, action) pairs.
    pub fn check_alignment(&mut self) -> Vec<(usize, AlignmentAction)>;
}
```

### Coordinator for Multiple Groups

```rust
/// Manages multiple alignment groups.
pub struct AlignmentGroupCoordinator {
    /// Groups by ID
    groups: HashMap<AlignmentGroupId, WatermarkAlignmentGroup>,

    /// Source to group mapping (a source can be in one group)
    source_groups: HashMap<usize, AlignmentGroupId>,
}

impl AlignmentGroupCoordinator {
    /// Creates a new coordinator.
    pub fn new() -> Self;

    /// Adds an alignment group.
    pub fn add_group(&mut self, config: AlignmentGroupConfig);

    /// Assigns a source to a group.
    pub fn assign_source_to_group(
        &mut self,
        source_id: usize,
        group_id: &AlignmentGroupId,
    ) -> Result<(), AlignmentError>;

    /// Reports a watermark update.
    ///
    /// Returns the action for the source, or None if source not in any group.
    pub fn report_watermark(
        &mut self,
        source_id: usize,
        watermark: i64,
    ) -> Option<AlignmentAction>;

    /// Checks alignment for all groups.
    pub fn check_all_alignments(&mut self) -> Vec<(usize, AlignmentAction)>;

    /// Returns metrics for all groups.
    pub fn all_metrics(&self) -> HashMap<AlignmentGroupId, AlignmentGroupMetrics>;
}
```

### Backpressure Integration (F013/F014)

```rust
/// Extension trait for integrating alignment with backpressure.
pub trait AlignmentBackpressure {
    /// Called when a source should be paused due to alignment.
    fn pause_for_alignment(&mut self, source_id: usize);

    /// Called when a source can resume after alignment.
    fn resume_from_alignment(&mut self, source_id: usize);
}

impl AlignmentBackpressure for ThreadPerCoreRuntime {
    fn pause_for_alignment(&mut self, source_id: usize) {
        // Send pause signal to source's core via SPSC queue
        if let Some(core_id) = self.source_core_assignment(source_id) {
            self.send_to_core(core_id, CoreMessage::PauseSource(source_id));
        }
    }

    fn resume_from_alignment(&mut self, source_id: usize) {
        if let Some(core_id) = self.source_core_assignment(source_id) {
            self.send_to_core(core_id, CoreMessage::ResumeSource(source_id));
        }
    }
}
```

### SQL Configuration

```sql
-- Configure alignment group in CREATE SOURCE
CREATE SOURCE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    order_time TIMESTAMP,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'orders',
    -- Alignment group configuration
    alignment.group = 'order-payment-join',
    alignment.max_drift = '5 minutes',
    alignment.mode = 'pause'  -- or 'warn', 'drop'
);

CREATE SOURCE payments (
    payment_id BIGINT,
    order_id BIGINT,
    payment_time TIMESTAMP,
    WATERMARK FOR payment_time AS payment_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'payments',
    alignment.group = 'order-payment-join',
    alignment.max_drift = '5 minutes',
    alignment.mode = 'pause'
);

-- Now orders and payments will be coordinated
-- If orders gets 5+ minutes ahead, it will pause
```

### Algorithm: Alignment Check

```
function report_watermark(source_id, watermark):
    source = sources[source_id]
    source.watermark = watermark

    # Update min/max
    min_watermark = min(all active source watermarks)
    max_watermark = max(all active source watermarks)
    current_drift = max_watermark - min_watermark

    # Update metrics
    metrics.current_drift = current_drift
    metrics.max_observed_drift = max(metrics.max_observed_drift, current_drift)

    # Check if this source is too far ahead
    if watermark == max_watermark and current_drift > max_drift:
        match enforcement_mode:
            Pause:
                source.is_paused = true
                source.pause_start = now()
                metrics.pause_events += 1
                return AlignmentAction::Pause
            WarnOnly:
                return AlignmentAction::Warn(current_drift)
            DropExcess:
                source.events_dropped_while_paused += 1
                return AlignmentAction::Drop

    return AlignmentAction::Continue

function should_resume(source_id):
    source = sources[source_id]
    if not source.is_paused:
        return false

    # Can resume if drift is now within bounds
    drift_if_resumed = source.watermark - min_watermark
    return drift_if_resumed <= max_drift
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `SourceNotInGroup` | Source not registered | Register source first |
| `GroupNotFound` | Unknown group ID | Create group first |
| `SourceAlreadyInGroup` | Source assigned to another group | Unassign first |

## Test Plan

### Unit Tests

- [ ] `test_alignment_group_single_source_no_pause`
- [ ] `test_alignment_group_two_sources_fast_paused`
- [ ] `test_alignment_group_resume_when_slow_catches_up`
- [ ] `test_alignment_group_warn_only_mode`
- [ ] `test_alignment_group_drop_excess_mode`
- [ ] `test_alignment_group_drift_calculation`
- [ ] `test_alignment_group_metrics_accurate`
- [ ] `test_alignment_coordinator_multiple_groups`
- [ ] `test_alignment_coordinator_source_assignment`

### Integration Tests

- [ ] Stream-stream join with alignment group
- [ ] Kafka sources with different partition lag
- [ ] Backpressure integration with thread-per-core

### Benchmarks

- [ ] `bench_alignment_check_2_sources` - Target: <100ns
- [ ] `bench_alignment_check_10_sources` - Target: <500ns
- [ ] `bench_alignment_coordinator_100_groups` - Target: <10μs

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Watermark report | < 100ns | HashMap lookup |
| Alignment check | < 500ns (10 sources) | Iterate sources |
| Pause/resume signal | < 1μs | Via SPSC queue |

## Completion Checklist

- [ ] `WatermarkAlignmentGroup` implemented
- [ ] `AlignmentGroupCoordinator` implemented
- [ ] Backpressure integration (pause/resume)
- [ ] SQL configuration parsing
- [ ] Unit tests passing (9+ tests)
- [ ] Integration with F019 (stream-stream joins)
- [ ] Benchmarks meet targets
- [ ] Documentation updated

## Open Questions

- [ ] Should we support weighted sources (some sources are more important)?
- [ ] How should we handle sources that are always slow (permanent laggards)?
- [ ] Should alignment groups span across cores in thread-per-core?

## References

- [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) - Section 8
- [F010: Watermarks](../phase-1/F010-watermarks.md)
- [F064: Per-Partition Watermarks](F064-per-partition-watermarks.md)
- [F019: Stream-Stream Joins](F019-stream-stream-joins.md)
- [Apache Flink: Watermark Alignment](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/#watermark-alignment)
