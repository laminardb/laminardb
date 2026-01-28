# F070: Task Budget Enforcement

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F070 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F013 |
| **Owner** | TBD |
| **Research** | [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md) |
| **Completed** | 2026-01-25 |
| **Tests** | 47 unit tests |

## Summary

Implement task budget enforcement to ensure Ring 0 latency guarantees are met. Each operation has a time budget, and exceeding it triggers metrics/alerts. Ring 1 background tasks cooperatively yield when Ring 0 has pending work or when their budget is exhausted.

## Motivation

### Why Budgets Matter

| Ring | Operation | Budget | Consequence if Exceeded |
|------|-----------|--------|-------------------------|
| 0 | Single event | 500ns | p99 latency violated |
| 0 | Event batch | 5μs | Batch deadline missed |
| 1 | Background chunk | 1ms | Ring 0 events delayed |

### Current Gap

The reactor has `max_iteration_time` but:
- No per-operation budget tracking
- No metrics for budget violations
- No cooperative yielding in Ring 1
- No alerting on sustained violations

## Goals

1. Implement `TaskBudget` tracking for all operations
2. Metrics for budget violations (count, exceeded-by amount)
3. Cooperative yielding in Ring 1 when budget exhausted
4. Ring 0 priority: Ring 1 yields when Ring 0 has pending events
5. Alerting integration for sustained budget violations

## Non-Goals

- Hard preemption (cooperative only)
- Per-event timeout enforcement
- Automatic task splitting

## Technical Design

### TaskBudget Struct

```rust
use std::time::Instant;
use metrics::{counter, histogram};

/// Tracks execution time budget for a task.
///
/// Created at task start, automatically records metrics on drop.
/// Use for all Ring 0 operations and Ring 1 chunks.
pub struct TaskBudget {
    /// When this task started
    start: Instant,
    /// Budget in nanoseconds
    budget_ns: u64,
    /// Task name for metrics
    name: &'static str,
    /// Ring for this task (0, 1, or 2)
    ring: u8,
}

impl TaskBudget {
    // Ring 0 budgets (microseconds or less)
    /// Single event processing: 500ns
    pub const RING0_EVENT_NS: u64 = 500;
    /// Batch of events: 5μs
    pub const RING0_BATCH_NS: u64 = 5_000;
    /// State lookup: 200ns
    pub const RING0_LOOKUP_NS: u64 = 200;
    /// Window trigger: 10μs
    pub const RING0_WINDOW_NS: u64 = 10_000;

    // Ring 1 budgets (milliseconds)
    /// Background work chunk: 1ms
    pub const RING1_CHUNK_NS: u64 = 1_000_000;
    /// Checkpoint preparation: 10ms
    pub const RING1_CHECKPOINT_NS: u64 = 10_000_000;
    /// WAL flush: 100μs
    pub const RING1_WAL_FLUSH_NS: u64 = 100_000;

    /// Create budget for Ring 0 single event.
    #[inline]
    pub fn ring0_event() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_EVENT_NS,
            name: "ring0_event",
            ring: 0,
        }
    }

    /// Create budget for Ring 0 batch.
    #[inline]
    pub fn ring0_batch() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_BATCH_NS,
            name: "ring0_batch",
            ring: 0,
        }
    }

    /// Create budget for Ring 0 state lookup.
    #[inline]
    pub fn ring0_lookup() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_LOOKUP_NS,
            name: "ring0_lookup",
            ring: 0,
        }
    }

    /// Create budget for Ring 1 background chunk.
    #[inline]
    pub fn ring1_chunk() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_CHUNK_NS,
            name: "ring1_chunk",
            ring: 1,
        }
    }

    /// Create custom budget.
    pub fn custom(name: &'static str, ring: u8, budget_ns: u64) -> Self {
        Self {
            start: Instant::now(),
            budget_ns,
            name,
            ring,
        }
    }

    /// Get elapsed time in nanoseconds.
    #[inline]
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Get remaining budget in nanoseconds (negative if exceeded).
    #[inline]
    pub fn remaining_ns(&self) -> i64 {
        self.budget_ns as i64 - self.elapsed_ns() as i64
    }

    /// Check if budget is exceeded.
    #[inline]
    pub fn exceeded(&self) -> bool {
        self.elapsed_ns() > self.budget_ns
    }

    /// Check if budget is almost exceeded (>80% used).
    #[inline]
    pub fn almost_exceeded(&self) -> bool {
        self.elapsed_ns() > (self.budget_ns * 8) / 10
    }

    /// Record metrics (called on drop).
    fn record_metrics(&self) {
        let elapsed = self.elapsed_ns();

        // Record duration histogram
        histogram!(
            "laminar.task.duration_ns",
            "task" => self.name,
            "ring" => self.ring.to_string(),
        ).record(elapsed as f64);

        // Record budget exceeded
        if elapsed > self.budget_ns {
            counter!(
                "laminar.task.budget_exceeded",
                "task" => self.name,
                "ring" => self.ring.to_string(),
            ).increment(1);

            histogram!(
                "laminar.task.budget_exceeded_by_ns",
                "task" => self.name,
                "ring" => self.ring.to_string(),
            ).record((elapsed - self.budget_ns) as f64);
        }
    }
}

impl Drop for TaskBudget {
    fn drop(&mut self) {
        self.record_metrics();
    }
}
```

### Usage in Ring 0

```rust
impl Reactor {
    /// Process a single event with budget tracking.
    pub fn process_event(&mut self, event: Event) {
        let _budget = TaskBudget::ring0_event();

        // Process event - metrics recorded automatically on drop
        let outputs = self.operators.process(&event, &mut self.ctx);
        self.emit_outputs(outputs);
    }

    /// Process batch of events with budget tracking.
    pub fn process_batch(&mut self, events: &[Event]) {
        let batch_budget = TaskBudget::ring0_batch();

        for event in events {
            let _event_budget = TaskBudget::ring0_event();
            let outputs = self.operators.process(&event, &mut self.ctx);
            self.emit_outputs(outputs);

            // Check if batch budget almost exceeded
            if batch_budget.almost_exceeded() {
                tracing::warn!(
                    "Batch budget almost exceeded after {} events",
                    events.len()
                );
                break;
            }
        }
    }
}
```

### Cooperative Yielding in Ring 1

```rust
impl Ring1Processor {
    /// Process background work with cooperative yielding.
    ///
    /// Yields when:
    /// 1. Budget is exceeded
    /// 2. Ring 0 has pending events
    pub fn process_background_work(&mut self) -> YieldReason {
        let budget = TaskBudget::ring1_chunk();

        while !budget.exceeded() {
            // Check if Ring 0 needs attention (priority)
            if self.ring0_has_pending() {
                return YieldReason::Ring0Priority;
            }

            // Get next work item
            match self.background_queue.pop() {
                Some(work) => self.process_work_item(work),
                None => return YieldReason::QueueEmpty,
            }
        }

        YieldReason::BudgetExceeded
    }

    /// Check if Ring 0 has pending events.
    fn ring0_has_pending(&self) -> bool {
        !self.ring0_inbox.is_empty()
    }
}

/// Reason Ring 1 yielded control.
#[derive(Debug, Clone, Copy)]
pub enum YieldReason {
    /// Budget exceeded, need to give Ring 0 a chance
    BudgetExceeded,
    /// Ring 0 has pending events (priority yield)
    Ring0Priority,
    /// No more work in queue
    QueueEmpty,
}
```

### Checkpoint Budget Management

```rust
impl CheckpointManager {
    /// Checkpoint with budget-aware chunking.
    ///
    /// Breaks checkpoint into chunks, yielding between chunks
    /// to avoid blocking Ring 0 for too long.
    pub async fn checkpoint_chunked(
        &mut self,
        state: &StateStore,
    ) -> Result<CheckpointMetadata, CheckpointError> {
        let total_keys = state.len();
        let chunk_size = 10_000; // Keys per chunk

        for chunk_start in (0..total_keys).step_by(chunk_size) {
            let budget = TaskBudget::ring1_chunk();

            // Process chunk
            let chunk_end = (chunk_start + chunk_size).min(total_keys);
            self.checkpoint_range(state, chunk_start, chunk_end)?;

            // Yield if budget exceeded
            if budget.exceeded() {
                tokio::task::yield_now().await;
            }
        }

        self.finalize_checkpoint()
    }
}
```

### Budget Metrics Dashboard

```rust
/// Budget violation aggregator for alerting.
pub struct BudgetMonitor {
    /// Window for violation rate calculation
    window_ms: u64,
    /// Violation counts per task
    violations: HashMap<String, ViolationStats>,
    /// Alert threshold (violations per second)
    alert_threshold: f64,
}

#[derive(Default)]
struct ViolationStats {
    count: u64,
    total_exceeded_ns: u64,
    last_window_count: u64,
}

impl BudgetMonitor {
    /// Check if alert threshold exceeded for any task.
    pub fn check_alerts(&self) -> Vec<BudgetAlert> {
        self.violations
            .iter()
            .filter_map(|(task, stats)| {
                let rate = stats.last_window_count as f64 /
                          (self.window_ms as f64 / 1000.0);
                if rate > self.alert_threshold {
                    Some(BudgetAlert {
                        task: task.clone(),
                        violation_rate: rate,
                        avg_exceeded_by_ns: stats.total_exceeded_ns / stats.count,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

pub struct BudgetAlert {
    pub task: String,
    pub violation_rate: f64,
    pub avg_exceeded_by_ns: u64,
}
```

### Integration with Reactor Event Loop

```rust
impl Reactor {
    pub fn run(&mut self) {
        loop {
            // Ring 0: Process events with budget
            let _ring0_budget = TaskBudget::custom("ring0_iteration", 0, 10_000);

            while let Some(event) = self.event_queue.pop_front() {
                self.process_event(event);

                // Check if iteration budget exceeded
                if _ring0_budget.exceeded() {
                    tracing::debug!("Ring 0 iteration budget exceeded");
                    break;
                }
            }

            // Ring 1: Background work (only if Ring 0 idle)
            if self.event_queue.is_empty() {
                let yield_reason = self.ring1.process_background_work();
                match yield_reason {
                    YieldReason::Ring0Priority => {
                        // Immediately continue to Ring 0
                        continue;
                    }
                    YieldReason::BudgetExceeded => {
                        // Log if this happens frequently
                        counter!("laminar.ring1.budget_yields").increment(1);
                    }
                    YieldReason::QueueEmpty => {
                        // Normal, can sleep
                    }
                }
            }

            // Sleep if nothing to do
            if self.should_sleep() {
                self.sleep_until_event();
            }
        }
    }
}
```

## Implementation Phases

### Phase 1: TaskBudget Core (1-2 days)

1. Implement `TaskBudget` struct
2. Add metrics recording (histogram, counter)
3. Factory methods for common budgets
4. Unit tests

### Phase 2: Ring 0 Integration (1-2 days)

1. Add budgets to reactor event processing
2. Add budgets to state store operations
3. Add budgets to operator processing
4. Benchmarks to verify overhead

### Phase 3: Ring 1 Yielding (1-2 days)

1. Implement cooperative yielding
2. Ring 0 priority detection
3. Checkpoint chunking
4. Integration tests

### Phase 4: Monitoring (1 day)

1. Budget metrics dashboard
2. Alert threshold configuration
3. Prometheus/Grafana integration
4. Documentation

## Test Cases

```rust
#[test]
fn test_budget_exceeded_detection() {
    let budget = TaskBudget::ring0_event();

    // Simulate work that takes longer than 500ns
    std::thread::sleep(Duration::from_micros(10));

    assert!(budget.exceeded());
    assert!(budget.remaining_ns() < 0);
}

#[test]
fn test_budget_metrics_recorded() {
    // Setup metrics recorder
    let recorder = TestRecorder::new();
    metrics::set_boxed_recorder(Box::new(recorder.clone()));

    {
        let _budget = TaskBudget::ring0_event();
        std::thread::sleep(Duration::from_micros(10));
    } // Metrics recorded on drop

    assert!(recorder.has_histogram("laminar.task.duration_ns"));
    assert!(recorder.has_counter("laminar.task.budget_exceeded"));
}

#[test]
fn test_ring1_yields_to_ring0() {
    let mut processor = Ring1Processor::new();
    processor.ring0_inbox.push(Event::test());

    // Add work to Ring 1
    processor.background_queue.push(Work::test());

    // Should yield immediately due to Ring 0 priority
    let reason = processor.process_background_work();
    assert!(matches!(reason, YieldReason::Ring0Priority));
}

#[test]
fn test_ring1_budget_yield() {
    let mut processor = Ring1Processor::new();

    // Add lots of slow work
    for _ in 0..1000 {
        processor.background_queue.push(Work::slow());
    }

    // Should yield due to budget
    let reason = processor.process_background_work();
    assert!(matches!(reason, YieldReason::BudgetExceeded));
}

#[test]
fn test_budget_overhead_acceptable() {
    // Budget tracking should add < 10ns overhead
    let iterations = 100_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _budget = TaskBudget::ring0_event();
    }
    let with_budget = start.elapsed();

    let start = Instant::now();
    for _ in 0..iterations {
        // No budget
    }
    let without_budget = start.elapsed();

    let overhead_ns = (with_budget - without_budget).as_nanos() / iterations as u128;
    assert!(overhead_ns < 10, "Budget overhead {} ns too high", overhead_ns);
}
```

## Acceptance Criteria

- [ ] TaskBudget struct with automatic metrics
- [ ] Ring 0 operations tracked with budgets
- [ ] Ring 1 cooperative yielding implemented
- [ ] Ring 0 priority over Ring 1
- [ ] Budget metrics exported (duration, violations)
- [ ] Alert integration for sustained violations
- [ ] Budget overhead < 10ns
- [ ] 10+ unit tests passing

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| TaskBudget overhead | < 10ns | Instant::now() + atomic |
| Ring 0 event budget | 500ns | Single event processing |
| Ring 0 batch budget | 5μs | Up to 10 events |
| Ring 1 chunk budget | 1ms | Background work chunk |

## Metrics Exported

| Metric | Type | Labels |
|--------|------|--------|
| `laminar.task.duration_ns` | Histogram | task, ring |
| `laminar.task.budget_exceeded` | Counter | task, ring |
| `laminar.task.budget_exceeded_by_ns` | Histogram | task, ring |
| `laminar.ring1.budget_yields` | Counter | - |
| `laminar.ring1.priority_yields` | Counter | - |

## References

- [Thread-Per-Core 2026 Research](../../research/laminardb-thread-per-core-2026-research.md)
- [Seastar Task Scheduling](https://seastar.io/tutorial/#scheduling)
- [Glommio Task Budgets](https://docs.rs/glommio/latest/glommio/)
