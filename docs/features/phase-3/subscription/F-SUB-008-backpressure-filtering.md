# F-SUB-008: Backpressure & Filtering

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-008 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SUB-004 (Dispatcher), F-SUB-005 (Push API) |
| **Created** | 2026-02-01 |

## Summary

Implements configurable backpressure strategies, subscription-level filtering with Ring 0 predicate push-down, and notification batching for throughput optimization. These are the optimization features that make the reactive subscription system production-ready under load.

**Backpressure**: Configurable strategies (DropOldest, DropNewest, Block, Sample) that determine behavior when a subscriber's channel buffer is full. Default is `DropOldest` for real-time/financial use cases where stale data is worse than missing data.

**Filtering**: Predicate expressions that filter events before they reach subscribers. Lightweight predicates (column equality, range checks) are pushed to Ring 0 to avoid unnecessary notifications. Complex predicates are evaluated in Ring 1.

**Batching**: Accumulates multiple notifications and delivers them as `ChangeEventBatch` to reduce per-event overhead for high-throughput scenarios.

**Research Reference**: [Reactive Subscriptions Research - Sections on Backpressure and Batching](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: Configurable `BackpressureStrategy` per subscription
- **FR-2**: `DropOldest`: when buffer full, oldest events are discarded
- **FR-3**: `DropNewest`: when buffer full, new events are discarded
- **FR-4**: `Block`: dispatcher blocks until buffer has space (Ring 1 only, never Ring 0)
- **FR-5**: `Sample(n)`: deliver every Nth event
- **FR-6**: Subscription filter predicate: `filter: "price > 100.0 AND symbol = 'AAPL'"`
- **FR-7**: Ring 0 predicate push-down for simple equality/range filters
- **FR-8**: Notification batching with configurable max_batch_size and max_batch_delay
- **FR-9**: Lagging subscriber detection with configurable threshold
- **FR-10**: Metrics: events filtered, events dropped, batch sizes, lag

### Non-Functional Requirements

- **NFR-1**: Ring 0 predicate evaluation < 50ns (simple column checks only)
- **NFR-2**: Batching reduces per-event dispatch overhead by 5-10x
- **NFR-3**: Backpressure never blocks Ring 0 (only Ring 1 Block strategy)
- **NFR-4**: Filter compilation at subscription creation time (not per-event)

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RING 0                                        │
│                                                                 │
│  [Operator Output] ──► Ring 0 Predicate Check ──► NotificationSlot
│                        (optional, simple only)                   │
│                        "price > 100.0" → skip if false           │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                    RING 1                                        │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│  │ Notification │     │   Filter     │     │   Batcher    │    │
│  │   Drain      │────►│ (complex     │────►│ (accumulate  │    │
│  │              │     │  predicates) │     │  up to N or  │    │
│  │              │     │              │     │  timeout)    │    │
│  └──────────────┘     └──────────────┘     └──────┬───────┘    │
│                                                    │            │
│                    ┌───────────────────────────────┘            │
│                    ▼                                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │             Backpressure Controller                        │  │
│  │                                                           │  │
│  │  DropOldest: send (overwrites old if lagged)              │  │
│  │  DropNewest: skip send if full                            │  │
│  │  Block: await send (blocks dispatcher, not Ring 0)        │  │
│  │  Sample(n): counter % n == 0                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                    │                                            │
├────────────────────┼────────────────────────────────────────────┤
│                    │  RING 2                                    │
│                    ▼                                            │
│               Subscriber channels                               │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::time::{Duration, Instant};

/// Backpressure strategy for subscriptions.
///
/// Determines behavior when a subscriber's channel buffer is full.
///
/// # Recommendation
///
/// Use `DropOldest` for real-time/financial applications where
/// stale data is worse than missing data. Use `Block` for
/// exactly-once pipelines where completeness matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// Drop oldest events when buffer full (real-time priority).
    ///
    /// Uses tokio broadcast's natural lagging behavior.
    /// Subscribers receive `Lagged(n)` error and skip ahead.
    DropOldest,

    /// Drop newest events when buffer full (completeness priority).
    ///
    /// New events are silently discarded until the subscriber catches up.
    DropNewest,

    /// Block the dispatcher until buffer has space.
    ///
    /// WARNING: This blocks the Ring 1 dispatcher, NOT Ring 0.
    /// Only use when a single slow subscriber is acceptable.
    Block,

    /// Sample: deliver every Nth event.
    ///
    /// Reduces load on slow subscribers at the cost of completeness.
    Sample(usize),
}

/// Compiled subscription filter.
///
/// Simple predicates are pushed to Ring 0 for zero-overhead filtering.
/// Complex predicates are evaluated in Ring 1.
#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    /// Original filter expression.
    pub expression: String,
    /// Ring 0 predicates (simple, zero-allocation checks).
    pub ring0_predicates: Vec<Ring0Predicate>,
    /// Ring 1 predicates (complex, may allocate).
    pub ring1_predicates: Vec<Ring1Predicate>,
}

/// Ring 0 predicate (must be zero-allocation, < 50ns).
///
/// Only simple column-level checks are supported:
/// - Equality: `column = value`
/// - Range: `column > value`, `column < value`
/// - Not null: `column IS NOT NULL`
#[derive(Debug, Clone)]
pub enum Ring0Predicate {
    /// Column equals a constant value.
    Eq {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column greater than a constant value.
    Gt {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column less than a constant value.
    Lt {
        column_index: usize,
        value: ScalarValue,
    },
    /// Column is between two values.
    Between {
        column_index: usize,
        low: ScalarValue,
        high: ScalarValue,
    },
}

/// Scalar value for Ring 0 predicate comparison.
///
/// Fixed-size, stack-allocated, no heap.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    /// Interned string index (not the string itself).
    StringIndex(u32),
}

/// Ring 1 predicate (can allocate, evaluated in Ring 1 dispatcher).
#[derive(Debug, Clone)]
pub enum Ring1Predicate {
    /// DataFusion expression evaluated against RecordBatch.
    Expression(String),
    /// Pre-compiled filter function.
    Compiled(Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch, ArrowError> + Send + Sync>),
}

/// Batching configuration for subscriptions.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum events per batch.
    pub max_batch_size: usize,
    /// Maximum time to wait for batch to fill.
    pub max_batch_delay: Duration,
    /// Whether batching is enabled.
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_batch_delay: Duration::from_micros(100),
            enabled: false,
        }
    }
}

/// Notification batcher that accumulates events before delivery.
pub struct NotificationBatcher {
    /// Buffer of accumulated events per source.
    buffers: HashMap<u32, Vec<ChangeEvent>>,
    /// Last flush time per source.
    last_flush: HashMap<u32, Instant>,
    /// Configuration.
    config: BatchConfig,
}

impl NotificationBatcher {
    /// Creates a new batcher.
    pub fn new(config: BatchConfig) -> Self {
        Self {
            buffers: HashMap::new(),
            last_flush: HashMap::new(),
            config,
        }
    }

    /// Adds an event to the batcher.
    ///
    /// Returns `Some(ChangeEventBatch)` if the batch is ready to deliver.
    pub fn add(
        &mut self,
        source_id: u32,
        source_name: &str,
        event: ChangeEvent,
    ) -> Option<ChangeEventBatch> {
        if !self.config.enabled {
            return Some(ChangeEventBatch::new(
                source_name.to_string(),
                vec![event],
            ));
        }

        let buffer = self.buffers.entry(source_id).or_default();
        buffer.push(event);

        let now = Instant::now();
        let last = self.last_flush.entry(source_id).or_insert(now);

        // Flush if batch is full or timeout expired
        if buffer.len() >= self.config.max_batch_size
            || now.duration_since(*last) >= self.config.max_batch_delay
        {
            *last = now;
            let events = std::mem::take(buffer);
            Some(ChangeEventBatch::new(source_name.to_string(), events))
        } else {
            None
        }
    }

    /// Forces flush of all pending batches.
    pub fn flush_all(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let mut results = Vec::new();

        for (source_id, buffer) in self.buffers.iter_mut() {
            if !buffer.is_empty() {
                let events = std::mem::take(buffer);
                results.push((
                    *source_id,
                    ChangeEventBatch::new(String::new(), events),
                ));
            }
        }

        results
    }

    /// Checks for timed-out batches and flushes them.
    pub fn flush_expired(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let now = Instant::now();
        let mut results = Vec::new();

        for (source_id, buffer) in self.buffers.iter_mut() {
            if buffer.is_empty() {
                continue;
            }

            let last = self.last_flush.get(source_id).copied().unwrap_or(now);
            if now.duration_since(last) >= self.config.max_batch_delay {
                let events = std::mem::take(buffer);
                self.last_flush.insert(*source_id, now);
                results.push((
                    *source_id,
                    ChangeEventBatch::new(String::new(), events),
                ));
            }
        }

        results
    }
}

/// Backpressure controller per subscription.
pub struct BackpressureController {
    /// Strategy.
    strategy: BackpressureStrategy,
    /// Events dropped count.
    dropped: u64,
    /// Sample counter (for Sample strategy).
    sample_counter: u64,
    /// Lag threshold for warning.
    lag_warning_threshold: u64,
}

impl BackpressureController {
    /// Creates a new controller.
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            dropped: 0,
            sample_counter: 0,
            lag_warning_threshold: 1000,
        }
    }

    /// Determines whether to deliver an event to this subscriber.
    ///
    /// Returns `true` if the event should be sent, `false` if dropped.
    pub fn should_deliver(&mut self) -> bool {
        match self.strategy {
            BackpressureStrategy::DropOldest => true, // Handled by broadcast lagging
            BackpressureStrategy::DropNewest => true, // Checked after send attempt
            BackpressureStrategy::Block => true,
            BackpressureStrategy::Sample(n) => {
                self.sample_counter += 1;
                if self.sample_counter % n as u64 == 0 {
                    true
                } else {
                    self.dropped += 1;
                    false
                }
            }
        }
    }

    /// Records a dropped event.
    pub fn record_drop(&mut self) {
        self.dropped += 1;
    }

    /// Returns the number of events dropped.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// Compiles a filter expression into Ring 0 and Ring 1 predicates.
pub fn compile_filter(
    expression: &str,
    schema: &SchemaRef,
) -> Result<SubscriptionFilter, FilterCompileError> {
    // Parse expression and classify predicates
    let mut ring0 = Vec::new();
    let mut ring1 = Vec::new();

    // Simple heuristic: single column comparisons go to Ring 0
    // Everything else goes to Ring 1
    // Full implementation would use the SQL parser from F006B

    Ok(SubscriptionFilter {
        expression: expression.to_string(),
        ring0_predicates: ring0,
        ring1_predicates: ring1,
    })
}

/// Filter compilation errors.
#[derive(Debug, thiserror::Error)]
pub enum FilterCompileError {
    #[error("invalid filter expression: {0}")]
    InvalidExpression(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("type mismatch: column {column} is {actual}, expected {expected}")]
    TypeMismatch {
        column: String,
        actual: String,
        expected: String,
    },
}
```

### Ring 0 Predicate Evaluation

```rust
impl Ring0Predicate {
    /// Evaluates the predicate against a RecordBatch row.
    ///
    /// Returns true if the row matches the predicate.
    /// This runs on the Ring 0 hot path - must be < 50ns.
    #[inline(always)]
    pub fn evaluate(&self, batch: &RecordBatch, row: usize) -> bool {
        match self {
            Ring0Predicate::Eq { column_index, value } => {
                Self::compare_eq(batch, *column_index, row, value)
            }
            Ring0Predicate::Gt { column_index, value } => {
                Self::compare_gt(batch, *column_index, row, value)
            }
            Ring0Predicate::Lt { column_index, value } => {
                Self::compare_lt(batch, *column_index, row, value)
            }
            Ring0Predicate::Between { column_index, low, high } => {
                Self::compare_gt(batch, *column_index, row, low)
                    && Self::compare_lt(batch, *column_index, row, high)
            }
        }
    }

    #[inline(always)]
    fn compare_eq(batch: &RecordBatch, col: usize, row: usize, value: &ScalarValue) -> bool {
        match value {
            ScalarValue::Int64(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(row) == *v)
                    .unwrap_or(false)
            }
            ScalarValue::Float64(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| (a.value(row) - *v).abs() < f64::EPSILON)
                    .unwrap_or(false)
            }
            ScalarValue::Bool(v) => {
                batch.column(col)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|a| a.value(row) == *v)
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    // compare_gt and compare_lt follow the same pattern...
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| SubscriptionConfig | `subscription/registry.rs` | Add backpressure and filter fields |
| SubscriptionDispatcher | `subscription/dispatcher.rs` | Integrate batcher and filter |
| NotificationHub | `subscription/notification.rs` | Ring 0 predicate check before notify |

### New Files

- `crates/laminar-core/src/subscription/backpressure.rs` - BackpressureController, BackpressureStrategy
- `crates/laminar-core/src/subscription/filter.rs` - SubscriptionFilter, Ring0Predicate, compile_filter
- `crates/laminar-core/src/subscription/batcher.rs` - NotificationBatcher, BatchConfig

## Test Plan

### Unit Tests

- [ ] `test_backpressure_drop_oldest` - DropOldest behavior
- [ ] `test_backpressure_drop_newest` - DropNewest skips on full
- [ ] `test_backpressure_sample` - Sample(n) delivers every Nth
- [ ] `test_backpressure_controller_dropped_count` - Counter accuracy
- [ ] `test_batcher_immediate_when_disabled` - No batching when disabled
- [ ] `test_batcher_size_trigger` - Flush on max_batch_size
- [ ] `test_batcher_timeout_trigger` - Flush on max_batch_delay
- [ ] `test_batcher_flush_all` - Force flush all sources
- [ ] `test_batcher_flush_expired` - Timed flush
- [ ] `test_ring0_predicate_eq_int64` - Integer equality
- [ ] `test_ring0_predicate_eq_float64` - Float equality
- [ ] `test_ring0_predicate_gt` - Greater than comparison
- [ ] `test_ring0_predicate_lt` - Less than comparison
- [ ] `test_ring0_predicate_between` - Range check
- [ ] `test_filter_compile_simple` - Simple expression parsing
- [ ] `test_filter_compile_column_not_found` - Error on bad column
- [ ] `test_ring1_predicate_expression` - Complex filter evaluation
- [ ] `test_lagging_subscriber_detection` - Lag threshold warning

### Integration Tests

- [ ] `test_backpressure_with_slow_subscriber` - End-to-end slow consumer
- [ ] `test_filter_push_down` - Ring 0 filter reduces notifications
- [ ] `test_batched_delivery` - Batched events received correctly

### Benchmarks

- [ ] `bench_ring0_predicate_eval` - Target: < 50ns
- [ ] `bench_batcher_add_event` - Target: < 20ns
- [ ] `bench_batcher_flush_64` - Target: < 200ns for 64 events
- [ ] `bench_backpressure_sample_overhead` - Target: < 5ns
- [ ] `bench_throughput_with_batching` - Target: > 10M events/sec

## Completion Checklist

- [ ] BackpressureStrategy enum with 4 strategies
- [ ] BackpressureController per-subscription
- [ ] NotificationBatcher with size/time triggers
- [ ] Ring0Predicate with zero-alloc evaluation
- [ ] Ring1Predicate with DataFusion expression support
- [ ] compile_filter() function
- [ ] Integration with dispatcher pipeline
- [ ] Lagging subscriber detection and metrics
- [ ] Unit tests passing (18+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation
- [ ] Code reviewed

## References

- [F-SUB-004: Subscription Dispatcher](F-SUB-004-subscription-dispatcher.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [Reactive Streams Specification - Backpressure](https://www.reactive-streams.org/)
- [tokio broadcast lagging behavior](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
