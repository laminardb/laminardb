# F065: Keyed Watermarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F065 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F010, F064 |
| **Owner** | TBD |
| **Created** | 2026-01-24 |
| **Research** | [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) |

## Summary

Implement per-key watermark tracking to achieve 99%+ data accuracy compared to 63-67% with traditional global watermarks. This is a **major 2025 advancement** identified in the research that addresses the fundamental problem of fast-moving keys causing late data drops for slower keys.

## Motivation

**From Research Section 3 (Keyed Watermarks - Major 2025 Advancement):**

> Apache Flink's standard watermark generation approach results in ~33% data loss when 50% of median-proximate keys experience delays, exceeding 37% when 50% of random keys are delayed.

**The Problem with Global Watermarks:**

```
Global Watermark Scenario (Traditional):
┌─────────────────────────────────────────────────────────────────┐
│              Global Watermark = 10:05 (from Key C)              │
│                                                                 │
│  Key A: ████████████░░░░░░ events at 10:03 → DROPPED (late!)   │
│  Key B: ██████░░░░░░░░░░░░ events at 10:01 → DROPPED (late!)   │
│  Key C: █████████████████ events at 10:08 → OK                 │
│                                                                 │
│  Result: Fast-moving Key C advances watermark, slow keys       │
│          have their valid events dropped as "late"             │
└─────────────────────────────────────────────────────────────────┘
```

**Keyed Watermarks Solution:**

```
Keyed Watermark Scenario (This Feature):
┌─────────────────────────────────────────────────────────────────┐
│             Per-Key Watermarks                                  │
│                                                                 │
│  Key A: watermark = 10:02 → events at 10:03 → OK               │
│  Key B: watermark = 10:00 → events at 10:01 → OK               │
│  Key C: watermark = 10:07 → events at 10:08 → OK               │
│                                                                 │
│  Global (for ordering): min(A,B,C) = 10:00                     │
│                                                                 │
│  Result: Each key tracks its own progress independently        │
│          99%+ accuracy vs 63-67% with global                   │
└─────────────────────────────────────────────────────────────────┘
```

**Research Finding (ScienceDirect, March 2025):**
> Keyed watermarks achieve **99%+ accuracy** compared to **63-67%** with global watermarks.

## Goals

1. Track watermarks per logical key (e.g., `user_id`, `device_id`)
2. Maintain global watermark as minimum across active keys
3. Per-key idle detection and marking
4. Memory-efficient storage for high-cardinality keys
5. Integration with window operators for per-key window triggering

## Non-Goals

- Automatic key inference (keys must be explicitly specified)
- Cross-stream key correlation (handled by joins)
- Adaptive per-key lateness (uniform lateness for now)

## Technical Design

### Architecture

**Ring:** Ring 1 for keyed watermark management (state overhead acceptable)

**Crate:** `laminar-core/src/time/`

### When to Use Keyed Watermarks

| Scenario | Recommendation |
|----------|---------------|
| Multi-tenant (varying data rates) | **Use keyed watermarks** |
| IoT devices (different reporting frequencies) | **Use keyed watermarks** |
| High-cardinality keys (>1M unique) | Consider memory trade-off |
| Low-cardinality keys (<10K unique) | **Use keyed watermarks** |
| Single-tenant, uniform rates | Global watermark sufficient |

### Data Structures

```rust
use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// Per-key watermark state.
#[derive(Debug, Clone)]
pub struct KeyWatermarkState {
    /// Maximum event time seen for this key
    pub max_event_time: i64,
    /// Current watermark (max_event_time - bounded_delay)
    pub watermark: i64,
    /// Last activity time (wall clock)
    pub last_activity: Instant,
    /// Whether this key is marked idle
    pub is_idle: bool,
}

impl KeyWatermarkState {
    pub fn new(bounded_delay: i64) -> Self {
        Self {
            max_event_time: i64::MIN,
            watermark: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
        }
    }

    /// Updates state with a new event timestamp.
    #[inline]
    pub fn update(&mut self, event_time: i64, bounded_delay: i64) -> bool {
        self.last_activity = Instant::now();
        self.is_idle = false;

        if event_time > self.max_event_time {
            self.max_event_time = event_time;
            let new_watermark = event_time.saturating_sub(bounded_delay);
            if new_watermark > self.watermark {
                self.watermark = new_watermark;
                return true; // Watermark advanced
            }
        }
        false
    }
}

/// Keyed watermark tracker configuration.
#[derive(Debug, Clone)]
pub struct KeyedWatermarkConfig {
    /// Maximum out-of-orderness for watermark calculation
    pub bounded_delay: Duration,
    /// Timeout before marking a key as idle
    pub idle_timeout: Duration,
    /// Maximum number of keys to track (for memory bounds)
    pub max_keys: Option<usize>,
    /// Eviction policy when max_keys reached
    pub eviction_policy: KeyEvictionPolicy,
}

impl Default for KeyedWatermarkConfig {
    fn default() -> Self {
        Self {
            bounded_delay: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            max_keys: None, // Unlimited
            eviction_policy: KeyEvictionPolicy::LeastRecentlyActive,
        }
    }
}

/// Policy for evicting keys when max_keys is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyEvictionPolicy {
    /// Evict key with oldest last_activity
    LeastRecentlyActive,
    /// Evict key with lowest watermark
    LowestWatermark,
    /// Reject new keys (error)
    RejectNew,
}

/// Tracks watermarks per logical key.
///
/// Provides fine-grained watermark tracking for multi-tenant workloads
/// and scenarios with significant event-time skew between keys.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{KeyedWatermarkTracker, KeyedWatermarkConfig, Watermark};
/// use std::time::Duration;
///
/// let config = KeyedWatermarkConfig {
///     bounded_delay: Duration::from_secs(5),
///     idle_timeout: Duration::from_secs(60),
///     ..Default::default()
/// };
///
/// let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);
///
/// // Fast tenant advances quickly
/// tracker.update("tenant_a".to_string(), 10_000);
/// tracker.update("tenant_a".to_string(), 15_000);
///
/// // Slow tenant at earlier time
/// tracker.update("tenant_b".to_string(), 5_000);
///
/// // Per-key watermarks
/// assert_eq!(tracker.watermark_for_key(&"tenant_a".to_string()), Some(10_000)); // 15000 - 5000
/// assert_eq!(tracker.watermark_for_key(&"tenant_b".to_string()), Some(0));      // 5000 - 5000
///
/// // Global watermark is minimum across active keys
/// assert_eq!(tracker.global_watermark(), Some(Watermark::new(0))); // min of 10000, 0
///
/// // Events for tenant_b at 3000 are NOT late (their key watermark allows it)
/// assert!(!tracker.is_late("tenant_b".to_string(), 3000));
///
/// // But events for tenant_a at 3000 ARE late (their key watermark is 10000)
/// assert!(tracker.is_late("tenant_a".to_string(), 3000));
/// ```
pub struct KeyedWatermarkTracker<K: Hash + Eq + Clone> {
    /// Per-key watermark state
    key_states: HashMap<K, KeyWatermarkState>,

    /// Global watermark (minimum across all active keys)
    global_watermark: i64,

    /// Configuration
    config: KeyedWatermarkConfig,

    /// Metrics
    metrics: KeyedWatermarkMetrics,
}

/// Metrics for keyed watermark tracking.
#[derive(Debug, Clone, Default)]
pub struct KeyedWatermarkMetrics {
    /// Total unique keys tracked
    pub total_keys: usize,
    /// Currently active (non-idle) keys
    pub active_keys: usize,
    /// Idle keys
    pub idle_keys: usize,
    /// Keys evicted due to max_keys limit
    pub evicted_keys: u64,
    /// Global watermark advancements
    pub global_advances: u64,
    /// Per-key watermark advancements
    pub key_advances: u64,
}
```

### API/Interface

```rust
impl<K: Hash + Eq + Clone + Send> KeyedWatermarkTracker<K> {
    /// Creates a new keyed watermark tracker.
    pub fn new(config: KeyedWatermarkConfig) -> Self;

    /// Updates the watermark for a specific key.
    ///
    /// # Returns
    /// - `Ok(Some(Watermark))` if the global watermark advances
    /// - `Ok(None)` if no global advancement
    /// - `Err(WatermarkError::MaxKeysReached)` if max_keys and RejectNew policy
    pub fn update(&mut self, key: K, event_time: i64) -> Result<Option<Watermark>, WatermarkError>;

    /// Batch update for multiple events (more efficient).
    ///
    /// Returns the new global watermark if it advanced.
    pub fn update_batch(&mut self, events: &[(K, i64)]) -> Result<Option<Watermark>, WatermarkError>;

    /// Returns the watermark for a specific key.
    pub fn watermark_for_key(&self, key: &K) -> Option<i64>;

    /// Returns the global watermark (minimum across active keys).
    pub fn global_watermark(&self) -> Option<Watermark>;

    /// Checks if an event is late for its key.
    ///
    /// Uses the key's individual watermark, not the global watermark.
    pub fn is_late(&self, key: K, event_time: i64) -> bool;

    /// Marks a key as idle.
    pub fn mark_idle(&mut self, key: &K) -> Option<Watermark>;

    /// Checks for keys that have been idle longer than the timeout.
    ///
    /// Should be called periodically from Ring 1.
    pub fn check_idle_keys(&mut self) -> Option<Watermark>;

    /// Returns the number of active keys.
    pub fn active_key_count(&self) -> usize;

    /// Returns the total number of tracked keys.
    pub fn total_key_count(&self) -> usize;

    /// Returns metrics.
    pub fn metrics(&self) -> &KeyedWatermarkMetrics;

    /// Forces recalculation of global watermark.
    ///
    /// Useful after bulk operations or recovery.
    pub fn recalculate_global(&mut self) -> Option<Watermark>;

    /// Removes a key from tracking.
    pub fn remove_key(&mut self, key: &K) -> Option<KeyWatermarkState>;

    /// Clears all tracked keys.
    pub fn clear(&mut self);
}
```

### Window Operator Integration

```rust
/// Extension trait for window operators with keyed watermarks.
pub trait KeyedWindowOperator<K: Hash + Eq + Clone> {
    /// Processes an event using keyed watermark for late detection.
    ///
    /// Uses the event's key's watermark instead of global watermark
    /// to determine if the event is late.
    fn process_keyed(
        &mut self,
        key: &K,
        event: &Event,
        keyed_tracker: &KeyedWatermarkTracker<K>,
        ctx: &mut OperatorContext,
    ) -> OutputVec;

    /// Triggers windows for a specific key based on its watermark.
    fn trigger_key_windows(
        &mut self,
        key: &K,
        key_watermark: i64,
        ctx: &mut OperatorContext,
    ) -> OutputVec;
}

/// Implementation for TumblingWindowOperator
impl<K, A> KeyedWindowOperator<K> for TumblingWindowOperator<A>
where
    K: Hash + Eq + Clone + Send,
    A: Aggregator,
{
    fn process_keyed(
        &mut self,
        key: &K,
        event: &Event,
        keyed_tracker: &KeyedWatermarkTracker<K>,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let event_time = event.event_time();

        // Use KEY's watermark for late detection, not global
        if let Some(key_watermark) = keyed_tracker.watermark_for_key(key) {
            if event_time < key_watermark {
                // Event is late for THIS KEY
                return self.handle_late_event(event, ctx);
            }
        }

        // Process normally
        self.process(event, ctx)
    }

    fn trigger_key_windows(
        &mut self,
        key: &K,
        key_watermark: i64,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        // Trigger windows for this key where key_watermark > window_end
        self.trigger_windows_up_to(key_watermark, ctx)
    }
}
```

### Memory Efficiency

For high-cardinality keys, we provide memory-bounded options:

```rust
/// Compact keyed watermark tracker using probabilistic data structures.
///
/// Trades some accuracy for memory efficiency when tracking millions of keys.
pub struct CompactKeyedWatermarkTracker {
    /// Approximate key states using Count-Min Sketch for frequency
    /// and HyperLogLog for cardinality
    sketch: CountMinSketch,

    /// Exact state for top-N most active keys
    top_keys: LruCache<Vec<u8>, KeyWatermarkState>,

    /// Configuration
    config: CompactKeyedWatermarkConfig,
}

/// Configuration for compact tracker.
pub struct CompactKeyedWatermarkConfig {
    /// Number of exact keys to track
    pub exact_key_count: usize,
    /// Error rate for sketch (e.g., 0.01 = 1%)
    pub sketch_error_rate: f64,
    /// Bounded delay
    pub bounded_delay: Duration,
}
```

### Algorithm: Global Watermark Calculation

```
function calculate_global_watermark():
    min_watermark = MAX_VALUE
    has_active = false

    for each (key, state) in key_states:
        if not state.is_idle:
            has_active = true
            min_watermark = min(min_watermark, state.watermark)

    if not has_active:
        # All keys idle - use max to allow progress
        min_watermark = max(all_key_watermarks)

    if min_watermark > global_watermark:
        global_watermark = min_watermark
        return Some(Watermark(min_watermark))
    else:
        return None
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `MaxKeysReached` | Key limit hit with RejectNew policy | Increase limit or change policy |
| `KeyNotFound` | Querying unknown key | Returns None, not error |

## Test Plan

### Unit Tests

- [ ] `test_keyed_tracker_single_key_updates_watermark`
- [ ] `test_keyed_tracker_multiple_keys_independent_watermarks`
- [ ] `test_keyed_tracker_global_is_minimum_of_active_keys`
- [ ] `test_keyed_tracker_fast_key_does_not_affect_slow_key`
- [ ] `test_keyed_tracker_is_late_uses_key_watermark_not_global`
- [ ] `test_keyed_tracker_idle_key_excluded_from_global`
- [ ] `test_keyed_tracker_all_idle_uses_max`
- [ ] `test_keyed_tracker_key_eviction_lru`
- [ ] `test_keyed_tracker_key_eviction_lowest_watermark`
- [ ] `test_keyed_tracker_batch_update_efficient`
- [ ] `test_keyed_tracker_remove_key_recalculates_global`
- [ ] `test_keyed_window_operator_uses_key_watermark`

### Integration Tests

- [ ] Multi-tenant workload with varying data rates per tenant
- [ ] IoT scenario with devices reporting at different frequencies
- [ ] High-cardinality keys with memory-bounded tracking

### Property Tests

- [ ] Global watermark never exceeds any active key's watermark
- [ ] Per-key watermark is monotonically increasing per key
- [ ] Idle key removal does not decrease global watermark

### Benchmarks

- [ ] `bench_keyed_watermark_update` - Target: <100ns per update
- [ ] `bench_keyed_watermark_1000_keys` - Target: <1μs for global calc
- [ ] `bench_keyed_watermark_100000_keys` - Target: <10μs for global calc
- [ ] `bench_keyed_watermark_memory_per_key` - Target: <128 bytes

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Per-key update | < 100ns | HashMap lookup + update |
| Global watermark calc | < 1μs (1K keys) | Full iteration |
| Global watermark calc | < 10μs (100K keys) | Full iteration |
| Memory per key | < 128 bytes | Including HashMap overhead |
| Batch update (100 events) | < 5μs | Amortized recalculation |

## Accuracy Comparison

**Based on Research (ScienceDirect March 2025):**

| Watermark Type | Accuracy | Late Data Dropped |
|----------------|----------|-------------------|
| Global | 63-67% | 33-37% |
| **Keyed** | **99%+** | **<1%** |

## Completion Checklist

- [ ] `KeyedWatermarkTracker` implemented
- [ ] Per-key late detection working
- [ ] Idle key detection
- [ ] Memory-bounded variant (CompactKeyedWatermarkTracker)
- [ ] Window operator integration
- [ ] Unit tests passing (12+ tests)
- [ ] Benchmarks meet targets
- [ ] Documentation updated

## Open Questions

- [ ] Should we support different bounded_delay per key?
- [ ] How to handle key cardinality estimation for memory planning?
- [ ] Should idle key eviction be automatic or manual?

## References

- [Watermark Generator Research 2026](../../research/watermark-generator-research-2026.md) - Section 3
- [Keyed Watermarks Paper (ScienceDirect, March 2025)](https://doi.org/10.1016/j.future.2025.107796)
- [F010: Watermarks](../phase-1/F010-watermarks.md)
- [F064: Per-Partition Watermarks](F064-per-partition-watermarks.md)
