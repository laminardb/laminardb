# Watermark Generator Research for Streaming Databases

> **Research Date:** January 2026  
> **Applicable To:** LaminarDB Phase 1+ Implementation  
> **Last Updated:** 2026-01-24

## Executive Summary

This document captures industry best practices and cutting-edge research on watermark generation for streaming databases as of January 2026. It is intended to guide LaminarDB's watermark implementation across all phases while ensuring the Phase 1 design accommodates future extensibility.

### Key Takeaways

1. **Keyed watermarks** (per-key tracking) achieve 99%+ accuracy vs 63-67% with global watermarks
2. **Idle source detection** is critical to prevent pipeline stalls
3. **EMIT ON WINDOW CLOSE** pattern reduces sink write amplification
4. **Watermark generation should be off the hot path** for low-latency systems
5. **Adaptive watermarks** using ML/time-series prediction are emerging but not yet mainstream

---

## Table of Contents

1. [Watermark Fundamentals](#1-watermark-fundamentals)
2. [Generation Strategies](#2-generation-strategies)
3. [Keyed Watermarks](#3-keyed-watermarks-major-2025-advancement)
4. [Idle Source Detection](#4-idle-source-detection--heartbeats)
5. [Watermark Propagation](#5-watermark-propagation)
6. [EMIT ON WINDOW CLOSE](#6-emit-on-window-close-eowc)
7. [Triggers and Late Data](#7-triggers-and-late-data-handling)
8. [Multi-Source Alignment](#8-multi-source-watermark-alignment)
9. [Low-Latency Considerations](#9-low-latency-considerations)
10. [SQL Declaration Patterns](#10-sql-declaration-patterns)
11. [Implementation Recommendations](#11-implementation-recommendations-for-laminardb)
12. [References](#12-references)

---

## 1. Watermark Fundamentals

### What is a Watermark?

A watermark is a **heuristic timestamp** injected into a data stream that signals:

> "I don't expect to see any more events older than time X"

Watermarks enable streaming systems to:

- **Handle out-of-order events**: Wait for late data within a bounded delay
- **Trigger window computations**: Close time windows when watermark passes window end
- **Track progress**: Provide event-time progress independent of wall-clock time
- **Manage state**: Inform when old state can be safely discarded

### Core Formula

```
watermark = max_observed_event_time - allowed_lateness
```

### Watermark Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│  Operator   │────▶│    Sink     │
│  (generate) │     │ (propagate) │     │  (consume)  │
└─────────────┘     └─────────────┘     └─────────────┘
      │                    │
      ▼                    ▼
  Watermark            Watermark
  Generator            = min(input watermarks)
```

---

## 2. Generation Strategies

### 2.1 Bounded Out-of-Orderness (Phase 1 Target)

The most common strategy. Assumes events arrive within a known maximum delay.

**Formula:**
```
watermark = max_event_time_seen - bounded_delay
```

**Flink Implementation:**
```java
WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
```

**RisingWave SQL:**
```sql
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

**Trade-offs:**
| Shorter Delay | Longer Delay |
|---------------|--------------|
| Lower latency | Higher latency |
| More dropped late data | Less dropped late data |
| Less state retention | More state retention |

**Recommendation:** Set delay to cover 99.9% of expected lateness.

### 2.2 Monotonous Timestamps

For strictly ordered streams (rare in production).

```
watermark = current_event_time
```

Only useful when:
- Single-threaded producer
- No network reordering
- Testing/development scenarios

### 2.3 Punctuated Watermarks

Watermarks triggered by specific events rather than periodic intervals.

**Use cases:**
- End-of-batch markers in the stream
- Explicit "flush" events from producers
- Event-driven architectures with irregular patterns

### 2.4 Adaptive/Predictive Watermarks (Emerging)

Uses machine learning to dynamically adjust watermark delay based on observed patterns.

**Research (Frontiers of Computer Science, 2021):**
- Time-series prediction models (LSTM) predict optimal watermark timing
- Dynamically adjusts based on disorder ratio
- Balances responsiveness and accuracy better than static approaches

**Concept Drift Detection:**
- Monitors changes in lateness distribution
- Adjusts bounded delay when patterns shift

**Status:** Research/experimental. Not yet mainstream in production systems.

---

## 3. Keyed Watermarks (Major 2025 Advancement)

### The Problem with Global Watermarks

Traditional systems maintain a **single global watermark** across all keys/partitions.

**Issue:** Fast-moving keys advance the watermark, causing events from slower keys to be dropped as "late."

**Research Finding (ScienceDirect, March 2025):**
> Apache Flink's standard watermark generation approach results in ~33% data loss when 50% of median-proximate keys experience delays, exceeding 37% when 50% of random keys are delayed.

### Keyed Watermarks Solution

Maintain **independent watermarks per logical sub-stream (key)**.

```
┌─────────────────────────────────────────────┐
│              Global Watermark               │
│    (traditional - single value)             │
└─────────────────────────────────────────────┘
                    vs
┌─────────────────────────────────────────────┐
│             Keyed Watermarks                │
│  Key A: ████████████░░░░ (ts: 10:05)       │
│  Key B: ██████░░░░░░░░░░ (ts: 10:02)       │
│  Key C: ████████████████ (ts: 10:08)       │
│  Global: min(A,B,C) = 10:02                │
└─────────────────────────────────────────────┘
```

### Benefits

| Aspect | Improvement |
|--------|-------------|
| Accuracy | 99%+ (vs 63-67% global) |
| Fault isolation | Only affected partition impacted |
| Fairness | Slow streams not penalized by fast streams |

### Trade-offs

| Aspect | Impact |
|--------|--------|
| State size | Moderate increase (one watermark per key) |
| Processing overhead | Negligible |
| Complexity | Higher implementation complexity |

### Implementation Sketch

```rust
/// Keyed watermark tracker
pub struct KeyedWatermarkTracker<K: Hash + Eq> {
    /// Per-key watermark state
    key_watermarks: HashMap<K, WatermarkState>,
    
    /// Global watermark (minimum across all active keys)
    global_watermark: Timestamp,
    
    /// Configuration
    config: WatermarkConfig,
}

struct WatermarkState {
    max_event_time: Timestamp,
    watermark: Timestamp,
    last_activity: Instant,
    is_idle: bool,
}

impl<K: Hash + Eq> KeyedWatermarkTracker<K> {
    pub fn update(&mut self, key: K, event_time: Timestamp) {
        let state = self.key_watermarks.entry(key).or_insert_with(|| {
            WatermarkState::new(self.config.bounded_delay)
        });
        
        state.update(event_time);
        state.last_activity = Instant::now();
        state.is_idle = false;
        
        // Recalculate global watermark
        self.recalculate_global();
    }
    
    fn recalculate_global(&mut self) {
        self.global_watermark = self.key_watermarks
            .values()
            .filter(|s| !s.is_idle)
            .map(|s| s.watermark)
            .min()
            .unwrap_or(Timestamp::MIN);
    }
}
```

### When to Use Keyed Watermarks

- Multi-tenant systems with varying data rates per tenant
- IoT with devices having different reporting frequencies
- Any scenario with significant event-time skew between keys

---

## 4. Idle Source Detection & Heartbeats

### The Idle Source Problem

When a source/partition stops producing events, its watermark stalls, blocking the entire pipeline's progress.

```
Partition 0: ████████████████ (active, ts: 10:05)
Partition 1: ████░░░░░░░░░░░░ (idle since 10:01)  ← BLOCKS PIPELINE
Partition 2: ████████████████ (active, ts: 10:06)

Global Watermark: stuck at 10:01
```

### Solution: Idleness Detection

**Flink Approach:**
```java
WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofMinutes(1))  // Mark idle after 1 min
```

**Behavior:**
1. Track last activity time per source/partition
2. After idle timeout, mark source as "idle"
3. Exclude idle sources from global watermark calculation
4. Reactivate when new events arrive

### Implementation Pattern

```rust
pub struct IdleAwareWatermarkSource {
    last_event_time: Option<Timestamp>,
    last_activity: Instant,
    idle_timeout: Duration,
    is_idle: bool,
}

impl IdleAwareWatermarkSource {
    pub fn check_and_update_idle_status(&mut self) -> bool {
        if self.last_activity.elapsed() > self.idle_timeout {
            self.is_idle = true;
        }
        self.is_idle
    }
    
    pub fn on_event(&mut self, event_time: Timestamp) {
        self.last_event_time = Some(event_time);
        self.last_activity = Instant::now();
        self.is_idle = false;  // Reactivate
    }
    
    /// Returns None if idle, Some(watermark) if active
    pub fn current_watermark(&self, bounded_delay: Duration) -> Option<Timestamp> {
        if self.is_idle {
            None
        } else {
            self.last_event_time.map(|t| t - bounded_delay)
        }
    }
}
```

### Heartbeat Mechanism (Alternative)

External signals carrying progress timestamps, useful when:
- Sources have known data patterns
- Explicit "end of batch" signals exist
- Coordination with external systems required

```rust
pub enum WatermarkAdvancement {
    /// Watermark from observed event
    FromEvent(Timestamp),
    
    /// Watermark from external heartbeat
    FromHeartbeat(Timestamp),
    
    /// Source marked idle, exclude from calculation
    Idle,
}
```

---

## 5. Watermark Propagation

### Propagation Rules

For operators with **single input:**
```
output_watermark = input_watermark
```

For operators with **multiple inputs:**
```
output_watermark = min(input_watermarks)
```

### Multi-Stream Watermark Policies

**Minimum Policy (Default):**
- Global watermark = minimum across all streams
- Safe: No data accidentally dropped
- Slow: Moves at pace of slowest stream

**Maximum Policy (Aggressive):**
- Global watermark = maximum across all streams
- Fast: Results emitted quickly
- Risk: May drop valid late data from slower streams

**Spark Configuration:**
```sql
SET spark.sql.streaming.multipleWatermarkPolicy = 'max';  -- default is 'min'
```

### Watermark Propagation Through Operators

```
┌──────────┐    WM=100    ┌──────────┐    WM=95     ┌──────────┐
│ Source A │─────────────▶│          │─────────────▶│          │
└──────────┘              │   Join   │              │  Window  │
┌──────────┐    WM=95     │          │              │   Agg    │
│ Source B │─────────────▶│ WM=min() │              │          │
└──────────┘              └──────────┘              └──────────┘
                               │
                          Output WM = 95
```

### Chained Stateful Operators

Spark 3.4+ supports watermark propagation simulation for:
- Stream-stream joins followed by aggregations
- Multiple stateful operators in single pipeline

**Key insight:** Each operator may transform the watermark based on its semantics (e.g., join condition time bounds).

---

## 6. EMIT ON WINDOW CLOSE (EOWC)

### Concept

Instead of emitting partial/updated results continuously, emit **once when window definitively closes**.

### Default Behavior: Emit-on-Update

```sql
-- Continuous updates as data arrives
CREATE MATERIALIZED VIEW counts AS
SELECT window_start, COUNT(*)
FROM TUMBLE(events, event_time, INTERVAL '1' MINUTE)
GROUP BY window_start;
```

**Output pattern:**
```
(10:00, 1)  → (10:00, 2) → (10:00, 3) → (10:00, 5)  -- updates
```

### EOWC Behavior

```sql
-- Single emission when window closes
CREATE MATERIALIZED VIEW counts AS
SELECT window_start, COUNT(*)
FROM TUMBLE(events, event_time, INTERVAL '1' MINUTE)
GROUP BY window_start
EMIT ON WINDOW CLOSE;
```

**Output pattern:**
```
(10:00, 5)  -- final result only
```

### When to Use EOWC

| Use Case | Why EOWC |
|----------|----------|
| Append-only sinks (Kafka, S3) | Avoid updates/retractions |
| Expensive computations (percentiles) | Compute once, not incrementally |
| Downstream expects final results | Simpler consumer logic |
| Reduce write amplification | Single write per window |

### Requirements

1. **Watermark must be defined** on the source
2. Window closes when `watermark > window_end`
3. Late data after window close is **dropped** (or handled separately)

### RisingWave Implementation

```sql
-- Source with watermark
CREATE SOURCE events (
    event_id INT,
    event_time TIMESTAMP,
    value INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (...);

-- EOWC materialized view
CREATE MATERIALIZED VIEW hourly_stats AS
SELECT 
    window_start,
    MAX(value) as max_val,
    AVG(value) as avg_val
FROM TUMBLE(events, event_time, INTERVAL '1' HOUR)
GROUP BY window_start
EMIT ON WINDOW CLOSE;
```

---

## 7. Triggers and Late Data Handling

### The Trade-off Triangle

```
        Correctness
           /\
          /  \
         /    \
        /      \
       /________\
    Latency    Cost
```

- **Slow watermark:** Correct results, high latency
- **Fast watermark:** Low latency, drops valid data
- **Retain state longer:** Correct + low latency, high memory cost

### Early-Fire and Late-Fire Triggers (Flink)

**Early-Fire:** Emit preliminary results before window closes

```sql
SET table.exec.emit.early-fire.enabled = true;
SET table.exec.emit.early-fire.delay = 1m;  -- Fire every minute
```

**Late-Fire:** Emit updates when late data arrives after window close

```sql
SET table.exec.emit.late-fire.enabled = true;
SET table.exec.emit.late-fire.delay = 1m;
SET table.exec.emit.allow-lateness = 1h;  -- Keep state for 1 hour
```

### Late Data Strategies

| Strategy | Description | Use When |
|----------|-------------|----------|
| Drop | Discard late events | Latency critical, minor inaccuracy OK |
| Side Output | Route to separate stream | Need to audit/reprocess late data |
| Recompute | Update window results | Correctness critical, updates OK |
| Allow Lateness | Extended window state | Known late data pattern |

### Implementation Considerations

```rust
pub enum LateDataPolicy {
    /// Drop events arriving after watermark
    Drop,
    
    /// Route to side output for separate handling
    SideOutput { output_tag: String },
    
    /// Allow updates within lateness bound
    AllowLateness { max_lateness: Duration },
}

pub struct WindowState {
    window_end: Timestamp,
    aggregation: AggregationState,
    policy: LateDataPolicy,
}

impl WindowState {
    pub fn should_accept(&self, event_time: Timestamp, watermark: Timestamp) -> LateDataDecision {
        if event_time >= watermark {
            LateDataDecision::Accept
        } else {
            match &self.policy {
                LateDataPolicy::Drop => LateDataDecision::Drop,
                LateDataPolicy::SideOutput { tag } => LateDataDecision::SideOutput(tag.clone()),
                LateDataPolicy::AllowLateness { max } => {
                    if watermark - event_time <= *max {
                        LateDataDecision::AcceptLate
                    } else {
                        LateDataDecision::Drop
                    }
                }
            }
        }
    }
}
```

---

## 8. Multi-Source Watermark Alignment

### The Problem

When sources have different processing speeds, fast sources can cause:
- Excessive state growth (buffering data waiting for slow sources)
- Memory pressure on downstream operators
- Uneven resource utilization

### Watermark Alignment Solution

Group sources and enforce maximum watermark drift.

**Flink Configuration:**
```java
WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(20))
    .withWatermarkAlignment(
        "alignment-group-1",           // Group name
        Duration.ofSeconds(20),        // Max allowed drift
        Duration.ofSeconds(1)          // Update interval
    )
```

### Behavior

1. Sources in same group coordinate watermarks
2. If a source advances too far ahead, consumption is **paused**
3. Other sources catch up, then paused source resumes
4. Prevents unbounded state growth

### Implementation Sketch

```rust
pub struct WatermarkAlignmentGroup {
    group_id: String,
    max_drift: Duration,
    update_interval: Duration,
    
    /// Watermarks from all group members
    member_watermarks: HashMap<SourceId, Timestamp>,
}

impl WatermarkAlignmentGroup {
    pub fn should_pause(&self, source_id: &SourceId) -> bool {
        let my_watermark = self.member_watermarks.get(source_id).unwrap();
        let min_watermark = self.member_watermarks.values().min().unwrap();
        
        (*my_watermark - *min_watermark) > self.max_drift
    }
    
    pub fn report_watermark(&mut self, source_id: SourceId, watermark: Timestamp) {
        self.member_watermarks.insert(source_id, watermark);
    }
}
```

### Split-Level Alignment (Flink 1.17+)

For sources with multiple splits/partitions per task:
- Alignment at individual split level
- Pauses specific splits, not entire source task
- Finer-grained control

---

## 9. Low-Latency Considerations

### Latency Budget Tiers

| Tier | E2E Latency | Constraints |
|------|-------------|-------------|
| Ultra-low | <5ms | No disk fsync, no cross-region, minimal buffering |
| Low | 5-100ms | SSD/NVMe OK, single region, moderate batching |
| Relaxed | 100ms-minutes | Aggressive batching, cost optimization |

### LaminarDB-Specific Recommendations

Given the **sub-500ns hot path** target:

**DO:**
- Generate watermarks in **Ring 1** (background), not Ring 0
- Use **epoch-based batching** for watermark propagation
- Memory-map watermark tracking state
- Use monotonic clocks for internal timing
- Batch watermark updates (don't emit per-event)

**DON'T:**
- Compute watermarks in the hot path
- Allocate memory for watermark operations
- Use locks in watermark tracking structures
- Emit watermarks synchronously with events

### Watermark Update Batching

```rust
pub struct BatchedWatermarkEmitter {
    /// Accumulated max event time since last emission
    pending_max_event_time: AtomicU64,
    
    /// Last emitted watermark
    last_emitted: AtomicU64,
    
    /// Emission interval
    emit_interval: Duration,
    
    /// Bounded delay for watermark calculation
    bounded_delay: Duration,
}

impl BatchedWatermarkEmitter {
    /// Called from Ring 0 - lock-free update
    #[inline]
    pub fn observe_event_time(&self, event_time: Timestamp) {
        self.pending_max_event_time.fetch_max(
            event_time.as_micros() as u64,
            Ordering::Relaxed
        );
    }
    
    /// Called from Ring 1 - periodic emission
    pub fn maybe_emit(&self) -> Option<Timestamp> {
        let max_seen = self.pending_max_event_time.load(Ordering::Acquire);
        let watermark = Timestamp::from_micros(max_seen) - self.bounded_delay;
        
        let last = self.last_emitted.load(Ordering::Acquire);
        if watermark.as_micros() as u64 > last {
            self.last_emitted.store(watermark.as_micros() as u64, Ordering::Release);
            Some(watermark)
        } else {
            None
        }
    }
}
```

### Nanosecond Timestamp Considerations

For sub-microsecond operations:
- Use `u64` nanoseconds internally (not `SystemTime`)
- Unique timestamps: combine nanosecond time + counter
- Clock source: `CLOCK_MONOTONIC_RAW` or TSC

```rust
/// High-resolution timestamp for low-latency systems
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct NanoTimestamp(u64);

impl NanoTimestamp {
    #[inline]
    pub fn now() -> Self {
        // Platform-specific high-resolution clock
        Self(precise_nanos())
    }
    
    #[inline]
    pub fn as_nanos(&self) -> u64 {
        self.0
    }
}
```

---

## 10. SQL Declaration Patterns

### Source-Level Watermark (RisingWave/Flink)

```sql
CREATE SOURCE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10, 2),
    order_time TIMESTAMP,
    
    -- Watermark declaration
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    connector = 'kafka',
    topic = 'orders',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

### Table with Watermark

```sql
CREATE TABLE events (
    event_id INT,
    event_time TIMESTAMP,
    payload JSONB,
    
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    connector = 'kafka',
    ...
);
```

### Generated Column for Nested Timestamps

```sql
CREATE SOURCE nested_events (
    id BIGINT,
    metadata STRUCT<
        created_at TIMESTAMP,
        source VARCHAR
    >,
    
    -- Extract nested timestamp
    event_time AS (metadata).created_at,
    
    -- Watermark on extracted column
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (...);
```

### Supported Time Units

- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `MONTH` (less common)
- `YEAR` (rare)

### Limitations

- Only **one watermark column** per source/table
- Watermark expression must return `TIMESTAMP`
- Cannot watermark on computed columns in some systems

---

## 11. Implementation Recommendations for LaminarDB

### Phase 1: Foundation (Current)

**Scope:**
- Bounded out-of-orderness watermark generator
- Single global watermark per source
- Basic idle detection
- SQL `WATERMARK FOR` clause

**Key Decisions:**
```rust
/// Core watermark trait - design for extensibility
pub trait WatermarkGenerator: Send + Sync {
    /// Update with new event timestamp
    fn on_event(&mut self, event_time: Timestamp);
    
    /// Get current watermark (may return None if no events yet)
    fn current_watermark(&self) -> Option<Timestamp>;
    
    /// Check/update idle status
    fn check_idle(&mut self) -> bool;
    
    /// Called periodically from Ring 1
    fn on_periodic_emit(&mut self) -> Option<Timestamp>;
}

/// Phase 1 implementation
pub struct BoundedOutOfOrdernessWatermark {
    max_event_time: Option<Timestamp>,
    bounded_delay: Duration,
    last_activity: Instant,
    idle_timeout: Duration,
    is_idle: bool,
}
```

**Integration Points:**
- Ring 1: Watermark generation and emission
- Ring 0: Lock-free event time observation
- SQL Parser: `WATERMARK FOR` clause

### Phase 2: Per-Partition Tracking

**Scope:**
- Track watermarks per source partition
- Enhanced idle detection per partition
- Partition-aware global watermark calculation

**Extension:**
```rust
pub struct PartitionedWatermarkTracker {
    partitions: HashMap<PartitionId, BoundedOutOfOrdernessWatermark>,
    global_watermark: Timestamp,
}
```

### Phase 3: Keyed Watermarks

**Scope:**
- Per-key watermark tracking
- Configurable key extraction
- Memory-efficient key watermark storage

**Extension:**
```rust
pub trait KeyedWatermarkGenerator<K>: WatermarkGenerator {
    fn on_keyed_event(&mut self, key: K, event_time: Timestamp);
    fn watermark_for_key(&self, key: &K) -> Option<Timestamp>;
}
```

### Phase 4: EMIT ON WINDOW CLOSE

**Scope:**
- Window-aware watermark consumption
- EOWC SQL syntax
- Late data policies

**SQL:**
```sql
CREATE MATERIALIZED VIEW ... EMIT ON WINDOW CLOSE;
```

### Phase 5: Multi-Source Alignment

**Scope:**
- Watermark alignment groups
- Cross-source coordination
- Backpressure integration

### Phase 6: Adaptive Watermarks

**Scope:**
- Lateness pattern learning
- Dynamic delay adjustment
- Concept drift detection

---

## 12. References

### Academic Papers

1. **Keyed Watermarks: A Fine-grained Watermark Generation for Apache Flink**  
   Yasser T., Arafa T., ElHelw M., Awad A.  
   *Future Generation Computer Systems*, March 2025  
   DOI: 10.1016/j.future.2025.107796

2. **Watermarks in Stream Processing Systems: Semantics and Comparative Analysis**  
   Begoli E., et al.  
   *Proceedings of the VLDB Endowment*, Vol 14, No 12

3. **The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost**  
   Akidau T., et al.  
   *VLDB Endowment*, 2015

4. **Adaptive Watermark Generation Mechanism Based on Time Series Prediction**  
   *Frontiers of Computer Science*, 2021

### System Documentation

5. **Apache Flink: Generating Watermarks**  
   https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/

6. **RisingWave: Watermarks**  
   https://docs.risingwave.com/processing/watermarks

7. **RisingWave: EMIT ON WINDOW CLOSE**  
   https://docs.risingwave.com/processing/emit-on-window-close

8. **Databricks: Watermarking in Structured Streaming**  
   https://www.databricks.com/blog/feature-deep-dive-watermarking-apache-spark-structured-streaming

### Industry Resources

9. **Latency Numbers Every Data Streaming Engineer Should Know**  
   StreamNative, September 2025

10. **Streaming Systems** (Book)  
    Akidau T., Chernyak S., Lax R.  
    O'Reilly Media, 2018

---

## Appendix A: Quick Reference

### Watermark Formula Cheat Sheet

| Strategy | Formula | Use Case |
|----------|---------|----------|
| Bounded OOO | `max_event - delay` | General purpose |
| Monotonous | `current_event` | Ordered streams |
| Per-Key | `min(key_watermarks)` | Multi-tenant |
| Aligned | `min(group_watermarks)` | Multi-source |

### Configuration Checklist

- [ ] Bounded delay set based on observed lateness distribution
- [ ] Idle timeout configured for sparse data sources  
- [ ] Late data policy defined (drop/side-output/allow)
- [ ] Watermark emission interval tuned for latency/overhead balance
- [ ] Per-partition tracking enabled for Kafka sources
- [ ] Alignment groups configured for multi-source joins

### Common Pitfalls

1. **Watermark never advances:** Check for idle partitions
2. **Too much late data dropped:** Increase bounded delay
3. **High memory usage:** Reduce allowed lateness, enable state TTL
4. **Results delayed:** Check for slow sources blocking global watermark
5. **Non-deterministic results:** Using processing time instead of event time

---

*Document generated for LaminarDB development. Update as implementation progresses.*
