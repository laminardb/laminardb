# Latest Emit Patterns in Streaming Databases (2026)

## Research Summary for LaminarDB F012 Gap Analysis

**Date:** January 2026  
**Context:** LaminarDB Phase 1 F012 (Streaming Query Optimizer) implementation  
**Goal:** Identify latest emit patterns and gaps in current LaminarDB design

---

## Executive Summary

The streaming database landscape in 2025-2026 has seen significant evolution in emit patterns, driven by three major developments:

1. **DBSP/Feldera's Z-set model** becoming the theoretical foundation for incremental computation
2. **Flink 2.0's disaggregated state management** with asynchronous execution
3. **Snowflake's Delayed View Semantics (DVS)** bridging streaming and batch

For LaminarDB's sub-500ns latency target, several patterns are **critical gaps** that should be addressed.

---

## 1. Modern Emit Pattern Taxonomy (2025-2026)

### 1.1 Primary Emit Strategies

| Strategy | Description | Latency Profile | Best For |
|----------|-------------|-----------------|----------|
| **EMIT ON UPDATE** | Emit partial results on every change | Ultra-low latency | Interactive dashboards, alerts |
| **EMIT ON WINDOW CLOSE** | Final result when watermark passes window end | Medium latency | Append-only sinks, Kafka, S3 |
| **EMIT FINAL** | Suppress intermediate, emit only finalized | High latency | BI reporting, exact aggregates |
| **EMIT DELTA** | Only emit changes (inserts/deletes) | Variable | CDC pipelines, replication |
| **EMIT RETRACTION** | Emit `-old, +new` pairs for corrections | Medium | Late data handling, corrections |

### 1.2 Industry Implementations

**RisingWave (v2.7 - Jan 2026):**
```sql
-- Emit on update (default - maintains view consistency)
CREATE MATERIALIZED VIEW mv AS
SELECT window_start, COUNT(*)
FROM TUMBLE(events, event_time, INTERVAL '1' MINUTE)
GROUP BY window_start;

-- Emit on window close (for append-only sinks)
CREATE MATERIALIZED VIEW mv AS
SELECT window_start, COUNT(*)
FROM TUMBLE(events, event_time, INTERVAL '1' MINUTE)
GROUP BY window_start
EMIT ON WINDOW CLOSE;
```

**ksqlDB/Confluent Flink:**
```sql
-- Emit changes (streaming)
SELECT * FROM pageviews EMIT CHANGES;

-- Emit final (windowed aggregation)
SELECT windowstart, item_id, SUM(quantity)
FROM orders
WINDOW TUMBLING (SIZE 20 SECONDS)
GROUP BY item_id
EMIT FINAL;
```

**Spark Structured Streaming (2025 - Real-time Mode):**
- **Append Mode**: Emit only finalized records (after watermark)
- **Update Mode**: Emit changed records each trigger
- **Complete Mode**: Emit entire state (deprecated for scale)

---

## 2. Key 2025-2026 Advancements

### 2.1 DBSP/Feldera Z-Set Model (VLDB 2025 Award)

The DBSP framework provides a **mathematical foundation** for incremental computation:

**Core Concept: Z-sets**
```
Z-set = multiset where elements have integer weights
  weight > 0 → insert
  weight < 0 → delete
  weight = 0 → no change
```

**Key Innovation:** Automatic incrementalization of ANY SQL query
- Work proportional to `O(|ΔInput|)` not `O(|Input|)`
- Supports full SQL including recursive queries
- Retractions are first-class citizens

**Feldera Implementation:**
```sql
-- Feldera automatically incrementalizes this
CREATE VIEW complex_aggregation AS
SELECT customer_id, 
       SUM(amount) as total,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median
FROM orders
GROUP BY customer_id;
-- Updates emit as changes to the view
```

### 2.2 Flink 2.0 Disaggregated State Management (VLDB 2025)

**Key Changes:**
1. **Asynchronous Execution Model**: State access no longer blocks operators
2. **ForSt State Backend**: DFS as primary storage, local disk as cache
3. **State V2 APIs**: Non-blocking state read/write

**Performance Impact:**
- 75%-120% throughput improvement for heavy I/O queries
- Instant recovery (no state download)
- Unlimited state size

**Emit Implications:**
- Watermark propagation can be async
- State updates streamed continuously to DFS
- Checkpoint overhead drastically reduced

### 2.3 Snowflake Dynamic Tables & Delayed View Semantics (SIGMOD 2025)

**Delayed View Semantics (DVS):**
- DT content equivalent to view computed at some past time
- "Data timestamp" tracks how fresh the view is
- Target lag controls freshness vs cost tradeoff

**Key Innovation:** Bridging streaming and batch
```sql
CREATE DYNAMIC TABLE my_aggregation
  TARGET_LAG = '1 minute'  -- Freshness SLA
  WAREHOUSE = my_warehouse
AS
SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id;
```

---

## 3. Emit Patterns by Use Case

### 3.1 Ultra-Low Latency (<5ms E2E)

**Requirements:**
- No disk fsync on hot path
- Memory-resident state
- Minimal network hops

**Pattern:** EMIT ON UPDATE with zero-copy
- Emit immediately on state change
- Use memory-mapped state stores
- Avoid any blocking operations

**LaminarDB Alignment:** ✅ Ring 0 architecture supports this

### 3.2 Low Latency (5-100ms E2E)

**Requirements:**
- Interactive dashboards
- Alerting systems
- Online feature engineering

**Pattern:** EMIT ON UPDATE with batching
- Micro-batch updates (epoch-based)
- Watermark-driven progress
- Delta emission with optional retractions

**LaminarDB Alignment:** ⚠️ Need epoch batching strategy

### 3.3 Append-Only Sinks (Kafka, S3, Iceberg)

**Requirements:**
- No updates to already-written data
- Exactly-once delivery
- Minimal write amplification

**Pattern:** EMIT ON WINDOW CLOSE
- Wait for watermark to pass window end
- Emit final, immutable results
- State GC after emission

**LaminarDB Alignment:** ⚠️ Partially designed, needs completion

### 3.4 Change Data Capture (CDC)

**Requirements:**
- Track all changes
- Support downstream replication
- Handle out-of-order updates

**Pattern:** EMIT DELTA with changelog
- `+I` (insert), `-D` (delete), `+U/-U` (update before/after)
- Sequence numbers for ordering
- Tombstones for deletes

**LaminarDB Alignment:** ⚠️ Not currently in F012 spec

---

## 4. Gap Analysis: LaminarDB vs Industry State-of-Art

### 4.1 CRITICAL GAPS (Must Address in Phase 1)

#### Gap 1: Explicit Emit Strategy SQL Syntax
**Industry Standard:**
```sql
-- RisingWave
EMIT ON WINDOW CLOSE

-- ksqlDB
EMIT CHANGES | EMIT FINAL

-- Spark
outputMode("append" | "update" | "complete")
```

**LaminarDB Current:** Not specified in F012
**Recommendation:** Add `EMIT` clause to streaming SQL grammar

#### Gap 2: Changelog/Retraction Support
**Industry Standard:** DBSP Z-sets, Flink retractions, Spark Update mode
**LaminarDB Current:** Mentioned in F012 but implementation unclear
**Recommendation:** Implement Z-set style weights for all operators

#### Gap 3: Watermark-Emit Coordination
**Industry Standard:** Watermark crossing window end triggers emit
**LaminarDB Current:** Watermark spec exists but emit coupling unclear
**Recommendation:** Add explicit watermark-to-emit binding in optimizer

### 4.2 IMPORTANT GAPS (Phase 1-2)

#### Gap 4: Async State Access Pattern
**Industry Standard:** Flink 2.0 State V2 async APIs
**LaminarDB Current:** Ring architecture separates concerns but not async
**Recommendation:** Evaluate async state access for Ring 1 operations

#### Gap 5: Epoch/Batch Emit Strategy
**Industry Standard:** RisingWave epoch batching, Spark micro-batch
**LaminarDB Current:** Not specified
**Recommendation:** Add configurable epoch size for non-hot-path workloads

#### Gap 6: CDC Changelog Format
**Industry Standard:** Debezium envelope, Flink CDC, Maxwell
**LaminarDB Current:** Not specified
**Recommendation:** Define standard changelog format for sinks

### 4.3 NICE-TO-HAVE GAPS (Phase 2+)

#### Gap 7: Target Lag / Freshness SLA
**Industry Standard:** Snowflake Dynamic Tables TARGET_LAG
**LaminarDB Current:** Not specified
**Recommendation:** Consider for Phase 2 enterprise features

#### Gap 8: Disaggregated State Tiering
**Industry Standard:** Flink 2.0 ForSt with DFS primary
**LaminarDB Current:** RocksDB for checkpoints only
**Recommendation:** Evaluate for very large state workloads

---

## 5. Recommended Updates to F012 Spec

### 5.1 SQL Syntax Extensions

```sql
-- Add EMIT clause to streaming queries
CREATE MATERIALIZED VIEW sales_per_minute AS
SELECT window_start, SUM(amount)
FROM TUMBLE(sales, event_time, INTERVAL '1 minute')
GROUP BY window_start
EMIT ON WINDOW CLOSE;  -- NEW

-- Emit strategy options
EMIT ON UPDATE          -- Default: emit partial results immediately
EMIT ON WINDOW CLOSE    -- Emit final when watermark passes
EMIT CHANGES            -- Emit changelog with +/- weights
EMIT FINAL              -- Emit only finalized (suppress intermediate)

-- Optional: freshness hint
WITH EMIT_DELAY '100ms'  -- Batch emissions for throughput
```

### 5.2 Optimizer Rules to Add

| Rule Name | Priority | Description |
|-----------|----------|-------------|
| `emit_strategy_propagation` | P0 | Propagate emit strategy through plan |
| `changelog_enable` | P0 | Enable retraction tracking when needed |
| `emit_window_close_optimization` | P1 | Optimize for append-only sinks |
| `emit_batching` | P1 | Batch emissions for throughput |
| `late_data_retraction` | P2 | Generate retractions for late arrivals |

### 5.3 Data Structures to Add

```rust
/// Emit strategy for streaming operators
#[derive(Clone, Copy, Debug)]
pub enum EmitStrategy {
    /// Emit on every state change (lowest latency)
    OnUpdate,
    /// Emit when watermark passes window end
    OnWindowClose,
    /// Emit changelog with weights
    Changelog,
    /// Suppress intermediate, emit only final
    Final,
}

/// Changelog record with Z-set weight
pub struct ChangelogRecord<T> {
    pub data: T,
    pub weight: i64,  // +1 insert, -1 delete, 0 no-op
    pub timestamp: EventTime,
}

/// Emit configuration per operator
pub struct EmitConfig {
    pub strategy: EmitStrategy,
    pub batch_delay: Option<Duration>,  // For throughput optimization
    pub changelog_enabled: bool,
    pub retraction_enabled: bool,
}
```

---

## 6. Implementation Prompts for Claude Code

### Prompt 1: Add Emit Strategy to SQL Parser

```
Context: LaminarDB F012 Streaming Query Optimizer
Task: Extend DataFusion SQL parser to support EMIT clause

Requirements:
1. Add EMIT clause to CREATE MATERIALIZED VIEW syntax
2. Support: EMIT ON UPDATE, EMIT ON WINDOW CLOSE, EMIT CHANGES, EMIT FINAL
3. Parse emit strategy and attach to StreamingPlan
4. Default to EMIT ON UPDATE if not specified

Files to modify:
- src/sql/parser.rs (or DataFusion extension point)
- src/optimizer/streaming_plan.rs

Reference: RisingWave SQL grammar for EMIT ON WINDOW CLOSE
```

### Prompt 2: Implement Z-Set Changelog Support

```
Context: LaminarDB F012 incremental computation
Task: Implement Z-set style changelog records

Requirements:
1. Define ChangelogRecord<T> with weight: i64
2. Modify all aggregate operators to track insert/delete weights
3. Implement retraction generation for late data
4. Add changelog serialization format for sinks

Key insight from DBSP:
- weight > 0 means insert
- weight < 0 means delete
- SUM of weights = current count

Files to create:
- src/types/changelog.rs
- src/operators/retraction.rs
```

### Prompt 3: Emit Strategy Optimizer Rule

```
Context: LaminarDB F012 optimizer rules
Task: Add emit_strategy_propagation rule

Requirements:
1. Analyze sink requirements (append-only vs upsert)
2. Propagate emit strategy requirements up the plan tree
3. Enable changelog tracking only when needed
4. Optimize for append-only sinks by using EMIT ON WINDOW CLOSE

Example transformation:
- Query with Kafka sink (append-only) → force EMIT ON WINDOW CLOSE
- Query with upsert table sink → allow EMIT ON UPDATE

Reference: Feldera's automatic incrementalization approach
```

### Prompt 4: Watermark-Emit Coordination

```
Context: LaminarDB F012 watermark handling
Task: Coordinate watermark advancement with emit triggering

Requirements:
1. For EMIT ON WINDOW CLOSE: trigger emit when watermark > window_end
2. Track pending windows per partition
3. Implement emit scheduling in Ring 1 (background)
4. Ensure exactly-once emission semantics

Key insight from RisingWave:
- Watermark must be defined on source
- Window close detection in aggregation operator
- State GC after emission

Files to modify:
- src/operators/window_aggregate.rs
- src/watermark/propagation.rs
```

### Prompt 5: Ring Architecture Emit Integration

```
Context: LaminarDB three-ring architecture
Task: Integrate emit patterns with Ring 0/1/2 separation

Requirements:
1. Ring 0 (hot path): EMIT ON UPDATE with zero allocation
2. Ring 1 (background): EMIT ON WINDOW CLOSE, batching, changelog generation
3. Ring 2 (control): Emit strategy configuration, plan compilation

Constraints:
- Ring 0 must never block on emit
- Ring 1 handles watermark advancement and window closing
- Emit to external sinks (Kafka, S3) always in Ring 1

Reference: Thread-per-core model from previous conversations
```

---

## 7. Testing Strategy for Emit Patterns

### 7.1 Property-Based Tests

```rust
// Property: EMIT ON UPDATE produces same final result as EMIT ON WINDOW CLOSE
#[proptest]
fn emit_strategies_produce_same_final_result(events: Vec<Event>) {
    let on_update_result = run_query_emit_on_update(&events);
    let on_window_close_result = run_query_emit_on_window_close(&events);
    
    // After all windows close, results should be identical
    assert_eq!(
        final_state(&on_update_result),
        final_state(&on_window_close_result)
    );
}

// Property: Changelog weights sum correctly
#[proptest]
fn changelog_weights_sum_to_current_count(changes: Vec<ChangelogRecord>) {
    let sum: i64 = changes.iter().map(|c| c.weight).sum();
    assert_eq!(sum, current_count_in_state());
}
```

### 7.2 Benchmarks

| Benchmark | Target | Metric |
|-----------|--------|--------|
| emit_on_update_latency | < 500ns | p99 latency |
| emit_on_window_close_throughput | > 1M events/sec | throughput |
| changelog_generation_overhead | < 10% | CPU overhead vs non-changelog |
| late_data_retraction_latency | < 1ms | retraction propagation time |

---

## 8. References

### Academic Papers
1. **DBSP: Automatic IVM for Rich Query Languages** - VLDB 2023/2025 (Best Paper)
2. **Disaggregated State Management in Flink 2.0** - VLDB 2025
3. **Streaming Democratized: Delayed View Semantics** - SIGMOD 2025

### Industry Documentation
- RisingWave: https://docs.risingwave.com/processing/emit-on-window-close
- Confluent Flink: https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing
- Spark Structured Streaming: https://docs.databricks.com/structured-streaming/output-mode
- Feldera: https://docs.feldera.com/

### Related LaminarDB Conversations
- Watermark generator trends (Jan 2026)
- Thread-per-core and ring architecture
- DBSP/Z-set research for F012

---

## Appendix: Quick Reference Card

### Emit Strategy Decision Tree

```
Is sink append-only (Kafka, S3)?
├── YES → EMIT ON WINDOW CLOSE
└── NO → Can sink handle updates?
    ├── YES → EMIT ON UPDATE (lowest latency)
    └── NO → Need changelog?
        ├── YES → EMIT CHANGES (with Z-set weights)
        └── NO → EMIT FINAL (suppress intermediate)
```

### Latency vs Emit Strategy

```
Latency Requirement    Recommended Strategy
─────────────────────────────────────────────
< 5ms                  EMIT ON UPDATE (Ring 0)
5-100ms               EMIT ON UPDATE + batching
100ms - 1min          EMIT ON WINDOW CLOSE
> 1min                EMIT FINAL or batch
```
