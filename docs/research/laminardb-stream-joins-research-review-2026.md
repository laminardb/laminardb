# LaminarDB Stream Joins: Research Review & Gap Analysis (2025-2026)

**Date:** January 24, 2026  
**Scope:** Review of LaminarDB stream join implementation against latest research and production systems

---

## Executive Summary

This document reviews LaminarDB's stream join implementation against the latest research and production systems from 2025-2026. Key findings indicate that while LaminarDB has solid foundational designs for several join types, there are significant opportunities to incorporate recent innovations in:

1. **Disaggregated State Management** (Flink 2.0)
2. **Incremental Join Computation** (DBSP/Feldera)
3. **CPU-Friendly Join State Encoding** (RisingWave July 2025)
4. **Asynchronous State Access** for join operators
5. **Multi-way Join Optimization** with adaptive probe ordering

---

## 1. Join Types: LaminarDB vs. Latest Research

### 1.1 Symmetric Hash Join (Stream-Stream Join)

**LaminarDB Current Design:**
- Maintains hash tables for both sides
- Interval-based state cleanup via watermarks
- State bounded by window size

**Latest Research (2025-2026):**

| Innovation | Source | Description | Relevance to LaminarDB |
|------------|--------|-------------|----------------------|
| **CPU-Friendly Encoding** | RisingWave July 2025 | Configurable encoding for cached join rows; 50% perf improvement when state fits in memory | HIGH - Direct applicability |
| **Asymmetric Optimization** | Epsio Blog 2025 | Leverage write-heavy vs read-heavy asymmetry to skip compaction on finished side | MEDIUM - Reduces overhead |
| **Disaggregated State** | Flink 2.0 (VLDB 2025) | Asynchronous state access with ForSt backend; out-of-order record processing | HIGH - Cloud-native scaling |
| **Sliding Window Hash Join** | Synnada/DataFusion | Order-preserving joins with build-side pruning | MEDIUM - Correctness guarantees |

**Gap Analysis:**
```
LaminarDB Status: ✅ Basic symmetric hash join
Missing:
  ❌ CPU-friendly row encoding options
  ❌ Asynchronous state access patterns
  ❌ Asymmetric optimization for uneven sides
  ❌ Build-side pruning for memory efficiency
```

**Recommended Actions:**
1. Implement configurable row encoding (compact vs. CPU-friendly)
2. Add session variable for encoding selection per query
3. Implement asymmetric compaction strategy

---

### 1.2 Temporal Join (Stream-Table / LATEST Join)

**LaminarDB Current Design:**
```sql
SELECT s.*, t.*
FROM stream s
JOIN LATEST table t ON s.key = t.key;
```
- Non-deterministic (result depends on processing time)
- Lower state requirements (only latest version)
- Good for rarely-changing dimensions

**Latest Research (2025-2026):**

| Innovation | Source | Description |
|------------|--------|-------------|
| **Append-Only vs Non-Append-Only** | RisingWave 2025 | Separate code paths - append-only needs no state; non-append-only materializes lookup results |
| **Index-Based Lookup** | RisingWave 2025 | Join against table indexes for performance |
| **Process-Time vs Event-Time** | RisingWave Docs | Clear semantic distinction with different consistency guarantees |

**RisingWave Temporal Join Categories:**
```
1. Append-Only Process-Time Temporal Join
   - No state maintained
   - Dimension table not updating
   - Highest performance
   
2. Non-Append-Only Process-Time Temporal Join
   - Internal state for LHS insertions
   - Supports retractions
   - Updates affect previously joined results
```

**Gap Analysis:**
```
LaminarDB Status: ✅ Basic LATEST join semantics
Missing:
  ❌ Append-only vs non-append-only distinction
  ❌ Index-based temporal joins
  ❌ Clear process-time vs event-time semantics
```

---

### 1.3 ASOF Join (Temporal Proximity Join)

**LaminarDB Current Design:**
```rust
pub struct AsofJoinOperator {
    right_state: HashMap<String, Vec<u8>>,
    tolerance_nanos: i64,
    watermark_nanos: i64,
}
```
- O(1) lookup for latest value per symbol
- Tolerance-based matching

**Latest Research (2025-2026):**

| System | Implementation | Key Innovation |
|--------|---------------|----------------|
| **DuckDB** (Feb 2025) | Adaptive plan selection | Loop join for small left tables; sorted merge for large |
| **Apache Pinot** (Sep 2025) | Hybrid hash + binary search | Hash on join key, binary search on time within groups |
| **RisingWave** (v2.1+) | Streaming ASOF | Continuous state maintenance with MVs |
| **Snowflake** | Batch ASOF | MATCH_CONDITION syntax with >=, <=, >, < operators |
| **DolphinDB** | Streaming engine | useSystemTime flag for process-time vs event-time |

**DuckDB AsOf Planning Innovation (Feb 2025):**
```
Small left table + Large right table:
  → Swap sides, stream large table through
  → Keep only latest matches
  → Very little memory, highly parallelizable

Large both sides:
  → Standard sorted merge approach
  → Configurable threshold: asof_loop_join_threshold (default: 64)
```

**Apache Pinot ASOF Implementation (Sep 2025):**
```sql
SELECT ... 
FROM table1 ASOF JOIN table2 
MATCH_CONDITION(table1.time_col >= table2.time_col) 
ON table1.join_key = table2.join_key;

-- Implementation:
-- 1. Hash partition on join_key (ON clause)
-- 2. Sort/tree structure on time_col within partitions
-- 3. Binary search for closest match
```

**Gap Analysis:**
```
LaminarDB Status: ✅ Basic ASOF with O(1) lookup
Missing:
  ❌ Adaptive plan selection (small vs large)
  ❌ Binary search within partitions for range queries
  ❌ Multiple comparison operators (>=, <=, >, <)
  ❌ Forward/Backward/Nearest modes
```

**Recommended Implementation:**
```rust
pub enum AsofDirection {
    Backward,  // t_left >= t_right (default, most common)
    Forward,   // t_left <= t_right
    Nearest,   // min(|t_left - t_right|)
}

pub struct AsofJoinOperator {
    // Keep sorted tree per join key for range queries
    right_state: HashMap<JoinKey, BTreeMap<Timestamp, Row>>,
    direction: AsofDirection,
    tolerance_nanos: Option<i64>,
    // For adaptive planning
    left_cardinality_estimate: usize,
    right_cardinality_estimate: usize,
}
```

---

### 1.4 Interval Join (Time-Bounded Stream-Stream)

**LaminarDB Current Design:**
```sql
SELECT a.*, b.*
FROM stream_a a
JOIN stream_b b
    ON a.key = b.key
    AND a.event_time BETWEEN b.event_time - INTERVAL '1 second'
                         AND b.event_time + INTERVAL '1 second';
```

**Latest Research:**

| Innovation | Source | Description |
|------------|--------|-------------|
| **Watermark-Based Cleanup** | RisingWave | State cleaning triggered on upstream messages per join key |
| **Stale State Warning** | RisingWave Docs | No messages = potential stale data; explicit documentation |
| **Symmetric Window** | Most systems | Both sides get same window bounds |

**Gap Analysis:**
```
LaminarDB Status: ✅ Basic interval join with state bounded by window
Missing:
  ❌ Per-key state cleanup tracking
  ❌ Explicit handling for idle keys
  ❌ Asymmetric interval support
```

---

### 1.5 Lookup Join (External System Join)

**LaminarDB Current Design:**
```sql
CREATE TABLE customer_profiles (...)
WITH (
    connector = 'mongodb',
    lookup.cache = 'partial',
    lookup.cache.max_rows = 10000,
    lookup.cache.ttl = '5 minutes'
);
```

**Latest Research (2025-2026):**

| Innovation | Source | Description |
|------------|--------|-------------|
| **Lookup Shuffle Optimization** | Flink 2.0 | Shuffle input based on external system's preferred distribution |
| **Async Lookup** | Flink 2.0 | Non-blocking lookups with reordering |
| **Cache Strategies** | RisingWave | `none`, `partial` (LRU), `full` (load all at startup) |

**Flink 2.0 Lookup Join Enhancement:**
```
Problem: Input stream distribution arbitrary; cache hit rate poor
Solution: Pre-shuffle input based on external system requirements

Before: random distribution → poor cache hits
After: hash by lookup key → same keys co-located → better cache
```

**Gap Analysis:**
```
LaminarDB Status: ✅ Basic lookup join with caching
Missing:
  ❌ Input reshuffling for cache locality
  ❌ Async lookup option with ordering guarantees
  ❌ Full cache strategy for small dimensions
```

---

## 2. Incremental Join Computation (DBSP Model)

### 2.1 DBSP Join Incrementalization

**Core Insight from DBSP (VLDB 2023, extended 2025):**

For a bilinear operator like JOIN:
```
(a ⋈ b)^Δ = a ⋈ b + z^{-1}(I(a)) ⋈ b + a ⋈ z^{-1}(I(b))
```

Simplified:
```
ΔOutput = ΔLeft ⋈ Right_state + Left_state ⋈ ΔRight + ΔLeft ⋈ ΔRight
```

**Implementation Pattern:**
```rust
impl IncrementalJoin {
    fn process_delta_left(&mut self, delta_left: ZSet) -> ZSet {
        // Join delta with right state
        let result1 = self.join(&delta_left, &self.right_state);
        // Update left state
        self.left_state.merge(&delta_left);
        result1
    }
    
    fn process_delta_right(&mut self, delta_right: ZSet) -> ZSet {
        // Join left state with delta
        let result2 = self.join(&self.left_state, &delta_right);
        // Update right state
        self.right_state.merge(&delta_right);
        result2
    }
}
```

**Key Benefit:**
```
Non-incremental: O(|DB|) per transaction
Incremental:     O(|ΔDB|) per transaction
Speedup:         O(|DB|/|ΔDB|) = potentially 10^6x or more
```

**Gap Analysis:**
```
LaminarDB Status: ⚠️ Partially implemented
Missing:
  ❌ Formal Z-set representation with weights
  ❌ Automatic incrementalization of join trees
  ❌ Proper handling of retraction (negative weights)
```

---

### 2.2 Multi-Way Join Optimization

**Latest Research (Runtime-Optimized Multi-Way Stream Joins, Nov 2024):**

| Technique | Description | Benefit |
|-----------|-------------|---------|
| **Adaptive Probe Order** | Monitor filter selectivity; reorder at runtime | 2-3x throughput |
| **A-Greedy Algorithm** | Adaptive sorting of pipeline filters | Handles changing data |
| **Rate-Based Partitioning** | Partition based on arrival rates | Better load balance |

**Implementation Sketch:**
```rust
struct MultiWayJoinOptimizer {
    probe_orders: Vec<ProbeOrder>,
    selectivity_estimates: HashMap<JoinCondition, f64>,
    
    fn reoptimize(&mut self, stats: &RuntimeStats) {
        // Recalculate optimal probe order based on observed selectivity
        for join in &mut self.probe_orders {
            join.reorder_by_selectivity(&self.selectivity_estimates);
        }
    }
}
```

**Gap Analysis:**
```
LaminarDB Status: ❌ Static join order
Missing:
  ❌ Runtime selectivity monitoring
  ❌ Adaptive probe order adjustment
  ❌ Multi-way join plan representation
```

---

## 3. Disaggregated State Management (Flink 2.0)

### 3.1 Architecture Overview

**Flink 2.0 Key Innovations (VLDB 2025):**
```
┌─────────────────────────────────────────────────────┐
│                    Task Manager                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Join Op   │  │   Agg Op    │  │  Window Op  │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │
│         │                 │                 │        │
│  ┌──────▼─────────────────▼─────────────────▼──────┐│
│  │              Async Execution Controller          ││
│  │         (Out-of-Order Record Processing)         ││
│  └──────────────────────┬───────────────────────────┘│
│                         │                            │
│  ┌──────────────────────▼───────────────────────────┐│
│  │              ForSt State Backend                  ││
│  │       (Local Cache + Remote DFS Primary)          ││
│  └───────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   Distributed FS    │
              │   (S3, HDFS, etc)   │
              └─────────────────────┘
```

**Key Results:**
- 94% reduction in checkpoint duration
- 49x faster recovery
- 50% cost savings

### 3.2 Asynchronous State Access for Joins

**Flink 2.0 reimplemented 7 SQL operators with async state:**
1. ✅ Hash Join
2. ✅ Sort Merge Join  
3. ✅ Nested Loop Join
4. ✅ Aggregate
5. ✅ Window Aggregate
6. ✅ Deduplication
7. ✅ Rank

**Pattern for Async Join:**
```java
// Synchronous (old)
Row leftRow = leftState.get(key);  // BLOCKS
emit(join(currentRow, leftRow));

// Asynchronous (new)
leftState.asyncGet(key).thenAccept(leftRow -> {
    emit(join(currentRow, leftRow));
});
// Continue processing other records while waiting
```

**Gap Analysis for LaminarDB:**
```
LaminarDB Status: ❌ Synchronous state access
Missing:
  ❌ Async state API
  ❌ Out-of-order processing with reordering
  ❌ Disaggregated state backend option
```

---

## 4. State Management Optimizations

### 4.1 Join State Encoding (RisingWave July 2025)

**CPU-Friendly Encoding:**
```rust
enum JoinStateEncoding {
    Compact,      // Smaller memory footprint, higher CPU decode cost
    CpuFriendly,  // Larger memory, faster access (50% improvement)
}

-- SQL configuration
SET streaming_join_row_encoding = 'cpu_friendly';
```

**When to use which:**
| Encoding | Use Case |
|----------|----------|
| Compact | State >> Memory, disk spills frequent |
| CPU-Friendly | State fits in memory, CPU-bound workloads |

### 4.2 State Bounds Analysis (Feldera 2025)

**Automatic State Retention via Static Dataflow Analysis:**
```
For each join:
1. Analyze upstream operators for key cardinality
2. Compute retention bounds based on window/watermark
3. Schedule automatic GC without explicit TTL
```

**Gap Analysis:**
```
LaminarDB Status: ⚠️ Manual TTL configuration
Missing:
  ❌ Automatic state bounds inference
  ❌ Static dataflow analysis for GC scheduling
```

---

## 5. Recommended Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks)
1. **CPU-Friendly Encoding Option**
   - Add configurable row encoding
   - Session variable for per-query selection
   
2. **ASOF Join Enhancements**
   - Add Forward/Backward/Nearest modes
   - Implement BTreeMap-based state for range queries

### Phase 2: Incremental Improvements (2-4 weeks)
1. **Temporal Join Categories**
   - Separate append-only vs non-append-only code paths
   - Index-based lookup support
   
2. **Join State Bounds Analysis**
   - Implement static analysis pass
   - Automatic TTL inference

### Phase 3: Architecture Evolution (4-8 weeks)
1. **Async State Access**
   - Design async state API
   - Implement for hash join operator
   - Out-of-order processing with reordering
   
2. **Multi-way Join Optimization**
   - Runtime selectivity monitoring
   - Adaptive probe ordering

### Phase 4: Advanced Features (8+ weeks)
1. **Disaggregated State Option**
   - ForSt-style remote state backend
   - Tiered caching
   
2. **Full DBSP Incrementalization**
   - Z-set representation
   - Automatic incrementalization pass

---

## 6. Key Research References

### Papers
1. **DBSP: Automatic Incremental View Maintenance** (VLDB 2023, extended 2025)
   - Budiu et al., Feldera Inc.
   - Foundation for incremental join computation

2. **Disaggregated State Management in Apache Flink 2.0** (VLDB 2025)
   - Mei, Xia, Lan et al., Alibaba Cloud
   - Async state, ForSt backend, 94% checkpoint reduction

3. **Runtime-Optimized Multi-way Stream Joins** (arXiv Nov 2024)
   - Hu & Qiu, South China University
   - Adaptive probe ordering

4. **Efficiently Joining Large Relations on Multi-GPU Systems** (VLDB 2025)
   - Maltenberger et al.
   - GPU-accelerated joins (future consideration)

### Production Systems
- **RisingWave**: July 2025 newsletter - CPU-friendly join encoding
- **Feldera**: State bounds analysis, DBSP implementation
- **DuckDB**: Feb 2025 - Adaptive AsOf join planning
- **Apache Pinot**: Sep 2025 - ASOF JOIN with hash+binary search

---

## 7. Summary Matrix

| Feature | LaminarDB | Flink 2.0 | RisingWave | Feldera | Priority |
|---------|-----------|-----------|------------|---------|----------|
| Symmetric Hash Join | ✅ | ✅ Async | ✅ | ✅ Incremental | - |
| Temporal Join | ✅ Basic | ✅ | ✅ Append/Non-Append | ✅ | HIGH |
| ASOF Join | ✅ Basic | ❌ | ✅ Streaming | ❌ | HIGH |
| Interval Join | ✅ | ✅ | ✅ | ✅ | - |
| Lookup Join | ✅ Cache | ✅ Shuffle | ✅ Index | N/A | MEDIUM |
| CPU-Friendly Encoding | ❌ | ❌ | ✅ | N/A | HIGH |
| Async State Access | ❌ | ✅ | ❌ | ❌ | HIGH |
| Adaptive Probe Order | ❌ | ✅ AQE | ❌ | ❌ | MEDIUM |
| State Bounds Analysis | ❌ | ❌ | ❌ | ✅ | MEDIUM |
| DBSP Incrementalization | ⚠️ | ❌ | ❌ | ✅ | HIGH |

---

*Document generated: January 24, 2026*
*Next review: After Phase 1 implementation*
