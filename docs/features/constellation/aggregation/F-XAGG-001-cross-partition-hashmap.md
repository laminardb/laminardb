# F-XAGG-001: papaya Lock-Free Cross-Partition HashMap

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-XAGG-001 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F013 (Thread-Per-Core) |
| **Blocks** | F-XAGG-002 (Gossip Aggregates), F-XAGG-003 (gRPC Fan-Out) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/aggregation/cross_partition.rs` |

## Summary

Implements a lock-free cross-partition aggregate store using the `papaya` concurrent HashMap for single-node (Tier 2) global aggregations. In LaminarDB's Constellation Architecture, each partition is exclusively owned by a single core thread (Ring 0, no locks, no sharing). However, certain queries require aggregates that span multiple partitions on the same machine -- for example, a global COUNT or SUM across all partitions of a topic. This feature bridges that gap by allowing each partition to publish its partial aggregate into a shared `papaya::HashMap` (Ring 1, 1-10us latency), and any partition or query thread to read the merged global result. The design preserves Ring 0's zero-lock guarantee: partitions never contend with each other, and readers never block writers.

## Goals

- Provide a `CrossPartitionAggregateStore` backed by `papaya::HashMap` for lock-free concurrent access
- Support publishing partial aggregates from partition reactor threads (Ring 0 boundary to Ring 1)
- Support reading merged global aggregates from any thread (query threads, monitoring, other partitions)
- Enforce associative + commutative merge semantics (only aggregates that can be correctly merged from partial results)
- Serialize partial aggregates using `rkyv` for zero-copy reads where possible
- Achieve 1-10us cross-partition aggregate read latency
- Provide an eventually consistent read model within a single node (no distributed locks, no global barriers)
- Expose metrics for aggregate staleness, publish/read counts, and merge latency

## Non-Goals

- Cross-node aggregate merging (covered by F-XAGG-002 Gossip and F-XAGG-003 gRPC)
- Strongly consistent global aggregates (requires barrier synchronization, see F-XAGG-003)
- Aggregates requiring ordering or non-commutative operations (e.g., FIRST_VALUE, LAST_VALUE)
- Persistence of cross-partition aggregates (ephemeral, rebuilt on recovery from per-partition state)
- Window-level cross-partition aggregates (this feature covers global/latest aggregates; windowed cross-partition aggregates compose on top)

## Existing Code Integration

| Type | Location | Usage |
|------|----------|-------|
| `OutputVec` (`SmallVec<[Output; 4]>`) | `laminar_core::operator` | Operators return `SmallVec` not `Vec` — aggregation operators should match |
| `HotPathGuard` | `laminar_core::alloc` | Validates zero-alloc on Ring 0 with `allocation-tracking` feature |
| `bumpalo` | workspace dependency | Arena allocator available for temporary Ring 0 allocations |
| `CachePadded<T>` | `laminar_core::tpc` | `#[repr(C, align(64))]` for hot-path atomics — use for metrics counters |

## Technical Design

### Architecture

**Ring**: Ring 1 (Background) -- cross-partition reads are not on the hot path. Partition-local aggregate computation remains in Ring 0; only the publish step crosses the Ring 0/Ring 1 boundary.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/aggregation/cross_partition.rs`

Each partition reactor computes its local partial aggregate as part of normal Ring 0 processing (e.g., incrementing a counter, updating a sum). Periodically -- either on every Nth event, on watermark advance, or on checkpoint barrier -- the partition publishes its partial aggregate to the shared `CrossPartitionAggregateStore`. Query threads or monitoring reads merge all partials on-demand.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Single Node (Tier 2)                                                     │
│                                                                           │
│  ┌──────────────┐  publish_partial()  ┌──────────────────────────────┐   │
│  │ Partition 0   │ ──────────────────> │                              │   │
│  │ (Ring 0)      │                     │  CrossPartitionAggregateStore │   │
│  └──────────────┘                     │  (papaya::HashMap)           │   │
│                                        │                              │   │
│  ┌──────────────┐  publish_partial()  │  Key: AggregateKey           │   │
│  │ Partition 1   │ ──────────────────> │  Val: PartialAggregate       │   │
│  │ (Ring 0)      │                     │                              │   │
│  └──────────────┘                     │  Lock-free reads + writes    │   │
│                                        │  Eventually consistent       │   │
│  ┌──────────────┐  publish_partial()  │                              │   │
│  │ Partition 2   │ ──────────────────> │                              │   │
│  │ (Ring 0)      │                     └───────────┬──────────────────┘   │
│  └──────────────┘                                  │                      │
│                                                     │ read_merged()       │
│                                        ┌────────────▼─────────────┐      │
│                                        │  Query Thread / Monitor  │      │
│                                        │  (Ring 1)                │      │
│                                        └──────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use papaya::HashMap as PapayaHashMap;
use std::sync::Arc;
use std::time::Instant;

/// Unique key identifying a specific aggregate across partitions.
///
/// Combines the pipeline ID, aggregate function, and optional window
/// identifier to produce a unique namespace for the aggregate.
///
/// # Allocation Note
///
/// `aggregate_name` uses `Arc<str>` instead of `String` to avoid
/// heap allocation when cloning the key on every publish. Keys are
/// constructed once at registration time and cloned on each publish;
/// `Arc<str>` clone is an atomic increment (~2ns) vs `String` clone
/// which allocates (~50-100ns).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateKey {
    /// Pipeline that produces this aggregate.
    pub pipeline_id: PipelineId,
    /// Name of the aggregate function (e.g., "count", "sum_amount").
    /// Uses `Arc<str>` for cheap cloning on the publish hot path.
    pub aggregate_name: Arc<str>,
    /// Optional window identifier for windowed aggregates.
    /// `None` for global (non-windowed) aggregates.
    pub window_id: Option<WindowId>,
}

impl AggregateKey {
    /// Create a new global (non-windowed) aggregate key.
    pub fn global(pipeline_id: PipelineId, name: impl Into<Arc<str>>) -> Self {
        Self {
            pipeline_id,
            aggregate_name: name.into(),
            window_id: None,
        }
    }

    /// Create a new windowed aggregate key.
    pub fn windowed(
        pipeline_id: PipelineId,
        name: impl Into<Arc<str>>,
        window_id: WindowId,
    ) -> Self {
        Self {
            pipeline_id,
            aggregate_name: name.into(),
            window_id: Some(window_id),
        }
    }
}

/// A partial aggregate value published by a single partition.
///
/// Partial aggregates must be associative and commutative so they
/// can be merged in any order without coordination.
///
/// Serialized via `rkyv` for efficient cross-thread sharing.
#[derive(Debug, Clone)]
pub struct PartialAggregate {
    /// The partition that produced this partial.
    pub partition_id: PartitionId,
    /// Serialized partial aggregate state (rkyv-encoded).
    /// Format depends on the aggregate function.
    pub state: AggregateState,
    /// Epoch at which this partial was computed.
    pub epoch: u64,
    /// Watermark timestamp of the partition when this partial was published.
    pub watermark_ms: i64,
    /// Wall-clock time when this partial was published.
    pub published_at: Instant,
}

/// The aggregate state payload. Each variant carries the partial
/// state for its aggregate function type.
#[derive(Debug, Clone)]
pub enum AggregateState {
    /// COUNT: number of events counted in this partition.
    Count(i64),
    /// SUM: running sum for this partition.
    Sum(f64),
    /// MIN: minimum value seen in this partition.
    Min(f64),
    /// MAX: maximum value seen in this partition.
    Max(f64),
    /// AVG: sum + count pair for merging into a global average.
    Avg { sum: f64, count: i64 },
    /// Custom: opaque rkyv-serialized bytes for user-defined aggregates.
    /// The merge function must be registered separately.
    Custom(Vec<u8>),
}

/// Trait for merge functions that combine partial aggregates.
///
/// Implementations MUST be associative and commutative:
///   merge(a, merge(b, c)) == merge(merge(a, b), c)   (associative)
///   merge(a, b) == merge(b, a)                        (commutative)
///
/// Violating these properties produces incorrect global results.
pub trait AggregateMerge: Send + Sync {
    /// Merge two partial aggregate states into a combined result.
    fn merge(&self, left: &AggregateState, right: &AggregateState) -> AggregateState;

    /// Extract a final scalar result from a merged aggregate state.
    fn finalize(&self, state: &AggregateState) -> ScalarValue;
}

/// Thread-safe handle for a single partition to publish its partial aggregates.
///
/// Obtained from `CrossPartitionAggregateStore::partition_handle()`.
/// Each partition holds one of these and calls `publish()` without locking.
pub struct PartitionAggregateHandle {
    store: Arc<CrossPartitionAggregateStore>,
    partition_id: PartitionId,
}

impl PartitionAggregateHandle {
    /// Publish a partial aggregate from this partition.
    ///
    /// This method is safe to call from the partition's Ring 0 reactor.
    /// It performs a single lock-free `insert` into the papaya HashMap.
    /// The key is scoped to (AggregateKey, PartitionId) so each partition's
    /// partial is stored independently.
    ///
    /// # Performance
    ///
    /// Target: < 500ns for the publish operation (lock-free insert).
    pub fn publish(
        &self,
        key: &AggregateKey,
        state: AggregateState,
        epoch: u64,
        watermark_ms: i64,
    ) {
        let partial = PartialAggregate {
            partition_id: self.partition_id,
            state,
            epoch,
            watermark_ms,
            published_at: Instant::now(),
        };
        let composite_key = CompositeKey {
            aggregate_key: key.clone(),
            partition_id: self.partition_id,
        };
        let guard = self.store.partials.guard();
        self.store.partials.insert(composite_key, partial, &guard);
    }
}
```

### Data Structures

```rust
use papaya::HashMap as PapayaHashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Composite key combining aggregate identity and partition identity.
/// Used as the HashMap key to store per-partition partials independently.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompositeKey {
    aggregate_key: AggregateKey,
    partition_id: PartitionId,
}

/// Central store for cross-partition aggregates within a single node.
///
/// Uses `papaya::HashMap` for lock-free concurrent access. Each partition
/// publishes its partial aggregate independently; readers merge on demand.
///
/// The store is shared via `Arc` across all partition threads and query threads.
pub struct CrossPartitionAggregateStore {
    /// Lock-free map from (aggregate_key, partition_id) -> PartialAggregate.
    partials: PapayaHashMap<CompositeKey, PartialAggregate>,

    /// Registry of merge functions by aggregate name.
    merge_functions: PapayaHashMap<String, Arc<dyn AggregateMerge>>,

    /// Set of known partition IDs for completeness checking.
    known_partitions: PapayaHashMap<PartitionId, ()>,

    /// Configuration for staleness bounds and publish frequency.
    config: AggregateStoreConfig,

    /// Metrics counters.
    metrics: AggregateStoreMetrics,
}

/// Configuration for the cross-partition aggregate store.
#[derive(Debug, Clone)]
pub struct AggregateStoreConfig {
    /// Maximum staleness allowed before a read triggers a warning.
    pub max_staleness: Duration,
    /// Expected number of partitions (for pre-allocation).
    pub expected_partitions: usize,
    /// Expected number of distinct aggregates (for pre-allocation).
    pub expected_aggregates: usize,
}

impl Default for AggregateStoreConfig {
    fn default() -> Self {
        Self {
            max_staleness: Duration::from_secs(5),
            expected_partitions: 16,
            expected_aggregates: 64,
        }
    }
}

/// Runtime metrics for the aggregate store.
#[derive(Debug, Default)]
pub struct AggregateStoreMetrics {
    /// Total number of partial publishes.
    pub publishes: std::sync::atomic::AtomicU64,
    /// Total number of merged reads.
    pub reads: std::sync::atomic::AtomicU64,
    /// Number of reads that returned stale data.
    pub stale_reads: std::sync::atomic::AtomicU64,
    /// Number of reads where not all partitions had reported.
    pub incomplete_reads: std::sync::atomic::AtomicU64,
}

/// Result of reading a merged cross-partition aggregate.
#[derive(Debug, Clone)]
pub struct MergedAggregateResult {
    /// The final merged scalar value.
    pub value: ScalarValue,
    /// Number of partitions that contributed partials.
    pub partitions_reporting: usize,
    /// Total number of known partitions.
    pub partitions_total: usize,
    /// Whether all partitions have reported (completeness).
    pub is_complete: bool,
    /// Maximum staleness across all contributing partials.
    pub max_staleness: Duration,
    /// Minimum watermark across all contributing partials.
    pub min_watermark_ms: i64,
}

impl CrossPartitionAggregateStore {
    /// Create a new cross-partition aggregate store.
    pub fn new(config: AggregateStoreConfig) -> Arc<Self> {
        Arc::new(Self {
            partials: PapayaHashMap::new(),
            merge_functions: PapayaHashMap::new(),
            known_partitions: PapayaHashMap::new(),
            config,
            metrics: AggregateStoreMetrics::default(),
        })
    }

    /// Register a partition as a known contributor.
    pub fn register_partition(&self, partition_id: PartitionId) {
        let guard = self.known_partitions.guard();
        self.known_partitions.insert(partition_id, (), &guard);
    }

    /// Register a merge function for an aggregate name.
    pub fn register_merge(
        &self,
        aggregate_name: impl Into<String>,
        merge_fn: Arc<dyn AggregateMerge>,
    ) {
        let guard = self.merge_functions.guard();
        self.merge_functions.insert(aggregate_name.into(), merge_fn, &guard);
    }

    /// Get a partition-scoped handle for publishing aggregates.
    pub fn partition_handle(
        self: &Arc<Self>,
        partition_id: PartitionId,
    ) -> PartitionAggregateHandle {
        self.register_partition(partition_id);
        PartitionAggregateHandle {
            store: Arc::clone(self),
            partition_id,
        }
    }

    /// Read the merged global aggregate for the given key.
    ///
    /// Collects all partials for the key, merges them using the registered
    /// merge function, and returns the finalized result with completeness
    /// and staleness metadata.
    ///
    /// # Performance
    ///
    /// Target: 1-10us depending on partition count and aggregate complexity.
    pub fn read_merged(&self, key: &AggregateKey) -> Result<MergedAggregateResult, AggregateError> {
        let merge_fn = {
            let guard = self.merge_functions.guard();
            self.merge_functions
                .get(&key.aggregate_name, &guard)
                .cloned()
                .ok_or_else(|| AggregateError::MergeFunctionNotFound(
                    key.aggregate_name.clone(),
                ))?
        };

        let now = Instant::now();
        let mut merged_state: Option<AggregateState> = None;
        let mut partitions_reporting = 0usize;
        let mut max_staleness = Duration::ZERO;
        let mut min_watermark = i64::MAX;

        // Iterate known partitions, collect their partials
        let partitions_guard = self.known_partitions.guard();
        let partials_guard = self.partials.guard();

        let known_count = self.known_partitions.len();

        for (partition_id, _) in self.known_partitions.pin().iter() {
            let composite = CompositeKey {
                aggregate_key: key.clone(),
                partition_id: *partition_id,
            };
            if let Some(partial) = self.partials.get(&composite, &partials_guard) {
                let staleness = now.duration_since(partial.published_at);
                max_staleness = max_staleness.max(staleness);
                min_watermark = min_watermark.min(partial.watermark_ms);

                merged_state = Some(match merged_state {
                    None => partial.state.clone(),
                    Some(existing) => merge_fn.merge(&existing, &partial.state),
                });
                partitions_reporting += 1;
            }
        }

        let state = merged_state.ok_or(AggregateError::NoPartials)?;
        let value = merge_fn.finalize(&state);

        self.metrics
            .reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if max_staleness > self.config.max_staleness {
            self.metrics
                .stale_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if partitions_reporting < known_count {
            self.metrics
                .incomplete_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(MergedAggregateResult {
            value,
            partitions_reporting,
            partitions_total: known_count,
            is_complete: partitions_reporting == known_count,
            max_staleness,
            min_watermark_ms: if min_watermark == i64::MAX { 0 } else { min_watermark },
        })
    }

    /// Remove all partials for a given aggregate key (e.g., when window closes).
    pub fn evict(&self, key: &AggregateKey) {
        let partitions_guard = self.known_partitions.guard();
        let partials_guard = self.partials.guard();
        for (partition_id, _) in self.known_partitions.pin().iter() {
            let composite = CompositeKey {
                aggregate_key: key.clone(),
                partition_id: *partition_id,
            };
            self.partials.remove(&composite, &partials_guard);
        }
    }
}
```

### Algorithm/Flow

#### Publish Flow (Partition Ring 0 -> Ring 1 Boundary)

```
1. Partition processes events in Ring 0, updating its local aggregate accumulator
2. On trigger (every N events, watermark advance, or checkpoint barrier):
   a. Partition calls handle.publish(key, state, epoch, watermark)
   b. publish() creates PartialAggregate with current Instant::now()
   c. publish() calls papaya::HashMap::insert(composite_key, partial)
      - Lock-free insert: uses compare-and-swap internally
      - Overwrites previous partial for same (key, partition) pair
   d. Increments publish counter metric (atomic relaxed)
3. Total publish overhead: < 500ns (single CAS operation)
```

#### Read/Merge Flow (Query Thread, Ring 1)

```
1. Query thread calls store.read_merged(key)
2. Store looks up registered merge function for aggregate_name
3. Store iterates all known partitions:
   a. For each partition, looks up composite_key in papaya HashMap
   b. If partial exists, merge into running accumulator
   c. Track staleness = now - partial.published_at
   d. Track min watermark across all partials
4. Finalize merged state into ScalarValue
5. Return MergedAggregateResult with completeness metadata
6. Total read latency: 1-10us depending on partition count
```

#### Consistency Model

```
Read Consistency: Eventually Consistent (within single node)

- No global lock, no barrier coordination for reads
- Each partition publishes independently at its own pace
- Readers may see a mix of old and new partials
- Staleness bounded by publish frequency (configurable)
- For strong consistency, use F-XAGG-003 (gRPC Fan-Out with barrier)

Guarantees:
- Monotonic writes per partition (each publish overwrites the previous)
- No torn reads (papaya provides atomic insert/read)
- Merge is correct IF all partials are present and merge function is associative+commutative
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `AggregateError::MergeFunctionNotFound` | No merge function registered for aggregate name | Register merge function before querying; return error to caller |
| `AggregateError::NoPartials` | No partitions have published partials for this key yet | Return error; caller may retry or return NULL |
| `AggregateError::IncompletePartials` | Not all partitions have reported (soft warning, not an error) | Return result with `is_complete: false`; caller decides whether to use partial result |
| `AggregateError::StaleData` | Max staleness exceeds configured threshold | Return result with staleness metadata; emit metric; caller decides |
| `AggregateError::InvalidMerge` | Merge function returned incompatible state variant | Log error, skip partition partial, return degraded result |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Partial publish latency | < 500ns | `bench_publish_partial` |
| Merged read latency (8 partitions) | < 5us | `bench_read_merged_8_partitions` |
| Merged read latency (64 partitions) | < 10us | `bench_read_merged_64_partitions` |
| papaya HashMap insert throughput | > 10M ops/sec | `bench_concurrent_publish_throughput` |
| papaya HashMap read throughput | > 20M ops/sec | `bench_concurrent_read_throughput` |
| Memory per partial aggregate | < 256 bytes | Static analysis |
| Zero locks on any path | 0 mutex acquisitions | Code review + lock detection test |

## Test Plan

### Unit Tests

- [ ] `test_aggregate_key_global_creates_none_window` - AggregateKey::global sets window_id to None
- [ ] `test_aggregate_key_windowed_sets_window_id` - AggregateKey::windowed stores window ID
- [ ] `test_partial_aggregate_state_count` - Count variant stores i64
- [ ] `test_partial_aggregate_state_avg` - Avg variant stores sum + count pair
- [ ] `test_merge_count_associative` - merge(a, merge(b, c)) == merge(merge(a, b), c)
- [ ] `test_merge_count_commutative` - merge(a, b) == merge(b, a)
- [ ] `test_merge_sum_associative` - Sum merge is associative
- [ ] `test_merge_avg_produces_correct_global_average` - (sum1+sum2)/(count1+count2)
- [ ] `test_merge_min_selects_smallest` - Min of all partials
- [ ] `test_merge_max_selects_largest` - Max of all partials
- [ ] `test_store_publish_and_read_single_partition` - Single partition roundtrip
- [ ] `test_store_publish_overwrites_previous_partial` - Latest-wins semantics
- [ ] `test_store_read_merged_all_partitions_complete` - is_complete = true
- [ ] `test_store_read_merged_incomplete_partitions` - is_complete = false
- [ ] `test_store_read_merged_no_partials_returns_error` - NoPartials error
- [ ] `test_store_read_merged_unregistered_merge_returns_error` - MergeFunctionNotFound
- [ ] `test_store_evict_removes_all_partials_for_key` - Cleanup after window close
- [ ] `test_store_staleness_tracking` - max_staleness reflects publish timing
- [ ] `test_store_metrics_increment` - Publishes, reads, stale/incomplete counters

### Integration Tests

- [ ] `test_cross_partition_count_across_8_threads` - 8 partition threads publish counts, query thread reads correct total
- [ ] `test_cross_partition_sum_under_concurrent_load` - Sustained publishes + reads with no data races
- [ ] `test_cross_partition_avg_eventual_convergence` - All partials converge to correct global average
- [ ] `test_cross_partition_with_checkpoint_barrier` - Partials published on barrier, merged after all barriers processed
- [ ] `test_cross_partition_partition_restart_recovery` - Partition dies, restarts, publishes new partial; old partial overwritten
- [ ] `test_cross_partition_multiple_aggregates` - Multiple aggregate keys coexist in same store

### Benchmarks

- [ ] `bench_publish_partial` - Target: < 500ns per publish
- [ ] `bench_read_merged_8_partitions` - Target: < 5us
- [ ] `bench_read_merged_64_partitions` - Target: < 10us
- [ ] `bench_concurrent_publish_throughput` - Target: > 10M ops/sec with 16 threads
- [ ] `bench_concurrent_read_throughput` - Target: > 20M ops/sec with 16 threads
- [ ] `bench_publish_memory_overhead` - Measure per-entry memory consumption

## Rollout Plan

1. **Phase 1**: Define `AggregateKey`, `PartialAggregate`, `AggregateState` types
2. **Phase 2**: Implement `CrossPartitionAggregateStore` with papaya HashMap
3. **Phase 3**: Implement built-in merge functions (Count, Sum, Min, Max, Avg)
4. **Phase 4**: Implement `PartitionAggregateHandle` for partition-scoped publishing
5. **Phase 5**: Unit tests for all merge semantics and store operations
6. **Phase 6**: Integration tests with multi-threaded partition simulators
7. **Phase 7**: Benchmarks + optimization (ensure < 10us target)
8. **Phase 8**: Metrics integration with laminar-observe
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should partials include a sequence number so readers can detect if they missed an update? Currently relying on wall-clock staleness detection.
- [ ] Should the store pre-allocate slots for all (key, partition) combinations at registration time, or grow dynamically? Pre-allocation avoids papaya resize overhead under load.
- [ ] Should there be a compaction mechanism that periodically garbage-collects evicted aggregates, or is explicit `evict()` sufficient?
- [ ] Should the merge be performed lazily on read (current design) or eagerly on publish? Eager merge reduces read latency but increases publish cost and requires a global merge lock.
- [ ] Should `AggregateState::Custom` support a typed registry with downcasting, or is opaque bytes with a registered merge function sufficient?

## Completion Checklist

- [ ] `AggregateKey` and `PartialAggregate` types implemented
- [ ] `AggregateState` enum with built-in variants
- [ ] `AggregateMerge` trait defined with associativity/commutativity contract
- [ ] `CrossPartitionAggregateStore` implemented with papaya HashMap
- [ ] `PartitionAggregateHandle` for lock-free partition publishing
- [ ] Built-in merge functions for Count, Sum, Min, Max, Avg
- [ ] `MergedAggregateResult` with completeness and staleness metadata
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with concurrent partition threads
- [ ] Benchmarks meet targets (< 10us read, < 500ns publish)
- [ ] Metrics wired to laminar-observe
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [papaya crate](https://docs.rs/papaya) -- Lock-free concurrent HashMap
- [F013: Thread-Per-Core](../../phase-2/F013-thread-per-core.md) -- Partition-exclusive ownership model
- [F014: SPSC Queues](../../phase-2/F014-spsc-queues.md) -- Inter-partition communication
- [F-XAGG-002: Gossip Aggregates](./F-XAGG-002-gossip-aggregates.md) -- Cross-node eventual consistency
- [F-XAGG-003: gRPC Fan-Out](./F-XAGG-003-grpc-aggregate-fanout.md) -- Cross-node strong consistency
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model and tier design
