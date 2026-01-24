# F057: Stream Join Optimizations

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F057 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 2 |
| **Effort** | M (5-7 days) |
| **Dependencies** | F019 |
| **Owner** | TBD |
| **Research** | [Stream Joins Research 2026](../../research/laminardb-stream-joins-research-review-2026.md) |

## Summary

Enhance the existing stream-stream join operator (F019) with optimizations from 2025-2026 research, including CPU-friendly row encoding, asymmetric compaction, and per-key state tracking.

## Current State (F019)

The existing `StreamJoinOperator` provides:
- ✅ Symmetric hash join with state for both sides
- ✅ Inner, Left, Right, Full outer joins
- ✅ Time-bounded matching
- ✅ Watermark-based state cleanup via timers
- ✅ FxHash key hashing
- ✅ Arrow IPC serialization

## Research Gaps Identified

| Gap | Source | Impact | Priority |
|-----|--------|--------|----------|
| CPU-friendly row encoding | RisingWave July 2025 | 50% perf improvement when state fits in memory | HIGH |
| Asymmetric optimization | Epsio 2025 | Skip compaction on finished side | MEDIUM |
| Per-key state cleanup | RisingWave 2025 | Better cleanup for sparse keys | MEDIUM |
| Build-side pruning | Synnada/DataFusion | Memory efficiency | LOW |

## Goals

1. Add configurable row encoding (compact vs CPU-friendly)
2. Implement asymmetric compaction strategy
3. Add per-key cleanup tracking
4. Improve build-side memory efficiency

## Technical Design

### 1. CPU-Friendly Row Encoding

**Problem**: Current Arrow IPC encoding prioritizes compactness over CPU access speed.

**Solution**: Add configurable encoding based on workload characteristics.

```rust
/// Row encoding strategy for join state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinRowEncoding {
    /// Compact encoding - smaller memory, higher CPU decode cost.
    /// Best when: state >> memory, disk spills frequent.
    #[default]
    Compact,

    /// CPU-friendly encoding - larger memory, faster access.
    /// Best when: state fits in memory, CPU-bound workloads.
    /// ~50% performance improvement in RisingWave benchmarks.
    CpuFriendly,
}

/// Configuration builder extension.
impl StreamJoinConfigBuilder {
    pub fn row_encoding(mut self, encoding: JoinRowEncoding) -> Self {
        self.row_encoding = Some(encoding);
        self
    }
}

/// SQL session variable
/// SET streaming_join_row_encoding = 'cpu_friendly';
```

**Implementation**:

```rust
impl JoinRow {
    fn serialize(&self, encoding: JoinRowEncoding) -> Result<Vec<u8>, OperatorError> {
        match encoding {
            JoinRowEncoding::Compact => {
                // Current Arrow IPC encoding
                Self::serialize_arrow_ipc(&self.batch)
            }
            JoinRowEncoding::CpuFriendly => {
                // Inline primitive values, avoid indirection
                // Store fixed-size columns as raw bytes
                // Use aligned memory for SIMD access
                Self::serialize_cpu_friendly(&self.batch)
            }
        }
    }

    fn serialize_cpu_friendly(batch: &RecordBatch) -> Result<Vec<u8>, OperatorError> {
        let mut buf = Vec::new();

        // Header: schema fingerprint (8 bytes) + num_rows (4 bytes)
        buf.extend_from_slice(&batch.schema().hash().to_le_bytes());
        buf.extend_from_slice(&(batch.num_rows() as u32).to_le_bytes());

        // For each column: inline the values
        for column in batch.columns() {
            match column.data_type() {
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    // Validity bitmap (1 bit per row, padded to 8)
                    let validity_bytes = (batch.num_rows() + 7) / 8;
                    if let Some(nulls) = arr.nulls() {
                        buf.extend_from_slice(nulls.buffer().as_slice());
                    } else {
                        buf.extend_from_slice(&vec![0xFF; validity_bytes]);
                    }
                    // Raw i64 values (8 bytes each, aligned)
                    buf.extend_from_slice(arr.values().as_slice());
                }
                // ... other types
            }
        }

        Ok(buf)
    }
}
```

### 2. Asymmetric Compaction Strategy

**Problem**: Both sides are treated equally for compaction, but often one side has much lower write rate.

**Solution**: Track write rates and skip compaction on "finished" or low-activity sides.

```rust
/// Per-side statistics for asymmetric optimization.
struct SideStats {
    /// Total events received.
    events_received: u64,
    /// Events in current compaction window.
    events_this_window: u64,
    /// Last event timestamp.
    last_event_time: i64,
    /// Estimated write rate (events/second).
    write_rate: f64,
}

impl StreamJoinOperator {
    /// Check if a side is "finished" (no recent activity).
    fn is_side_finished(&self, side: JoinSide) -> bool {
        let stats = match side {
            JoinSide::Left => &self.left_stats,
            JoinSide::Right => &self.right_stats,
        };

        // Consider finished if:
        // 1. No events in last compaction window
        // 2. Write rate below threshold
        let time_since_last = self.current_watermark - stats.last_event_time;
        stats.events_this_window == 0 &&
            time_since_last > self.config.idle_threshold_ms
    }

    /// Compaction strategy: skip finished side.
    fn should_compact(&self, side: JoinSide) -> bool {
        if self.is_side_finished(side) {
            // Skip compaction - this side is finished
            // Only clean up entries that are definitively expired
            false
        } else {
            // Normal compaction schedule
            true
        }
    }
}
```

### 3. Per-Key State Cleanup Tracking

**Problem**: Current cleanup uses global watermark, but sparse keys may have stale entries.

**Solution**: Track per-key last activity and cleanup idle keys more aggressively.

```rust
/// Per-key metadata for cleanup tracking.
struct KeyMetadata {
    /// Last event timestamp for this key.
    last_activity: i64,
    /// Number of events for this key.
    event_count: u64,
    /// Cleanup timer registered.
    cleanup_timer: Option<i64>,
}

impl StreamJoinOperator {
    /// Enhanced cleanup that considers per-key activity.
    fn cleanup_key_state(&mut self, key_hash: u64, ctx: &mut OperatorContext) {
        let metadata = self.key_metadata.get(&key_hash);

        if let Some(meta) = metadata {
            let idle_time = ctx.processing_time - meta.last_activity;

            if idle_time > self.config.key_idle_threshold_ms {
                // Key is idle - more aggressive cleanup
                self.remove_all_state_for_key(key_hash, ctx);
                self.key_metadata.remove(&key_hash);
                self.metrics.idle_key_cleanups += 1;
            }
        }
    }

    /// Called periodically to scan for idle keys.
    fn scan_idle_keys(&mut self, ctx: &mut OperatorContext) {
        let threshold = ctx.processing_time - self.config.key_idle_threshold_ms;

        let idle_keys: Vec<_> = self.key_metadata
            .iter()
            .filter(|(_, meta)| meta.last_activity < threshold)
            .map(|(k, _)| *k)
            .collect();

        for key_hash in idle_keys {
            self.cleanup_key_state(key_hash, ctx);
        }
    }
}
```

### 4. Build-Side Pruning (Memory Efficiency)

**Problem**: Build side state can grow unbounded for long-running joins.

**Solution**: Implement early pruning based on watermark and probe-side progress.

```rust
impl StreamJoinOperator {
    /// Prune build-side entries that cannot produce future matches.
    ///
    /// An entry can be pruned if:
    /// 1. Its timestamp + time_bound < current watermark (standard cleanup)
    /// 2. For inner join: probe side watermark has passed beyond match window
    fn prune_build_side(&mut self, ctx: &mut OperatorContext) {
        let probe_watermark = match self.config.build_side {
            JoinSide::Left => self.right_watermark,
            JoinSide::Right => self.left_watermark,
        };

        let prune_threshold = probe_watermark - self.config.time_bound_ms;

        // For inner joins, we can be more aggressive
        if self.config.join_type == JoinType::Inner {
            self.prune_entries_before(self.config.build_side, prune_threshold, ctx);
        }
    }
}
```

## Configuration

```rust
/// Extended configuration for stream-stream joins.
#[derive(Debug, Clone)]
pub struct StreamJoinConfig {
    // Existing fields...

    /// Row encoding strategy (default: Compact).
    pub row_encoding: JoinRowEncoding,

    /// Enable asymmetric compaction optimization.
    pub asymmetric_compaction: bool,

    /// Threshold for considering a side "finished" (ms).
    pub idle_threshold_ms: i64,

    /// Enable per-key cleanup tracking.
    pub per_key_tracking: bool,

    /// Threshold for idle key cleanup (ms).
    pub key_idle_threshold_ms: i64,

    /// Enable build-side pruning.
    pub build_side_pruning: bool,

    /// Which side to use as build side (None = auto-select).
    pub build_side: Option<JoinSide>,
}

impl Default for StreamJoinConfig {
    fn default() -> Self {
        Self {
            row_encoding: JoinRowEncoding::Compact,
            asymmetric_compaction: true,
            idle_threshold_ms: 60_000, // 1 minute
            per_key_tracking: true,
            key_idle_threshold_ms: 300_000, // 5 minutes
            build_side_pruning: true,
            build_side: None,
        }
    }
}
```

## SQL Configuration

```sql
-- Set row encoding for session
SET streaming_join_row_encoding = 'cpu_friendly';

-- Enable asymmetric compaction
SET streaming_join_asymmetric_compaction = true;

-- Configure idle thresholds
SET streaming_join_idle_threshold = '1 minute';
SET streaming_join_key_idle_threshold = '5 minutes';
```

## Implementation Phases

### Phase 1: CPU-Friendly Encoding (2-3 days)

1. Implement `JoinRowEncoding` enum
2. Add CPU-friendly serialization path
3. Benchmark encoding performance
4. Add config option and SQL variable

### Phase 2: Asymmetric Compaction (1-2 days)

1. Add per-side statistics tracking
2. Implement idle detection
3. Skip compaction for finished sides
4. Add metrics for optimization effectiveness

### Phase 3: Per-Key Tracking (2 days)

1. Add `KeyMetadata` tracking
2. Implement idle key scan
3. Aggressive cleanup for idle keys
4. Memory overhead monitoring

### Phase 4: Build-Side Pruning (1 day)

1. Track per-side watermarks
2. Implement early pruning logic
3. Verify correctness for all join types

## Metrics

```rust
/// Extended metrics for join optimizations.
pub struct StreamJoinMetrics {
    // Existing fields...

    /// Rows encoded with CPU-friendly format.
    pub cpu_friendly_encodes: u64,
    /// Rows encoded with compact format.
    pub compact_encodes: u64,

    /// Compactions skipped due to asymmetric optimization.
    pub asymmetric_skips: u64,

    /// Idle keys cleaned up.
    pub idle_key_cleanups: u64,

    /// Build-side entries pruned early.
    pub build_side_prunes: u64,
}
```

## Test Cases

```rust
#[test]
fn test_cpu_friendly_encoding_roundtrip() {
    // Verify data integrity with CPU-friendly encoding
}

#[test]
fn test_asymmetric_finished_side() {
    // Verify compaction is skipped for finished side
}

#[test]
fn test_idle_key_cleanup() {
    // Verify idle keys are cleaned up aggressively
}

#[test]
fn test_build_side_pruning() {
    // Verify early pruning doesn't affect correctness
}

#[test]
fn test_encoding_performance_comparison() {
    // Benchmark compact vs CPU-friendly
}
```

## Acceptance Criteria

- [ ] CPU-friendly encoding implemented with 30%+ speedup for memory-resident state
- [ ] Asymmetric compaction reduces overhead for uneven streams
- [ ] Per-key tracking reduces memory for sparse key patterns
- [ ] Build-side pruning reduces memory for long-running joins
- [ ] All optimizations configurable via SQL
- [ ] Existing tests still passing
- [ ] 10+ new tests for optimization scenarios
- [ ] Metrics exposed for observability

## Backward Compatibility

All optimizations are:
- **Off by default** for compact encoding (current behavior)
- **On by default** for asymmetric and per-key tracking (safe optimizations)
- **Configurable** via SQL or config

No breaking changes to existing API.

## Performance Targets

| Optimization | Expected Improvement | Measurement |
|--------------|---------------------|-------------|
| CPU-friendly encoding | 30-50% throughput | Memory-resident workload |
| Asymmetric compaction | 20-40% CPU reduction | Uneven stream rates |
| Per-key tracking | 20-50% memory reduction | Sparse key patterns |
| Build-side pruning | 10-30% memory reduction | Long-running joins |

## References

- RisingWave July 2025: CPU-friendly join encoding
- Epsio Blog 2025: Asymmetric optimization patterns
- Synnada/DataFusion: Build-side pruning strategies
