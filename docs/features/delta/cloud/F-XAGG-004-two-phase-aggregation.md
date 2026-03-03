# F-XAGG-004: Two-Phase Cross-Partition Aggregation

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-XAGG-004 |
| **Status** | Done |
| **Priority** | P1 |
| **Phase** | 7c |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DISAGG-001 |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-core` |
| **Module** | `crates/laminar-core/src/aggregation/two_phase.rs` (new) |

## Summary

Two-phase aggregation for correct results across partitions without requiring state shuffle. Each partition produces partial aggregates (Ring 0). A merge step on Ring 2 combines partials into final results. Supports COUNT, SUM, AVG (sum+count), MIN, MAX, APPROX_DISTINCT (HLL merge).

## Goals

- `PartialAggregate`: serializable intermediate accumulator state per partition
- `MergeAggregator`: combines partials into final result on Ring 2
- Wire into existing `CrossPartitionAggregateStore` and gRPC fanout
- `can_use_two_phase()` detection for streaming optimizer
- Arrow IPC serialization for shipping partials between nodes

## Non-Goals

- DISTINCT aggregates (require full shuffle)
- User-defined aggregate merge functions (future extension)

## Technical Design

### Architecture

```
Partition 0: COUNT(*) GROUP BY symbol -> partial(AAPL: 500)
Partition 1: COUNT(*) GROUP BY symbol -> partial(AAPL: 300)  -> Ring 2 merge -> final(AAPL: 800)
Partition 2: COUNT(*) GROUP BY symbol -> partial(AAPL: 200)
```

### Core Types

- **`TwoPhaseKind`** — enum: Count, Sum, Avg, Min, Max, ApproxDistinct
- **`PartialState`** — intermediate state per function (serializable via serde)
- **`PartialAggregate`** — one group's partial from one partition (group_key + states + metadata)
- **`MergeAggregator`** — groups partials by key, merges each group, produces finals
- **`HllSketch`** — minimal HyperLogLog sketch (precision 4-18, default 8 = 256 registers)

### Supported Functions

| Function | Partial State | Merge Logic |
|----------|--------------|-------------|
| COUNT | `count: i64` | sum of counts |
| SUM | `sum: f64` | sum of sums |
| AVG | `sum: f64, count: i64` | `total_sum / total_count` |
| MIN | `min: Option<f64>` | min of mins |
| MAX | `max: Option<f64>` | max of maxes |
| APPROX_DISTINCT | HLL sketch bytes | HLL union (register max) |

### Store Integration

- `publish_partials()` — serializes and publishes to `CrossPartitionAggregateStore`
- `collect_partials()` — collects and deserializes from store for a group key

### Arrow IPC

- `encode_batch_to_ipc()` — `RecordBatch` → Arrow IPC file format bytes
- `decode_batch_from_ipc()` — Arrow IPC bytes → `RecordBatch`

### Delta Bridge

Behind `#[cfg(feature = "delta")]`: `PartialState::to_aggregate_state()` / `from_aggregate_state()` for conversion to/from gossip `AggregateState`.

## API

```rust
// Detection
can_use_two_phase(&["COUNT", "SUM", "AVG"]) // -> true
TwoPhaseKind::from_name("COUNT")            // -> Some(Count)

// Phase 1: create partials
let partial = PartialAggregate {
    group_key: b"AAPL".to_vec(),
    partition_id: 0,
    states: vec![PartialState::Count(500)],
    watermark_ms: 1000, epoch: 1,
};
publish_partials(&store, &[partial])?;

// Phase 2: collect and merge
let collected = collect_partials(&store, b"AAPL");
let merger = MergeAggregator::new(vec![TwoPhaseKind::Count]);
let merged = merger.merge_all(&collected)?;
let finals = MergeAggregator::finalize(&merged[b"AAPL".as_ref()]);

// Arrow IPC for wire transport
let ipc = encode_batch_to_ipc(&batch)?;
let decoded = decode_batch_from_ipc(&ipc)?;
```

## Files Modified

| File | Change |
|------|--------|
| `crates/laminar-core/src/aggregation/two_phase.rs` | **New** — full implementation |
| `crates/laminar-core/src/aggregation/mod.rs` | Added `pub mod two_phase` |
| `docs/features/delta/cloud/F-XAGG-004-two-phase-aggregation.md` | Updated |
| `docs/features/delta/INDEX.md` | Updated |

## Test Plan

- 3-partition pipeline, same group key across all 3, verify merged COUNT is sum
- AVG with different counts per partition produces correct weighted average
- MIN/MAX across partitions returns global min/max
- `can_use_two_phase()` detection for supported/unsupported functions
- Arrow IPC roundtrip encode/decode
- JSON serialization roundtrip for partials
- Store publish/collect integration
- HLL sketch: add, merge, serialization, error cases
- MergeAggregator: single group, multi group, empty, function count mismatch

## Completion Checklist

- [x] `PartialAggregate` struct and `PartialState` enum
- [x] `MergeAggregator` combiner
- [x] `CrossPartitionAggregateStore` integration (`publish_partials`, `collect_partials`)
- [x] `can_use_two_phase()` detection
- [x] Arrow IPC serialization (`encode_batch_to_ipc`, `decode_batch_from_ipc`)
- [x] `HllSketch` for APPROX_DISTINCT
- [x] Delta bridge (`PartialState ↔ AggregateState`, feature-gated)
- [x] 33 unit tests + 2 doc-tests
- [x] Feature INDEX.md updated
