# F-SQL-007: Multi-Partition Streaming Scans

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SQL-007 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (2-3 days) |
| **Dependencies** | F005B (Advanced DataFusion Integration) |
| **Completed** | 2026-02-15 |
| **PR** | #94 |

## Summary

Enable multi-partition streaming scans so DataFusion can utilize multiple
cores for parallel query execution. Previously all streaming scans were
forced single-threaded (single partition).

## Motivation

DataFusion queries over streaming sources were limited to a single partition,
preventing parallel scan execution. This is a bottleneck for:

1. **Ad-hoc SELECT on sources** (`SourceSnapshotProvider`) — snapshot batches
   were wrapped in a single-partition `MemTable`.
2. **Streaming queries** (`ChannelStreamSource`) — single bridge, single
   partition forced `CoalescePartitionsExec` to collapse everything.

Multi-partition unbounded sources are standard DataFusion practice (MemTable,
Parquet, CSV all do it). This feature enables parallel scans while keeping
the Ring 0 hot path untouched (all changes in Ring 2 query path).

## Design

### StreamSource Trait Extension

```rust
fn num_partitions(&self) -> usize { 1 }

fn stream(
    &self,
    partition: usize,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
) -> Result<SendableRecordBatchStream, DataFusionError>;
```

### ChannelStreamSource

Per-partition bridges and senders:

- `with_partitions(schema, n)` — N independent channels
- `take_sender(partition)` / `take_senders()` — per-partition sender access
- `reset()` — recreates all bridges, returns `Vec<BridgeSender>`

### StreamingScanExec

Reads `source.num_partitions()`, sets `Partitioning::UnknownPartitioning(n)`.
`execute(partition)` validates range and delegates to `source.stream(partition, ...)`.

### SourceSnapshotProvider

Distributes snapshot batches round-robin across `num_partitions` partitions
via `MemTable`. Partition count sourced from `SessionContext::target_partitions()`.

### Context Factory

`create_partitioned_streaming_context(n)` sets DataFusion's `target_partitions`
to match the source partition count.

## Files Changed

| File | Change |
|------|--------|
| `crates/laminar-sql/src/datafusion/source.rs` | `num_partitions()` + `partition` param |
| `crates/laminar-sql/src/datafusion/channel_source.rs` | Per-partition bridges/senders |
| `crates/laminar-sql/src/datafusion/exec.rs` | Multi-partition `Partitioning` + validation |
| `crates/laminar-sql/src/datafusion/mod.rs` | `create_partitioned_streaming_context()` |
| `crates/laminar-sql/src/datafusion/execute.rs` | Updated `take_sender(0)` call |
| `crates/laminar-sql/src/datafusion/table_provider.rs` | Updated MockSource |
| `crates/laminar-db/src/table_provider.rs` | Round-robin batch distribution |
| `crates/laminar-db/src/db.rs` | Wire `target_partitions` to provider |

## Testing

- Default `num_partitions()` returns 1
- Multi-partition channel source: N senders -> N partitions -> query collects all
- Out-of-range partition returns error
- Partitioned streaming context sets correct `target_partitions`
- All existing tests updated for new `stream(partition, ...)` signature

## Performance

All changes in Ring 2 (query path, separate Tokio runtime). Ring 0 hot path
is completely untouched. No new locks or allocations on the hot path.
