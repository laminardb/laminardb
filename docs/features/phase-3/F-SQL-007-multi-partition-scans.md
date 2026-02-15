# F-SQL-007: Multi-Partition Streaming Scans

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SQL-007 |
| **Status** | ✅ Done |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | S (1 day) |
| **Dependencies** | F005B (Advanced DataFusion Integration) |
| **Completed** | 2026-02-15 |

## Summary

Enable multi-partition scans for `SourceSnapshotProvider` (ad-hoc
`SELECT * FROM source`) so DataFusion can utilize multiple cores for
parallel query execution over source snapshots.

## Motivation

Ad-hoc `SELECT * FROM source` queries use `SourceSnapshotProvider`, which
previously wrapped all snapshot batches into a single-partition `MemTable`.
This prevented DataFusion from parallelizing the scan across cores.

The main streaming execution path (`StreamExecutor` micro-batch pattern)
already gets multi-partition scanning for free: `SessionContext::new()`
defaults `target_partitions` to `num_cpus`, and `MemTable` natively
distributes across partitions.

## Design

### SourceSnapshotProvider

Distributes snapshot batches round-robin across `num_partitions` partitions
via `MemTable`. The partition count is sourced from
`SessionContext::target_partitions()`.

- `num_partitions` field, clamped to `1..=256`
- `scan()` distributes batches: `partitions[i % num_partitions].push(batch)`
- Empty partitions get empty `Vec` (DataFusion handles this gracefully)

### db.rs Wiring

Passes `ctx.state().config().target_partitions()` to
`SourceSnapshotProvider::new()` when registering snapshot tables.

## Files Changed

| File | Change |
|------|--------|
| `crates/laminar-db/src/table_provider.rs` | Round-robin batch distribution |
| `crates/laminar-db/src/db.rs` | Wire `target_partitions` to provider |

## Testing

- Verified via existing snapshot query tests
- Round-robin distribution produces correct total row counts
- Single-batch and multi-batch snapshots both work correctly

## Performance

All changes in Ring 2 (query path, separate Tokio runtime). Ring 0 hot path
is completely untouched. No new locks or allocations on the hot path.
