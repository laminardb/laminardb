# F-POPT-010: MVCC TableStore

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-010 |
| **Status** | Draft |
| **Priority** | P2 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-CONN-002 |
| **Crate** | `laminar-sql` |
| **Module** | `table_provider.rs` |
| **Audit Ref** | M8 |

## Summary

`Arc<Mutex<TableStore>>` is shared between the streaming pipeline and SQL
query threads. Contention during concurrent queries and pipeline processing
degrades both paths. Replace with a snapshot/MVCC approach.

## Problem

`table_provider.rs:30` — The pipeline writes to `TableStore` on every
batch, and SQL queries read from it for lookup joins and table scans. The
`Mutex` serializes these operations, causing head-of-line blocking when a
long-running SQL query holds the lock during a pipeline write.

## Proposed Fix

Use a copy-on-write or MVCC approach:
1. Pipeline writes go to a mutable "current" version
2. SQL queries acquire an immutable snapshot (cheap Arc clone of the current
   state) at query start
3. No lock contention between readers and the single writer

Implementation: `arc-swap` for atomic pointer swap of the current
`TableStore` version, or a custom epoch-based reclamation scheme.

## Trade-offs

- Snapshots consume memory until all readers release them
- Slightly stale reads for SQL queries (acceptable for streaming analytics)
- More complex than the current Mutex but eliminates contention entirely
