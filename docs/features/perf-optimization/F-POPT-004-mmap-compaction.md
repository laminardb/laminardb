# F-POPT-004: MmapStateStore Compaction

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-004 |
| **Status** | Draft |
| **Priority** | P3 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-STATE-001 |
| **Crate** | `laminar-core` |
| **Module** | `state/mmap.rs` |
| **Audit Ref** | M19 |

## Summary

`MmapStateStore` uses an append-only layout that never reclaims space from
deleted or overwritten keys. Long-running pipelines accumulate dead space.
Implement background compaction.

## Problem

`state/mmap.rs:523` — Overwritten and deleted entries leave dead bytes in
the mmap file. Over hours/days of operation, the file grows without bound
even if the live key count is stable.

## Proposed Fix

Implement a compaction pass that:
1. Scans the hash index to identify live entries
2. Copies live entries to a new mmap region in sorted order
3. Atomically swaps the new region in (rename + remap)
4. Reclaims the old file

Trigger compaction when dead space exceeds a configurable threshold
(e.g., 50% of file size or 100MB absolute).

## Trade-offs

- Compaction requires a brief pause or copy-on-write during the swap
- Could run on a background thread during checkpoint quiescence
- Alternative: use a log-structured merge approach (more complex but
  amortizes compaction cost)
