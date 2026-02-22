# F-POPT-003: AHashMapStore Key Deduplication

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-003 |
| **Status** | Draft |
| **Priority** | P2 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-002 |
| **Crate** | `laminar-core` |
| **Module** | `state/ahash_store.rs` |
| **Audit Ref** | M17 |

## Summary

`AHashMapStore` stores keys in both an `AHashMap` (for O(1) lookups) and a
`BTreeSet` (for ordered prefix scans). This doubles memory usage for every
key. Use a single ordered map or arena-based key storage.

## Problem

`ahash_store.rs:34` — Every key is stored twice: once in the hash map and
once in the B-tree set. For workloads with millions of small keys, this
doubles the key memory footprint.

## Proposed Fix

Option A: Replace both structures with `indexmap::IndexMap` which provides
O(1) lookups and deterministic iteration order.

Option B: Store keys in a byte arena with `Arc<[u8]>` shared between both
structures (single allocation per key).

Option C: Remove the `BTreeSet` and implement prefix scan via hash map
iteration with filtering (trades scan performance for memory).

## Trade-offs

- `indexmap` doesn't support prefix scans natively (iteration is insertion
  order, not lexicographic)
- Arena approach reduces memory but adds indirection
- Removing ordered index hurts prefix_scan performance; acceptable if
  F-POPT-001 moves scans to a different path
