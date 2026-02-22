# F-POPT-009: SQL Plan Cache

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-009 |
| **Status** | Draft |
| **Priority** | P3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F005, F006 |
| **Crate** | `laminar-sql` |
| **Module** | `execute.rs` |
| **Audit Ref** | H11 |

## Summary

`execute_streaming_sql` performs full parse→plan→optimize on every
invocation. Add an LRU plan cache to avoid redundant work for repeated
queries.

## Problem

`execute.rs:93` — No caching of parsed or optimized plans. Currently each
streaming query is planned once per pipeline lifetime (setup-time cost), so
impact is low. This becomes significant if ad-hoc SQL re-entry is supported
(interactive queries on running pipelines).

## Proposed Fix

LRU cache keyed on a hash of the SQL text + schema fingerprint. Cache the
optimized `LogicalPlan` after the first parse→plan→optimize cycle. On
subsequent calls with the same SQL and schema, return the cached plan.

Invalidate cache entries when:
- Schema changes (source/sink DDL)
- UDF registrations change
- Cache reaches size limit (LRU eviction)

## Trade-offs

- Low priority: current pipelines plan once at startup
- Becomes important for interactive SQL console (Phase 5, F049)
- Cache invalidation must be correct — stale plans with wrong schema
  produce silent data corruption
