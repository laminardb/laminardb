# F-POPT-006: Pooled Arrow Builders for Join Output

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-006 |
| **Status** | Draft |
| **Priority** | P1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F019 |
| **Crate** | `laminar-core` |
| **Module** | `operator/stream_join.rs` |
| **Audit Ref** | H7 |

## Summary

`create_joined_event()` allocates 3 Arrow array structures per join match
(left columns, right columns, joined batch). Use pre-allocated
`ArrayBuilder` pools that accumulate multiple matches before materializing.

## Problem

`stream_join.rs:1948` — Every join match creates a new `RecordBatch` with
freshly allocated arrays. For high-cardinality joins (N:M), this produces
thousands of tiny single-row batches per second.

## Proposed Fix

Maintain per-operator `ArrayBuilder` columns (one builder per output field).
Append matched rows to the builders. Flush to a `RecordBatch` when the
builder reaches a configurable batch size (e.g., 1024 rows) or on watermark
advance.

```
probe event → append to builders → (if full) → flush RecordBatch → emit
                                   (if watermark) → flush + emit
```

## Trade-offs

- Adds latency: output is delayed until batch fills or watermark triggers flush
- Increases memory: builders hold partial batches
- Significantly reduces allocation rate and improves downstream processing
  efficiency (larger batches)
