# F-POPT-005: Zero-Copy Join Row Encoding

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-005 |
| **Status** | Draft |
| **Priority** | P1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F019 |
| **Crate** | `laminar-core` |
| **Module** | `operator/stream_join.rs` |
| **Audit Ref** | H3 |

## Summary

`JoinRow::with_encoding()` serializes the full Arrow batch and clones the
join key per event stored in join state. Replace with a zero-copy-friendly
storage format that avoids per-event serialization.

## Problem

`stream_join.rs:1525` — Each event stored in join state goes through
`JoinRow::with_encoding()` which serializes the Arrow columns and copies
the key bytes. At high join rates, this dominates CPU time.

## Proposed Fix

Store raw Arrow IPC buffers directly in join state. Batch multiple events
before serializing (amortize encoding cost across N events). Use
`arrow_ipc::writer::StreamWriter` to pre-serialize column data, then store
the serialized bytes with an offset index for per-row access.

Alternative: Use `rkyv` zero-copy deserialization to avoid the
decode step on probe.

## Trade-offs

- Arrow IPC batching requires flushing partial batches on watermark advance
- rkyv approach ties state format to struct layout (migration on schema change)
- Current per-event approach is simpler and handles late arrivals naturally
