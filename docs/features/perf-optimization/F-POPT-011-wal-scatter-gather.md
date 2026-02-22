# F-POPT-011: WAL Scatter-Gather Writes

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-011 |
| **Status** | Draft |
| **Priority** | P1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F007, F062 |
| **Crate** | `laminar-storage` |
| **Module** | `per_core_wal/writer.rs` |
| **Audit Ref** | C13 |

## Summary

Per-core WAL `append()` copies `key.to_vec()` + `value.to_vec()` per entry,
creating 3 copies of data before writing. Use scatter-gather I/O to write
directly from borrowed slices.

## Problem

`per_core_wal/writer.rs:159` — Each WAL append allocates owned copies of
the key and value, then serializes them into the WAL frame. The data flows
through: caller buffer → owned Vec → serialized frame → write buffer.
Two intermediate copies are unnecessary.

## Proposed Fix

Accept `&[u8]` key and value slices and write the WAL frame using
`writev()` (vectored I/O) or a pre-formatted frame buffer:

1. Compute frame header (length, CRC, flags) from the slice lengths
2. Use `IoSlice` array: `[header_bytes, key_slice, value_slice]`
3. Single `write_vectored()` call — no intermediate allocations

For io_uring paths, use `writev` SQEs with scatter-gather buffers.

## Trade-offs

- Requires changing the `WalWriter` API from `(Vec<u8>, Vec<u8>)` to
  `(&[u8], &[u8])`
- CRC must be computed over the slices (already possible with streaming CRC)
- Callers that currently pass owned Vecs can pass slices instead (breaking
  API change)
