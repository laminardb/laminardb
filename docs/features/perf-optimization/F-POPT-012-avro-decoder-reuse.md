# F-POPT-012: Avro Decoder Reuse

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-012 |
| **Status** | Draft |
| **Priority** | P3 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F-SCHEMA-006 |
| **Crate** | `laminar-connectors` |
| **Module** | `kafka/avro.rs` |
| **Audit Ref** | M22 |

## Summary

The Avro decoder is reconstructed for every batch because `arrow_avro`'s
`Decoder` lacks a `reset()` method. Cache or pool decoders to avoid
repeated initialization.

## Problem

`kafka/avro.rs:148` — A new `arrow_avro::Decoder` is built per batch,
re-parsing the Avro schema and re-building internal column converters each
time. For small, frequent batches this adds measurable overhead.

## Proposed Fix

Option A: **Upstream contribution** — Add `Decoder::reset()` to `arrow-avro`
that clears internal row buffers without re-parsing the schema. This is the
cleanest fix but blocked on upstream acceptance.

Option B: **Decoder pool** — Maintain a small pool (1-2 decoders per schema
ID). Return decoders to the pool after use. Since schemas rarely change,
the pool hit rate approaches 100%.

Option C: **Wrapper with cached schema** — Cache the parsed Avro schema and
only reconstruct the Decoder column converters (skip schema parsing).

## Trade-offs

- Option A is blocked on upstream `arrow-avro` API changes
- Option B adds pool management complexity
- Option C requires understanding `arrow-avro` internals to separate schema
  parsing from converter construction
