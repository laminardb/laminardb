# F-POPT-008: Binary JSON Representation

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-POPT-008 |
| **Status** | Draft |
| **Priority** | P2 |
| **Effort** | XL (2-4 weeks) |
| **Dependencies** | F-SCHEMA-011, F-SCHEMA-013 |
| **Crate** | `laminar-sql` |
| **Module** | `json_extensions.rs` |
| **Audit Ref** | H9 |

## Summary

8+ JSON extension UDFs perform full serde_json decode→manipulate→encode
per row. Replace with a binary JSON column type (JSONB-style) or SIMD-based
tree surgery that operates without full re-serialization.

## Problem

`json_extensions.rs:120+` — Every JSON UDF call deserializes the input
string to `serde_json::Value`, performs a small manipulation (path access,
key insertion, etc.), then serializes back to a string. For N rows with
average JSON size S, this is O(N * S) allocations.

## Proposed Fix

Option A: **JSONB column type** — Store JSON as a binary format (similar to
PostgreSQL JSONB) that supports path access, key lookup, and mutation
without full deserialization. Parse JSON→JSONB on ingestion, operate on
JSONB in UDFs, serialize JSONB→JSON only on output.

Option B: **simd-json integration** — Use `simd-json` for tape-based
parsing that avoids DOM allocation. Path access and extraction can operate
on the tape directly.

Option C: **Cached DOM per batch** — Parse each JSON value once per batch,
cache the DOM in a column-level side structure, and reuse across multiple
UDF invocations in the same query.

## Trade-offs

- JSONB is the most impactful but requires a custom Arrow extension type
  and changes to the serde layer
- simd-json tape is read-only (mutations still require materialization)
- Cached DOM is simplest but still allocates the full DOM once per row
