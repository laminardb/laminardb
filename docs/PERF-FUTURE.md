# Performance — Architectural Improvements

Remaining items from the 2026-02-22 performance audit that require
architectural changes (not local fixes). Organized by subsystem.

---

## State Store

### C9 — AST→SQL→AST double parse
- **Location**: `laminar-sql/src/planner/mod.rs:568`
- **Problem**: `plan.statement.to_string()` serializes the AST to SQL, then
  DataFusion re-parses it. Full reparse per query plan.
- **Fix**: Pass `LogicalPlan` directly to DataFusion instead of round-tripping
  through SQL text. Requires refactoring the planner to produce a
  `LogicalPlan` natively rather than relying on DataFusion's SQL parser.

### C13 — Per-core WAL key/value copies
- **Location**: `laminar-storage/src/per_core_wal/writer.rs:159`
- **Problem**: `key.to_vec()` + `value.to_vec()` per WAL append creates 3
  copies of data per entry.
- **Fix**: Accept `&[u8]` slices and write directly to the WAL buffer using
  scatter-gather I/O or a pre-serialized frame layout. Requires changing the
  WAL entry format.

---

## Join Operators

### H3 — JoinRow encoding overhead
- **Location**: `laminar-core/src/operator/stream_join.rs:1525`
- **Problem**: `JoinRow::with_encoding()` serializes the full Arrow batch and
  clones the key per event stored in join state.
- **Fix**: Store raw column buffers (or Arrow IPC pre-serialized) in join state
  instead of re-encoding per event. Requires redesigning `JoinRow` storage
  format to be zero-copy-friendly.

### H7 — Arrow allocation per join match
- **Location**: `laminar-core/src/operator/stream_join.rs:1948`
- **Problem**: `create_joined_event()` allocates 3 Arrow array structures
  per match (left columns, right columns, joined batch).
- **Fix**: Use pre-allocated `ArrayBuilder` pools that are reset between
  matches, or batch multiple matches before materializing a `RecordBatch`.
  Requires changes to the join output pipeline.

---

## SQL Execution

### H9 — JSON UDF serde round-trip
- **Location**: `laminar-sql/src/json_extensions.rs:120+`
- **Problem**: 8+ JSON extension UDFs decode→manipulate→encode per row via
  `serde_json`. O(rows × json_size) allocations.
- **Fix**: Use a tree-surgery approach (e.g., `simd-json` or `jsonb`-style
  binary representation) that operates on the parsed DOM without
  re-serializing. Requires a shared JSON value cache or binary JSON column
  type.

### H11 — SQL plan caching
- **Location**: `laminar-sql/src/execute.rs:93`
- **Problem**: No plan caching in `execute_streaming_sql` — full
  parse→plan→optimize each invocation.
- **Note**: Currently each streaming query is planned once per pipeline
  lifetime, so the impact is setup-time only. This becomes important if
  ad-hoc SQL re-entry is supported (e.g., interactive queries on running
  pipelines).
- **Fix**: LRU cache keyed on SQL text hash, with cache invalidation on
  schema changes.

---

## Reactor & Dispatch

### M1 — prefix_scan Box\<dyn Iterator\>
- **Location**: `laminar-core/src/state/mod.rs:579`
- **Problem**: `prefix_scan()` allocates `Box<dyn Iterator>` on every scan,
  introducing a heap allocation + vtable dispatch per call.
- **Fix**: Use a generic associated type (GAT) or return a concrete iterator
  type per store implementation. Requires changing the `StateStore` trait.

### M16 — Virtual dispatch in reactor hot loop
- **Location**: `laminar-core/src/reactor/mod.rs:98`
- **Problem**: `dyn Operator` and `dyn StateStore` trait objects in the
  reactor's per-event processing loop. Each call goes through vtable
  indirection (pipeline stall + no inlining).
- **Fix**: Monomorphize the reactor over operator/store types using generics,
  or use an enum dispatch pattern. Trade-off: increases binary size and
  compile time.

---

## Data Structures

### M17 — AHashMapStore key duplication
- **Location**: `laminar-core/src/state/ahash_store.rs:34`
- **Problem**: Keys are stored in both the `AHashMap` and a `BTreeSet`
  (for ordered iteration / prefix scan). Double memory for every key.
- **Fix**: Use a single ordered map (e.g., `indexmap` or a B-tree with hash
  acceleration), or store keys in an arena with references from both
  structures.

### M8 — TableStore Mutex contention
- **Location**: `laminar-sql/src/table_provider.rs:30`
- **Problem**: `Arc<Mutex<TableStore>>` shared between the streaming pipeline
  and SQL query threads. Contention during concurrent queries and pipeline
  processing.
- **Fix**: Separate read and write paths — use a snapshot/MVCC approach where
  SQL queries read from an immutable snapshot while the pipeline writes to
  the current version.

### M19 — MmapStateStore fragmentation
- **Location**: `laminar-core/src/state/mmap.rs:523`
- **Problem**: Append-only layout never reclaims space from deleted or
  overwritten keys. Long-running pipelines accumulate dead space.
- **Fix**: Implement background compaction (copy live entries to a new mmap
  region, swap atomically). Requires a compaction scheduler and concurrent
  read support during compaction.

---

## Connectors

### M22 — Avro decoder rebuilt per batch
- **Location**: `laminar-connectors/src/kafka/avro.rs:148`
- **Problem**: `arrow_avro::Decoder` is reconstructed for every batch
  because it lacks a `reset()` method.
- **Fix**: Upstream contribution to `arrow-avro` adding `Decoder::reset()`,
  or maintain a decoder pool. Blocked on upstream API.
