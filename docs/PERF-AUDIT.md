# LaminarDB Performance Audit Report

**Date**: 2026-02-22
**Branch**: `feat/schema_infer`
**Scope**: Full codebase (471 Rust source files across 8 crates)

## Executive Summary

| Area | Findings | CRIT | HIGH | MED | LOW | Verdict |
|------|----------|------|------|-----|-----|---------|
| Core hot-path allocations | 27 | 9 | 8 | 6 | 4 | Significant issues |
| Locking & synchronization | 11 | 0 | 1 | 4 | 6 | Meets targets (Ring 0 clean) |
| SQL execution | 12 | 2 | 3 | 5 | 2 | Significant issues |
| Cache & data layout | 17 | 1 | 4 | 4 | 8 | Minor optimizations needed |
| Connectors & storage | 19 | 2 | 5 | 5 | 7 | Significant issues |
| Benchmarks & unsafe | 7 gaps | — | — | — | — | Good coverage, safe code |
| **Total** | **93** | **14** | **21** | **24** | **27** | |

**Overall verdict**: Minor optimizations recommended for Ring 0 core; significant optimizations needed in connectors and SQL layer.

The core reactor, SPSC queues, broadcast channels, and operator framework are well-designed — lock-free with correct memory ordering. But the state store access patterns and operator key extraction introduce per-event heap allocations that violate the zero-allocation constraint. The SQL and connector layers have the heaviest issues.

---

## CRITICAL Findings (14 total)

### Core State Store — Per-Event Allocations

| # | Finding | Location | Impact |
|---|---------|----------|--------|
| C1 | `StateStoreExt::put_typed()` calls `rkyv::to_bytes()` allocating `AlignedVec` per write | `state/mod.rs:360` | Every window/session accumulator update |
| C2 | `AHashMapStore::get()` does `Bytes::copy_from_slice(v)` per lookup | `ahash_store.rs:74` | Heap alloc on every state read |
| C3 | `InMemoryStore::put()` unconditional `key.to_vec()` via `entry()` API | `state/mod.rs:555` | Heap alloc on every state write |
| C4 | `InMemoryStore::put()` does `Bytes::copy_from_slice(value)` per write | `state/mod.rs:552` | Heap alloc on every state write |

### Core Operators — Per-Event Allocations

| # | Finding | Location | Impact |
|---|---------|----------|--------|
| C5 | `StreamJoinOperator::make_state_key()` allocates `Vec<u8>` (28 bytes) per event | `stream_join.rs:1845` | Should be `[u8; 28]` |
| C6 | `StreamJoinOperator::extract_key()` allocates `Vec<u8>` per event | `stream_join.rs:1819` | Should be `SmallVec<[u8; 16]>` |
| C7 | `StreamJoinOperator::probe_opposite_side()` allocates `Vec` + `Vec<u8>` per probe | `stream_join.rs:1896,1909` | Multiple allocs per join event |
| C8 | `SessionWindowOperator::extract_key()` allocates `Vec<u8>` per event | `session_window.rs:502` | Should be `SmallVec<[u8; 16]>` |

### SQL Execution — Per-Batch Overhead

| # | Finding | Location | Impact |
|---|---------|----------|--------|
| C9 | AST->SQL->AST double parse: `plan.statement.to_string()` then DataFusion re-parses | `planner/mod.rs:568` | Full reparse per query plan |
| C10 | Per-invocation `SessionContext::new()` in lambda HOFs (array_transform, etc.) | `complex_type_lambda.rs:47` | New context + batch clone per UDF call |

### Connectors — Per-Message Overhead

| # | Finding | Location | Impact |
|---|---------|----------|--------|
| C11 | Kafka source `payload.to_vec()` per message (1000 heap allocs per batch) | `kafka/source.rs:339` | 100-250ms/sec at 500K msg/sec |
| C12 | Kafka sink row-by-row `producer.send().await` (sequential per row) | `kafka/sink.rs:367` | 5-50ms per 1K-row batch |

### Storage — WAL Write Path

| # | Finding | Location | Impact |
|---|---------|----------|--------|
| C13 | Per-core WAL `key.to_vec()` + `value.to_vec()` per append | `per_core_wal/writer.rs:159` | 3 copies of data per WAL entry |
| C14 | WAL `rkyv::to_bytes()` allocates `AlignedVec` per append | `wal.rs:208` | Additional alloc per WAL write |

---

## HIGH Findings (21 total)

### Core (8)

| # | Finding | Location |
|---|---------|----------|
| H1 | `Reactor::poll()` allocates `Vec<Output>` every call (use `poll_into()`) | `reactor/mod.rs:344` |
| H2 | `Output::SideOutput` name is `String` — alloc per late event (use `Arc<str>`) | `operator/mod.rs:55` |
| H3 | `JoinRow::with_encoding()` serializes full batch + clones key per event | `stream_join.rs:1525` |
| H4 | `InMemoryStore::get()` doesn't implement `get_ref()` (no zero-copy path) | `state/mod.rs:547` |
| H5 | `MmapStateStore::get()` does `Bytes::copy_from_slice` per lookup | `state/mmap.rs:516` |
| H6 | `SlidingWindowOperator::process()` allocates `Vec<WindowId>` per event | `sliding_window.rs:678` |
| H7 | `create_joined_event()` allocates 3 Arrow structures per match | `stream_join.rs:1948` |
| H8 | Window `on_timer()` allocates `Int64Array::from(vec![...])` per emission | `window.rs:3861` |

### SQL (3)

| # | Finding | Location |
|---|---------|----------|
| H9 | serde_json round-trip in 8+ JSON extension UDFs (decode->manipulate->encode per row) | `json_extensions.rs:120+` |
| H10 | Per-accumulator `Schema`/`Field` allocation with `format!()` per window partition | `aggregate_bridge.rs:283` |
| H11 | No plan caching in `execute_streaming_sql` (full parse->plan->optimize each time) | `execute.rs:93` |

### Connectors & Storage (5)

| # | Finding | Location |
|---|---------|----------|
| H12 | Kafka sink per-row key `Vec<u8>` extraction | `kafka/sink.rs:178` |
| H13 | JSON serializer per-row `serde_json::to_vec` + `Map::new()` | `serde/json.rs:130` |
| H14 | CDC `tuple_to_json()` creates HashMap + clones columns per row | `cdc/postgres/changelog.rs:68` |
| H15 | Debezium double JSON parse (serialize `Value` -> re-parse bytes) | `serde/debezium.rs:134` |
| H16 | WAL per-core writer 3 separate `write_all()` calls instead of coalesced | `per_core_wal/writer.rs:193` |

### Cache & Layout (4)

| # | Finding | Location |
|---|---------|----------|
| H17 | `Output` enum variant size disparity (Watermark=8B vs CheckpointComplete=32B+) | `operator/mod.rs:46` |
| H18 | Default `InMemoryStore` uses `BTreeMap` (O(log n)) — `AHashMapStore` should be default | `reactor/mod.rs:137` |
| H19 | `SessionIndex::find_overlapping()` allocates `Vec<SessionId>` per event | `session_window.rs:222` |
| H20 | `Instant::now()` syscall per event per key in `KeyWatermarkState::update()` | `keyed_watermark.rs:115` |

### Locking (1)

| # | Finding | Location |
|---|---------|----------|
| H21 | `parking_lot::RwLock` on streaming Sink broadcast acquired every processing cycle | `streaming/sink.rs:227` |

---

## MEDIUM Findings (24 total)

### Core (6)

| # | Finding | Location |
|---|---------|----------|
| M1 | `prefix_scan()` allocates `Box<dyn Iterator>` on every scan | `state/mod.rs:579` |
| M2 | `LookupJoinOperator::process()` clones `Vec<u8>` key to pending buffer | `lookup_join.rs:851` |
| M3 | `WatermarkBoundedSortOperator::process()` may trigger Vec reallocation | `watermark_sort.rs:198` |
| M4 | `DagExecutor::process_watermark()` SmallVec spills for >8 downstream nodes | `executor.rs:302` |
| M5 | `ChangelogAwareStore::put()` production sink may allocate | `changelog_aware.rs:94` |
| M6 | `AHashMapStore::put()` allocates `key_vec` even for updates | `ahash_store.rs:83` |

### Locking (4)

| # | Finding | Location |
|---|---------|----------|
| M7 | `std::sync::RwLock` (not parking_lot) in SubscriptionRegistry + write lock per delivery | `registry.rs:192` |
| M8 | `Arc<parking_lot::Mutex<TableStore>>` contention between pipeline and SQL queries | `table_provider.rs:30` |
| M9 | `tokio::sync::Mutex` on Source/Sink connectors blocks during checkpoint | `db.rs:2361` |
| M10 | `parking_lot::RwLock` on lookup join table — contention with CDC updates | `lookup/mod.rs:239` |

### SQL (5)

| # | Finding | Location |
|---|---------|----------|
| M11 | Projection/filter cloning in `StreamingScanExec::execute()` | `exec.rs:240` |
| M12 | Per-batch `RecordBatch::try_new` in `ProjectingStream` (use `batch.project()`) | `channel_source.rs:256` |
| M13 | Window UDF scalar iteration instead of Arrow compute kernels | `window_udf.rs:451` |
| M14 | `DataFusionAccumulatorAdapter::serialize()` uses `to_string()` for state | `aggregate_bridge.rs:205` |
| M15 | JSON path recompilation per batch when path is scalar constant | `json_path.rs:310` |

### Cache & Layout (4)

| # | Finding | Location |
|---|---------|----------|
| M16 | Virtual dispatch (`dyn Operator`, `dyn StateStore`) in reactor hot loop | `reactor/mod.rs:98` |
| M17 | `AHashMapStore` duplicates keys in both AHashMap and BTreeSet | `ahash_store.rs:34` |
| M18 | `std::collections::HashSet<WindowId>` instead of `FxHashSet` in window operators | `window.rs:3272` |
| M19 | `MmapStateStore` never reclaims space (append-only fragmentation) | `state/mmap.rs:523` |

### Connectors & Storage (5)

| # | Finding | Location |
|---|---------|----------|
| M20 | CDC `ColumnValue::Text` allocates String per column per row | `cdc/postgres/decoder.rs:518` |
| M21 | Debezium no `deserialize_batch()` — falls back to per-record + concat | `serde/debezium.rs` |
| M22 | Avro decoder rebuilt per batch | `kafka/avro.rs:148` |
| M23 | Per-core WAL `SystemTime::now()` per entry (~20-50ns) | `per_core_wal/entry.rs:179` |
| M24 | Incremental checkpoint `std::HashMap` instead of `FxHashMap` | `incremental/manager.rs:207` |

---

## LOW Findings (27 total)

<details>
<summary>Click to expand LOW findings</summary>

### Core (4)
- `MmapStateStore::put()` unconditional `key.to_vec()` via `entry()` — `state/mmap.rs:535`
- `Reactor::submit_batch()` takes `Vec<Event>` by value — `reactor/mod.rs:186`
- `TimerService` BinaryHeap grows dynamically — `time/mod.rs:329`
- Missing `#[inline]` on `AHashMapStore::put()`/`delete()` — `ahash_store.rs:82`

### Locking (6)
- `Ordering::SeqCst` where Relaxed/AcqRel suffices (5 locations) — checkpoint, WAL, transaction IDs
- `parking_lot::RwLock` on WebSocket fanout (I/O path, acceptable) — `websocket/fanout.rs:99`

### SQL (2)
- `Mutex` on `ChannelStreamSource` (setup-only, acceptable) — `channel_source.rs:63`
- Double `supports_filters` + Expr cloning (planning path) — `table_provider.rs:95`

### Cache & Layout (8)
- `TimerRegistration` struct padding waste (`Option<usize>` = 16 bytes) — `time/mod.rs:277`
- `SessionIndex::remove()` O(n) shift — `session_window.rs:213`
- `ChangelogAwareStore` Arc+vtable overhead (~2-5ns, acceptable) — `changelog_aware.rs:60`
- `WatermarkTracker` parallel Vecs instead of struct-of-arrays — `watermark.rs:436`
- `StreamJoinOperator` ~500+ byte struct with hot/cold data mixed — `stream_join.rs:1100`
- `TumblingWindowOperator::last_emitted` HashMap allocated even for non-changelog strategies — `window.rs:3288`
- `prefix_successor()` allocates `Vec<u8>` per prefix scan — `state/mod.rs:110`
- No `#[cold]` on error paths in operators

### Connectors & Storage (7)
- Kafka source `PartitionInfo` String allocations inside per-message loop — `kafka/source.rs:347`
- Kafka offset commit TPL rebuild (infrequent, negligible) — `kafka/source.rs:204`
- WAL `validate_record` allocates Vec for CRC check (recovery path only) — `wal.rs:433`
- Lookup table `batch.clone()` returns full RecordBatch clone — `lookup/mod.rs:311`
- Checkpoint SHA-256 computed synchronously on upload — `checkpointer.rs:213`
- Incremental checkpoint `std::HashMap` for state — `incremental/manager.rs:207`
- Per-core WAL 3 separate `write_all()` calls — `per_core_wal/writer.rs:193`

</details>

---

## What's Done Well (Positives)

The audit found strong engineering in the core:

- **Reactor is fully lock-free** — single-threaded, pre-allocated buffers, spin-loop hint for idle
- **SPSC queue is textbook correct** — `CachePadded` head/tail, Acquire/Release ordering, power-of-2 bitmask
- **Broadcast channel is lock-free on hot path** — 64-byte aligned `CursorSlot` with proper atomic ordering
- **`SmallVec` already used** for `OutputVec<[Output; 4]>`, `FiredTimersVec`, `TimerKey`
- **`WindowId::to_key_inline()`** returns stack-allocated `[u8; 16]`
- **State stores are `Send` but NOT `Sync`** by design — no locks needed
- **All operators are lock-free** — zero Mutex/RwLock in any operator
- **Pipeline counters avoid false sharing** — `#[repr(C)]` with explicit cache-line padding
- **Streaming channel stats** use `CachePadded` individually
- **Benchmark coverage is excellent** — 26 categories with explicit target validation

---

## Benchmark Coverage

### Covered (26 areas)

State store, throughput, latency, reactor, windows, joins, streaming ring, DAG routing/latency/throughput/stress, TPC SPSC/routing/runtime, checkpoint barrier, WAL, storage checkpoint, JIT compiler, delta checkpoint, cache, lookup join, io_uring, subscriptions.

### Missing (7 areas)

1. **SQL parsing/planning latency** (no benchmarks in `laminar-sql`)
2. **Filter/Map operator processing in isolation**
3. **Serde paths** (Avro/JSON/Arrow IPC codec throughput)
4. **Connector I/O** (Kafka produce/consume, CDC decode)
5. **Multi-threaded contention** under concurrent load
6. **Zero-allocation verification** with `HotPathDetectingAlloc`
7. **Large-state checkpoint/recovery** (realistic > 100MB)

---

## Unsafe Code Audit

**46 files** with `unsafe`, **~87 blocks** total.

| Category | Blocks | Verdict |
|----------|--------|---------|
| Lock-free structures (ring buffers, SPSC, broadcast, multicast) | ~25 | All **SOUND** |
| JIT compiler (Cranelift fn ptrs, transmute, execute) | ~10 | All **SOUND** |
| Send/Sync impls | 12 | 11 SOUND, 1 **QUESTIONABLE** |
| FFI boundary (C-ABI exports) | ~20 | All **SOUND** with null checks |
| OS-level (CPU affinity, NUMA, CPUID) | ~10 | All **SOUND** |
| io_uring | ~8 | All **SOUND** |
| Zero-copy serde (`from_raw_parts`) | 2 | SOUND (could use `bytemuck`) |

Every `unsafe` block has a `// SAFETY:` comment. No undefined behavior risks identified.

**One QUESTIONABLE item**: `DataFusionAccumulatorAdapter` wraps `RefCell<Box<dyn Accumulator>>` with an `unsafe impl Send`. Sound only if single-thread invariant holds; fragile design.

---

## Top 10 Fixes by Impact (Prioritized)

| Priority | Fix | Est. Impact | Effort |
|----------|-----|-------------|--------|
| **P0** | Thread-local reusable `AlignedVec` for `put_typed()` rkyv serialization | Eliminates #1 most frequent alloc | Low |
| **P0** | Replace `Vec<u8>` with `[u8; 28]` / `SmallVec<[u8; 16]>` in join+session key extraction | Eliminates 4-6 allocs per join event | Low |
| **P0** | Implement `get_ref()` on `InMemoryStore` + `MmapStateStore`, update `get_typed()` to prefer it | Eliminates heap copy per state read | Low |
| **P0** | Fix `InMemoryStore::put()` / `AHashMapStore::put()` to `get_mut()` before `entry()` | Eliminates unconditional key alloc on updates | Low |
| **P1** | Kafka sink: batch sends with `FuturesUnordered` instead of sequential await | 10-100x sink throughput | Medium |
| **P1** | Kafka source: arena buffer for payloads instead of per-message `to_vec()` | Eliminates 1000 allocs per batch | Medium |
| **P1** | Cache `LogicalPlan` / `SessionContext` in SQL layer (C9, C10) | Eliminates re-parse + re-plan per batch | Medium |
| **P2** | Box infrequent `Output` enum variants (`SideOutput`, `Changelog`, `CheckpointComplete`) | Better SmallVec cache utilization | Low |
| **P2** | Change default state store from `InMemoryStore` (BTreeMap) to `AHashMapStore` | O(1) vs O(log n) lookups | Low |
| **P2** | Pass processing time from reactor instead of `Instant::now()` per event per key | Eliminates millions of syscalls/sec | Medium |
