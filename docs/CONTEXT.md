# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-14

### What Was Accomplished
- **Fix: EMIT ON WINDOW CLOSE in Embedded SQL Executor** (PR #90, closes #85)
  - `EMIT ON WINDOW CLOSE` was parsed and planned correctly but lost at the `StreamRegistration` boundary — `handle_create_stream()` discarded the planner result (`let _ = planner.plan()`)
  - Implemented watermark-gated source accumulation for EOWC/Final streams:
    - **`batch_filter.rs`** (NEW): Shared `filter_batch_by_timestamp()` with `ThresholdOp::GreaterEq/Less`, replacing 130-line `filter_late_rows` body
    - **`connector_manager.rs`**: Extended `StreamRegistration` with `emit_clause` + `window_config`
    - **`db.rs`**: Capture planner result; pass emit/window to registration; pass watermark to `execute_cycle()`
    - **`stream_executor.rs`**: `EowcState` per-query accumulator, EOWC branch in `execute_cycle()`, `compute_closed_boundary()` for tumbling/session/sliding
  - Non-EOWC streams: zero change, zero overhead
  - 304 laminar-db tests pass (8 new EOWC tests + 296 existing)

Previous session (2026-02-11):
- **F083–F089: SQL Compiler Integration** — 7 features wiring JIT into end-to-end SQL execution
- **F078–F082: Plan Compiler Core** — 5 features building the JIT compiler stack

Previous session (2026-02-08):
- **pgwire-replication integration for PostgreSQL CDC WAL streaming** (PR #74, closes #58)
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** — ALL 9 FEATURES COMPLETE

### Where We Left Off

**Phase 2: 38/38 features COMPLETE (100%)** ✅
**Phase 2.5: 12/12 features COMPLETE (100%)** ✅ — F078–F082 (Plan Compiler) ✅, F083–F089 (SQL Compiler Integration) ✅
**Phase 3: 67/76 features COMPLETE (88%)**

**Test counts**: ~1,700+ laminar-core (with jit), ~3,100+ with all feature flags

### Immediate Next Steps

**Phase 3 remaining work**:
1. F027 follow-ups: TLS cert path support for pgwire-replication, initial snapshot, auto-reconnect
2. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
3. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
4. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)

### Open Issues
- **pgwire-replication TLS**: `SslMode::Require`/`VerifyCa`/`VerifyFull` currently fall back to disabled — need cert path configuration plumbing.
- **iceberg-rust crate**: Deferred until compatible with workspace DataFusion. Business logic complete in F032.
- No other blockers.

---

## Quick Reference

### Key Modules
```
laminar-core/src/
  dag/            # DAG pipeline: topology, multicast, routing, executor, checkpointing
  streaming/      # In-memory streaming: ring buffer, SPSC/MPSC channels, source, sink
  subscription/   # Reactive push-based: events, notifications, registry, dispatcher, backpressure, filtering
  time/           # Watermarks: partitioned, keyed, alignment groups
  operator/       # Windows, joins (stream/asof/temporal), changelog, lag_lead, table_cache (LRU + xor)
  state/          # State stores: InMemoryStore, ChangelogAwareStore (Ring 0 wrapper), ChangelogSink trait
  mv/             # Cascading materialized views
  tpc/            # Thread-per-core: SPSC, key router, core handle, backpressure, runtime
  sink/           # Exactly-once: transactional sink, epoch adapter
  alloc/          # Zero-allocation enforcement
  numa/           # NUMA-aware memory
  io_uring/       # io_uring + three-ring I/O
  xdp/            # XDP/eBPF network optimization
  budget/         # Task budget enforcement
  compiler/       # Plan compiler: EventRow, JIT expr compiler (Cranelift), constant folding,
                  #   pipeline extraction, pipeline compilation, cache, fallback,
                  #   metrics (always-available types), query (StreamingQuery lifecycle),
                  #   batch_reader (Arrow→EventRow), orchestrate (compile_streaming_query),
                  #   event_time (schema-aware extraction), compilation_metrics (observability),
                  #   breaker_executor (stateful pipeline bridge)

laminar-sql/src/
  parser/         # Streaming SQL: windows, emit, late data, joins, aggregation, analytics, ranking
  planner/        # StreamingPlanner, QueryPlan
  translator/     # Operator config builders: window, join, analytic, order, having, DDL
  datafusion/     # DataFusion integration: UDFs, aggregate bridge, execute_streaming_sql

laminar-connectors/src/
  kafka/          # Source, sink, Avro serde, schema registry, partitioner, backpressure
  postgres/       # Sink (COPY BINARY + upsert + exactly-once)
  cdc/postgres/   # CDC source (pgoutput decoder, Z-set changelog, replication I/O)
  cdc/mysql/      # CDC source (binlog decoder, GTID, Z-set changelog)
  lakehouse/      # Delta Lake + Iceberg sinks (buffering, epoch, changelog)
  storage/        # Cloud storage: provider detection, credential resolver, config validation, secret masking
  bridge/         # DAG ↔ connector bridge (source/sink bridges, runtime orchestration)
  sdk/            # Connector SDK: retry, rate limiting, circuit breaker, test harness, schema discovery
  serde/          # Format implementations: JSON, CSV, raw, Debezium, Avro

laminar-storage/src/
  incremental/    # Incremental checkpointing (RocksDB backend)
  per_core_wal/   # Per-core WAL segments
  checkpoint_manifest.rs  # Unified CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint
  checkpoint_store.rs     # CheckpointStore trait, FileSystemCheckpointStore (atomic writes)
  changelog_drainer.rs    # Ring 1 SPSC changelog consumer

laminar-db/src/
  batch_filter.rs # Shared timestamp-based batch filtering (late-row + EOWC closed-window)
  stream_executor.rs # DataFusion micro-batch executor with EOWC accumulation
  checkpoint_coordinator.rs  # Unified checkpoint orchestrator (F-CKP-003)
  recovery_manager.rs       # Unified recovery: load manifest, restore all state (F-CKP-007)
  api/            # FFI-ready API: Connection, Writer, QueryStream, ArrowSubscription
  ffi/            # C FFI: opaque handles, Arrow C Data Interface, async callbacks
```

### Useful Commands
```bash
cargo test --all --lib                    # Run all tests (base features)
cargo test --all --lib --features kafka   # Include Kafka/Avro tests
cargo bench --bench dag_bench             # DAG pipeline benchmarks
cargo clippy --all -- -D warnings         # Lint
```
