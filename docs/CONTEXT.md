# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-11

### What Was Accomplished
- **F083–F089: SQL Compiler Integration** — 7 features wiring the JIT compiler stack (F078–F082) into end-to-end SQL query execution
  - **F083: Batch Row Reader** — Arrow `RecordBatch` → `EventRow` conversion in bump arena
    - `BatchRowReader` with pre-downcast `ColumnAccessor` enum, 14 type variants
    - Round-trip correctness: `RecordBatch → EventRow → RowBatchBridge → RecordBatch`
    - 17 tests in `batch_reader.rs`
  - **F084: SQL Compiler Orchestrator** — Single `compile_streaming_query()` entry point
    - LogicalPlan → extract pipelines → compile → wire bridges → `CompiledStreamingQuery`
    - Breaker detection returns `None` for stateful operators (aggregate/sort/join)
    - `extract_table_scan()` pulls source scan for DataFusion execution
    - 12 tests in `orchestrate.rs`
  - **F085: LaminarDB JIT Query Execution** — Wire compiled path into `handle_query()`
    - `jit` feature on `laminar-db` forwarding to `laminar-core/jit`
    - `CompilerCache` field on `LaminarDB` (jit-gated)
    - `try_compile_query()` → `bridge_compiled_query()` with transparent fallback
    - `handle_query()` tries JIT first, falls through to DataFusion on `None`
    - 291 laminar-db tests pass
  - **F086: Adaptive Compilation Warmup** — Background JIT with seamless swap
    - `AdaptiveQueryRunner` using `tokio::sync::oneshot` for non-blocking compile results
    - `ExecutionMode` enum: Interpreted → Compiling → Compiled / FallbackPermanent
    - 6 tests in `adaptive.rs`
  - **F087: Compiled Stateful Pipeline Bridge** — Multi-segment compiled/interpreted execution
    - `Ring1Operator` trait for stateful operators at pipeline boundaries
    - `BreakerExecutor` wraps Ring1Operator with stats tracking
    - `CompiledQueryGraph` for linear chain of executors
    - 13 tests in `breaker_executor.rs` (jit-gated)
  - **F088: Schema-Aware Event Time Extraction** — Extract timestamps from data schema
    - `RowEventTimeExtractor` with auto-detection (first TimestampMicros, then well-known names)
    - `EventTimeConfig` with configurable column and watermark delay
    - Integrated into `bridge_compiled_query()` for correct event-time processing
    - 18 tests in `event_time.rs`
  - **F089: Compilation Metrics & Observability** — Track compilation stats
    - `CompilationMetrics` with atomic counters (compiled/fallback/error/compile_time)
    - `MetricsSnapshot` with `compilation_rate()`, `CacheSnapshot` with `hit_rate()`/`fill_ratio()`
    - 14 tests in `compilation_metrics.rs`
  - Phase 2.5 SQL Compiler Integration: **7/7 features COMPLETE (100%)** ✅

Previous session (2026-02-11):
- **F078–F082: Plan Compiler Core** — 5 features building the JIT compiler stack
  - F078: EventRow format, F079: Cranelift expression compiler, F080: Pipeline extraction/compilation/cache
  - F081: Ring 0/Ring 1 SPSC bridge, F082: StreamingQuery lifecycle management

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
