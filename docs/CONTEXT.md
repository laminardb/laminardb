# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-10

### What Was Accomplished
- **F079: Compiled Expression Evaluator** — Cranelift JIT for DataFusion Expr trees
  - New `jit` feature in laminar-core gating Cranelift 0.128 + datafusion-expr/common
  - `ExprCompiler` compiles filter (`FilterFn`) and scalar (`ScalarFn`) expressions to native x86-64
  - Supports: column refs, literals (i8–u64, f32/f64, bool), arithmetic (+−×÷%), all 6 comparisons, AND/OR with short-circuit, NOT, IS NULL/IS NOT NULL, CAST, CASE/WHEN
  - Full SQL null propagation (NULL in arithmetic → NULL)
  - Constant folding pre-pass (`fold_constants`) eliminates literal sub-expressions before codegen
  - 48 new tests, all passing; clippy clean with `-D warnings`
  - 4 new files: `error.rs`, `jit.rs`, `fold.rs`, `expr.rs` in `compiler/` module
  - Phase 2.5 Plan Compiler: 2/5 features complete (40%)

Previous session (2026-02-10):
- **F017D+E: Session Emit Strategies + Timer Persistence** — Completes Issue #55
- **F017B: Session State Refactoring** — Multi-session per key support
- **F017C: Session Merging & Overlap Detection** — Core fix for Issue #55

Previous session (2026-02-08):
- **pgwire-replication integration for PostgreSQL CDC WAL streaming** (PR #74, closes #58)
- **FFI API Surface Update** — Exposed full LaminarDB API through `api::Connection` (PR #49)
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** — ALL 9 FEATURES COMPLETE

### Where We Left Off

**Phase 2: 38/38 features COMPLETE (100%)** ✅
**Phase 2.5: 2/5 features COMPLETE (40%)** — F078 ✅, F079 ✅
**Phase 3: 67/76 features COMPLETE (88%)**

**Test counts**: ~1,530 laminar-core (with jit), ~3,100+ with all feature flags

### Immediate Next Steps

**Phase 2.5 Plan Compiler** (next priority):
1. F080: Plan Compiler Core — compile full DataFusion LogicalPlan to native pipeline
2. F081: Ring 0/Ring 1 Pipeline Bridge
3. F082: Streaming Query Lifecycle

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
  compiler/       # Plan compiler: EventRow, JIT expr compiler (Cranelift), constant folding

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
