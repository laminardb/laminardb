# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-08

### What Was Accomplished
- **FFI API Surface Update** — Exposed full LaminarDB API through `api::Connection` for Python bindings (PR #49)
  - `connection.rs`: 18 new passthrough methods — `source_info()`, `sink_info()`, `stream_info()`, `query_info()`, `pipeline_topology()`, `pipeline_state()`, `pipeline_watermark()`, `total_events_processed()`, `source_count()`, `sink_count()`, `active_query_count()`, `metrics()`, `source_metrics()`, `all_source_metrics()`, `stream_metrics()`, `all_stream_metrics()`, `cancel_query()`, `shutdown()`, `subscribe()`
  - `mod.rs`: Re-exported `SourceInfo`, `SinkInfo`, `StreamInfo`, `QueryInfo`, `PipelineTopology`, `PipelineNode`, `PipelineEdge`, `PipelineNodeType`, `PipelineMetrics`, `PipelineState`, `SourceMetrics`, `StreamMetrics`
  - `db.rs`: Added `pub(crate) subscribe_raw()` helper to support `Connection::subscribe()` without `FromBatch` trait bounds
  - 6 new tests, 307 total laminar-db tests passing, clippy clean
- **F027 PostgreSQL CDC Replication I/O** (#44, PR #48) - MERGED
  - `postgres_io.rs` (NEW, 455 lines): Replication wire format parsing (`XLogData`, `PrimaryKeepalive`), `encode_standby_status` (34-byte 'r' tag), `build_start_replication_query`, `connect` with `replication=database`, `ensure_replication_slot` via `simple_query` (10 unit tests)
  - `source.rs`: Feature-gated `open()` connects to PostgreSQL, resolves slot's `confirmed_flush_lsn`; `close()` aborts connection handle; `connection_handle` field
  - `mod.rs`: Registered `postgres_io` module, added `password` config key
  - **Note**: WAL streaming (`START_REPLICATION` via `CopyBoth`) deferred — `tokio-postgres` 0.7 does not support `CopyBothDuplex`. Wire format + slot management are ready for future integration.

Previous session (2026-02-08):
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** - ALL 9 FEATURES COMPLETE
  - F-CKP-001 through F-CKP-009: Manifest, two-phase sink, coordinator, operator state, changelog wiring, WAL coordination, recovery manager, end-to-end tests, observability
  - Phase C gate: clippy clean, fmt clean, doc clean, 2,767+ lib tests + 12 integration tests

Previous session (2026-02-07):
- **F-OBS-001: Pipeline Observability API** - COMPLETE (23 new tests)
- **F-CONN-002D: RocksDB-Backed Persistent Table Store** - COMPLETE (10 new tests)
- **F-CONN-002C: PARTIAL Cache Mode & Xor Filter** - COMPLETE (40 new tests)
- **F-CONN-002B: Connector-Backed Table Population** - COMPLETE (19 new tests)
- **F-SQL-006: Window Frame (ROWS BETWEEN)** - COMPLETE (22 new tests)
- **F-SQL-005: Multi-Way JOIN Support** - COMPLETE (21 new tests)
- **F-SQL-004: HAVING Clause Execution** - COMPLETE (22 new tests)
- **F-CONN-003: Avro Serialization Hardening** - COMPLETE (~40 new tests)

### Where We Left Off

**Phase 3: 67/76 features COMPLETE (88%)**

All Phase 1 (12), Phase 1.5 (1), Phase 2 (34), and Unified Checkpoint (9) features are complete.
See [INDEX.md](./features/INDEX.md) for the full feature-by-feature breakdown.

**Test counts**: ~2,767 base, ~2,777+ with `rocksdb`, ~3,100+ with all feature flags (`kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `mysql-cdc`, `ffi`, `rocksdb`)

### Immediate Next Steps
1. F027 follow-ups: TLS support, initial snapshot, auto-reconnect (see plan in `postgres_io.rs` future work)
2. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
3. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
4. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)
1. Python bindings (`laminardb-python` repo): Update to use the new `api::Connection` methods
2. F027 follow-ups: TLS support, initial snapshot, auto-reconnect (see plan in `postgres_io.rs` future work)
3. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
4. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
5. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)

### Open Issues
- **tokio-postgres CopyBoth**: v0.7 lacks `CopyBothDuplex` for WAL streaming. Wire format parsing is ready; actual streaming awaits upstream support or a raw TCP approach.
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
