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

Previous session (2026-02-08):
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** - ALL 9 FEATURES COMPLETE
  - **F-CKP-001**: Checkpoint Manifest & Store — `CheckpointManifest`, `ConnectorCheckpoint`, `OperatorCheckpoint`, `FileSystemCheckpointStore` with atomic writes (laminar-storage)
  - **F-CKP-002**: Two-Phase Sink Protocol — `pre_commit()` added to `SinkConnector` trait, implemented for Kafka/PG/Delta/Iceberg sinks, `SinkConnectorCapabilities::with_two_phase_commit()`
  - **F-CKP-003**: Checkpoint Coordinator — `CheckpointCoordinator` with full checkpoint cycle: source snapshot → sink pre-commit → manifest persist → sink commit/rollback
  - **F-CKP-004**: Operator State Persistence — `manifest_operators_to_dag_states()` and `dag_snapshot_to_manifest_operators()` conversion functions
  - **F-CKP-005**: Changelog Buffer Wiring — `ChangelogAwareStore<S>` wrapper (Ring 0, ~2-5ns), `ChangelogSink` trait, `ChangelogDrainer` (Ring 1)
  - **F-CKP-006**: WAL Checkpoint Coordination — `WalPrepareResult`, `prepare_wal_for_checkpoint()`, `truncate_wal_after_checkpoint()`
  - **F-CKP-007**: Unified Recovery Manager — `RecoveryManager` loads manifest, restores sources/sinks/tables, `RecoveredState` with error tracking
  - **F-CKP-008**: End-to-End Recovery Tests — 12 integration tests covering happy path, fresh start, multiple checkpoints, offsets, operator state, WAL, epoch resume, incremental, prune, JSON round-trip
  - **F-CKP-009**: Checkpoint Observability — `checkpoints_completed/failed`, `last_checkpoint_duration_ms`, `checkpoint_epoch` in `PipelineCounters`
  - Fixed lakehouse delta/iceberg tests broken by F-CKP-002 two-phase commit refactor
  - Phase C gate: clippy clean, fmt clean, doc clean (0 warnings), 2,767+ lib tests + 12 integration tests all passing

Previous session (2026-02-07):
- **F-OBS-001: Pipeline Observability API** - COMPLETE (23 new tests, 253 laminar-db tests total)
  - `metrics.rs` (NEW): `PipelineState`, `PipelineCounters` (atomic), `PipelineMetrics`, `SourceMetrics`, `StreamMetrics`, `CounterSnapshot`, `is_backpressured()`, `utilization()` (10 unit tests)
  - `db.rs`: Added `counters: Arc<PipelineCounters>`, `start_time: Instant` fields; 7 public API methods: `metrics()`, `source_metrics()`, `all_source_metrics()`, `stream_metrics()`, `all_stream_metrics()`, `total_events_processed()`, `counters()`; instrumented both `start_embedded_pipeline` and `start_connector_pipeline` with counter increments for events_ingested/emitted/cycles/batches + cycle timing (13 integration tests)
  - `handle.rs`: Added `capacity()`, `is_backpressured()` to `SourceHandle<T>` and `pending()`, `capacity()`, `is_backpressured()` to `UntypedSourceHandle`
  - `catalog.rs`: Added `get_stream_entry()` for stream metrics access
  - `lib.rs`: Added `mod metrics;` and re-exports for all public metric types

Previous session (2026-02-07):
- **F-CONN-002D: RocksDB-Backed Persistent Table Store** - COMPLETE (10 new tests, 223 laminar-db tests with rocksdb)
  - `table_backend.rs` (NEW): `TableBackend` enum (InMemory/Persistent), Arrow IPC serde, `RocksDB` config (bloom filter, LZ4, block-based table), `open_rocksdb_for_tables()` (8 tests)
  - `table_provider.rs` (NEW): `ReferenceTableProvider` implementing DataFusion `TableProvider` — live scan from `TableStore`, no re-registration needed (5 tests)
  - `table_store.rs`: Refactored to use `TableBackend`, `row_count` tracking, `new_with_rocksdb()`, `create_table_persistent()`, `maybe_spill_to_rocksdb()`, `checkpoint_rocksdb()`, `is_persistent()`, `drop_table()` drops CF (7 new rocksdb tests)
  - `db.rs`: DDL `WITH (storage = 'persistent')` parsing, `ReferenceTableProvider` registration, persistent table skip in `sync_table_to_datafusion()`
  - `connector_manager.rs`: Added `storage` field to `TableRegistration`
  - `error.rs`: Added `Storage(String)` variant
  - `pipeline_checkpoint.rs`: Added `table_store_checkpoint_path` field
  - `config.rs`: Added `table_spill_threshold` (default 1,000,000)
  - `Cargo.toml`: `rocksdb` feature flag, `arrow-ipc` dep
  - Platform: Block-based table (not PlainTable — Windows compat), `set_use_direct_reads` Linux-only, `Arc<parking_lot::Mutex<rocksdb::DB>>` for CF management

Previous session (2026-02-07):
- **F-CONN-002C: PARTIAL Cache Mode & Xor Filter** - COMPLETE (40 new tests)
- **F-CONN-002B: Connector-Backed Table Population** - COMPLETE (19 new tests)
- **F-SQL-006: Window Frame (ROWS BETWEEN)** - COMPLETE (22 new tests)
- **F-SQL-005: Multi-Way JOIN Support** - COMPLETE (21 new tests)
- **F-SQL-004: HAVING Clause Execution** - COMPLETE (22 new tests)
- **F-CONN-003: Avro Serialization Hardening** - COMPLETE (~40 new tests)

### Where We Left Off

**Phase 3: 63/67 features COMPLETE (94%)**

All Phase 1 (12), Phase 1.5 (1), Phase 2 (34), and Unified Checkpoint (9) features are complete.
See [INDEX.md](./features/INDEX.md) for the full feature-by-feature breakdown.

**Test counts**: ~2,767 base, ~2,777+ with `rocksdb`, ~3,100+ with all feature flags (`kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `mysql-cdc`, `ffi`, `rocksdb`)

### Immediate Next Steps
1. Python bindings (`laminardb-python` repo): Update to use the new `api::Connection` methods
2. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
3. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
4. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)

### Open Issues
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
  cdc/postgres/   # CDC source (pgoutput decoder, Z-set changelog)
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
