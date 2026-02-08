# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-08

### What Was Accomplished
- **pgwire-replication integration for PostgreSQL CDC WAL streaming** (PR #74, closes #58)
  - Integrated `pgwire-replication` v0.2 — native CopyBoth transport for logical replication, unblocking real-time CDC
  - `Cargo.toml`: Added `pgwire-replication` as optional dep, gated under `postgres-cdc` feature
  - `postgres_io.rs`: `connect()` is now a regular control-plane connection (removed `replication=database`); `ensure_replication_slot()` uses `pg_create_logical_replication_slot()` SQL function; new `build_replication_config()` maps `PostgresCdcConfig` → `pgwire_replication::ReplicationConfig`
  - `decoder.rs`: Made `pg_timestamp_to_unix_ms()` public for PG-epoch timestamp conversion
  - `source.rs`: Added `repl_client` field; `open()` connects pgwire-replication after slot management; `poll_batch()` calls `receive_replication_events()` for production path; `process_replication_event()` maps Begin/XLogData/Commit/KeepAlive to existing `process_wal_message()`; `close()` gracefully shuts down replication client
  - All 558 connector tests pass, clippy clean, docs clean

Previous session (2026-02-08):
- **FFI API Surface Update** — Exposed full LaminarDB API through `api::Connection` for Python bindings (PR #49)
- **F027 PostgreSQL CDC Replication I/O** (#44, PR #48) - MERGED
  - Wire format parsing, slot management, `connect` with `replication=database`
  - WAL streaming was deferred due to `tokio-postgres` CopyBoth limitation (now resolved via pgwire-replication)

Previous session (2026-02-08):
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)** - ALL 9 FEATURES COMPLETE

Previous session (2026-02-07):
- **F-OBS-001, F-CONN-002B/C/D, F-SQL-004/005/006, F-CONN-003** — all complete

### Where We Left Off

**Phase 3: 67/76 features COMPLETE (88%)**

All Phase 1 (12), Phase 1.5 (1), Phase 2 (34), and Unified Checkpoint (9) features are complete.
See [INDEX.md](./features/INDEX.md) for the full feature-by-feature breakdown.

**Test counts**: ~2,767 base, ~2,777+ with `rocksdb`, ~3,100+ with all feature flags (`kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `mysql-cdc`, `ffi`, `rocksdb`)

### Immediate Next Steps
1. F027 follow-ups: TLS cert path support for pgwire-replication, initial snapshot, auto-reconnect
2. Python bindings (`laminardb-python` repo): Update to use the new `api::Connection` methods
3. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
4. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
5. Remaining Phase 3 gaps (F029, F030, F033, F058, F061)

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
