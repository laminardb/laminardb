# Session Context

> This file tracks session continuity. Update at the end of each session.
> For historical details, see [CONTEXT_ARCHIVE.md](./CONTEXT_ARCHIVE.md)
> For feature tracking, see [INDEX.md](./features/INDEX.md)

## Last Session

**Date**: 2026-02-07

### What Was Accomplished
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

**Phase 3: 53/67 features COMPLETE (79%)**

All Phase 1 (12), Phase 1.5 (1), and Phase 2 (34) features are complete.
See [INDEX.md](./features/INDEX.md) for the full feature-by-feature breakdown.

**Test counts**: ~2,641 base, ~2,651+ with `rocksdb`, ~2,980+ with all feature flags (`kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `mysql-cdc`, `ffi`, `rocksdb`)

### Immediate Next Steps
1. F-OBS-001: Pipeline Observability API
2. F031B/C/D: Delta Lake advanced (recovery, compaction, schema evolution)
3. F032A: Iceberg I/O (blocked by iceberg-rust DF 52.0 compat)
4. F-DEMO-001/002/003: Production demo pipeline + TUI

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

laminar-db/src/
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
