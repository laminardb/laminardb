# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.16.0] - 2026-02-28

### Added
- **Stateful Streaming SQL (F-SSQL-000 through F-SSQL-006)**:
  - Streaming aggregation hardening with incremental per-window accumulators
  - StreamExecutor state checkpoint integration
  - Ring 0 SQL operator routing for tumbling, hopping, and session windows
  - Streaming physical optimizer rule (`StreamingPhysicalValidator`)
  - DataFusion cooperative scheduling integration
  - Dynamic watermark filter pushdown (`WatermarkDynamicFilter`)
- **SQL audit P2 fixes**:
  - SHOW SOURCES/SINKS/STREAMS with metadata columns
  - SHOW CREATE SOURCE/SINK for DDL reconstruction
  - Window offset support for timezone-aligned tumble/hop
  - EXPLAIN ANALYZE with execution metrics
  - ASOF NEAREST join direction
- **Unified error handling** with structured `LDB-NNNN` error codes across 8 code ranges
- Zero-alloc `HotPathError` enum for Ring 0 (2 bytes, `Copy`)
- `DbError::code()` and `DbError::is_transient()` for programmatic error handling
- Binance WebSocket streaming SQL demo (`examples/binance-ws/`)
- Event-driven fan-out/fan-in pipeline architecture
- End-to-end barrier wiring and checkpoint operational maturity
- Benchmark baselines document (`docs/BENCHMARKS.md`)

### Fixed
- QueueFull panic in throughput benchmark
- WAL guard against OOM on corrupted entry length (MAX_WAL_ENTRY_SIZE validation)
- Sink integration audit fixes
- Identifier normalization disabled for mixed-case column support

### Changed
- Replaced std HashMap with FxHashMap/AHashMap on hot paths in laminar-db

## [0.15.0] - 2026-02-22

### Added
- Comprehensive project documentation (README, ARCHITECTURE, CONTRIBUTING, crate READMEs)
- PR template and SECURITY.md
- Interactive data flow playground on docs website
- WebSocket source and sink connectors registered in connector registry
- MySQL CDC and Iceberg connectors registered in connector registry

### Fixed
- `cancel_query()` now returns an error for nonexistent query IDs
- Mobile responsiveness for data flow playground
- Windows CI protoc installation (switched to arduino/setup-protoc)
- Cross-compilation protoc availability in containers

### Changed
- Removed ~460 lines of boilerplate via macros and dead code deletion

## [0.14.0] - 2026-02-18

### Added
- Docs and website redesign with interactive playground
- Schema inference for connectors
- Multi-partition streaming scans

### Fixed
- SELECT on streaming sources now works correctly
- Manual `checkpoint()` wired to disk persistence
- Connector config specs aligned with `from_config()` implementations

## [0.13.0] - 2026-02-14

### Added
- Deferred architectural fixes
- EXPLAIN formatting improvements
- Per-operator metrics
- SQL fixes and improvements

### Fixed
- `EMIT ON WINDOW CLOSE` in embedded SQL executor
- Cross-build failures for musl and aarch64 targets

### Changed
- Major dependency upgrades (sysinfo 0.33→0.38)

## [0.12.0] - 2026-02-11

### Added
- **SQL Compiler Integration (F083-F089)**: End-to-end JIT compilation pipeline
  - Batch Row Reader for Arrow RecordBatch to EventRow conversion
  - SQL Compiler Orchestrator with `compile_streaming_query()` entry point
  - Adaptive compilation warmup with transparent interpreted-to-compiled swap
  - Compiled stateful pipeline bridge for multi-segment execution
  - Schema-aware event time extraction
  - Compilation metrics and observability
- **Plan Compiler Core (F078-F082)**: Cranelift-based JIT compiler stack
  - EventRow format for zero-allocation row representation
  - Cranelift expression compiler with constant folding
  - Pipeline extraction, compilation, and caching
  - Ring 0/Ring 1 SPSC bridge
  - StreamingQuery lifecycle management
- **Unified Checkpoint System (F-CKP-001 through F-CKP-009)**
  - Checkpoint manifest and store
  - Two-phase sink protocol
  - Unified checkpoint coordinator
  - Operator state persistence
  - WAL checkpoint coordination
  - Unified recovery manager
  - End-to-end recovery tests
  - Checkpoint observability

### Changed
- Updated utoipa from 4.1 to 5.4
- Updated toml from 0.9 to 1.0
- Updated crossterm from 0.28 to 0.29

## [0.11.0] - 2026-02-08

### Added
- pgwire-replication integration for PostgreSQL CDC WAL streaming
- MySQL CDC I/O integration (F028A) with mysql_async binlog support
- Delta Lake I/O integration (F031A) with object_store backend
- Connector-backed table population (F-CONN-002B)
- PARTIAL cache mode with xor filter (F-CONN-002C)
- RocksDB-backed persistent table store (F-CONN-002D)
- Avro serialization hardening (F-CONN-003)
- Cloud storage infrastructure (credential resolver, config validation, secret masking)
- FFI API module with Arrow C Data Interface and async callbacks
- Pipeline observability API (F-OBS-001)
- Market data TUI demo with Ratatui
- DAG pipeline architecture (topology, multicast, routing, executor, checkpointing)
- Reactive subscription system (change events, notification slots, dispatcher)
- Connector SDK (retry, rate limiting, circuit breaker, test harness)
- Serde format implementations (JSON, CSV, raw, Debezium, Avro)

## [0.10.0] - 2026-01-24

### Added
- Phase 2 Production Hardening (38 features)
  - Thread-per-core architecture with CPU pinning
  - SPSC queue communication
  - Sliding, hopping, and session windows
  - Stream-stream, ASOF, temporal, and lookup joins
  - Incremental checkpointing with RocksDB backend
  - Exactly-once sinks with two-phase commit
  - Per-core WAL segments
  - io_uring advanced optimization (Linux)
  - NUMA-aware memory allocation
  - Three-ring I/O architecture
  - Task budget enforcement
  - Zero-allocation enforcement
  - XDP/eBPF network optimization (Linux)
  - Changelog/retraction with Z-sets
  - Cascading materialized views
  - Per-partition and keyed watermarks
  - Watermark alignment groups
  - Advanced DataFusion integration
  - Composite aggregator with f64 support
  - DataFusion aggregate bridge
  - Retractable FIRST/LAST accumulators
  - Extended aggregation parser

## [0.1.0] - 2026-01-01

### Added
- Phase 1 Core Engine (12 features)
  - Core reactor event loop
  - Memory-mapped state store
  - State store interface
  - Tumbling windows
  - DataFusion integration
  - Basic SQL parser
  - Write-ahead log with CRC32C checksums
  - Basic checkpointing
  - Event time processing
  - Watermarks
  - EMIT clause
  - Late data handling
- Production SQL parser (F006B) with 129 tests
