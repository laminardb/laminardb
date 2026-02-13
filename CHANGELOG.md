# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive project documentation (README, ARCHITECTURE, CONTRIBUTING, crate READMEs)
- PR template and SECURITY.md

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
