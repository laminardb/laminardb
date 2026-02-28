# Steering Document

> Last Updated: February 28, 2026

## Current Focus

**Phase 3 Connectors & Integration** -- 85% complete (85/100 features)

### Active Work

Phase 3 is nearly complete. The remaining 15 features are all in Draft status:

| Feature | Priority | Notes |
|---------|----------|-------|
| MongoDB CDC Source (F029) | P2 | Not started |
| Redis Lookup Table (F030) | P1 | Not started |
| Delta Lake Recovery (F031B) | P1 | Spec written |
| Delta Lake Compaction (F031C) | P1 | Spec written |
| Delta Lake Schema Evolution (F031D) | P1 | Spec written |
| Iceberg I/O Integration (F032A) | P1 | Blocked by iceberg-datafusion DF 52.0 compat |
| Parquet File Source (F033) | P2 | Not started |
| Async State Access (F058) | P2 | Not started |
| Historical Backfill (F061) | P2 | Not started |
| Protobuf Format Decoder (F-SCHEMA-008) | P2 | Spec written |
| Stateful Streaming SQL (F-SSQL-000 through F-SSQL-006) | -- | Complete (merged PR #128) |
| SQL audit P2 (SHOW, EXPLAIN ANALYZE, ASOF NEAREST) | -- | Complete (merged PR #136) |
| Unified error handling with LDB-NNNN codes | -- | Complete (merged PR #134) |

### Phase 6 Status

- **Phase 6a** (Partition-Parallel Embedded): 27/29 features complete (93%)
- **Phase 6b** (Delta Foundation): 14/14 features complete (100%)
- **Phase 6c** (Delta Production Hardening): 0/10 features (planned)

### Next Priorities

1. Complete remaining Phase 3 P1 features (Delta Lake advanced, Redis lookup)
2. Begin Phase 6c server infrastructure (TOML config, engine construction, HTTP API)
3. Phase 4/5 security and admin features can begin in parallel

---

## Completed Phases

### Phase 1: Core Engine -- COMPLETE (12/12)

Reactor, state stores, tumbling windows, DataFusion integration, WAL, checkpointing, event time processing, watermarks, EMIT clause, late data handling.

### Phase 1.5: Production SQL Parser -- COMPLETE (1/1)

129 parser tests covering all streaming SQL extensions.

### Phase 2: Production Hardening -- COMPLETE (38/38)

Thread-per-core, all window types, all join types, exactly-once sinks, two-phase commit, per-core WAL, incremental checkpointing, NUMA, io_uring, Z-set changelog, cascading MVs, watermark variants, JIT groundwork.

### Phase 2.5: JIT Compiler -- COMPLETE (12/12)

Cranelift JIT compilation, adaptive compilation warmup, compiled stateful pipeline bridge.

### Phase 6b: Delta Foundation -- COMPLETE (14/14)

Gossip discovery, Raft consensus, partition ownership, distributed checkpoints, gRPC services, cross-node aggregation.

---

## Active Decisions

### Decided

| Decision | Choice | Rationale | ADR |
|----------|--------|-----------|-----|
| Hash map implementation | AHashMap (Ring 0), FxHashMap (general) | Faster for small keys, DoS-resistant | ADR-001 |
| Serialization format | rkyv | Zero-copy deserialization, ~1.2ns access | ADR-002 |
| SQL parser strategy | Extend sqlparser-rs | DataFusion compatible, streaming extensions | ADR-003 |
| Checkpoint strategy | Three-tier hybrid | Ring 0 changelog, Ring 1 WAL + directory snapshots | ADR-004 |
| Async runtime | tokio (Ring 1 only) | Industry standard, not on hot path | -- |
| WAL format | Custom (keep) | Simpler than RocksDB WAL, CRC32C checksums | -- |
| Retraction model | Z-sets (DBSP/Feldera) | Mathematical foundation for correctness | F063 |
| Schema trait architecture | Extensible connector traits | Pluggable format decoders, schema inference | ADR-006 |
| Distributed coordination | openraft + chitchat | Proven Raft for consensus, gossip for discovery | -- |
| Cache implementation | foyer | S3-FIFO eviction, serde for HybridCache | -- |
| Secondary indexes | redb | Embedded B-tree, no external dependencies | -- |
| Concurrent HashMap | papaya | Lock-free, better than dashmap for cross-partition | -- |

---

## Performance Validation Status

| Metric | Target | Validated | Benchmark |
|--------|--------|-----------|-----------|
| State lookup | < 500ns | Benchmarks exist | `state_bench` |
| Throughput/core | 500K/sec | Benchmarks exist | `throughput_bench` |
| p99 latency | < 10us | Benchmarks exist | `latency_bench` |
| Checkpoint recovery | < 10s | Integration tests | `checkpoint_bench` |
| Window trigger | < 10us | Benchmarks exist | `window_bench` |
| Join throughput | -- | Benchmarks exist | `join_bench`, `lookup_join_bench` |
| Cache performance | -- | Benchmarks exist | `cache_bench` |
| JIT compilation | -- | Benchmarks exist | `compiler_bench` |

17 benchmark suites exist in `laminar-core`, 2 in `laminar-storage`.

---

## Technical Debt

### Resolved (P0)

All Phase 1 P0 issues resolved:
- WAL durability fixes (fdatasync, CRC32C, torn write detection)
- Watermark persistence in WAL commits and checkpoint metadata
- Recovery integration tests (6 comprehensive tests)

### Remaining

- [ ] Phase 4/5 crate stubs are empty (auth, admin, observe) -- implementation planned
- [ ] Server binary is skeleton only -- Phase 6c will implement
- [ ] Iceberg I/O integration blocked by iceberg-datafusion compatibility
- [ ] Performance optimization features (12 identified, all planned)

---

## Team Agreements

### Code Review

- All Ring 0 code requires performance review
- Security code requires security review
- Architecture changes need architect sign-off

### Definition of Done

- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests if applicable
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG entry added

---

## Milestones

| Milestone | Target Date | Status |
|-----------|-------------|--------|
| Phase 1 Complete | 2026-01-24 | Done |
| Phase 1 P0 Hardening | 2026-01-24 | Done |
| Phase 2 Complete | 2026-01-31 | Done |
| Phase 2.5 JIT Complete | 2026-02-03 | Done |
| Phase 6a/6b Complete | 2026-02-15 | Done (6b), 93% (6a) |
| Phase 3 Complete | TBD | 85% |
| Phase 6c Server | TBD | Planned |
| Phase 4 Security | TBD | Planned |
| Phase 5 Admin | TBD | Planned |
| First public release | TBD | Planned |
