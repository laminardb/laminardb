# Steering Document

> Last Updated: March 23, 2026

## Current Focus

**Overall: 213/249 features complete (86%)**

### Active Work

Phase 3 is nearly complete with 5 remaining features. Phase 6c server infrastructure is 80% done. Phase 5 admin features are 40% done.

| Feature | Priority | Phase | Notes |
|---------|----------|-------|-------|
| Redis Lookup Table (F030) | P1 | Phase 3 | Not started |
| Parquet Streaming Source (F033) | P2 | Phase 3 | Lookup source exists, streaming TBD |
| Async State Access (F058) | P2 | Phase 3 | Not started |
| Historical Backfill (F061) | P2 | Phase 3 | Not started |
| Protobuf Format Decoder (F-SCHEMA-008) | P2 | Phase 3 | Spec written |
| Unaligned Checkpoints (F-DCKP-006) | P2 | Phase 6c | Config structs only |
| redb Secondary Indexes (F-IDX-001) | P2 | Phase 6a | Spec only, no implementation |

### Recently Completed (not previously tracked)

| Feature | Phase | Notes |
|---------|-------|-------|
| MongoDB CDC Source (F029) | Phase 3 | Full change stream, resume tokens, testcontainers tests (4,507 LOC) |
| Delta Lake Recovery (F031B) | Phase 3 | Epoch skip, txn actions, conflict retry |
| Delta Lake Compaction (F031C) | Phase 3 | Adaptive interval, vacuum, background task |
| Delta Lake Schema Evolution (F031D) | Phase 3 | Diff engine, safe widening, wired to sink |
| Iceberg I/O Integration (F032A) | Phase 3 | REST/Glue/Hive catalogs, incremental mode |
| MmapStateStore (F-STATE-003) | Phase 6a | Full implementation (1,008 LOC) |
| All 6 SERVER features (F-SERVER-001 to 006) | Phase 6c | TOML config, engine, HTTP API, hot reload, delta mode, graceful shutdown |
| Transactional Sink 2PC (F-E2E-002) | Phase 6c | Kafka/Postgres/Delta/Files implementations |
| Idempotent Sink (F-E2E-003) | Phase 6c | Per-sink dedup strategies |
| Admin REST API (F046) | Phase 5 | Axum, 13 endpoints |
| Prometheus Export (F050) | Phase 5 | /metrics with 15+ counters |
| Health Checks (F052) | Phase 5 | /health + /ready |
| CLI Tools (F055) | Phase 5 | clap args, --validate-checkpoints |
| JSONB (F-POPT-008) | Perf | Binary JSON codec, O(log n) field access |
| SQL Plan Cache (F-POPT-009) | Perf | Compiled PhysicalExpr + cached plan fallback |

### Phase Status

- **Phase 3** (Connectors): 95/100 features complete (95%)
- **Phase 5** (Admin & Observability): 4/10 features complete (40%)
- **Phase 6a** (Partition-Parallel Embedded): 27/29 features complete (93%)
- **Phase 6b** (Delta Foundation): 14/14 features complete (100%)
- **Phase 6c** (Delta Production Hardening): 8/10 features complete (80%)
- **Perf Optimization**: 2/12 features complete (17%)

### Next Priorities

1. Complete remaining Phase 3 features (Redis lookup, Parquet streaming source)
2. Phase 4 security features can begin (no implementation exists)
3. Phase 5 remaining admin features (web dashboard, OTel, alerting)
4. Unaligned checkpoint state machine (F-DCKP-006)

---

## Completed Phases

### Phase 1: Core Engine -- COMPLETE (12/12)

Reactor, state stores, tumbling windows, DataFusion integration, WAL, checkpointing, event time processing, watermarks, EMIT clause, late data handling.

### Phase 1.5: Production SQL Parser -- COMPLETE (1/1)

129 parser tests covering all streaming SQL extensions.

### Phase 2: Production Hardening -- COMPLETE (38/38)

All window types, all join types, exactly-once sinks, two-phase commit, incremental checkpointing, Z-set changelog, cascading MVs, watermark variants. Per-core WAL removed (dead code — recovery uses manifest snapshots, not WAL replay). Thread-per-core superseded by StreamingCoordinator (PR #204). NUMA (`hwloc`) and `io_uring` feature flags removed — they were never wired into the code after the TPC removal.

### Phase 2.5: JIT Compiler -- REMOVED

Cranelift JIT compilation was implemented but removed. Compiled `PhysicalExpr`
projections in `StreamExecutor` provide the same zero-overhead per-cycle benefit
without a JIT dependency.

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
| Lookup join execution | Hash-indexed snapshot | Arrow RowConverter key encoding, pre-filtered at plan time | -- |
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
- Recovery integration tests (6 tests covering all recovery paths)

### Remaining

- [ ] Phase 4 security features -- no implementation exists (crate stubs deleted)
- [ ] Phase 5 remaining: web dashboard, OTel, alerting, SQL console, advanced config
- [ ] Performance optimization features (10 remaining of 12 identified)
- [ ] Unaligned checkpoint state machine (F-DCKP-006)
- [ ] redb secondary indexes (F-IDX-001) -- spec only

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
| Phase 3 Complete | TBD | 95% |
| Phase 6c Server | TBD | 80% |
| Phase 4 Security | TBD | Planned |
| Phase 5 Admin | TBD | 40% |
| First public release | TBD | Planned |
