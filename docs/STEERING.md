# Steering Document

> Last Updated: January 2026

## Current Focus

**Phase 2 Production Hardening** - ✅ **COMPLETE** (29/29 features)

### Sprint Priority: Phase 3 - Connectors & Integration

Phase 2 Production Hardening is complete with all 29 features implemented.

**Phase 2 Completed**: F013 (Thread-Per-Core), F014 (SPSC), F015 (CPU Pinning), F016 (Sliding Windows), F017 (Session Windows), F018 (Hopping), F019 (Stream-Stream Joins), F020 (Lookup Joins), F021 (Temporal Joins), F067 (io_uring), F068 (NUMA), F071 (Zero-Alloc), F011B (EMIT Extension), F063 (Changelog/Retraction), F059 (FIRST/LAST), F023 (Exactly-Once Sinks), F022 (Incremental Checkpointing), F062 (Per-Core WAL), F069 (Three-Ring I/O), F070 (Task Budget), F073 (Zero-Alloc Polling), F060 (Cascading MVs), F056 (ASOF Joins), F064 (Per-Partition Watermarks), F065 (Keyed Watermarks), F024 (Two-Phase Commit), F066 (Watermark Alignment Groups), F072 (XDP/eBPF)

**Next Priority** (updated based on research reviews):

**Thread-Per-Core Optimizations**:
1. ~~**F071 (Zero-Allocation Enforcement)**~~ - ✅ COMPLETE
2. ~~**F067 (io_uring Advanced)**~~ - ✅ COMPLETE
3. ~~**F068 (NUMA-Aware Memory)**~~ - ✅ COMPLETE
4. ~~**F070 (Task Budget Enforcement)**~~ - ✅ COMPLETE - latency SLA guarantees
5. ~~**F069 (Three-Ring I/O)**~~ - ✅ COMPLETE - latency/main/poll ring separation

**Emit & Checkpoint**:
6. ~~**F011B (EMIT Clause Extension)**~~ - ✅ COMPLETE - OnWindowClose/Changelog/Final
7. ~~**F063 (Changelog/Retraction)**~~ - ✅ COMPLETE - Z-set foundation, unblocks F023, F060
8. ~~**F059 (FIRST/LAST Value Aggregates)**~~ - ✅ COMPLETE - Essential for OHLC, unblocks F060
9. ~~**F023 (Exactly-Once Sinks)**~~ - ✅ COMPLETE - Transactional + idempotent sinks
10. ~~**F022 (Incremental Checkpointing)**~~ - ✅ COMPLETE - RocksDB backend, SPSC changelog
11. ~~**F062 (Per-Core WAL)**~~ - ✅ COMPLETE - lock-free per-core writers, epoch ordering

### Phase 1 Hardening (Complete)

Phase 1 features are functionally complete. A comprehensive audit against 2025-2026 best practices identified critical gaps that have been fixed.

#### P0 - Must Fix Before Phase 2

| # | Issue | Feature | Effort | Impact |
|---|-------|---------|--------|--------|
| 1 | **WAL uses fsync not fdatasync** | F007 | 1 hour | 50-100μs/sync wasted on metadata |
| 2 | **No CRC32 checksum in WAL** | F007 | 4 hours | Cannot detect corruption |
| 3 | **No torn write detection** | F007 | 4 hours | Crash recovery may fail |
| 4 | **Watermark not persisted in WAL** | F010 | 4 hours | Recovery loses watermark progress |
| 5 | **No recovery integration test** | F007/F008 | 1 day | Untested critical path |

#### P1 - Early Phase 2

| # | Issue | Feature | Effort | Status | Impact |
|---|-------|---------|--------|--------|--------|
| 6 | Per-core WAL segments | **F062** | 3-5 days | ✅ Done | Required for F013 (thread-per-core) |
| 7 | Async checkpointing | **F022** | 1-2 weeks | ✅ Done | Blocks Ring 0 currently |
| 8 | MAP_PRIVATE for checkpoints | F002 | 2-3 days | 📝 Draft | CoW isolation for snapshots |
| 9 | io_uring integration | F001/F007 | 3-5 days | ✅ Done | Blocking I/O on hot path |

**New ADR**: [ADR-004: Checkpoint Strategy](adr/ADR-004-checkpoint-strategy.md) documents the three-tier checkpoint architecture decision.

### Audit Summary

| Category | Status |
|----------|--------|
| Features Audited | 12 |
| Fully Implemented | 5 |
| Partial (gaps found) | 6 |
| Needs Work | 1 |

**Full audit report**: All P0 items resolved.

---

## Active Decisions

### Decided

| Decision | Choice | Rationale | ADR |
|----------|--------|-----------|-----|
| Hash map implementation | FxHashMap | Faster than std HashMap for small keys | ADR-001 |
| Serialization format | rkyv | Zero-copy deserialization, ~1.2ns access | ADR-002 |
| SQL parser strategy | Extend sqlparser-rs | DataFusion compatible, streaming extensions | ADR-003 |
| Checkpoint strategy | Three-tier hybrid | Ring 0 changelog, Ring 1 WAL+RocksDB | ADR-004 |
| Async runtime | tokio (Ring 1 only) | Industry standard, not on hot path | - |
| WAL format | Custom (keep) | Simpler than RocksDB WAL, add checksums | - |

### Pending

| Decision | Options | Deadline | Owner |
|----------|---------|----------|-------|
| io_uring crate | tokio-uring vs io_uring | Phase 2 | TBD |
| NUMA crate | libc vs numa crate | Phase 2 | TBD |
| ~~Retraction model~~ | ~~Z-set vs Flink changelog~~ | ~~Phase 2 Week 2~~ | ✅ **Decided: Z-set** (F063) |

**Notes**:
- Retraction model decided: DBSP/Feldera Z-sets chosen for mathematical foundation. See [F063: Changelog/Retraction](features/phase-2/F063-changelog-retraction.md).
- io_uring and NUMA implementation details specified in [F067](features/phase-2/F067-io-uring-optimization.md) and [F068](features/phase-2/F068-numa-aware-memory.md) respectively.

---

## Blocked Items

| Item | Blocker | Unblock Action |
|------|---------|----------------|
| ~~Phase 2 Start~~ | ~~P0 hardening fixes~~ | ✅ Complete - 5 critical items done |
| Benchmarking | CI setup | Set up benchmark CI |
| Performance validation | No benchmarks exist | Add criterion benchmarks |

---

## Technical Debt

### Critical (P0) - ✅ ALL COMPLETE

- [x] ~~Phase 1 features complete~~
- [x] ~~WAL durability fixes~~ - fdatasync, CRC32C, torn write detection
- [x] ~~Watermark persistence~~ - In WAL commits and checkpoint metadata
- [x] ~~Recovery integration test~~ - 6 comprehensive tests in wal_state_store.rs

### High (P0/P1) - Thread-Per-Core Research Gaps

- [ ] **Zero-allocation enforcement** - See [F071: Zero-Allocation Enforcement](features/phase-2/F071-zero-allocation-enforcement.md)
- [ ] **io_uring advanced** - See [F067: io_uring Advanced Optimization](features/phase-2/F067-io-uring-optimization.md)
- [ ] **NUMA awareness** - See [F068: NUMA-Aware Memory Allocation](features/phase-2/F068-numa-aware-memory.md)
- [ ] **Task budgeting** - See [F070: Task Budget Enforcement](features/phase-2/F070-task-budget-enforcement.md)
- [ ] **Three-ring I/O** - See [F069: Three-Ring I/O Architecture](features/phase-2/F069-three-ring-io.md)

### High (P1)

- [ ] **Production SQL Parser** - Current F006 is POC only (see ADR-003, F006B spec)
- [ ] **Async checkpointing** - See [F022: Incremental Checkpointing](features/phase-2/F022-incremental-checkpointing.md)
- [ ] **Per-core WAL** - See [F062: Per-Core WAL Segments](features/phase-2/F062-per-core-wal.md)

### Medium (P2)

- [ ] Add property-based tests for serialization
- [ ] Document unsafe blocks in core crate
- [ ] Add tracing spans for debugging
- [ ] Prefix scan optimization (currently O(n))
- [ ] madvise hints for mmap
- [ ] Huge page support for large state

---

## Performance Validation Status

| Metric | Target | Verified | Action |
|--------|--------|----------|--------|
| State lookup | < 500ns | ⚠️ Claimed | Add benchmark |
| Throughput/core | 500K/sec | ❌ Not tested | Add benchmark |
| p99 latency | < 10μs | ❌ Not tested | Add histogram |
| Checkpoint recovery | < 10s | ❌ Not tested | Add benchmark |

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

## Upcoming Milestones

| Milestone | Target Date | Status |
|-----------|-------------|--------|
| Phase 1 Features Complete | 2026-01-24 | ✅ Done |
| Phase 1 P0 Hardening Complete | 2026-01-24 | ✅ Done |
| Phase 2 Start | 2026-01-24 | ✅ Started (7/28 features complete) |
| Phase 2 P0 Features | TBD | 🚧 In Progress |
| First public demo | TBD | 📋 Planned |

---

## Communication

- **Weekly**: Update STEERING.md priorities
- **Phase End**: Full retrospective and planning
