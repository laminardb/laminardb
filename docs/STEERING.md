# Steering Document

> Last Updated: January 2026

## Current Focus

**Phase 2 Production Hardening** - Thread-per-core, advanced windows, checkpointing

### Sprint Priority: Phase 2 In Progress

Phase 1 P0 hardening is complete. Phase 2 is underway with 7/17 features complete.

**Completed**: F013 (Thread-Per-Core), F014 (SPSC), F015 (CPU Pinning), F016 (Sliding Windows), F018 (Hopping), F019 (Stream-Stream Joins), F020 (Lookup Joins)

**Next Priority**:
1. F059 (FIRST/LAST Value Aggregates) - P0, essential for OHLC
2. F022 (Incremental Checkpointing) - P1, async checkpoint + RocksDB
3. F062 (Per-Core WAL) - P1, required for F013 integration

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
| 6 | Per-core WAL segments | **F062** | 3-5 days | 📝 Spec | Required for F013 (thread-per-core) |
| 7 | Async checkpointing | **F022** | 1-2 weeks | 📝 Spec | Blocks Ring 0 currently |
| 8 | MAP_PRIVATE for checkpoints | F002 | 2-3 days | 📝 Draft | CoW isolation for snapshots |
| 9 | io_uring integration | F001/F007 | 3-5 days | 📝 Draft | Blocking I/O on hot path |

**New ADR**: [ADR-004: Checkpoint Strategy](adr/ADR-004-checkpoint-strategy.md) documents the three-tier checkpoint architecture decision.

### Audit Summary

| Category | Status |
|----------|--------|
| Features Audited | 12 |
| Fully Implemented | 5 |
| Partial (gaps found) | 6 |
| Needs Work | 1 |

**Full audit report**: See `docs/PHASE1_AUDIT.md`

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
| io_uring crate | tokio-uring vs io_uring | Phase 2 Week 1 | TBD |
| Retraction model | Z-set vs Flink changelog | Phase 2 Week 2 | TBD |

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

### High (P1)

- [ ] **Production SQL Parser** - Current F006 is POC only (see ADR-003, F006B spec)
- [ ] **Async checkpointing** - See [F022: Incremental Checkpointing](features/phase-2/F022-incremental-checkpointing.md)
- [ ] **Per-core WAL** - See [F062: Per-Core WAL Segments](features/phase-2/F062-per-core-wal.md)
- [ ] **io_uring integration** - Blocking I/O on hot path (no spec yet)

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
| Phase 2 Start | 2026-01-24 | ✅ Started (7/17 features complete) |
| Phase 2 P0 Features | TBD | 🚧 In Progress |
| First public demo | TBD | 📋 Planned |

---

## Communication

- **Daily**: Quick status in CONTEXT.md
- **Weekly**: Update STEERING.md priorities
- **Phase End**: Full retrospective and planning
