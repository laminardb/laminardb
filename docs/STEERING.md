# Steering Document

> Last Updated: January 2026

## Current Focus

**Phase 1 Hardening** - Critical fixes before Phase 2

### Sprint Priority: Phase 1 Hardening

Phase 1 features are functionally complete, but a comprehensive audit against 2025-2026 best practices identified **critical gaps** that must be fixed before Phase 2.

#### P0 - Must Fix Before Phase 2

| # | Issue | Feature | Effort | Impact |
|---|-------|---------|--------|--------|
| 1 | **WAL uses fsync not fdatasync** | F007 | 1 hour | 50-100μs/sync wasted on metadata |
| 2 | **No CRC32 checksum in WAL** | F007 | 4 hours | Cannot detect corruption |
| 3 | **No torn write detection** | F007 | 4 hours | Crash recovery may fail |
| 4 | **Watermark not persisted in WAL** | F010 | 4 hours | Recovery loses watermark progress |
| 5 | **No recovery integration test** | F007/F008 | 1 day | Untested critical path |

#### P1 - Early Phase 2

| # | Issue | Feature | Effort | Impact |
|---|-------|---------|--------|--------|
| 6 | Per-core WAL segments | F007 | 2-3 days | Required for F013 (thread-per-core) |
| 7 | Async checkpointing | F008 | 2-3 days | Blocks Ring 0 currently |
| 8 | MAP_PRIVATE for checkpoints | F002 | 2-3 days | CoW isolation for snapshots |
| 9 | io_uring integration | F001/F007 | 3-5 days | Blocking I/O on hot path |

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
| Phase 2 Start | P0 hardening fixes | Complete 5 critical items |
| Benchmarking | CI setup | Set up benchmark CI |
| Performance validation | No benchmarks exist | Add criterion benchmarks |

---

## Technical Debt

### Critical (P0)

- [x] ~~Phase 1 features complete~~
- [ ] **WAL durability fixes** - fsync→fdatasync, CRC32, torn write detection
- [ ] **Watermark persistence** - Store in WAL for recovery
- [ ] **Recovery integration test** - Full checkpoint + WAL replay verification

### High (P1)

- [ ] **Production SQL Parser** - Current F006 is POC only (see ADR-003)
- [ ] **Async checkpointing** - Currently blocks Ring 0
- [ ] **io_uring integration** - Blocking I/O on hot path
- [ ] **Per-core WAL** - Required for thread-per-core (F013)

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
| Phase 1 Hardening Complete | TBD | 🚧 In Progress |
| Phase 2 Start | TBD | ⏳ Blocked on hardening |
| First public demo | TBD | 📋 Planned |

---

## Communication

- **Daily**: Quick status in CONTEXT.md
- **Weekly**: Update STEERING.md priorities
- **Phase End**: Full retrospective and planning
