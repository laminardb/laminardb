# Steering Document

> Last Updated: January 2026

## Current Focus

**Phase 1: Core Engine** - Building the foundational streaming engine

### Sprint Priority

1. **F007 - Write-Ahead Log** (P1)
   - Durability layer for state stores
   - Must support group commit and recovery

2. **F008 - Basic Checkpointing** (P1)
   - Periodic state snapshots
   - Target: < 10s recovery time

3. **F009 - Event Time Processing** (P1)
   - Time-based event semantics
   - Foundation for watermarks

## Active Decisions

### Decided

| Decision | Choice | Rationale | ADR |
|----------|--------|-----------|-----|
| Hash map implementation | FxHashMap | Faster than std HashMap for small keys | ADR-001 |
| Serialization format | rkyv | Zero-copy deserialization, ~1.2ns access | ADR-002 |
| Async runtime | tokio (Ring 1 only) | Industry standard, not on hot path | - |

### Pending

| Decision | Options | Deadline | Owner |
|----------|---------|----------|-------|
| WAL format | Custom vs RocksDB | End of Phase 1 | TBD |
| Timer wheel implementation | Hierarchical vs Hashed | Week 2 | TBD |

## Blocked Items

| Item | Blocker | Unblock Action |
|------|---------|----------------|
| Benchmarking | CI setup | Set up benchmark CI |

*F003 State Store - COMPLETED (was blocked by F001)*

## Technical Debt

- [ ] **CRITICAL: Production SQL Parser** - Current F006 implementation is POC only
  - See ADR-003 and F006-sql-parser-improvements.md
  - Required before Phase 2 features (joins, advanced windows)
- [ ] Add property-based tests for serialization
- [ ] Document unsafe blocks in core crate
- [ ] Add tracing spans for debugging

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

## Upcoming Milestones

| Milestone | Target Date | Status |
|-----------|-------------|--------|
| Phase 1 Complete | TBD | 🚧 In Progress |
| First public demo | TBD | 📋 Planned |
| Phase 2 Start | TBD | 📋 Planned |

## Communication

- **Daily**: Quick status in CONTEXT.md
- **Weekly**: Update STEERING.md priorities
- **Phase End**: Full retrospective and planning
