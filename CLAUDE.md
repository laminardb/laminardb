# LaminarDB

A high-performance embedded streaming database in Rust. Think "SQLite for stream processing" with sub-microsecond latency.

## Quick Start

```bash
cargo build --release          # Build all crates
cargo test                     # Run all tests
cargo bench                    # Run benchmarks
cargo clippy -- -D warnings    # Lint (must pass)
cargo doc --no-deps --open     # Generate docs
```

## Project Structure

```
crates/
├── laminar-core/      # Reactor, state stores, operators
├── laminar-sql/       # DataFusion integration, SQL parsing
├── laminar-storage/   # WAL, checkpoints, Delta Lake/Iceberg
├── laminar-connectors/# Kafka, CDC, lookup joins
├── laminar-auth/      # RBAC, ABAC, authentication
├── laminar-admin/     # Dashboard UI, REST API
├── laminar-observe/   # Metrics, tracing, health checks
└── laminar-server/    # Standalone server binary
```

## Key Documents

- @docs/STEERING.md - Current priorities and decisions
- @docs/ARCHITECTURE.md - System design and ring model
- @docs/ROADMAP.md - Phase timeline and milestones
- @docs/features/INDEX.md - Feature tracking
- @docs/COMPETITIVE.md - Competitive landscape analysis

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| State lookup | < 500ns | `cargo bench --bench state_bench` |
| Throughput/core | 500K events/sec | `cargo bench --bench throughput` |
| p99 latency | < 10μs | `cargo bench --bench latency` |
| Checkpoint | < 10s recovery | Integration tests |

## Critical Constraints

- **Zero allocations on hot path** - Use arena allocators
- **No locks on hot path** - SPSC queues, lock-free structures
- **Unsafe requires justification** - Comment with `// SAFETY:` explanation
- **All public APIs documented** - `#![deny(missing_docs)]`

## Development Workflow

### Starting a Session
1. Run `/status` to see current state
2. Pick up the next priority item

### Ending a Session
1. Note any blockers or decisions needed
2. List immediate next steps

## Ring Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                         │
│  Zero allocations, no locks, < 1μs latency                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │ Reactor │─▶│Operators│─▶│  State  │─▶│  Emit   │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
├─────────────────────────────────────────────────────────────────┤
│                     RING 1: BACKGROUND                          │
│  Async I/O, checkpoints, can allocate                           │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                         │
│  │Checkpoint│  │  WAL   │  │Compaction│                         │
│  └─────────┘  └─────────┘  └─────────┘                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 2: CONTROL PLANE                       │
│  Configuration, metrics, admin                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                         │
│  │  Admin  │  │ Metrics │  │  Auth   │                         │
│  └─────────┘  └─────────┘  └─────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

## Agents

- **architect** - Design review, ring model validation
- **security-reviewer** - Auth code audits
- **performance-auditor** - Hot path violation checks
- **stage-gate** - Phase completion validation
- **feature-spec-writer** - Generate feature specifications

## Commands

- `/status` - Current project state
- `/new-feature` - Create feature specification
- `/complete-feature` - Run completion checklist
- `/gate-check` - Validate phase completion
- `/adr` - Create architecture decision record
