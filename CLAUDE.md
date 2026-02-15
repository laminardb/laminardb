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

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - System design and ring model
- [ROADMAP.md](docs/ROADMAP.md) - Phase timeline and milestones
- [STEERING.md](docs/STEERING.md) - Current priorities and decisions
- [features/INDEX.md](docs/features/INDEX.md) - Feature tracking

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
