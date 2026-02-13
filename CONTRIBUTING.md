# Contributing to LaminarDB

Thank you for your interest in contributing to LaminarDB! This guide covers everything you need to get started.

## Prerequisites

- **Rust 1.85+** (stable channel) -- see [rust-toolchain.toml](rust-toolchain.toml)
- **cargo-deny** (optional) -- `cargo install cargo-deny` for dependency auditing

### Linux-only (optional features)

Some features require system libraries on Linux:

```bash
# hwloc (NUMA topology discovery)
sudo apt-get install libhwloc-dev

# XDP/eBPF
sudo apt-get install libelf-dev

# Kafka (rdkafka)
sudo apt-get install cmake pkg-config libsasl2-dev
```

## Getting Started

```bash
# Clone and build
git clone https://github.com/laminardb/laminardb.git
cd laminardb
cargo build

# Run all tests
cargo test --all

# Run tests with optional features
cargo test --all --features kafka,postgres-cdc,mysql-cdc

# Lint (must pass CI)
cargo clippy --all -- -D warnings

# Format check
cargo +nightly fmt --all -- --check
```

## Project Structure

| Crate | Ring | Purpose |
|-------|------|---------|
| `laminar-core` | 0 | Reactor, operators, state stores, streaming channels, JIT compiler |
| `laminar-sql` | -- | SQL parser (streaming extensions), query planner, DataFusion integration |
| `laminar-storage` | 1 | WAL, incremental checkpointing, RocksDB backend |
| `laminar-connectors` | 1 | Kafka, PostgreSQL CDC, MySQL CDC, Delta Lake, connector SDK |
| `laminar-db` | -- | Unified database facade, checkpoint coordination, FFI API |
| `laminar-auth` | 2 | JWT authentication, RBAC, ABAC |
| `laminar-admin` | 2 | REST API (Axum), Swagger UI |
| `laminar-observe` | 2 | Prometheus metrics, OpenTelemetry tracing |
| `laminar-derive` | -- | Proc macros for `Record` and `FromRecordBatch` traits |
| `laminar-server` | -- | Standalone server binary |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

## Development Guidelines

### Code Style

- Format with `cargo fmt` (nightly, default settings)
- Lint with `cargo clippy -- -D warnings`
- All public APIs must be documented (`#![deny(missing_docs)]`)
- Use `thiserror` for library errors, `anyhow` for application code
- Prefer explicit types for public APIs, inference for locals
- Group imports: std, external crates, internal modules

### Ring 0 Rules

Code running on the hot path (Ring 0) must follow strict constraints:

- **No heap allocations** -- Use bump/arena allocators. The `allocation-tracking` feature panics on allocation in marked sections.
- **No locks** -- Use SPSC queues for inter-ring communication. No `Mutex`, `RwLock`, or `AtomicOrdering` beyond `Relaxed`.
- **No system calls** -- Use io_uring (Linux) for async I/O. Avoid `println!`, file I/O, or network calls.
- **No unbounded iteration** -- Task budget enforcement limits operator time slices.
- **`// SAFETY:` comments** -- All `unsafe` blocks must have a safety justification comment.

If you're modifying Ring 0 code, run the relevant benchmarks before and after:

```bash
cargo bench --bench state_bench
cargo bench --bench throughput_bench
cargo bench --bench latency_bench
```

### Testing

```bash
# Unit tests (all crates)
cargo test --all

# Specific crate
cargo test -p laminar-core

# With feature flags
cargo test --all --features kafka,postgres-cdc,mysql-cdc,rocksdb

# Run benchmarks
cargo bench

# Specific benchmark
cargo bench --bench dag_bench
```

### Feature Flags

Many features are optional to keep compile times manageable:

| Flag | Crate | Purpose |
|------|-------|---------|
| `jit` | laminar-core, laminar-db | Cranelift JIT compilation |
| `kafka` | laminar-connectors | Kafka source/sink, Avro serde |
| `postgres-cdc` | laminar-connectors | PostgreSQL CDC source |
| `postgres-sink` | laminar-connectors | PostgreSQL sink |
| `mysql-cdc` | laminar-connectors | MySQL CDC source |
| `rocksdb` | laminar-storage, laminar-db | RocksDB-backed state store |
| `delta-lake` | laminar-connectors | Delta Lake sink |
| `ffi` | laminar-db | C FFI layer |
| `allocation-tracking` | laminar-core | Panic on hot-path allocation |
| `io-uring` | laminar-core | Linux io_uring integration |

## Pull Request Process

1. **Fork** the repository and create a feature branch from `main`
2. **Make your changes** -- keep commits focused and atomic
3. **Add tests** for new functionality
4. **Run the full check suite**:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --all -- -D warnings
   cargo test --all
   ```
5. **Update documentation** if you changed public APIs
6. **Open a Pull Request** against `main`

### PR Checklist

- [ ] Tests pass (`cargo test --all`)
- [ ] No clippy warnings (`cargo clippy --all -- -D warnings`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] Public APIs are documented
- [ ] Ring 0 code verified (no allocations/locks) if applicable
- [ ] Benchmarks run if performance-sensitive code changed

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(core): add session window merge detection
fix(sql): handle NULL in ASOF JOIN match condition
perf(state): reduce FxHashMap lookup from 500ns to 350ns
docs(readme): add Python quick start example
refactor(storage): extract WAL segment into separate module
test(connectors): add Kafka exactly-once integration test
chore(deps): update arrow to 57.2
```

## Feature Specifications

Each feature has a specification document in `docs/features/`. If you're implementing a new feature:

1. Check the [Feature Index](docs/features/INDEX.md) for existing specs
2. For new features, create a spec using `/new-feature` or follow the template in `docs/features/`
3. Reference the feature ID (e.g., F025) in commits and PR titles

## Architecture Decisions

Significant architectural changes are documented as ADRs (Architecture Decision Records) in `docs/adr/`. If your change involves a design decision with multiple valid approaches, create an ADR first.

## Reporting Issues

Use the [GitHub issue templates](https://github.com/laminardb/laminardb/issues/new/choose):

- **Bug Report** -- For bugs with reproduction steps
- **Feature Request** -- For new feature suggestions

## Questions?

Feel free to open an issue for any questions!
