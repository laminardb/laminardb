# Contributing to LaminarDB

Hey! Thanks for your interest in contributing. Whether it's a bug fix, a new connector, better tests, or a typo in the docs -- we appreciate it.

This guide will get you from zero to a working build in a few minutes.

## Quick Setup

You need **Rust stable** (1.85+). That's it for the default build.

```bash
git clone https://github.com/laminardb/laminardb.git
cd laminardb
cargo build
cargo test --all --lib
cargo clippy --all -- -D warnings
```

If everything passes, you're good to go.

### Optional system libraries (Linux only)

Some feature-gated connectors need extra libs. You only need these if you're working on that specific connector:

```bash
# Kafka (rdkafka)
sudo apt-get install cmake pkg-config libsasl2-dev

# NUMA topology (hwloc)
sudo apt-get install libhwloc-dev
```

## How the project is organized

LaminarDB is a Rust workspace with 7 crates. Here's what each one does:

| Crate | What it does |
|-------|-------------|
| **laminar-core** | The engine. Reactor, operators, state stores, DAG executor, streaming channels, checkpoint barriers. This is the hot path. |
| **laminar-sql** | SQL parser with streaming extensions (EMIT, watermarks, windows), query planner, DataFusion integration. |
| **laminar-storage** | WAL, incremental checkpointing, checkpoint manifests, per-core WAL segments. |
| **laminar-connectors** | All external connectors: Kafka, PostgreSQL CDC, MySQL CDC, MongoDB CDC, Delta Lake, Iceberg, WebSocket, Parquet, files. Also the schema framework and serde layer. |
| **laminar-db** | The main entry point. Ties everything together -- checkpoint coordination, recovery, streaming executor, FFI API. |
| **laminar-derive** | Proc macros: `Record`, `FromRecordBatch`, `FromRow`, `ConnectorConfig`. |
| **laminar-server** | Standalone server binary with TOML config, Axum HTTP API, hot reload, Prometheus metrics. |

For the full architecture, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Where things live

A few common starting points:

- **Connector traits**: `crates/laminar-connectors/src/connector.rs` -- `SourceConnector` and `SinkConnector`
- **Schema decoders**: `crates/laminar-connectors/src/schema/` -- JSON, CSV, Avro, Parquet
- **SQL parser**: `crates/laminar-sql/src/parser/` -- streaming SQL extensions
- **State stores**: `crates/laminar-core/src/state/` -- AHashMap, Mmap backends
- **DAG operators**: `crates/laminar-core/src/dag/operators/` -- joins, windows, aggregations
- **Checkpoint system**: `crates/laminar-core/src/checkpoint/` -- barriers, alignment
- **Server HTTP API**: `crates/laminar-server/src/http.rs` -- REST endpoints
- **FFI layer**: `crates/laminar-db/src/ffi/` -- C bindings for language interop
- **Feature tracking**: `docs/features/INDEX.md` -- what's done, what's not

## Feature flags

Most connectors are behind feature flags so the default build stays fast. Here are the ones you'll see most:

| Flag | What it enables |
|------|----------------|
| `kafka` | Kafka source/sink with Avro serde |
| `postgres-cdc` | PostgreSQL CDC (logical replication) source |
| `postgres-sink` | PostgreSQL sink |
| `mysql-cdc` | MySQL CDC (binlog) source |
| `mongodb-cdc` | MongoDB change stream source and sink |
| `delta-lake` | Delta Lake source and sink |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink |
| `files` | File source and sink (AutoLoader-style) |
| `parquet-lookup` | Parquet file lookup table source |
| `ffi` | C FFI layer and Arrow C Data Interface |
| `delta` | Distributed delta mode (Raft, gossip, gRPC) |

To run tests with a specific connector:

```bash
cargo test --all --features kafka
cargo test --all --features postgres-cdc,postgres-sink
```

## The hot path rules

If you're touching code in `laminar-core` that runs on the event processing path (Ring 0), there are some strict rules:

- **No heap allocations.** Use pre-allocated buffers, SmallVec, arena allocators.
- **No locks.** SPSC queues for inter-ring communication. No Mutex, no RwLock.
- **No system calls.** No println!, no file I/O, no network calls on the hot path.
- **`// SAFETY:` comments** on every `unsafe` block. No exceptions.

If you're not sure whether your code is on the hot path, it probably isn't. The hot path is the reactor loop in `laminar-core` -- most contributions (connectors, SQL, storage) run in Ring 1 where these rules don't apply.

Run benchmarks before and after if you're touching performance-sensitive code:

```bash
cargo bench --bench state_bench       # State store lookups (<500ns target)
cargo bench --bench latency_bench     # End-to-end event latency (<10us target)
cargo bench --bench streaming_bench   # Throughput per core (500K events/sec target)
```

There are 13 benchmark suites across the workspace -- see `crates/*/benches/` for the full list.

## Running tests

```bash
# All unit tests (this is what CI runs)
cargo test --all --lib

# A specific crate
cargo test -p laminar-core

# A specific test by name
cargo test -p laminar-sql test_parse_tumbling_window

# With connector features
cargo test --all --features kafka,postgres-cdc,mysql-cdc

# Benchmarks
cargo bench --bench dag_bench
```

We have ~4,400 tests across the workspace. If you're adding new functionality, please add tests. If you're fixing a bug, a regression test is always welcome.

## Code style

- **Format**: `cargo +nightly fmt --all` (we use nightly rustfmt for import grouping)
- **Lint**: `cargo clippy --all -- -D warnings` (must be clean)
- **Docs**: All public APIs need doc comments. We enforce `#![deny(missing_docs)]`.
- **Errors**: Use `thiserror` in library crates, `anyhow` in the server binary.
- **Imports**: Group as std, external crates, then internal modules.

## Making a pull request

1. Fork the repo, create a branch from `main`
2. Make your changes -- keep commits focused
3. Add tests for new code
4. Run the checks:
   ```bash
   cargo +nightly fmt --all -- --check
   cargo clippy --all -- -D warnings
   cargo test --all --lib
   ```
5. Open a PR against `main`

### Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(connectors): add Redis lookup table source
fix(sql): handle NULL in ASOF JOIN match condition
test(checkpoint): add barrier alignment integration test
perf(state): reduce AHashMap lookup from 500ns to 350ns
docs(contributing): update project structure
refactor(storage): extract WAL segment into separate module
chore(deps): update arrow to 57.2
```

### Review policy

We welcome AI-assisted contributions, but we need a human in the loop.

**Every PR has a CI-enforced human attestation gate.** The PR template includes two checkboxes and a reviewer notes section. CI will block your PR if:

- The "I have personally reviewed this entire diff" checkbox isn't checked
- The "review comments explain why" checkbox isn't checked
- The reviewer notes section is empty or too brief (minimum 30 characters explaining what you verified and why the change is correct)
- The "Why" section doesn't explain the motivation (minimum 20 characters)

This gate is a required status check on `main` -- there's no way around it.

**AI-generated code is fine.** Use Copilot, Claude, Cursor, whatever helps you write better code faster. But *you* must understand what the code does, *you* must have run the tests, and *you* must be able to explain it in review.

**Automated PR bots will be blocked.** If we see a pattern of PRs that look like an agent running on autopilot -- bulk low-quality PRs, no human in the review loop, copy-paste responses to review comments -- we'll close them and may block the account.

**Open a draft PR early** if you want feedback before finishing. We'd rather help you course-correct than review a big surprise.

## Good places to start

Check out issues labeled [`good first issue`](https://github.com/laminardb/laminardb/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) -- these are scoped to be approachable without deep knowledge of the internals.

Some areas that are especially welcoming to new contributors:

- **Tests** -- We can always use more. Property tests, integration tests, edge cases.
- **Documentation** -- Config options, connector setup guides, architecture docs.
- **Connectors** -- Adding a new source or sink is self-contained. The `SourceConnector` and `SinkConnector` traits in `connector.rs` are the interface.
- **Language bindings** -- Java, Node.js, .NET bindings are all open for contribution. The Python bindings and C FFI layer are the reference implementations.

## Questions?

Open an issue or drop a comment on any existing issue. There's no such thing as a dumb question. We'd rather help you get unstuck than have you give up quietly.
