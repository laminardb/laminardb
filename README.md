# LaminarDB

**Ultra-low latency embedded streaming SQL database written in Rust.**

[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org)

Think "SQLite for stream processing." LaminarDB is an embedded streaming database that lets you run continuous SQL queries over real-time data with sub-microsecond latency. No cluster, no JVM, no external dependencies -- just link it as a Rust library or use the Python bindings.

Built on [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/), LaminarDB targets use cases like financial market data, IoT edge analytics, and real-time application state where every microsecond counts.

## Quick Start

### As a Rust Library

```rust
use laminar_db::LaminarDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    // Create a streaming source
    db.execute("CREATE SOURCE trades (
        symbol VARCHAR NOT NULL,
        price DOUBLE NOT NULL,
        volume BIGINT NOT NULL,
        ts BIGINT NOT NULL
    )").await?;

    // Define a continuous aggregation with a tumbling window
    db.execute("CREATE STREAM vwap AS
        SELECT symbol, SUM(price * volume) / SUM(volume) AS vwap, COUNT(*) AS trades
        FROM trades
        GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
    ").await?;

    // Push data and subscribe to results
    let source = db.source_untyped("trades")?;
    // source.push(batch);

    // Start the streaming pipeline
    db.start().await?;

    Ok(())
}
```

### Python

```bash
pip install laminardb
```

```python
import laminardb

db = laminardb.open(":memory:")

db.execute("CREATE SOURCE sensors (device_id VARCHAR, temp DOUBLE, ts BIGINT)")
db.execute("""
    CREATE STREAM avg_temp AS
    SELECT device_id, AVG(temp) AS avg_temp, COUNT(*) AS readings
    FROM sensors
    GROUP BY device_id, tumble(ts, INTERVAL '10' SECOND)
""")

db.insert("sensors", {"device_id": "a1", "temp": 22.5, "ts": 1700000000000})
db.start()
```

See the [laminardb-python](https://github.com/laminardb/laminardb-python) repository for full documentation.

### Standalone Server

```bash
cargo run --release --bin laminardb
```

## Key Features

- **Streaming SQL** -- Tumbling, sliding, hopping, and session windows with standard SQL syntax
- **Sub-microsecond latency** -- Three-ring architecture with zero allocations on the hot path
- **Embedded-first** -- Link as a Rust library, no external dependencies
- **Apache DataFusion** -- Full SQL support via DataFusion, including joins, aggregations, and UDFs
- **Arrow-native** -- Zero-copy data flow with Apache Arrow RecordBatch at every boundary
- **Exactly-once semantics** -- WAL, incremental checkpointing, and two-phase commit
- **Thread-per-core** -- Linear scaling with CPU cores, NUMA-aware memory allocation
- **Connectors** -- Kafka, PostgreSQL CDC, MySQL CDC, Delta Lake, with a connector SDK
- **JIT compilation** -- Optional Cranelift-based query compilation for Ring 0 execution
- **Python bindings** -- Full-featured Python API with PyArrow zero-copy interop

## Architecture

LaminarDB uses a three-ring architecture to separate latency-critical event processing from background I/O and control plane operations:

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                         │
│  Zero allocations, no locks, < 1us latency                      │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Reactor │─>│ Operators│─>│  State   │─>│   Emit   │         │
│  │  Loop   │  │ (window, │  │  Store   │  │ (output) │         │
│  │         │  │  join,   │  │ (mmap/   │  │          │         │
│  │         │  │  filter) │  │  fxhash) │  │          │         │
│  └─────────┘  └──────────┘  └──────────┘  └──────────┘         │
│       |                                                         │
│       | SPSC queues (lock-free)                                 │
│       v                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 1: BACKGROUND                          │
│  Async I/O, can allocate, bounded latency impact                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │Checkpoint│  │   WAL    │  │Compaction│  │  Timer   │        │
│  │ Manager  │  │  Writer  │  │  Thread  │  │  Wheel   │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│       |                                                         │
│       | Channels (bounded)                                      │
│       v                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 2: CONTROL PLANE                       │
│  No latency requirements, full flexibility                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  Admin   │  │ Metrics  │  │   Auth   │  │  Config  │        │
│  │   API    │  │  Export  │  │  Engine  │  │ Manager  │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

## Performance Targets

| Metric | Target | Benchmark |
|--------|--------|-----------|
| State lookup | < 500ns p99 | `cargo bench --bench state_bench` |
| Throughput/core | 500K events/sec | `cargo bench --bench throughput_bench` |
| p99 latency | < 10us | `cargo bench --bench latency_bench` |
| Checkpoint recovery | < 10s | Integration tests |
| Window trigger | < 10us | `cargo bench --bench window_bench` |

Run all benchmarks:

```bash
cargo bench
```

## Streaming SQL

LaminarDB extends standard SQL with streaming primitives:

```sql
-- Tumbling window aggregation
CREATE STREAM ohlc_1min AS
SELECT symbol,
       MIN(price) AS low, MAX(price) AS high,
       SUM(volume) AS total_volume
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE);

-- Session windows (gap-based)
CREATE STREAM user_sessions AS
SELECT user_id, COUNT(*) AS clicks, MAX(ts) - MIN(ts) AS duration
FROM clickstream
GROUP BY user_id, session(ts, INTERVAL '30' SECOND);

-- Stream-to-stream joins
CREATE STREAM enriched_orders AS
SELECT o.order_id, o.symbol, t.price AS market_price
FROM orders o
JOIN trades t ON o.symbol = t.symbol
    AND t.ts BETWEEN o.ts - INTERVAL '5' SECOND AND o.ts;

-- ASOF joins for point-in-time lookups
SELECT o.*, t.price AS last_trade_price
FROM orders o
ASOF JOIN trades t MATCH_CONDITION(o.ts >= t.ts) ON o.symbol = t.symbol;
```

## Comparison

| Feature | LaminarDB | Apache Flink | Kafka Streams | RisingWave | Materialize |
|---------|-----------|--------------|---------------|------------|-------------|
| Deployment | Embedded | Distributed | Embedded | Distributed | Distributed |
| Latency | < 1us | ~10ms | ~1ms | ~10ms | ~10ms |
| SQL support | Full | Limited | None | Full | Full |
| Exactly-once | Yes | Yes | Yes | Yes | Yes |
| No JVM | Yes | No | No | Yes | Yes |
| Language bindings | Rust, Python | Java | Java | SQL only | SQL only |
| License | Apache-2.0 | Apache-2.0 | Apache-2.0 | Apache-2.0 | BSL |

## Project Structure

```
crates/
├── laminar-core/        Core engine: reactor, operators, state, windows, joins
├── laminar-sql/         SQL layer: DataFusion integration, streaming SQL parser
├── laminar-storage/     Durability: WAL, checkpointing, RocksDB, Delta Lake
├── laminar-connectors/  Connectors: Kafka, PostgreSQL CDC, MySQL CDC, Delta Lake
├── laminar-db/          Unified database facade and FFI API
├── laminar-auth/        Authentication and authorization (JWT, RBAC, ABAC)
├── laminar-admin/       Admin REST API (Axum, Swagger UI)
├── laminar-observe/     Observability: Prometheus metrics, OpenTelemetry tracing
├── laminar-derive/      Derive macros for Record and FromRecordBatch traits
└── laminar-server/      Standalone server binary
examples/
└── demo/                Market data TUI demo with Ratatui
```

## Language Bindings

| Language | Package | Status |
|----------|---------|--------|
| **Rust** | [laminar-db](https://crates.io/crates/laminar-db) | Primary |
| **Python** | [laminardb](https://github.com/laminardb/laminardb-python) | Implemented |
| C/C++ | Via FFI (`--features ffi`) | Implemented |
| Java | laminardb-java | Planned |
| Node.js | @laminardb/node | Planned |

## Building from Source

```bash
# Prerequisites: Rust 1.85+ (stable)
rustup update stable

# Clone and build
git clone https://github.com/laminardb/laminardb.git
cd laminardb
cargo build --release

# Run tests
cargo test --all

# Run with optional features
cargo test --all --features kafka,postgres-cdc,mysql-cdc

# Lint
cargo clippy --all -- -D warnings

# Generate API docs
cargo doc --no-deps --open

# Run the market data demo
cargo run -p laminardb-demo
```

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) -- Three-ring design, data flow, state management
- [Development Roadmap](docs/ROADMAP.md) -- Phases, milestones, and feature status
- [Feature Index](docs/features/INDEX.md) -- All 160 features with specifications
- [Contributing Guide](CONTRIBUTING.md) -- How to build, test, and submit PRs
- [API Reference](https://laminardb.io/api/laminar_db) -- Rustdoc API documentation
- [Python Bindings](https://github.com/laminardb/laminardb-python) -- Python API and docs

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style guidelines, and the pull request process.

## License

Apache License 2.0 -- see [LICENSE](LICENSE) for details.
