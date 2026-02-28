# LaminarDB

**Embedded streaming SQL database written in Rust.**

[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org)

Think "SQLite for stream processing." LaminarDB is an embedded streaming database that lets you run continuous SQL queries over real-time data with sub-microsecond latency. No cluster, no JVM, no external dependencies -- just link it as a Rust library.

Built on [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/), LaminarDB targets use cases like financial market data, IoT edge analytics, and real-time application state where every microsecond counts.

---

## Getting Started

### 1. Add the dependency

```bash
cargo add laminar-db
```

Or in your `Cargo.toml`:

```toml
[dependencies]
laminar-db = "0.16"
tokio = { version = "1", features = ["full"] }
```

### 2. Write your first streaming query

```rust
use laminar_db::LaminarDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open an in-memory database
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
        SELECT symbol,
               SUM(price * CAST(volume AS DOUBLE)) / SUM(CAST(volume AS DOUBLE)) AS vwap,
               COUNT(*) AS trades
        FROM trades
        GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
    ").await?;

    // Get a handle to push data
    let source = db.source_untyped("trades")?;

    // Start the streaming pipeline
    db.start().await?;

    // Push data via source.push(record_batch)
    // Subscribe to results via db.subscribe::<T>("vwap")

    db.shutdown().await?;
    Ok(())
}
```

### 3. Build and run

```bash
cargo build --release
cargo run
```

### Python Quick Start

LaminarDB also has Python bindings via [laminardb-python](https://github.com/laminardb/laminardb-python):

```bash
pip install laminardb
```

```python
import laminardb

conn = laminardb.open(":memory:")
conn.execute("CREATE SOURCE sensors (ts BIGINT, device VARCHAR, value DOUBLE)")
conn.insert("sensors", [
    {"ts": 1, "device": "sensor_a", "value": 42.0},
    {"ts": 2, "device": "sensor_b", "value": 43.5},
])

conn.sql("SELECT * FROM sensors WHERE value > 42.0").show()
#    ts    device  value
# 0   2  sensor_b   43.5

conn.close()
```

See the [Python README](https://github.com/laminardb/laminardb-python) for the full API including streaming subscriptions, DataFrame integration, and async support.

---

## Feature Overview

### Streaming SQL

LaminarDB extends standard SQL with streaming primitives. All window types, join types, and EMIT strategies listed below are implemented and tested.

#### Window Types

| Window | Syntax | Description |
|--------|--------|-------------|
| Tumbling | `tumble(ts, INTERVAL '1' MINUTE)` | Fixed-size, non-overlapping windows |
| Sliding | `slide(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)` | Overlapping windows with configurable slide |
| Hopping | `hop(ts, INTERVAL '10' SECOND, INTERVAL '5' SECOND)` | Alias for sliding windows |
| Session | `session(ts, INTERVAL '30' SECOND)` | Gap-based dynamic windows with merge support |

```sql
-- Tumbling window: 1-minute OHLC bars
CREATE STREAM ohlc_1m AS
SELECT symbol,
       CAST(tumble(ts, INTERVAL '1' MINUTE) AS BIGINT) AS window_start,
       first_value(price) AS open,
       MAX(price) AS high,
       MIN(price) AS low,
       last_value(price) AS close,
       SUM(volume) AS volume
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- Session window: detect activity bursts
CREATE STREAM user_sessions AS
SELECT user_id,
       COUNT(*) AS clicks,
       MAX(ts) - MIN(ts) AS duration_ms
FROM clickstream
GROUP BY user_id, session(ts, INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;
```

#### Join Types

| Join | Description |
|------|-------------|
| Stream-Stream | Time-bounded inner/outer joins between two streams |
| ASOF | Point-in-time lookups (match closest preceding row) |
| Temporal | Versioned joins with time-validity semantics |
| Lookup | Enrichment joins against reference/lookup tables |

```sql
-- Stream-stream join with time window
CREATE STREAM enriched_orders AS
SELECT o.order_id, o.symbol, t.price AS market_price
FROM orders o
INNER JOIN trades t ON o.symbol = t.symbol
    AND t.ts BETWEEN o.ts - 10000 AND o.ts + 10000;

-- ASOF join for point-in-time lookup
SELECT o.*, t.price AS last_trade_price
FROM orders o
ASOF JOIN trades t ON o.symbol = t.symbol AND o.ts >= t.ts;

-- Lookup join against a reference table
CREATE LOOKUP TABLE instruments FROM POSTGRES (
    hostname = 'db.example.com', port = '5432',
    database = 'market', query = 'SELECT * FROM instruments'
);

SELECT t.symbol, t.price, i.sector, i.exchange
FROM trades t
JOIN instruments i ON t.symbol = i.symbol;
```

#### EMIT Strategies

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `EMIT ON WINDOW CLOSE` | Emit once when the window closes | Final aggregates (OHLC bars, billing) |
| `EMIT CHANGES` | Emit insert/retract pairs on every update | Live dashboards, cascading MVs |
| `EMIT FINAL` | Alias for ON WINDOW CLOSE | Explicit final-only output |

`EMIT CHANGES` uses the **Z-set changelog model** (DBSP/Feldera-inspired): each update produces a `+1` insert for the new value and a `-1` retraction for the previous value, enabling correct incremental computation and cascading materialized views.

#### Analytics Functions

```sql
-- LAG/LEAD
SELECT symbol, price,
       LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) AS prev_price,
       price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) AS delta
FROM trades;

-- Ranking
SELECT symbol, price,
       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price DESC) AS rank
FROM trades;

-- Cascading materialized views (MV reading from MV)
CREATE MATERIALIZED VIEW ohlc_1h AS
SELECT symbol,
       first_value(open) AS open, MAX(high) AS high,
       MIN(low) AS low, last_value(close) AS close
FROM ohlc_1m
GROUP BY symbol, tumble(window_start, INTERVAL '1' HOUR);
```

#### Introspection

```sql
-- Show all registered sources/sinks/streams with metadata columns
SHOW SOURCES;
SHOW SINKS;
SHOW STREAMS;

-- Reconstruct the DDL for a source or sink
SHOW CREATE SOURCE trades;

-- Explain a query plan with execution metrics
EXPLAIN ANALYZE SELECT symbol, COUNT(*) FROM trades GROUP BY symbol;
```

#### Additional SQL Features

- **Window offsets** -- `tumble(ts, INTERVAL '1' HOUR, INTERVAL '8' HOUR)` for timezone-aligned windows
- **ASOF NEAREST** -- Match by minimum absolute time difference via `MATCH_CONDITION(NEAREST(...))`
- **date_trunc / date_bin** -- Available via DataFusion 52 built-ins
- **UNNEST** -- Available via DataFusion 52 built-ins
- **Structured error codes** -- Every error carries a stable `LDB-NNNN` code for grep-able diagnostics

See [docs/SQL_REFERENCE.md](docs/SQL_REFERENCE.md) for the complete SQL dialect reference, including gotchas and tested patterns.

### Watermarks and Event Time

LaminarDB uses event-time processing with watermarks to handle out-of-order data.

```sql
-- Bounded out-of-orderness: allow up to 5 seconds of late data
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts BIGINT,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
);
```

Watermark types:
- **Per-partition** -- independent watermark per source partition
- **Per-key** -- independent watermark per key (e.g., per symbol)
- **Alignment groups** -- synchronized watermarks across related sources for join correctness

Late data can be dropped (default) or redirected to a side output.

---

## Connectors

LaminarDB connects to external systems via feature-gated connectors. Each connector implements the `SourceConnector` or `SinkConnector` trait with exactly-once semantics via two-phase commit.

### Source Connectors

| Connector | Feature Flag | Description | Status |
|-----------|-------------|-------------|--------|
| **Kafka** | `kafka` | Consumer group, backpressure, Schema Registry | Implemented |
| **PostgreSQL CDC** | `postgres-cdc` | Logical replication (pgoutput), Z-set changelog | Implemented |
| **MySQL CDC** | `mysql-cdc` | Binlog decoding, GTID position tracking | Implemented |
| **WebSocket Client** | `websocket` | Connect to external WebSocket servers | Implemented |
| **WebSocket Server** | `websocket` | Accept incoming WebSocket connections | Implemented |
| **Delta Lake** | `delta-lake` | Read from Delta Lake tables (version polling) | Implemented |
| MongoDB CDC | -- | -- | Planned |

### Sink Connectors

| Connector | Feature Flag | Description | Status |
|-----------|-------------|-------------|--------|
| **Kafka** | `kafka` | Exactly-once transactions, configurable partitioning | Implemented |
| **PostgreSQL** | `postgres-sink` | COPY BINARY, upsert, co-transactional exactly-once | Implemented |
| **Delta Lake** | `delta-lake` | S3/Azure/GCS, epoch-aligned Parquet commits | Implemented |
| **Apache Iceberg** | -- | Buffering, epoch, partition transforms | Implemented (business logic; I/O integration planned) |
| **WebSocket Server** | `websocket` | Fan-out results to connected subscribers | Implemented |
| **WebSocket Client** | `websocket` | Push results to external WebSocket server | Implemented |

### Kafka Source Example

```sql
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts BIGINT
) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'market-trades',
    group_id = 'laminar-analytics',
    format = 'json',
    offset_reset = 'earliest'
);
```

Supported formats: `json`, `csv`, `avro` (with Schema Registry), `raw` (bytes), `debezium` (CDC envelope).

### PostgreSQL CDC Example

```sql
CREATE SOURCE orders_cdc (
    id INT, customer_id INT, amount DOUBLE, status VARCHAR, ts BIGINT
) FROM POSTGRES_CDC (
    hostname = 'db.example.com',
    port = '5432',
    database = 'shop',
    slot.name = 'laminar_orders',
    publication = 'laminar_pub'
);
```

CDC events are emitted as a Z-set changelog with `_op` column (`I`=insert, `U`=update, `D`=delete).

### MySQL CDC Example

```sql
CREATE SOURCE products_cdc (
    id INT, name VARCHAR, price DOUBLE, updated_at BIGINT
) FROM MYSQL_CDC (
    hostname = 'mysql.example.com',
    port = '3306',
    database = 'inventory',
    table = 'products',
    server_id = '12345'
);
```

### Delta Lake Sink Example

```sql
CREATE SINK lake INTO DELTA_LAKE (
    path = 's3://my-bucket/trade_summary',
    write_mode = 'append',
    delivery.guarantee = 'exactly-once'
) AS SELECT * FROM trade_summary;
```

Cloud storage backends: S3 (`delta-lake-s3`), Azure ADLS (`delta-lake-azure`), GCS (`delta-lake-gcs`). Supports Glue and Unity catalogs.

### Connector SDK

Build custom connectors using the SDK with operational resilience built in:

```rust
use laminar_connectors::connector::{SourceConnector, SinkConnector};
use laminar_connectors::sdk::{RetryPolicy, CircuitBreaker, RateLimiter};
```

The SDK provides retry policies with exponential backoff, circuit breakers, rate limiters, and a test harness for connector development.

---

## Schema Framework

LaminarDB includes a pluggable schema framework for format decoding, schema inference, and schema evolution.

### Format Decoders

| Format | Inference | Description |
|--------|-----------|-------------|
| JSON | Yes | Type inference, nested object/array support |
| CSV | Yes | Header-based inference, type sampling |
| Avro | Yes | Schema Registry integration |
| Parquet | Yes | Metadata-driven schema |
| Protobuf | -- | Planned |

### Additional Capabilities

- **Schema Evolution** -- additive column changes with backward/forward compatibility
- **Dead Letter Queue** -- malformed records routed to DLQ for inspection
- **Schema Hints** -- partial schema specification with wildcard inference for the rest
- **JSON Functions** -- PostgreSQL-compatible JSON extraction, table-valued functions, LaminarDB extensions
- **Array/Struct/Map Functions** -- complex type manipulation in SQL

---

## Architecture

LaminarDB uses a three-ring architecture to separate latency-critical event processing from background I/O and control plane operations:

```
+------------------------------------------------------------------+
|                        RING 0: HOT PATH                          |
|  Zero allocations, no locks, < 1us latency                      |
|  +---------+  +----------+  +----------+  +----------+          |
|  | Reactor |->| Operators|->|  State   |->|   Emit   |          |
|  |  Loop   |  | (window, |  |  Store   |  | (output) |          |
|  |         |  |  join,   |  | (ahash/  |  |          |          |
|  |         |  |  filter) |  |  fxhash) |  |          |          |
|  +---------+  +----------+  +----------+  +----------+          |
|       |                                                          |
|       | SPSC queues (lock-free)                                  |
|       v                                                          |
+------------------------------------------------------------------+
|                     RING 1: BACKGROUND                           |
|  Async I/O, can allocate, bounded latency impact                 |
|  +----------+  +----------+  +----------+  +----------+         |
|  |Checkpoint|  |   WAL    |  |Compaction|  |  Timer   |         |
|  | Manager  |  |  Writer  |  |  Thread  |  |  Wheel   |         |
|  +----------+  +----------+  +----------+  +----------+         |
|       |                                                          |
|       | Channels (bounded)                                       |
|       v                                                          |
+------------------------------------------------------------------+
|                     RING 2: CONTROL PLANE                        |
|  No latency requirements, full flexibility                       |
|  +----------+  +----------+  +----------+  +----------+         |
|  |  Admin   |  | Metrics  |  |   Auth   |  |  Config  |         |
|  |   API    |  |  Export  |  |  Engine  |  | Manager  |         |
|  +----------+  +----------+  +----------+  +----------+         |
+------------------------------------------------------------------+
```

- **Ring 0** -- CPU-pinned reactor loop, zero heap allocations, SPSC queues, optional Cranelift JIT compilation
- **Ring 1** -- Tokio async runtime for WAL, checkpointing, connectors, changelog draining
- **Ring 2** -- Admin API, metrics, auth, configuration (planned; crate stubs exist)

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

---

## Checkpointing and Recovery

LaminarDB provides exactly-once semantics through a coordinated checkpointing system:

1. **Ring 0 Changelog** -- Every state mutation is captured by `ChangelogAwareStore` (~2-5ns overhead per mutation)
2. **Per-Core WAL** -- Each CPU core writes to its own WAL segment with CRC32C checksums and torn write detection
3. **Incremental Checkpoints** -- Only changed state is written; backed by directory-based delta snapshots
4. **Checkpoint Coordinator** -- Orchestrates consistent snapshots across all operators and sinks using Chandy-Lamport barriers
5. **Two-Phase Commit** -- Sinks participate in the checkpoint protocol with pre-commit/commit phases
6. **Recovery** -- `RecoveryManager` restores from the latest checkpoint manifest, replays WAL, resumes connectors from committed offsets

```rust
use laminar_db::LaminarDB;

let db = LaminarDB::builder()
    .storage_dir("./data")
    .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
        interval: std::time::Duration::from_secs(30),
        ..Default::default()
    })
    .build()
    .await?;
```

---

## JIT Compilation

LaminarDB optionally compiles DataFusion logical plans into native machine code via Cranelift for Ring 0 execution. Enable with the `jit` feature flag.

The `AdaptiveQueryRunner` runs queries interpreted first, compiles in the background, and hot-swaps to compiled execution when ready -- zero downtime during compilation.

```toml
[dependencies]
laminar-db = { version = "0.16", features = ["jit"] }
```

---

## Derive Macros

The `laminar-derive` crate provides procedural macros to eliminate boilerplate:

```rust
use laminar_derive::{Record, FromRow, ConnectorConfig};

// Typed data ingestion
#[derive(Record)]
struct Trade {
    symbol: String,
    price: f64,
    volume: i64,
    #[event_time]
    ts: i64,
}

// Typed result consumption
#[derive(FromRow)]
struct OhlcBar {
    symbol: String,
    window_start: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

// Connector configuration parsing
#[derive(ConnectorConfig)]
struct MySourceConfig {
    #[config(key = "bootstrap.servers", required)]
    bootstrap_servers: String,

    #[config(key = "batch.size", default = "1000")]
    batch_size: usize,

    #[config(key = "timeout.ms", default = "30000", duration_ms)]
    timeout: std::time::Duration,
}
```

---

## Distributed Mode (Delta Architecture)

LaminarDB extends from single-process to multi-node via the Delta architecture. Enable with the `delta` feature flag.

Implemented:
- Gossip-based node discovery (chitchat)
- Raft metadata consensus (openraft)
- Partition ownership with epoch fencing
- Cross-node gRPC RPC (tonic)
- Distributed checkpoint coordination
- Partitioned lookups and cross-node aggregation

```toml
[dependencies]
laminar-db = { version = "0.16", features = ["delta"] }
```

---

## Performance Targets

| Metric | Target | Measured | Benchmark |
|--------|--------|----------|-----------|
| State lookup | < 500ns p99 | 10-105ns | `cargo bench --bench state_bench` |
| Throughput/core | 500K events/sec | 1.1-1.46M events/sec | `cargo bench --bench throughput_bench` |
| p99 latency | < 10us | 0.55-1.16us | `cargo bench --bench latency_bench` |
| Checkpoint recovery | < 10s | 1.39ms | `cargo bench --bench checkpoint_bench` |
| Window trigger | < 10us | -- | `cargo bench --bench window_bench` |
| Changelog overhead | ~2-5ns/mutation | -- | `ChangelogAwareStore` wrapper |

Run all benchmarks:

```bash
cargo bench
```

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for measured baselines on real hardware. Available benchmark suites (17 in laminar-core, 2 in laminar-storage):

```bash
cargo bench --bench state_bench          # State lookup latency
cargo bench --bench throughput_bench     # Throughput per core
cargo bench --bench latency_bench        # p99 latency
cargo bench --bench window_bench         # Window operations
cargo bench --bench join_bench           # Join throughput
cargo bench --bench dag_bench            # DAG pipeline
cargo bench --bench lookup_join_bench    # Lookup join throughput
cargo bench --bench cache_bench          # foyer cache hit/miss
cargo bench --bench compiler_bench       # JIT compilation
cargo bench --bench streaming_bench      # Streaming channels
cargo bench --bench subscription_bench   # Subscription dispatch
cargo bench --bench reactor_bench        # Reactor loop
cargo bench --bench tpc_bench            # Thread-per-core
cargo bench --bench checkpoint_bench     # Checkpoint cycle
cargo bench --bench wal_bench            # WAL write throughput
```

---

## Comparison

| Feature | LaminarDB | Apache Flink | Kafka Streams | RisingWave | Materialize |
|---------|-----------|--------------|---------------|------------|-------------|
| Deployment | Embedded | Distributed | Embedded | Distributed | Distributed |
| Latency | < 1us | ~10ms | ~1ms | ~10ms | ~10ms |
| SQL support | Full (DataFusion) | Limited | None | Full | Full |
| Exactly-once | Yes | Yes | Yes | Yes | Yes |
| No JVM | Yes | No | No | Yes | Yes |
| Windows | Tumble/Slide/Session/Hop | All | Tumble/Slide/Session | Tumble/Hop | N/A |
| CDC sources | PostgreSQL, MySQL | Many | Debezium only | PostgreSQL, MySQL | PostgreSQL |
| Lakehouse sinks | Delta Lake, Iceberg | Limited | No | Delta Lake, Iceberg | No |
| JIT compilation | Yes (Cranelift) | No | No | No | No |
| License | Apache-2.0 | Apache-2.0 | Apache-2.0 | Apache-2.0 | BSL |

---

## Project Structure

```
crates/
  laminar-core/        Core engine: reactor, operators, state, windows, joins, JIT compiler
  laminar-sql/         SQL layer: DataFusion integration, streaming SQL parser
  laminar-storage/     Durability: WAL, checkpointing, per-core WAL, recovery
  laminar-connectors/  Connectors: Kafka, CDC, WebSocket, Delta Lake, Iceberg, SDK
  laminar-db/          Unified database facade, checkpoint coordination, FFI API
  laminar-auth/        Authentication and authorization (planned -- stubs only)
  laminar-admin/       Admin REST API (planned -- stubs only)
  laminar-observe/     Observability: metrics, tracing (planned -- stubs only)
  laminar-derive/      Derive macros: Record, FromRecordBatch, FromRow, ConnectorConfig
  laminar-server/      Standalone server binary (skeleton)
examples/
  demo/                Market data TUI demo with Ratatui
  binance-ws/          Live Binance WebSocket streaming SQL demo
```

### Crate Dependency Graph

```
laminar-db  (facade)
  |-- laminar-core        (Ring 0 engine)
  |-- laminar-sql         (SQL parsing + DataFusion)
  |-- laminar-storage     (WAL + checkpointing)
  |-- laminar-connectors  (external connectors)
        |-- laminar-core
        |-- laminar-storage

laminar-derive            (proc macros, no internal deps)
laminar-server            (binary: laminar-db + laminar-admin + laminar-auth + laminar-observe)
```

---

## Project Status

LaminarDB is in active development. Here is the current phase progress:

| Phase | Description | Features | Status |
|-------|-------------|----------|--------|
| Phase 1 | Core Engine | 12/12 | Complete |
| Phase 1.5 | Production SQL Parser | 1/1 | Complete |
| Phase 2 | Production Hardening | 38/38 | Complete |
| Phase 2.5 | JIT Compiler | 12/12 | Complete |
| Phase 3 | Connectors & Integration | 85/100 | 85% complete |
| Phase 4 | Enterprise Security | 0/11 | Planned (stubs exist) |
| Phase 5 | Admin & Observability | 0/10 | Planned (stubs exist) |
| Phase 6a | Delta Partition-Parallel | 27/29 | 93% complete |
| Phase 6b | Delta Foundation | 14/14 | Complete |
| Phase 6c | Delta Production Hardening | 0/10 | Planned |
| Perf Optimization | Architectural improvements | 0/12 | Planned |
| **Total** | | **189/249** | **76%** |

See [docs/features/INDEX.md](docs/features/INDEX.md) for the full feature tracking breakdown and [docs/ROADMAP.md](docs/ROADMAP.md) for the phase timeline.

---

## Feature Flags

| Flag | Crate | Description |
|------|-------|-------------|
| `kafka` | laminar-db, laminar-connectors | Kafka source/sink, Avro serde, Schema Registry |
| `postgres-cdc` | laminar-db, laminar-connectors | PostgreSQL CDC source via logical replication |
| `postgres-sink` | laminar-db, laminar-connectors | PostgreSQL sink via COPY BINARY |
| `mysql-cdc` | laminar-db, laminar-connectors | MySQL CDC source via binlog replication |
| `delta-lake` | laminar-db, laminar-connectors | Delta Lake sink and source |
| `delta-lake-s3` | laminar-connectors | S3 storage backend for Delta Lake |
| `delta-lake-azure` | laminar-connectors | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | laminar-connectors | GCS storage backend for Delta Lake |
| `websocket` | laminar-db, laminar-connectors | WebSocket source and sink connectors |
| `jit` | laminar-db, laminar-core | Cranelift JIT query compilation |
| `ffi` | laminar-db | C FFI with Arrow C Data Interface |
| `api` | laminar-db | FFI-friendly API module for language bindings |
| `delta` | laminar-db, laminar-core | Distributed delta mode (gossip, Raft, gRPC) |
| `allocation-tracking` | laminar-core | Panic on hot-path allocation |
| `io-uring` | laminar-core | Linux 5.10+ io_uring integration |
| `hwloc` | laminar-core | Enhanced NUMA topology discovery |
| `xdp` | laminar-core | Linux eBPF/XDP network optimization |

---

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

# Format check
cargo fmt --all -- --check

# Generate API docs
cargo doc --no-deps --open

# Run the market data demo
cargo run -p laminardb-demo

# Run benchmarks
cargo bench
```

---

## Language Bindings

| Language | Package | Status |
|----------|---------|--------|
| **Rust** | [`laminar-db`](https://crates.io/crates/laminar-db) | Primary API |
| **Python** | [`laminardb`](https://pypi.org/project/laminardb/) | Via [laminardb-python](https://github.com/laminardb/laminardb-python) |
| C/C++ | Via FFI (`--features ffi`) | Implemented (Arrow C Data Interface) |
| Java | laminardb-java | Planned |
| Node.js | @laminardb/node | Planned |

---

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) -- Three-ring design, data flow, state management
- [SQL Reference](docs/SQL_REFERENCE.md) -- Streaming SQL dialect, gotchas, tested patterns
- [Development Roadmap](docs/ROADMAP.md) -- Phases, milestones, and feature status
- [Feature Index](docs/features/INDEX.md) -- All 249 features with specifications
- [Contributing Guide](CONTRIBUTING.md) -- How to build, test, and submit PRs
- [API Reference](https://docs.rs/laminar-db) -- Rustdoc API documentation

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style guidelines, Ring 0 rules, and the pull request process.

---

## License

Apache License 2.0 -- see [LICENSE](LICENSE) for details.
