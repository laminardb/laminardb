# LaminarDB

**Ultra-low latency embedded streaming SQL database written in Rust.**

[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org)

Think "SQLite for stream processing." LaminarDB is an embedded streaming database that lets you run continuous SQL queries over real-time data with sub-microsecond latency. No cluster, no JVM, no external dependencies -- just link it as a Rust library or use the Python bindings.

Built on [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/), LaminarDB targets use cases like financial market data, IoT edge analytics, and real-time application state where every microsecond counts.

## Install

### Rust (crates.io)

```bash
cargo add laminar-db
```

LaminarDB is published to [crates.io](https://crates.io/crates/laminar-db) as six workspace crates:

| Crate | Description |
|-------|-------------|
| [`laminar-db`](https://crates.io/crates/laminar-db) | Unified database facade (start here) |
| [`laminar-core`](https://crates.io/crates/laminar-core) | Reactor, operators, state stores, windows, joins |
| [`laminar-sql`](https://crates.io/crates/laminar-sql) | DataFusion integration, streaming SQL parser |
| [`laminar-storage`](https://crates.io/crates/laminar-storage) | WAL, checkpointing, RocksDB backend |
| [`laminar-connectors`](https://crates.io/crates/laminar-connectors) | Kafka, PostgreSQL CDC, MySQL CDC, Delta Lake |
| [`laminar-derive`](https://crates.io/crates/laminar-derive) | `#[derive(Record, FromRecordBatch)]` macros |

### Python (PyPI)

```bash
pip install laminardb
```

See the [laminardb-python](https://github.com/laminardb/laminardb-python) repository for the full Python API.

## Quick Start

### Rust

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

## Windows

LaminarDB supports four window types for time-based aggregation over streaming data.

### Tumbling Windows

Fixed-size, non-overlapping windows. Each event belongs to exactly one window.

```sql
CREATE STREAM trades_1m AS
SELECT symbol, SUM(volume) AS total_volume, COUNT(*) AS trade_count
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

### Sliding Windows

Overlapping windows with configurable size and slide interval. An event may appear in multiple windows.

```sql
CREATE STREAM moving_avg AS
SELECT symbol, AVG(price) AS avg_price
FROM trades
GROUP BY symbol, slide(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

### Hopping Windows

Alias for sliding windows -- size and hop interval define how windows overlap.

```sql
SELECT sensor_id, MAX(temp) AS peak_temp
FROM readings
GROUP BY sensor_id, hop(ts, INTERVAL '10' SECOND, INTERVAL '5' SECOND);
```

### Session Windows

Dynamic windows that close after a configurable gap of inactivity. Events within the gap are grouped into the same session.

```sql
CREATE STREAM user_sessions AS
SELECT user_id,
       COUNT(*) AS clicks,
       MAX(ts) - MIN(ts) AS duration_ms
FROM clickstream
GROUP BY user_id, session(ts, INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;
```

Session windows support:
- **Merging**: overlapping sessions merge into a single window
- **Per-key state**: each key maintains its own session independently
- **Watermark closure**: sessions close when the watermark passes the gap threshold

## EMIT Strategies

The `EMIT` clause controls when window results are produced downstream.

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `EMIT ON WINDOW CLOSE` | Emit once when the window closes | Final aggregates (OHLC bars, billing) |
| `EMIT CHANGES` | Emit insert/retract pairs on every update | Live dashboards, cascading MVs |
| `EMIT FINAL` | Emit only the final value when the window closes | Same as ON WINDOW CLOSE (explicit alias) |

```sql
-- Emit running updates as a changelog (insert + retract pairs)
CREATE STREAM live_totals AS
SELECT symbol, SUM(volume) AS running_volume
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
EMIT CHANGES;

-- Emit only the final bar when the window closes
CREATE STREAM ohlc_1m AS
SELECT symbol,
       FIRST(price) AS open, MAX(price) AS high,
       MIN(price) AS low, LAST(price) AS close,
       SUM(volume) AS volume
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

`EMIT CHANGES` uses the **Z-set changelog model** (DBSP/Feldera-inspired): each update produces a `+1` insert for the new value and a `-1` retraction for the previous value, enabling correct incremental downstream computation including cascading materialized views.

## Watermarks and Event Time

LaminarDB uses event-time processing with watermarks to handle out-of-order data.

### Watermarks

Watermarks declare "all events with timestamp <= W have arrived." Windows close when the watermark passes their end boundary.

```sql
-- Bounded out-of-orderness: allow up to 5 seconds of late data
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts BIGINT
) WITH (
    watermark = 'ts',
    allowed_lateness = INTERVAL '5' SECOND
);
```

### Watermark Granularity

| Type | Description |
|------|-------------|
| **Per-partition** | Independent watermark per source partition -- one slow partition doesn't block others |
| **Per-key** | Independent watermark per key (e.g., per symbol) for heterogeneous data rates |
| **Alignment groups** | Synchronized watermarks across related sources for join correctness |

### Late Data Handling

Events arriving after the watermark passes can be:
- **Dropped** (default) -- silently discarded
- **Redirected to a side output** -- captured for reprocessing or alerting

```sql
-- Allow 10 seconds of late data before dropping
CREATE STREAM results AS
SELECT symbol, COUNT(*) AS cnt
FROM trades
GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
WITH (allowed_lateness = INTERVAL '10' SECOND);
```

## Checkpointing and Recovery

LaminarDB provides exactly-once semantics through a coordinated checkpointing system.

### How It Works

1. **Ring 0 Changelog**: Every state mutation is captured in a zero-allocation changelog (~2-5ns overhead per mutation) via `ChangelogAwareStore`
2. **Per-Core WAL**: Each CPU core writes to its own WAL segment -- no locks, no contention, epoch-ordered
3. **Incremental Checkpoints**: Only changed state is written, backed by RocksDB for efficient delta snapshots
4. **Checkpoint Coordinator**: Orchestrates consistent snapshots across all operators, sinks, and connectors using Chandy-Lamport barriers
5. **Two-Phase Commit**: Sinks (Kafka, PostgreSQL, Delta Lake) participate in the checkpoint protocol with pre-commit/commit phases

### Recovery

On restart, the `RecoveryManager` restores the last consistent checkpoint:
- Loads the `CheckpointManifest` (operator states, connector offsets, sink positions)
- Restores state stores from the incremental RocksDB snapshot
- Replays WAL entries written after the checkpoint
- Resumes connectors from their checkpointed offsets -- no duplicate processing

```rust
// Enable checkpointing with 30-second intervals
let db = LaminarDB::builder()
    .storage_dir("./data")
    .checkpoint(CheckpointConfig {
        interval: Duration::from_secs(30),
        ..Default::default()
    })
    .build().await?;
```

### Exactly-Once Sinks

Sinks participate in the checkpoint protocol to guarantee exactly-once delivery:

| Sink | Mechanism |
|------|-----------|
| **Kafka** | Kafka transactions -- begin on epoch start, commit on checkpoint |
| **PostgreSQL** | Co-transactional -- sink writes + offset update in the same DB transaction |
| **Delta Lake** | Epoch-aligned Parquet file commits with atomic manifest updates |

## Connectors

### Kafka Source

Consume from Kafka topics with consumer group management, backpressure, and Schema Registry integration.

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

Avro with Schema Registry:

```sql
CREATE SOURCE events (...) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'events',
    format = 'avro',
    schema.registry.url = 'http://schema-registry:8081'
);
```

### Kafka Sink

Produce to Kafka topics with exactly-once transactions, configurable partitioning, and compression.

```sql
CREATE SINK output INTO KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'trade-summaries',
    format = 'json',
    delivery.guarantee = 'exactly-once',
    compression.type = 'snappy'
) AS SELECT * FROM trade_summary;
```

### PostgreSQL CDC

Capture real-time changes from PostgreSQL using logical replication (pgoutput protocol). Changes are emitted as a Z-set changelog with `_op` column (`I`=insert, `U`=update, `D`=delete).

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

-- Aggregate CDC changes
CREATE STREAM customer_totals AS
SELECT customer_id, COUNT(*) AS total_orders, SUM(amount) AS total_spent
FROM orders_cdc
WHERE _op IN ('I', 'U')
GROUP BY customer_id
EMIT CHANGES;
```

### PostgreSQL Sink

Write results to PostgreSQL using COPY BINARY protocol with upsert support and co-transactional exactly-once delivery.

### MySQL CDC

Capture changes from MySQL using binlog decoding with GTID-based position tracking. Emits the same Z-set changelog format as PostgreSQL CDC.

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

### Delta Lake Sink

Write streaming results to Delta Lake tables on S3, Azure ADLS, or GCS with exactly-once epoch-aligned commits.

```sql
CREATE SINK lake INTO DELTA_LAKE (
    path = 's3://my-bucket/trade_summary',
    write_mode = 'append'
) AS SELECT * FROM trade_summary;
```

### Connector SDK

Build custom connectors using the built-in SDK with retry policies, circuit breakers, rate limiting, and a test harness:

```rust
use laminar_connectors::sdk::{RetryPolicy, CircuitBreaker, RateLimiter};
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

-- Cascading materialized views (MV reading from MV)
CREATE MATERIALIZED VIEW ohlc_1h AS
SELECT symbol,
       FIRST(open) AS open, MAX(high) AS high,
       MIN(low) AS low, LAST(close) AS close
FROM ohlc_1min
GROUP BY symbol, tumble(bar_start, INTERVAL '1' HOUR);

-- LAG/LEAD window functions
SELECT symbol, price,
       LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) AS prev_price,
       price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY ts) AS delta
FROM trades;

-- Ranking functions
SELECT symbol, price,
       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price DESC) AS rank
FROM trades;
```

### SQL Extensions Summary

| Extension | Description |
|-----------|-------------|
| `tumble(ts, interval)` | Fixed-size non-overlapping windows |
| `slide(ts, size, slide)` | Overlapping windows with configurable slide |
| `hop(ts, size, hop)` | Alias for sliding windows |
| `session(ts, gap)` | Gap-based dynamic windows |
| `EMIT ON WINDOW CLOSE` | Emit results when window closes |
| `EMIT CHANGES` | Emit changelog (insert/retract pairs) |
| `ASOF JOIN ... MATCH_CONDITION` | Point-in-time lookups |
| `FIRST(col)` / `LAST(col)` | First/last value aggregates (retractable) |
| `CREATE SOURCE ... FROM KAFKA(...)` | Kafka source DDL |
| `CREATE SOURCE ... FROM POSTGRES_CDC(...)` | PostgreSQL CDC DDL |
| `CREATE SOURCE ... FROM MYSQL_CDC(...)` | MySQL CDC DDL |
| `CREATE SINK ... INTO KAFKA(...)` | Kafka sink DDL |
| `CREATE SINK ... INTO DELTA_LAKE(...)` | Delta Lake sink DDL |
| `WITH (allowed_lateness = ...)` | Late data handling |
| `${VAR}` in SQL strings | Config variable substitution |

## Comparison

| Feature | LaminarDB | Apache Flink | Kafka Streams | RisingWave | Materialize |
|---------|-----------|--------------|---------------|------------|-------------|
| Deployment | Embedded | Distributed | Embedded | Distributed | Distributed |
| Latency | < 1us | ~10ms | ~1ms | ~10ms | ~10ms |
| SQL support | Full | Limited | None | Full | Full |
| Exactly-once | Yes | Yes | Yes | Yes | Yes |
| No JVM | Yes | No | No | Yes | Yes |
| Windows | Tumble/Slide/Session/Hop | All | Tumble/Slide/Session | Tumble/Hop | N/A |
| CDC sources | PostgreSQL, MySQL | Many | Debezium only | PostgreSQL, MySQL | PostgreSQL |
| Lakehouse sinks | Delta Lake, Iceberg | Limited | No | Delta Lake, Iceberg | No |
| Language bindings | Rust, Python, C | Java | Java | SQL only | SQL only |
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
| **Rust** | [`laminar-db`](https://crates.io/crates/laminar-db) | Primary |
| **Python** | [`laminardb`](https://pypi.org/project/laminardb/) | Implemented |
| C/C++ | Via FFI (`--features ffi`) | Implemented |
| Java | laminardb-java | Planned |
| Node.js | @laminardb/node | Planned |

The Python bindings use [PyO3](https://pyo3.rs/) with [pyo3-arrow](https://crates.io/crates/pyo3-arrow) for zero-copy Arrow interop. See [laminardb-python](https://github.com/laminardb/laminardb-python) for the full API.

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

### Feature Flags

| Flag | Crate | Description |
|------|-------|-------------|
| `kafka` | laminar-db | Kafka source/sink, Avro serde, Schema Registry |
| `postgres-cdc` | laminar-db | PostgreSQL CDC source via logical replication |
| `postgres-sink` | laminar-db | PostgreSQL sink via COPY BINARY |
| `mysql-cdc` | laminar-db | MySQL CDC source via binlog |
| `delta-lake` | laminar-db | Delta Lake sink (S3/Azure/GCS) |
| `rocksdb` | laminar-db | RocksDB-backed persistent table store |
| `jit` | laminar-db | Cranelift JIT query compilation |
| `ffi` | laminar-db | C FFI with Arrow C Data Interface |

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) -- Three-ring design, data flow, state management
- [Development Roadmap](docs/ROADMAP.md) -- Phases, milestones, and feature status
- [Feature Index](docs/features/INDEX.md) -- All 160 features with specifications
- [Contributing Guide](CONTRIBUTING.md) -- How to build, test, and submit PRs
- [API Reference](https://docs.rs/laminar-db) -- Rustdoc API documentation
- [Python Bindings](https://github.com/laminardb/laminardb-python) -- Python API and docs

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style guidelines, and the pull request process.

## License

Apache License 2.0 -- see [LICENSE](LICENSE) for details.
