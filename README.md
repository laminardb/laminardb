[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org)

# LaminarDB

A streaming SQL engine for Rust — embed it in your process or run it standalone.

Stream processing today means running a JVM cluster (Flink), paying for a managed service (RisingWave, Confluent), or hand-rolling state machines. LaminarDB brings continuous SQL queries, event-time windowing, and exactly-once checkpointing to a single Rust crate. Rust gives you predictable latency without GC pauses and memory safety without a runtime. No JVM. No cluster. `cargo add laminar-db` and go.

## Quick Start

### Rust

```toml
[dependencies]
laminar-db = "0.18"
tokio = { version = "1", features = ["full"] }
```

```rust
use laminar_db::LaminarDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    db.execute("CREATE SOURCE trades (
        symbol VARCHAR NOT NULL,
        price DOUBLE NOT NULL,
        volume BIGINT NOT NULL,
        ts BIGINT NOT NULL,
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    )").await?;

    db.execute("CREATE STREAM vwap AS
        SELECT symbol,
               SUM(price * CAST(volume AS DOUBLE)) / SUM(CAST(volume AS DOUBLE)) AS vwap,
               COUNT(*) AS trades
        FROM trades
        GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
        EMIT ON WINDOW CLOSE
    ").await?;

    let source = db.source_untyped("trades")?;
    db.start().await?;

    // Push Arrow RecordBatches via source.push(batch)
    // Read results via db.subscribe::<T>("vwap")

    db.shutdown().await?;
    Ok(())
}
```

This example compiles and runs against the `laminar-db` v0.18 public API. See [`examples/binance-ws`](examples/binance-ws) for a complete working demo that streams live Binance trades through 18 SQL pipeline stages with a TUI dashboard.

### Python

Python bindings are available via the separate [`laminardb-python`](https://github.com/laminardb/laminardb-python) repository:

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
conn.close()
```

---

## What It Is

LaminarDB is a streaming SQL engine built on [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/). It runs continuous queries over unbounded data streams, manages windowed state, and handles event-time semantics including watermarks and late data.

Three deployment modes:

| Mode | How | Status |
|------|-----|--------|
| **Embedded** | `cargo add laminar-db` — runs inside your Rust process | ✅ Implemented |
| **Standalone** | `laminardb` binary with HTTP API, configurable via TOML | ✅ Implemented |
| **Distributed** | Multi-node via gossip discovery, Raft consensus, gRPC — `--features delta` | 🔧 Implemented; production hardening pending (Phase 6c) |

The embedded mode is the primary deployment target. You get a `LaminarDB` handle, register sources with SQL DDL, push `RecordBatch` data in, subscribe to output streams, and let the engine handle windowing, joins, checkpointing, and exactly-once delivery.

---

## Streaming SQL

Standard SQL with streaming extensions. Built on DataFusion 52.

### Window Types

| Window | Syntax | Status |
|--------|--------|--------|
| Tumbling | `TUMBLE(ts, INTERVAL '1' MINUTE)` | ✅ |
| Sliding / Hopping | `HOP(ts, INTERVAL '10' SECOND, INTERVAL '5' SECOND)` | ✅ |
| Session | `SESSION(ts, INTERVAL '30' SECOND)` | ✅ |
| Cumulate | `CUMULATE(ts, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)` | 🔧 Parsed, not yet in streaming pipeline |

```sql
-- 1-minute OHLC bars
CREATE STREAM ohlc_1m AS
SELECT symbol,
       FIRST_VALUE(price) AS open,
       MAX(price) AS high, MIN(price) AS low,
       LAST_VALUE(price) AS close,
       SUM(volume) AS volume
FROM trades
GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;

-- Session windows: detect activity bursts
CREATE STREAM user_sessions AS
SELECT user_id, COUNT(*) AS clicks,
       MAX(ts) - MIN(ts) AS duration_ms
FROM clickstream
GROUP BY user_id, SESSION(ts, INTERVAL '30' SECOND)
EMIT ON WINDOW CLOSE;
```

### Join Types

| Join | Description | Status |
|------|-------------|--------|
| Inner / Left / Right / Full | Standard SQL joins | ✅ |
| Left Semi / Anti | Existence checks | ✅ |
| ASOF | Point-in-time lookup (backward, forward, nearest) | ✅ |
| Lookup | Enrichment against reference tables (Postgres, Parquet) | ✅ |
| Stream-Stream | Time-bounded joins between two streams | ✅ |
| Temporal | Versioned joins with time-validity semantics (`FOR SYSTEM_TIME AS OF`) | ✅ |

```sql
-- ASOF join: latest trade price for each order
SELECT o.*, t.price AS last_trade_price
FROM orders o
ASOF JOIN trades t ON o.symbol = t.symbol AND o.ts >= t.ts;

-- Lookup join against external Postgres table
CREATE LOOKUP TABLE instruments FROM POSTGRES (
    hostname = 'db.example.com', port = '5432',
    database = 'market', query = 'SELECT * FROM instruments'
);

SELECT t.symbol, t.price, i.sector, i.exchange
FROM trades t
JOIN instruments i ON t.symbol = i.symbol;
```

### EMIT Strategies

| Strategy | Behavior |
|----------|----------|
| `EMIT ON WINDOW CLOSE` | Emit once when the window closes |
| `EMIT CHANGES` | Emit insert/retract pairs (Z-set changelog) on every update |
| `EMIT FINAL` | Alias for ON WINDOW CLOSE |

`EMIT CHANGES` produces `+1` insert and `-1` retraction pairs, enabling correct incremental computation and cascading materialized views.

### Watermarks and Event Time

```sql
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts BIGINT,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
);
```

Watermark types: per-partition, per-key, and alignment groups (synchronized across related sources for join correctness). Late data can be dropped or redirected to a side output.

### DDL

```sql
CREATE SOURCE ... [FROM connector(...)]
CREATE STREAM ... AS SELECT ...
CREATE MATERIALIZED VIEW ... AS SELECT ...
CREATE SINK ... INTO connector(...) AS SELECT ...
CREATE LOOKUP TABLE ... FROM POSTGRES(...) | PARQUET(...)
DROP SOURCE | STREAM | SINK | MATERIALIZED VIEW
SHOW SOURCES | STREAMS | SINKS | MATERIALIZED VIEWS
SHOW CREATE SOURCE name
DESCRIBE [EXTENDED] table_name
EXPLAIN ANALYZE SELECT ...
```

All aggregation functions from DataFusion 52 are available: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST_VALUE`, `LAST_VALUE`, `STDDEV`, `PERCENTILE_CONT`, `APPROX_COUNT_DISTINCT`, `LAG`, `LEAD`, `ROW_NUMBER`, and 40+ more. JSON extraction, array/struct/map functions, and `UNNEST` are also supported.

---

## Connectors

Feature-gated connectors for external systems. Each implements `SourceConnector` or `SinkConnector` with two-phase commit for exactly-once semantics.

### Sources

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Consumer group, Schema Registry, Avro/JSON/CSV/Debezium | ✅ |
| PostgreSQL CDC | `postgres-cdc` | Logical replication (pgoutput), Z-set changelog | ✅ |
| MySQL CDC | `mysql-cdc` | Binlog replication, GTID position tracking | ✅ |
| WebSocket Client | `websocket` | Connect to external WebSocket servers | ✅ |
| WebSocket Server | `websocket` | Accept incoming WebSocket connections | ✅ |
| Delta Lake | `delta-lake` | Read from Delta Lake tables, version polling | ✅ |
| Files (AutoLoader) | `files` | Glob pattern discovery, watch mode, Parquet/CSV | ✅ |
| Parquet Lookup | `parquet-lookup` | Read Parquet files as reference tables | ✅ |
| Postgres Lookup | -- | Query external Postgres tables for enrichment | ✅ |

### Sinks

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Exactly-once transactions, configurable partitioning | ✅ |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, co-transactional exactly-once | ✅ |
| Delta Lake | `delta-lake` | S3/Azure/GCS, epoch-aligned Parquet commits | ✅ |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | ✅ |
| WebSocket Client | `websocket` | Push to external WebSocket server | ✅ |
| Files | `files` | Parquet/CSV with timestamp/partition templates | ✅ |
| Apache Iceberg | -- | -- | 📋 Planned |

Cloud storage backends for Delta Lake: S3 (`delta-lake-s3`), Azure ADLS (`delta-lake-azure`), GCS (`delta-lake-gcs`). Supports Unity and Glue catalogs.

### Connector Example

```sql
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts BIGINT,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) FROM KAFKA (
    brokers = '${KAFKA_BROKERS}',
    topic = 'market-trades',
    group_id = 'laminar-analytics',
    format = 'json',
    offset_reset = 'earliest'
);

CREATE SINK trade_archive INTO DELTA_LAKE (
    path = 's3://my-bucket/trade_summary',
    write_mode = 'append',
    delivery.guarantee = 'exactly-once'
) AS SELECT * FROM trade_summary;
```

Supported formats: `json`, `csv`, `avro` (with Schema Registry), `raw` (bytes), `debezium` (CDC envelope).

Custom connectors can be built using the `SourceConnector` / `SinkConnector` traits with retry policies, circuit breakers, and rate limiters from the connector SDK.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                     SOURCE CONNECTORS                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ Kafka    │  │ Postgres │  │  File    │  tokio tasks      │
│  │ Source   │  │ CDC      │  │ Source   │                   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
│       │ mpsc         │ mpsc        │ mpsc                    │
│       └──────┬───────┘─────────────┘                         │
│              ▼                                                │
├──────────────────────────────────────────────────────────────┤
│                  STREAMING COORDINATOR                        │
│  Single tokio task: SQL cycles, compiled projections,         │
│  cached logical plans, checkpoint barriers                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Compiled │→ │ Operators│→ │  State   │→ │  Sink    │    │
│  │Projection│  │ (window, │  │  Store   │  │ Writers  │    │
│  │/ Cached  │  │  join,   │  │ (ahash/  │  │          │    │
│  │  Plans   │  │  filter) │  │  foyer)  │  │          │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
├──────────────────────────────────────────────────────────────┤
│                     BACKGROUND I/O                            │
│  Tokio async runtime, bounded latency impact                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │Checkpoint│  │   WAL    │  │Changelog │  │  Timer   │    │
│  │ Manager  │  │  Writer  │  │ Drainer  │  │  Wheel   │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
├──────────────────────────────────────────────────────────────┤
│                      CONTROL PLANE                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │  Admin   │  │ Metrics  │  │  Config  │                  │
│  │   API    │  │  Export  │  │ Manager  │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
└──────────────────────────────────────────────────────────────┘
```

- **Streaming coordinator** — Single tokio task driving SQL execution cycles. Source connectors push batches via mpsc channels; the coordinator runs compiled projections / cached logical plans, routes results to sinks, and manages checkpoint barriers. Sub-microsecond for compiled single-source projections; microseconds for incremental aggregations and cached-plan queries; DataFusion fallback for complex queries.
- **Background I/O** — Tokio async runtime handling WAL writes, checkpointing, connector I/O, and changelog draining.
- **Admin** — HTTP admin API, metrics, configuration management. Auth and observability are planned (Phase 4/5).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

### Checkpointing and Recovery

1. **WAL** — Write-ahead log segments with CRC32C checksums and torn write detection
2. **Coordinated Snapshots** — Barrier-based checkpoint protocol: coordinator injects barriers into all sources; operators with multiple inputs align barriers before snapshotting
3. **Two-Phase Commit** — Sinks participate in pre-commit/commit phases for exactly-once delivery
4. **Recovery** — `RecoveryManager` restores from the latest checkpoint manifest, replays WAL, resumes connectors from committed offsets

```rust
// Note: StreamCheckpointConfig is from laminar-core (add as a dependency)
let db = LaminarDB::builder()
    .storage_dir("./data")
    .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
        interval: std::time::Duration::from_secs(30),
        ..Default::default()
    })
    .build()
    .await?;
```

On crash, events between the last completed checkpoint and the crash are lost. Checkpoint interval is configurable; shorter intervals reduce the data loss window at the cost of higher I/O overhead.

### Compiled Query Execution

Non-aggregate single-source queries are compiled to `PhysicalExpr` projections on first execution, eliminating per-cycle SQL parsing overhead. Complex queries cache their optimized logical plans to skip repeated planning.

---

## Benchmarks

Benchmark suites are in `crates/laminar-core/benches/`, `crates/laminar-storage/benches/`, and `crates/laminar-db/benches/` (18 total, using Criterion):

| Metric | Target | Measured (mean) | Benchmark File |
|--------|--------|-----------------|----------------|
| State lookup | < 500ns | 10–105ns | `state_bench.rs` |
| Throughput/core | 500K events/sec | 1.1–1.46M events/sec | `throughput_bench.rs` |
| Event latency | < 10us | 0.55–1.16us | `latency_bench.rs` |
| Checkpoint recovery | < 10s | 1.39ms | `checkpoint_bench.rs` |

These are Criterion mean latencies measured on development hardware (AMD Ryzen AI 7 350, see [BENCHMARKS.md](docs/BENCHMARKS.md)) against minimal operator chains (single tumbling window for latency, single state lookup for state bench). Real pipelines with multiple operators, joins, and state lookups will show higher latency. p99 under sustained load is not yet measured. Run `cargo bench` to measure on your own hardware.

Additional benchmark suites: `window_bench`, `join_bench`, `lookup_join_bench`, `cache_bench`, `compiler_bench`, `streaming_bench`, `subscription_bench`, `reactor_bench`, `tpc_bench`, `dag_bench`, `dag_stress`, `wal_bench`, `io_uring_bench`, `recovery_bench`.

---

## Language Bindings

| Language | Package | Status |
|----------|---------|--------|
| **Rust** | [`laminar-db`](https://crates.io/crates/laminar-db) | Primary API |
| **Python** | [`laminardb`](https://pypi.org/project/laminardb/) | Via [`laminardb-python`](https://github.com/laminardb/laminardb-python) (separate repo) |
| **C/C++** | Via FFI (`--features ffi`) | ✅ Arrow C Data Interface, 78 exported functions |
| Java | `laminardb-java` | 📋 Planned |
| Node.js | `@laminardb/node` | 📋 Planned |

---

## Why LaminarDB vs. X?

| | LaminarDB | Apache Flink | Kafka Streams | RisingWave | kdb+ |
|---|---|---|---|---|---|
| Deployment | Library or binary | JVM cluster | JVM library | Distributed cluster | Proprietary binary |
| Language | Rust (+ Python, C FFI) | Java/Scala/Python | Java | Rust/Python/SQL | Q/Python |
| Embed in your process | Yes | No | Yes (JVM) | No | No |
| Latency (per-event, microbench) | Sub-microsecond (compiled queries, microbench) | Milliseconds–seconds | Milliseconds | Milliseconds | Microseconds |
| Operational overhead | None (embedded) or single binary | K8s operator or YARN | Kafka cluster | etcd/K8s/S3 | Vendor support |
| SQL | Full (DataFusion) | Flink SQL | None (DSL only) | Full (Postgres-compatible) | Q language |
| Exactly-once | Yes (checkpoint + 2PC) | Yes | Yes | Yes | No |
| License | Apache-2.0 | Apache-2.0 | Apache-2.0 | Apache-2.0 | Commercial |
| Maturity | Pre-1.0, active development | Production (10+ years) | Production | Production | Production (25+ years) |

**Where competitors are stronger:**
- Flink has a decade of production hardening, hundreds of connectors, TB-scale state management (RocksDB backend), and a massive ecosystem.
- kdb+ has unmatched time-series query performance, handles billions of rows in-memory, and has decades of finance-specific optimization.
- RisingWave offers managed cloud deployment with zero operational overhead and Postgres wire compatibility.
- Kafka Streams has been validated across thousands of production deployments for JVM-based event processing at scale.

**Where LaminarDB fits:**
- You want stream processing without a cluster or JVM dependency.
- You need sub-millisecond latency and are willing to work with a pre-1.0 project.
- You want to embed a streaming SQL engine directly in a Rust, C, or Python application.

---

## Project Status

**Version 0.18.0** — active development, pre-1.0. APIs may change between minor versions.

| Phase | Description | Progress |
|-------|-------------|----------|
| Phase 1 | Core Engine | ✅ 12/12 |
| Phase 1.5 | SQL Parser | ✅ 1/1 |
| Phase 2 | Production Hardening | ✅ 38/38 |
| Phase 2.5 | JIT Compiler | ❌ Removed |
| Phase 3 | Connectors & Integration | 🔧 85/100 |
| Phase 4 | Enterprise Security (Auth, RBAC) | 📋 Planned |
| Phase 5 | Admin & Observability | 📋 Planned |
| Phase 6a | Distributed Partition-Parallel | ✅ 27/29 (+2 superseded) |
| Phase 6b | Distributed Foundation | ✅ 14/14 |
| Phase 6c | Distributed Production Hardening | 📋 Planned |

Test coverage: 3,000+ tests (unit + integration). CI runs on Linux and Windows.

See [docs/ROADMAP.md](docs/ROADMAP.md) for the full phase timeline.

---

## Feature Flags

| Flag | Description |
|------|-------------|
| `kafka` | Kafka source/sink, Avro serde, Schema Registry |
| `postgres-cdc` | PostgreSQL CDC source via logical replication |
| `postgres-sink` | PostgreSQL sink via COPY BINARY |
| `mysql-cdc` | MySQL CDC source via binlog replication |
| `delta-lake` | Delta Lake source and sink |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source and sink (Parquet, CSV) |
| `ffi` | C FFI with Arrow C Data Interface |
| `delta` | Distributed mode (gossip, Raft, gRPC) |
| `parquet-lookup` | Parquet lookup source for reference tables |
| `allocation-tracking` | Panic on hot-path allocation (`laminar-core` only) |
| `io-uring` | Linux 5.10+ io_uring integration |

---

## Building from Source

```bash
# Prerequisites: Rust 1.85+ (stable)
git clone https://github.com/laminardb/laminardb.git
cd laminardb

cargo build --release
cargo test
cargo clippy -- -D warnings
cargo bench                    # Run all benchmarks
cargo doc --no-deps --open     # Generate API docs

# With optional connectors
cargo test --features kafka,postgres-cdc,mysql-cdc,delta-lake,websocket

# Run the Binance WebSocket demo
cargo run -p binance-ws
```

## Project Structure

```text
crates/
  laminar-core/        Core engine: reactor, operators, state, windows, joins
  laminar-sql/         SQL parser, DataFusion integration, streaming optimizer
  laminar-storage/     WAL, checkpointing, per-core WAL, recovery
  laminar-connectors/  Kafka, CDC, WebSocket, Delta Lake, files, connector SDK
  laminar-db/          Unified database facade, checkpoint coordination, FFI
  laminar-derive/      Derive macros: Record, FromRecordBatch, FromRow, ConnectorConfig
  laminar-server/      Standalone server binary (HTTP API, Docker, Helm)
examples/
  demo/                Market data TUI demo with Ratatui
  binance-ws/          Live Binance WebSocket streaming SQL demo
  microstructure/      Market microstructure analysis demo
```

---

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) — Three-ring design, data flow, state management
- [SQL Reference](docs/SQL_REFERENCE.md) — Streaming SQL dialect, tested patterns, gotchas
- [Roadmap](docs/ROADMAP.md) — Phases, milestones, feature status
- [API Reference](https://docs.rs/laminar-db) — Rustdoc

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, Ring 0 rules, and the PR process.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
