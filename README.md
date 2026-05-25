[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange)](https://www.rust-lang.org)
[![Docker Hub](https://img.shields.io/badge/docker-laminardb%2Flaminardb--server-2496ed?logo=docker&logoColor=white)](https://hub.docker.com/r/laminardb/laminardb-server)
[![Website](https://img.shields.io/badge/website-laminardb.io-blue)](https://laminardb.io)

# LaminarDB

A streaming SQL engine for Rust. Embed it as a library or run the standalone server. Continuous queries, event-time windows, exactly-once checkpoints. No JVM, no cluster required.

## Quick Start

### Rust

```toml
[dependencies]
laminar-db = "0.23"
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
        ts TIMESTAMP NOT NULL,
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

This example compiles and runs against the `laminar-db` v0.23 public API. See [`examples/binance-ws`](examples/binance-ws) for a complete working demo that streams live Binance trades through 18 SQL pipeline stages with a TUI dashboard.

### Python

Python bindings are available via the separate [`laminardb-python`](https://github.com/laminardb/laminardb-python) repository:

```bash
pip install laminardb
```

```python
import laminardb

conn = laminardb.open(":memory:")
conn.execute("CREATE SOURCE sensors (ts TIMESTAMP, device VARCHAR, value DOUBLE)")
conn.insert("sensors", [
    {"ts": 1, "device": "sensor_a", "value": 42.0},
    {"ts": 2, "device": "sensor_b", "value": 43.5},
])
conn.sql("SELECT * FROM sensors WHERE value > 42.0").show()
conn.close()
```

---

## Deployment modes

| Mode | How |
|------|-----|
| Embedded | `cargo add laminar-db`. Runs in-process. |
| Standalone | `laminardb` binary. TOML config, REST API, Postgres wire protocol, Prometheus metrics, hot reload. |

### Prebuilt binaries

Every [GitHub release](https://github.com/laminardb/laminardb/releases/latest) attaches static `laminardb-server` binaries — no build toolchain required. Targets: **Linux** x86_64/aarch64 (`gnu` and `musl`), **macOS** Intel and Apple Silicon, **Windows** x86_64.

```bash
# Resolve the latest tag, download, extract, run
VERSION=$(curl -s https://api.github.com/repos/laminardb/laminardb/releases/latest | grep tag_name | cut -d '"' -f4)
curl -LO "https://github.com/laminardb/laminardb/releases/download/${VERSION}/laminardb-server-x86_64-unknown-linux-gnu-${VERSION}.tar.gz"
tar xzf laminardb-server-*.tar.gz
./laminardb --config laminardb.toml
```

### Docker

Multi-arch images are published to **Docker Hub** and **GHCR** on every release:

```bash
docker run -p 8080:8080 laminardb/laminardb-server:latest          # Docker Hub
docker run -p 8080:8080 ghcr.io/laminardb/laminardb-server:latest  # GHCR
```

The image ships a default config at `/etc/laminardb/laminardb.toml` (mount your own over it) and persists state in `/var/lib/laminardb`. A full `docker compose` stack — server plus Redpanda, Prometheus, and Grafana — is in [`docker-compose.yml`](docker-compose.yml).

Built on [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://datafusion.apache.org/). Embedded is the primary target.

---

## Streaming SQL

Standard SQL with streaming extensions. Built on DataFusion 52.

### Window Types

| Window | Syntax | Status |
|--------|--------|--------|
| Tumbling | `TUMBLE(ts, INTERVAL '1' MINUTE)` | ✅ |
| Sliding / Hopping | `HOP(ts, INTERVAL '10' SECOND, INTERVAL '5' SECOND)` | ✅ |
| Session | `SESSION(ts, INTERVAL '30' SECOND)` | ✅ |

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
| Interval (stream-stream) | Time-bounded join, `ts BETWEEN other.ts - INTERVAL … AND other.ts + INTERVAL …` | ✅ |
| ASOF | Point-in-time lookup — backward (`>=`), forward, or `NEAREST` | ✅ |
| Temporal Probe | Fan each left row out across fixed time offsets (e.g. markout curves) | ✅ |
| Temporal | Versioned join with time-validity (`FOR SYSTEM_TIME AS OF`) | ✅ |
| Lookup | Enrichment against reference tables (Postgres, Parquet) | ✅ |

```sql
-- ASOF join: latest trade price for each order
SELECT o.*, t.price AS last_trade_price
FROM orders o
ASOF JOIN trades t ON o.symbol = t.symbol AND o.ts >= t.ts;

-- Interval join: match orders to fills within a 10-second window
SELECT o.order_id, f.fill_price
FROM orders o
INNER JOIN fills f
ON o.order_id = f.order_id
AND f.ts BETWEEN o.ts AND o.ts + INTERVAL '10' SECOND;

-- Temporal probe join: sample a reference price at fixed horizons after each trade
SELECT t.symbol, p.offset_ms, mid AS ref_price
FROM trades t
TEMPORAL PROBE JOIN prices r
    ON (symbol) TIMESTAMPS (ts, ts)
    RANGE FROM 0s TO 30s STEP 5s AS p;

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
| `EMIT AFTER WATERMARK` / `EMIT ON WATERMARK` | Emit when the watermark advances past the window end |
| `EMIT ON WINDOW CLOSE` | Emit once when the window closes |
| `EMIT ON UPDATE` | Emit on every update (incremental) |
| `EMIT EVERY INTERVAL 'N' UNIT` (or `EMIT PERIODICALLY INTERVAL 'N' UNIT`) | Emit on a fixed time interval |
| `EMIT CHANGES` | Emit insert/retract pairs (Z-set changelog) on every update |
| `EMIT FINAL` | Emit the final result only |

`EMIT CHANGES` produces `+1` insert and `-1` retraction pairs, enabling correct incremental computation and cascading materialized views.

### Watermarks and Event Time

```sql
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts TIMESTAMP,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
);
```

Watermark types: per-partition, per-key, and alignment groups (synchronized across related sources for join correctness). Late data can be dropped, tolerated with `ALLOW LATENESS INTERVAL 'N' UNIT`, or redirected to a side output with `LATE DATA TO <sink_name>`.

### DDL

```sql
CREATE SOURCE ... [FROM connector(...)]
CREATE STREAM ... AS SELECT ...                    [WITH ('retain_history' = '64mb')]
CREATE MATERIALIZED VIEW ... AS SELECT ...
CREATE SINK ... INTO connector(...) AS SELECT ...
CREATE LOOKUP TABLE ... FROM POSTGRES(...) | PARQUET(...)
DROP SOURCE | STREAM | SINK | MATERIALIZED VIEW
SHOW SOURCES | STREAMS | SINKS | MATERIALIZED VIEWS
SHOW CREATE SOURCE name
DESCRIBE [EXTENDED] table_name
EXPLAIN ANALYZE SELECT ...
SUBSCRIBE <stream> [AS OF EPOCH n] [WHERE …]      -- live tail of a stream
DECLARE c CURSOR FOR SUBSCRIBE … ; FETCH n FROM c -- cursored consumption
```

`retain_history` keeps a bounded ring of recent committed epochs in memory; combined with `SUBSCRIBE … AS OF EPOCH n`, a client can resume from the last epoch it saw and reconnect without gaps.

All aggregation functions from DataFusion 52 are available: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `FIRST_VALUE`, `LAST_VALUE`, `STDDEV`, `PERCENTILE_CONT`, `APPROX_COUNT_DISTINCT`, `LAG`, `LEAD`, `ROW_NUMBER`, and 40+ more. JSON extraction, array/struct/map functions, and `UNNEST` are also supported.

---

## AI Functions

Call models inline in streaming SQL. Each function names a model from a registry; the model's backend — a **remote LLM** over HTTP or a **local ONNX encoder** run in-process — is hidden behind the function.

| Function | Task | Backends |
|----------|------|----------|
| `ai_classify(text, model => …, labels => ARRAY[…])` | Zero/few-shot classification | remote · local |
| `ai_sentiment(text, model => …)` | Sentiment | remote · local |
| `ai_embed(text, model => …)` | Embedding vector | remote · local |
| `ai_extract` / `ai_complete` / `ai_summarize` / `ai_translate` / `ai_gen` | Generation / extraction | remote |

```sql
CREATE STREAM flagged AS
SELECT id, headline,
       ai_sentiment(headline, model => 'finbert') AS sentiment
FROM news;
```

Inference runs **off the hot path**: the operator serves cache hits inline and hands misses to a background worker, so the streaming pipeline never blocks on a model call. Results are cached by `(content, model, params)`. Models, providers, and per-task defaults are configured in `[ai.providers.*]` / `[models.*]` / `[ai.defaults]`.

Local models are encoder-only (BERT / DistilBERT / MiniLM family) and run on **ONNX Runtime**, which is loaded at runtime — install ONNX Runtime ≥ 1.24 and set `ORT_DYLIB_PATH` (or put the library on the search path). Generative tasks require a remote provider. See the [server configuration guide](crates/laminar-server/README.md#ai-functions) for the full setup.

---

## Connectors

Feature-gated connectors for external systems. Each implements `SourceConnector` or `SinkConnector` with two-phase commit for exactly-once semantics.

### Sources

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Consumer group, Schema Registry, Avro/JSON/CSV/Debezium | ✅ |
| PostgreSQL CDC | `postgres-cdc` | Logical replication (pgoutput), Z-set changelog | ✅ |
| MySQL CDC | `mysql-cdc` | Binlog replication, GTID position tracking | ✅ |
| MongoDB CDC | `mongodb-cdc` | Change streams, resume token tracking | ✅ |
| OpenTelemetry OTLP | `otel` | OTLP/gRPC receiver for traces, metrics, and logs | ✅ |
| WebSocket Client | `websocket` | Connect to external WebSocket servers | ✅ |
| WebSocket Server | `websocket` | Accept incoming WebSocket connections | ✅ |
| Delta Lake | `delta-lake` | Read from Delta Lake tables, version polling | ✅ |
| Iceberg | `iceberg` | Apache Iceberg table source (REST/Glue/Hive catalogs) | ✅ |
| Files (AutoLoader) | `files` | Glob pattern discovery, watch mode, Parquet/CSV | ✅ |
| Parquet Lookup | `parquet-lookup` | Read Parquet files as reference tables | ✅ |
| Postgres Lookup | `postgres-cdc` | Query external Postgres tables for enrichment | ✅ |

### Sinks

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Exactly-once transactions, configurable partitioning | ✅ |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, co-transactional exactly-once | ✅ |
| MongoDB | `mongodb-cdc` | Ordered/unordered writes, upsert, CDC replay | ✅ |
| Delta Lake | `delta-lake` | S3/Azure/GCS, epoch-aligned Parquet commits | ✅ |
| Iceberg | `iceberg` | Apache Iceberg sink (REST/Glue/Hive catalogs) | ✅ |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | ✅ |
| WebSocket Client | `websocket` | Push to external WebSocket server | ✅ |
| Files | `files` | Parquet/CSV with timestamp/partition templates | ✅ |

Cloud storage backends for Delta Lake: S3 (`delta-lake-s3`), Azure ADLS (`delta-lake-azure`), GCS (`delta-lake-gcs`). Supports Unity and Glue catalogs.

### Connector Example

```sql
CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, volume BIGINT, ts TIMESTAMP,
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

Custom connectors can be built by implementing the `SourceConnector` or `SinkConnector` trait and registering with `ConnectorRegistry`.

---

## Postgres Wire Protocol

The standalone server speaks the Postgres v3 wire protocol. Any libpq-derived client (`psql`, JDBC, asyncpg, `tokio-postgres`, Grafana's Postgres datasource) can connect and tail a stream:

```bash
psql "host=db.internal port=5433 dbname=laminardb user=alice" \
  -c "SUBSCRIBE avg_price WHERE symbol = 'AAPL'"
```

Enable the listener by setting `pgwire_bind` in `laminardb.toml`. Auth is trust on loopback or MD5 for remote binds; TLS, mTLS (`pgwire_tls_client_ca`), TLS 1.3 pinning, hot-reload of certificates, and `pg_authid`-style pre-hashed passwords are all supported. See [crates/laminar-server/README.md](crates/laminar-server/README.md) for configuration.

| Statement | Behavior |
|-----------|----------|
| `SUBSCRIBE <stream>` | Stream rows as they're committed |
| `SUBSCRIBE … WHERE <expr>` | Server-side filter, schema-aware |
| `SUBSCRIBE … AS OF EPOCH n` | Replay from epoch `n` (stream must be `WITH ('retain_history' = '…')`) |
| `DECLARE c CURSOR FOR SUBSCRIBE …` + `FETCH n FROM c` | Cursored consumption for `\set FETCH_COUNT n` clients |
| `SELECT version()` / `SELECT 1` / transaction control | The handful of meta-commands clients issue at startup |

DDL (`CREATE SOURCE`, `CREATE STREAM`, etc.) goes through the HTTP API (`POST /api/v1/sql`). The pgwire surface is intentionally narrow: read-side only.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                     SOURCE CONNECTORS                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │  Kafka   │  │ Postgres │  │   File   │  tokio tasks      │
│  │  Source  │  │   CDC    │  │  Source  │  (main runtime)   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                   │
│       │              │              │                        │
│       └──── tokio::sync::mpsc ──────┘                         │
│                      ▼                                        │
├──────────────────────────────────────────────────────────────┤
│              STREAMING COORDINATOR                            │
│  Single tokio task on dedicated `laminar-compute` thread:     │
│  SQL cycles, compiled projections, cached logical plans,     │
│  barrier injection and alignment                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Compiled │→ │ Operators│→ │  State   │→ │  Sink    │    │
│  │Projection│  │ (window, │  │ (per-grp │  │ Writers  │    │
│  │/ Cached  │  │  join,   │  │ FxHashMap│  │          │    │
│  │  Plans   │  │  filter) │  │ / foyer) │  │          │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
├──────────────────────────────────────────────────────────────┤
│                     BACKGROUND I/O                            │
│  Main tokio runtime, runs alongside source/sink tasks         │
│  ┌──────────────┐  ┌──────────────┐                          │
│  │  Checkpoint  │  │   Sink I/O   │                          │
│  │ Coordinator  │  │ (Kafka/Delta │                          │
│  │  (2PC, store)│  │  /Postgres…) │                          │
│  └──────────────┘  └──────────────┘                          │
├──────────────────────────────────────────────────────────────┤
│                      CONTROL PLANE                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │  Admin   │  │ Metrics  │  │  Config  │                   │
│  │   API    │  │  Export  │  │ Manager  │                   │
│  └──────────┘  └──────────┘  └──────────┘                   │
└──────────────────────────────────────────────────────────────┘
```

- **Streaming coordinator.** Single tokio task on a dedicated single-threaded runtime (the `laminar-compute` thread), isolating CPU-bound event processing from I/O on the main runtime. Source connectors push batches in via `tokio::sync::mpsc`; the coordinator runs compiled projections or cached logical plans, routes results to sinks, and manages checkpoint barriers. Compiled single-source projections are sub-microsecond; incremental aggregations and cached-plan queries are microseconds; complex queries fall back to DataFusion.
- **Background I/O.** Source connectors, sink writers, and the checkpoint coordinator all run on the main tokio work-stealing runtime.
- **Admin.** HTTP REST API (Axum), Prometheus metrics, ad-hoc SQL, hot reload, manual checkpoints. No built-in auth on the HTTP API; put it behind a reverse proxy. The Postgres-wire listener has MD5 + TLS + mTLS auth (see above).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

### Checkpointing and Recovery

1. **Coordinated snapshots.** Chandy-Lamport barriers injected at sources; operators with multiple inputs align before snapshotting.
2. **Two-phase commit.** Exactly-once sinks participate in pre-commit / commit phases coordinated by `CheckpointCoordinator`.
3. **Atomic manifest.** Each checkpoint writes a JSON manifest (operator state + connector offsets) via temp-file-plus-rename, to filesystem or object store (S3 / GCS / Azure).
4. **Recovery.** `RecoveryManager` loads the latest manifest, restores operator state, rolls back exactly-once sinks, and resumes connectors from committed offsets.

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

Criterion suites live under `crates/laminar-core/benches/`, `crates/laminar-db/benches/`, and `crates/laminar-connectors/benches/`. Run `cargo bench` to measure on your own hardware. The numbers in the docs are from a developer laptop on minimal operator chains; they are not continuously validated and do not represent p99 under load.

---

## Language Bindings

| Language | Package |
|----------|---------|
| Rust | [`laminar-db`](https://crates.io/crates/laminar-db) |
| Python | [`laminardb`](https://pypi.org/project/laminardb/) (in [`laminardb-python`](https://github.com/laminardb/laminardb-python)) |
| C / C++ | FFI (`--features ffi`), Arrow C Data Interface |

---

## Feature Flags

| Flag | Description |
|------|-------------|
| `kafka` | Kafka source/sink, Avro serde, Schema Registry |
| `postgres-cdc` | PostgreSQL CDC source via logical replication (also enables Postgres lookup) |
| `postgres-sink` | PostgreSQL sink via COPY BINARY |
| `mysql-cdc` | MySQL CDC source via binlog replication |
| `mongodb-cdc` | MongoDB CDC source and sink |
| `delta-lake` | Delta Lake source and sink |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling Parquet/CSV/JSON) |
| `otel` | OpenTelemetry OTLP/gRPC source (traces, metrics, logs) |
| `parquet-lookup` | Parquet lookup source for reference tables |
| `api` / `ffi` | C FFI layer with Arrow C Data Interface |

---

## Building from Source

```bash
# Prerequisites: Rust 1.95+ (stable)
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
  laminar-core/        Core engine: operators, windows, streaming channels, checkpoint barriers, error codes
  laminar-sql/         SQL parser, planner, DataFusion integration, streaming optimizer, watermark pushdown
  laminar-storage/     Checkpoint manifest and store (filesystem + object store)
  laminar-connectors/  Kafka, CDC (PG/MySQL/Mongo), WebSocket, Files, Delta Lake, Iceberg, OTEL
  laminar-db/          Unified database facade, StreamingCoordinator, checkpoint coordination, recovery, FFI
  laminar-derive/      Derive macros: Record, FromRecordBatch, FromRow, ConnectorConfig
  laminar-server/      Standalone server binary (HTTP API, Docker, Helm)
examples/
  demo/                Market data TUI demo with Ratatui
  binance-ws/          Live Binance WebSocket streaming SQL demo
  microstructure/      Market microstructure analysis demo
  claude-code-aiops/   OpenTelemetry ingest + streaming SQL dashboard
  server-demo/         Standalone server walkthrough
```

---

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md): design overview, data flow, state management.
- [SQL Reference](docs/SQL_REFERENCE.md): streaming SQL dialect, tested patterns.
- [API Reference](https://docs.rs/laminar-db): rustdoc.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, Ring 0 rules, and the PR process.

## Support

- [GitHub Issues](https://github.com/laminardb/laminardb/issues) for bug reports and feature requests.
- [GitHub Discussions](https://github.com/laminardb/laminardb/discussions) for questions.
- Email: support@laminardb.io.

## License

Apache License 2.0. See [LICENSE](LICENSE).
