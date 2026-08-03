[![CI](https://github.com/laminardb/laminardb/actions/workflows/ci.yml/badge.svg)](https://github.com/laminardb/laminardb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.95%2B-orange)](https://www.rust-lang.org)
[![Docker Hub](https://img.shields.io/badge/docker-laminardb%2Flaminardb--server-2496ed?logo=docker&logoColor=white)](https://hub.docker.com/r/laminardb/laminardb-server)
[![Website](https://img.shields.io/badge/website-laminardb.io-blue)](https://laminardb.io)

# LaminarDB

A streaming SQL engine for Rust. Embed it as a library or run the standalone server. Continuous queries, event-time windows, and checkpointed recovery. No JVM, no cluster required.

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
| Cluster | Multi-node deployment. Static or gossip discovery, lease-fenced control paths, dynamic partition/vnode rebalance, and distributed checkpoints. |

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

## LaminarDB Console

LaminarDB features a web console to author, observe, and manage streaming SQL pipelines. 

The console includes:
* **Interactive SQL Worksheet**: Author and execute streaming SQL queries with support for live subscription tailing over WebSockets.
* **Dependency & Lineage DAG**: Interactive visual topology diagram showing active data streams from Sources to Sinks and Materialized Views.
* **Catalog Schema Browser**: Introspect catalog metadata, schema definitions, and connector options.
* **Cluster & Partition Monitor**: Live node discovery status and a virtual node (vnode) partition lease assignment heatmap.

The official web console is hosted and ready to use at:
👉 **[laminardb.github.io/laminardb-console-ui](https://laminardb.github.io/laminardb-console-ui/)**

For source code or local deployment options, see the [`laminardb-console-ui`](https://github.com/laminardb/laminardb-console-ui) repository.

---

## Cluster Mode & Setup

LaminarDB supports multi-node cluster deployments. Keyed aggregates and interval joins use 256
stable virtual nodes (vnodes) by default in every deployment tier. Embedded and single-node
runtimes own all configured vnodes in process; clusters distribute the same topology across nodes.

### Architecture & Dynamics

* **Membership & Discovery**: Nodes discover one another using either a gossip-based protocol (Chitchat peer-to-peer membership over a configured `gossip_port`) or a static seeds list.
* **Coordination**: Membership selects a leader candidate, while a renewable shared-store lease fences leader-only control paths. Vnode assignments are CAS-published through `AssignmentSnapshotStore`; there is no embedded Raft service.
* **Dynamic Rebalancing**: Stable key groups (256 by default) are dynamically distributed across active cluster nodes. When a new node joins or an existing node departs (or fails), the leader automatically rebalances the assignments. When shutting down gracefully, a node announces a `Draining` state, letting the leader reallocate its key groups before the node terminates.
* **Distributed Checkpoints**: The current coordinator seals shared checkpoint artifacts. Its accepted replacement packs one dirty-vnode object per node with manifest-indexed byte ranges; that cutover is still in progress.
* **Checkpoint Store**: Cluster execution requires a shared `object_store` URL. Single-node replay-capable delivery currently remains fenced to the local filesystem path.

> [!IMPORTANT]
> Cluster exactly-once is admitted only for exact-certified sources and a cluster-certified,
> checkpoint-committable sink. The current admitted path is Kafka input to direct S3/S3A
> append-mode Delta Lake. Kafka output remains at-least-once. Other exact combinations fail closed with `[LDB-5035]`
> before connector I/O. The four-mode production soak matrix is still pending.
> The accepted state design is authoritative in-memory `FxHashMap` state per vnode with
> object-store-only checkpoint durability. Public backend selection is removed, but the internal
> checkpoint-artifact cutover and four-mode production soak matrix remain pending.
> Cluster SQL admits stateless pipelines, supported non-windowed keyed aggregates, and one bounded
> join stage over direct append-only, watermarked sources. The join supports `INNER`, `LEFT`,
> `RIGHT`, `FULL`, `LEFT/RIGHT SEMI`, and `LEFT/RIGHT ANTI` with ordered `VARCHAR`/`BIGINT`
> equality keys and a positive finite event-time bound. Every join projection needs an explicit
> alias. Output from any of the eight kinds can feed a separate named keyed aggregate stream;
> fused `JOIN ... GROUP BY` and cluster windowed aggregation remain fail-closed.
> Cluster materialized views also fail closed regardless of query shape because their
> retained output lacks a planner-certified distribution and assignment-fenced checkpoint/read
> lifecycle. See the [current distributed-state status](docs/DISTRIBUTED_STATE.md) for the exact
> boundary and validation gate.

### Cluster Configuration Example

To deploy in cluster mode, configure `[discovery]` in `laminardb.toml` and set `server.mode` to `"cluster"`.

```toml
node_id = "node-1" # Required and unique per node

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
delivery = "at_least_once"
key_groups = 256

[discovery]
strategy = "gossip" # "gossip" or "static"
gossip_port = 7946
advertise_host = "10.0.0.1"
seeds = ["10.0.0.1:7946", "10.0.0.2:7946"]
cluster_tls_cert = "/etc/laminardb/tls/node.crt"
cluster_tls_key = "/etc/laminardb/tls/node.key"
cluster_tls_client_ca = "/etc/laminardb/tls/cluster-ca.crt"
cluster_tls_server_name = "laminardb-cluster.internal"

[checkpoint]
url = "s3://my-bucket/laminardb/checkpoints"
interval = "30s"
timeout = "120s"
```

Cluster barrier/shuffle RPC uses plaintext when all four `cluster_tls_*` fields are omitted. To enable mTLS, configure all four fields; every node certificate must chain to the configured CA and contain `cluster_tls_server_name` as a SAN. These fields do not wrap Chitchat gossip, so restrict `gossip_port` to a trusted network. Use mTLS for production clusters unless transport security is provided by the deployment network.

> [!NOTE]
> If `server.mode` is set to `"single"` (the default), no discovery, cluster control-plane, or shuffle services are started or bound, even when the binary includes cluster support.


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

LaminarDB uses one bounded event-time join machinery in local and cluster execution, under both
at-least-once and exactly-once delivery. It supports `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFT SEMI`,
`RIGHT SEMI`, `LEFT ANTI`, and `RIGHT ANTI` joins over direct append-only sources. Each source needs
a watermark on a `TIMESTAMP NOT NULL` column. Equality keys may be ordered composites of
`VARCHAR`/`BIGINT` columns, with the type matching at each position, and the directional time bound
must be positive and finite. Every projected expression needs an explicit alias.

```sql
CREATE STREAM order_fills AS
SELECT o.account_id AS account_id,
       o.amount AS amount,
       f.fill_price AS fill_price
FROM orders o
INNER JOIN fills f
ON o.tenant_id = f.tenant_id
AND o.order_id = f.order_id
AND f.ts BETWEEN o.ts AND o.ts + INTERVAL '10' SECOND;

-- Any supported join kind can feed this separate named aggregate stage.
CREATE STREAM filled_amounts AS
SELECT account_id, SUM(amount) AS total_amount, COUNT(*) AS match_count
FROM order_fills
GROUP BY account_id;
```

Within this bounded stream-stream path, fused `JOIN ... GROUP BY`, intermediate-input, cross,
as-of, unbounded, general non-equality, and multi-way joins fail closed. Temporal and lookup joins
are separate local enrichment paths and are rejected in cluster mode. A join cycle is capped at
262,144 output rows and 64 MiB; exceeding either limit is a terminal hot-key fanout error, with no
continuation or spill path.

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
CREATE SOURCE ... [FROM connector (...)] [FORMAT format [WITH (...)]]
CREATE STREAM ... AS SELECT ...                    [WITH ('retain_history' = '64mb')]
CREATE MATERIALIZED VIEW ... AS SELECT ...
CREATE SINK ... FROM input [INTO connector (...)] [FORMAT format [WITH (...)]]
CREATE LOOKUP TABLE ... (...) WITH ('connector' = '<lookup-connector>', ...)
DROP SOURCE | STREAM | SINK | MATERIALIZED VIEW
SHOW SOURCES | STREAMS | SINKS | MATERIALIZED VIEWS
SHOW CREATE SOURCE name
DESCRIBE [EXTENDED] table_name
EXPLAIN ANALYZE SELECT ...
SUBSCRIBE <stream> [AS OF EPOCH n] [WHERE …]      -- live tail of a stream
DECLARE c CURSOR FOR SUBSCRIBE … ; FETCH n FROM c -- cursored consumption
```

`retain_history` keeps a bounded suffix of committed epochs in memory. Resume only from a progress marker the client durably recorded: WebSocket emits `type=progress` frames, while pgwire emits `__laminar_kind=progress` rows with epoch and checkpoint ID. `SUBSCRIBE … AS OF EPOCH n` then starts strictly after that committed cut or fails visibly if it is unavailable.

The managed non-windowed aggregate path used after a bounded join supports non-`DISTINCT` `COUNT`,
`SUM`, `AVG`, `MIN`, and `MAX` in local and cluster mode. Local SQL has additional DataFusion and
window-function paths, but they are not part of the distributed join-and-aggregate contract.

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

Feature-gated connectors for external systems. Each advertises a typed recovery, durability, topology, and input contract; startup rejects combinations that cannot uphold the pipeline-wide delivery guarantee.

### Sources

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Replayable, splittable, and exact-delivery certified | ✅ |
| PostgreSQL CDC | `postgres-cdc` | Resume-only pgoutput replication; fresh startup is rejected | ✅ |
| MongoDB CDC | `mongodb-cdc` | UUID-bound fixed-collection resume; replayable at-least-once only | ✅ |
| OpenTelemetry OTLP | `otel` | OTLP/gRPC receiver for traces, metrics, and logs | ✅ |
| WebSocket Client | `websocket` | Connect to external WebSocket servers | ✅ |
| WebSocket Server | `websocket` | Accept incoming WebSocket connections | ✅ |
| Delta Lake | `delta-lake` | Version polling; local best-effort-only `Ephemeral` singleton, unavailable in cluster | ✅ |
| Iceberg | `iceberg` | REST catalog polling; local best-effort-only `Ephemeral` singleton, unavailable in cluster | ✅ |
| Files (AutoLoader) | `files` | Glob pattern discovery, watch mode, Parquet/CSV | ✅ |
| Postgres Lookup | `postgres-cdc` | Connector name `postgres`; external table enrichment | ✅ |

### Sinks

| Connector | Feature Flag | Notes | Status |
|-----------|-------------|-------|--------|
| Kafka | `kafka` | Durable at-least-once, configurable partitioning | ✅ |
| PostgreSQL | `postgres-sink` | COPY BINARY and upsert, durable at-least-once | ✅ |
| MongoDB | `mongodb-cdc` | Majority-journaled ordered writes, upsert/CDC replay, durable at-least-once | ✅ |
| Delta Lake | `delta-lake` | Coordinated append supports local exact delivery; cluster exact admission is limited to direct S3/S3A. Azure/GCS targets remain cluster at-least-once pending native fault soaks | ✅ |
| Iceberg | `iceberg` | REST catalog append, durable at-least-once; exactly-once is rejected because no checkpoint-bound catalog cursor is implemented | ✅ |
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
    'bootstrap.servers' = '${KAFKA_BROKERS}',
    topic = 'market-trades',
    'group.id' = 'laminar-analytics',
    'auto.offset.reset' = 'earliest'
) FORMAT JSON;

CREATE SINK trade_archive
FROM trade_summary
INTO "delta-lake" (
    "table.path" = 's3://my-bucket/trade_summary',
    "write.mode" = 'append'
);
```

Delivery is one pipeline-wide runtime setting (`[server].delivery` for the standalone server or
`LaminarDbBuilder::delivery_guarantee` when embedded). Per-sink delivery flags are rejected rather
than silently creating mixed checkpoint semantics.

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

## LaminarDB Console UI

The standalone server embeds HTTP REST and WebSocket control-plane APIs. A separate Vite-based single-page application (SPA) client connects to these endpoints to provide an administrative and monitoring console.

### Control-Plane API Endpoints

The HTTP API binds to `bind` configured under `[server]`. It serves the following control-plane and telemetry endpoints:

* **DDL & SQL Execution**: `POST /api/v1/sql` accepts SQL queries. It returns JSON-formatted results (including Arrow record batches, metadata, and error diagnostics).
* **Lineage & Dependency Graph**: `GET /api/v1/graph` traces upstream and downstream relationship edges (`source -> stream -> MV -> sink`) to generate dependency DAGs.
* **Cluster Management**:
  * `GET /api/v1/cluster/nodes` returns the list of active/draining/suspected nodes.
  * `GET /api/v1/cluster/vnodes` returns the configured key-group assignments.
  * `GET /api/v1/cluster/leader` returns the current durable leader-lease holder.
  * `GET /api/v1/cluster/checkpoints` returns completed checkpoint metadata.
* **Pipeline Administration**:
  * `GET /api/v1/sources` | `/api/v1/sinks` | `/api/v1/streams` | `/api/v1/mvs` to inspect existing entities.
  * `POST /api/v1/pipeline/start` and `POST /api/v1/pipeline/stop` to control individual pipeline run states.
  * `POST /api/v1/checkpoint` to trigger manual checkpoints.
  * `POST /api/v1/reload` to trigger configuration and TLS certificate hot-reloading.

Authentication is gated using a token defined by `server.console_token` in headers/query parameters. CORS origins are restricted via `server.console_cors_allowed_origins`.

---

## Environment Variables

LaminarDB standalone server supports environment variable interpolation inside the configuration file.

* **Syntax**: String entries in `laminardb.toml` accept `${VAR_NAME}` for mandatory variables (server fails to start if missing) and `${VAR_NAME:-fallback}` for optional values.
* **SQL Queries**: Substitution is run on the raw config content at load time. This means environment variables can be embedded inside SQL statements declared in `[[pipeline]]` blocks or the top-level `sql` DDL configuration.
* **Dynamic Queries**: Environment variables are **not** expanded in SQL commands executed dynamically post-startup (via pgwire or the REST `POST /api/v1/sql` endpoint).

---

LaminarDB supports multiple deployment profiles (In-Process, Embedded, Standalone Server, and Cluster Mode) to match different operational requirements:

### 1. In-Process Mode (BareMetal Profile)
Runs entirely in-memory within the host application process. Since it does not persist state to disk, it has zero I/O overhead and starts up instantly. Ideal for stateless streaming transformations, unit testing, or ephemeral sidecars.

```mermaid
graph TD
    classDef hostClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef appClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef coordClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:2px,font-weight:bold;
    classDef stateClass fill:#06b6d4,fill-opacity:0.15,stroke:#06b6d4,stroke-width:1px;

    subgraph HostApp["Host Application Process (Rust/Python/C)"]
        App["Application Logic"]:::appClass
        subgraph Engine["LaminarDB Engine (In-Process / BareMetal)"]
            Coord["Streaming Coordinator<br/>('laminar-compute' thread)"]:::coordClass
            InMemState["Per-Vnode In-Memory State<br/>(FxHashMap)"]:::stateClass
            Coord <--> InMemState
        end
        App -->|"Push RecordBatches<br/>via Direct API"| Coord
        Coord -->|"Pull Results<br/>via Subscription"| App
    end

    style HostApp fill:#3b82f6,fill-opacity:0.05,stroke:#3b82f6,stroke-width:1.5px,stroke-dasharray: 5 5
    style Engine fill:#8b5cf6,fill-opacity:0.05,stroke:#8b5cf6,stroke-width:1.5px
```

### 2. Embedded Mode (Embedded / Durable Profiles)
Runs inside the host application process with the same vnode state used by a cluster. Checkpoints
use a local `file://` URL or a remote object-store URL. After a crash, the recovery manager restores
the latest committed checkpoint and replays sources from its stored offsets.

```mermaid
graph TD
    classDef appClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef coordClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:2px,font-weight:bold;
    classDef stateClass fill:#06b6d4,fill-opacity:0.15,stroke:#06b6d4,stroke-width:1px;
    classDef recoveryClass fill:#10b981,fill-opacity:0.15,stroke:#10b981,stroke-width:1px;
    classDef storageClass fill:#f97316,fill-opacity:0.15,stroke:#f97316,stroke-width:1px;

    subgraph HostApp["Host Application Process (Rust/Python/C)"]
        direction TB
        App["Application Logic"]:::appClass
        subgraph Engine["LaminarDB Engine (Embedded Profile)"]
            Coord["Streaming Coordinator<br/>('laminar-compute' thread)"]:::coordClass
            InMemState["Per-Vnode In-Memory State<br/>(FxHashMap)"]:::stateClass
            Checkpoint["Checkpoint Coordinator"]:::recoveryClass
            Recovery["Recovery Manager"]:::recoveryClass
            
            Coord <--> InMemState
            Checkpoint -.->|"Snapshot State"| InMemState
            Recovery -.->|"Restore State"| InMemState
        end
        App -->|"Push RecordBatches"| Coord
        Coord -->|"Pull Results"| App
    end
    Checkpoint -->|"Write Checkpoint"| Storage["Checkpoint Storage"]:::storageClass
    Storage -->|"Restore Manifest<br/>and Operator State"| Recovery
    
    style HostApp fill:#3b82f6,fill-opacity:0.05,stroke:#3b82f6,stroke-width:1.5px,stroke-dasharray: 5 5
    style Engine fill:#8b5cf6,fill-opacity:0.05,stroke:#8b5cf6,stroke-width:1.5px
```

### 3. Standalone Server Mode
Runs as a dedicated native daemon binary (`laminar-server`). It wraps the embedded engine and exposes standard client protocols (PostgreSQL pgwire, REST HTTP API, WebSockets) so external processes can run DDL/DML queries or stream ingest/consume data.

```mermaid
graph TD
    classDef clientClass fill:#10b981,fill-opacity:0.15,stroke:#10b981,stroke-width:1px;
    classDef portClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef coordClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:2px,font-weight:bold;
    classDef stateClass fill:#06b6d4,fill-opacity:0.15,stroke:#06b6d4,stroke-width:1px;
    classDef checkpointClass fill:#78716c,fill-opacity:0.15,stroke:#78716c,stroke-width:1px;
    classDef storageClass fill:#f97316,fill-opacity:0.15,stroke:#f97316,stroke-width:1px;

    Client1["PostgreSQL Clients (pgwire)"]:::clientClass
    Client2["REST API Clients (HTTP)"]:::clientClass
    Client3["WebSocket Clients (Subscriptions)"]:::clientClass
    
    subgraph Server["LaminarDB Standalone Server (laminar-server)"]
        direction TB
        subgraph Ports["Interface Listeners"]
            REST["Axum HTTP REST Listener"]:::portClass
            WS["WebSocket Listener"]:::portClass
            PgWire["Postgres Wire Listener"]:::portClass
        end
        
        subgraph Engine["LaminarDB Engine (Embedded Library)"]
            Coord["Streaming Coordinator<br/>('laminar-compute')"]:::coordClass
            State["Per-Vnode In-Memory State<br/>(FxHashMap)"]:::stateClass
            Checkpoint["Checkpoint Coordinator"]:::checkpointClass
            Coord <--> State
            Checkpoint -.-> State
        end
        
        REST -->|"Push Events, DDL,<br/>and Admin"| Coord
        WS -->|"Streaming Subscription<br/>Batches"| Coord
        PgWire -->|"SQL DDL and<br/>DML Execution"| Coord
    end
    
    Client1 -->|"Port 5432"| PgWire
    Client2 -->|"Port 8000"| REST
    Client3 -->|"Port 8000 /ws"| WS
    Checkpoint -->|"Write Checkpoints"| Storage["Durable Storage (Local Disk / S3)"]:::storageClass

    style Server fill:#6b7280,fill-opacity:0.05,stroke:#4b5563,stroke-width:1.5px
    style Ports fill:#3b82f6,fill-opacity:0.05,stroke:#3b82f6,stroke-width:1.5px
    style Engine fill:#8b5cf6,fill-opacity:0.05,stroke:#8b5cf6,stroke-width:1.5px
```

### 4. Cluster Mode (Distributed Deployment)
Runs as a distributed cluster of cooperative nodes. Nodes use static membership or **Chitchat Gossip**, fence leader-only work with a renewable shared-store lease, publish VNode assignments through create/CAS operations, exchange partition streams via high-performance **gRPC & Arrow-Flight** shuffles, and persist coordinated checkpoints to shared object storage.

```mermaid
graph TD
    classDef clientClass fill:#10b981,fill-opacity:0.15,stroke:#10b981,stroke-width:1px;
    classDef engineClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:2px,font-weight:bold;
    classDef controlClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef gossipClass fill:#06b6d4,fill-opacity:0.15,stroke:#06b6d4,stroke-width:1px;
    classDef vnodeClass fill:#78716c,fill-opacity:0.15,stroke:#78716c,stroke-width:1px;
    classDef storageClass fill:#f97316,fill-opacity:0.15,stroke:#f97316,stroke-width:1px;

    Client["Clients / Load Balancer"]:::clientClass
    
    subgraph Node1["LaminarDB Node 1 (Coordinator Leader)"]
        direction TB
        E1["Streaming Engine"]:::engineClass
        Control1["Lease-Fenced<br/>Control Plane"]:::controlClass
        Gossip1["Chitchat Gossip"]:::gossipClass
        VNodes1["Owned Key Groups<br/>(Dynamic Subset)"]:::vnodeClass
        E1 <--> VNodes1
    end

    subgraph Node2["LaminarDB Node 2 (Follower)"]
        direction TB
        E2["Streaming Engine"]:::engineClass
        Control2["Cluster Control<br/>Follower"]:::controlClass
        Gossip2["Chitchat Gossip"]:::gossipClass
        VNodes2["Owned Key Groups<br/>(Dynamic Subset)"]:::vnodeClass
        E2 <--> VNodes2
    end

    %% Client Operations
    Client -->|"REST / pgwire"| Node1
    Client -->|"REST / pgwire"| Node2

    %% Node Communication
    Gossip1 ---|"Peer Discovery"| Gossip2
    E1 ---|"gRPC and Arrow-Flight<br/>Data Shuffle"| E2

    %% Distributed Durability
    Control1 -->|"Leader Lease and<br/>Assignment CAS"| SharedStore["Shared Object Store"]:::storageClass
    Control2 -->|"Read Shared<br/>Control State"| SharedStore
    Node1 -->|"Coordinated 2-Phase<br/>Commit Checkpoints"| SharedStore
    Node2 -->|"Coordinated 2-Phase<br/>Commit Checkpoints"| SharedStore:::storageClass

    style Node1 fill:#6b7280,fill-opacity:0.05,stroke:#4b5563,stroke-width:1.5px
    style Node2 fill:#6b7280,fill-opacity:0.05,stroke:#4b5563,stroke-width:1.5px
```

### 5. Internal Threading Model (Within a Single Node)
Within each engine node, a dedicated execution thread isolates CPU-bound stream processing from
connector I/O. This reduces scheduling interference; production latency remains workload-dependent
and subject to the soak gate.

```mermaid
graph TB
    classDef computeClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:1px;
    classDef mainClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef blockingClass fill:#f97316,fill-opacity:0.15,stroke:#f97316,stroke-width:1px;
    
    classDef sourceClass fill:#3b82f6,fill-opacity:0.1,stroke:#3b82f6,stroke-width:1px;
    classDef sinkClass fill:#10b981,fill-opacity:0.15,stroke:#10b981,stroke-width:1px;
    classDef serviceClass fill:#78716c,fill-opacity:0.15,stroke:#78716c,stroke-width:1px;
    classDef coordClass fill:#8b5cf6,fill-opacity:0.15,stroke:#8b5cf6,stroke-width:2px,font-weight:bold;

    subgraph ComputeRuntime["Dedicated 'laminar-compute' Thread (Single-Threaded Runtime)"]
        subgraph Coord["Streaming Coordinator (Hot Path)"]
            direction LR
            Proj["Projections & Filters"]:::computeClass
            Window["Window Operators"]:::computeClass
            Join["Join Operators"]:::computeClass
            LocalState["Per-Vnode State (FxHashMap)"]:::computeClass
            
            Proj --> Window --> Join
            Window <--> LocalState
            Join <--> LocalState
        end
    end

    subgraph MainRuntime["Main Tokio Runtime (Multi-Threaded)"]
        subgraph Sources["Source Connectors (Tokio Tasks)"]
            S1["Kafka Source"]:::sourceClass
            S2["Postgres CDC"]:::sourceClass
            S3["WebSocket Source"]:::sourceClass
        end
        
        subgraph Sinks["Sink Connectors (Tokio Tasks)"]
            SinkIO["Sinks (Kafka, Delta Lake, PG)"]:::sinkClass
        end
        
        subgraph CoreServices["Background Services"]
            API["REST & WebSockets API (Axum)"]:::serviceClass
            CheckpointCoord["Checkpoint Coordinator"]:::serviceClass
            Metrics["Prometheus Metrics"]:::serviceClass
        end
    end

    subgraph BlockingPool["Tokio Blocking Thread Pool"]
        Serial["State Serialization (rkyv to bytes)"]:::blockingClass
    end

    %% Data and Control Paths
    Sources -->|"tokio::sync::mpsc<br/>(Arrow RecordBatches)"| Proj
    Join -->|"Output Batches"| SinkIO
    CheckpointCoord -.->|"Trigger Checkpoint<br/>Barrier"| Sources
    CheckpointCoord -.->|"Offload Serialization"| Serial

    style ComputeRuntime fill:#8b5cf6,fill-opacity:0.05,stroke:#8b5cf6,stroke-width:1.5px
    style MainRuntime fill:#3b82f6,fill-opacity:0.05,stroke:#3b82f6,stroke-width:1.5px
    style BlockingPool fill:#f97316,fill-opacity:0.05,stroke:#f97316,stroke-width:1.5px
    style Coord fill:#8b5cf6,fill-opacity:0.05,stroke:#8b5cf6,stroke-width:1.5px
```

* **Streaming coordinator.** Single tokio task on a dedicated single-threaded runtime (the `laminar-compute` thread), isolating CPU-bound event processing from I/O on the main runtime. Source connectors push batches in via `tokio::sync::mpsc`; the coordinator runs compiled projections or cached logical plans, routes results to sinks, and manages checkpoint barriers. Compiled single-source projections are sub-microsecond; incremental aggregations and cached-plan queries are microseconds; complex queries fall back to DataFusion.
* **Background I/O.** Source connectors, sink writers, and the checkpoint coordinator all run on the main tokio work-stealing runtime.
* **Admin.** HTTP REST API (Axum), Prometheus metrics, ad-hoc SQL, hot reload, manual checkpoints. No built-in auth on the HTTP API; put it behind a reverse proxy. The Postgres-wire listener has MD5 + TLS + mTLS auth (see above).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

### Checkpointing and Recovery

1. **Coordinated snapshots.** Chandy-Lamport barriers injected at sources; operators with multiple inputs align before snapshotting.
2. **External commit.** In admitted exactly-once pipelines, checkpoint-committable sinks stage output and publish it only after the durable checkpoint decision.
3. **Durable decision.** Prepared and finalized manifests bind the deployment, pipeline, exact attempt, state seals, connector positions, and participants. Filesystem/object-store writes use create/CAS boundaries appropriate to the backend.
4. **Recovery.** `RecoveryManager` accepts the latest finalized identity-matching manifest, restores state and source positions, and reconciles coordinated sinks from their exact external cursor.

```rust
// Note: StreamCheckpointConfig is from laminar-core (add as a dependency)
let db = LaminarDB::builder()
    .storage_dir("./data")
    .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
        interval_ms: Some(30_000),
        ..Default::default()
    })
    .build()
    .await?;
```

Recovery resumes from the latest finalized checkpoint. Replayable sources may resend records after
that cut under at-least-once delivery; an admitted exactly-once pipeline suppresses duplicate
external visibility through coordinated sink commits. Non-replayable sources are admitted only
under `best_effort`, where failure can lose accepted events. Shorter checkpoint intervals reduce
replay work but increase storage and coordination I/O.

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
| `postgres-cdc` | PostgreSQL CDC source via logical replication (also builds the standalone `postgres` lookup connector) |
| `postgres-sink` | PostgreSQL sink via COPY BINARY |
| `mongodb-cdc` | MongoDB CDC source and sink |
| `delta-lake` | Delta Lake source and sink |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling Parquet/CSV/JSON) |
| `otel` | OpenTelemetry OTLP/gRPC source (traces, metrics, logs) |
| `parquet-lookup` | Parquet schema and codec helpers; no standalone connector |
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
cargo test --features kafka,postgres-cdc,mongodb-cdc,delta-lake,websocket

# Run the Binance WebSocket demo
cargo run -p binance-ws
```

## Project Structure

```text
crates/
  laminar-core/        Core engine: operators, windows, streaming channels, checkpoint barriers, error codes, storage/checkpoint stores
  laminar-sql/         SQL parser, planner, DataFusion integration, streaming optimizer, watermark pushdown
  laminar-connectors/  Kafka, CDC (PostgreSQL/MongoDB), WebSocket, Files, Delta Lake, Iceberg, OTEL
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
