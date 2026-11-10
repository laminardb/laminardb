<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="brand/exports/svg/laminardb-logo-on-dark.svg">
    <img src="brand/exports/svg/laminardb-logo.svg" alt="LaminarDB" width="250">
  </picture>
</p>

[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![Docker Hub](https://img.shields.io/badge/docker-laminardb%2Flaminardb--server-2496ed?logo=docker&logoColor=white)](https://hub.docker.com/r/laminardb/laminardb-server)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

# LaminarDB

**Streaming SQL that keeps its answers current.**

LaminarDB is a streaming SQL engine written in Rust. Point it at Kafka, a PostgreSQL or MongoDB
change stream, files or a message queue, write ordinary SQL, and LaminarDB updates the result as
each event arrives. Send results to a sink, push them to live subscribers, or query them as
materialised views. Run it inside your Rust program, as a single server, or as a cluster: the
engine and the SQL are the same.

- **It's just SQL.** Event-time windows (`TUMBLE`, `HOP`, `SESSION`), watermarks, keyed
  aggregates, interval and temporal joins and materialised views, built on Apache Arrow and
  DataFusion.
- **Correct when things fail.** Checkpoints restore state and source positions together.
  Delivery is at-least-once by default and exactly-once where the source and sink can prove it,
  such as Kafka into Kafka, Delta Lake or Iceberg.
- **No silent downgrades.** A pipeline whose connectors can't honour the guarantee you asked for
  is rejected at startup, before it reads or writes anything.
- **Change data capture without the plumbing.** Follow PostgreSQL and MongoDB directly, with no
  Kafka or Debezium in between.
- **Embedded or standalone.** `cargo add laminar-db`, or run one binary that serves HTTP,
  WebSocket, the PostgreSQL wire protocol and Prometheus metrics.
- **Scales out on object storage.** Cluster nodes share checkpoints in S3, GCS or Azure and find
  one another through gossip or a static peer list.

LaminarDB is pre-1.0; see the [latest release](https://github.com/laminardb/laminardb/releases/latest)
for what's new. This README follows the `main` branch and marks anything added since the latest
release with *(unreleased)*. Website: [laminardb.io](https://laminardb.io).

## Quick start

You need Docker. This runs the latest server release with the built-in `generator` source, so
nothing else has to be running, and keeps a running total in a materialised view.

Save this as `laminardb.toml`:

```toml
sql = '''
CREATE SOURCE ticks FROM GENERATOR ('rows.per.second' = '10');

CREATE MATERIALIZED VIEW tick_totals AS
    SELECT COUNT(*) AS rows_seen, MAX(seq) AS last_seq
    FROM ticks;
'''

[server]
mode = "single"
bind = "0.0.0.0:8080"
console_token = "${LAMINAR_CONSOLE_TOKEN}"

[checkpoint]
url = "file:///var/lib/laminardb/checkpoints"
interval = "10s"
```

Start the server (commands are for a Bash-compatible shell):

```bash
export LAMINAR_CONSOLE_TOKEN="$(openssl rand -hex 32)"
docker run --rm -d --name laminardb -p 127.0.0.1:8080:8080 \
  -e LAMINAR_CONSOLE_TOKEN \
  -v "$PWD/laminardb.toml:/etc/laminardb/laminardb.toml:ro" \
  laminardb/laminardb-server:latest
```

Query the view, wait a few seconds, and query it again:

```bash
curl -s http://localhost:8080/api/v1/sql \
  -H "Authorization: Bearer $LAMINAR_CONSOLE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM tick_totals"}'
```

```json
{"result_type":"query","data":[{"rows_seen":"46","last_seq":"45"}]}
```

The numbers grow between queries because the view is updated as rows arrive; yours will differ.
The generator emits `(seq, ts_ms, value)` rows at a fixed rate and can replay them from a
checkpoint, so the server's default at-least-once delivery accepts it. 64-bit integers are
returned as strings so that they survive JSON parsing in JavaScript. Stop the server with
`docker stop laminardb`.

## Embed it in Rust

The same engine runs in-process. Add the crates:

```bash
cargo add laminar-db laminar-derive
cargo add tokio --features full
```

Then push typed rows in and receive typed results:

```rust
use laminar_db::{LaminarDB, TypedSubscriptionFrame};
use laminar_derive::{FromRow, Record};

#[derive(Clone, Record)]
struct Trade {
    symbol: String,
    volume: i64,
}

#[derive(Debug, FromRow)]
struct BigTrade {
    symbol: String,
    volume: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;
    db.execute("CREATE SOURCE trades (symbol VARCHAR NOT NULL, volume BIGINT NOT NULL)")
        .await?;
    db.execute("CREATE STREAM big_trades AS SELECT symbol, volume FROM trades WHERE volume >= 100")
        .await?;
    db.start().await?;

    let trades = db.source::<Trade>("trades")?;
    let mut big_trades = db.subscribe::<BigTrade>("big_trades").await?;

    trades.push_batch([
        Trade { symbol: "AAPL".into(), volume: 100 },
        Trade { symbol: "MSFT".into(), volume: 30 },
    ]);

    while let Some(frame) = big_trades.next_frame().await? {
        if let TypedSubscriptionFrame::Rows { rows, .. } = frame {
            println!("{rows:?}"); // [BigTrade { symbol: "AAPL", volume: 100 }]
            break;
        }
    }
    Ok(())
}
```

`LaminarDB::open()` runs in memory. `LaminarDB::builder()` adds checkpoint storage, connectors
and a delivery guarantee; see the [API documentation](https://docs.rs/laminar-db). `FromRow`
fills struct fields by column name. The default features
include the Kafka client, which builds librdkafka from source and needs CMake and a C compiler;
`--no-default-features` gives a lighter build, and you can then turn on only the connectors you
need.

## Example: per-minute volume from Kafka

This pipeline reads trades from a Kafka topic, totals each symbol's volume per minute of event
time, and writes each finished minute to another topic. You need Docker; the steps start Apache
Kafka and the latest LaminarDB release on a private Docker network.

1. Start Kafka and create the two topics:

   ```bash
   docker network create laminardb-example
   docker run -d --rm --name kafka --network laminardb-example \
     -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
     -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
     -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
     -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
     -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
     -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
     apache/kafka:4.1.1
   sleep 15
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic trades
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic minute-volume
   ```

2. Save this as `laminardb.toml`:

   ```toml
   sql = '''
   CREATE SOURCE trades (
       symbol VARCHAR NOT NULL,
       price  DOUBLE NOT NULL,
       volume BIGINT NOT NULL,
       ts     TIMESTAMP NOT NULL,
       WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
   ) FROM KAFKA (
       'bootstrap.servers' = 'kafka:9092',
       'topic' = 'trades',
       'group.id' = 'minute-volume',
       'startup.mode' = 'earliest'
   ) FORMAT JSON;

   CREATE STREAM minute_volume AS
       SELECT symbol,
              tumble(ts, INTERVAL '1' MINUTE) AS minute,
              SUM(volume) AS volume,
              MAX(price) AS high
       FROM trades
       GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
       EMIT ON WINDOW CLOSE;

   CREATE SINK minute_volume_out FROM minute_volume
       INTO KAFKA ('bootstrap.servers' = 'kafka:9092', 'topic' = 'minute-volume')
       FORMAT JSON;
   '''

   [server]
   mode = "single"
   bind = "0.0.0.0:8080"
   console_token = "${LAMINAR_CONSOLE_TOKEN}"

   [checkpoint]
   url = "file:///var/lib/laminardb/checkpoints"
   interval = "5s"
   ```

   The watermark tells LaminarDB to wait five seconds of event time for late trades before it
   closes a minute. With at-least-once delivery, a Kafka source must start from `earliest`, a
   timestamp or explicit offsets, rather than the consumer group's committed offsets (the
   default).

3. Start LaminarDB:

   ```bash
   export LAMINAR_CONSOLE_TOKEN="$(openssl rand -hex 32)"
   docker run -d --rm --name laminardb --network laminardb-example -p 127.0.0.1:8080:8080 \
     -e LAMINAR_CONSOLE_TOKEN \
     -v "$PWD/laminardb.toml:/etc/laminardb/laminardb.toml:ro" \
     laminardb/laminardb-server:latest
   ```

4. Send five trades, then read the output topic:

   ```bash
   docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka:9092 --topic trades <<'EOF'
   {"symbol": "AAPL", "price": 189.50, "volume": 100, "ts": "2026-10-18T09:30:05Z"}
   {"symbol": "AAPL", "price": 189.70, "volume": 50, "ts": "2026-10-18T09:30:40Z"}
   {"symbol": "MSFT", "price": 411.20, "volume": 30, "ts": "2026-10-18T09:30:41Z"}
   {"symbol": "AAPL", "price": 189.60, "volume": 70, "ts": "2026-10-18T09:31:10Z"}
   {"symbol": "MSFT", "price": 411.90, "volume": 20, "ts": "2026-10-18T09:32:30Z"}
   EOF
   sleep 10
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
     --topic minute-volume --from-beginning --timeout-ms 10000
   ```

   You should see three rows, one per finished minute and symbol. The consumer then exits after
   ten seconds without new messages and logs a `TimeoutException`; that is expected.

   ```json
   {"symbol":"AAPL","minute":"2026-10-18T09:30:00","volume":150,"high":189.7}
   {"symbol":"AAPL","minute":"2026-10-18T09:31:00","volume":70,"high":189.6}
   {"symbol":"MSFT","minute":"2026-10-18T09:30:00","volume":30,"high":411.2}
   ```

   MSFT's 09:32 minute is missing on purpose: the newest trade is 09:32:30, so the watermark is at
   09:32:25 and that minute stays open until a trade at 09:33:05 or later arrives.

5. Clean up with `docker stop laminardb kafka` and `docker network rm laminardb-example`.

## Install

- **Binaries:** the [latest release](https://github.com/laminardb/laminardb/releases/latest)
  for Linux (x86_64 and aarch64 glibc, x86_64 musl), macOS (x86_64 and Apple silicon) and
  Windows (x86_64), with `SHA256SUMS`.
- **Containers:** [Docker Hub](https://hub.docker.com/r/laminardb/laminardb-server) or
  [GitHub Container Registry](https://github.com/laminardb/laminardb/pkgs/container/laminardb-server).
  `latest` is the newest release, a version tag such as `X.Y.Z` or `X.Y` pins one, and `main`
  is rebuilt from the `main` branch.
- **Kubernetes:** the [Helm chart](deploy/helm/laminardb/README.md). Set `image.tag` to the
  release you deploy, because it defaults to the chart's `appVersion`.
- **Local stack:** [Docker Compose](docker-compose.yml).
- **From source:** Rust 1.95 or newer, CMake and a C compiler, then
  `cargo build --release -p laminar-server --bin laminardb`.

The [deployment guide](deploy/README.md) covers setup and operations.

## Deployment modes

| Mode | When to use it |
|---|---|
| Embedded | Run LaminarDB inside a Rust program with the `laminar-db` crate. Checkpoints go to a local directory. |
| Single-node server | Run one server with HTTP, WebSocket subscriptions, optional PostgreSQL-compatible connections and local checkpoints. |
| Cluster | Run several servers over a shared checkpoint store. Sources must be Kafka, AI functions are rejected, and cluster SQL is narrower than single-node SQL. Any node can read materialised views at a committed checkpoint. |

Client libraries are released separately from the engine and lag behind it:

| Language | Package | Notes |
|---|---|---|
| Rust | `cargo add laminar-db` · [API docs](https://docs.rs/laminar-db) | Published to crates.io with each server release. |
| Python | `pip install laminardb` · [repository](https://github.com/laminardb/laminardb-python) | The PyPI build lags well behind the engine; newer bindings build from the repository. |
| Node.js / TypeScript | `npm install @laminardb/node` · [repository](https://github.com/laminardb/laminardb-nodejs) | Alpha, built against an older engine without connectors. |
| Java | `io.laminardb:laminardb` · [repository](https://github.com/laminardb/laminardb-java) | Alpha, built against an older engine without connectors. |

## SQL

| Statement | What it does |
|---|---|
| `CREATE SOURCE` | Declares incoming data: its connector, columns (or columns read from the connector) and watermark. |
| `CREATE STREAM` | Runs a continuous query over sources and other streams. |
| `CREATE MATERIALIZED VIEW` | Keeps a result you can query at any time. |
| `CREATE SINK` | Sends a stream to an external system. |
| `SUBSCRIBE` | Pushes live results of a stream or view over WebSocket (`/ws/{name}`) or a PostgreSQL-protocol cursor. HTTP SQL rejects it. |

Queries can filter and project, aggregate by key, group by event-time windows, join two streams
within a time bound, join each event to the version of another source that was current at its
event time (`FOR SYSTEM_TIME AS OF`), and call streaming UDFs. `EXPLAIN` and `EXPLAIN ANALYZE` show the plan. The
[SQL reference](docs/SQL_REFERENCE.md) has the syntax, tested patterns and the
[cluster SQL boundary](docs/SQL_REFERENCE.md#cluster-sql-boundary).

The server accepts SQL at `POST /api/v1/sql` and, when `pgwire_bind` is set, from `psql` or any
PostgreSQL driver. Ad hoc reads can choose a read level: `latest`, `committed` (one
checkpoint) or `recent`, which waits for a checkpoint holding every record the views' Kafka
sources had upstream when the read arrived.

## Delivery guarantees

Each pipeline runs under one guarantee. LaminarDB checks that every source and sink can honour it
before any connector I/O, and never downgrades it silently.

| Guarantee | What you get |
|---|---|
| Best effort | No replay; output can be lost on failure. The embedded default; not available in a cluster. |
| At-least-once | The server default. State and source positions are restored from the last checkpoint and later input is replayed, so output can repeat. |
| Exactly-once | Committed output appears once. It needs a source certified for exact replay and a sink that commits with each checkpoint. |

Exactly-once is admitted for Kafka into Kafka, Delta Lake or Iceberg when embedded or on a single
node, and for Kafka into Delta Lake (direct S3) or Iceberg (REST catalog on S3) in a cluster.
[Delivery guarantees](docs/DELIVERY_GUARANTEES.md) has the full matrix and the test evidence
behind each exactly-once composition.

## Connectors

| Use | Connectors |
|---|---|
| Sources | Kafka (including Debezium), NATS, PostgreSQL logical replication, MongoDB change streams, Delta Lake (snapshots and change data feed), Iceberg, files, WebSocket, OpenTelemetry and the built-in `generator`. Optional builds add RabbitMQ, IBM MQ and FIX. |
| Sinks | Kafka, NATS, PostgreSQL, MongoDB, Delta Lake, Iceberg, files and WebSocket. Optional builds add RabbitMQ and IBM MQ. |
| Lookups | PostgreSQL, MongoDB, Delta Lake and Iceberg. |

The NATS, WebSocket and OpenTelemetry sources and Iceberg snapshot reads are best-effort only, so
the server's default at-least-once delivery rejects them; set `delivery = "best_effort"` to use
them.

RabbitMQ, IBM MQ, FIX and FpML are optional build features (`rabbitmq`, `ibmmq`, `fix`, `fpml`)
and are not in the Docker image. Of the release binaries, x86_64 Linux (glibc) includes all
four, x86_64 Linux (musl) none, and the others FIX and FpML. See the guides for
[RabbitMQ](docs/RABBITMQ.md), [IBM MQ](docs/IBMMQ.md) and [FIX](docs/FIX_SOURCE.md), and the
[connector guide](crates/laminar-connectors/README.md) for every option.

### PostgreSQL change data capture

Follow a PostgreSQL table through logical replication and keep a copy of it, or running results
over it, up to date. A mirror (`output.mode = 'upsert'`, the default) keeps one row per primary
key in a sink. A changelog (`output.mode = 'changelog'`) retracts the old row and adds the new
one, so filters and `SUM`/`COUNT`/`AVG` totals stay exact as rows change or disappear. It runs
embedded or on a single-node server, with at-least-once delivery.
[Try it locally](crates/laminar-connectors/README.md#try-postgresql-cdc-locally) or read the
[guide](crates/laminar-connectors/README.md#postgresql-cdc).

### MongoDB change data capture

Read a collection's change stream and write it to PostgreSQL, Delta Lake, Iceberg, files or
another MongoDB. `output.mode = 'document'` keeps a mirror of each document; the default
`history` mode writes one row per change, deletes included, for audit logs and lake tables. It
runs embedded or on a single-node server.
[Try it locally](crates/laminar-connectors/README.md#try-mongodb-cdc-locally) or read the
[guide](crates/laminar-connectors/README.md#mongodb-cdc).

### FpML trade confirmations

Derivatives confirmations, clearing messages and loan notices often arrive as FpML: large XML
documents full of nested legs, cross-references and amounts that must never be rounded. LaminarDB
reads them from files, Kafka, RabbitMQ, IBM MQ or a FIX field such as SecurityXML, validates each
one against the official FpML 5.13 schema, and turns the parts you need into typed columns. The
fourteen bundled profiles cover every FpML 5.13 confirmation product, so there is no parser to
write per product. It needs the `fpml` feature and runs embedded or on a single-node server.
[Try it locally](docs/FPML_SOURCE.md#try-it-locally) or read the [guide](docs/FPML_SOURCE.md).

### Schemas from connector metadata

Leave out a source's columns when the connector can read them, for example from an Avro schema in
a schema registry or from a Parquet file; the generator and OpenTelemetry have fixed schemas.
Sinks take their fields from the query they read, so use aliases and casts to fit the
destination.

```sql
CREATE SOURCE events FROM KAFKA (
  'bootstrap.servers' = 'localhost:19092', 'topic' = 'events',
  'group.id' = 'schema-example', 'schema.registry.url' = 'http://localhost:8081'
) FORMAT AVRO;
CREATE STREAM output AS SELECT label, id FROM events;
CREATE SINK archive FROM output INTO KAFKA (
  'bootstrap.servers' = 'localhost:19092', 'topic' = 'archive',
  'schema.registry.url' = 'http://localhost:8081'
) FORMAT AVRO;
```

LaminarDB saves the resolved schema before the pipeline starts and reuses it on restart.
`DESCRIBE events` shows each field and where it came from. JSON and CSV sources need declared
columns unless file sampling is turned on, and most sinks create a missing destination only when
you allow it. The [schema guide](docs/SCHEMA_RESOLUTION.md#settings) lists every setting.

## AI functions

SQL can call models to classify text, score sentiment, create embeddings or generate text:

```sql
CREATE STREAM scored_news AS
SELECT headline, ai_sentiment(headline, model => 'finbert') AS sentiment
FROM news;
```

The functions are `ai_classify`, `ai_sentiment`, `ai_embed`, `ai_extract`, `ai_complete`,
`ai_summarize`, `ai_translate` and `ai_gen`. Models use a remote provider (OpenAI-compatible or
Anthropic) or, for classification, sentiment and embeddings, a local ONNX encoder. Local models
need ONNX Runtime 1.24 or newer on the host, which the Docker image does not include. A query may
contain one aliased AI call over one table, and cluster mode rejects AI functions. See the
[AI setup guide](crates/laminar-server/README.md#ai-functions).

## Configuration

The server reads `laminardb.toml`, as in the [quick start](#quick-start). `${NAME}` takes a
value from the environment, and `$${` writes a literal `${`. The Docker image ships a file with
the quick start's `[server]` and `[checkpoint]` sections (checkpointing every 30 seconds) and no
pipeline; mount your own at `/etc/laminardb/laminardb.toml`. The container exits at startup if
`LAMINAR_CONSOLE_TOKEN` is not set. Mount persistent storage at `/var/lib/laminardb` so that
checkpoints survive container replacement. See the
[server configuration](crates/laminar-server/README.md) for every setting and the
[example configuration](examples/laminardb.toml) for sources, streams and sinks.

### Cluster configuration

Each node needs a unique ID and address. Nodes share one checkpoint store and find one another
through gossip or a static peer list:

```toml
node_id = "node-1"

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
console_token = "${LAMINAR_CONSOLE_TOKEN}"
delivery = "at_least_once"

[discovery]
strategy = "gossip"
advertise_host = "10.0.0.1"
seeds = ["10.0.0.1:7946", "10.0.0.2:7946"]

[checkpoint]
url = "s3://my-bucket/laminardb/checkpoints"
```

Change `node_id` and `advertise_host` on each node. Use a shared S3, GCS or Azure location;
cluster mode rejects `file://` checkpoints. Sources must be Kafka, the only source that can be
split across nodes. Cluster mTLS protects gRPC control and shuffle traffic but not gossip, so keep
gossip on a trusted or isolated network. Read the
[cluster setup guide](crates/laminar-server/README.md#cluster-control-plane-tls-mtls),
[Helm chart](deploy/helm/laminardb/README.md) and
[cluster SQL boundary](docs/SQL_REFERENCE.md#cluster-sql-boundary) before deploying.

### Change a running cluster's topology

A migration adds, drops or replaces sources, streams and sinks, and keeps their progress. It can
also add or drop materialised views. Reset an incompatible object with an ordered
`DROP`/`CREATE` in the same request, dependents first; reset state processes only new input. The
cluster pauses at a checkpoint and returns an asynchronous receipt, and every required node must
be ready to resume. The vnode owner map then stays frozen until the new topology is active and
has committed its own checkpoint, so don't scale in or out until then. See the [REST API](crates/laminar-server/README.md#rest-api) for the
endpoints.

## Running in production

LaminarDB is pre-1.0, so test your own workload and recovery path first. Configure persistent
checkpoints, authentication and network security. Measure peak process memory under normal load,
bursts and recovery, because individual engine limits do not cap total process memory, and size
checkpoint frequency and container resources from those measurements. For topology migrations,
also measure pause time and peak restore memory, and budget storage for retained migration roots:
the limit is 64 retained operations, and deleting authority or root objects to get round it is
not safe. The [server tuning guide](crates/laminar-server/README.md#memory-limits-and-production-tuning)
and [site guide](https://laminardb.io/docs/#production-tuning) cover memory limits, monitoring and
allocator settings.

**Monitoring.** The server exposes `/health` and `/ready` for health checks and `/metrics` for
Prometheus. Watch input, output, checkpoint success, latency and process memory; the repository
includes [Prometheus and Grafana examples](grafana/README.md).

**Console.** The [LaminarDB Console](https://github.com/laminardb/laminardb-console-ui) is a
browser app for a server you run: browse the catalogue, run SQL, and follow checkpoints, metrics
and lineage. The [hosted build](https://laminardb.github.io/laminardb-console-ui/) connects from
your browser straight to your server's HTTP API; nothing is hosted for you. A server that binds to
a non-loopback address must set `console_token`, and if you restrict browser origins with
`console_cors_allowed_origins`, include the Console's origin. The Console names the server
version it was last reviewed against and shows a notice when it connects to a different one.

## Documentation

- [SQL reference](docs/SQL_REFERENCE.md)
- [Delivery guarantees](docs/DELIVERY_GUARANTEES.md)
- [Server configuration and REST API](crates/laminar-server/README.md)
- [Connector guide](crates/laminar-connectors/README.md)
- [Connector schema resolution](docs/SCHEMA_RESOLUTION.md)
- [FpML](docs/FPML_SOURCE.md) and [FIX](docs/FIX_SOURCE.md) source guides
- [Rust API](https://docs.rs/laminar-db)
- [Examples](examples/)

## Contributing and support

See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute. Use
[GitHub Issues](https://github.com/laminardb/laminardb/issues) for bugs and feature requests,
[GitHub Discussions](https://github.com/laminardb/laminardb/discussions) for questions, or email
support@laminardb.io.

## Licence

Apache License 2.0. See [LICENSE](LICENSE).

Builds with the `fix` or `fpml` feature, including the release binaries other than x86_64 Linux
(musl), also contain third-party software under its own licence; release archives
carry the notices in `licenses/`.
This product includes software developed by quickfixengine.org (http://www.quickfixengine.org/).
libxml2 is under the MIT License, and the bundled FpML schemas are FpML Specifications under the
FpML Public License (Licensor: International Swaps and Derivatives Association, Inc.).
