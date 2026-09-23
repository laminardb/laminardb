[![Website](https://img.shields.io/badge/website-laminardb.io-blue)](https://laminardb.io)
[![Crates.io](https://img.shields.io/crates/v/laminar-db.svg)](https://crates.io/crates/laminar-db)
[![docs.rs](https://docs.rs/laminar-db/badge.svg)](https://docs.rs/laminar-db)
[![Docker Hub](https://img.shields.io/badge/docker-laminardb%2Flaminardb--server-2496ed?logo=docker&logoColor=white)](https://hub.docker.com/r/laminardb/laminardb-server)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

# LaminarDB

LaminarDB turns live data into continuously updated results with SQL. For example, it can read
trades from Kafka, calculate totals every minute, and send the results to a table or application.
Use it inside a Rust, Python, Node.js, or Java application, or run it as a standalone server.
You can start with one process; a cluster is optional.

The basic flow is **source → SQL query → result**. A source brings in data, a stream updates the
query result as new data arrives, and a sink or subscription delivers that result.

## Configuration

The server reads a `laminardb.toml` file. This example uses a local checkpoint directory and an
HTTP token supplied through an environment variable:

```toml
[server]
mode = "single"
bind = "0.0.0.0:8080"
console_token = "${LAMINAR_CONSOLE_TOKEN}"

[checkpoint]
url = "file:///var/lib/laminardb/checkpoints"
interval = "30s"
```

The Docker image includes a configuration like this one. Mount persistent storage at
`/var/lib/laminardb` if you want checkpoints to survive container replacement. See the
[configuration reference](https://laminardb.io/docs/) for all settings and
[example configuration](examples/laminardb.toml) for sources, streams, and sinks.

## Quick start

Start the server with Docker (commands shown for a Bash-compatible shell):

```bash
export LAMINAR_CONSOLE_TOKEN="$(openssl rand -hex 32)"
docker run --rm -p 8080:8080 \
  -e LAMINAR_CONSOLE_TOKEN \
  -v laminardb-data:/var/lib/laminardb \
  laminardb/laminardb-server:latest
```

In another terminal, check that it is running:

```bash
curl http://localhost:8080/health
```

To use LaminarDB inside an application, choose a client library:

| Language | Get started |
|---|---|
| Rust | `cargo add laminar-db` · [API docs](https://docs.rs/laminar-db) |
| Python | `pip install laminardb` · [examples](https://github.com/laminardb/laminardb-python) |
| Node.js / TypeScript | `npm install @laminardb/node` · [examples](https://github.com/laminardb/laminardb-nodejs) |
| Java | [Maven setup and examples](https://github.com/laminardb/laminardb-java) |

## Modes

| Mode | When to use it |
|---|---|
| Embedded | Run LaminarDB inside a Rust, Python, Node.js, or Java application. |
| Single-node server | Run one server with HTTP, optional Postgres-compatible connections, and local checkpoints. |
| Cluster | Run multiple servers with a shared checkpoint store. Cluster SQL and connector support are narrower than single-node support. |

## Cluster configuration

Each cluster node needs a unique ID and address. Nodes share the same checkpoint store and discover
one another through gossip or a static peer list. A minimal starting point is:

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

Change `node_id` and `advertise_host` for each node. Use a shared S3, GCS, or Azure checkpoint
location. Cluster mTLS protects gRPC control and shuffle traffic, but not gossip. With
`strategy = "gossip"`, keep gossip traffic on a trusted or isolated network. See the
[cluster setup guide](crates/laminar-server/README.md#cluster-control-plane-tls-mtls),
[Helm chart](deploy/helm/laminardb/README.md), and
[cluster SQL limits](docs/SQL_REFERENCE.md#cluster-sql-boundary) before deploying.

## Production tuning

LaminarDB is pre-1.0. Test your own workload and recovery path before production use. Configure
persistent checkpoints, authentication, and network security. Measure peak process memory under
normal load, bursts, and recovery; individual engine limits do not cap total process memory.
Choose checkpoint frequency and container resources from those measurements.

The [server tuning guide](crates/laminar-server/README.md#memory-limits-and-production-tuning)
and [site guide](https://laminardb.io/docs/#production-tuning) cover memory limits, monitoring,
and allocator settings in detail.

## SQL statements

| Statement | What it does |
|---|---|
| `CREATE SOURCE` | Defines incoming data. |
| `CREATE STREAM` | Runs a continuous SQL query over incoming data. |
| `CREATE MATERIALIZED VIEW` | Keeps a queryable result in embedded or single-node mode. |
| `CREATE SINK` | Sends a stream to an external system. |
| `SUBSCRIBE` | Sends live results to a connected client. |

For a `trades` source with a timestamp and watermark, this query calculates volume in one-minute
windows as events arrive:

```sql
CREATE STREAM minute_volume AS
SELECT symbol, SUM(volume) AS total_volume
FROM trades
GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

LaminarDB also supports filters, keyed aggregates, bounded joins, and event-time windows. The
[SQL reference](docs/SQL_REFERENCE.md) has complete syntax and mode-specific limits. The server
accepts SQL through its HTTP API and can expose a Postgres-compatible connection for queries and
subscriptions.

## Connectors

| Use | Available connectors |
|---|---|
| Sources | Kafka, NATS, local files, WebSockets, OpenTelemetry, and supported Iceberg reads. |
| Sinks | Kafka, NATS, PostgreSQL, MongoDB, local files, WebSockets, Delta Lake, and Iceberg. |
| Lookups | PostgreSQL, MongoDB, Delta Lake, and Iceberg. |

PostgreSQL and MongoDB change-data-capture ingestion is not yet available as a streaming source.
Connector options and delivery guarantees depend on the source, sink, storage, and deployment
mode. See the [connector guide](crates/laminar-connectors/README.md) for those details.

## AI functions

SQL can call models to classify text, score sentiment, create embeddings, or generate text. For
example:

```sql
CREATE STREAM scored_news AS
SELECT headline, ai_sentiment(headline, model => 'finbert') AS sentiment
FROM news;
```

Models can use a remote provider or a local encoder, depending on the function. See the
[AI setup guide](crates/laminar-server/README.md#ai-functions).

## Console UI

The [web console](https://laminardb.github.io/laminardb-console-ui/) connects to a LaminarDB
server. Use it to write SQL, inspect streams and sinks, and view pipeline activity. You can also
use the HTTP API directly; non-local HTTP access requires a console token.

## Monitoring

The server exposes `/health` and `/ready` for health checks and `/metrics` for Prometheus.
Monitor input, output, checkpoint success, latency, and process memory. The repository includes
[Prometheus and Grafana examples](grafana/README.md).

## Deployment

Choose the packaging that fits your environment:

- [Prebuilt binaries](https://github.com/laminardb/laminardb/releases/latest) for Linux, macOS, and Windows.
- [Docker Hub](https://hub.docker.com/r/laminardb/laminardb-server) or [GitHub Container Registry](https://github.com/laminardb/laminardb/pkgs/container/laminardb-server) images.
- [Docker Compose](docker-compose.yml) for a local stack.
- [Helm chart](deploy/helm/laminardb/README.md) for Kubernetes.

See the [deployment guide](deploy/README.md) for setup and operational details.

## More documentation

- [SQL reference](docs/SQL_REFERENCE.md)
- [Server configuration](crates/laminar-server/README.md)
- [Connector guide](crates/laminar-connectors/README.md)
- [Rust API](https://docs.rs/laminar-db)
- [Examples](examples/)

## Contributing and support

See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute. Use
[GitHub Issues](https://github.com/laminardb/laminardb/issues) for bugs and feature requests,
[GitHub Discussions](https://github.com/laminardb/laminardb/discussions) for questions, or email
support@laminardb.io.

## License

Apache License 2.0. See [LICENSE](LICENSE).
