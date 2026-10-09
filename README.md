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

### Change a running cluster's topology

Cluster migrations support adding, dropping and replacing sources, streams or sinks.
Progress is preserved by default; reset incompatible objects with ordered DROP/CREATE
in one migration request, dropping dependents first. New or reset state processes future input.
Migrations pause at a checkpoint and return an asynchronous receipt. All required nodes
must be ready to resume; node IDs and the complete vnode owner map must stay unchanged.
See the [server REST API](crates/laminar-server/README.md#rest-api) for topology endpoints.

## Production tuning

LaminarDB is pre-1.0. Test your own workload and recovery path before production use. Configure
persistent checkpoints, authentication, and network security. Measure peak process memory under
normal load, bursts, and recovery; individual engine limits do not cap total process memory.
Choose checkpoint frequency and container resources from those measurements.

For topology migrations, measure pause duration and peak restore memory, and budget storage
for retained migration roots. The current limit is 64 retained operations; do not delete
authority or root objects to bypass it.

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
| Sources | Kafka, NATS, MongoDB change streams, local files, WebSockets, OpenTelemetry, and supported Iceberg reads. |
| Sinks | Kafka, NATS, PostgreSQL, MongoDB, local files, WebSockets, Delta Lake, and Iceberg. |
| Lookups | PostgreSQL, MongoDB, Delta Lake, and Iceberg. |

MongoDB change streams can feed sinks directly, either as change history or as a mirror of
each document. See [MongoDB change data capture](#mongodb-change-data-capture) to get started.
PostgreSQL change-data-capture ingestion is not yet available as a streaming source.
Connector options and delivery guarantees depend on the source, sink, storage, and deployment
mode. See the [connector guide](crates/laminar-connectors/README.md) for those details.

### MongoDB change data capture

LaminarDB can read changes straight from a MongoDB change stream and write them to
PostgreSQL, Delta Lake, Iceberg, files, or another MongoDB. There is no Kafka or Debezium in
between.

You can use it in two ways:

- **Mirror** (`output.mode = 'document'`): keep a copy of each document up to date. Inserts
  and updates become upserts and deletes remove the row. PostgreSQL and Delta Lake support
  this.
- **History** (the default): write one row for every change, deletes included. This suits
  audit logs and lake tables, for example files, Delta Lake, Iceberg, or a PostgreSQL table. A
  MongoDB sink can also replay the history into an exact copy of the collection.

#### Try it locally

You need Docker, a Rust toolchain, and a checkout of this repository. These steps mirror a
MongoDB collection into a PostgreSQL table.

1. Start MongoDB and PostgreSQL:

   ```bash
   docker compose -f tests/docker/mongodb-cdc-compose.yml up -d --wait
   ```

   MongoDB listens on `127.0.0.1:27117` and PostgreSQL on `127.0.0.1:15433` (user `laminar`,
   password `laminar-test-secret`, database `mirror`). Change streams only work on a replica
   set, so even this test MongoDB is a one-node replica set.

2. Create the collection. A mirror needs the full document after every update, so turn on
   pre- and post-images:

   ```bash
   docker exec laminardb-mongo-rs mongosh --port 27117 --quiet --eval \
     'db.getSiblingDB("app").createCollection("users", {changeStreamPreAndPostImages: {enabled: true}})'
   ```

3. Save this as `laminardb.toml`:

   ```toml
   sql = '''
   CREATE SOURCE users (
       _id VARCHAR NOT NULL, name VARCHAR, email VARCHAR, doc VARCHAR,
       PRIMARY KEY (_id)
   ) FROM "mongodb-cdc" (
       'connection.uri' = 'mongodb://127.0.0.1:27117/?directConnection=true&tls=false',
       'database' = 'app', 'collection' = 'users',
       'output.mode' = 'document', 'full.document.mode' = 'required',
       'snapshot.mode' = 'initial',
       'objectid.columns' = '_id', 'document.json.column' = 'doc'
   );

   CREATE SINK users_pg FROM users INTO "postgres-sink" (
       'hostname' = '127.0.0.1', 'port' = '15433', 'database' = 'mirror',
       'username' = 'laminar', 'password' = '$${PG_PASSWORD}', 'ssl.mode' = 'disable',
       'table.name' = 'users', 'auto.create.table' = 'true',
       'write.mode' = 'upsert', 'primary.key' = '_id', 'changelog.mode' = 'true'
   );
   '''

   [server]
   bind = "127.0.0.1:8080"
   delivery = "at_least_once"

   [checkpoint]
   url = "file:///tmp/laminardb-cdc"
   interval = "1s"
   ```

   LaminarDB connects to MongoDB over TLS unless the URI says `tls=false`. The test MongoDB has
   no TLS, so this URI turns it off. Passwords can't be written into the SQL: `$${PG_PASSWORD}`
   reaches the SQL as `${PG_PASSWORD}`, and LaminarDB reads it from the environment when it
   connects.

4. Start the server. The first build takes a few minutes.

   ```bash
   PG_PASSWORD=laminar-test-secret cargo run --release -p laminar-server --bin laminardb -- \
     --config laminardb.toml
   ```

5. In another terminal, change some data in MongoDB, then look at PostgreSQL:

   ```bash
   docker exec laminardb-mongo-rs mongosh --port 27117 --quiet --eval '
     const users = db.getSiblingDB("app").users;
     users.insertOne({name: "Ada", email: "ada@example.com"});
     users.insertOne({name: "Alan", email: "alan@example.com"});
     users.updateOne({name: "Ada"}, {$set: {email: "ada@lovelace.dev"}});
     users.deleteOne({name: "Alan"});'

   docker exec laminardb-cdc-postgres psql -U laminar -d mirror -c 'SELECT _id, name, email FROM users'
   ```

   You should see one row: Ada, with her new email. Alan was inserted and then deleted, so he
   is gone. Changes usually arrive in well under a second.

If you stop the server and start it again, it carries on from its last checkpoint. It does
not copy the collection a second time.

#### Things to know

- Run it with checkpointing on and at-least-once delivery. After a crash, some changes may be
  applied twice. Upserts and deletes make that harmless.
- In a mirror, the `PRIMARY KEY` must be `_id`, and every other column must allow nulls,
  because a delete only carries the key. List ObjectId columns in `objectid.columns`.
- Column types must match the stored values. For example, a MongoDB double can't go into a
  `BIGINT` column. If you don't want to map fields one by one, `document.json.column` holds the
  whole document as JSON.
- `snapshot.mode = 'initial'` copies the documents that already exist, then follows changes
  from that point. Without it, only changes made after the first start are captured.
- The source stops with an error, instead of guessing, when the oplog no longer holds the
  changes it needs, when the collection is dropped or replaced, or when a post-image is
  missing. Size the oplog for the longest outage you expect.
- It runs embedded or on a single-node server. Cluster mode and exactly-once delivery aren't
  supported yet.

The [connector guide](crates/laminar-connectors/README.md#mongodb-cdc) covers every option, the
history record format, and failure handling in detail.

### Connector schemas

You can omit source columns when the connector can read them from metadata, such as an Avro
schema in Schema Registry or a Parquet file. Generators and OpenTelemetry have fixed schemas.
For OpenTelemetry, leave `format` out of the TOML source and omit `FORMAT` in SQL; it uses
the native OTLP protocol. Codec-based sources use their connector's default when `format` is omitted.
JSON and CSV sources need declared columns unless file sampling is explicitly enabled.
Sinks get their input fields from the source or stream they read. The query must fit the
destination's fields and types; use aliases and casts in a stream to adjust the output.

For example, if the `events` and `archive` topics already have Avro value schemas containing
`id` and `label`, you can use them without repeating those fields in SQL:

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

By default, this reads the latest schema under each topic's `<topic>-value` subject once when
you create the object. These settings control selection, sampling and external creation:

| Setting | When to use it |
|---|---|
| `schema.registry.url` | Avro registry service URL, such as `http://localhost:8081`. Supply the service URL, not a subject or schema-resource URL. |
| `schema.registry.value.subject` | Choose a value subject explicitly. Use it when multiple topics, a regex subscription or a naming strategy would make selection ambiguous. |
| `schema.registry.value.version` or `schema.registry.value.id` | Pin a positive version or schema ID. Set one of these, not both. Without either, creation resolves latest once. |
| `schema.registry.record.name` | Supply the record name when using the existing record-based subject naming strategies. An explicit value subject overrides subject derivation. |
| `schema.registry.auto.register` | Defaults to `false`. Set it to `true` on a Kafka sink to permit registration during creation. Changing `schema.compatibility` also requires this permission. |
| `schema.inference` | Defaults to `false`. Set it to `true` for CSV or JSON file sources to sample up to four files, 1 MiB and 1,000 rows within ten seconds. Empty or all-null samples fail; a sample cannot prove what future files will contain. |
| `auto.create` | Defaults to `false` for Delta Lake and Iceberg sinks and MongoDB standard collections. Set it to `true` to permit creation of a missing destination. |
| `auto.create.table` | Defaults to `false` for PostgreSQL sinks. Set it to `true` to permit creation of a missing table. |
| `schema.evolution` | Keep it `false` for Delta sinks using durable schema contracts. Change an incompatible target through a controlled migration. |

For instance, a local Delta sink may create its table when you ask it to:

```sql
CREATE SINK delta_out FROM output INTO "delta-lake"
  ('table.path' = './delta-output', 'auto.create' = 'true');
```

Durable deployments save the resolved contract before starting the connector. Restarts reuse
that contract, including the selected registry identity. Keep historical Avro writer schemas
in the registry for replay. Connectivity, authorization and destination identity checks still
apply after restart. Creating an external table or registering a schema can succeed before
catalog publication fails, leaving an unused resource for you to review.

Use `DESCRIBE events` to inspect resolved fields and their origin. The
[schema guide](docs/SCHEMA_RESOLUTION.md) covers supported formats, mappings, recovery and
migration. Schema discovery uses the existing deployment and delivery restrictions.

MongoDB time-series sinks permit creation through the existing `timeseries.time_field`
setting. Saved bindings track the backing bucket UUID, so dropping and recreating a
time-series collection requires a new binding.

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
- [Connector schema resolution and recovery](docs/SCHEMA_RESOLUTION.md)
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
