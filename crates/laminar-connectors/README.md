# laminar-connectors

Source fields resolve from authoritative metadata, built-in protocols or explicit
declarations before startup. Sinks bind their query schema before opening. See
[schema resolution](../../docs/SCHEMA_RESOLUTION.md) for the direction/format matrix,
bounded sampling, separate preparation policies and durable reader/writer contracts.

External system connectors for LaminarDB. Exactly-once admission requires an exact-certified source and a checkpoint-committable sink with coordinated external publication.

## Connectors

### Source Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | Replayable ALO; exact-certified input for coordinated EO pipelines | Implemented |
| PostgreSQL CDC | `postgres-cdc` | Raw JSON change envelopes lack canonical primary-keyed row/delete records; initial and resume admission reject before I/O | Not admitted |
| MongoDB CDC | `mongodb-cdc` | Change streams as history records or keyed document mutations; optional initial snapshot; replayable at-least-once, embedded/single-node | Implemented |
| NATS | `nats` | Core or JetStream ingestion; ephemeral because acknowledgements are not checkpoint-owned | Implemented |
| OpenTelemetry (OTLP/gRPC) | `otel` | OTLP/gRPC receiver (traces, metrics, logs) via tonic | Implemented |
| WebSocket Client | `websocket` | tokio-tungstenite | Implemented |
| Delta Lake Source | `delta-lake` | Ephemeral singleton full-changelog reader; ordinary streaming routes, durable delivery and cluster use reject it | Reader only |
| Iceberg Source | `iceberg` | Bounded snapshot scans or replayable append-lineage reads; changelog fails closed | Implemented |
| File Auto-Loader | `files` | Local directory watch/glob discovery, Parquet/CSV/JSON; remote URLs fail at startup | Implemented |

Feature flags compile connector implementations; startup validates the complete source/SQL/sink
composition. PostgreSQL CDC remains rejected even when a stored resume position exists; see
`postgres_cdc_admission_rejects_unexecuted_options_and_reference_use` in
[CDC admission tests](tests/cdc_admission.rs). Its lookup connector and supported sinks are
separate capabilities.

### MongoDB CDC

`mongodb-cdc` reads one collection's change stream with the official Rust driver and delivers it
straight to sinks: MongoDB → LaminarDB → destinations, with no broker in between. It runs in
embedded and single-node server mode; cluster mode rejects it (singleton sources and mutable
sinks have no fenced placement).

New to it? The [main README](../../README.md#mongodb-change-data-capture) walks through
mirroring a collection into PostgreSQL on your machine.

| `output.mode` | Rows | Contract | Destinations |
|---|---|---|---|
| `history` (default) | One immutable, versioned record per change event (and per snapshot copy) | Append-only | Append-only sinks; tested with files, Delta and Iceberg append, PostgreSQL append, and MongoDB `cdc_replay` |
| `document` | Full post-image → keyed put; delete → key-only tombstone | Keyed upsert | PostgreSQL upsert + `changelog.mode`, Delta `write.mode=upsert` |

`snapshot.mode=never` (default) captures changes made after the source first opens; documents
that already exist, and changes older than the oplog, are not read. `snapshot.mode=initial` first
copies the whole collection, then streams every change from the copy's point in time.

#### Setup

The source needs MongoDB 6.0+ (tested with 8.0) as a replica set or sharded cluster; a MongoDB
sink needs 8.0+. Sharded clusters are untested, and their `PRIMARY KEY` must include the shard
key. `document` mode and `full.document.mode=required` need pre/post-images on the source
collection:

```javascript
db.runCommand({ collMod: "users", changeStreamPreAndPostImages: { enabled: true } })
```

Connections use TLS unless `connection.uri` sets `tls=false`; `tlsInsecure` and
`tlsAllowInvalidCertificates` are rejected. Secrets must be `${VAR}` references, resolved from
`LaminarDB::builder().config_var(..)` or the environment. In a server TOML `sql` block write `$${VAR}`, because the server expands `${VAR}` in
the file before running the DDL. A document mirror into PostgreSQL and Delta, plus an exact
MongoDB copy:

```sql
CREATE SOURCE users (
    _id VARCHAR NOT NULL, name VARCHAR, age BIGINT, doc VARCHAR,
    PRIMARY KEY (_id)
) FROM "mongodb-cdc" (
    'connection.uri' = 'mongodb://${MONGO_USER}:${MONGO_PASSWORD}@db1,db2,db3/?replicaSet=rs0',
    'database' = 'app', 'collection' = 'users',
    'output.mode' = 'document', 'full.document.mode' = 'required',
    'snapshot.mode' = 'initial',
    'objectid.columns' = '_id', 'document.json.column' = 'doc'
);

CREATE SINK users_pg FROM users INTO "postgres-sink" (
    'hostname' = 'pg', 'port' = '5432', 'database' = 'mirror', 'username' = 'laminar',
    'password' = '${PG_PASSWORD}', 'table.name' = 'users', 'auto.create.table' = 'true',
    'write.mode' = 'upsert', 'primary.key' = '_id', 'changelog.mode' = 'true'
);

CREATE SINK users_delta FROM users INTO "delta-lake" (
    'table.path' = 's3://lake/users', 'write.mode' = 'upsert', 'merge.key.columns' = '_id'
);

CREATE SOURCE users_history FROM "mongodb-cdc" (
    'connection.uri' = 'mongodb://${MONGO_USER}:${MONGO_PASSWORD}@db1,db2,db3/?replicaSet=rs0',
    'database' = 'app', 'collection' = 'users', 'full.document.mode' = 'required'
);

CREATE SINK users_copy FROM users_history INTO "mongodb-sink" (
    'connection.uri' = 'mongodb://${MONGO_USER}:${MONGO_PASSWORD}@backup/?replicaSet=rs1',
    'database' = 'backup', 'collection' = 'users', 'auto.create' = 'true',
    'write.mode' = 'cdc_replay', 'replay.source.namespace' = 'app.users'
);
```

Run with `delivery.guarantee = at_least_once` and checkpointing enabled. Each source reads its
own change stream; several sinks may read one source.

#### Event history records

`mongodb_history_schema()` (version 1) has `event_id`, `event_version`, `operation` (the server
`operationType`, or `snapshot` for copied documents), `database`, `collection`,
`collection_uuid`, `document_key`, `full_document`, `update_description`, `event_details`,
`resume_token`, `cluster_time_seconds`, `cluster_time_increment`, `wall_time`, `txn_number`,
`lsid`, and `snapshot_id`.

- Document fields are canonical Extended JSON v2, so `Int32`/`Int64`, `ObjectId`/string,
  `Decimal128`, dates, binary, nested values, explicit `null` and missing fields stay distinct.
- `event_id` is the SHA-256 of the deployment identity, collection UUID, and resume token, so a
  replayed event keeps its identity; snapshot rows hash the snapshot time and `_id`.
- Repeated changes are never collapsed. A delete is a `delete` record; append targets keep the
  older records.
- Lifecycle and metadata events (`drop`, `rename`, `invalidate`, `createIndexes`, `modify`, …)
  are recorded with their remaining fields in `event_details`. After an `invalidate` the source
  resumes with `startAfter` and stops if the collection was recreated.
- Operation names are data. No column is named `_op` or `__weight`, so no sink applies a history
  record as a delete. `cdc_replay` interprets them explicitly: insert/replace/snapshot replace by
  document key, update replaces with the post-image (or applies `updateDescription` in
  `full.document.mode=delta`), delete deletes by key, metadata events write nothing, and
  destructive lifecycle events stop the sink.

#### Document replication

`document` mode requires `full.document.mode=required`, which delivers the post-image as of each
change, never a later lookup. It rejects a change-stream `pipeline`: filtering would leave stale
rows behind.

- The `PRIMARY KEY` must be the collection's document key: `_id`, plus every shard-key field on
  a sharded collection. A key field that changes in place stops the source.
- Non-key columns must be nullable: a delete carries only the key, so its other values are
  `NULL` rather than invented. A missing image fails the source; it is never a delete.
- Columns map exactly or fail the batch: `VARCHAR` ← string, `INT` ← int32, `BIGINT` ← int32/
  int64, `DOUBLE` ← double/int32, `BOOLEAN`, `DECIMAL(p,s)` ← decimal128/integers without
  rounding, `TIMESTAMP` ← date, `BYTEA` ← generic binary. `objectid.columns` lists `VARCHAR`
  columns holding ObjectIds as lowercase hex; ObjectIds are rejected elsewhere, so an ObjectId
  and an equal-looking string can never share a key. Nested and mixed values go through
  `document.json.column`, the whole document as canonical Extended JSON. Typed columns read
  both a missing field and `null` as `NULL`; the JSON column keeps the difference.
  `DECIMAL(p,s)` lands as PostgreSQL `NUMERIC(p,s)` and Delta `decimal(p,s)` without rounding;
  an existing PostgreSQL column must hold at least that scale and integer width.
- Sinks apply `_op = 'U'` and `_op = 'D'` rows by that key: PostgreSQL upserts and deletes by
  primary key, and Delta merges. A sink must declare exactly the source key. Filters, streams,
  joins, and aggregates over a document source are rejected: they would need retractions of
  rows the engine does not retain.

#### Initial snapshot

The server chooses a majority snapshot time *T*. The source checks the change stream can start
at *T*, commits *T* in a checkpoint before copying anything, copies the collection in `_id`
order with `readConcern: snapshot` at *T*, then streams every change at or after *T*. The copy
holds every write at or before *T* and the stream every change at or after it, so nothing is
missed. Changes at exactly *T* (whole transactions included) arrive twice; puts, deletes and
history consumers tolerate that. Resume tokens are never decoded.

- A restart during the copy resumes after the last checkpointed `_id` at the same *T*. A
  restart before *T* commits copies nothing, so no partial copy can mix two snapshot times.
- The copy must finish within the server's `minSnapshotHistoryWindowInSeconds` (default 300 s)
  and the oplog must still hold *T*; otherwise the source fails and names the recovery
  action: empty the targets and restart with fresh pipeline state.
- Supported on replica sets only, because `_id` need not be unique across shards. It needs
  checkpointing and at-least-once delivery. With manual checkpoints (no interval), the copy
  starts after the first `checkpoint()` and logs that it is waiting.
- Use new or empty targets. The snapshot never truncates or deletes rows that are absent from
  the source.
- `mongodb_cdc_snapshot_in_progress` is 1 while copied documents are being emitted. Once it is
  0 the mirror is catching up on changes. It is complete as of a point in time when the sink has
  applied the changes up to that time.

#### Delivery, recovery and failures

- At-least-once. Sinks are idempotent per key or event id, so replay after a crash converges.
  Writes across MongoDB, PostgreSQL and a lakehouse are not one transaction: one target may
  briefly lead another, and a replayed history record keeps its `event_id`.
- Checkpoints store the last *emitted* resume token, never the reader's newer position, plus a
  row sequence. Idle post-batch tokens advance progress only after every earlier event.
- A saved token does not reserve oplog history. Size the oplog for the longest outage.
- Recovery checks that the deployment identity and collection UUID are unchanged, so a drop,
  rename, recreate or endpoint change cannot rebind silently. The deployment identity comes
  from `replSetGetConfig` (replica sets) or `config.version` (sharded clusters). Atlas M0/Flex
  tiers reject `replSetGetConfig` and are unsupported.
- Network errors, elections, and labelled resumable errors retry with bounded backoff; only
  stream progress resets the budget. History loss, rejected resume tokens, missing post-images,
  authorization failures, and rejected options stop the source with a specific error. It never
  restarts from "now".
- Required privileges: `changeStream` and `find` on the collection, `listCollections` on the
  database, and `replSetGetConfig` (or read on `config.version` for sharded clusters).
- A sink write that exceeds `sink.write.timeout.ms` has an unknown outcome: the engine retires
  that writer and replays from the last checkpoint. Within one process the late write was
  tested not to overwrite newer values. After a crash, a MongoDB `bulkWrite` the dead process
  already sent can still complete on the server after the restarted process writes; driver
  3.x has no server-side time limit for `bulkWrite`, so only the server bounds that window.
- Exactly-once is not available. Events are limited to 16 MiB unsplit;
  `$changeStreamSplitLargeEvent` is not supported.

### Recovery and native client APIs

The engine admits source and sink compositions through their typed contracts. Connectors
use their client libraries for fetching, acknowledgements, retries, transactions and storage
I/O. A library capability becomes a delivery guarantee only when the connector implements
the corresponding checkpoint and recovery protocol.

Kafka input uses librdkafka partition offsets, assignment, seek, pause/resume and broker
commits. Broker commits use the engine's durable checkpoint positions; automatic commits
cannot advance recovery authority. Kafka preserves partition order. It provides neither a
cross-partition replay order nor fixed poll batches. Stateful process replay currently rejects
that missing order contract in embedded, single-node and cluster modes. Best-effort process
execution remains available locally. There is no Kafka `replay.order` setting.

Delta uses delta-rs writers, log-store APIs and application transactions; Iceberg uses native
writers and catalog transactions. Their storage libraries handle the configured object store.
The engine coordinates staged output with source positions and managed state. Provider wiring
alone does not certify that complete recovery protocol. See the
[object-store support matrix](../../docs/cloud-object-store-support.md) for current admission
and qualification evidence.

Latency depends on native fetch/write batching, queue bounds, acknowledgements, checkpoint
frequency and provider calls. Connector I/O runs outside the compute runtime. Delivery
admission does not establish a latency measurement.

Delta's `cdf_contract_is_full_changelog` in [reader tests](src/lakehouse/delta_source/tests.rs)
checks its reader contract. That contract is not an admitted append-only streaming source:
`mutation_sources_fail_before_connector_io` in [engine admission tests](../laminar-db/src/pipeline_lifecycle/connector_admission_tests.rs)
covers the ordinary route's rejection, and the positioned mutable join routes require ordering
and recovery capabilities that this reader lacks. Finite reference/lookup reads are separate.

### On-demand lookup sources (partial cache mode)

`CREATE LOOKUP TABLE ... WITH ('strategy' = 'on-demand', 'cache.memory' = '64mb')`
caches the hot working set in a byte-bounded RAM cache and fetches misses on demand —
the only model that addresses dimension tables larger than memory. Each backend
batches all missed keys of a probe into one pushed-down, key-filtered fetch
(`pk IN (...)` / `= ANY($1)` / `$in`) and realigns results to the input order.

| Backend | Feature Flag | Miss fetch | Notes |
|---------|-------------|-----------|-------|
| Delta Lake | `delta-lake` | `WHERE pk IN (...)` (file/partition pruning) | Warns if not clustered on the key |
| Iceberg | `iceberg` | Native scan `with_filter(pk IN ...)` (manifest pruning) | Reloads snapshot per fetch |
| PostgreSQL | `postgres-cdc` | Pooled (`deadpool`) `WHERE pk = ANY($1)` | Single-column key; server-auth TLS via `ssl.mode` + optional `ssl.ca.cert.path` |
| MongoDB | `mongodb-cdc` | `find({ pk: { $in: [...] } })` | Projects documents into the declared schema |

Misses run off the compute thread (the lookup-enrich operator is async-decoupled),
results are byte-bounded with optional TTL (`cache.ttl`), and a source error
backpressures rather than dropping rows.

### Sink Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | Durable broker-acknowledged at-least-once; no transactional commit | Implemented |
| NATS | `nats` | JetStream durable at-least-once after stream validation; Core is ephemeral | Implemented |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, durable at-least-once | Implemented |
| MongoDB | `mongodb-cdc` | Majority-journaled ordered writes, upsert, history `cdc_replay`, durable at-least-once | Implemented |
| Delta Lake | `delta-lake` | Coordinated append; cluster EO is certified only for direct S3/S3A | Implemented |
| Iceberg | `iceberg` | Rolling append writer; direct ALO, coordinated local EO, and REST + direct S3/S3A cluster EO; MOR/COW fail closed | Implemented |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | Implemented |
| WebSocket Client | `websocket` | Push to external server | Implemented |
| Files | `files` | Local CSV, JSON, Parquet rolling output; remote URLs fail at startup | Implemented |

Iceberg REST supports no authentication, a resolved static bearer token, or OAuth2 client credentials with proactive token refresh. Access delegation, vended storage credentials, and remote signing fail closed. Cluster exactly-once Iceberg admission remains limited to no authentication or static bearer authentication pending an OAuth2 cluster recovery fault matrix. Data-storage credentials for cluster exactly-once belong under `storage.property.*`; secret-bearing `catalog.property.*` values are treated as uncertified catalog authentication.

Backend support and native-provider evidence are tracked independently in the
[cloud object-store support matrix](../../docs/cloud-object-store-support.md). Azure Iceberg is
experimental; remote Files source/sink URLs are unsupported.

### Upsert sinks and changelog collapse

An upsert sink (`write.mode = 'upsert'`) holds the current per-key state of its
table. Its source changelog (a Z-set `__weight` column from an aggregating MV,
or an `_op` column from CDC) can carry many events per key in one epoch, so the
sink collapses each epoch to one row per merge key before the MERGE — keeping it
cardinality-safe and stripping the `__weight`/`_op`/`_ts_ms` metadata from the
table. The collapse (`changelog` module) backs Delta upsert today and is
sink-agnostic by design, for future reuse by an Iceberg MOR sink. Two
requirements:

- **`merge.key.columns` must be unique over the sink's input** — collapse errors
  loudly if two live rows share a key, rather than dropping data.
- **Partition or Z-order the table by the merge key** for copy-on-write MERGE
  performance (each commit then rewrites only the touched files).

## Key Modules

| Module | Purpose |
|--------|---------|
| `connector` | Core traits: `SourceConnector`, `SinkConnector`, `SourceBatch`, `WriteResult` |
| `config` | `ConnectorConfig`, `ConfigKeySpec`, `ConnectorInfo`, `ConnectorState` |
| `registry` | `ConnectorRegistry` for registering and looking up connectors by name |
| `kafka` | Kafka source/sink, Avro serde, schema registry, partitioner, backpressure |
| `postgres` | PostgreSQL durable at-least-once sink (COPY BINARY, upsert/changelog) |
| `postgres/cdc` | PostgreSQL replication/decoding implementation; CDC source admission remains rejected |
| `mongodb` | Change-stream CDC source (history and document modes, initial snapshot), lookup reads and durable at-least-once majority-journaled sink |
| `otel` | OpenTelemetry OTLP/gRPC receiver for traces, metrics, and logs (tonic server) |
| `websocket` | WebSocket client source and client/server sinks (fan-out, backpressure, reconnect) |
| `lakehouse` | Delta Lake source and sink (buffering, epoch, changelog, recovery, schema evolution) and Apache Iceberg source and sink (REST catalog) |
| `files` | File source (auto-loader, glob, watch) and sink (rolling, CSV/JSON/Parquet) |
| `lookup` | Lookup table support: PostgreSQL and Parquet reference tables |
| `reference` | Finite startup snapshots for reference tables |
| `storage` | Cloud storage: provider detection, credential resolver, config validation, secret masking |
| `serde` | Format implementations: JSON, CSV, raw, Debezium, Avro |
| `schema` | Schema framework: inference, resolution, evolution, decoders (JSON/CSV/Avro/Parquet) |
| `testing` | Mock connectors for unit testing |

## Schema Framework

| Sub-module | Description |
|------------|-------------|
| `schema::traits` | Format codec traits and schema inference/evolution types |
| `schema::resolver` | Schema resolution and merge engine |
| `schema::inference` | Format inference registry |
| `schema::json` | JSON format decoder with type inference |
| `schema::csv` | CSV format decoder with header/type sampling |
| `schema::avro` | Avro decoder with Schema Registry integration |
| `schema::parquet` | Parquet metadata-driven decoder |
| `schema::evolution` | Schema evolution engine (additive columns) |
| `schema::bridge` | Format bridge functions (JSON-to-Avro, etc.) |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `kafka` | rdkafka, Avro serde, schema registry (reqwest) |
| `postgres-cdc` | PostgreSQL replication implementation (CDC source rejected); also builds the supported `postgres` lookup source |
| `postgres-sink` | PostgreSQL sink via tokio-postgres |
| `mongodb-cdc` | MongoDB CDC source, sink, and lookup |
| `nats` | NATS Core and JetStream source/sink via async-nats |
| `changelog-collapse` | Sink-agnostic Z-set/CDC changelog collapse for upsert sinks (pulled in by `delta-lake`) |
| `delta-lake` | Delta Lake sink/source via deltalake crate |
| `delta-lake-s3` | S3 storage backend for Delta Lake |
| `delta-lake-azure` | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | GCS storage backend for Delta Lake |
| `delta-lake-unity` | Databricks Unity catalog for Delta Lake |
| `delta-lake-glue` | AWS Glue catalog for Delta Lake |
| `iceberg` | Apache Iceberg source and sink with REST, S3, and filesystem support |
| `iceberg-gcs` / `iceberg-azure` | REST Iceberg with GCS / experimental Azure ADLS storage |
| `iceberg-catalog-rest` | REST catalog; other typed catalog features currently fail with an explicit capability error |
| `iceberg-storage-s3` / `iceberg-storage-gcs` / `iceberg-storage-azure` / `iceberg-storage-fs` | Isolated OpenDAL storage backends for Iceberg |
| `otel` | OpenTelemetry OTLP/gRPC source (traces, metrics, logs) |
| `parquet-lookup` | Parquet schema and codec helpers; no standalone connector |
| `websocket` | WebSocket source and sink (tokio-tungstenite) |
| `files` | Local file source (auto-loader) and sink (rolling files); remote URLs are rejected |

## Custom Connectors

Build custom connectors by implementing the `SourceConnector` or `SinkConnector` trait and registering with the `ConnectorRegistry`:

```rust
use laminar_connectors::connector::{SourceConnector, SinkConnector};
use laminar_connectors::registry::ConnectorRegistry;

// Register a custom source
let registry = ConnectorRegistry::new();
registry.register_source("my-source", info, factory_fn);
```

## Related Crates

- [`laminar-core`](../laminar-core) -- Streaming channels, sink abstractions, and checkpoint manifest types
- [`laminar-db`](../laminar-db) -- Connector manager and checkpoint coordinator
