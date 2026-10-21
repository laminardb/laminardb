# laminar-connectors

Source fields resolve from authoritative metadata, built-in protocols or explicit
declarations before startup. Sinks bind their query schema before opening. See
[schema resolution](../../docs/SCHEMA_RESOLUTION.md) for the direction/format matrix,
bounded sampling, separate preparation policies and durable reader/writer contracts.

External system connectors for LaminarDB. Exactly-once admission requires an exact-certified source and a checkpoint-committable sink with coordinated external publication. The [delivery guarantee matrix](../../docs/DELIVERY_GUARANTEES.md) lists what every source and sink pair admits in each runtime mode.

## Connectors

### Source Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | Replayable ALO with typed record metadata; exact-certified input for coordinated EO pipelines | Implemented |
| PostgreSQL CDC | `postgres-cdc` | Logical replication of one table as typed keyed upserts or a retractable changelog; optional initial snapshot; replayable at-least-once, embedded/single-node | Implemented |
| MongoDB CDC | `mongodb-cdc` | Change streams as history records or keyed document mutations; optional initial snapshot; replayable at-least-once, embedded/single-node | Implemented |
| NATS | `nats` | Core or JetStream ingestion; ephemeral because acknowledgements are not checkpoint-owned | Implemented |
| RabbitMQ | `rabbitmq` | AMQP 0-9-1 classic/quorum queue consumer; manual acknowledgements released only by a durable checkpoint (commit-coupled at-least-once), embedded/single-node ([guide](../../docs/RABBITMQ.md)) | Implemented |
| IBM MQ | `ibmmq` | MQI syncpoint gets committed with each durable checkpoint; queue-defined poison moves to `BOQNAME` in the same unit of work; IBM client loaded at run time; embedded/single-node ([guide](../../docs/IBMMQ.md)) | Implemented |
| OpenTelemetry (OTLP/gRPC) | `otel` | OTLP/gRPC receiver (traces, metrics, logs) via tonic | Implemented |
| WebSocket Client | `websocket` | tokio-tungstenite | Implemented |
| Delta Lake Source | `delta-lake` | Change data feed and snapshots through the Delta kernel as a retractable changelog; replayable at-least-once at commit/file boundaries, embedded/single-node | Implemented |
| Iceberg Source | `iceberg` | Bounded snapshot scans or replayable append-lineage reads; changelog fails closed | Implemented |
| File Auto-Loader | `files` | Local directory watch/glob discovery, Parquet/CSV/JSON; remote URLs fail at startup | Implemented |
| FpML documents | `fpml` | `FORMAT FPML` on the files, Kafka, RabbitMQ, and IBM MQ sources, and `fpml.field` on the FIX source: schema-validated FpML 5.13 documents projected to typed rows by profiles; embedded/single-node ([guide](../../docs/FPML_SOURCE.md)) | Implemented |

Feature flags compile connector implementations; startup validates the complete source/SQL/sink
composition before any connector I/O.

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

### PostgreSQL CDC

`postgres-cdc` reads one table through PostgreSQL logical replication (`pgoutput`) and delivers
typed rows straight to sinks or to retractable SQL: PostgreSQL → LaminarDB → destinations, with
no broker or Debezium in between. It runs in embedded and single-node server mode; cluster mode
rejects it.

New to it? The [main README](../../README.md#postgresql-change-data-capture) walks through
mirroring a table on your machine.

| `output.mode` | Rows | Contract | Consumers |
|---|---|---|---|
| `upsert` (default) | Current row per primary key; delete → key-only tombstone; a primary-key change → tombstone for the old key, then the new row | Keyed upsert | A keyed sink reading the source directly; tested with the PostgreSQL upsert sink (`changelog.mode = 'true'`). Streams over it are rejected |
| `changelog` | Z-set with a trailing `__weight BIGINT NOT NULL`: insert `+1`, delete `−1`, update `−1` old row then `+1` new row | Full changelog | A PostgreSQL upsert sink with `changelog.mode`, directly or through projections, filters, and non-windowed `COUNT`/`SUM`/`AVG` aggregates with `EMIT CHANGES`; windows, `MIN`/`MAX`, joins with other streams, and append sinks are rejected. Needs `snapshot.mode=initial` |

`snapshot.mode=initial` (default) copies the table, then streams every later change.
`snapshot.mode=never` streams only changes committed after the source creates its slot.

#### Setup

PostgreSQL 17 or newer with `wal_level = logical` (tested with 17 and 18). Prepare the table,
publication and role once; LaminarDB creates and owns the replication slots (see
[Slots](#slots)):

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION orders_pub FOR TABLE public.orders;
CREATE ROLE laminar LOGIN REPLICATION PASSWORD '...';
GRANT SELECT ON public.orders TO laminar;
```

Startup fails before it creates a slot unless:

- the publication is `FOR TABLE` exactly this one table, publishes all of `insert, update,
  delete, truncate` (the default), and has no row filter;
- the table is an ordinary table (not partitioned, a view or a foreign table) with a primary key
  and `REPLICA IDENTITY FULL`, so updates and deletes carry the complete old row and unchanged
  TOAST values can be restored;
- the primary key is not `DEFERRABLE`. A deferred key lets one transaction such as
  `UPDATE t SET id = id + 1` move a key value between rows, so a row can arrive before the delete
  of the row that held its key, and a consumer applying changes by key would then delete it;
- the declared `PRIMARY KEY` equals the table's primary key, every declared column exists with a
  supported type, and a column declared `NOT NULL` is `NOT NULL` in PostgreSQL. Declared columns
  may be a subset of the table's columns;
- with `snapshot.mode=initial`, the role can `SELECT` every declared column and row-level
  security does not apply to its reads of the table. Logical replication ignores row-level security, so a filtered copy would miss rows the
  stream later updates. Use a role with `BYPASSRLS`, or the table owner without
  `FORCE ROW LEVEL SECURITY`;
- with `snapshot.mode=initial`, the server is a primary, not a standby, and the primary-key
  columns use deterministic collations, their own collation in the index, and default operator
  classes (see [Initial snapshot](#initial-snapshot)).

```sql
CREATE SOURCE orders (
    id BIGINT NOT NULL, status VARCHAR, amount DECIMAL(12,2), updated_at TIMESTAMP,
    PRIMARY KEY (id)
) FROM "postgres-cdc" (
    'host' = 'pg', 'port' = '5432', 'database' = 'shop',
    'username' = 'laminar', 'password' = '${PG_PASSWORD}', 'ssl.mode' = 'verify-full',
    'publication' = 'orders_pub', 'slot.name' = 'laminar_orders', 'table' = 'public.orders'
);

CREATE SINK orders_mirror FROM orders INTO "postgres-sink" (
    'hostname' = 'replica', 'port' = '5432', 'database' = 'mirror',
    'username' = 'laminar', 'password' = '${MIRROR_PASSWORD}', 'table.name' = 'orders',
    'write.mode' = 'upsert', 'primary.key' = 'id', 'changelog.mode' = 'true',
    'auto.create.table' = 'true'
);
```

Run with at-least-once delivery and checkpointing enabled. Secrets must be `${VAR}` references;
in a server TOML `sql` block write `$${VAR}`.

| Property | Default | Meaning |
|---|---|---|
| `host`, `port`, `database` | required, `5432`, required | Server and database |
| `username`, `password` | `postgres`, none | Role with `REPLICATION` and `SELECT` on the table |
| `ssl.mode`, `ssl.ca.cert.path` | `verify-full`, bundled public CA roots | `verify-full` or `disable` |
| `publication`, `slot.name`, `table` | required | `slot.name` is a prefix of at most 46 bytes of lower-case letters, digits and `_` (see [Slots](#slots)); `table` is `schema.table` |
| `snapshot.mode` | `initial` | `initial` or `never` |
| `output.mode` | `upsert` | `upsert` or `changelog` |
| `max.buffered.bytes` | `268435456` (256 MiB) | All source memory for raw WAL, decoded transactions and Arrow building; 1 MiB to 4 GiB |

Removed keys (`table.include`, `table.exclude`, `start.lsn`, `max.buffered.events`,
`max.poll.records`, `poll.timeout.ms`, …) are rejected with the reason.

#### Slots

A new source picks a random 16-hex-digit claim id and records it in a checkpoint before it
touches PostgreSQL. After that checkpoint commits, it creates the slot `<slot.name>_<claim id>`,
for example `laminar_orders_3f9a0c27d1e4b856`. A slot name is never reused. Logs, errors and
reset instructions name the exact slot.

- The source reads nothing until its claim commits, and emits no row until the next checkpoint,
  which names the new slot, commits too. Rows therefore start flowing about two checkpoint
  intervals after the source starts, in both snapshot modes. With a manual checkpoint schedule
  the slot is created after the first `checkpoint()` and rows flow after the second.
- Creating a logical slot waits for every running transaction that has written something. The
  wait has no deadline. Every 30 seconds the source logs a warning that names those sessions and
  any prepared transactions.
- A restart uses only the slot its committed checkpoint names. The slot must be a logical
  `pgoutput` slot of this database, not temporary, two-phase, failover or synced, not
  invalidated, and must not have lost WAL. A session of the same claim left over from an earlier
  run (`application_name` `laminar:<claim id>:…`) is ended first. A slot held by any other
  session fails the start with a retryable error that names its pid, `application_name` and
  client address.
- A restart from a committed claim whose slot already exists adopts that slot from its
  consistent point, in both snapshot modes. With `snapshot.mode=initial` the source also creates
  a temporary slot `laminar_probe_<claim id>_<…>` to learn which transactions the copy must
  include; PostgreSQL drops it when that session ends a moment later. Like any slot creation, it
  waits for running transactions that have written something.
- Other slots under the prefix are reported and never dropped. Each inactive one is logged with
  the WAL it retains and the statement that drops it, and counted in
  `postgres_cdc_orphaned_slots`. Give every source its own prefix: an inactive slot of another
  source under the same prefix looks like an orphan. Drop orphans once no pipeline restores from
  a checkpoint that names them:

  ```sql
  SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE slot_name ~ '^<prefix>_[0-9a-f]{16}$' AND NOT active;
  ```

#### Types

| PostgreSQL | Declare as |
|---|---|
| `bool` | `BOOLEAN` |
| `int2` / `int4` / `int8` | `SMALLINT` / `INT` / `BIGINT` |
| `float4` / `float8` | `REAL` / `DOUBLE`; NaN and infinities are kept |
| `numeric(p,s)` with `p ≤ 38`, `s ≥ 0` | `DECIMAL(p,s)`, exact. Unconstrained `numeric`, NaN and infinities are rejected |
| `text`, `varchar`, `char`, `name`, `json`, `jsonb`, `uuid` | `VARCHAR`; JSON as PostgreSQL prints it |
| `bytea` | `BYTEA` |
| `date`, `time` | `DATE`, `TIME` (microseconds) |
| `timestamp`, `timestamptz` | `TIMESTAMP` (microseconds; `timestamptz` as UTC) |

Other types, arrays, and mismatched declarations fail at startup; an `infinity` date or
timestamp fails the source. Snapshot and replication sessions pin `DateStyle`, `TimeZone=UTC`,
`extra_float_digits` and `bytea_output`, so a value decodes the same from either path.

#### Row semantics

- Rows arrive in commit order, one whole transaction at a time; a rolled-back transaction emits
  nothing. Repeated changes to one key in a transaction are all delivered, in order.
- `upsert` deletes carry only the key, so non-key columns must be nullable.
- An unchanged TOAST value in an update is restored from the full old row.
- `TRUNCATE` stops the source: it has no row images to apply. Reset as described below.
- A change to the table's columns, types or replica identity stops the source at the first row
  change after it. The publication and slot are rechecked, and orphaned slots reported, as
  checkpoints commit, at most every 30 seconds; that check is best-effort, so restrict who can
  alter them.

#### Initial snapshot

The source streams from the slot's consistent point and, at the same time, copies the table in
primary-key chunks. Each row reaches the target once as copied state, followed by every change
committed after it was copied: no gap and no overlap.

- Each chunk is one short `REPEATABLE READ, READ ONLY` transaction. It takes an `ACCESS SHARE`
  lock on the table, reads its snapshot and WAL position, reads the next primary-key range (sized
  from `max.buffered.bytes`, at most 65,536 rows), and writes an empty logical-decoding message.
  The message lets the stream confirm it has passed that position even when nothing else is
  written. No transaction stays open between chunks, so vacuum is not held back. A DDL
  statement that needs a stronger lock waits for at most one chunk. A chunk transaction waits at
  most 5 s for its lock, each statement in it runs for at most 20 s, and an attempt that takes
  longer than 30 s is abandoned and retried on a new session. One poll can still take up to
  60 s against a slow server, so keep the checkpoint timeout above that while a copy runs (the
  default is 120 s): a poll that exceeds it fails the source, and in `changelog` mode that
  failure needs the reset below.
- A chunk is emitted once the stream has passed the position it was read at, in batches of the
  pipeline's poll size, so a chunk of wide rows still fits a sink's input limit. Changes its
  snapshot missed are merged into it first. A change is matched to a chunk by transaction
  visibility, not by WAL position. A transaction that has committed but is not yet visible to
  new snapshots, such as one waiting for a synchronous standby, pauses copying until it becomes
  visible; streaming continues meanwhile. After 30 seconds the source logs the sessions it waits
  for and their wait events.
- No row is copied until a checkpoint naming the new slot commits, about one checkpoint interval
  after the slot is created.
- Checkpoints keep committing during the copy and confirm the slot's position, so PostgreSQL
  releases WAL while the table is copied. The source also checks the slot's `wal_status` every 10
  seconds and fails if `max_slot_wal_keep_size` removed WAL it needs.
- A change to the table's columns, types, replica identity or primary key during the copy stops
  the source before the next chunk is read. A table rewrite is not visible consistently to a
  snapshot taken across it.
- A restart inside the copy resumes it in `upsert` mode. Deletes are always forwarded during the
  copy, so rows the target received before the restart are corrected. In `changelog` mode the
  restart fails closed and names the reset: a changelog consumer would keep rows it received
  before the restart that changed before they were copied again.
- The copy needs a primary server, because it reads WAL insert positions. Primary-key columns
  must use deterministic collations, the column's own collation in the index, and the default
  operator class: keys are compared on the server in the index's order, and equal keys must be
  equal texts. These are checked at startup, before any slot exists.
- Throughput: on the workstation of the latency figures below, 1,000,000 narrow rows were copied
  into a PostgreSQL mirror on the same server in 4.9–6.1 s (17 chunks, about 165,000–200,000
  rows/s), with or without concurrent writes, and 5,000,000 rows in 27.5 s; 100,000 rows of
  4 KiB took about 4.5 s. A chunk waited about 10 ms (median) for the stream to pass its
  position; the slowest waited up to 2 s, about as long as the slowest checkpoint. During the
  5,000,000-row copy the slot's confirmed position advanced 11 times, and the slot retained at
  most 190 MiB of the 937 MiB of WAL written meanwhile.

#### Delivery, recovery and resources

- At-least-once. The slot's confirmed position advances only after a checkpoint commits (sinks
  flushed, manifest persisted), so PostgreSQL keeps WAL for every row still in the pipeline.
  After a crash the source replays from the last committed position. Keyed sinks converge, and
  aggregates restore their state from the same checkpoint, so replayed changes are not counted
  twice. Exactly-once is not available.
- A checkpoint records the server's system identifier and timeline, the database, publication,
  table and exact slot. Resuming against another server, a promoted replica, or a changed
  publication fails closed.
- A lost replication connection, or another recoverable fault, restarts the pipeline in process
  when supervision is on (the server default; `LaminarDB::enable_supervision()` when embedded):
  the source reattaches to its slot at the last committed position. A restart before the first
  checkpoint commits makes a new claim. A restart after the claim commits but before a checkpoint
  naming the slot commits has emitted nothing, so it creates the claimed slot or adopts it from
  its consistent point; neither needs a reset. A restart inside the initial snapshot resumes the
  copy in `upsert` mode and fails closed with the reset in `changelog` mode.
- The PostgreSQL sink paces itself by how fast the target applied its recent statements. It
  flushes before the next batch would make a flush take more than half of `statement.timeout.ms`
  (30 s by default), and splits a flush into statements of about a quarter of it inside one
  target transaction, so a slow target receives more, smaller transactions instead of a
  statement that times out. A batch is never split, and read directly from the source a batch
  holds whole transactions, so one source transaction must apply within the sink's write
  deadline (`statement.timeout.ms` plus 5 s; twice `statement.timeout.ms` plus 5 s with
  `changelog.mode`; or `sink.write.timeout.ms` when set) and the checkpoint timeout (120 s by default). A larger transaction
  faults the pipeline on every attempt: raise those limits, or speed up the target.
- Retained WAL grows while the pipeline is stopped or slow. Set `max_slot_wal_keep_size` and
  watch `postgres_cdc_replication_lag_bytes`; a slot that has lost WAL cannot resume. An idle
  captured table does not hold back WAL written by other tables: once every received change has
  been emitted, the source position follows the server's keepalives, and the next committed
  checkpoint confirms it.
- `max.buffered.bytes` bounds the source. One transaction may use about a sixth of it while it is
  decoded; a larger one fails the source with a message naming the limit. While downstream is
  slow the source stops reading WAL but still reports committed progress to the server.
- Reset, when an error asks for it: stop the pipeline, run the
  `SELECT pg_drop_replication_slot('<slot>')` the error names (or the prefix query under
  [Slots](#slots)), delete the pipeline's checkpoints, empty the targets, and start again.
- Latency: rows reach the PostgreSQL sink mostly on its own flush schedule
  (`flush.interval.ms`, default 250 ms) rather than at checkpoints. On one Windows 11 workstation
  (release build, PostgreSQL 17 in Docker, sink in the same server), once the source streamed,
  single-row commits reached the target in 120–128 ms p50 and under 270 ms p99 with 1 s
  checkpoints, and 81–84 ms p50 with 100 ms checkpoints; stream subscribers saw them in 8–13 ms
  p50 and 28–36 ms p99. A 200,000-row transaction arrived 2.3 s after commit (about 89,000
  rows/s), and 200,000 rows written while stopped were caught up 1.7 s after a restart. A new
  source streamed 2.6 s after it started with 1 s checkpoints and 0.6–0.8 s with 100 ms
  checkpoints (see [Slots](#slots)); rows committed before that arrive with the initial copy.
  Rerun the ignored `latency` tests in `crates/laminar-db/tests/postgres_cdc_e2e.rs` on your own
  hardware before relying on a number.
- Metrics: `postgres_cdc_events_received_total`, `postgres_cdc_inserts_total`,
  `postgres_cdc_updates_total`, `postgres_cdc_deletes_total`, `postgres_cdc_transactions_total`,
  `postgres_cdc_snapshot_rows_total`, `postgres_cdc_confirmed_flush_lsn`,
  `postgres_cdc_replication_lag_bytes`.

#### Unique constraints on the target

A primary-key change, or a unique value moving from one row to another, reaches the sink as a
delete and an insert in the same epoch. The PostgreSQL sink applies each epoch in one transaction
with `SET CONSTRAINTS ALL DEFERRED`, so other unique constraints on the target must be
`DEFERRABLE`. Changelog upsert sinks reject non-deferrable unique or exclusion constraints and
plain unique indexes at startup.

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

### Kafka

Client: rust-rdkafka 0.39.0 over librdkafka 2.16.0 (a vendored `rdkafka-sys` with regenerated
bindings, static CMake build; see `vendor/rdkafka-sys/VENDORED.md`). The table records what this
tree implements; "rejected" means startup fails before connector I/O with the stated reason.

| Capability | Embedded / single-node | Cluster |
|---|---|---|
| Source, append event log, at-least-once | Implemented; a compacted topic (`cleanup.policy` includes `compact`) is rejected under a guarantee | Implemented (engine-assigned partitions per vnode) |
| Source as exactly-once input | Implemented (exact-certified, `read_committed` required) | Implemented with a certified exactly-once sink |
| Source, Debezium envelope (keyed upsert by `PRIMARY KEY`) | Temporal-join right side, bounded interval join, direct keyed sinks | Rejected outside the admitted join routes |
| Source, Kafka table (key + value = upsert, key + null = delete) | Not implemented | Not implemented |
| Source, regex subscription or partition/topic additions under a guarantee | Rejected | Rejected |
| Sink, append, at-least-once | Implemented | Implemented (independent writers) |
| Sink, upsert (compacted topic), at-least-once | Implemented (singleton writer) | Rejected: no fencing for a former owner's delayed writes |
| Sink, exactly-once (append and upsert) | Implemented: checkpoint-staged output published by one fenced transactional committer | Rejected until the cluster kill-9 qualification passes; the protocol already handles multiple participants and leader fencing tokens |
| Sink, upsert under exactly-once | Implemented (any participant may stage; only the fenced committer writes) | Rejected with cluster exactly-once |

Remaining known gaps, tracked until fixed: OAUTHBEARER has no token provider (rejected); GSSAPI
works only where librdkafka reports `sasl_gssapi` (the Windows build uses SSPI; Linux builds do
not include Cyrus SASL); snappy is not built on Windows; zstd requires the `kafka-zstd` feature.

#### Client configuration

Every option the connector sets has a typed option. A `kafka.*` passthrough property that sets one
of them, or a librdkafka alias of one (`linger.ms`/`queue.buffering.max.ms`,
`acks`/`request.required.acks`, ...), or a protected property (credentials, TLS, idempotence,
transactions, `isolation.level`, `group.protocol`, offset storage) fails validation. SASL
mechanisms, TLS and compression codecs are checked against librdkafka's `builtin.features`
before a client is created. Sources and sinks log their effective configuration with secrets
replaced by `***`.

`latency.profile` supplies batching defaults beneath explicit options; it never changes acks,
idempotence, isolation or the delivery guarantee:

| Profile | Source `max.poll.records` / `fetch.min.bytes` / `fetch.max.wait.ms` | Sink `linger.ms` / `batch.size` / `compression.type` |
|---|---|---|
| `low_latency` | 256 / 1 / 10 | 0 / 16384 / none |
| `balanced` (default) | 1000 / librdkafka default (1) / librdkafka default (500) | 5 / 16384 / none |
| `throughput` | 8192 / 65536 / 100 | 50 / 1048576 / lz4 |

`group.protocol = consumer` selects KIP-848 and rejects the classic `session.timeout.ms`,
`heartbeat.interval.ms` and `partition.assignment.strategy`. Guaranteed delivery assigns partitions
manually, so the group protocol and static membership affect only best-effort subscriptions and
advisory offset commits; guaranteed delivery rejects `group.instance.id`.

#### Source records

- `metadata.columns` appends typed columns after the value columns, in this order: `_key`
  (nullable Binary), `_topic`, `_partition`, `_offset`, `_timestamp` (nullable ms timestamp),
  `_timestamp_type` (`create_time`, `log_append_time`, `not_available`) and `_headers`
  (`List<Struct<key Utf8, value nullable Binary>>`, in record order with duplicates). A value
  field with one of those names is rejected. Keys and headers are copied only when selected.
- A record with a NULL value produces no row in an append or Debezium source (for Debezium it is
  the tombstone that follows a delete, which the `d` event already applied) and still advances
  the cursor; a poll of only such records emits a zero-row batch that carries the cursor.
- A record larger than `record.max.bytes` (key and value; default 16 MiB) or with a non-UTF-8
  header name cannot become a row. It fails a guaranteed source at that offset and is skipped,
  with its offset, by a best-effort one, like an undecodable record.
- Retention: a guaranteed append source checks each subscribed topic's `cleanup.policy` at
  startup and rejects `compact`, which can delete records inside a checkpoint's replay range
  (read with DescribeConfigs; when that is denied or unanswered the source starts and logs a
  warning). A Debezium source reads compacted topics: its keyed state converges on the newest
  record per key. Records that `retention.ms` or `retention.bytes` remove before a restart cannot
  be replayed; the source then fails at startup instead of skipping them.
- Avro writer schemas: with a Schema Registry, at most 64 writer schemas are registered at once;
  the least recently used one that the current poll does not need is evicted and fetched again
  when it reappears. A single poll needing more than 64 fails with a `max.poll.records` hint.
  Registry TLS uses the default trust roots unless `schema.registry.ssl.ca.location` is set, and a
  client certificate (`schema.registry.ssl.certificate.location` plus `.key.location`) applies in
  both cases.
- Memory: the reader queue holds at most `reader.channel.capacity` records and
  `reader.channel.max.bytes` connector-owned bytes (default 64 MiB); a poll stages at most
  `max.poll.records` records and `max.poll.bytes` bytes (default 16 MiB, at least one record).
  librdkafka's own prefetch is bounded per partition by `queued.max.messages.kbytes` (16 MiB),
  which it may overshoot by one fetch response (`fetch.message.max.bytes`), or by a single larger
  message up to `receive.message.max.bytes`.

#### Sink keys and partitions

`key.column` names one column, or several with `key.format = 'json'`. `key.format` decides the
key bytes:

| `key.format` | Key bytes | Column types |
|---|---|---|
| `native` (default) | UTF-8 for strings, raw bytes for binary (Java `StringSerializer`/`ByteArraySerializer`) | Utf8, LargeUtf8, Utf8View, Binary, LargeBinary, BinaryView, FixedSizeBinary |
| `kafka` | `native` plus big-endian `Short`/`Integer`/`Long`/`Float`/`Double` serializer bytes | adds Int16, Int32, Int64, Float32, Float64 |
| `text` | The SQL text rendering of the value | any single column |
| `json` | A JSON object of the key columns, NULLs written explicitly | any; never a NULL key |

A SQL NULL key is a record without a key, distinct from an empty key. Keyed records are routed
with Kafka's Java partitioner, `toPositive(murmur2(key)) % partitions`, checked against Java
reference vectors; records without a key rotate partitions once per written batch. Every record
carries an explicit partition, and records within a partition keep their write order; there is no
order across partitions.

`partition.count` pins the partition count keys are hashed over. Opening fails if the topic has
fewer partitions; added partitions receive no keyed records, so existing keys keep their
partition. `envelope = 'upsert'` requires `partition.count` and the `key-hash` partitioner (the
`round-robin` and `sticky` strategies ignore the key), and rejects NULL merge keys before
producing a batch. Without a pin, an append sink hashes over the count it observed at open, so
partitions added before a restart move keys from then on.

#### Exactly-once sink protocol

librdkafka (2.16.0 included) cannot resume another producer's open transaction (no KIP-939), so an epoch's
output must be recoverable without the process that buffered it. The sink stages it in the
checkpoint itself and publishes it only after the engine's durable Commit decision:

1. During epoch `N`, `write_batch` serializes and routes each record (explicit partition, key,
   value, headers) into a bounded in-memory epoch buffer. No broker I/O.
2. `pre_commit(N)` returns the compressed buffer as the phase-one descriptor. The engine
   persists it in the checkpoint manifest with source positions and operator state, publishes
   the committed index and records Commit.
3. The designated committer (the single process in embedded/single-node mode, the leader in
   cluster mode, or whichever process recovers) owns one transactional producer whose
   `transactional.id` is derived from the deployment-scoped commit namespace (pipeline identity,
   deployment UUID, sink id). Every committer of that sink uses the same id, so
   `init_transactions` fences the predecessor and resolves its open transaction.
4. The committer reads the sink's cursor marker from the compacted commit topic under
   `read_committed`. If it already shows checkpoint `N`, the commit is done. Otherwise one
   transaction produces every participant's staged records in participant and record order
   plus the marker `{checkpoint, fencing token, batch fingerprint}`, then commits.
5. Source acknowledgements and advisory broker offset commits follow the engine's committed
   checkpoint; they are never recovery authority.

| Failure point | What is durable | Recovery |
|---|---|---|
| Before `pre_commit` returns | Nothing for `N` | Restore `N-1`, replay input; nothing external |
| After `pre_commit`, before Commit | Possibly a manifest, no Commit | Abort `N`, replay; nothing external |
| After Commit, before publication | Checkpoint `N` with staged output | Fence, cursor shows `N-1`, publish `N` |
| Mid-transaction (partial produce) | Open transaction | Successor `init_transactions` aborts it; publish `N` |
| Commit response lost | Unknown | Fence, then the marker decides: visible means done, absent means publish again |
| After commit, before the engine observes it | Marker `N` | Cursor equals `N`: already committed |
| Transaction timeout or abortable error | Nothing visible | Abort and publish again from the staged descriptor |
| Fenced by a successor | Nothing more from this producer can commit | Outcome unknown; the successor's fenced cursor read decides |

Configuration: `delivery = exactly_once` (pipeline-wide) plus `commit.topic`, an existing topic
with `cleanup.policy=compact` (checked before fencing; one topic may serve several sinks, each
under its own key). `transaction.timeout.ms` (default 60000, at most 900000) bounds publication;
the delivery deadline is capped to it. `epoch.max.bytes` (default 64 MiB) bounds one epoch's
staged key and value bytes: a write past it fails with a sizing error (output bytes per second x
checkpoint interval), and an epoch whose compressed descriptor exceeds the engine's 16 MiB
descriptor limit fails its checkpoint. `dlq.topic` is rejected because dead-lettered records
would escape the transaction. Output becomes visible when its checkpoint commits, so visibility
latency is about half the checkpoint interval plus checkpoint persistence and one transaction;
the latency profile tunes the publication batching only. The first transactional use of a fresh
Apache Kafka cluster creates `__transaction_state`, which can outlast one commit deadline; the
engine retries the commit.

Fencing: a committer reads the newest marker before `init_transactions` and refuses to fence
when it carries a newer fencing token, so an obsolete committer cannot abort a successor that has
already committed. An obsolete committer that fences a successor which has not yet committed can
only make that successor's in-flight commit fail with an unknown outcome; the successor's retry
fences back, and the cursor check stops the obsolete committer from publishing. Only the engine's
leadership lease keeps an obsolete committer from repeating that interruption.

Evidence: `tests/kafka_exactly_once.rs` (real broker) covers invisible staging and an idempotent
retried commit, a successor publishing a cut its predecessor never published, a successor
fencing an open predecessor transaction (its records exist but are aborted), a lost commit
response reconciled by the marker, obsolete-committer fencing and token regression, multiple
participants with an overlapping recovery batch and empty participants, and upsert tombstones.
It passes on Redpanda 26.1.13 and Apache Kafka 4.1.1.

Scope: each committed checkpoint's output becomes visible atomically to `read_committed`
consumers of the output topic, once per logical output record. It does not deduplicate equal
input records, cover side effects inside UDFs, or make several sinks' commits one atomic
snapshot.

### Delta Lake change data feed

The `delta-lake` source reads a Delta table's change data feed (CDF) and snapshots with the Delta
kernel and emits a full changelog: `__weight` is `+1` for inserts and update post-images and `-1`
for deletes and update pre-images. Its consumers are the same as for a PostgreSQL CDC changelog:
projections and filters, `COUNT`/`SUM`/`AVG` with `EMIT CHANGES`, and full-changelog sinks such as
Delta `write.mode=upsert` or PostgreSQL upsert with `changelog.mode=true`. `MIN`/`MAX`,
`DISTINCT` aggregates, and append-only sinks are rejected before any I/O.

| Option | Meaning |
|---|---|
| `read.mode = 'changes'` (default), `starting.version` unset | Pin the version current at first start, emit that snapshot as inserts, then every later commit |
| `starting.version = '<n>'` | Emit commits from version `n` (inclusive), without a snapshot. Rows that existed before `n` arrive only as their later updates and deletes |
| `starting.version = 'latest'` | Emit only commits after the version current at first start |
| `read.mode = 'snapshot'` | Emit the latest snapshot and then complete; no CDF needed |
| `poll.interval.ms` (default 1000) | How often the log is checked for new commits |

Changes mode requires `delta.enableChangeDataFeed = true`. The kernel applies deletion vectors
(a vector added to a file becomes deletes of exactly those rows), reads `cdc` files when a commit
has them instead of deriving changes from its `add`/`remove` actions, and skips
`dataChange = false` rewrites such as `OPTIMIZE`. It rejects column-mapped tables in changes mode
(snapshots read them) and any reader feature it does not support.

Recovery is at-least-once in embedded and single-node mode; cluster mode and exactly-once reject
the source. Replay units are one commit, or one file of the pinned snapshot in path order. The
checkpoint cursor (table id, phase, next commit or completed files) only ever stands between
units, so a barrier that arrives inside a commit waits for the commit's last row. A checkpoint
therefore takes at least as long as reading the largest commit or snapshot file; keep the
checkpoint timeout above that. Resuming re-reads the unit after the cursor and never resumes
inside one. The reader runs on the blocking pool with at most 256 MiB of decoded rows queued and
hands batches out in `max_records` slices; one decoded batch above that bound fails the read.

A recreated table (new table id), a log that regressed behind the cursor, an expired or vacuumed
commit or snapshot file, or a schema change fails the source instead of reading another position.
Reset the source (DROP/CREATE) to start again. Reference tables stream their snapshot batch by
batch; lookups are separate.

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
| Kafka | `kafka` | Durable broker-acknowledged at-least-once (append, or upsert from a singleton writer); exactly-once append and upsert in embedded/single-node mode via checkpoint-staged output and one fenced transactional committer | Implemented |
| NATS | `nats` | JetStream durable at-least-once after stream validation; Core is ephemeral | Implemented |
| RabbitMQ | `rabbitmq` | Publisher-confirmed, mandatory publishes to one exchange; durable at-least-once for persistent messages on durable queues | Implemented |
| IBM MQ | `ibmmq` | `MQPUT` under syncpoint with `MQCMIT` per unit of work; durable at-least-once for persistent messages | Implemented |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, durable at-least-once | Implemented |
| MongoDB | `mongodb-cdc` | Majority-journaled ordered writes, upsert, history `cdc_replay`, durable at-least-once | Implemented |
| Delta Lake | `delta-lake` | Coordinated append (cluster EO certified only for direct S3/S3A); at-least-once append, MERGE upsert on non-null keys, and whole-table overwrite | Implemented |
| Iceberg | `iceberg` | Rolling append writer; direct ALO, coordinated local EO, and REST + direct S3/S3A cluster EO; MOR/COW fail closed | Implemented |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | Implemented |
| WebSocket Client | `websocket` | Push to external server | Implemented |
| Files | `files` | Local CSV, JSON, Parquet rolling output; remote URLs fail at startup | Implemented |

Iceberg REST supports no authentication, a resolved static bearer token, or OAuth2 client credentials with proactive token refresh. Access delegation, vended storage credentials, and remote signing fail closed. Cluster exactly-once Iceberg admission remains limited to no authentication or static bearer authentication pending an OAuth2 cluster recovery fault matrix. Data-storage credentials for cluster exactly-once belong under `storage.property.*`; secret-bearing `catalog.property.*` values are treated as uncertified catalog authentication.

Backend support and native-provider evidence are tracked independently in the
[cloud object-store support matrix](../../docs/cloud-object-store-support.md). Azure Iceberg is
experimental; remote Files source/sink URLs are unsupported.

Delta sink write modes have distinct contracts. `append` writes every input row; a changelog input
keeps its `__weight` column, so the table is a change log, not current state. `upsert` collapses
each flush to one row per `merge.key.columns` value and MERGEs it; the keys must be non-nullable
because SQL-equality MERGE never matches a NULL key. `overwrite` atomically replaces the whole
table, every partition included, with each flush's rows. Only coordinated `append` is
exactly-once. Coordinated append stages files with a writer that does not evaluate CHECK
constraints, column invariants, or generation expressions, so tables declaring them are rejected
for it; at-least-once `append` and `upsert` use delta-rs operations that enforce them. A configured
`partition.columns` is applied when `auto.create` creates the table and must match an existing
table's partitioning. A retry after a failed write reopens the table read-only and fails if the
target disappeared, was recreated, or no longer matches the bound schema.

#### Unity Catalog

`catalog.type = 'unity'` addresses a table as `uc://<catalog>.<schema>.<table>` through the Unity
Catalog API (`catalog.workspace_url`), with either `catalog.access_token` or OAuth
machine-to-machine credentials (`catalog.client_id`, `catalog.client_secret`; tokens renew
automatically). With the default `catalog.storage.credentials = 'vended'`, LaminarDB requests
temporary credentials for that table — `READ` for sources, reference and lookup tables,
`READ_WRITE` for sinks — and renews them before they expire, between replay units and before each
flush or coordinated pre-commit. Vending needs external data access enabled on the metastore and
`EXTERNAL USE SCHEMA` for the principal; a refusal fails the connector. On-demand lookups keep the
credentials they open with, so they refuse vended credentials; use a reference table instead.
Tables with row filters or column masks are not vended. `'configured'` uses the
connector's own `storage.*` credentials or cloud identity instead and bypasses Unity Catalog's
storage governance.

External Delta tables can be read and written. Managed tables can be read with vended credentials
and are never written: their commits belong to the catalog (catalog-managed commits), which the
pinned delta-rs writer does not implement. Views and non-Delta formats are rejected. Registering
a missing table (`catalog.storage.location`) requires `catalog.access_token`. Open-source Unity
Catalog uses the same API; plain `http` is accepted only for a loopback server.

```sql
CREATE SOURCE orders FROM "delta-lake" (
  'table.path' = 'uc://main.sales.orders',
  'catalog.type' = 'unity',
  'catalog.workspace_url' = 'https://<workspace>.cloud.databricks.com',
  'catalog.client_id' = '${UC_CLIENT_ID}',
  'catalog.client_secret' = '${UC_CLIENT_SECRET}',
  'catalog.name' = 'main',
  'catalog.schema' = 'sales'
);
-- Upsert merge keys must be non-nullable: region is NOT NULL in this table.
CREATE STREAM revenue AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS orders FROM orders GROUP BY region EMIT CHANGES;
CREATE SINK revenue_lake FROM revenue INTO "delta-lake" (
  'table.path' = 's3://lake/revenue', 'write.mode' = 'upsert',
  'merge.key.columns' = 'region', 'auto.create' = 'true'
);
```

#### Delta support matrix

delta-rs 0.32.4 with its Delta kernel (buoyant_kernel 0.22.2), Arrow 58, DataFusion 53. ALO is
at-least-once; EO is exactly-once. "Local" means embedded and single-node; cluster rules are in
the [object-store matrix](../../docs/cloud-object-store-support.md).

| Table access | Snapshot / CDF read | Append | MERGE upsert | Overwrite | Evidence |
|---|---|---|---|---|---|
| Direct storage (`file`, `s3`, `az`, `gs`) | Yes / yes, local ALO | ALO; coordinated EO (cluster EO direct S3/S3A only) | ALO | ALO, whole table | Local tests; Spark 4.2 / delta-spark 4.4 oracle; cloud per object-store matrix |
| Unity Catalog external table | Yes / yes, vended or configured credentials | ALO; coordinated EO local | ALO | ALO | Mocked Unity API tests; live Databricks and OSS UC unverified |
| Unity Catalog managed table | Yes / yes, vended credentials only | Rejected | Rejected | Rejected | Mocked Unity API tests; live unverified |
| Catalog-managed (`catalogManaged`, catalog commits) | Rejected | Rejected | Rejected | Rejected | Blocked: delta-rs has no catalog commit path ([delta-rs#4549](https://github.com/delta-io/delta-rs/issues/4549)); the kernel UC committer (`delta-kernel-unity-catalog` 0.1.0) is a separate delta_kernel 0.29 writer stack |
| Row filters, column masks, ABAC | Rejected (Unity refuses vending) | Rejected | Rejected | Rejected | Blocked: Databricks enforces these for external engines only through Spark server-side planning; `configured` credentials would bypass them and must not be used for such tables |
| AWS Glue | Location lookup, then as direct storage | As direct storage; no registration or schema sync | ALO | ALO | Existing Glue lookup tests |
| Hive Metastore, Polaris generic tables, Delta Sharing | Not implemented | — | — | — | Polaris generic tables (beta) offer no commit coordination or credential vending; Delta Sharing is a read-only sharing protocol |

Table features follow the libraries' own protocol checks. Reads: deletion vectors (snapshot and
CDF, tested with Spark-written vectors), column mapping (snapshot only; CDF rejects it), and
timestamp_ntz are supported; the kernel rejects reader features it does not support. Writes
use delta-rs's writer feature list: append-only, invariants, CHECK constraints, generated columns,
change data feed, deletion vectors, timestamp_ntz and variant are accepted (coordinated append
additionally rejects invariants, CHECK constraints and generated columns); column mapping,
identity columns, row tracking, clustering, type widening, UniForm (Iceberg compatibility) and
catalog-managed tables are rejected before data is published. Only the cases named as tested
have dedicated tests; the rest rely on the upstream check.

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
| `postgres/cdc` | PostgreSQL CDC source: catalog contract, snapshot, `pgoutput` decoding, typed rows, slot feedback |
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
| `postgres-cdc` | PostgreSQL CDC source; also builds the `postgres` lookup source |
| `postgres-sink` | PostgreSQL sink via tokio-postgres |
| `mongodb-cdc` | MongoDB CDC source, sink, and lookup |
| `nats` | NATS Core and JetStream source/sink via async-nats |
| `rabbitmq` | RabbitMQ AMQP 0-9-1 source/sink via lapin (pure Rust, rustls) |
| `ibmmq` | IBM MQ source/sink via the `mqi` binding; IBM's client library is loaded at run time, so builds need no IBM software |
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
| `fpml` | FpML format for the files, Kafka, RabbitMQ, and IBM MQ sources and FIX fields; builds a vendored libxml2 with CMake |

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
