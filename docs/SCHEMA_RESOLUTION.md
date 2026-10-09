# Connector schema resolution

Implementation contract for LaminarDB 0.32.0, updated 2026-10-08.

Sources can omit columns when their connector has an authoritative metadata or
protocol schema. Sinks derive their input fields from a named source or stream.
Creation resolves and validates the contract before activation. Durable local
deployments publish it in the checkpoint namespace; cluster deployments retain it
in the fenced catalog manifest. Ephemeral embedded deployments retain it in memory.

SQL, server configuration and embedded `execute` use the same lifecycle. A console
does not need to expand columns. Custom connector factories declare capabilities in
`ConnectorInfo::schema_capabilities` and implement the existing source, sink,
reference-table or lookup resolution hook. Direct connector users must resolve,
validate, explicitly prepare and retain the binding themselves; using a raw
connector does not create a database catalog or grant durable delivery.

## Direction and format capabilities

All entries require their connector feature. Resolution does not widen runtime or
delivery admission. A registered connector whose feature is disabled reports that
limitation before activation.

| Registered name | Direction | Formats / authority | Omitted fields | Native identity and restrictions |
| --- | --- | --- | --- | --- |
| `generator` | Streaming source | Fixed `seq`, `ts_ms`, `value` protocol | Built-in | Deterministic generator configuration; explicit fields must retain the full protocol layout |
| `kafka` | Streaming source | Registry-backed Avro | Metadata | Registry scope, concrete value subject/version/ID, complete native schema and references |
| `kafka` | Streaming source | Plain JSON, CSV, Debezium JSON | Explicit fields required | A registry URL requires Avro; it does not select a different codec |
| `kafka` | Streaming source | Raw / bytes | Built-in `value` | Existing Kafka metadata/header settings remain separate |
| `kafka` | Sink | Avro, JSON, CSV, raw | Bound query | Avro uses a concrete registry writer ID; raw needs one Utf8 query field; no Debezium writer |
| `postgres-cdc` | Streaming source | Fixed pgoutput JSON envelope plus publication metadata | Metadata | PostgreSQL 17+, system/database/publication/slot identities and published relation layouts; requires the existing recovery slot; no initial snapshot-to-WAL admission |
| `postgres` | Reference / lookup source | PostgreSQL table catalog | Metadata | Database/relation OIDs, column OIDs/modifiers/defaults/constraints; keys separately declared and validated |
| `postgres-sink` | Sink | Named PostgreSQL COPY / keyed writes | Bound query plus target metadata | Database/relation OIDs and target layout; explicit table-creation policy; no implicit evolution |
| `delta-lake` | Streaming / reference / lookup source | Delta log/catalog metadata | Metadata | Table ID, native schema, protocol and column mapping; the SQL schema is pinned while the data cursor advances |
| `delta-lake` | Sink | Delta target metadata | Bound query | Table ID and validated writer mapping; `auto.create` is separate; durable bindings require `schema.evolution=false` |
| `iceberg` | Streaming / reference / lookup source | Catalog/table metadata | Metadata | Table UUID, schema/field IDs, partition spec and sort order; data position remains a checkpoint concern |
| `iceberg` | Sink | Iceberg target metadata | Bound query | ID-aware alignment and native commits; creation requires `auto.create`; provider features and existing exact certification still apply |
| `files` | Streaming source | Parquet / Arrow IPC embedded metadata | Metadata | Deterministically sorted bounded local file set, representative file size/mtime, complete Arrow schema |
| `files` | Streaming source | Text | Built-in | Fixed text decoder schema; optional `_metadata` remains separately configured |
| `files` | Streaming source | CSV / newline-delimited JSON | Explicit or opt-in sample | Sampling does not establish keys, uniqueness or event time |
| `files` | Sink | JSON, CSV, text, Parquet, Arrow IPC | Bound query | New writer needs no fictional remote schema; existing binary dataset must be compatible; one OS writer lease per output prefix |
| `mongodb-cdc` | Streaming source | `output.mode=history`: fixed versioned history records; `document`: declared typed projection | Metadata | Deployment and collection UUID bound before the first record; document mode needs a `PRIMARY KEY` equal to the document key and nullable non-key columns |
| `mongodb` | Lookup source | BSON collection validator | Closed flat validator, otherwise explicit projection | Collection UUID and validator retained; supported scalar types only; unique single-column key/index separately validated |
| `mongodb-sink` | Sink | BSON query writer plus collection validator | Bound query | Standard collection UUID or explicit time-series bucket UUID; flat validator checks; explicit `auto.create` for a missing standard collection; explicit time-series settings retain their existing preparation policy |
| `nats` | Streaming source | JSON / CSV / Debezium JSON; raw | Explicit fields; raw built-in | No authoritative JSON descriptor; existing ephemeral source contract remains |
| `nats` | Sink | JSON / CSV / raw | Bound query | Core and JetStream delivery admission remains; raw requires one Utf8 field |
| `websocket` | Streaming source | JSON / CSV / binary | Explicit fields required | Validate the installed decoder before connecting; no remote discovery API |
| `websocket` | Sink server / client | JSON | Bound query | Server and client retain their different topology contracts |
| `otel` | Streaming source | OTLP traces / metrics / logs | Built-in per signal | Explicit fields must retain that signal's full protocol layout |
| Custom factories | Registered directions | Declared capability and hook | According to that declaration | Empty, malformed, oversized and wrong-direction/wrong-connector results are rejected |

Features are `kafka`, `postgres-cdc`, `postgres-sink`, `delta-lake`, `iceberg`
(or its catalog/storage feature composition), `files`, `mongodb-cdc`, `nats`,
`websocket` and `otel`. The generator requires no external connector feature.
Parquet lookup helpers are not a separately registered connector.

Native protocols (generator, OTLP, PostgreSQL, MongoDB and lakehouse connectors)
do not select a serialization codec through `FORMAT`; omit that clause. WebSocket
binary readers require exactly one Binary or LargeBinary field. WebSocket sinks
always emit JSON and reject `FORMAT` settings in both server and client mode.

Embedded and single-node deployments use these contracts within their existing
delivery constraints. Cluster admission remains fail-closed: whole-node reference
tables/materialized views and unsupported distributed SQL remain rejected.
Cluster exactly-once still requires Kafka input and the certified direct S3/S3A
append Delta sink or REST-catalog/direct-S3 append Iceberg sink. Kafka output
remains at-least-once. Schema discovery alone cannot certify any composition.

## Reader, query and writer fields

Explicit source columns preserve their names, order, types and projection.
Metadata does not append business fields or infer missing declarations. Configured
protocol metadata columns are separate. Keys, watermarks and event-time declarations
are validated independently; a timestamp field or a sampled unique value grants
no planner guarantee.

The full Delta streaming-source contract includes its existing `__weight`
changelog column. An explicit compatible projection can omit it. Delta reference
and lookup tables expose business fields only.

A sink begins with the bound input query schema and resolves the destination
schema separately. It maps fields by name or native field identity, rejects extra
business fields and checks required fields, nullability and supported types.
Aliases must name destination fields. Put intentional casts and projections in
`CREATE STREAM`. There is no general implicit cast policy.

PostgreSQL uses named COPY columns, allowing omitted columns only when the database
write path can apply their default/generated/nullable behavior. Generated fields
cannot be supplied as ordinary query fields. Lakehouse alignment follows the
installed Rust libraries and existing writer policy. Delta's established top-level
millisecond-to-microsecond timestamp normalization is retained; unsupported lossy
types, including UInt64, fail before target creation. Engine changelog fields are
consumed only by the supported keyed writer mode and remain outside business data.

MongoDB lookup inference needs a flat, closed `$jsonSchema` with one supported
non-null BSON type per property. Optional properties become nullable. ObjectId,
nested fields, heterogeneous unions, open fields and complex predicates require
an explicit supported projection. Lookup scalars are Int32, Int64, Float64,
Boolean, Utf8 and LargeUtf8. Explicit projection does not turn a validator into
a complete document schema. MongoDB sink validators have a narrower provable
flat policy; complex predicates and opaque CDC replay into a validated target
are rejected when compatibility cannot be established.

## Kafka selection and historical writers

`schema.registry.url` names the registry service, not a `/subjects/...` or
`/schemas/...` resource. Use `FORMAT AVRO` and one of these concrete selectors:

| Property | Policy |
| --- | --- |
| `schema.registry.value.subject` | Explicit value subject; overrides subject derivation |
| `schema.registry.value.version` | Positive concrete version; cannot be combined with an ID |
| `schema.registry.value.id` | Positive concrete ID; cannot be combined with a version |
| No version/ID | Resolve latest once during creation and persist its concrete result |
| Existing naming strategy / `schema.registry.record.name` | Used only when it identifies an unambiguous subject |
| `schema.registry.auto.register` | Defaults to false; explicit sink registration occurs during preparation |

Multiple topics, a regex subscription or record naming without a record/subject
selector must identify one unambiguous reader contract. Authentication failures,
missing subjects and unsupported schema formats fail without sampling or a string
fallback. Registry JSON Schema and Protobuf codecs are not implemented. Kafka keys
retain the existing raw key-column encoding; registry-backed key codecs report
unsupported rather than borrowing the value contract.

Each Avro message's wire ID selects its actual writer schema. The decoder resolves
that writer into the committed reader, including supported native defaults and
record resolution. It does not reinterpret historical records using creation-time
latest. Tombstones and existing CDC handling remain separate. The registry must
retain every historical writer needed for replay: persisting the reader does not
persist every observed or future writer. An unavailable writer fails before cursor
advancement; connector-side bounded fetches apply backpressure.

Sinks prepare a writer once and emit its concrete ID. Restart neither re-registers
nor selects a newer latest. Registry compatibility and actual Arrow-to-writer
compatibility are independent checks. Native defaults, unions, record names and
references are retained rather than reconstructed from Arrow.

## Sampling and resource bounds

File discovery and ingestion use the same directory/glob interpretation. Native
metadata discovery sorts paths, admits at most 4096 entries and 64 selected files,
bounds embedded schema footers to 1 MiB, and honors `max_file_bytes`. Subsequent
files are validated against the frozen reader before their cursor is published.
These local file identities use path/size/mtime; they do not certify arbitrary
same-size, same-mtime content replacement. Remote file URLs remain subject to the
checkout's existing file-connector storage restrictions.

CSV/JSON inference requires `schema.inference = 'true'`. It samples at most four
files, 1 MiB and 1000 rows with a ten-second operation bound and four blocking
worker permits. Cancellation does not release a permit while its worker still
runs. Selection is deterministic; empty and all-null samples fail. Inference
does not consume the ingestion cursor and cannot prove future homogeneity.

Shared resolution admits eight concurrent operations with a thirty-second queue
and work deadline. A binding is at most 4 MiB with 4096 fields, bounded nesting
and 64 native references. Kafka additionally bounds registry bodies, individual
schemas, transitive bytes/depth, writer IDs and scoped caches. Credentials and
tokens are excluded from native identity/fingerprints. HTTP redirects cannot
forward registry credentials to another origin.

## Durable creation, recovery and migration

Creation validates deployment/delivery and dependency generations, resolves
metadata without holding catalog locks, validates the mapping, performs only
explicitly authorized preparation, rechecks authority and conditionally publishes
the binding. Activation uses that definition. Catalog dependencies changing during
resolution require a retry; shutdown and lost authority fence stale completion.

Bindings use representation version 1, complete Arrow/native JSON and canonical
SHA-256 fingerprints. They preserve nested metadata, decimal precision/scale and
timestamp units/timezones. The original user DDL is retained separately from the
resolved contract. Unchanged schema-less startup configuration and `IF NOT EXISTS`
reuse a binding; real defining-configuration drift fails. Local contracts are in
`catalog/schema-contracts-v1.json` under the checkpoint namespace's exclusive OS
lease. Cluster catalog entries carry the binding through the existing generation
and conditional manifest authority. Recovery identities include the contract.

Existing explicit legacy catalog records reconstruct their declared logical
schema deterministically. They do not acquire a new native identity by fetching
latest. Registry-backed Avro, metadata-bound tables and CDC collections whose legacy record lacks native
identity fail before activation and require migration. An unresolved legacy record
that lacks fields fails with a controlled
migration remedy. To migrate, preserve the existing catalog/checkpoint cut, obtain
the native identity and complete schema for that cut, and validate a deliberate
replacement definition through the deployment's existing catalog migration path.
Do not erase state or edit a checkpoint to make a different schema load.

For rollback, retain a snapshot of the original catalog/checkpoint namespace and
use a binary that understands every committed binding version. A pre-binding
binary cannot safely reinterpret new schema-less entries. Reverting application
code alone is insufficient after publishing new contracts; coordinate catalog and
state compatibility before returning to the saved cut. There is no automatic
latest-based migration or silent fallback to a different checkpoint.

Replay can produce many small interval-join batches. When this exceeds a graph
port's batch-count budget, LaminarDB uses its existing bounded coalescer only if
every downstream consumer is an initialized COUNT/MIN/MAX aggregate whose input
projection contains columns or literals without a filter. Rows retain their
order and schema. Weighted input and unsupported Arrow representations preserve
their original batch boundaries. Newly combined batches are bounded at 1,024
rows and 256 KiB of logical data, and all destinations still pass the original
count and retained byte limits before publication.

Other plans and output that still cannot fit retain terminal admission failure.
The graph generation is fenced against retry and checkpoint drain, and durable
terminal authority continues to keep intake closed across recovery and leadership
changes. Upgrading does not erase terminal faults from an earlier run.

External creation/registration and catalog publication do not share a transaction.
Failure or lost leadership can leave an unused authorized external table, collection
or schema. LaminarDB does not delete it automatically. Fencing prevents local
activation after stale completion. Native PostgreSQL transactions and lakehouse
commit checks protect their supported write paths. MongoDB UUID checks around
cache misses and before flush detect drift, but its name-based write API does not
offer an atomic UUID precondition; replacement between the final check and write
remains a connector limitation. Connectivity, authorization and resource identity
checks are still required after restart.

`DESCRIBE name` retains its existing first three columns and adds schema origin,
contract generation, mapping, native identity/version and fingerprint. Resolution
metrics are `laminar_schema_resolution_seconds`,
`laminar_schema_resolution_total` and `laminar_schema_resolution_active`, with
bounded direction/outcome labels. Kafka exposes schema cache hit/miss, fetch failure,
fetch-time and unresolved-record gauges through its existing metrics family.

## Examples

These use the installed connector syntax. External resources must exist unless
creation is explicitly enabled. Use environment references for secrets in durable
SQL. Server TOML's existing `$${ENV}` interpolation rules remain unchanged.

Registry-backed Kafka source and query-derived writer:

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
DESCRIBE events;
```

PostgreSQL reference snapshot, on-demand lookup join and existing sink, with separately declared keys:

```sql
CREATE TABLE dimension_snapshot (PRIMARY KEY (id)) WITH (
  connector = 'postgres',
  connection = '${SCHEMA_PG_CONNECTION}', table = 'public.dimensions',
  "ssl.mode" = 'disable'
);
CREATE LOOKUP TABLE dimensions (PRIMARY KEY (id)) WITH (
  'connector' = 'postgres', 'strategy' = 'on-demand',
  'connection' = '${SCHEMA_PG_CONNECTION}', 'table' = 'public.dimensions',
  'ssl.mode' = 'disable'
);
CREATE SOURCE dimension_requests FROM GENERATOR ('rows.per.second' = '100');
CREATE STREAM dimensions_output AS
  SELECT d.label, d.id FROM dimension_requests r JOIN dimensions d ON r.seq = d.id;
CREATE SINK dimensions_copy FROM dimensions_output INTO "postgres-sink" (
  'hostname' = 'localhost', 'database' = 'example', 'username' = 'example',
  'password' = '${SCHEMA_PG_PASSWORD}', 'table.name' = 'dimensions_copy',
  'port' = '${SCHEMA_PG_PORT}', 'ssl.mode' = 'disable'
);
```

Built-in source to a query-derived local file or deliberately created Delta target:

```sql
CREATE SOURCE generated FROM GENERATOR ('rows.per.second' = '100');
CREATE STREAM generated_output AS SELECT seq, value FROM generated;
CREATE SINK files_out FROM generated_output INTO FILES
  ('path' = './output', 'prefix' = 'events') FORMAT PARQUET;
CREATE SINK delta_out FROM generated_output INTO "delta-lake"
  ('table.path' = './delta-output', 'auto.create' = 'true');
```

Parquet metadata and explicit bounded JSON sampling:

```sql
CREATE SOURCE parquet_events FROM FILES ('path' = './input/*.parquet') FORMAT PARQUET;
CREATE SOURCE json_events FROM FILES
  ('path' = './json-input', 'schema.inference' = 'true') FORMAT JSON;
```

The examples are exercised by `schema_sql_examples.rs`. Connector configuration
and feature flags are listed in the [connector guide](../crates/laminar-connectors/README.md).

Native semantics are checked against the installed libraries. Background references:
[Confluent subject strategies and formats](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html),
[registry IDs, versions and references](https://docs.confluent.io/platform/current/schema-registry/develop/api.html),
[Avro directional resolution](https://avro.apache.org/docs/1.12.0/specification/#schema-resolution),
[PostgreSQL relation messages](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html),
[Iceberg field identity](https://iceberg.apache.org/docs/latest/evolution/),
[Delta schema validation](https://docs.delta.io/delta-batch/), and
[Arrow types and metadata](https://arrow.apache.org/docs/format/Columnar.html).
Spark's Delta feature documentation does not establish support in delta-rs.
