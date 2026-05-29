# laminar-connectors

External system connectors for LaminarDB. Each connector implements the `SourceConnector` or `SinkConnector` trait and supports exactly-once semantics via two-phase commit.

## Connectors

### Source Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | rdkafka consumer groups, Schema Registry | Implemented |
| PostgreSQL CDC | `postgres-cdc` | pgoutput logical replication | Implemented |
| MySQL CDC | `mysql-cdc` | Binlog decoding, GTID position tracking | Implemented |
| MongoDB CDC | `mongodb-cdc` | Change streams, resume token tracking, `$changeStreamSplitLargeEvent` support | Implemented |
| OpenTelemetry (OTLP/gRPC) | `otel` | OTLP/gRPC receiver (traces, metrics, logs) via tonic | Implemented |
| WebSocket Client | `websocket` | tokio-tungstenite | Implemented |
| WebSocket Server | `websocket` | tokio-tungstenite listener | Implemented |
| Delta Lake Source | `delta-lake` | Version polling via deltalake crate | Implemented |
| Iceberg Source | `iceberg` | REST/Glue/Hive catalog, incremental reads | Implemented |
| File Auto-Loader | `files` | Directory watch, glob pattern discovery, Parquet/CSV/JSON | Implemented |
| Parquet Lookup | `parquet-lookup` | Static / slowly-changing dimension table source | Implemented |

### On-demand lookup sources (partial cache mode)

`CREATE LOOKUP TABLE ... WITH (cache_mode = 'partial' | 'none')` caches the hot
working set in a byte-bounded RAM cache and fetches cache misses on demand —
the only model that addresses dimension tables larger than memory. Each backend
batches all missed keys of a probe into one pushed-down, key-filtered fetch
(`pk IN (...)` / `= ANY($1)` / `$in`) and realigns results to the input order.

| Backend | Feature Flag | Miss fetch | Notes |
|---------|-------------|-----------|-------|
| Delta Lake | `delta-lake` | `WHERE pk IN (...)` (file/partition pruning) | Warns if not clustered on the key |
| Iceberg | `iceberg` | Native scan `with_filter(pk IN ...)` (manifest pruning) | Reloads snapshot per fetch |
| PostgreSQL | `postgres-cdc` | Pooled (`deadpool`) `WHERE pk = ANY($1)` | Single-column key; server-auth TLS via `sslmode` + optional `sslrootcert` |
| MongoDB | `mongodb-cdc` | `find({ pk: { $in: [...] } })` | Projects documents into the declared schema |

Misses run off the compute thread (the lookup-enrich operator is async-decoupled),
results are byte-bounded with optional TTL (`cache.ttl`), and a source error
backpressures rather than dropping rows.

### Sink Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | rdkafka transactions | Implemented |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, co-transactional | Implemented |
| MongoDB | `mongodb-cdc` | Ordered/unordered writes, upsert, CDC replay | Implemented |
| Delta Lake | `delta-lake` | Epoch-aligned Parquet commits, recovery, compaction, schema evolution | Implemented |
| Iceberg | `iceberg` | REST/Glue/Hive catalog commits | Implemented |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | Implemented |
| WebSocket Client | `websocket` | Push to external server | Implemented |
| Files | `files` | CSV, JSON, Parquet, rolling file output | Implemented |

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
| `postgres` | PostgreSQL sink (COPY BINARY, upsert, exactly-once) |
| `cdc/postgres` | PostgreSQL CDC source (pgoutput decoder, Z-set changelog, replication I/O) |
| `cdc/mysql` | MySQL CDC source (binlog decoder, GTID, Z-set changelog) |
| `mongodb` | MongoDB CDC source (change streams, resume tokens) and sink (ordered/unordered writes) |
| `otel` | OpenTelemetry OTLP/gRPC receiver for traces, metrics, and logs (tonic server) |
| `websocket` | WebSocket source/sink (client, server, fan-out, backpressure, reconnect) |
| `lakehouse` | Delta Lake source and sink (buffering, epoch, changelog, recovery, compaction, schema evolution) and Apache Iceberg source and sink (REST/Glue/Hive catalogs) |
| `files` | File source (auto-loader, glob, watch) and sink (rolling, CSV/JSON/Parquet) |
| `lookup` | Lookup table support: PostgreSQL and Parquet reference tables |
| `reference` | Reference table source trait and refresh modes |
| `storage` | Cloud storage: provider detection, credential resolver, config validation, secret masking |
| `serde` | Format implementations: JSON, CSV, raw, Debezium, Avro |
| `schema` | Schema framework: inference, resolution, evolution, decoders (JSON/CSV/Avro/Parquet) |
| `testing` | Mock connectors for unit testing |

## Schema Framework

| Sub-module | Description |
|------------|-------------|
| `schema::traits` | `SchemaProvider`, `SchemaInferable`, `SchemaRegistryAware`, `SchemaEvolvable` traits |
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
| `postgres-cdc` | PostgreSQL CDC via pgwire-replication (also enables Postgres lookup source) |
| `postgres-sink` | PostgreSQL sink via tokio-postgres |
| `mysql-cdc` | MySQL CDC via mysql_async (rustls, no OpenSSL) |
| `mongodb-cdc` | MongoDB CDC source and sink via mongodb crate |
| `changelog-collapse` | Sink-agnostic Z-set/CDC changelog collapse for upsert sinks (pulled in by `delta-lake`) |
| `delta-lake` | Delta Lake sink/source via deltalake crate |
| `delta-lake-s3` | S3 storage backend for Delta Lake |
| `delta-lake-azure` | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | GCS storage backend for Delta Lake |
| `delta-lake-unity` | Databricks Unity catalog for Delta Lake |
| `delta-lake-glue` | AWS Glue catalog for Delta Lake |
| `iceberg` | Apache Iceberg source and sink (REST/Glue/Hive catalogs) |
| `otel` | OpenTelemetry OTLP/gRPC source (traces, metrics, logs) |
| `parquet-lookup` | Parquet schema reader (used by `files` and reference tables) |
| `websocket` | WebSocket source and sink (tokio-tungstenite) |
| `files` | File source (auto-loader) and sink (rolling files) |

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

- [`laminar-core`](../laminar-core) -- Streaming channels and sink abstractions
- [`laminar-storage`](../laminar-storage) -- Checkpoint manifest types
- [`laminar-db`](../laminar-db) -- Connector manager and checkpoint coordinator
