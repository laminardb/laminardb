# laminar-connectors

External system connectors for LaminarDB -- Kafka, CDC, MongoDB, WebSocket, OpenTelemetry, Delta Lake, Iceberg, and files.

## Overview

Source and sink connectors for external systems. Each connector implements the `SourceConnector` or `SinkConnector` trait and supports exactly-once semantics via two-phase commit.

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
| Postgres Lookup | `postgres-cdc` | Pool-backed lookup source with predicate pushdown | Implemented |
| Parquet Lookup | `parquet-lookup` | Static / slowly-changing dimension table source | Implemented |

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
| `delta-lake` | Delta Lake sink/source via deltalake crate |
| `delta-lake-s3` | S3 storage backend for Delta Lake |
| `delta-lake-azure` | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | GCS storage backend for Delta Lake |
| `delta-lake-unity` | Databricks Unity catalog for Delta Lake |
| `delta-lake-glue` | AWS Glue catalog for Delta Lake |
| `iceberg` | Apache Iceberg source and sink (REST/Glue/Hive catalogs) |
| `otel` | OpenTelemetry OTLP/gRPC source (traces, metrics, logs) |
| `parquet-lookup` | Parquet file lookup source |
| `websocket` | WebSocket source and sink (tokio-tungstenite) |
| `files` | File source (auto-loader) and sink (rolling files) |
| `kafka-discovery` | Kafka-based discovery for delta mode |

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
