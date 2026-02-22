# laminar-connectors

External system connectors for LaminarDB -- Kafka, CDC, WebSocket, lakehouse sinks, and the Connector SDK.

## Overview

Provides source and sink connectors for integrating LaminarDB with external systems. Each connector implements the `SourceConnector` or `SinkConnector` trait and supports exactly-once semantics via two-phase commit.

## Connectors

### Source Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | rdkafka consumer groups, Schema Registry | Implemented |
| PostgreSQL CDC | `postgres-cdc` | pgoutput logical replication | Implemented |
| MySQL CDC | `mysql-cdc` | Binlog decoding, GTID position tracking | Implemented |
| WebSocket Client | `websocket` | tokio-tungstenite | Implemented |
| WebSocket Server | `websocket` | tokio-tungstenite listener | Implemented |
| Delta Lake Source | `delta-lake` | Version polling via deltalake crate | Implemented |

### Sink Connectors

| Connector | Feature Flag | Protocol | Status |
|-----------|-------------|----------|--------|
| Kafka | `kafka` | rdkafka transactions | Implemented |
| PostgreSQL | `postgres-sink` | COPY BINARY, upsert, co-transactional | Implemented |
| Delta Lake | `delta-lake` | Epoch-aligned Parquet commits | Implemented |
| Apache Iceberg | -- | Buffering, partition transforms | Implemented (business logic) |
| WebSocket Server | `websocket` | Fan-out to connected subscribers | Implemented |
| WebSocket Client | `websocket` | Push to external server | Implemented |

## Key Modules

| Module | Purpose |
|--------|---------|
| `connector` | Core traits: `SourceConnector`, `SinkConnector`, `SourceBatch`, `WriteResult` |
| `kafka` | Kafka source/sink, Avro serde, schema registry, partitioner, backpressure |
| `postgres` | PostgreSQL sink (COPY BINARY, upsert, exactly-once) |
| `cdc/postgres` | PostgreSQL CDC source (pgoutput decoder, Z-set changelog, replication I/O) |
| `cdc/mysql` | MySQL CDC source (binlog decoder, GTID, Z-set changelog) |
| `websocket` | WebSocket source/sink (client, server, fan-out, backpressure, reconnect) |
| `lakehouse` | Delta Lake and Iceberg sinks (buffering, epoch, changelog) |
| `storage` | Cloud storage: provider detection, credential resolver, config validation, secret masking |
| `bridge` | DAG-to-connector bridge (source/sink bridges, runtime orchestration) |
| `sdk` | Connector SDK: retry policies, rate limiting, circuit breaker, test harness |
| `serde` | Format implementations: JSON, CSV, raw, Debezium, Avro |
| `schema` | Schema framework: inference, resolution, evolution, decoders (JSON/CSV/Avro/Parquet), DLQ |
| `registry` | `ConnectorRegistry` for registering and looking up connectors by name |
| `lookup` | Lookup table support for enrichment joins |
| `reference` | Reference table source trait and refresh modes |
| `error_handling` | Dead letter queue and error routing for malformed records |

## Schema Framework

The schema module provides a pluggable framework for format decoding and schema management:

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
| `postgres-cdc` | PostgreSQL CDC via pgwire-replication |
| `postgres-sink` | PostgreSQL sink via tokio-postgres |
| `mysql-cdc` | MySQL CDC via mysql_async (rustls, no OpenSSL) |
| `delta-lake` | Delta Lake sink/source via deltalake crate |
| `delta-lake-s3` | S3 storage backend for Delta Lake |
| `delta-lake-azure` | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | GCS storage backend for Delta Lake |
| `delta-lake-unity` | Databricks Unity catalog for Delta Lake |
| `delta-lake-glue` | AWS Glue catalog for Delta Lake |
| `parquet-lookup` | Parquet file lookup source |
| `websocket` | WebSocket source and sink (tokio-tungstenite) |
| `kafka-discovery` | Kafka-based discovery for delta mode |

## Connector SDK

Build custom connectors with operational resilience:

```rust
use laminar_connectors::connector::{SourceConnector, SinkConnector};
use laminar_connectors::sdk::{RetryPolicy, CircuitBreaker, RateLimiter};
use laminar_connectors::registry::ConnectorRegistry;

// Register a custom source
let registry = ConnectorRegistry::new();
registry.register_source("my-source", info, factory_fn);
```

The SDK provides:
- **RetryPolicy** -- configurable retry with exponential backoff
- **CircuitBreaker** -- fail-fast when downstream is unavailable
- **RateLimiter** -- rate-limit source polling or sink writes
- **Test Harness** -- mock connectors for unit testing

## Related Crates

- [`laminar-core`](../laminar-core) -- Streaming channels and sink abstractions
- [`laminar-storage`](../laminar-storage) -- Checkpoint manifest types
- [`laminar-db`](../laminar-db) -- Connector manager and checkpoint coordinator
