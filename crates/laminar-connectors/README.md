# laminar-connectors

External system connectors for LaminarDB -- Kafka, CDC, lookup tables, and lakehouse sinks.

## Overview

Provides source and sink connectors for integrating LaminarDB with external systems. Each connector implements the `SourceConnector` or `SinkConnector` trait and supports exactly-once semantics via two-phase commit.

## Key Modules

| Module | Purpose |
|--------|---------|
| `kafka` | Kafka source/sink, Avro serde, schema registry, partitioner, backpressure |
| `postgres` | PostgreSQL sink (COPY BINARY, upsert, exactly-once) |
| `cdc/postgres` | PostgreSQL CDC source (pgoutput decoder, Z-set changelog, replication I/O) |
| `cdc/mysql` | MySQL CDC source (binlog decoder, GTID, Z-set changelog) |
| `lakehouse` | Delta Lake and Iceberg sinks (buffering, epoch, changelog) |
| `storage` | Cloud storage: provider detection, credential resolver, config validation, secret masking |
| `bridge` | DAG-to-connector bridge (source/sink bridges, runtime orchestration) |
| `sdk` | Connector SDK: retry, rate limiting, circuit breaker, test harness, schema discovery |
| `serde` | Format implementations: JSON, CSV, raw, Debezium, Avro |
| `registry` | `ConnectorRegistry` for registering and looking up connectors |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `kafka` | rdkafka, Avro serde, schema registry |
| `postgres-cdc` | PostgreSQL CDC via pgwire-replication |
| `postgres-sink` | PostgreSQL sink via tokio-postgres |
| `mysql-cdc` | MySQL CDC via mysql_async |
| `delta-lake` | Delta Lake sink via deltalake crate |
| `delta-lake-s3` | S3 storage backend for Delta Lake |
| `delta-lake-azure` | Azure storage backend for Delta Lake |
| `delta-lake-gcs` | GCS storage backend for Delta Lake |

## Related Crates

- [`laminar-core`](../laminar-core) -- Streaming channels and sink abstractions
- [`laminar-storage`](../laminar-storage) -- Checkpoint manifest types
- [`laminar-db`](../laminar-db) -- Connector manager and checkpoint coordinator
