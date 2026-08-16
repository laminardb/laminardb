# laminar-db

Unified database facade for LaminarDB. The main entry point that wires the SQL parser, query planner, DataFusion context, streaming infrastructure, and connector registry.

## Key Types

- **`LaminarDB`** -- Main database handle. Manages sources, streams, sinks, and the streaming pipeline lifecycle.
- **`LaminarDbBuilder`** -- Fluent builder for constructing `LaminarDB` with custom configuration, connectors, UDFs, and deployment profiles.
- **`ExecuteResult`** -- Result of executing a SQL statement (DDL, query, rows affected, metadata).
- **`QueryHandle`** -- Handle to a running streaming query with schema and subscription access.
- **`SourceHandle<T>`** / **`UntypedSourceHandle`** -- Typed and untyped handles for pushing data into sources.
- **`TypedSubscription<T>`** -- Subscription to a named stream with automatic RecordBatch-to-struct conversion.
- **`SubscriptionRegistry`** / **`SubscriptionPortal`** -- Broadcast fan-out and per-consumer pump.
- **`CheckpointCoordinator`** -- Seals source/operator state, records the exact durable decision, and hands coordinated external publication to the designated committer.
- **`RecoveryManager`** -- Restores operator state, connector offsets, and watermarks from the latest checkpoint.
- **`Profile`** -- Deployment profile (`BareMetal`, `Embedded`, `Durable`, `Cluster`).
- **`PipelineMetrics`** / **`PipelineCounters`** -- Real-time pipeline observability.
- **`DbError`** -- Structured error type with stable `LDB-NNNN` codes.

## Architecture

This crate sits at the top of the dependency graph, integrating other LaminarDB crates:

```
laminar-db
  |-- laminar-core        (operators, streaming channels, checkpoint barriers, storage)
  |-- laminar-sql         (SQL parsing + DataFusion)
  |-- laminar-connectors  (external connectors)
```

## Feature Flags

| Flag | Purpose |
|------|---------|
| `api` | FFI-friendly API module with `Connection`, `Writer`, `QueryStream` |
| `ffi` | C FFI layer with `extern "C"` functions and Arrow C Data Interface (implies `api`) |
| `kafka` | Kafka source/sink connector |
| `postgres-cdc` | PostgreSQL CDC source (also builds the standalone `postgres` lookup connector) |
| `postgres-sink` | PostgreSQL sink |
| `mongodb-cdc` | MongoDB CDC source and sink |
| `delta-lake` | Delta Lake sink and source |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `delta-lake-all` | All Delta Lake storage backends and catalogs |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling files) |
| `parquet-lookup` | Parquet schema and codec helpers; no standalone connector |
| `otel` | OpenTelemetry OTLP/gRPC source |
| `cluster` | Distributed mode with gRPC control plane, vnode state, and gossip/static discovery; ALO plus capability-gated EO. |
| `aws` / `gcs` / `azure` | Object-store checkpoint backends (forwards to laminar-core) |

## Related Crates

- [`laminar-core`](../laminar-core) -- Operators, streaming channels, window assigners, checkpoint barriers, storage
- [`laminar-sql`](../laminar-sql) -- SQL parser and DataFusion integration
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-derive`](../laminar-derive) -- Derive macros for typed data handling

