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
- **`CheckpointCoordinator`** -- Orchestrates two-phase commit checkpoints across all operators and sinks.
- **`RecoveryManager`** -- Restores operator state, connector offsets, and watermarks from the latest checkpoint.
- **`Profile`** -- Deployment profile (`BareMetal`, `Embedded`, `Durable`, `Delta`).
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
| `postgres-cdc` | PostgreSQL CDC source (also enables Postgres lookup) |
| `postgres-sink` | PostgreSQL sink |
| `mysql-cdc` | MySQL CDC source via binlog |
| `mongodb-cdc` | MongoDB CDC source and sink |
| `delta-lake` | Delta Lake sink and source |
| `delta-lake-s3` / `delta-lake-azure` / `delta-lake-gcs` | Cloud storage backends for Delta Lake |
| `delta-lake-unity` / `delta-lake-glue` | Databricks Unity / AWS Glue catalogs for Delta Lake |
| `delta-lake-all` | All Delta Lake storage backends and catalogs |
| `iceberg` | Apache Iceberg source and sink |
| `websocket` | WebSocket source and sink connectors |
| `files` | File source (AutoLoader) and sink (rolling files) |
| `parquet-lookup` | Parquet lookup source for reference tables |
| `otel` | OpenTelemetry OTLP/gRPC source |
| `cluster` | Full distributed mode scaffolding (gRPC, gossip, Raft). Not production-ready. |
| `aws` / `gcs` / `azure` | Object-store checkpoint backends (forwards to laminar-core) |

## Internal Architecture

### Core SQL Operator Routing

The `core_window_state` module routes tumbling, hopping, and session window aggregates through optimized `CoreWindowAssigner` state instead of the generic DataFusion path. Detection is lazy on the first EOWC (Emit On Window Close) cycle, and non-qualifying queries fall through to `IncrementalEowcState` or raw-batch processing.

### EOWC Incremental Accumulators

The `eowc_state` module provides incremental per-window accumulators that maintain running aggregation state. This avoids re-scanning all records when a window closes, instead using the pre-computed accumulator values.

## Related Crates

- [`laminar-core`](../laminar-core) -- Operators, streaming channels, window assigners, checkpoint barriers, storage
- [`laminar-sql`](../laminar-sql) -- SQL parser and DataFusion integration
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-derive`](../laminar-derive) -- Derive macros for typed data handling

