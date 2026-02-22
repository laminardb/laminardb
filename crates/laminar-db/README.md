# laminar-db

Unified database facade for LaminarDB.

## Overview

This crate provides `LaminarDB`, the primary user-facing type that ties together the SQL parser, query planner, DataFusion context, streaming infrastructure, and connector registry into a single interface. It is the entry point for all LaminarDB applications.

## Key Types

- **`LaminarDB`** -- Main database handle. Manages sources, streams, sinks, and the streaming pipeline lifecycle.
- **`LaminarDbBuilder`** -- Fluent builder for constructing `LaminarDB` with custom configuration, connectors, UDFs, and deployment profiles.
- **`ExecuteResult`** -- Result of executing a SQL statement (DDL, query, rows affected, metadata).
- **`QueryHandle`** -- Handle to a running streaming query with schema and subscription access.
- **`SourceHandle<T>`** / **`UntypedSourceHandle`** -- Typed and untyped handles for pushing data into sources.
- **`TypedSubscription<T>`** -- Subscription to a named stream with automatic RecordBatch-to-struct conversion.
- **`CheckpointCoordinator`** -- Orchestrates two-phase commit checkpoints across all operators and sinks.
- **`RecoveryManager`** -- Restores operator state, connector offsets, and watermarks from the latest checkpoint.
- **`Profile`** -- Deployment profile (InMemory, Durable, Delta).
- **`PipelineMetrics`** / **`PipelineCounters`** -- Real-time pipeline observability.

## Usage

```rust
use laminar_db::LaminarDB;

let db = LaminarDB::open()?;

// Create a source
db.execute("CREATE SOURCE trades (
    symbol VARCHAR, price DOUBLE, ts BIGINT
)").await?;

// Create a continuous query
db.execute("CREATE STREAM avg_price AS
    SELECT symbol, AVG(price) AS avg
    FROM trades
    GROUP BY symbol, tumble(ts, INTERVAL '1' MINUTE)
").await?;

// Push data
let source = db.source_untyped("trades")?;

// Start the pipeline
db.start().await?;

// Shutdown
db.shutdown().await?;
```

### Using the Builder

```rust
let db = LaminarDB::builder()
    .config_var("KAFKA_BROKERS", "localhost:9092")
    .buffer_size(131_072)
    .storage_dir("./data")
    .profile(Profile::Durable)
    .register_udf(my_scalar_udf)
    .register_udaf(my_aggregate_udf)
    .register_connector(|registry| {
        registry.register_source("my-source", info, factory);
    })
    .build()
    .await?;
```

## Architecture

This crate sits at the top of the dependency graph, integrating all other LaminarDB crates:

```
laminar-db
  |-- laminar-core        (Ring 0 engine)
  |-- laminar-sql         (SQL parsing + DataFusion)
  |-- laminar-storage     (WAL + checkpointing)
  |-- laminar-connectors  (external connectors)
```

## Feature Flags

| Flag | Purpose |
|------|---------|
| `api` | FFI-friendly API module with `Connection`, `Writer`, `QueryStream` |
| `ffi` | C FFI layer with `extern "C"` functions and Arrow C Data Interface |
| `jit` | Cranelift JIT compilation (forwards to laminar-core) |
| `kafka` | Kafka source/sink connector |
| `postgres-cdc` | PostgreSQL CDC source |
| `postgres-sink` | PostgreSQL sink |
| `delta-lake` | Delta Lake sink and source |
| `durable` | Object-store checkpoint profiles |
| `delta` | Full distributed mode (Durable + gRPC + gossip + Raft) |

## Related Crates

- [`laminar-core`](../laminar-core) -- Ring 0 engine (operators, state, streaming)
- [`laminar-sql`](../laminar-sql) -- SQL parser and DataFusion integration
- [`laminar-storage`](../laminar-storage) -- WAL and checkpointing
- [`laminar-connectors`](../laminar-connectors) -- External system connectors
- [`laminar-derive`](../laminar-derive) -- Derive macros for typed data handling
