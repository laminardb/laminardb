# laminar-derive

Derive macros for LaminarDB -- `Record`, `FromRecordBatch`, `FromRow`, and `ConnectorConfig`.

## Overview

Provides procedural macros that automatically generate Arrow RecordBatch conversion code and connector configuration parsing, eliminating boilerplate in typed data ingestion, result consumption, and connector development.

## Macros

### `#[derive(Record)]`

Generates `Record::schema()`, `Record::to_record_batch()`, and `Record::event_time()` for typed data ingestion.

```rust
use laminar_derive::Record;

#[derive(Record)]
struct Trade {
    symbol: String,
    price: f64,
    volume: i64,
    #[event_time]
    ts: i64,
}
```

**Attributes:**
- `#[event_time]` -- marks a field as the event time column
- `#[column("name")]` -- overrides the Arrow column name
- `#[nullable]` -- marks a non-Option field as nullable in the schema

**Supported types:** `bool`, `i8`-`i64`, `u8`-`u64`, `f32`, `f64`, `String`, `Vec<u8>`, `Option<T>`

### `#[derive(FromRecordBatch)]`

Generates `from_batch()` and `from_batch_all()` for deserializing Arrow RecordBatches into typed structs. Fields are matched by name.

```rust
use laminar_derive::FromRecordBatch;

#[derive(FromRecordBatch)]
struct OhlcBar {
    symbol: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}
```

### `#[derive(FromRow)]`

Like `FromRecordBatch`, but also implements `laminar_db::FromBatch`. Fields are matched by **position** (column order in SELECT), not by name.

```rust
use laminar_derive::FromRow;

#[derive(FromRow)]
struct Result {
    symbol: String,    // 1st SELECT column
    total: i64,        // 2nd SELECT column
    avg_price: f64,    // 3rd SELECT column
}
```

### `#[derive(ConnectorConfig)]`

Generates `from_config()`, `validate()`, and `config_keys()` for connector configuration parsing.

```rust
use laminar_derive::ConnectorConfig;

#[derive(ConnectorConfig)]
struct MySourceConfig {
    #[config(key = "bootstrap.servers", required, description = "Kafka brokers")]
    bootstrap_servers: String,

    #[config(key = "batch.size", default = "1000")]
    batch_size: usize,

    #[config(key = "timeout.ms", default = "30000", duration_ms)]
    timeout: std::time::Duration,

    #[config(key = "api.key", env = "MY_API_KEY")]
    api_key: Option<String>,
}
```

**Attributes:**
- `#[config(key = "...")]` -- config key name
- `#[config(required)]` -- field is required
- `#[config(default = "...")]` -- default value
- `#[config(env = "...")]` -- environment variable fallback
- `#[config(description = "...")]` -- documentation for the key
- `#[config(duration_ms)]` -- parse as `Duration` from milliseconds

## Related Crates

- [`laminar-core`](../laminar-core) -- `Record` trait definition
- [`laminar-db`](../laminar-db) -- `TypedSubscription<T>` and `SourceHandle<T>` that use derived traits
- [`laminar-connectors`](../laminar-connectors) -- Connector SDK that uses `ConnectorConfig`
