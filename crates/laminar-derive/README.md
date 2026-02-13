# laminar-derive

Derive macros for LaminarDB `Record` and `FromRecordBatch` traits.

## Overview

Provides procedural macros that automatically generate Arrow RecordBatch conversion code for Rust structs, enabling zero-boilerplate typed data ingestion and result consumption.

## Usage

```rust
use laminar_derive::FromRecordBatch;

#[derive(FromRecordBatch)]
struct Trade {
    symbol: String,
    price: f64,
    volume: i64,
    ts: i64,
}
```

The `FromRecordBatch` derive generates `from_batch()` and `from_batch_all()` methods that convert Arrow RecordBatches into typed Rust structs. Used with `LaminarDB::subscribe::<Trade>("stream_name")` for typed result consumption.

## Related Crates

- [`laminar-core`](../laminar-core) -- `Record` trait definition
- [`laminar-db`](../laminar-db) -- `TypedSubscription<T>` and `SourceHandle<T>` that use derived traits
