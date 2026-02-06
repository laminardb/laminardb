# FFI Gap Analysis

> Generated: 2026-02-06

This document compares the required API contract for language bindings against what currently exists in LaminarDB.

## Legend

- **Exists?**: `Y` = Yes, `N` = No, `P` = Partial
- **Changes Needed**: Description of work required

---

## Connection Management

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| `open(path)` | P | `LaminarDB::open()` | Add path parameter (currently in-memory only) |
| `open_with_options(path, options)` | P | `LaminarDB::open_with_config()` | Add path to `LaminarConfig` |
| `close()` → `Result<(), ApiError>` | P | `LaminarDB::close()` | Returns `()`, needs `Result` and proper cleanup |
| `Send + Sync` | P | Implicit | Need explicit `unsafe impl` for FFI guarantee |

### Details

**Current `close()` implementation**:
```rust
pub fn close(&self) {
    self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
}
```

**Required for FFI**:
```rust
pub fn close(self) -> Result<(), ApiError> {
    // Proper cleanup, wait for background tasks, return errors
}
```

---

## Schema Management

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| `create_table(name, schema)` | N | - | Must use SQL `CREATE SOURCE/TABLE` |
| `get_schema(table)` | P | Via `sources()` iteration | Add direct `get_schema(name)` method |
| `list_tables()` | P | `sources()` | Rename/alias for FFI clarity |
| `drop_table(name)` | N | - | Must use SQL `DROP SOURCE` |

### Details

**Current pattern** (SQL-based):
```rust
db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)").await?;
```

**Required for FFI** (programmatic):
```rust
let schema = Schema::new(vec![
    Field::new("symbol", DataType::Utf8, false),
    Field::new("price", DataType::Float64, false),
]);
db.create_source("trades", &schema)?;
```

**Recommendation**: Add thin wrappers that generate SQL internally, OR add programmatic variants.

---

## Data Ingestion

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| `insert(table, RecordBatch)` → `u64` | P | `source_untyped().push_arrow()` | Different return type; need row count |
| `writer(table)` → `Writer` | P | `source_untyped()` | UntypedSourceHandle is similar |
| `Writer::write(batch)` | Y | `push_arrow()` | Rename for FFI clarity |
| `Writer::flush()` | N | - | No explicit flush (auto-buffered) |
| `Writer::close()` → `Result` | N | - | No close method on handles |
| `Writer::schema()` | Y | `schema()` | Exists |

### Details

**Current pattern**:
```rust
let handle = db.source_untyped("trades")?;
handle.push_arrow(batch)?;  // Returns (), not row count
// No explicit close - relies on Drop
```

**Required for FFI**:
```rust
let writer = db.writer("trades")?;
writer.write(batch)?;
writer.flush()?;
writer.close()?;  // Explicit cleanup
```

**Recommendation**: Create `Writer` wrapper around `UntypedSourceHandle` with explicit lifecycle.

---

## Query Execution

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| `query(sql)` → `QueryResult` | Y | `execute()` | Returns `ExecuteResult`, need unwrapping |
| `query_stream(sql)` → `QueryStream` | Y | Via `QueryHandle::subscribe()` | Requires `FromBatch` trait |
| `QueryResult::schema()` | Y | `QueryHandle::schema()` | Exists |
| `QueryResult::batches()` | P | Must iterate subscription | Need materialized result variant |
| `QueryResult::num_rows()` | N | - | Need to compute from batches |
| `QueryStream::next()` | Y | `TypedSubscription::recv()` | Exists but typed |

### Details

**Current pattern** (typed, streaming only):
```rust
let result = db.execute("SELECT * FROM trades").await?;
let mut handle = result.into_query()?;
let sub = handle.subscribe::<Trade>()?;  // Requires FromBatch
for records in sub {
    // records: Vec<Trade>
}
```

**Required for FFI** (untyped, supports materialized):
```rust
let result = db.query("SELECT * FROM trades")?;
// Materialized:
let batches = result.batches();  // &[RecordBatch]
// Or streaming:
let stream = db.query_stream("SELECT * FROM trades")?;
while let Some(batch) = stream.next()? {
    // batch: RecordBatch
}
```

**Key Gap**: No untyped `subscribe_arrow()` method that returns raw `RecordBatch` instead of `Vec<T>`.

---

## Subscriptions

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| `subscribe(sql)` → `Subscription` | P | `db.subscribe<T>(name)` | By name only, requires FromBatch |
| `Subscription::schema()` | P | Via inner subscription | Need to expose |
| `Subscription::next()` (blocking) | Y | `recv()` | Exists but typed |
| `Subscription::try_next()` (non-blocking) | Y | `poll()` | Exists but typed |
| `Subscription::is_active()` | P | `QueryHandle::is_active()` | Not on subscription directly |
| `Subscription::cancel()` | P | `QueryHandle::cancel()` | Not on subscription directly |

### Details

**Current pattern** (typed):
```rust
let sub: TypedSubscription<Trade> = db.subscribe("ohlc_stream")?;
if let Some(records) = sub.poll() {
    // records: Vec<Trade>
}
```

**Required for FFI** (untyped Arrow):
```rust
let sub = db.subscribe_arrow("ohlc_stream")?;
if let Some(batch) = sub.poll() {
    // batch: RecordBatch
}
```

---

## Error Handling

| Required | Exists? | Location | Changes Needed |
|----------|---------|----------|----------------|
| Unified `ApiError` | P | `DbError` | Rename or wrap for FFI |
| `ApiError::code()` → `i32` | N | - | Add numeric error codes |
| `ApiError::message()` → `&str` | P | `Display` impl | Need dedicated method |
| Standard error constructors | N | - | Add `table_not_found()` etc. |

### Details

**Current DbError**:
```rust
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("Source '{0}' not found")]
    SourceNotFound(String),
    // ... many variants, no codes
}
```

**Required ApiError**:
```rust
pub enum ApiError {
    #[error("Schema error ({code}): {message}")]
    Schema { code: i32, message: String },
    // ...
}

impl ApiError {
    pub fn code(&self) -> i32 { /* ... */ }
    pub fn message(&self) -> &str { /* ... */ }

    // Constructors with standard codes:
    pub fn table_not_found(name: &str) -> Self {
        Self::Schema { code: 200, message: format!("Table not found: {name}") }
    }
}
```

---

## Summary: Required Changes

### Minimal Changes (API Module)

1. **`api/error.rs`**: New `ApiError` enum with numeric codes, `From<DbError>` impl
2. **`api/connection.rs`**: `Connection` wrapper with explicit `close()` returning `Result`
3. **`api/query.rs`**: Add `subscribe_arrow()` for untyped streaming
4. **`api/subscription.rs`**: Untyped `ArrowSubscription` that returns `RecordBatch`
5. **`api/schema.rs`**: Add `get_schema(name)` convenience method
6. **Thread safety**: Add explicit `unsafe impl Send for Connection {}` etc.

### Feature Flag

```toml
[features]
api = ["parking_lot"]  # FFI-friendly API surface
```

### New Module Structure

```
laminar-db/src/
├── api/
│   ├── mod.rs          # Public re-exports
│   ├── error.rs        # ApiError with codes
│   ├── connection.rs   # Connection wrapper
│   ├── schema.rs       # Schema operations
│   ├── ingestion.rs    # Writer type
│   ├── query.rs        # QueryResult, QueryStream
│   └── subscription.rs # ArrowSubscription
├── db.rs               # Existing LaminarDB (unchanged)
└── ...
```

---

## Priority Matrix

| Gap | Priority | Effort | Impact |
|-----|----------|--------|--------|
| Numeric error codes | P0 | 2 hours | FFI error handling |
| Untyped subscription | P0 | 4 hours | Python/Java can't use FromBatch |
| Explicit close() | P0 | 2 hours | Resource management |
| Send + Sync markers | P0 | 1 hour | Thread safety guarantee |
| get_schema(name) | P1 | 1 hour | Convenience |
| Writer wrapper | P1 | 2 hours | Explicit lifecycle |
| Programmatic create_table | P2 | 4 hours | Avoid SQL for simple cases |

---

## Compatibility Notes

### Arrow C Data Interface

LaminarDB already uses Arrow 54 which supports FFI. Language bindings can use:

- **Python**: `pyarrow.RecordBatch._import_from_c()`
- **Java**: Arrow Java C Data Interface
- **Node.js**: `apache-arrow` npm package
- **.NET**: `Apache.Arrow` NuGet package

### Async Considerations

- `execute()` is async → FFI layer must block or expose future
- Most other methods are sync → direct FFI possible
- Recommendation: Create sync wrapper `execute_blocking()` for FFI
