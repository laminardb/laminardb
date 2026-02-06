# FFI Discovery Report

> Generated: 2026-02-06

## Overview

This document captures the analysis of LaminarDB's codebase for FFI/language binding support.

## Project Structure

```
crates/
├── laminar-core/       # Ring 0: Hot path reactor, operators, state
├── laminar-sql/        # SQL parser, planner, DataFusion integration
├── laminar-storage/    # WAL, checkpoints, incremental snapshots
├── laminar-connectors/ # External connectors (Kafka, PostgreSQL, Delta Lake)
├── laminar-auth/       # Authentication & authorization
├── laminar-admin/      # Admin API & dashboard
├── laminar-observe/    # Observability, metrics, tracing
├── laminar-server/     # Standalone server binary
├── laminar-derive/     # Procedural macros (#[derive(Record)], etc.)
└── laminar-db/         # Main facade (single entry point) ← PRIMARY FFI TARGET
```

## Main Database Facade: `laminar-db`

### Public Exports (lib.rs)

```rust
pub use builder::LaminarDbBuilder;
pub use catalog::{SourceCatalog, SourceEntry};
pub use config::LaminarConfig;
pub use db::LaminarDB;
pub use error::DbError;
pub use handle::{
    DdlInfo, ExecuteResult, FromBatch, PipelineEdge, PipelineNode, PipelineNodeType,
    PipelineTopology, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    StreamInfo, TypedSubscription, UntypedSourceHandle,
};
pub use laminar_connectors::registry::ConnectorRegistry;
```

### Core Types

#### `LaminarDB` (main entry point)

**File**: `crates/laminar-db/src/db.rs`

**Internal Structure**:
```rust
pub struct LaminarDB {
    catalog: Arc<SourceCatalog>,
    planner: parking_lot::Mutex<StreamingPlanner>,
    ctx: SessionContext,
    config: LaminarConfig,
    config_vars: Arc<HashMap<String, String>>,
    shutdown: std::sync::atomic::AtomicBool,
    checkpoint_manager: parking_lot::Mutex<StreamCheckpointManager>,
    connector_manager: parking_lot::Mutex<ConnectorManager>,
    connector_registry: Arc<ConnectorRegistry>,
    mv_registry: parking_lot::Mutex<MvRegistry>,
    state: std::sync::atomic::AtomicU8,
    runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_signal: Arc<tokio::sync::Notify>,
}
```

**Public Methods**:
| Method | Signature | Notes |
|--------|-----------|-------|
| `open()` | `fn open() -> Result<Self, DbError>` | In-memory default |
| `open_with_config()` | `fn open_with_config(config: LaminarConfig) -> Result<Self, DbError>` | Custom config |
| `builder()` | `fn builder() -> LaminarDbBuilder` | Fluent builder |
| `execute()` | `async fn execute(&self, sql: &str) -> Result<ExecuteResult, DbError>` | SQL execution |
| `source<T>()` | `fn source<T: Record>(&self, name: &str) -> Result<SourceHandle<T>, DbError>` | Typed source |
| `source_untyped()` | `fn source_untyped(&self, name: &str) -> Result<UntypedSourceHandle, DbError>` | Arrow source |
| `subscribe<T>()` | `fn subscribe<T: FromBatch>(&self, name: &str) -> Result<TypedSubscription<T>, DbError>` | Stream subscription |
| `sources()` | `fn sources(&self) -> Vec<SourceInfo>` | List sources |
| `sinks()` | `fn sinks(&self) -> Vec<SinkInfo>` | List sinks |
| `streams()` | `fn streams(&self) -> Vec<StreamInfo>` | List streams |
| `queries()` | `fn queries(&self) -> Vec<QueryInfo>` | List queries |
| `pipeline_topology()` | `fn pipeline_topology(&self) -> PipelineTopology` | DAG topology |
| `start()` | `async fn start(&self) -> Result<(), DbError>` | Start pipeline |
| `checkpoint()` | `fn checkpoint(&self) -> Result<Option<u64>, DbError>` | Trigger checkpoint |
| `restore_checkpoint()` | `fn restore_checkpoint(&self) -> Result<StreamCheckpoint, DbError>` | Get checkpoint |
| `is_checkpoint_enabled()` | `fn is_checkpoint_enabled(&self) -> bool` | Check enabled |
| `close()` | `fn close(&self)` | Shutdown (no Result) |
| `is_closed()` | `fn is_closed(&self) -> bool` | Check shutdown |
| `connector_registry()` | `fn connector_registry(&self) -> &ConnectorRegistry` | Get registry |

### Handle Types

#### `ExecuteResult`

```rust
pub enum ExecuteResult {
    Ddl(DdlInfo),           // CREATE, DROP, ALTER
    Query(QueryHandle),     // SELECT (streaming)
    RowsAffected(u64),      // INSERT INTO
    Metadata(RecordBatch),  // SHOW, DESCRIBE
}
```

#### `QueryHandle`

```rust
pub struct QueryHandle {
    id: u64,
    schema: SchemaRef,
    sql: String,
    subscription: Option<Subscription<ArrowRecord>>,
    active: bool,
}

// Methods:
pub fn schema(&self) -> &SchemaRef
pub fn sql(&self) -> &str
pub fn id(&self) -> u64
pub fn is_active(&self) -> bool
pub fn subscribe<T: FromBatch>(&mut self) -> Result<TypedSubscription<T>, DbError>
pub fn cancel(&mut self)
```

#### `SourceHandle<T>` (typed)

```rust
pub struct SourceHandle<T: Record> {
    entry: Arc<SourceEntry>,
    _phantom: PhantomData<T>,
}

// Methods:
pub fn push(&self, record: T) -> Result<(), StreamingError>
pub fn push_batch(&self, records: impl IntoIterator<Item = T>) -> usize
pub fn push_arrow(&self, batch: RecordBatch) -> Result<(), StreamingError>
pub fn watermark(&self, timestamp: i64)
pub fn current_watermark(&self) -> i64
pub fn pending(&self) -> usize
pub fn name(&self) -> &str
pub fn schema(&self) -> &SchemaRef
```

#### `UntypedSourceHandle` (Arrow-only)

```rust
pub struct UntypedSourceHandle {
    entry: Arc<SourceEntry>,
}

// Methods:
pub fn push_arrow(&self, batch: RecordBatch) -> Result<(), StreamingError>
pub fn watermark(&self, timestamp: i64)
pub fn current_watermark(&self) -> i64
pub fn name(&self) -> &str
pub fn schema(&self) -> &SchemaRef
```

#### `TypedSubscription<T>`

```rust
pub struct TypedSubscription<T: FromBatch> {
    inner: Subscription<ArrowRecord>,
    _phantom: PhantomData<T>,
}

// Methods:
pub fn poll(&self) -> Option<Vec<T>>
pub fn recv(&self) -> Result<Vec<T>, RecvError>
pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<T>, RecvError>
pub fn poll_each<F: FnMut(T) -> bool>(&self, max_batches: usize, f: F) -> usize
```

### Error Types

#### `DbError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    Sql(laminar_sql::Error),
    Engine(laminar_core::Error),
    Streaming(StreamingError),
    DataFusion(DataFusionError),
    SourceNotFound(String),
    SinkNotFound(String),
    QueryNotFound(String),
    SourceAlreadyExists(String),
    SinkAlreadyExists(String),
    StreamNotFound(String),
    StreamAlreadyExists(String),
    TableNotFound(String),
    TableAlreadyExists(String),
    InsertError(String),
    SchemaMismatch(String),
    InvalidOperation(String),
    SqlParse(ParseError),
    Shutdown,
    Checkpoint(String),
    UnresolvedConfigVar(String),
    Connector(String),
    Pipeline(String),
    MaterializedView(String),
}
```

## Arrow Integration

### Dependencies

```toml
# From workspace Cargo.toml
arrow = "54"
arrow-array = "54"
arrow-schema = "54"
arrow-csv = "54"
arrow-json = "54"
datafusion = "52"
```

### Key Types Used

- `RecordBatch` - Primary data container
- `Schema` / `SchemaRef` - Column definitions
- `Field` / `DataType` - Column metadata
- Arrow arrays: `Int64Array`, `StringArray`, `Float64Array`, etc.

### Usage Patterns

1. **Source push**: `source.push_arrow(batch: RecordBatch)`
2. **Query results**: `ExecuteResult::Query(QueryHandle)` → `subscribe()` → `RecordBatch`
3. **Schema access**: `schema: SchemaRef` on handles
4. **Metadata**: `ExecuteResult::Metadata(RecordBatch)`

## Async Runtime

### Tokio Usage

```toml
tokio = { version = "1.43", features = ["full"] }
```

- **Ring 0**: Sync processing (no tokio)
- **Ring 1**: Background I/O (`tokio::spawn`)
- **Public API**: `async fn execute()`, `async fn start()`

### Sync-Compatible Methods

Already sync (no async):
- `open()`, `open_with_config()`
- `source()`, `source_untyped()`
- `subscribe()`
- `sources()`, `sinks()`, `streams()`, `queries()`
- `checkpoint()`, `restore_checkpoint()`
- `close()`, `is_closed()`

## Thread Safety

### Concurrency Primitives

| Primitive | Location | Usage |
|-----------|----------|-------|
| `Arc` | LaminarDB fields | Shared ownership |
| `parking_lot::Mutex` | Planner, checkpoint | Mutable state |
| `parking_lot::RwLock` | SourceCatalog | Multiple readers |
| `AtomicBool` | Shutdown flag | Lock-free |
| `AtomicU8` | State machine | Lifecycle |
| `AtomicU64` | Query ID counter | ID generation |

### Send + Sync Status

| Type | Send | Sync | Notes |
|------|------|------|-------|
| `LaminarDB` | Implicitly | Implicitly | All fields are thread-safe |
| `SourceHandle<T>` | Yes (T: Send) | Requires check | Arc<SourceEntry> |
| `UntypedSourceHandle` | Yes | Requires check | Arc<SourceEntry> |
| `TypedSubscription<T>` | Yes | Requires check | Inner Subscription |
| `QueryHandle` | Requires check | Requires check | Contains Option |

## Existing FFI Code

**None found**. No `#[no_mangle]`, `extern "C"`, or C FFI patterns exist in the codebase.

## Derive Macros

### `#[derive(Record)]` (laminar-derive)

Generates `Record` trait for Rust structs:
```rust
#[derive(Record)]
struct Trade {
    symbol: String,
    price: f64,
    #[event_time]
    timestamp: i64,
}
```

### `#[derive(FromRecordBatch)]` / `#[derive(FromRow)]`

Generates `FromBatch` trait for deserialization:
```rust
#[derive(FromRecordBatch)]
struct OhlcBar {
    symbol: String,
    open: f64,
}
```

## Key Observations

1. **Well-structured facade**: `LaminarDB` is the single entry point
2. **Arrow-native**: RecordBatch used throughout
3. **Partial thread safety**: Most internal types are protected, public handles need verification
4. **Async/sync split**: Core ops are sync, execution is async
5. **No FFI layer**: No existing C bindings to build on
6. **Typed vs Untyped**: Both patterns exist (SourceHandle<T> vs UntypedSourceHandle)
7. **Error handling**: Rich error enum but no numeric codes
