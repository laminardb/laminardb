# ADR-006: Extensible Schema Trait Architecture

## Status

Accepted

## Date

2026-02-21

## Context

LaminarDB's connector ecosystem has grown to include Kafka, WebSocket, Parquet, Iceberg, PostgreSQL CDC, MySQL CDC, and more. Each of these sources has fundamentally different schema capabilities:

- **Kafka** with Avro integrates with the Confluent Schema Registry, but Kafka with JSON has no schema at all.
- **Parquet** and **Iceberg** are self-describing: the schema is embedded in file metadata or catalog entries.
- **WebSocket** and **CSV** carry no schema information; the only option is to sample data and infer types.
- **Iceberg** supports full schema evolution (add/drop/rename columns, type widening), while **CSV** does not.

The existing F034 Connector SDK (implemented in `crates/laminar-connectors/src/connector.rs`) defines lifecycle traits (`SourceConnector`, `SinkConnector`) and a serialization framework (`RecordDeserializer`, `RecordSerializer`), but it has no standardized way for connectors to express their **schema capabilities**. The `discover_schema()` method on `SourceConnector` is a single optional method that returns `Option<SchemaRef>` -- it conflates self-describing sources, registry-backed sources, and inference-capable sources into one call with no way to distinguish them.

This creates several problems:

1. **New connectors must know engine internals.** There is no clear contract for how a third-party connector should expose schema inference, registry integration, or evolution support.
2. **The schema resolver has no way to compose strategies.** A Kafka source configured with JSON format should use sample-based inference, but the same source configured with Avro should use the schema registry. The current single-method design cannot express this.
3. **No extensibility path for new formats.** Adding a new format (e.g., MessagePack, CBOR, FlatBuffers) requires modifying existing code instead of registering a new implementation.
4. **Schema resolution happens ad hoc.** Some sources resolve schemas at creation time, others at first read. There is no guarantee that schema resolution stays off the Ring 0 hot path.

This ADR captures the binding contract for all future connector development. It supersedes the schema discovery portion of F034 and establishes the trait-based framework that all sources and sinks must implement.

## Decision

**All sources and sinks in LaminarDB MUST be implemented through the trait-based connector framework.** No hardcoded source/sink logic in the engine. Schema capabilities are expressed through five orthogonal capability traits, discovered at runtime through optional downcast methods on the top-level `SourceConnector` and `SinkConnector` traits.

---

### 1. The Five Capability Traits

The schema system is decomposed into five orthogonal traits that can be composed independently. A source only implements the traits relevant to its capabilities.

```
                          +----------------------------+
                          |    SchemaResolver           |  <-- Orchestrator
                          |  (picks strategy, merges)   |
                          +-------------+--------------+
                                        |
              +-------------------------+-------------------------+
              |                         |                         |
    +---------v--------+   +-----------v---------+  +-----------v-----------+
    | SchemaProvider    |   | SchemaInferable     |  | SchemaEvolvable       |
    | (self-describing) |   | (sample-based)      |  | (evolution support)   |
    +------------------+   +--------------------+  +----------------------+
              |                         |
    +---------v-----------+   +---------v--------+
    | SchemaRegistryAware |   | FormatDecoder    |
    | (external registry) |   | (bytes -> Arrow) |
    +--------------------+   +-----------------+
```

#### 1.1 `FormatDecoder` / `FormatEncoder` -- The Foundation

Every source needs a way to turn raw bytes into Arrow `RecordBatch`es, and every sink needs the reverse. This is the one mandatory capability.

```rust
/// Errors that can occur during schema or format operations.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Schema inference failed: {0}")]
    InferenceFailed(String),
    #[error("Schema incompatible: {0}")]
    Incompatible(String),
    #[error("Registry error: {0}")]
    RegistryError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Evolution rejected: {0}")]
    EvolutionRejected(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type SchemaResult<T> = Result<T, SchemaError>;

/// Raw bytes from a source, with optional metadata.
pub struct RawRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: Option<i64>,
    pub headers: Vec<(String, Vec<u8>)>,
    /// Opaque source-specific metadata (e.g., Kafka offset, partition).
    pub metadata: SourceMetadata,
}

/// Source-specific metadata -- extensible via Any.
pub struct SourceMetadata {
    inner: Box<dyn std::any::Any + Send + Sync>,
}

/// Decodes raw bytes into Arrow RecordBatches.
///
/// Implementations are format-specific (JSON, Avro, CSV, Parquet, etc.)
/// and are composed with a source connector at CREATE SOURCE time.
///
/// The decoder is stateless after construction -- all schema info is baked
/// in at creation time so the hot path has zero schema lookups.
///
/// **Ring**: Ring 1 (hot decode path). Must be allocation-conscious.
/// **Thread safety**: `Send + Sync` required -- decoders may be shared
/// across async tasks within Ring 1.
pub trait FormatDecoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this decoder produces. Frozen after construction.
    fn output_schema(&self) -> SchemaRef;

    /// Decode a batch of raw records into an Arrow RecordBatch.
    ///
    /// This is the Ring 1 hot path -- implementations must be allocation-
    /// conscious and avoid any I/O or locking.
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch>;

    /// Decode a single record. Default delegates to decode_batch.
    fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
        self.decode_batch(std::slice::from_ref(record))
    }

    /// Human-readable format name for diagnostics.
    fn format_name(&self) -> &str;
}

/// Encodes Arrow RecordBatches into raw bytes for sinks.
///
/// **Ring**: Ring 1. Symmetric counterpart of FormatDecoder.
/// **Thread safety**: `Send + Sync` required.
pub trait FormatEncoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this encoder expects as input.
    fn input_schema(&self) -> SchemaRef;

    /// Encode a RecordBatch into a vector of raw byte payloads (one per row).
    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>>;

    /// Human-readable format name.
    fn format_name(&self) -> &str;
}
```

**Contract**: Any new format (MessagePack, CBOR, FlatBuffers, custom binary) only needs to implement `FormatDecoder`/`FormatEncoder`. The rest of the system does not care how bytes become Arrow.

#### 1.2 `SchemaProvider` -- Self-Describing Sources

Sources that carry their schema with them (Parquet metadata, Iceberg catalog, Avro embedded schema) implement `SchemaProvider`.

```rust
/// Sources that can provide their own schema without external input.
///
/// Examples: Parquet files (metadata), Iceberg (catalog), Avro with
/// embedded schema, Protobuf with descriptor.
///
/// **Ring**: Ring 2 -- `provide_schema()` may involve I/O (reading file
/// headers, querying a catalog, parsing descriptors). Called once at
/// CREATE SOURCE time, never on the hot path.
/// **Thread safety**: `Send + Sync` required.
/// **Error handling**: Return `SchemaError::InferenceFailed` if the
/// source metadata is unavailable or corrupt.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Retrieve the schema from the source itself.
    ///
    /// This is a Ring 2 operation -- may involve I/O (reading file headers,
    /// querying a catalog, parsing descriptors).
    async fn provide_schema(&self, config: &SourceConfig) -> SchemaResult<SchemaRef>;

    /// Whether the provided schema is authoritative (vs. best-effort).
    ///
    /// Authoritative means the schema is guaranteed correct (e.g., Parquet
    /// metadata). Non-authoritative means it is a best guess (e.g., a single
    /// Avro message's embedded schema may not represent all variations).
    fn is_authoritative(&self) -> bool;

    /// Optional: provide field-level metadata (e.g., Iceberg field IDs,
    /// Avro logical types, column descriptions).
    async fn field_metadata(&self, config: &SourceConfig)
        -> SchemaResult<HashMap<String, FieldMeta>>
    {
        Ok(HashMap::new()) // default: no extra metadata
    }
}

/// Extended field metadata beyond Arrow's built-in.
#[derive(Debug, Clone)]
pub struct FieldMeta {
    /// Iceberg-style field ID for evolution tracking.
    pub field_id: Option<i32>,
    /// Human description of the field.
    pub description: Option<String>,
    /// Original type name from the source (e.g., Avro logical type).
    pub source_type: Option<String>,
    /// Default value expression (SQL string).
    pub default_expr: Option<String>,
    /// Arbitrary key-value properties.
    pub properties: HashMap<String, String>,
}
```

#### 1.3 `SchemaInferable` -- Sample-Based Inference

For sources that carry no schema metadata (JSON, CSV, raw text), the framework infers from data samples.

```rust
/// Configuration for sample-based schema inference.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Number of records to sample.
    pub sample_size: usize,
    /// Max time to spend collecting samples.
    pub sample_timeout: Duration,
    /// Default type for fields that are null in all samples.
    pub null_type: DataType,
    /// Default type for numbers (BIGINT, DOUBLE, DECIMAL).
    pub number_type: NumberInference,
    /// Timestamp format patterns to try, in order.
    pub timestamp_formats: Vec<String>,
    /// Max nesting depth to infer (beyond this -> JSONB).
    pub max_depth: usize,
    /// How to handle arrays: first_element, union, or jsonb.
    pub array_strategy: ArrayInference,
    /// Threshold for struct vs. map inference (like DuckDB's
    /// map_inference_threshold). If an object has more than this
    /// many distinct keys across samples, infer MAP instead of STRUCT.
    pub map_threshold: usize,
    /// Threshold for field appearance. If a field appears in fewer
    /// than this fraction of samples, it is treated as optional.
    pub field_appearance_threshold: f64,
}

#[derive(Debug, Clone)]
pub enum NumberInference {
    BigInt,          // default integers to BIGINT
    Double,          // default all numbers to DOUBLE
    Decimal(u8, u8), // default to DECIMAL(p, s)
}

#[derive(Debug, Clone)]
pub enum ArrayInference {
    /// Use the type of the first element for the entire array.
    FirstElement,
    /// Union all element types (may produce a wider type).
    Union,
    /// Give up and store as JSONB.
    Jsonb,
}

/// Sources that support schema inference by sampling data.
///
/// The framework calls `sample_records` to get raw data, then passes
/// it to `infer_from_samples`. This two-step design lets the source
/// handle its own I/O (Kafka consumer, file read, WebSocket connect)
/// while the inference logic is reusable across formats.
///
/// **Ring**: Ring 2 -- both `sample_records()` and `infer_from_samples()`
/// involve I/O or CPU-intensive work. Called once at CREATE SOURCE time.
/// **Thread safety**: `Send + Sync` required.
/// **Error handling**: Return `SchemaError::InferenceFailed` with a
/// descriptive message if sampling times out or produces no usable data.
#[async_trait]
pub trait SchemaInferable: Send + Sync {
    /// Collect a sample of raw records from the source.
    ///
    /// Implementations handle their own connection/disconnection.
    /// The returned records are raw bytes -- format-specific parsing
    /// happens in `infer_from_samples`.
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>>;

    /// Infer Arrow schema from the sampled records.
    ///
    /// Default implementation delegates to format-specific inference
    /// via the `FormatInference` registry. Override this if your source
    /// needs custom inference logic.
    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let inferrer = FORMAT_INFERENCE_REGISTRY
            .get(format)
            .ok_or_else(|| SchemaError::InferenceFailed(
                format!("No inference support for format '{format}'")
            ))?;
        inferrer.infer(samples, inference_config)
    }
}

/// Result of schema inference -- includes confidence information.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// The inferred Arrow schema.
    pub schema: SchemaRef,
    /// Per-field inference details.
    pub field_details: Vec<FieldInferenceDetail>,
    /// Number of samples used.
    pub sample_count: usize,
    /// Fields that were ambiguous (e.g., mixed types).
    pub warnings: Vec<InferenceWarning>,
}

#[derive(Debug, Clone)]
pub struct FieldInferenceDetail {
    pub field_name: String,
    pub inferred_type: DataType,
    /// Fraction of samples where this field was non-null.
    pub appearance_rate: f64,
    /// How confident we are in the inferred type (0.0 - 1.0).
    pub confidence: f64,
    /// Types seen across samples (for diagnostics).
    pub observed_types: Vec<DataType>,
}

#[derive(Debug, Clone)]
pub struct InferenceWarning {
    pub field_name: String,
    pub message: String,
    pub severity: WarningSeverity,
}

#[derive(Debug, Clone, Copy)]
pub enum WarningSeverity {
    Info,
    Warning,
    Error,
}
```

#### 1.4 `SchemaRegistryAware` -- External Registry Integration

For sources where schema lives in an external registry (Confluent Schema Registry, AWS Glue Schema Registry, custom registries).

```rust
/// Schema registry configuration.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Registry URL.
    pub url: String,
    /// Subject name (e.g., "trades-value").
    pub subject: Option<String>,
    /// Schema type (avro, protobuf, json_schema).
    pub schema_type: RegistrySchemaType,
    /// Compatibility mode for evolution.
    pub compatibility: CompatibilityMode,
    /// Credentials (if needed).
    pub credentials: Option<RegistryCredentials>,
}

#[derive(Debug, Clone, Copy)]
pub enum RegistrySchemaType {
    Avro,
    Protobuf,
    JsonSchema,
}

#[derive(Debug, Clone, Copy)]
pub enum CompatibilityMode {
    None,
    Backward,
    Forward,
    Full,
    BackwardTransitive,
    ForwardTransitive,
    FullTransitive,
}

/// A resolved schema from a registry, with version tracking.
#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    pub schema: SchemaRef,
    /// Registry-assigned schema ID (e.g., the 4-byte Confluent ID).
    pub schema_id: u32,
    /// Schema version in the registry.
    pub version: u32,
    /// Raw schema definition (Avro JSON, Protobuf descriptor, etc.)
    pub raw_schema: Vec<u8>,
    /// Subject this schema belongs to.
    pub subject: String,
}

/// Sources that integrate with an external schema registry.
///
/// **Ring**: Ring 2 -- all methods involve network I/O to the registry.
/// Called at CREATE SOURCE time and on schema-ID cache misses.
/// **Thread safety**: `Send + Sync` required.
/// **Error handling**: Return `SchemaError::RegistryError` for
/// connectivity, authentication, or compatibility failures.
#[async_trait]
pub trait SchemaRegistryAware: Send + Sync {
    /// Fetch the latest schema from the registry.
    async fn fetch_schema(
        &self,
        registry_config: &RegistryConfig,
    ) -> SchemaResult<RegisteredSchema>;

    /// Fetch a specific schema version by ID.
    async fn fetch_schema_by_id(
        &self,
        registry_config: &RegistryConfig,
        schema_id: u32,
    ) -> SchemaResult<RegisteredSchema>;

    /// Check if a new schema is compatible with the current one.
    async fn check_compatibility(
        &self,
        registry_config: &RegistryConfig,
        current: &RegisteredSchema,
        proposed: &SchemaRef,
    ) -> SchemaResult<bool>;

    /// Register a new schema (for sinks that write to registry-backed topics).
    async fn register_schema(
        &self,
        registry_config: &RegistryConfig,
        schema: &SchemaRef,
    ) -> SchemaResult<RegisteredSchema>;

    /// Build a decoder that can handle schema ID headers (e.g., Confluent
    /// wire format: [magic_byte, 4-byte schema_id, payload]).
    fn build_registry_decoder(
        &self,
        registered: &RegisteredSchema,
    ) -> SchemaResult<Box<dyn FormatDecoder>>;
}
```

#### 1.5 `SchemaEvolvable` -- Schema Evolution Support

For sources/sinks that need to handle schema changes over time without downtime.

```rust
/// Describes what kind of schema change occurred.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    ColumnAdded {
        name: String,
        data_type: DataType,
        nullable: bool,
        default: Option<String>,
    },
    ColumnDropped {
        name: String,
    },
    ColumnRenamed {
        old_name: String,
        new_name: String,
    },
    TypeWidened {
        name: String,
        from: DataType,
        to: DataType,
    },
    TypeNarrowed {
        name: String,
        from: DataType,
        to: DataType,
    },
}

/// Result of evaluating a schema change.
#[derive(Debug, Clone)]
pub enum EvolutionVerdict {
    /// Change is safe and can be applied automatically.
    Compatible(Vec<SchemaChange>),
    /// Change requires user confirmation (e.g., type narrowing).
    RequiresConfirmation(Vec<SchemaChange>, String),
    /// Change is rejected as incompatible.
    Incompatible(String),
}

/// Sources/sinks that support schema evolution.
///
/// **Ring**: Ring 2 -- evolution evaluation is a control-plane operation.
/// **Thread safety**: `Send + Sync` required.
/// **Error handling**: Return `SchemaError::EvolutionRejected` when a
/// proposed change violates the configured compatibility mode.
#[async_trait]
pub trait SchemaEvolvable: Send + Sync {
    /// Compute the diff between the current and proposed schema.
    fn diff_schemas(
        &self,
        current: &SchemaRef,
        proposed: &SchemaRef,
    ) -> Vec<SchemaChange>;

    /// Evaluate whether a set of changes is compatible.
    fn evaluate_evolution(
        &self,
        changes: &[SchemaChange],
        compatibility: CompatibilityMode,
    ) -> EvolutionVerdict;

    /// Apply schema evolution -- returns the new merged schema and
    /// an optional projection map (old column index -> new column index).
    fn apply_evolution(
        &self,
        current: &SchemaRef,
        changes: &[SchemaChange],
    ) -> SchemaResult<(SchemaRef, ColumnProjection)>;

    /// Track schema version history. Returns the current version number.
    fn schema_version(&self) -> u64;
}

/// Maps old column positions to new positions after evolution.
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    /// For each column in the NEW schema, where it came from in the OLD schema.
    /// None means it is a newly added column (fill with default/NULL).
    pub mappings: Vec<Option<usize>>,
}
```

---

### 2. Top-Level Connector Traits with Capability Discovery

The `SourceConnector` and `SinkConnector` traits are the top-level traits that every connector implements. Schema capabilities are discovered via optional downcast methods.

```rust
/// The top-level trait for all source connectors.
///
/// Every source implements this. Capability traits (SchemaProvider,
/// SchemaInferable, etc.) are discovered via the `as_*` methods.
/// This pattern avoids forcing every source to implement irrelevant
/// traits while still allowing the SchemaResolver to discover what
/// a source can do.
#[async_trait]
pub trait SourceConnector: Send + Sync + std::fmt::Debug {
    /// Unique name of this connector type (e.g., "kafka", "websocket", "file").
    fn connector_type(&self) -> &str;

    /// Validate the source configuration.
    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()>;

    /// Build the FormatDecoder for this source + format combination.
    ///
    /// The schema has already been resolved by the SchemaResolver.
    fn build_decoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>>;

    /// Start producing records. Returns a stream of RawRecords.
    async fn start(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn RecordStream>>;

    // -- Capability discovery ------------------------------------------------

    /// Does this source carry its own schema?
    fn as_schema_provider(&self) -> Option<&dyn SchemaProvider> {
        None // default: no
    }

    /// Can this source infer schema from samples?
    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        None // default: no
    }

    /// Does this source integrate with a schema registry?
    fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> {
        None // default: no
    }

    /// Does this source support schema evolution?
    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        None // default: no
    }

    /// Supported formats for this connector.
    fn supported_formats(&self) -> &[&str];
}

/// Parallel trait for sinks.
#[async_trait]
pub trait SinkConnector: Send + Sync + std::fmt::Debug {
    fn connector_type(&self) -> &str;
    async fn validate_config(&self, config: &SinkConfig) -> SchemaResult<()>;

    fn build_encoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn FormatEncoder>>;

    async fn start(
        &self,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn RecordSink>>;

    fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> {
        None
    }
    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        None
    }
    fn supported_formats(&self) -> &[&str];
}
```

---

### 3. Capability Discovery Pattern

We use `as_schema_provider() -> Option<&dyn SchemaProvider>` for capability discovery. This is a deliberate choice over three alternatives:

**Why not mandatory trait bounds?**

If `SourceConnector` required `SchemaProvider + SchemaInferable + SchemaRegistryAware + SchemaEvolvable`, every source would have to implement all four traits even when they are irrelevant. A WebSocket source would need a no-op `SchemaRegistryAware` implementation. This violates the Interface Segregation Principle and makes it harder to add new capability traits in the future without breaking all existing connectors.

**Why not enum-based dispatch?**

An enum like `SchemaCapability { Registry(RegistryConfig), Inferred(InferenceConfig), ... }` creates a closed set. Adding a new capability means adding an enum variant, which is a breaking change for all match arms. The trait-based approach is an open set -- new capabilities can be added by defining new traits without modifying existing code.

**Why not generic type parameters?**

A design like `SourceConnector<S: SchemaStrategy>` is more type-safe, but it loses object safety. The `SchemaResolver` needs to work with `&dyn SourceConnector` because the concrete type is not known until runtime (it comes from the `ConnectorRegistry` factory). Generic type parameters would require either monomorphization at the registry level (impossible with dynamic dispatch) or type erasure that reimplements what `dyn Trait` already provides.

**Trade-offs of the chosen approach:**

- (+) Object-safe: works with `Box<dyn SourceConnector>` and `&dyn SourceConnector`
- (+) Open for extension: new capability traits can be added without modifying existing connectors
- (+) Default implementations return `None`, so connectors only opt into what they support
- (-) Slight runtime overhead from dynamic dispatch (but only in Ring 2, never on the hot path)
- (-) No compile-time guarantee that a connector implements the capabilities it claims (must be tested)

---

### 4. Trait Composition Matrix

This table shows which existing and planned sources implement which capability traits:

| Source | FormatDecoder | SchemaProvider | SchemaInferable | SchemaRegistryAware | SchemaEvolvable |
|--------|:---:|:---:|:---:|:---:|:---:|
| **Kafka** | Yes | No | Yes (JSON/CSV) | Yes (Avro/Proto) | Yes (via registry) |
| **WebSocket** | Yes | No | Yes (JSON) | No | No |
| **File (CSV)** | Yes | No | Yes | No | No |
| **File (Parquet)** | Yes | Yes (metadata) | Yes (auto) | No | No |
| **Iceberg** | Yes | Yes (catalog) | No | No | Yes |
| **RabbitMQ** | Yes | No | Yes (JSON) | No | No |
| **NATS** (3rd party) | Yes | No | Yes | No | No |
| **Delta Lake** (3rd party) | Yes | Yes (log) | No | No | Yes |
| **Postgres CDC** (future) | Yes | Yes (pg_catalog) | No | No | Yes |

Key observations:
- `FormatDecoder` is universal -- every source must produce Arrow `RecordBatch`.
- `SchemaProvider` and `SchemaInferable` are nearly mutually exclusive: self-describing sources do not need inference, and inference-only sources have no metadata to provide.
- `SchemaRegistryAware` is currently Kafka-specific but is designed to support AWS Glue and custom registries.
- `SchemaEvolvable` correlates with sources that have version-tracked schemas (registries, catalogs).

---

### 5. Connector Factory and Registry

```rust
/// Factory for creating source connectors from DDL configuration.
pub trait SourceConnectorFactory: Send + Sync {
    /// The connector type name used in SQL (e.g., "kafka", "websocket").
    fn connector_type(&self) -> &str;

    /// Create a new source connector from parsed DDL config.
    fn create(&self, config: &SourceConfig) -> SchemaResult<Box<dyn SourceConnector>>;

    /// Describe the configuration options this connector accepts.
    fn config_schema(&self) -> ConnectorConfigSchema;
}

pub trait SinkConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    fn create(&self, config: &SinkConfig) -> SchemaResult<Box<dyn SinkConnector>>;
    fn config_schema(&self) -> ConnectorConfigSchema;
}

/// Registry of all available connector factories.
pub struct ConnectorRegistry {
    sources: HashMap<String, Box<dyn SourceConnectorFactory>>,
    sinks: HashMap<String, Box<dyn SinkConnectorFactory>>,
}
```

---

### 6. Extension Checklist

Step-by-step guide for adding a new source connector:

1. **Create a struct implementing `SourceConnector`.** The struct holds any connector-specific configuration. Implement the required methods: `connector_type()`, `validate_config()`, `build_decoder()`, `start()`, and `supported_formats()`.

2. **Decide which capability traits apply.** Consult the trait composition matrix above. Override the relevant `as_*` methods to return `Some(self)`, then implement the corresponding traits on your struct. Most new message-queue sources will only need `SchemaInferable`. Self-describing sources (file formats with metadata, catalog-backed tables) will need `SchemaProvider`. Only sources with external registries need `SchemaRegistryAware`.

3. **Implement `SourceConnectorFactory` for the registry.** The factory creates instances of your connector from DDL configuration. Implement `connector_type()`, `create()`, and `config_schema()`. The `config_schema()` method documents required and optional configuration keys.

4. **Register in `ConnectorRegistry`.** Call `registry.register_source(Box::new(YourSourceFactory))` at startup. For built-in connectors, this goes in `ConnectorRegistry::new()`. For third-party connectors, call it from user code or plugin initialization.

5. **Write the SQL `CREATE SOURCE` parser extension.** If your connector uses the standard `FROM <TYPE> (key = value, ...)` syntax, no parser changes are needed. If it requires new DDL syntax (e.g., a new clause), extend the `StreamingSqlDialect` in `laminar-sql`.

6. **Add tests using the mock harness.** Use the `MockSourceConnector` and `test_source_connector()` harness from `laminar-connectors/src/testing.rs` for unit tests. Write integration tests that exercise the full lifecycle: creation, schema resolution, decoding, and checkpoint/restore.

7. **If your source supports a new format, register it in `FormatInferenceRegistry`.** Implement the `FormatInference` trait for your format and call `registry.register(Box::new(YourFormatInference))`. This enables sample-based inference for any source that uses your format.

---

### 7. Non-Negotiable Constraints

These rules are not guidelines -- they are hard requirements that all connector implementations must satisfy:

1. **All decoders produce Arrow `RecordBatch`.** No raw bytes, no custom structs, no JSON strings past Ring 1. The entire engine operates on Arrow columnar format from Ring 1 onward.

2. **Schema resolution is always Ring 2.** The `SchemaResolver`, `SchemaProvider::provide_schema()`, `SchemaInferable::sample_records()`, and `SchemaRegistryAware::fetch_schema()` are all Ring 2 operations. They run once at `CREATE SOURCE` time. They must never be called from Ring 0 or Ring 1 hot paths.

3. **Decoders are stateless after construction.** The Arrow schema is baked into the decoder at creation time. `FormatDecoder::decode_batch()` must not perform schema lookups, registry fetches, or any other stateful operation. This is what makes Ring 1 decoding fast.

4. **`FormatDecoder::decode_batch()` must be allocation-conscious.** Minimize heap allocations per batch. Reuse buffers where possible. Prefer Arrow's builder pattern (append to pre-allocated arrays) over constructing new vectors per record. The target is sub-microsecond per record.

5. **All new formats register in `FormatInferenceRegistry` if they support inference.** If a format can have its schema inferred from sample data (JSON, CSV, MessagePack can; raw bytes cannot), it must register a `FormatInference` implementation. This ensures that `INFER SCHEMA` works automatically for any source using that format.

6. **All traits must be object-safe (`dyn`-compatible).** No associated types with `Self` bounds, no generic methods, no `Sized` requirements. Every trait must work behind `&dyn Trait` and `Box<dyn Trait>`. This is required for the capability discovery pattern and the `ConnectorRegistry`.

---

### 8. Schema Resolution Flow

The `SchemaResolver` orchestrates schema resolution using the following priority:

```
User SQL:  CREATE SOURCE trades (...) FROM KAFKA (...) FORMAT JSON INFER SCHEMA (...)
                    |                       |               |           |
                    v                       v               v           v
             DeclaredSchema          ConnectorRegistry  FormatName  InferenceConfig
             (user columns)          .create_source()
                    |                       |
                    |                       v
                    |               KafkaSource (Box<dyn SourceConnector>)
                    |                       |
                    |         +-------------+--------------------+
                    |         | capability discovery              |
                    |         v                                   v
                    |   as_schema_inferable() -> Some     as_schema_registry_aware() -> Some
                    |         |                                   |
                    v         v                                   |
            +-------------------------------+                    |
            |     SchemaResolver            |                    |
            |                               |                    |
            |  1. Has full DDL? -> use it   |                    |
            |  2. Has registry? -> fetch    |<-------------------+
            |  3. Self-describing? -> use   |
            |  4. Inferable? -> sample      |
            |  5. Merge with user hints     |
            +---------------+---------------+
                            |
                            v
                      ResolvedSchema {
                        schema: Arc<Schema>,
                        resolution: Inferred { ... },
                        field_origins: { ... }
                      }
                            |
                            v
                  source.build_decoder(schema, "json", config)
                            |
                            v
                  JsonDecoder (Box<dyn FormatDecoder>)
                            |
                            v
                  Frozen in Ring 1 -- hot path ready
```

Priority order:
1. If the user provided a full explicit schema with no wildcard, use it directly.
2. If `as_schema_registry_aware()` returns `Some` and a registry config is provided, fetch from the registry.
3. If `as_schema_provider()` returns `Some`, use the self-describing schema.
4. If `as_schema_inferable()` returns `Some`, sample data and infer.
5. If none of the above apply and the user did not declare a schema, return an error.

User-declared columns always take priority over inferred/provided columns when both are present (the wildcard `*` case).

---

### 9. Extension Points Summary

| Extension | What to Implement | Layer |
|-----------|-------------------|-------|
| **New source** (e.g., NATS, Redis Streams) | `SourceConnector` + relevant capability traits + `SourceConnectorFactory` | Core |
| **New sink** (e.g., Elasticsearch) | `SinkConnector` + `SinkConnectorFactory` | Core |
| **New format** (e.g., CBOR, FlatBuffers) | `FormatDecoder` + `FormatEncoder` | Format layer |
| **New inference** (e.g., MessagePack) | `FormatInference` + register in `FormatInferenceRegistry` | Inference layer |
| **New registry** (e.g., AWS Glue) | `SchemaRegistryAware` impl on your source | Registry layer |
| **New evolution** (e.g., Delta Lake) | `SchemaEvolvable` impl | Evolution layer |

Each extension point is independent. Adding a new format does not require touching any source. Adding a new source does not require implementing any format. The `SchemaResolver` automatically discovers and composes whatever capabilities are available.

## Consequences

### Positive

- **Third-party connectors only depend on the trait crate, not LaminarDB internals.** A connector author implements the traits defined in this ADR and registers a factory. They never need to import `laminar-core`, `laminar-sql`, or `laminar-storage`.
- **New formats and sources can be added without modifying existing code.** The `ConnectorRegistry` and `FormatInferenceRegistry` are open for registration. No match arms to update, no enum variants to add.
- **Schema resolution is composable.** The `SchemaResolver` auto-discovers capabilities via the `as_*` methods and picks the best strategy. A single source can expose different capabilities depending on its format configuration (Kafka with JSON exposes inference; Kafka with Avro exposes registry).
- **Clear performance boundaries.** Ring 0/1/2 separation is encoded in the trait contracts. Schema resolution is Ring 2 (one-time). Decoding is Ring 1 (per-batch). Ring 0 never touches connector code. This is not just a convention -- it is enforced by the trait design (async methods for Ring 2, sync methods for Ring 1).
- **Object safety enables dynamic dispatch.** All traits work with `&dyn Trait` and `Box<dyn Trait>`, which is essential for the factory/registry pattern and for the `SchemaResolver` to work with heterogeneous sources.

### Negative

- **Dynamic dispatch overhead for trait methods.** Each `decode_batch()` call goes through a vtable indirection. This is mitigated by: (a) decode happens in Ring 1, not Ring 0; (b) batching amortizes the per-call overhead; (c) the compiler can still inline within a monomorphic call site if the concrete type is known.
- **More traits to understand for new contributors.** Five capability traits plus the two top-level traits is a larger surface area than the current single-trait design. This is mitigated by: (a) most connectors only implement 1-2 capability traits; (b) the extension checklist provides a clear path; (c) the trait composition matrix makes it obvious which traits to implement.
- **Capability discovery adds slight runtime overhead.** The `as_*` methods are called during schema resolution (Ring 2, once per `CREATE SOURCE`). The overhead is negligible -- it is a single virtual dispatch returning `Option<&dyn Trait>`.

### Neutral

- **Existing F034 connector implementations need to add capability discovery methods.** The current `SourceConnector` and `SinkConnector` traits in `connector.rs` need to be extended with the `as_*` methods. All existing implementations can return `None` initially and be upgraded incrementally.
- **SQL parser needs extension for new DDL syntax.** The `INFER SCHEMA (...)` and `USING SCHEMA REGISTRY (...)` clauses require parser support in `laminar-sql`. This is tracked separately as part of the schema inference feature work.

## Alternatives Considered

### Monolithic Trait

One big trait that all sources implement, with all methods for all capabilities:

```rust
trait SourceConnector {
    fn provide_schema(&self) -> Option<SchemaRef> { None }
    fn sample_records(&self) -> Option<Vec<RawRecord>> { None }
    fn fetch_registry_schema(&self) -> Option<RegisteredSchema> { None }
    fn diff_schemas(&self, ...) -> Vec<SchemaChange> { vec![] }
    // ... 20+ methods, most returning None/empty
}
```

Rejected because it violates the Interface Segregation Principle. Every source carries the weight of every capability's types in its vtable. Adding a new capability means adding methods to the trait, which is a breaking change for all implementors (even with default implementations, it pollutes the API surface).

### Generic Associated Types

A more type-safe design using GATs:

```rust
trait SourceConnector {
    type SchemaStrategy: SchemaStrategy;
    fn schema_strategy(&self) -> &Self::SchemaStrategy;
}
```

Rejected because it loses object safety. `SourceConnector` with an associated type cannot be used as `dyn SourceConnector` -- the concrete `SchemaStrategy` type must be known at compile time. The `ConnectorRegistry` needs dynamic dispatch because the concrete connector type is determined at runtime from the SQL `FROM <TYPE>` clause.

### Plugin Architecture with Shared Libraries

Load connectors as `.so`/`.dll` shared libraries at runtime:

```rust
// liblaminar_connector_nats.so
#[no_mangle]
pub extern "C" fn create_connector() -> *mut dyn SourceConnector { ... }
```

Rejected because: (a) shared library loading is platform-dependent and fragile; (b) ABI compatibility between the plugin and the host is not guaranteed across Rust compiler versions; (c) LaminarDB is an embedded database -- the deployment model is a single binary, not a plugin host; (d) the trait-based approach already supports third-party connectors via Cargo dependencies without any FFI complexity.

## References

- [F034: Connector SDK](../features/phase-3/F034-connector-sdk.md) -- the lifecycle trait framework this ADR extends
- [ADR-003: SQL Parser Strategy](ADR-003-sql-parser-strategy.md) -- the parser that will support `INFER SCHEMA` and `USING SCHEMA REGISTRY`
- [ADR-004: Checkpoint Strategy](ADR-004-checkpoint-strategy.md) -- the Ring 0/1/2 architecture referenced throughout
- [ADR-005: Tiered Reference Tables](ADR-005-tiered-reference-tables.md) -- the `ReferenceTableSource` trait pattern
- [Extensible Schema Traits Research](../research/extensible-schema-traits.md) -- full design specification
- [Schema Inference Design Research](../research/schema-inference-design.md) -- format-specific inference details
- [Apache Flink Source API v2 (FLIP-27)](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27) -- industry connector framework
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/) -- registry integration model
- [Apache Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/) -- evolution model reference
