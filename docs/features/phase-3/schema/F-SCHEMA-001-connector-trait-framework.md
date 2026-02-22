# F-SCHEMA-001: Extensible Connector Trait Framework

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-001 |
| **Phase** | 3 (Connectors & Integration) |
| **Priority** | P0 (Foundation for all schema-aware connectors) |
| **Status** | Draft |
| **Effort** | XL (3-4 weeks) |
| **Dependencies** | F034 (Connector SDK) |
| **Crate** | `laminar-connector-api` (new), extensions in `laminar-connectors` |
| **Research** | [Schema Inference Design](../../../research/schema-inference-design.md), [Extensible Schema Traits](../../../research/extensible-schema-traits.md) |

---

## Summary

Define five composable capability traits (`FormatDecoder`/`FormatEncoder`, `SchemaProvider`, `SchemaInferable`, `SchemaRegistryAware`, `SchemaEvolvable`) that augment the existing `SourceConnector`/`SinkConnector` lifecycle from F034. These traits are published in a new zero-dependency `laminar-connector-api` crate so that third-party connectors can implement them without pulling in the full LaminarDB dependency tree. A `SchemaResolver` orchestrator discovers which traits a connector implements and resolves the final frozen Arrow schema at `CREATE SOURCE`/`CREATE SINK` time, keeping the Ring 0 hot path allocation-free.

---

## Motivation

### Problem

The F034 Connector SDK provides `SourceConnector` and `SinkConnector` with a single `discover_schema()` method that returns `Option<SchemaRef>`. This is insufficient for production use:

1. **No format awareness.** JSON, Avro, CSV, and Protobuf all need different decode/encode paths, but F034 treats serialization as an orthogonal `RecordDeserializer`/`RecordSerializer` pair with no connection to schema resolution.
2. **No schema registry integration.** Kafka + Avro sources need to fetch schemas from Confluent Schema Registry, cache schema IDs, and handle the 5-byte wire-format header. None of this is representable in F034.
3. **No inference support.** JSON and CSV sources carry no schema metadata. The framework provides no way for a connector to advertise "I can sample records and infer types."
4. **No evolution support.** When a Kafka topic's Avro schema adds a field mid-stream, the system has no protocol for detecting the change, validating compatibility, and hot-swapping the decoder.
5. **Third-party extensibility.** External developers who want to add a NATS or Redis Streams connector must depend on the entire `laminar-connectors` crate to get the trait definitions.

### Solution

Decompose schema capabilities into five orthogonal traits. Each connector implements only the traits relevant to its capabilities. A `SchemaResolver` automatically discovers what a source can do via capability-discovery methods (`as_schema_provider()`, `as_schema_inferable()`, etc.) and picks the best resolution strategy.

### Why a New Crate

The trait definitions (the "contract") must live in a separate `laminar-connector-api` crate that:
- Has minimal dependencies (only `arrow`, `async-trait`, `thiserror`, `anyhow`)
- Can be depended on by third-party connector crates without pulling in Kafka, Postgres, or storage libraries
- Provides a stable API surface that evolves independently from connector implementations

---

## Goals

1. Define the five capability traits with full doc comments, error types, and supporting structs.
2. Add capability-discovery methods to `SourceConnector` and `SinkConnector` (backward compatible with F034 -- all new methods have default `None` implementations).
3. Create the `laminar-connector-api` crate containing trait definitions, error types, config types, and the `ConnectorRegistry`/factory traits.
4. Implement `SchemaResolver` that resolves schemas using a priority cascade: explicit DDL > schema registry > self-describing > sample-based inference.
5. Publish `SourceConfig`/`SinkConfig` with typed accessors and `ConnectorConfigSchema` for self-documenting configuration.
6. Ensure all traits are object-safe (usable as `dyn Trait`).
7. Provide compile-time tests that verify object safety.

## Non-Goals

- **Format implementations.** Actual JSON, Avro, CSV decoders/encoders are separate features (F-SCHEMA-002+). This spec defines the traits they implement.
- **Inference algorithms.** The `SchemaInferable` trait defines the interface; the actual JSON/CSV inference engines are separate features.
- **Schema registry client.** The `SchemaRegistryAware` trait defines the contract; the Confluent Schema Registry HTTP client is a separate feature.
- **SQL parser changes.** `CREATE SOURCE ... INFER SCHEMA (...)` syntax is a separate parser feature that consumes these traits.
- **Runtime execution.** The `ConnectorRuntime` from F034 is not modified here; it will be extended in follow-up features to call `SchemaResolver`.

---

## Technical Design

### Architecture Overview

```
                   ┌─────────────────────────────────────────────────────────────┐
                   │              CREATE SOURCE ... FORMAT ... INFER SCHEMA ...    │
                   │                              (SQL DDL)                        │
                   └────────────────────────────────┬────────────────────────────┘
                                                    │
                                                    ▼
                   ┌─────────────────────────────────────────────────────────────┐
                   │                      SchemaResolver                          │
                   │                    (Ring 2 — Control)                        │
                   │                                                              │
                   │  Priority: Explicit > Registry > Provider > Inference        │
                   │                                                              │
                   │  Inputs:  DeclaredSchema (from DDL)                          │
                   │           &dyn SourceConnector (capability discovery)        │
                   │           SourceConfig, InferenceConfig, RegistryConfig      │
                   │                                                              │
                   │  Output:  ResolvedSchema { schema, resolution, origins }     │
                   └────────────────────────────────┬────────────────────────────┘
                                                    │
                   ┌────────────────────────────────┼────────────────────────────┐
                   │                                │                             │
          ┌────────▼────────┐           ┌───────────▼──────────┐    ┌───────────▼──────────┐
          │ SchemaRegistry- │           │   SchemaProvider      │    │  SchemaInferable     │
          │    Aware         │           │  (self-describing)    │    │  (sample-based)      │
          │ (external reg.)  │           │                       │    │                      │
          │                  │           │  Parquet, Iceberg,    │    │  JSON, CSV,          │
          │  Confluent SR,   │           │  Delta Lake,          │    │  MessagePack         │
          │  AWS Glue        │           │  Postgres CDC         │    │                      │
          └──────────────────┘           └───────────────────────┘    └───────────┬──────────┘
                                                                                  │
                                                                     ┌────────────▼─────────┐
                                                                     │ FormatInference      │
                                                                     │ Registry             │
                                                                     │ (per-format logic)   │
                                                                     └──────────────────────┘
                                                    │
                                                    ▼
          ┌──────────────────────────────────────────────────────────────────────────────────┐
          │                     source.build_decoder(resolved_schema, format, config)         │
          │                              → Box<dyn FormatDecoder>                             │
          │                                                                                   │
          │                         Frozen in Ring 1 — hot path ready                         │
          └──────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
          ┌──────────────────────────────────────────────────────────────────────────────────┐
          │  Ring 0 (Hot Path)                                                                │
          │                                                                                   │
          │  FormatDecoder.decode_batch(&[RawRecord]) → RecordBatch                          │
          │  Zero-allocation, no schema lookups, no I/O, no locking                          │
          └──────────────────────────────────────────────────────────────────────────────────┘


Trait Hierarchy:
                                                                          Ring
  ┌─────────────────────────┐
  │    SourceConnector      │  (F034 base — lifecycle + capability discovery)     1/2
  │    SinkConnector        │
  └──────────┬──────────────┘
             │  as_*() capability discovery
             │
  ┌──────────┼──────────────────────────────────────────────────────────────┐
  │          │          │              │               │                     │
  │   FormatDecoder  FormatEncoder  SchemaProvider  SchemaInferable   Schema │
  │   (bytes→Arrow)  (Arrow→bytes)  (self-desc.)   (sample-based)   Registry│
  │                                                                  Aware  │
  │   Ring 1 (hot)   Ring 1 (hot)   Ring 2          Ring 2          Ring 2  │
  │                                                                         │
  │                                              SchemaEvolvable            │
  │                                              (evolution)                │
  │                                              Ring 2                     │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Trait / Operation | Ring | Latency Target | Frequency | Notes |
|-------------------|------|---------------|-----------|-------|
| `FormatDecoder::decode_batch()` | Ring 1 | < 50us/batch | Every batch | Hot path; allocation-conscious |
| `FormatDecoder::output_schema()` | Ring 0/1 | < 10ns | Rare (cached) | Returns `SchemaRef` (Arc clone) |
| `FormatEncoder::encode_batch()` | Ring 1 | < 50us/batch | Every batch | Hot path; allocation-conscious |
| `SchemaProvider::provide_schema()` | Ring 2 | < 5s | Once at CREATE SOURCE | May involve I/O |
| `SchemaProvider::field_metadata()` | Ring 2 | < 5s | Once at CREATE SOURCE | May involve I/O |
| `SchemaInferable::sample_records()` | Ring 2 | < 10s | Once at CREATE SOURCE | Opens temp connection, reads N records |
| `SchemaInferable::infer_from_samples()` | Ring 2 | < 1s | Once at CREATE SOURCE | CPU-bound inference |
| `SchemaRegistryAware::fetch_schema()` | Ring 2 | < 2s | At CREATE SOURCE + on schema ID miss | HTTP to registry |
| `SchemaRegistryAware::build_registry_decoder()` | Ring 2 | < 100ms | On schema ID miss | Builds new decoder |
| `SchemaEvolvable::diff_schemas()` | Ring 2 | < 1ms | On schema change | Pure computation |
| `SchemaEvolvable::apply_evolution()` | Ring 2 | < 1ms | On schema change | Pure computation |
| `SchemaResolver::resolve()` | Ring 2 | < 15s | Once at CREATE SOURCE | Orchestrates all above |
| `ConnectorRegistry::create_source()` | Ring 2 | < 1ms | Once at CREATE SOURCE | Factory lookup |

### Error Types

```rust
use std::collections::HashMap;

/// Errors that can occur during schema or format operations.
///
/// This is the primary error type for all five capability traits.
/// It is designed to be informative enough for end-user SQL error
/// messages while preserving the original cause chain.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    /// Schema inference failed (could not determine types from samples).
    #[error("Schema inference failed: {0}")]
    InferenceFailed(String),

    /// Schema is incompatible with the expected structure.
    #[error("Schema incompatible: {0}")]
    Incompatible(String),

    /// Error communicating with an external schema registry.
    #[error("Registry error: {0}")]
    RegistryError(String),

    /// Error decoding raw bytes into Arrow format.
    #[error("Decode error: {0}")]
    DecodeError(String),

    /// Schema evolution was rejected (e.g., type narrowing).
    #[error("Evolution rejected: {0}")]
    EvolutionRejected(String),

    /// Missing required configuration key.
    #[error("Missing required config key: {0}")]
    MissingConfig(String),

    /// Invalid configuration value.
    #[error("Invalid config value for '{key}': {message}")]
    InvalidConfig {
        /// The configuration key.
        key: String,
        /// Description of what is wrong.
        message: String,
    },

    /// Arrow error from the underlying Arrow library.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    /// Catch-all for connector-specific errors.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Convenience type alias for schema operations.
pub type SchemaResult<T> = Result<T, SchemaError>;
```

### Core Supporting Types

```rust
use std::collections::HashMap;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

/// Raw bytes from a source, with optional metadata.
///
/// This is the unit of data that flows between a source connector
/// and a format decoder. It carries the raw payload plus any
/// source-specific metadata (offsets, partitions, headers).
#[derive(Debug)]
pub struct RawRecord {
    /// Optional key bytes (e.g., Kafka message key).
    pub key: Option<Vec<u8>>,

    /// The value payload bytes.
    pub value: Vec<u8>,

    /// Optional event timestamp (epoch milliseconds).
    pub timestamp: Option<i64>,

    /// Key-value headers (e.g., Kafka headers).
    pub headers: Vec<(String, Vec<u8>)>,

    /// Opaque source-specific metadata (e.g., Kafka offset,
    /// partition, or NATS sequence number).
    pub metadata: SourceMetadata,
}

/// Source-specific metadata -- extensible via `Any`.
///
/// Each connector can store its own metadata type here.
/// The `SchemaResolver` and format decoders do not inspect
/// this; it exists so that connectors can pass context
/// through the decode pipeline for offset tracking.
pub struct SourceMetadata {
    inner: Box<dyn std::any::Any + Send + Sync>,
}

impl SourceMetadata {
    /// Creates empty metadata.
    pub fn empty() -> Self {
        Self {
            inner: Box::new(()),
        }
    }

    /// Creates metadata from a typed value.
    pub fn new<T: std::any::Any + Send + Sync>(value: T) -> Self {
        Self {
            inner: Box::new(value),
        }
    }

    /// Attempts to downcast to a concrete type.
    pub fn downcast_ref<T: std::any::Any>(&self) -> Option<&T> {
        self.inner.downcast_ref()
    }
}

impl std::fmt::Debug for SourceMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SourceMetadata { ... }")
    }
}

/// Extended field metadata beyond Arrow's built-in `Field::metadata()`.
///
/// Captures format-specific or catalog-specific information that
/// enriches the schema for evolution tracking and diagnostics.
#[derive(Debug, Clone, Default)]
pub struct FieldMeta {
    /// Iceberg-style field ID for evolution tracking.
    /// Enables column renames/reorders without data migration.
    pub field_id: Option<i32>,

    /// Human-readable description of the field.
    pub description: Option<String>,

    /// Original type name from the source (e.g., Avro logical
    /// type like `"timestamp-millis"`, Postgres `"timestamptz"`).
    pub source_type: Option<String>,

    /// Default value expression (SQL string, e.g., `"'UNKNOWN'"`).
    pub default_expr: Option<String>,

    /// Arbitrary key-value properties for extension.
    pub properties: HashMap<String, String>,
}
```

### Trait 1: `FormatDecoder` / `FormatEncoder`

These are the Ring 1 hot-path traits. Every source needs a decoder; every sink needs an encoder.

```rust
/// Decodes raw bytes into Arrow `RecordBatch`es.
///
/// Implementations are format-specific (JSON, Avro, CSV, Parquet,
/// Protobuf, etc.) and are composed with a source connector at
/// `CREATE SOURCE` time.
///
/// # Hot Path Contract
///
/// The decoder is **stateless after construction** -- all schema
/// information is baked in at creation time so the hot path has
/// **zero schema lookups**. Implementations must be allocation-
/// conscious and avoid any I/O or locking in `decode_batch()`.
///
/// # Object Safety
///
/// This trait is object-safe. It is used as `Box<dyn FormatDecoder>`
/// in the connector runtime.
pub trait FormatDecoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this decoder produces.
    ///
    /// Frozen after construction. The returned `SchemaRef` is an
    /// `Arc<Schema>` that can be cheaply cloned.
    fn output_schema(&self) -> SchemaRef;

    /// Decode a batch of raw records into an Arrow `RecordBatch`.
    ///
    /// This is the **Ring 1 hot path** -- implementations must be
    /// allocation-conscious and avoid any I/O or locking.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::DecodeError` if any record cannot be
    /// parsed according to the frozen schema.
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch>;

    /// Decode a single record.
    ///
    /// Default delegates to `decode_batch` with a single-element slice.
    fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
        self.decode_batch(std::slice::from_ref(record))
    }

    /// Human-readable format name for diagnostics (e.g., `"json"`,
    /// `"avro"`, `"csv"`).
    fn format_name(&self) -> &str;
}

/// Encodes Arrow `RecordBatch`es into raw bytes for sinks.
///
/// # Object Safety
///
/// This trait is object-safe. It is used as `Box<dyn FormatEncoder>`
/// in the sink connector runtime.
pub trait FormatEncoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this encoder expects as input.
    fn input_schema(&self) -> SchemaRef;

    /// Encode a `RecordBatch` into a vector of raw byte payloads
    /// (one `Vec<u8>` per row).
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::DecodeError` if the batch schema does
    /// not match `input_schema()`.
    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>>;

    /// Human-readable format name.
    fn format_name(&self) -> &str;
}
```

**Object Safety Analysis:** Both traits are object-safe. All methods take `&self` and return owned or reference types. No associated types, no generic methods, no `Self`-returning methods.

### Trait 2: `SchemaProvider`

For self-describing sources that carry their schema (Parquet metadata, Iceberg catalog, Postgres `pg_catalog`, Delta Lake transaction log).

```rust
/// Sources that can provide their own schema without external input.
///
/// # Examples
///
/// - **Parquet files**: Schema embedded in file metadata (exact Arrow mapping).
/// - **Iceberg tables**: Schema in catalog metadata with field IDs.
/// - **Delta Lake**: Schema in the transaction log (`_delta_log`).
/// - **PostgreSQL CDC**: Schema from `pg_catalog` system tables.
///
/// # Ring Placement
///
/// All methods are **Ring 2** operations -- they may perform I/O
/// (reading file headers, querying a catalog, parsing descriptors).
///
/// # Object Safety
///
/// This trait is object-safe. Used as `&dyn SchemaProvider` via
/// `SourceConnector::as_schema_provider()`.
#[async_trait::async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Retrieve the schema from the source itself.
    ///
    /// Called once at `CREATE SOURCE` time. The returned schema
    /// is used as-is (if authoritative) or merged with user
    /// declarations (if not).
    ///
    /// # Errors
    ///
    /// Returns `SchemaError` if the source cannot be reached or
    /// its metadata cannot be parsed into an Arrow schema.
    async fn provide_schema(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<SchemaRef>;

    /// Whether the provided schema is **authoritative** (guaranteed
    /// correct) versus **best-effort**.
    ///
    /// - `true`: Schema is guaranteed correct (e.g., Parquet metadata,
    ///   Iceberg catalog). The resolver trusts it fully.
    /// - `false`: Schema is a best guess (e.g., a single Avro message's
    ///   embedded schema may not represent all variations).
    fn is_authoritative(&self) -> bool;

    /// Provide field-level metadata beyond what Arrow captures.
    ///
    /// Returns a map from field name to `FieldMeta`. The default
    /// implementation returns an empty map.
    ///
    /// # Examples
    ///
    /// - Iceberg: field IDs, column descriptions
    /// - Avro: logical types (timestamp-millis, decimal)
    /// - Postgres: column comments, default expressions
    async fn field_metadata(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<HashMap<String, FieldMeta>> {
        Ok(HashMap::new())
    }
}
```

**Object Safety Analysis:** Object-safe. `async fn` in traits with `async_trait` desugars to `fn(...) -> Pin<Box<dyn Future>>`, which is object-safe. The default method on `field_metadata` uses no `Self` type.

### Trait 3: `SchemaInferable`

For sources that carry no schema metadata (JSON over Kafka, CSV files, WebSocket JSON streams). Schema is inferred by sampling data.

```rust
/// Configuration for sample-based schema inference.
///
/// Controls how many records to sample, how long to wait, and
/// what heuristics to apply when resolving ambiguous types.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Number of records to sample.
    pub sample_size: usize,

    /// Maximum time to spend collecting samples.
    pub sample_timeout: Duration,

    /// Default type for fields that are null in all samples.
    pub null_type: DataType,

    /// Default type for numbers (BIGINT, DOUBLE, DECIMAL).
    pub number_type: NumberInference,

    /// Timestamp format patterns to try, in priority order.
    pub timestamp_formats: Vec<String>,

    /// Maximum nesting depth to infer (beyond this, collapse
    /// to JSONB/LargeBinary).
    pub max_depth: usize,

    /// How to handle arrays: first_element, union, or jsonb.
    pub array_strategy: ArrayInference,

    /// If an object has more than this many distinct keys across
    /// samples, infer `MAP<VARCHAR, ...>` instead of `STRUCT`.
    /// Inspired by DuckDB's `map_inference_threshold`.
    pub map_threshold: usize,

    /// Field appearance threshold. If a field appears in fewer
    /// than this fraction of samples, it is marked nullable.
    pub field_appearance_threshold: f64,
}

/// How to infer numeric types.
#[derive(Debug, Clone)]
pub enum NumberInference {
    /// Default all integers to BIGINT (i64).
    BigInt,
    /// Default all numbers to DOUBLE (f64).
    Double,
    /// Default to DECIMAL(p, s).
    Decimal(u8, u8),
}

/// How to infer array element types.
#[derive(Debug, Clone)]
pub enum ArrayInference {
    /// Use the type of the first element for the entire array.
    FirstElement,
    /// Union all element types (may produce a wider type).
    Union,
    /// Give up and store as JSONB (LargeBinary).
    Jsonb,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            sample_timeout: Duration::from_secs(5),
            null_type: DataType::Utf8,
            number_type: NumberInference::Double,
            timestamp_formats: vec![
                "iso8601".into(),
                "%Y-%m-%d %H:%M:%S".into(),
                "%Y-%m-%dT%H:%M:%S%.fZ".into(),
            ],
            max_depth: 5,
            array_strategy: ArrayInference::FirstElement,
            map_threshold: 50,
            field_appearance_threshold: 0.1,
        }
    }
}

/// Result of schema inference -- includes confidence information.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// The inferred Arrow schema.
    pub schema: SchemaRef,

    /// Per-field inference details (confidence, observed types).
    pub field_details: Vec<FieldInferenceDetail>,

    /// Number of samples actually used.
    pub sample_count: usize,

    /// Warnings produced during inference (ambiguities, low
    /// confidence, type coercions).
    pub warnings: Vec<InferenceWarning>,
}

/// Per-field inference detail for diagnostics.
#[derive(Debug, Clone)]
pub struct FieldInferenceDetail {
    /// Field name.
    pub field_name: String,

    /// The inferred Arrow data type.
    pub inferred_type: DataType,

    /// Fraction of samples where this field was non-null (0.0-1.0).
    pub appearance_rate: f64,

    /// Confidence in the inferred type (0.0-1.0).
    pub confidence: f64,

    /// All distinct types observed across samples (for diagnostics).
    pub observed_types: Vec<DataType>,
}

/// A warning produced during schema inference.
#[derive(Debug, Clone)]
pub struct InferenceWarning {
    /// The field this warning relates to.
    pub field_name: String,

    /// Human-readable warning message.
    pub message: String,

    /// Severity of the warning.
    pub severity: WarningSeverity,
}

/// Severity level for inference warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningSeverity {
    /// Informational (e.g., "field `extra_data` inferred as JSONB
    /// due to > 50 distinct keys").
    Info,
    /// Warning (e.g., "field `value` has mixed types: Int64 and
    /// Float64; widened to Float64").
    Warning,
    /// Error-level (e.g., "field `payload` could not be typed;
    /// defaulted to VARCHAR").
    Error,
}

/// Sources that support schema inference by sampling data.
///
/// The framework calls `sample_records()` to get raw data, then
/// passes it to `infer_from_samples()`. This two-step design lets
/// the source handle its own I/O (Kafka consumer, WebSocket connect,
/// file read) while the inference logic is reusable across formats.
///
/// # Ring Placement
///
/// Both methods are **Ring 2** operations. `sample_records()` opens a
/// temporary connection and reads data; `infer_from_samples()` is
/// CPU-bound type inference.
///
/// # Object Safety
///
/// This trait is object-safe. Used as `&dyn SchemaInferable` via
/// `SourceConnector::as_schema_inferable()`.
#[async_trait::async_trait]
pub trait SchemaInferable: Send + Sync {
    /// Collect a sample of raw records from the source.
    ///
    /// Implementations handle their own connection and disconnection.
    /// The returned records are raw bytes -- format-specific parsing
    /// happens in `infer_from_samples()`.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError` if the source cannot be connected to
    /// or if sampling times out.
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>>;

    /// Infer Arrow schema from the sampled records.
    ///
    /// The default implementation delegates to the
    /// `FormatInferenceRegistry` for format-specific logic.
    /// Override this if your source needs custom inference
    /// (e.g., a binary format with its own type system).
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::InferenceFailed` if no inference
    /// implementation is registered for the given format.
    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema>;
}
```

**Object Safety Analysis:** Object-safe. Both async methods desugar to `Pin<Box<dyn Future>>` return types via `async_trait`. No generic parameters, no `Self`-returning methods.

### Trait 4: `SchemaRegistryAware`

For sources/sinks that integrate with an external schema registry (Confluent Schema Registry, AWS Glue Schema Registry).

```rust
/// Schema registry configuration.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Registry URL (e.g., `"http://schema-registry:8081"`).
    pub url: String,

    /// Subject name (e.g., `"trades-value"`).
    /// If `None`, derived from the topic name: `"{topic}-value"`.
    pub subject: Option<String>,

    /// Schema type stored in the registry.
    pub schema_type: RegistrySchemaType,

    /// Compatibility mode for evolution checks.
    pub compatibility: CompatibilityMode,

    /// Credentials for authenticated registries.
    pub credentials: Option<RegistryCredentials>,
}

/// Schema type supported by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrySchemaType {
    /// Apache Avro.
    Avro,
    /// Protocol Buffers.
    Protobuf,
    /// JSON Schema.
    JsonSchema,
}

/// Compatibility mode for schema evolution.
///
/// Follows the Confluent Schema Registry compatibility levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityMode {
    /// No compatibility checking.
    None,
    /// New schema can read data written by old schema.
    Backward,
    /// Old schema can read data written by new schema.
    Forward,
    /// Both backward and forward compatible.
    Full,
    /// Backward compatible with all previous versions.
    BackwardTransitive,
    /// Forward compatible with all previous versions.
    ForwardTransitive,
    /// Fully compatible with all previous versions.
    FullTransitive,
}

/// Credentials for authenticating with a schema registry.
#[derive(Debug, Clone)]
pub struct RegistryCredentials {
    /// Username or API key.
    pub username: String,
    /// Password or API secret.
    pub password: String,
}

/// A resolved schema from a registry, with version tracking.
#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    /// The resolved Arrow schema.
    pub schema: SchemaRef,

    /// Registry-assigned schema ID (e.g., the 4-byte Confluent ID
    /// in the wire format header).
    pub schema_id: u32,

    /// Schema version number in the registry.
    pub version: u32,

    /// Raw schema definition (Avro JSON string, Protobuf
    /// descriptor bytes, JSON Schema document, etc.).
    pub raw_schema: Vec<u8>,

    /// Subject this schema belongs to.
    pub subject: String,
}

/// Sources/sinks that integrate with an external schema registry.
///
/// This trait enables:
/// 1. Fetching the latest schema at `CREATE SOURCE` time.
/// 2. Looking up schemas by ID when encountering new wire-format
///    headers (e.g., Confluent's `[0x00, schema_id: 4 bytes]`).
/// 3. Compatibility checking before schema evolution.
/// 4. Schema registration for sinks writing to registry-backed topics.
///
/// # Ring Placement
///
/// All methods are **Ring 2** operations (HTTP calls to the registry).
///
/// # Object Safety
///
/// This trait is object-safe. Used as `&dyn SchemaRegistryAware` via
/// `SourceConnector::as_schema_registry_aware()`.
#[async_trait::async_trait]
pub trait SchemaRegistryAware: Send + Sync {
    /// Fetch the latest schema from the registry for the configured
    /// subject.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::RegistryError` if the registry is
    /// unreachable or the subject does not exist.
    async fn fetch_schema(
        &self,
        registry_config: &RegistryConfig,
    ) -> SchemaResult<RegisteredSchema>;

    /// Fetch a specific schema version by its registry-assigned ID.
    ///
    /// Used when the decoder encounters a record with a schema ID
    /// that is not in the local cache.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::RegistryError` if the ID is unknown.
    async fn fetch_schema_by_id(
        &self,
        registry_config: &RegistryConfig,
        schema_id: u32,
    ) -> SchemaResult<RegisteredSchema>;

    /// Check if a proposed schema is compatible with the current
    /// one, according to the registry's compatibility mode.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if compatible, `Ok(false)` if not.
    async fn check_compatibility(
        &self,
        registry_config: &RegistryConfig,
        current: &RegisteredSchema,
        proposed: &SchemaRef,
    ) -> SchemaResult<bool>;

    /// Register a new schema in the registry.
    ///
    /// Used by sinks that write to registry-backed topics and need
    /// to register output schemas.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::RegistryError` if registration fails
    /// (e.g., compatibility violation).
    async fn register_schema(
        &self,
        registry_config: &RegistryConfig,
        schema: &SchemaRef,
    ) -> SchemaResult<RegisteredSchema>;

    /// Build a decoder that can handle schema-ID headers.
    ///
    /// For the Confluent wire format, this builds a decoder that
    /// reads `[0x00, 4-byte schema_id, payload]` and uses the
    /// registered schema to decode the payload.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError` if the decoder cannot be constructed
    /// for the registered schema.
    fn build_registry_decoder(
        &self,
        registered: &RegisteredSchema,
    ) -> SchemaResult<Box<dyn FormatDecoder>>;
}
```

**Object Safety Analysis:** Object-safe. The `build_registry_decoder` method returns `Box<dyn FormatDecoder>` which is fine for object safety. All async methods desugar via `async_trait`.

### Trait 5: `SchemaEvolvable`

For sources/sinks that handle schema changes over time without downtime.

```rust
/// Describes a single schema change.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// A new column was added.
    ColumnAdded {
        /// Column name.
        name: String,
        /// Arrow data type.
        data_type: DataType,
        /// Whether the column is nullable.
        nullable: bool,
        /// Default value expression (SQL), if any.
        default: Option<String>,
    },
    /// A column was removed.
    ColumnDropped {
        /// The dropped column name.
        name: String,
    },
    /// A column was renamed.
    ColumnRenamed {
        /// Original column name.
        old_name: String,
        /// New column name.
        new_name: String,
    },
    /// A column's type was widened (e.g., Int32 -> Int64).
    TypeWidened {
        /// Column name.
        name: String,
        /// Original type.
        from: DataType,
        /// New (wider) type.
        to: DataType,
    },
    /// A column's type was narrowed (e.g., Int64 -> Int32).
    /// This is typically rejected as a breaking change.
    TypeNarrowed {
        /// Column name.
        name: String,
        /// Original type.
        from: DataType,
        /// New (narrower) type.
        to: DataType,
    },
}

/// Result of evaluating a set of schema changes.
#[derive(Debug, Clone)]
pub enum EvolutionVerdict {
    /// All changes are safe and can be applied automatically.
    Compatible(Vec<SchemaChange>),

    /// Changes require user confirmation (e.g., type narrowing
    /// with potential data loss).
    RequiresConfirmation(Vec<SchemaChange>, String),

    /// Changes are rejected as incompatible.
    Incompatible(String),
}

/// Maps old column positions to new positions after evolution.
///
/// Used to build a projection that adapts old-schema records
/// to the new schema layout.
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    /// For each column in the **new** schema, where it came from
    /// in the **old** schema. `None` means the column is newly
    /// added and should be filled with the default value or NULL.
    pub mappings: Vec<Option<usize>>,
}

/// Sources/sinks that support schema evolution.
///
/// Enables detecting, evaluating, and applying schema changes
/// without pipeline downtime. The evolution flow is:
///
/// 1. Detect a schema change (new registry ID, altered catalog, etc.)
/// 2. Call `diff_schemas()` to compute the set of changes.
/// 3. Call `evaluate_evolution()` to check compatibility.
/// 4. If compatible, call `apply_evolution()` to get the new schema
///    and a projection map.
/// 5. Atomically swap the decoder in Ring 1.
///
/// # Ring Placement
///
/// All methods are **Ring 2** operations (pure computation, no I/O).
///
/// # Object Safety
///
/// This trait is object-safe.
#[async_trait::async_trait]
pub trait SchemaEvolvable: Send + Sync {
    /// Compute the diff between the current and proposed schema.
    ///
    /// Returns a list of `SchemaChange` values describing every
    /// difference.
    fn diff_schemas(
        &self,
        current: &SchemaRef,
        proposed: &SchemaRef,
    ) -> Vec<SchemaChange>;

    /// Evaluate whether a set of changes is compatible with the
    /// given compatibility mode.
    ///
    /// Returns an `EvolutionVerdict` indicating whether the changes
    /// can be applied automatically, require confirmation, or are
    /// rejected.
    fn evaluate_evolution(
        &self,
        changes: &[SchemaChange],
        compatibility: CompatibilityMode,
    ) -> EvolutionVerdict;

    /// Apply schema evolution.
    ///
    /// Returns the new merged schema and a `ColumnProjection` that
    /// maps old column indices to new column indices.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::EvolutionRejected` if the changes
    /// cannot be applied (e.g., conflicting type changes).
    fn apply_evolution(
        &self,
        current: &SchemaRef,
        changes: &[SchemaChange],
    ) -> SchemaResult<(SchemaRef, ColumnProjection)>;

    /// Returns the current schema version number.
    ///
    /// Monotonically increasing. Starts at 1 for the initial schema.
    fn schema_version(&self) -> u64;
}
```

**Object Safety Analysis:** Object-safe. All methods take references and return owned types. No generics, no `Sized` constraints.

### `SourceConnector` / `SinkConnector` with Capability Discovery

These extend the existing F034 traits with `as_*()` methods for capability discovery. All new methods have default implementations returning `None`, so **existing connector implementations remain valid without changes**.

```rust
/// The top-level trait for all source connectors.
///
/// Every source implements this. The five capability traits
/// (`SchemaProvider`, `SchemaInferable`, `SchemaRegistryAware`,
/// `SchemaEvolvable`, `FormatDecoder`) are discovered via the
/// `as_*()` methods.
///
/// This extends the F034 `SourceConnector` by adding:
/// - Capability discovery methods (all default to `None`)
/// - `build_decoder()` for creating format decoders
/// - `supported_formats()` for advertising format support
///
/// # Backward Compatibility
///
/// All new methods have default implementations. Existing F034
/// connector implementations compile unchanged.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync + std::fmt::Debug {
    /// Unique name of this connector type (e.g., `"kafka"`,
    /// `"websocket"`, `"file"`, `"iceberg"`).
    fn connector_type(&self) -> &str;

    /// Validate the source configuration.
    ///
    /// Called at `CREATE SOURCE` time before any other method.
    /// Should check that all required config keys are present
    /// and have valid values.
    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()>;

    /// Build the `FormatDecoder` for this source + format combination.
    ///
    /// Called after the `SchemaResolver` has produced the final
    /// frozen schema. The decoder is used for the lifetime of the
    /// source in Ring 1.
    fn build_decoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>>;

    /// Start producing records.
    ///
    /// Returns a boxed `RecordStream` that yields `RawRecord`s.
    /// The stream runs in Ring 1 and pushes decoded batches into
    /// Ring 0 via SPSC channels.
    async fn start(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn RecordStream>>;

    // -- Capability discovery ------------------------------------

    /// Does this source carry its own schema?
    ///
    /// Returns `Some(&dyn SchemaProvider)` if the source can
    /// provide its own schema from metadata (e.g., Parquet,
    /// Iceberg, Postgres CDC).
    fn as_schema_provider(&self) -> Option<&dyn SchemaProvider> {
        None
    }

    /// Can this source infer schema from data samples?
    ///
    /// Returns `Some(&dyn SchemaInferable)` if the source can
    /// sample records for inference (e.g., Kafka with JSON,
    /// WebSocket, CSV files).
    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        None
    }

    /// Does this source integrate with an external schema registry?
    ///
    /// Returns `Some(&dyn SchemaRegistryAware)` if the source
    /// works with a registry (e.g., Kafka + Confluent SR).
    fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> {
        None
    }

    /// Does this source support schema evolution?
    ///
    /// Returns `Some(&dyn SchemaEvolvable)` if the source can
    /// handle schema changes at runtime (e.g., Kafka + Avro,
    /// Iceberg, Delta Lake).
    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        None
    }

    /// List the serialization formats this connector supports.
    ///
    /// Used by SQL validation to reject unsupported format
    /// combinations at `CREATE SOURCE` time.
    fn supported_formats(&self) -> &[&str];
}

/// The top-level trait for all sink connectors.
///
/// Parallel to `SourceConnector`, with capability discovery for
/// encoding, registry integration, and evolution.
#[async_trait::async_trait]
pub trait SinkConnector: Send + Sync + std::fmt::Debug {
    /// Unique name of this connector type.
    fn connector_type(&self) -> &str;

    /// Validate the sink configuration.
    async fn validate_config(&self, config: &SinkConfig) -> SchemaResult<()>;

    /// Build the `FormatEncoder` for this sink + format combination.
    fn build_encoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn FormatEncoder>>;

    /// Start the sink for writing.
    async fn start(
        &self,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn RecordSink>>;

    // -- Capability discovery ------------------------------------

    /// Does this sink integrate with an external schema registry?
    fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> {
        None
    }

    /// Does this sink support schema evolution?
    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        None
    }

    /// Supported serialization formats.
    fn supported_formats(&self) -> &[&str];
}
```

**Object Safety Analysis:** Both traits are object-safe. The `as_*()` methods return `Option<&dyn Trait>` which is fine. The `supported_formats()` method returns `&[&str]` which is also safe.

### `SourceConfig` / `SinkConfig`

Typed configuration accessors that replace the raw `HashMap<String, String>` from F034's `ConnectorConfig`.

```rust
/// Typed configuration for a source connector.
///
/// Wraps a `HashMap<String, String>` with typed accessor methods
/// and required-key validation. Created from the SQL `CREATE SOURCE`
/// `WITH (...)` clause or `FROM ... (...)` clause.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Connector type name (e.g., `"kafka"`).
    pub connector_type: String,

    /// Data format (e.g., `"json"`, `"avro"`, `"csv"`).
    pub format: String,

    /// Raw key-value options.
    options: HashMap<String, String>,
}

impl SourceConfig {
    /// Creates a new `SourceConfig`.
    pub fn new(
        connector_type: impl Into<String>,
        format: impl Into<String>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            connector_type: connector_type.into(),
            format: format.into(),
            options,
        }
    }

    /// Gets a required option, returning `SchemaError::MissingConfig`
    /// if it is absent.
    pub fn get_required(&self, key: &str) -> SchemaResult<&str> {
        self.options
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| SchemaError::MissingConfig(key.to_string()))
    }

    /// Gets an optional option.
    pub fn get_optional(&self, key: &str) -> Option<&str> {
        self.options.get(key).map(String::as_str)
    }

    /// Gets an option parsed to type `T`.
    ///
    /// Returns `None` if the key is absent. Returns
    /// `SchemaError::InvalidConfig` if parsing fails.
    pub fn get_typed<T: std::str::FromStr>(
        &self,
        key: &str,
    ) -> SchemaResult<Option<T>>
    where
        T::Err: std::fmt::Display,
    {
        match self.options.get(key) {
            None => Ok(None),
            Some(v) => v.parse().map(Some).map_err(|e| {
                SchemaError::InvalidConfig {
                    key: key.to_string(),
                    message: e.to_string(),
                }
            }),
        }
    }

    /// Gets a required option parsed to type `T`.
    pub fn require_typed<T: std::str::FromStr>(
        &self,
        key: &str,
    ) -> SchemaResult<T>
    where
        T::Err: std::fmt::Display,
    {
        let value = self.get_required(key)?;
        value.parse().map_err(|e: T::Err| {
            SchemaError::InvalidConfig {
                key: key.to_string(),
                message: e.to_string(),
            }
        })
    }

    /// Returns a reference to all raw options.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

/// Typed configuration for a sink connector.
///
/// Mirrors `SourceConfig` for sinks.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Connector type name.
    pub connector_type: String,
    /// Data format.
    pub format: String,
    /// Raw key-value options.
    options: HashMap<String, String>,
}

// SinkConfig has the same accessor methods as SourceConfig.
// (Implementation identical, omitted for brevity.)
```

### `ConnectorConfigSchema` -- Self-Documenting Configuration

```rust
/// Describes the configuration options a connector accepts.
///
/// Used by `DESCRIBE CONNECTOR` to show available options,
/// by SQL validation to catch typos, and by the admin UI
/// to render connector configuration forms.
#[derive(Debug, Clone)]
pub struct ConnectorConfigSchema {
    /// Required configuration keys.
    pub required: Vec<ConfigOption>,
    /// Optional configuration keys.
    pub optional: Vec<ConfigOption>,
}

/// A single configuration option.
#[derive(Debug, Clone)]
pub struct ConfigOption {
    /// Key name (e.g., `"brokers"`, `"topic"`).
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Expected value type (for documentation).
    pub value_type: ConfigValueType,
    /// Default value, if any.
    pub default: Option<String>,
    /// Example value.
    pub example: Option<String>,
}

/// Type hint for configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigValueType {
    /// A string value.
    String,
    /// An integer value.
    Integer,
    /// A boolean value.
    Boolean,
    /// A duration (e.g., `"5s"`, `"100ms"`).
    Duration,
    /// A URL.
    Url,
    /// A comma-separated list.
    List,
    /// A secret (masked in logs).
    Secret,
}

impl ConnectorConfigSchema {
    /// Creates a new empty config schema.
    pub fn new() -> Self {
        Self {
            required: Vec::new(),
            optional: Vec::new(),
        }
    }

    /// Adds a required config option (builder pattern).
    pub fn required(
        mut self,
        name: &str,
        description: &str,
    ) -> Self {
        self.required.push(ConfigOption {
            name: name.to_string(),
            description: description.to_string(),
            value_type: ConfigValueType::String,
            default: None,
            example: None,
        });
        self
    }

    /// Adds an optional config option (builder pattern).
    pub fn optional(
        mut self,
        name: &str,
        description: &str,
    ) -> Self {
        self.optional.push(ConfigOption {
            name: name.to_string(),
            description: description.to_string(),
            value_type: ConfigValueType::String,
            default: None,
            example: None,
        });
        self
    }

    /// Adds a required config option with a value type.
    pub fn required_typed(
        mut self,
        name: &str,
        description: &str,
        value_type: ConfigValueType,
    ) -> Self {
        self.required.push(ConfigOption {
            name: name.to_string(),
            description: description.to_string(),
            value_type,
            default: None,
            example: None,
        });
        self
    }
}
```

### `ConnectorRegistry` and Factory Traits

```rust
/// Factory trait for creating source connectors from DDL configuration.
///
/// Each connector type registers a factory. The `ConnectorRegistry`
/// looks up the factory by connector type name from the SQL DDL.
pub trait SourceConnectorFactory: Send + Sync {
    /// The connector type name used in SQL (e.g., `"kafka"`,
    /// `"websocket"`, `"nats"`).
    fn connector_type(&self) -> &str;

    /// Create a new source connector instance from configuration.
    ///
    /// Called once per `CREATE SOURCE` statement.
    fn create(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn SourceConnector>>;

    /// Describe the configuration this connector accepts.
    ///
    /// Used by `DESCRIBE CONNECTOR 'kafka'` and SQL validation.
    fn config_schema(&self) -> ConnectorConfigSchema;
}

/// Factory trait for creating sink connectors.
pub trait SinkConnectorFactory: Send + Sync {
    /// The connector type name.
    fn connector_type(&self) -> &str;

    /// Create a new sink connector instance.
    fn create(
        &self,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn SinkConnector>>;

    /// Describe the configuration this connector accepts.
    fn config_schema(&self) -> ConnectorConfigSchema;
}

/// Registry of all available connector factories.
///
/// Maps connector type names (from SQL `FROM KAFKA (...)` or
/// `INTO KAFKA (...)`) to factory instances. Populated at startup
/// with built-in connectors; third-party connectors register via
/// `register_source()` / `register_sink()`.
pub struct ConnectorRegistry {
    sources: HashMap<String, Box<dyn SourceConnectorFactory>>,
    sinks: HashMap<String, Box<dyn SinkConnectorFactory>>,
}

impl ConnectorRegistry {
    /// Creates a new registry with built-in connectors.
    pub fn new() -> Self {
        let mut reg = Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        };
        // Built-in connectors are registered here.
        // (Actual registrations depend on feature flags.)
        reg
    }

    /// Registers a source connector factory.
    pub fn register_source(
        &mut self,
        factory: Box<dyn SourceConnectorFactory>,
    ) {
        self.sources.insert(
            factory.connector_type().to_string(),
            factory,
        );
    }

    /// Registers a sink connector factory.
    pub fn register_sink(
        &mut self,
        factory: Box<dyn SinkConnectorFactory>,
    ) {
        self.sinks.insert(
            factory.connector_type().to_string(),
            factory,
        );
    }

    /// Creates a source connector by type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the connector type is unknown or if
    /// the factory rejects the configuration.
    pub fn create_source(
        &self,
        connector_type: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn SourceConnector>> {
        self.sources
            .get(connector_type)
            .ok_or_else(|| {
                SchemaError::Other(anyhow::anyhow!(
                    "Unknown source connector: '{connector_type}'. \
                     Available: {:?}",
                    self.sources.keys().collect::<Vec<_>>()
                ))
            })?
            .create(config)
    }

    /// Creates a sink connector by type name.
    pub fn create_sink(
        &self,
        connector_type: &str,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn SinkConnector>> {
        self.sinks
            .get(connector_type)
            .ok_or_else(|| {
                SchemaError::Other(anyhow::anyhow!(
                    "Unknown sink connector: '{connector_type}'. \
                     Available: {:?}",
                    self.sinks.keys().collect::<Vec<_>>()
                ))
            })?
            .create(config)
    }

    /// Lists all registered source connector types with their
    /// configuration schemas.
    pub fn list_source_types(&self) -> Vec<(&str, ConnectorConfigSchema)> {
        self.sources
            .values()
            .map(|f| (f.connector_type(), f.config_schema()))
            .collect()
    }

    /// Lists all registered sink connector types.
    pub fn list_sink_types(&self) -> Vec<(&str, ConnectorConfigSchema)> {
        self.sinks
            .values()
            .map(|f| (f.connector_type(), f.config_schema()))
            .collect()
    }
}
```

### `FormatInferenceRegistry`

Pluggable per-format inference logic, separate from the decode hot path.

```rust
/// Format-specific inference logic.
///
/// Separate from `FormatDecoder` because inference is a one-time
/// Ring 2 operation, while decoding is the continuous Ring 1 hot path.
pub trait FormatInference: Send + Sync {
    /// The format name this inferrer handles (e.g., `"json"`, `"csv"`).
    fn format_name(&self) -> &str;

    /// Infer a schema from raw record samples.
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema>;
}

/// Registry of format inference implementations.
///
/// Thread-safe: populated at startup, read-only afterwards.
pub struct FormatInferenceRegistry {
    inferrers: HashMap<String, Box<dyn FormatInference>>,
}

impl FormatInferenceRegistry {
    /// Creates a new registry with built-in format inferrers.
    pub fn new() -> Self {
        Self {
            inferrers: HashMap::new(),
        }
    }

    /// Registers a format inference implementation.
    pub fn register(&mut self, inferrer: Box<dyn FormatInference>) {
        self.inferrers.insert(
            inferrer.format_name().to_string(),
            inferrer,
        );
    }

    /// Looks up the inferrer for a given format name.
    pub fn get(&self, format: &str) -> Option<&dyn FormatInference> {
        self.inferrers.get(format).map(|b| b.as_ref())
    }
}
```

### Object Safety Summary

| Trait | Object-Safe | Reason |
|-------|-------------|--------|
| `FormatDecoder` | Yes | All methods take `&self`, return owned/ref types |
| `FormatEncoder` | Yes | All methods take `&self`, return owned/ref types |
| `SchemaProvider` | Yes | `async_trait` desugars to `Pin<Box<dyn Future>>` |
| `SchemaInferable` | Yes | `async_trait` desugars to `Pin<Box<dyn Future>>` |
| `SchemaRegistryAware` | Yes | `async_trait`; `build_registry_decoder` returns `Box<dyn>` |
| `SchemaEvolvable` | Yes | Sync methods, all take `&self` |
| `SourceConnector` | Yes | `as_*()` returns `Option<&dyn Trait>` |
| `SinkConnector` | Yes | `as_*()` returns `Option<&dyn Trait>` |
| `SourceConnectorFactory` | Yes | Returns `Box<dyn SourceConnector>` |
| `SinkConnectorFactory` | Yes | Returns `Box<dyn SinkConnector>` |
| `FormatInference` | Yes | All methods take `&self` |

---

## SQL Interface

### `CREATE SOURCE` Mapping

```sql
-- Mode 1: Explicit schema (no trait invocation except build_decoder)
CREATE SOURCE market_data (
    symbol      VARCHAR NOT NULL,
    price       DOUBLE NOT NULL,
    quantity    BIGINT NOT NULL,
    timestamp   TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers     = 'broker1:9092',
    topic       = 'trades'
)
FORMAT JSON;
```

Mapping:
1. `FROM KAFKA (...)` -> `ConnectorRegistry::create_source("kafka", config)`
2. `FORMAT JSON` -> stored in `SourceConfig.format`
3. `(symbol VARCHAR NOT NULL, ...)` -> `DeclaredSchema`
4. `SchemaResolver::resolve(declared, source, config, "json", ...)` -> uses `FullyDeclared` path
5. `source.build_decoder(resolved_schema, "json", config)` -> `Box<dyn FormatDecoder>`

```sql
-- Mode 2: Schema registry (SchemaRegistryAware invoked)
CREATE SOURCE market_data
FROM KAFKA (
    brokers = 'broker1:9092',
    topic   = 'trades'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url       = 'http://schema-registry:8081',
    subject   = 'trades-value',
    evolution = 'forward_compatible'
);
```

Mapping:
1. `ConnectorRegistry::create_source("kafka", config)`
2. `USING SCHEMA REGISTRY (...)` -> `RegistryConfig`
3. `source.as_schema_registry_aware()` -> `Some(&dyn SchemaRegistryAware)`
4. `SchemaResolver::resolve(None, source, config, "avro", None, Some(registry_config))`
5. Resolution: `fetch_schema(registry_config)` -> `RegisteredSchema`
6. `source.build_decoder(registered.schema, "avro", config)` or `build_registry_decoder(registered)`

```sql
-- Mode 3: Auto-inference (SchemaInferable invoked)
CREATE SOURCE raw_events
FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'events'
)
FORMAT JSON
INFER SCHEMA (
    sample_size    = 1000,
    sample_timeout = '5s',
    null_as        = 'VARCHAR',
    number_as      = 'DOUBLE'
);
```

Mapping:
1. `ConnectorRegistry::create_source("kafka", config)`
2. `INFER SCHEMA (...)` -> `InferenceConfig`
3. `source.as_schema_inferable()` -> `Some(&dyn SchemaInferable)`
4. `SchemaResolver::resolve(None, source, config, "json", Some(inference_config), None)`
5. Resolution: `sample_records(config, inference_config)` -> `Vec<RawRecord>`
6. `infer_from_samples(samples, "json", inference_config)` -> `InferredSchema`

```sql
-- Mode 4: Partial schema + wildcard (hybrid)
CREATE SOURCE sensor_data (
    device_id   VARCHAR NOT NULL,
    temperature DOUBLE,
    *
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'sensors'
)
FORMAT JSON
INFER SCHEMA (
    sample_size     = 500,
    wildcard_prefix = 'extra_'
);
```

Mapping:
1. User-declared columns -> `DeclaredSchema { columns: [...], has_wildcard: true }`
2. `SchemaResolver::resolve(Some(declared), ...)` detects wildcard
3. Infers remaining fields via `SchemaInferable`
4. Merges: user-declared columns take priority, inferred fields get `extra_` prefix

### `CREATE SINK` Mapping

```sql
CREATE SINK alerts INTO KAFKA (
    brokers = 'broker1:9092',
    topic   = 'alerts'
)
FORMAT AVRO
USING SCHEMA REGISTRY (
    url = 'http://schema-registry:8081'
)
FROM alert_view;
```

Mapping:
1. `ConnectorRegistry::create_sink("kafka", config)`
2. `sink.as_schema_registry_aware()` -> `Some(&dyn SchemaRegistryAware)`
3. `register_schema(registry_config, output_schema)` -> register the output schema
4. `sink.build_encoder(schema, "avro", config)` -> `Box<dyn FormatEncoder>`

---

## Concrete Examples

### Example 1: WebSocket -- Minimal Connector (FormatDecoder + SchemaInferable)

WebSocket sources only support JSON/raw formats and sample-based inference. No registry, no self-describing schema, no evolution.

```rust
#[derive(Debug)]
pub struct WebSocketSource;

#[async_trait]
impl SourceConnector for WebSocketSource {
    fn connector_type(&self) -> &str { "websocket" }

    async fn validate_config(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<()> {
        config.get_required("url")?;
        Ok(())
    }

    fn build_decoder(
        &self,
        schema: SchemaRef,
        format: &str,
        _config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        match format {
            "json" => Ok(Box::new(JsonDecoder::new(schema))),
            "raw"  => Ok(Box::new(RawDecoder::new(schema))),
            _ => Err(SchemaError::Other(anyhow::anyhow!(
                "WebSocket only supports json/raw, got '{format}'"
            ))),
        }
    }

    async fn start(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn RecordStream>> {
        Ok(Box::new(
            WebSocketRecordStream::connect(config).await?,
        ))
    }

    // Only inference -- no registry, no self-describing, no evolution
    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        Some(self)
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "raw"]
    }
}

#[async_trait]
impl SchemaInferable for WebSocketSource {
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>> {
        let ws = WebSocketClient::connect(
            config.get_required("url")?,
        ).await?;
        let samples = ws
            .receive_n(
                inference_config.sample_size,
                inference_config.sample_timeout,
            )
            .await?;
        ws.close().await?;
        Ok(samples)
    }

    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        // Delegate to the global format inference registry
        let registry = FormatInferenceRegistry::global();
        let inferrer = registry.get(format).ok_or_else(|| {
            SchemaError::InferenceFailed(format!(
                "No inference support for format '{format}'"
            ))
        })?;
        inferrer.infer(samples, inference_config)
    }
}
```

**Trait composition:** `SourceConnector` + `SchemaInferable` (2 of 5 traits).

### Example 2: Kafka -- Full-Featured Connector (All Five Traits)

Kafka implements nearly all capability traits because it supports multiple formats, schema registry integration, sample-based inference, and schema evolution.

```rust
#[derive(Debug)]
pub struct KafkaSource {
    config: KafkaConfig,
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn connector_type(&self) -> &str { "kafka" }

    async fn validate_config(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<()> {
        config.get_required("brokers")?;
        config.get_required("topic")?;
        Ok(())
    }

    fn build_decoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        match format {
            "json" => Ok(Box::new(JsonDecoder::new(schema))),
            "csv"  => Ok(Box::new(CsvDecoder::new(schema, config)?)),
            "avro" => Ok(Box::new(AvroDecoder::new(schema)?)),
            "raw"  => Ok(Box::new(RawDecoder::new(schema))),
            _ => Err(SchemaError::Other(anyhow::anyhow!(
                "Unsupported format: {format}"
            ))),
        }
    }

    async fn start(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn RecordStream>> {
        Ok(Box::new(
            KafkaRecordStream::connect(&self.config).await?,
        ))
    }

    // Kafka supports inference (JSON/CSV), registry (Avro),
    // and evolution (via registry)
    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        Some(self)
    }

    fn as_schema_registry_aware(
        &self,
    ) -> Option<&dyn SchemaRegistryAware> {
        Some(self)
    }

    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        Some(self)
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "avro", "protobuf", "raw"]
    }
}

#[async_trait]
impl SchemaInferable for KafkaSource {
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>> {
        // Create a temporary consumer, read N messages, disconnect
        let consumer = create_temp_consumer(config).await?;
        let samples = consumer
            .consume_n(
                inference_config.sample_size,
                inference_config.sample_timeout,
            )
            .await?;
        consumer.disconnect().await?;
        Ok(samples)
    }

    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let registry = FormatInferenceRegistry::global();
        let inferrer = registry.get(format).ok_or_else(|| {
            SchemaError::InferenceFailed(format!(
                "No inference support for format '{format}'"
            ))
        })?;
        inferrer.infer(samples, inference_config)
    }
}

#[async_trait]
impl SchemaRegistryAware for KafkaSource {
    async fn fetch_schema(
        &self,
        registry_config: &RegistryConfig,
    ) -> SchemaResult<RegisteredSchema> {
        let client = SchemaRegistryClient::new(
            &registry_config.url,
        )?;
        let subject = registry_config
            .subject
            .as_deref()
            .unwrap_or(&format!("{}-value", self.config.topic));
        let schema = client.get_latest(subject).await?;
        Ok(convert_to_registered(schema)?)
    }

    async fn fetch_schema_by_id(
        &self,
        registry_config: &RegistryConfig,
        schema_id: u32,
    ) -> SchemaResult<RegisteredSchema> {
        let client = SchemaRegistryClient::new(
            &registry_config.url,
        )?;
        let schema = client.get_by_id(schema_id).await?;
        Ok(convert_to_registered(schema)?)
    }

    async fn check_compatibility(
        &self,
        registry_config: &RegistryConfig,
        current: &RegisteredSchema,
        proposed: &SchemaRef,
    ) -> SchemaResult<bool> {
        let client = SchemaRegistryClient::new(
            &registry_config.url,
        )?;
        client
            .test_compatibility(&current.subject, proposed)
            .await
    }

    async fn register_schema(
        &self,
        registry_config: &RegistryConfig,
        schema: &SchemaRef,
    ) -> SchemaResult<RegisteredSchema> {
        let client = SchemaRegistryClient::new(
            &registry_config.url,
        )?;
        let subject = registry_config
            .subject
            .as_deref()
            .unwrap_or(&format!("{}-value", self.config.topic));
        client.register(subject, schema).await
    }

    fn build_registry_decoder(
        &self,
        registered: &RegisteredSchema,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        Ok(Box::new(ConfluentAvroDecoder::new(registered)?))
    }
}

#[async_trait]
impl SchemaEvolvable for KafkaSource {
    fn diff_schemas(
        &self,
        current: &SchemaRef,
        proposed: &SchemaRef,
    ) -> Vec<SchemaChange> {
        // Compare field-by-field, detect adds/drops/renames/type changes
        compute_schema_diff(current, proposed)
    }

    fn evaluate_evolution(
        &self,
        changes: &[SchemaChange],
        compatibility: CompatibilityMode,
    ) -> EvolutionVerdict {
        evaluate_compatibility(changes, compatibility)
    }

    fn apply_evolution(
        &self,
        current: &SchemaRef,
        changes: &[SchemaChange],
    ) -> SchemaResult<(SchemaRef, ColumnProjection)> {
        apply_schema_changes(current, changes)
    }

    fn schema_version(&self) -> u64 {
        // Tracked via registry version
        self.current_schema_version.load(Ordering::Relaxed)
    }
}
```

**Trait composition:** `SourceConnector` + `SchemaInferable` + `SchemaRegistryAware` + `SchemaEvolvable` (4 of 5 capability traits; `SchemaProvider` is not implemented because Kafka is not self-describing).

### Example 3: NATS -- Third-Party Extension Workflow

A third-party developer adds NATS JetStream support in their own crate `laminardb-connector-nats`. They depend only on `laminar-connector-api`, not on the full `laminar-connectors` crate.

```toml
# Cargo.toml for laminardb-connector-nats
[dependencies]
laminar-connector-api = "0.1"   # Only the trait crate
async-nats = "0.35"
tokio = { version = "1", features = ["macros", "time"] }
async-trait = "0.1"
```

```rust
use laminar_connector_api::*;

#[derive(Debug)]
pub struct NatsSource;

#[async_trait]
impl SourceConnector for NatsSource {
    fn connector_type(&self) -> &str { "nats" }

    async fn validate_config(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<()> {
        config.get_required("url")?;
        config.get_required("subject")?;
        Ok(())
    }

    fn build_decoder(
        &self,
        schema: SchemaRef,
        format: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        // Reuse LaminarDB's built-in decoders via the format
        // decoder registry
        laminar_connector_api::formats::build_decoder(
            format, schema, config,
        )
    }

    async fn start(
        &self,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn RecordStream>> {
        let client = async_nats::connect(
            config.get_required("url")?,
        ).await
        .map_err(|e| SchemaError::Other(e.into()))?;

        let subscriber = client
            .subscribe(config.get_required("subject")?.to_string())
            .await
            .map_err(|e| SchemaError::Other(e.into()))?;

        Ok(Box::new(NatsRecordStream::new(subscriber)))
    }

    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        Some(self)
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "avro", "raw"]
    }
}

#[async_trait]
impl SchemaInferable for NatsSource {
    async fn sample_records(
        &self,
        config: &SourceConfig,
        inf: &InferenceConfig,
    ) -> SchemaResult<Vec<RawRecord>> {
        let client = async_nats::connect(
            config.get_required("url")?,
        ).await
        .map_err(|e| SchemaError::Other(e.into()))?;

        let mut sub = client
            .subscribe(
                config.get_required("subject")?.to_string(),
            )
            .await
            .map_err(|e| SchemaError::Other(e.into()))?;

        let mut samples = Vec::with_capacity(inf.sample_size);
        let deadline = std::time::Instant::now()
            + inf.sample_timeout;

        while samples.len() < inf.sample_size
            && std::time::Instant::now() < deadline
        {
            match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                sub.next(),
            )
            .await
            {
                Ok(Some(msg)) => {
                    samples.push(RawRecord {
                        key: None,
                        value: msg.payload.to_vec(),
                        timestamp: None,
                        headers: vec![],
                        metadata: SourceMetadata::empty(),
                    });
                }
                _ => continue,
            }
        }
        Ok(samples)
    }

    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        format: &str,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let registry = FormatInferenceRegistry::global();
        let inferrer = registry.get(format).ok_or_else(|| {
            SchemaError::InferenceFailed(format!(
                "No inference support for format '{format}'"
            ))
        })?;
        inferrer.infer(samples, inference_config)
    }
}

// Factory for registration:
pub struct NatsSourceFactory;

impl SourceConnectorFactory for NatsSourceFactory {
    fn connector_type(&self) -> &str { "nats" }

    fn create(
        &self,
        _config: &SourceConfig,
    ) -> SchemaResult<Box<dyn SourceConnector>> {
        Ok(Box::new(NatsSource))
    }

    fn config_schema(&self) -> ConnectorConfigSchema {
        ConnectorConfigSchema::new()
            .required("url", "NATS server URL")
            .required("subject", "NATS subject to subscribe to")
            .optional("queue_group", "Consumer queue group name")
            .optional("durable_name", "Durable consumer name")
    }
}
```

Registration at application startup:

```rust
registry.register_source(Box::new(NatsSourceFactory));
```

The connector is immediately usable in SQL:

```sql
CREATE SOURCE nats_events (
    event_type VARCHAR,
    payload    JSONB,
    ts         TIMESTAMP
) FROM NATS (
    url     = 'nats://localhost:4222',
    subject = 'events.>'
)
FORMAT JSON;
```

### Trait Composition Matrix

| Source | FormatDecoder | SchemaProvider | SchemaInferable | SchemaRegistryAware | SchemaEvolvable |
|--------|:---:|:---:|:---:|:---:|:---:|
| **Kafka** | Y | - | Y (JSON/CSV) | Y (Avro/Proto) | Y (via registry) |
| **WebSocket** | Y | - | Y (JSON) | - | - |
| **File (CSV)** | Y | - | Y | - | - |
| **File (Parquet)** | Y | Y (metadata) | Y (auto) | - | - |
| **Iceberg** | Y | Y (catalog) | - | - | Y |
| **RabbitMQ** | Y | - | Y (JSON) | - | - |
| **Delta Lake** | Y | Y (log) | - | - | Y |
| **Postgres CDC** | Y | Y (pg_catalog) | - | - | Y |
| **NATS** (3rd party) | Y | - | Y | - | - |

---

## Module Structure

```
crates/laminar-connector-api/
    Cargo.toml
    src/
        lib.rs                  # Crate root, re-exports all public types
        error.rs                # SchemaError, SchemaResult
        raw.rs                  # RawRecord, SourceMetadata, FieldMeta
        config.rs               # SourceConfig, SinkConfig, ConfigOption,
                                #   ConnectorConfigSchema, ConfigValueType
        format.rs               # FormatDecoder, FormatEncoder traits
        provider.rs             # SchemaProvider trait
        inferable.rs            # SchemaInferable, InferenceConfig,
                                #   InferredSchema, NumberInference,
                                #   ArrayInference, FieldInferenceDetail,
                                #   InferenceWarning, WarningSeverity
        registry_aware.rs       # SchemaRegistryAware, RegistryConfig,
                                #   RegisteredSchema, RegistrySchemaType,
                                #   CompatibilityMode, RegistryCredentials
        evolvable.rs            # SchemaEvolvable, SchemaChange,
                                #   EvolutionVerdict, ColumnProjection
        source.rs               # SourceConnector trait (with as_* methods)
        sink.rs                 # SinkConnector trait (with as_* methods)
        factory.rs              # SourceConnectorFactory, SinkConnectorFactory,
                                #   ConnectorRegistry
        inference_registry.rs   # FormatInference trait,
                                #   FormatInferenceRegistry
        resolver.rs             # SchemaResolver, DeclaredSchema,
                                #   ResolvedSchema, ResolutionKind,
                                #   FieldOrigin
        stream.rs               # RecordStream, RecordSink traits
                                #   (async streaming interfaces)
```

### Dependency Graph

```
laminar-connector-api (new crate, minimal deps)
    depends on: arrow, async-trait, thiserror, anyhow

laminar-connectors (existing crate)
    depends on: laminar-connector-api (re-exports traits)
    provides:   KafkaSource, WebSocketSource, FileSource, etc.
    provides:   JsonDecoder, AvroDecoder, CsvDecoder, etc.

laminar-sql (existing crate)
    depends on: laminar-connector-api (for SourceConfig, SchemaResolver)
    uses:       SchemaResolver in DDL execution

Third-party crates (e.g., laminardb-connector-nats)
    depends on: laminar-connector-api ONLY
```

---

## Implementation Plan

### Step 1: Create `laminar-connector-api` Crate (3 days, M effort)

- Create the crate skeleton with `Cargo.toml` (deps: `arrow`, `async-trait`, `thiserror`, `anyhow`)
- Implement `error.rs`: `SchemaError`, `SchemaResult`
- Implement `raw.rs`: `RawRecord`, `SourceMetadata`, `FieldMeta`
- Implement `config.rs`: `SourceConfig`, `SinkConfig`, `ConnectorConfigSchema`, `ConfigOption`, `ConfigValueType`
- Implement `stream.rs`: `RecordStream`, `RecordSink` trait stubs
- Add compile-time object-safety tests

### Step 2: Core Capability Traits (4 days, L effort)

- Implement `format.rs`: `FormatDecoder`, `FormatEncoder`
- Implement `provider.rs`: `SchemaProvider`
- Implement `inferable.rs`: `SchemaInferable`, `InferenceConfig`, `InferredSchema`, all supporting types
- Implement `registry_aware.rs`: `SchemaRegistryAware`, `RegistryConfig`, `RegisteredSchema`, all enums
- Implement `evolvable.rs`: `SchemaEvolvable`, `SchemaChange`, `EvolutionVerdict`, `ColumnProjection`
- Add comprehensive doc comments and examples on every public item

### Step 3: Source/Sink Connector Traits + Factories (3 days, M effort)

- Implement `source.rs`: `SourceConnector` with `as_*()` methods
- Implement `sink.rs`: `SinkConnector` with `as_*()` methods
- Implement `factory.rs`: `SourceConnectorFactory`, `SinkConnectorFactory`, `ConnectorRegistry`
- Implement `inference_registry.rs`: `FormatInference`, `FormatInferenceRegistry`
- Ensure backward compatibility with F034's existing trait signatures

### Step 4: SchemaResolver (4 days, L effort)

- Implement `resolver.rs`: `SchemaResolver` with the priority cascade
- Implement `DeclaredSchema`, `ResolvedSchema`, `ResolutionKind`, `FieldOrigin`
- Implement wildcard merging logic (declared columns + inferred remainder)
- Implement schema merge conflict detection
- Unit tests for all resolution strategies (explicit, registry, provider, inference, hybrid)

### Step 5: Wire into `laminar-connectors` (3 days, M effort)

- Add `laminar-connector-api` as a dependency of `laminar-connectors`
- Re-export all trait types from `laminar-connectors::api`
- Migrate existing `SourceConnector`/`SinkConnector` in `connector.rs` to use the new traits (or re-export)
- Ensure existing tests still compile and pass
- Add `as_*()` default implementations to existing connectors where appropriate (WebSocket gets `as_schema_inferable`, Kafka gets all applicable, etc.)

### Step 6: Integration Testing and Documentation (3 days, M effort)

- Write integration test with mock connector implementing all five traits
- Write compile-time object-safety test battery
- Write configuration validation test suite
- Add crate-level documentation with examples
- Update `docs/features/INDEX.md` with the new feature entry

**Total estimated effort: 20 days (XL)**

---

## Testing Strategy

### Compile-Time Object Safety Verification

```rust
/// Compile-time test: all traits must be object-safe.
///
/// If any trait is not object-safe, this file will fail to compile.
#[cfg(test)]
mod object_safety {
    use super::*;

    fn _assert_format_decoder_object_safe(
        _: &dyn FormatDecoder,
    ) {}

    fn _assert_format_encoder_object_safe(
        _: &dyn FormatEncoder,
    ) {}

    fn _assert_schema_provider_object_safe(
        _: &dyn SchemaProvider,
    ) {}

    fn _assert_schema_inferable_object_safe(
        _: &dyn SchemaInferable,
    ) {}

    fn _assert_schema_registry_aware_object_safe(
        _: &dyn SchemaRegistryAware,
    ) {}

    fn _assert_schema_evolvable_object_safe(
        _: &dyn SchemaEvolvable,
    ) {}

    fn _assert_source_connector_object_safe(
        _: &dyn SourceConnector,
    ) {}

    fn _assert_sink_connector_object_safe(
        _: &dyn SinkConnector,
    ) {}

    fn _assert_source_factory_object_safe(
        _: &dyn SourceConnectorFactory,
    ) {}

    fn _assert_sink_factory_object_safe(
        _: &dyn SinkConnectorFactory,
    ) {}

    fn _assert_format_inference_object_safe(
        _: &dyn FormatInference,
    ) {}
}
```

### Unit Tests: Config Validation

```rust
#[test]
fn source_config_get_required_present() {
    let config = SourceConfig::new(
        "kafka",
        "json",
        HashMap::from([("brokers".into(), "b:9092".into())]),
    );
    assert_eq!(config.get_required("brokers").unwrap(), "b:9092");
}

#[test]
fn source_config_get_required_missing() {
    let config = SourceConfig::new("kafka", "json", HashMap::new());
    let err = config.get_required("brokers").unwrap_err();
    assert!(matches!(err, SchemaError::MissingConfig(_)));
}

#[test]
fn source_config_get_typed_valid() {
    let config = SourceConfig::new(
        "kafka",
        "json",
        HashMap::from([("batch_size".into(), "1024".into())]),
    );
    let val: Option<usize> = config.get_typed("batch_size").unwrap();
    assert_eq!(val, Some(1024));
}

#[test]
fn source_config_get_typed_invalid() {
    let config = SourceConfig::new(
        "kafka",
        "json",
        HashMap::from([("batch_size".into(), "not_a_number".into())]),
    );
    let err = config.get_typed::<usize>("batch_size").unwrap_err();
    assert!(matches!(err, SchemaError::InvalidConfig { .. }));
}

#[test]
fn connector_config_schema_builder() {
    let schema = ConnectorConfigSchema::new()
        .required("url", "Server URL")
        .required("topic", "Topic name")
        .optional("group_id", "Consumer group");
    assert_eq!(schema.required.len(), 2);
    assert_eq!(schema.optional.len(), 1);
    assert_eq!(schema.required[0].name, "url");
}
```

### Unit Tests: Connector Registry

```rust
#[test]
fn registry_create_unknown_source() {
    let registry = ConnectorRegistry::new();
    let config = SourceConfig::new("unknown", "json", HashMap::new());
    let err = registry.create_source("unknown", &config).unwrap_err();
    // Error message lists available connector types
    let msg = format!("{err}");
    assert!(msg.contains("Unknown source connector"));
}

#[test]
fn registry_register_and_create() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source(Box::new(MockSourceFactory));
    let config = SourceConfig::new("mock", "json", HashMap::new());
    let source = registry.create_source("mock", &config);
    assert!(source.is_ok());
    assert_eq!(source.unwrap().connector_type(), "mock");
}
```

### Integration Test: Mock Connector Implementing All Five Traits

```rust
/// Mock connector that implements all five capability traits.
/// Used to verify the full SchemaResolver flow end-to-end.
#[derive(Debug)]
struct FullMockSource {
    /// Schema to return from provide_schema().
    provided_schema: SchemaRef,
    /// Samples to return from sample_records().
    samples: Vec<RawRecord>,
    /// Schema to return from fetch_schema().
    registered: RegisteredSchema,
}

#[async_trait]
impl SourceConnector for FullMockSource {
    fn connector_type(&self) -> &str { "mock-full" }
    async fn validate_config(&self, _: &SourceConfig) -> SchemaResult<()> {
        Ok(())
    }
    fn build_decoder(
        &self, schema: SchemaRef, _: &str, _: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        Ok(Box::new(MockDecoder(schema)))
    }
    async fn start(&self, _: &SourceConfig)
        -> SchemaResult<Box<dyn RecordStream>>
    {
        Ok(Box::new(MockStream))
    }
    fn as_schema_provider(&self)
        -> Option<&dyn SchemaProvider> { Some(self) }
    fn as_schema_inferable(&self)
        -> Option<&dyn SchemaInferable> { Some(self) }
    fn as_schema_registry_aware(&self)
        -> Option<&dyn SchemaRegistryAware> { Some(self) }
    fn as_schema_evolvable(&self)
        -> Option<&dyn SchemaEvolvable> { Some(self) }
    fn supported_formats(&self) -> &[&str] { &["json"] }
}

// (impl SchemaProvider, SchemaInferable, SchemaRegistryAware,
//  SchemaEvolvable for FullMockSource -- each delegates to stored data)

#[tokio::test]
async fn resolver_uses_explicit_schema_when_fully_declared() {
    let source = FullMockSource::new(/* ... */);
    let declared = DeclaredSchema {
        columns: vec![
            DeclaredColumn {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
            },
        ],
        has_wildcard: false,
        wildcard_prefix: None,
    };
    let config = SourceConfig::new("mock-full", "json", HashMap::new());
    let resolver = SchemaResolver;
    let resolved = resolver
        .resolve(Some(&declared), &source, &config, "json", None, None)
        .await
        .unwrap();
    assert!(matches!(resolved.resolution, ResolutionKind::Declared));
    assert_eq!(resolved.schema.fields().len(), 1);
    assert_eq!(resolved.schema.field(0).name(), "id");
}

#[tokio::test]
async fn resolver_falls_through_to_registry() {
    // When no explicit schema, and registry config is provided,
    // the resolver should use SchemaRegistryAware::fetch_schema()
    let source = FullMockSource::new(/* ... */);
    let registry_config = RegistryConfig {
        url: "http://mock:8081".into(),
        subject: Some("test-value".into()),
        schema_type: RegistrySchemaType::Avro,
        compatibility: CompatibilityMode::Backward,
        credentials: None,
    };
    let config = SourceConfig::new("mock-full", "avro", HashMap::new());
    let resolver = SchemaResolver;
    let resolved = resolver
        .resolve(None, &source, &config, "avro", None, Some(&registry_config))
        .await
        .unwrap();
    assert!(matches!(
        resolved.resolution,
        ResolutionKind::Registry { .. }
    ));
}

#[tokio::test]
async fn resolver_falls_through_to_provider() {
    // When no explicit schema and no registry config, but source
    // is SchemaProvider, use provide_schema()
    let source = FullMockSource::new(/* ... */);
    let config = SourceConfig::new("mock-full", "parquet", HashMap::new());
    let resolver = SchemaResolver;
    let resolved = resolver
        .resolve(None, &source, &config, "parquet", None, None)
        .await
        .unwrap();
    assert!(matches!(
        resolved.resolution,
        ResolutionKind::SourceProvided
    ));
}

#[tokio::test]
async fn resolver_falls_through_to_inference() {
    // Source that is only SchemaInferable (not Provider or Registry)
    let source = WebSocketSource;
    let inference_config = InferenceConfig {
        sample_size: 10,
        ..Default::default()
    };
    let config = SourceConfig::new("websocket", "json", HashMap::from([
        ("url".into(), "ws://localhost:8080".into()),
    ]));
    let resolver = SchemaResolver;
    let resolved = resolver
        .resolve(
            None, &source, &config, "json",
            Some(&inference_config), None,
        )
        .await
        .unwrap();
    assert!(matches!(
        resolved.resolution,
        ResolutionKind::Inferred { .. }
    ));
}

#[tokio::test]
async fn resolver_wildcard_merges_declared_and_inferred() {
    // Declared: device_id VARCHAR NOT NULL, temperature DOUBLE, *
    // Inferred: device_id, temperature, humidity, pressure
    // Result: device_id (Declared), temperature (Declared),
    //         extra_humidity (Inferred), extra_pressure (Inferred)
    let declared = DeclaredSchema {
        columns: vec![
            DeclaredColumn {
                name: "device_id".into(),
                data_type: DataType::Utf8,
                nullable: false,
                default: None,
            },
            DeclaredColumn {
                name: "temperature".into(),
                data_type: DataType::Float64,
                nullable: true,
                default: None,
            },
        ],
        has_wildcard: true,
        wildcard_prefix: Some("extra_".into()),
    };
    // ... setup mock source with inferable schema ...
    let resolved = resolver
        .resolve(Some(&declared), &source, &config, "json",
                 Some(&inference_config), None)
        .await
        .unwrap();

    // User-declared columns first
    assert_eq!(resolved.schema.field(0).name(), "device_id");
    assert_eq!(
        *resolved.field_origins.get("device_id").unwrap(),
        FieldOrigin::UserDeclared,
    );

    // Inferred columns with prefix
    let inferred_names: Vec<_> = resolved.schema.fields()
        .iter()
        .filter(|f| f.name().starts_with("extra_"))
        .map(|f| f.name().as_str())
        .collect();
    assert!(inferred_names.contains(&"extra_humidity"));
    assert!(inferred_names.contains(&"extra_pressure"));
}
```

---

## Success Criteria

| Criterion | Target | Measurement |
|-----------|--------|-------------|
| All 11 traits compile as object-safe | 100% | Compile-time test suite |
| `SourceConfig` / `SinkConfig` typed accessors | 10+ unit tests | `cargo test` |
| `ConnectorRegistry` create/register cycle | 5+ unit tests | `cargo test` |
| `SchemaResolver` priority cascade | 5+ integration tests (1 per strategy) | `cargo test` |
| Wildcard merge correctness | 3+ tests | `cargo test` |
| Mock connector implementing all 5 traits | 1 integration test | `cargo test` |
| `laminar-connector-api` crate compiles with minimal deps | `arrow`, `async-trait`, `thiserror`, `anyhow` only | `cargo tree` |
| No Ring 0 latency regression | < 500ns state lookup | `cargo bench --bench state_bench` |
| Backward compatibility with F034 | All existing connector tests pass unchanged | `cargo test -p laminar-connectors` |
| Documentation coverage | All public items documented | `#![deny(missing_docs)]` |
| Total test count for new crate | >= 30 tests | `cargo test -p laminar-connector-api` |

---

## Competitive Comparison

| Capability | LaminarDB (F-SCHEMA-001) | Apache Flink | RisingWave | Materialize | ClickHouse |
|------------|-------------------------|-------------|------------|-------------|------------|
| **Trait-based connector SDK** | 5 composable traits | Monolithic Source/Sink API (Java) | Built-in connectors only | Built-in only | Built-in only |
| **Third-party connector API** | `laminar-connector-api` crate (stable, minimal deps) | Flink Connector SDK (requires full Flink dep) | No public SDK | No public SDK | No public SDK |
| **Schema registry integration** | `SchemaRegistryAware` trait | Built into Avro format | Built into Avro format | Built into Avro format | Partial (Avro only) |
| **Sample-based inference** | `SchemaInferable` trait (JSON, CSV, any format) | Not supported (DDL only) | Not supported (DDL only) | Not supported (DDL only) | File formats only |
| **Schema evolution** | `SchemaEvolvable` trait (diff + evaluate + apply) | Format-specific only | Avro only | Avro only | Limited |
| **Self-describing sources** | `SchemaProvider` trait | Not abstracted | Implicit per-source | Implicit | Implicit |
| **Wildcard schema hints** | `CREATE SOURCE (..., *)` | Not supported | Not supported | Not supported | Not supported |
| **Format decoder separation** | `FormatDecoder` Ring 1 hot path (< 50us/batch) | Mixed with connector (JVM overhead) | Mixed with connector | Mixed with connector | Mixed with connector |
| **Capability discovery** | `as_*()` methods (compile-time safe) | Marker interfaces (runtime cast) | N/A | N/A | N/A |
| **Object safety** | All 11 traits verified | N/A (Java) | N/A (Rust, but not trait-based) | N/A (Rust, but not trait-based) | N/A (C++) |
| **Config self-documentation** | `ConnectorConfigSchema` | `ConfigDef` (Java) | TOML docs | Not available | Not available |

---

## References

1. **Schema Inference Design** -- [`docs/research/schema-inference-design.md`](../../../research/schema-inference-design.md)
   Full design for schema inference, format decoders, SQL DDL modes, transformation functions, and competitive analysis.

2. **Extensible Schema Traits** -- [`docs/research/extensible-schema-traits.md`](../../../research/extensible-schema-traits.md)
   Trait-based architecture for the five capability traits, `SchemaResolver`, `ConnectorRegistry`, concrete examples (Kafka, WebSocket, Iceberg, NATS), and composition matrix.

3. **F034 Connector SDK** -- [`docs/features/phase-3/F034-connector-sdk.md`](../F034-connector-sdk.md)
   The existing connector framework that F-SCHEMA-001 extends. Defines the `SourceConnector`/`SinkConnector` lifecycle, serialization framework, connector runtime, and SQL integration.

4. **Apache Flink Source API v2 (FLIP-27)** -- Flink's redesigned source interface separating split discovery from reading.

5. **Confluent Schema Registry** -- Wire format specification (magic byte + 4-byte schema ID + payload) and compatibility levels.

6. **DuckDB JSON Auto-Detection** -- `map_inference_threshold` for struct-vs-map heuristics, CSV type detection cascade.

7. **Apache Iceberg Schema Evolution** -- Field-ID-based evolution that survives column renames and reorders.
