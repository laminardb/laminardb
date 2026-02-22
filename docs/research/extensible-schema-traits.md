# LaminarDB Extensible Schema Framework

## Trait-Based Architecture for Source/Sink Schema Inference, Detection & Hinting

### Design Specification — February 2026

---

## 1. Design Philosophy

The schema system is decomposed into **five orthogonal traits** that can be composed independently. A new source/sink only implements the traits relevant to its capabilities. The framework never assumes a source can do something it can't — a CSV file source doesn't need to implement schema registry support, and a Kafka Avro source doesn't need sample-based inference.

```
                          ┌──────────────────────────┐
                          │    SchemaResolver         │  ← Orchestrator
                          │  (picks strategy, merges) │
                          └────────────┬─────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
    ┌─────────▼────────┐   ┌──────────▼─────────┐  ┌──────────▼──────────┐
    │ SchemaProvider    │   │ SchemaInferable     │  │ SchemaEvolvable     │
    │ (self-describing) │   │ (sample-based)      │  │ (evolution support) │
    └──────────────────┘   └────────────────────┘  └─────────────────────┘
              │                        │                        │
    ┌─────────▼────────┐   ┌──────────▼─────────┐
    │ SchemaRegistryAware│  │ FormatDecoder       │
    │ (external registry)│  │ (bytes → Arrow)     │
    └──────────────────┘   └────────────────────┘
```

**Key principle**: Traits are about *capabilities*, not *source types*. A single source (like Kafka) might implement multiple capability traits depending on its format. The same `KafkaSource` struct can be a `SchemaRegistryAware` when using Avro, a `SchemaInferable` when using JSON, or simply accept explicit schemas — all through the same trait system.

---

## 2. Core Traits

### 2.1 `FormatDecoder` / `FormatEncoder` — The Foundation

Every source needs a way to turn raw bytes into Arrow RecordBatches, and every sink needs the reverse. This is the one mandatory trait.

```rust
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

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

/// Source-specific metadata — extensible via Any.
pub struct SourceMetadata {
    inner: Box<dyn std::any::Any + Send + Sync>,
}

/// Decodes raw bytes into Arrow RecordBatches.
///
/// Implementations are format-specific (JSON, Avro, CSV, Parquet, etc.)
/// and are composed with a source connector at CREATE SOURCE time.
///
/// The decoder is stateless after construction — all schema info is baked
/// in at creation time so the hot path has zero schema lookups.
pub trait FormatDecoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this decoder produces. Frozen after construction.
    fn output_schema(&self) -> SchemaRef;

    /// Decode a batch of raw records into an Arrow RecordBatch.
    ///
    /// This is the Ring 1 hot path — implementations must be allocation-
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
pub trait FormatEncoder: Send + Sync + std::fmt::Debug {
    /// The Arrow schema this encoder expects as input.
    fn input_schema(&self) -> SchemaRef;

    /// Encode a RecordBatch into a vector of raw byte payloads (one per row).
    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>>;

    /// Human-readable format name.
    fn format_name(&self) -> &str;
}
```

**Why this matters for extensibility**: Any new format (MessagePack, CBOR, FlatBuffers, custom binary) only needs to implement `FormatDecoder`/`FormatEncoder`. The rest of the system doesn't care how bytes become Arrow.

### 2.2 `SchemaProvider` — Self-Describing Sources

Some sources carry their schema with them. Parquet files have metadata, Iceberg tables have catalog schemas, Avro messages have embedded schemas. These implement `SchemaProvider`.

```rust
/// Sources that can provide their own schema without external input.
///
/// Examples: Parquet files (metadata), Iceberg (catalog), Avro with
/// embedded schema, Protobuf with descriptor.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Retrieve the schema from the source itself.
    ///
    /// This is a Ring 2 operation — may involve I/O (reading file headers,
    /// querying a catalog, parsing descriptors).
    async fn provide_schema(&self, config: &SourceConfig) -> SchemaResult<SchemaRef>;

    /// Whether the provided schema is authoritative (vs. best-effort).
    ///
    /// Authoritative means the schema is guaranteed correct (e.g., Parquet
    /// metadata). Non-authoritative means it's a best guess (e.g., a single
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

**Implementing for a new source** (e.g., adding Delta Lake):

```rust
pub struct DeltaLakeSchemaProvider;

#[async_trait]
impl SchemaProvider for DeltaLakeSchemaProvider {
    async fn provide_schema(&self, config: &SourceConfig) -> SchemaResult<SchemaRef> {
        let table_uri = config.get_required("uri")?;
        let table = deltalake::open_table(table_uri).await?;
        let arrow_schema = table.schema()?.as_ref().try_into()?;
        Ok(Arc::new(arrow_schema))
    }

    fn is_authoritative(&self) -> bool { true }

    async fn field_metadata(&self, config: &SourceConfig)
        -> SchemaResult<HashMap<String, FieldMeta>>
    {
        // Delta Lake has column metadata, descriptions, etc.
        let table = deltalake::open_table(config.get_required("uri")?).await?;
        Ok(extract_delta_metadata(table.schema()?))
    }
}
```

### 2.3 `SchemaInferable` — Sample-Based Inference

For sources that carry no schema metadata (JSON, CSV, raw text), we infer from data samples.

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
    /// Max nesting depth to infer (beyond this → JSONB).
    pub max_depth: usize,
    /// How to handle arrays: first_element, union, or jsonb.
    pub array_strategy: ArrayInference,
    /// Threshold for struct vs. map inference (like DuckDB's
    /// map_inference_threshold). If an object has more than this
    /// many distinct keys across samples, infer MAP instead of STRUCT.
    pub map_threshold: usize,
    /// Threshold for field appearance. If a field appears in fewer
    /// than this fraction of samples, it's treated as optional.
    pub field_appearance_threshold: f64,
}

#[derive(Debug, Clone)]
pub enum NumberInference {
    BigInt,         // default integers to BIGINT
    Double,         // default all numbers to DOUBLE
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

/// Sources that support schema inference by sampling data.
///
/// The framework calls `sample_records` to get raw data, then passes
/// it to `infer_from_samples`. This two-step design lets the source
/// handle its own I/O (Kafka consumer, file read, WebSocket connect)
/// while the inference logic is reusable across formats.
#[async_trait]
pub trait SchemaInferable: Send + Sync {
    /// Collect a sample of raw records from the source.
    ///
    /// Implementations handle their own connection/disconnection.
    /// The returned records are raw bytes — format-specific parsing
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
        // Look up the format-specific inference implementation
        let inferrer = FORMAT_INFERENCE_REGISTRY
            .get(format)
            .ok_or_else(|| SchemaError::InferenceFailed(
                format!("No inference support for format '{format}'")
            ))?;
        inferrer.infer(samples, inference_config)
    }
}

/// Result of schema inference — includes confidence information.
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

### 2.4 `SchemaRegistryAware` — External Registry Integration

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

### 2.5 `SchemaEvolvable` — Schema Evolution Support

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

    /// Apply schema evolution — returns the new merged schema and
    /// an optional projection map (old column index → new column index).
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
    /// None means it's a newly added column (fill with default/NULL).
    pub mappings: Vec<Option<usize>>,
}
```

---

## 3. The Schema Resolver — Orchestrator

The `SchemaResolver` is the orchestrator that combines user-declared schemas, schema hints, and trait capabilities to produce the final frozen Arrow schema.

```rust
/// User-provided schema declaration from DDL.
#[derive(Debug, Clone)]
pub struct DeclaredSchema {
    /// Explicitly declared columns with types.
    pub columns: Vec<DeclaredColumn>,
    /// Whether a wildcard (*) was used (infer remaining fields).
    pub has_wildcard: bool,
    /// Prefix for wildcard-inferred fields.
    pub wildcard_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
}

/// The resolution strategy determined by the resolver.
#[derive(Debug)]
enum ResolutionStrategy {
    /// User declared everything — no inference needed.
    FullyDeclared,
    /// Schema comes from the source itself (Parquet, Iceberg).
    SourceProvided,
    /// Schema comes from an external registry (Avro + Confluent).
    RegistryProvided,
    /// Schema inferred from data samples (JSON, CSV).
    Inferred,
    /// Mix: user declared some columns, rest inferred/provided.
    Hybrid {
        declared: DeclaredSchema,
        provider: Box<dyn std::any::Any + Send + Sync>,
    },
}

/// The main orchestrator. Given a source's capabilities and user's
/// DDL, it resolves the final Arrow schema.
pub struct SchemaResolver;

impl SchemaResolver {
    /// Resolve the schema for a source, using the best available strategy.
    ///
    /// Priority order:
    /// 1. If user provided a full explicit schema → use it
    /// 2. If user provided hints + wildcard → merge hints with inference
    /// 3. If source is SchemaRegistryAware and format uses registry → fetch
    /// 4. If source is SchemaProvider (self-describing) → use it
    /// 5. If source is SchemaInferable → sample and infer
    /// 6. Otherwise → error
    pub async fn resolve(
        &self,
        declared: Option<&DeclaredSchema>,
        source: &dyn SourceConnector,
        source_config: &SourceConfig,
        format: &str,
        inference_config: Option<&InferenceConfig>,
        registry_config: Option<&RegistryConfig>,
    ) -> SchemaResult<ResolvedSchema> {
        // Case 1: Fully declared, no wildcard
        if let Some(decl) = declared {
            if !decl.has_wildcard {
                return Ok(ResolvedSchema {
                    schema: declared_to_arrow(decl)?,
                    resolution: ResolutionKind::Declared,
                    field_origins: decl.columns.iter()
                        .map(|c| (c.name.clone(), FieldOrigin::UserDeclared))
                        .collect(),
                    warnings: vec![],
                });
            }
        }

        // Case 2: Registry available
        if let (Some(reg_config), Some(registry_source)) = (
            registry_config,
            source.as_schema_registry_aware(),
        ) {
            let registered = registry_source.fetch_schema(reg_config).await?;
            return self.merge_with_declared(
                declared,
                registered.schema.clone(),
                ResolutionKind::Registry { schema_id: registered.schema_id },
            );
        }

        // Case 3: Self-describing source
        if let Some(provider) = source.as_schema_provider() {
            let provided = provider.provide_schema(source_config).await?;
            return self.merge_with_declared(
                declared,
                provided,
                ResolutionKind::SourceProvided,
            );
        }

        // Case 4: Sample-based inference
        if let Some(inferable) = source.as_schema_inferable() {
            let inf_config = inference_config
                .cloned()
                .unwrap_or_default();
            let samples = inferable.sample_records(source_config, &inf_config).await?;
            let inferred = inferable.infer_from_samples(&samples, format, &inf_config).await?;
            return self.merge_with_declared(
                declared,
                inferred.schema.clone(),
                ResolutionKind::Inferred {
                    sample_count: inferred.sample_count,
                    warnings: inferred.warnings,
                },
            );
        }

        // No strategy available
        Err(SchemaError::InferenceFailed(
            "No schema provided and source does not support inference. \
             Please declare an explicit schema in CREATE SOURCE.".into()
        ))
    }

    /// Merge user-declared columns with auto-resolved schema.
    /// User declarations take priority.
    fn merge_with_declared(
        &self,
        declared: Option<&DeclaredSchema>,
        resolved: SchemaRef,
        resolution_kind: ResolutionKind,
    ) -> SchemaResult<ResolvedSchema> {
        let Some(decl) = declared else {
            // No user declarations — use resolved as-is
            return Ok(ResolvedSchema {
                field_origins: resolved.fields().iter()
                    .map(|f| (f.name().clone(), FieldOrigin::AutoResolved))
                    .collect(),
                schema: resolved,
                resolution: resolution_kind,
                warnings: vec![],
            });
        };

        let mut fields = Vec::new();
        let mut origins = HashMap::new();
        let mut warnings = Vec::new();

        // First: add all explicitly declared columns
        for col in &decl.columns {
            fields.push(Field::new(&col.name, col.data_type.clone(), col.nullable));
            origins.insert(col.name.clone(), FieldOrigin::UserDeclared);
        }

        // Then: if wildcard, add non-conflicting resolved columns
        if decl.has_wildcard {
            let prefix = decl.wildcard_prefix.as_deref().unwrap_or("");
            for field in resolved.fields() {
                let declared_names: HashSet<_> = decl.columns.iter()
                    .map(|c| c.name.as_str())
                    .collect();

                if !declared_names.contains(field.name().as_str()) {
                    let name = format!("{prefix}{}", field.name());
                    fields.push(Field::new(&name, field.data_type().clone(), true));
                    origins.insert(name, FieldOrigin::WildcardInferred);
                }
            }
        }

        Ok(ResolvedSchema {
            schema: Arc::new(Schema::new(fields)),
            resolution: resolution_kind,
            field_origins: origins,
            warnings,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub schema: SchemaRef,
    pub resolution: ResolutionKind,
    pub field_origins: HashMap<String, FieldOrigin>,
    pub warnings: Vec<InferenceWarning>,
}

#[derive(Debug, Clone)]
pub enum ResolutionKind {
    Declared,
    SourceProvided,
    Registry { schema_id: u32 },
    Inferred { sample_count: usize, warnings: Vec<InferenceWarning> },
}

#[derive(Debug, Clone)]
pub enum FieldOrigin {
    UserDeclared,
    AutoResolved,
    WildcardInferred,
    DefaultAdded,
}
```

---

## 4. The Source Connector Trait — Capability Discovery

The `SourceConnector` trait is the top-level trait that every source implements. It uses **capability discovery** via optional downcast methods instead of requiring all sources to implement all traits.

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

    // ── Capability discovery ──────────────────────────────────────

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

## 5. Format Inference Registry — Pluggable Per-Format Inference

Format-specific inference logic (how to infer types from JSON vs CSV vs MessagePack) is registered in a global registry. New formats register their inference logic independently.

```rust
/// Format-specific inference logic.
///
/// This is separate from FormatDecoder because inference is a one-time
/// operation (Ring 2), while decoding is the continuous hot path (Ring 1).
pub trait FormatInference: Send + Sync {
    /// The format name this inferrer handles (e.g., "json", "csv").
    fn format_name(&self) -> &str;

    /// Infer schema from raw record samples.
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema>;
}

/// Global registry of format inference implementations.
/// New formats register at startup or via feature flags.
pub struct FormatInferenceRegistry {
    inferrers: HashMap<String, Box<dyn FormatInference>>,
}

impl FormatInferenceRegistry {
    pub fn new() -> Self {
        let mut reg = Self { inferrers: HashMap::new() };
        // Built-in registrations
        reg.register(Box::new(JsonFormatInference));
        reg.register(Box::new(CsvFormatInference));
        reg.register(Box::new(MsgpackFormatInference));
        reg
    }

    pub fn register(&mut self, inferrer: Box<dyn FormatInference>) {
        self.inferrers.insert(inferrer.format_name().to_string(), inferrer);
    }

    pub fn get(&self, format: &str) -> Option<&dyn FormatInference> {
        self.inferrers.get(format).map(|b| b.as_ref())
    }
}

/// Lazy-initialized global registry.
/// Thread-safe: populated at startup, read-only afterwards.
static FORMAT_INFERENCE_REGISTRY: LazyLock<FormatInferenceRegistry> =
    LazyLock::new(FormatInferenceRegistry::new);
```

---

## 6. Connector Registry — Dynamic Source/Sink Discovery

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

impl ConnectorRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
        };
        // Built-in connectors
        reg.register_source(Box::new(KafkaSourceFactory));
        reg.register_source(Box::new(WebSocketSourceFactory));
        reg.register_source(Box::new(FileSourceFactory));
        reg.register_source(Box::new(IcebergSourceFactory));
        reg.register_source(Box::new(RabbitMqSourceFactory));

        reg.register_sink(Box::new(KafkaSinkFactory));
        reg.register_sink(Box::new(FileSinkFactory));
        reg.register_sink(Box::new(IcebergSinkFactory));
        reg.register_sink(Box::new(WebSocketSinkFactory));
        reg
    }

    pub fn register_source(&mut self, factory: Box<dyn SourceConnectorFactory>) {
        self.sources.insert(factory.connector_type().to_string(), factory);
    }

    pub fn register_sink(&mut self, factory: Box<dyn SinkConnectorFactory>) {
        self.sinks.insert(factory.connector_type().to_string(), factory);
    }

    pub fn create_source(
        &self,
        connector_type: &str,
        config: &SourceConfig,
    ) -> SchemaResult<Box<dyn SourceConnector>> {
        self.sources.get(connector_type)
            .ok_or_else(|| SchemaError::Other(
                anyhow::anyhow!("Unknown source connector: '{connector_type}'. \
                    Available: {:?}", self.sources.keys().collect::<Vec<_>>())
            ))?
            .create(config)
    }

    pub fn create_sink(
        &self,
        connector_type: &str,
        config: &SinkConfig,
    ) -> SchemaResult<Box<dyn SinkConnector>> {
        self.sinks.get(connector_type)
            .ok_or_else(|| SchemaError::Other(
                anyhow::anyhow!("Unknown sink connector: '{connector_type}'")
            ))?
            .create(config)
    }
}
```

---

## 7. Concrete Examples: Implementing New Sources

### 7.1 Kafka Source — Full-Featured

Kafka implements nearly all capability traits because it's so versatile.

```rust
#[derive(Debug)]
pub struct KafkaSource {
    config: KafkaConfig,
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn connector_type(&self) -> &str { "kafka" }

    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()> {
        config.require("brokers")?;
        config.require("topic")?;
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
            _ => Err(SchemaError::Other(anyhow::anyhow!("Unsupported format: {format}"))),
        }
    }

    async fn start(&self, config: &SourceConfig)
        -> SchemaResult<Box<dyn RecordStream>>
    {
        Ok(Box::new(KafkaRecordStream::connect(&self.config).await?))
    }

    // ── Kafka supports inference (JSON/CSV), registry (Avro), and evolution ──

    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        Some(self) // Kafka can sample messages for inference
    }

    fn as_schema_registry_aware(&self) -> Option<&dyn SchemaRegistryAware> {
        Some(self) // Kafka integrates with Confluent Schema Registry
    }

    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        Some(self) // Kafka supports schema evolution via registry
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
            .consume_n(inference_config.sample_size, inference_config.sample_timeout)
            .await?;
        consumer.disconnect().await?;
        Ok(samples)
    }
}

#[async_trait]
impl SchemaRegistryAware for KafkaSource {
    async fn fetch_schema(&self, registry_config: &RegistryConfig)
        -> SchemaResult<RegisteredSchema>
    {
        let client = SchemaRegistryClient::new(&registry_config.url)?;
        let subject = registry_config.subject.as_deref()
            .unwrap_or(&format!("{}-value", self.config.topic));
        let schema = client.get_latest(subject).await?;
        Ok(convert_to_registered(schema)?)
    }

    async fn fetch_schema_by_id(&self, registry_config: &RegistryConfig, schema_id: u32)
        -> SchemaResult<RegisteredSchema>
    {
        let client = SchemaRegistryClient::new(&registry_config.url)?;
        let schema = client.get_by_id(schema_id).await?;
        Ok(convert_to_registered(schema)?)
    }

    async fn check_compatibility(
        &self,
        registry_config: &RegistryConfig,
        current: &RegisteredSchema,
        proposed: &SchemaRef,
    ) -> SchemaResult<bool> {
        let client = SchemaRegistryClient::new(&registry_config.url)?;
        client.test_compatibility(&current.subject, proposed).await
    }

    async fn register_schema(
        &self,
        registry_config: &RegistryConfig,
        schema: &SchemaRef,
    ) -> SchemaResult<RegisteredSchema> {
        let client = SchemaRegistryClient::new(&registry_config.url)?;
        let subject = registry_config.subject.as_deref()
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
```

### 7.2 WebSocket Source — Minimal Implementation

WebSocket is simpler: it only does JSON inference, no registry, no self-describing schema.

```rust
#[derive(Debug)]
pub struct WebSocketSource;

#[async_trait]
impl SourceConnector for WebSocketSource {
    fn connector_type(&self) -> &str { "websocket" }

    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()> {
        config.require("url")?;
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
            _ => Err(SchemaError::Other(anyhow::anyhow!("WebSocket only supports json/raw"))),
        }
    }

    async fn start(&self, config: &SourceConfig)
        -> SchemaResult<Box<dyn RecordStream>>
    {
        Ok(Box::new(WebSocketRecordStream::connect(config).await?))
    }

    // Only inference — no registry, no self-describing
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
        let ws = WebSocketClient::connect(config.get_required("url")?).await?;
        let samples = ws
            .receive_n(inference_config.sample_size, inference_config.sample_timeout)
            .await?;
        ws.close().await?;
        Ok(samples)
    }
}
```

### 7.3 Iceberg Source — Self-Describing + Evolvable

Iceberg carries its schema in catalog metadata and supports full evolution.

```rust
#[derive(Debug)]
pub struct IcebergSource;

#[async_trait]
impl SourceConnector for IcebergSource {
    fn connector_type(&self) -> &str { "iceberg" }

    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()> {
        config.require("catalog_uri")?;
        config.require("table")?;
        Ok(())
    }

    fn build_decoder(
        &self,
        schema: SchemaRef,
        _format: &str,
        _config: &SourceConfig,
    ) -> SchemaResult<Box<dyn FormatDecoder>> {
        // Iceberg always uses Parquet internally
        Ok(Box::new(ParquetDecoder::new(schema)))
    }

    async fn start(&self, config: &SourceConfig)
        -> SchemaResult<Box<dyn RecordStream>>
    {
        Ok(Box::new(IcebergScanStream::connect(config).await?))
    }

    // Self-describing AND evolvable — but NOT inferable
    fn as_schema_provider(&self) -> Option<&dyn SchemaProvider> {
        Some(self)
    }

    fn as_schema_evolvable(&self) -> Option<&dyn SchemaEvolvable> {
        Some(self)
    }

    fn supported_formats(&self) -> &[&str] {
        &["parquet"]
    }
}

#[async_trait]
impl SchemaProvider for IcebergSource {
    async fn provide_schema(&self, config: &SourceConfig) -> SchemaResult<SchemaRef> {
        let catalog = build_iceberg_catalog(config).await?;
        let table = catalog.load_table(config.get_required("table")?).await?;
        let iceberg_schema = table.metadata().current_schema();
        Ok(iceberg_schema_to_arrow(iceberg_schema)?)
    }

    fn is_authoritative(&self) -> bool { true }

    async fn field_metadata(&self, config: &SourceConfig)
        -> SchemaResult<HashMap<String, FieldMeta>>
    {
        let catalog = build_iceberg_catalog(config).await?;
        let table = catalog.load_table(config.get_required("table")?).await?;
        let schema = table.metadata().current_schema();
        Ok(schema.fields().iter().map(|f| {
            (f.name.clone(), FieldMeta {
                field_id: Some(f.id),
                description: f.doc.clone(),
                source_type: Some(format!("{:?}", f.field_type)),
                default_expr: None,
                properties: HashMap::new(),
            })
        }).collect())
    }
}
```

### 7.4 Adding a Completely New Source (e.g., NATS JetStream)

A third-party developer adds NATS support:

```rust
// In their own crate: laminardb-connector-nats

#[derive(Debug)]
pub struct NatsSource;

#[async_trait]
impl SourceConnector for NatsSource {
    fn connector_type(&self) -> &str { "nats" }

    async fn validate_config(&self, config: &SourceConfig) -> SchemaResult<()> {
        config.require("url")?;
        config.require("subject")?;
        Ok(())
    }

    fn build_decoder(&self, schema: SchemaRef, format: &str, config: &SourceConfig)
        -> SchemaResult<Box<dyn FormatDecoder>>
    {
        // Reuse LaminarDB's built-in decoders!
        laminardb::formats::build_decoder(format, schema, config)
    }

    async fn start(&self, config: &SourceConfig)
        -> SchemaResult<Box<dyn RecordStream>>
    {
        let client = async_nats::connect(config.get_required("url")?).await?;
        let subscriber = client.subscribe(config.get_required("subject")?).await?;
        Ok(Box::new(NatsRecordStream::new(subscriber)))
    }

    fn as_schema_inferable(&self) -> Option<&dyn SchemaInferable> {
        Some(self) // NATS messages can be sampled
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "avro", "raw"]
    }
}

#[async_trait]
impl SchemaInferable for NatsSource {
    async fn sample_records(&self, config: &SourceConfig, inf: &InferenceConfig)
        -> SchemaResult<Vec<RawRecord>>
    {
        let client = async_nats::connect(config.get_required("url")?).await?;
        let mut sub = client.subscribe(config.get_required("subject")?).await?;
        let mut samples = Vec::with_capacity(inf.sample_size);
        let deadline = Instant::now() + inf.sample_timeout;

        while samples.len() < inf.sample_size && Instant::now() < deadline {
            if let Ok(Some(msg)) = timeout(Duration::from_millis(100), sub.next()).await {
                samples.push(RawRecord {
                    key: msg.headers.as_ref().map(|h| h.to_string().into_bytes()),
                    value: msg.payload.to_vec(),
                    timestamp: None,
                    headers: vec![],
                    metadata: SourceMetadata::empty(),
                });
            }
        }
        Ok(samples)
    }
}

// Registration:
pub struct NatsSourceFactory;
impl SourceConnectorFactory for NatsSourceFactory {
    fn connector_type(&self) -> &str { "nats" }
    fn create(&self, config: &SourceConfig) -> SchemaResult<Box<dyn SourceConnector>> {
        Ok(Box::new(NatsSource))
    }
    fn config_schema(&self) -> ConnectorConfigSchema {
        ConnectorConfigSchema::new()
            .required("url", "NATS server URL")
            .required("subject", "NATS subject to subscribe to")
            .optional("queue_group", "Consumer queue group name")
    }
}
```

Then in user code or plugin initialization:

```rust
registry.register_source(Box::new(NatsSourceFactory));
```

And it's immediately usable in SQL:

```sql
CREATE SOURCE nats_events (
    event_type VARCHAR,
    payload JSONB,
    ts TIMESTAMP
) FROM NATS (
    url     = 'nats://localhost:4222',
    subject = 'events.>'
)
FORMAT JSON;
```

---

## 8. Trait Composition Matrix

| Source | FormatDecoder | SchemaProvider | SchemaInferable | SchemaRegistryAware | SchemaEvolvable |
|--------|:---:|:---:|:---:|:---:|:---:|
| **Kafka** | ✅ | ❌ | ✅ (JSON/CSV) | ✅ (Avro/Proto) | ✅ (via registry) |
| **WebSocket** | ✅ | ❌ | ✅ (JSON) | ❌ | ❌ |
| **File (CSV)** | ✅ | ❌ | ✅ | ❌ | ❌ |
| **File (Parquet)** | ✅ | ✅ (metadata) | ✅ (auto) | ❌ | ❌ |
| **Iceberg** | ✅ | ✅ (catalog) | ❌ | ❌ | ✅ |
| **RabbitMQ** | ✅ | ❌ | ✅ (JSON) | ❌ | ❌ |
| **NATS** (3rd party) | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Delta Lake** (3rd party) | ✅ | ✅ (log) | ❌ | ❌ | ✅ |
| **Postgres CDC** (future) | ✅ | ✅ (pg_catalog) | ❌ | ❌ | ✅ |

---

## 9. The Full CREATE SOURCE Flow

```
User SQL:  CREATE SOURCE trades (...) FROM KAFKA (...) FORMAT JSON INFER SCHEMA (...)
                    │                       │               │           │
                    ▼                       ▼               ▼           ▼
             DeclaredSchema          ConnectorRegistry  FormatName  InferenceConfig
             (user columns)          .create_source()
                    │                       │
                    │                       ▼
                    │               KafkaSource (Box<dyn SourceConnector>)
                    │                       │
                    │         ┌─────────────┼────────────────────┐
                    │         │ capability discovery              │
                    │         ▼                                   ▼
                    │   as_schema_inferable() → Some(&self)  as_schema_registry_aware() → Some
                    │         │                                   │
                    ▼         ▼                                   │
            ┌───────────────────────────┐                         │
            │     SchemaResolver        │                         │
            │                           │                         │
            │  1. Has full DDL? → use it│                         │
            │  2. Has registry? → fetch │◄────────────────────────┘
            │  3. Self-describing? → N/A│
            │  4. Inferable? → sample   │
            │  5. Merge with hints      │
            └───────────┬───────────────┘
                        │
                        ▼
                  ResolvedSchema {
                    schema: Arc<Schema>,
                    resolution: Inferred { ... },
                    field_origins: { symbol: Declared, price: Declared, extra_*: Inferred }
                  }
                        │
                        ▼
              source.build_decoder(schema, "json", config)
                        │
                        ▼
              JsonDecoder (Box<dyn FormatDecoder>)
                        │
                        ▼
              Frozen in Ring 1 — hot path ready
```

---

## 10. Extension Points Summary

| Extension | What to Implement | Traits |
|-----------|-------------------|--------|
| **New source** (e.g., NATS, Redis Streams) | `SourceConnector` + relevant capability traits + `SourceConnectorFactory` | Core |
| **New sink** (e.g., Elasticsearch) | `SinkConnector` + `SinkConnectorFactory` | Core |
| **New format** (e.g., Avro, CBOR) | `FormatDecoder` + `FormatEncoder` | Format layer |
| **New inference** (e.g., MessagePack) | `FormatInference` + register in `FormatInferenceRegistry` | Inference layer |
| **New registry** (e.g., AWS Glue) | `SchemaRegistryAware` impl on your source | Registry layer |
| **New evolution** (e.g., Delta Lake) | `SchemaEvolvable` impl | Evolution layer |

Each extension point is independent. Adding a new format doesn't require touching any source. Adding a new source doesn't require implementing any format. The `SchemaResolver` automatically discovers and composes whatever capabilities are available.
