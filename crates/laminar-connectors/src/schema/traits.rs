//! Capability traits for the extensible connector framework.
//!
//! Six traits define the schema capabilities a connector can implement:
//!
//! | Trait | Purpose | Sync/Async |
//! |-------|---------|------------|
//! | [`FormatDecoder`] | Decode raw bytes → Arrow | Sync |
//! | [`FormatEncoder`] | Encode Arrow → raw bytes | Sync |
//! | [`SchemaProvider`] | Authoritative schema from source | Async |
//! | [`SchemaInferable`] | Sample-based schema inference | Async |
//! | [`SchemaRegistryAware`] | Schema registry integration | Async |
//! | [`SchemaEvolvable`] | Schema evolution & diffing | Sync |
//!
//! Connectors opt in to capabilities by implementing the relevant traits
//! and returning `Some(self)` from the corresponding `as_*()` method on
//! [`SourceConnector`](crate::connector::SourceConnector) or
//! [`SinkConnector`](crate::connector::SinkConnector).

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;

use super::error::SchemaResult;
use super::types::RawRecord;

// ── FormatDecoder ──────────────────────────────────────────────────

/// Decodes raw bytes into Arrow `RecordBatch`es.
///
/// Unlike [`RecordDeserializer`](crate::serde::RecordDeserializer) which
/// takes `&[u8]` slices, `FormatDecoder` works with [`RawRecord`]s that
/// carry metadata, headers, and timestamps alongside the payload.
pub trait FormatDecoder: Send + Sync {
    /// Returns the Arrow schema produced by this decoder.
    fn output_schema(&self) -> SchemaRef;

    /// Decodes a batch of raw records into an Arrow `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::DecodeError`](super::error::SchemaError::DecodeError)
    /// if the input cannot be parsed.
    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch>;

    /// Decodes a single raw record into an Arrow `RecordBatch` with one row.
    ///
    /// Default implementation delegates to [`decode_batch`](Self::decode_batch)
    /// with a single-element slice.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::DecodeError`](super::error::SchemaError::DecodeError)
    /// if the input cannot be parsed.
    fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
        self.decode_batch(std::slice::from_ref(record))
    }

    /// Returns the name of the format this decoder handles (e.g., `"json"`).
    fn format_name(&self) -> &str;
}

// ── FormatEncoder ──────────────────────────────────────────────────

/// Encodes Arrow `RecordBatch`es into raw bytes.
pub trait FormatEncoder: Send + Sync {
    /// Returns the expected input schema.
    fn input_schema(&self) -> SchemaRef;

    /// Encodes a `RecordBatch` into a vector of byte records.
    ///
    /// Each element in the returned vector represents one serialized record.
    ///
    /// # Errors
    ///
    /// Returns a schema error if encoding fails.
    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>>;

    /// Returns the name of the format this encoder produces (e.g., `"json"`).
    fn format_name(&self) -> &str;
}

// ── SchemaProvider ─────────────────────────────────────────────────

/// A connector that can provide its schema from the source system.
///
/// Examples: `PostgreSQL` CDC reading `information_schema`, a Kafka topic
/// with an embedded Avro schema, or a file source with a header row.
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Fetches the schema from the source system.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the schema cannot be retrieved.
    async fn provide_schema(&self) -> SchemaResult<SchemaRef>;

    /// Returns `true` if this provider's schema is authoritative
    /// (i.e., should take precedence over inference).
    fn is_authoritative(&self) -> bool {
        false
    }

    /// Returns per-field metadata for the provided schema.
    ///
    /// Default returns an empty map.
    async fn field_metadata(&self) -> SchemaResult<HashMap<String, super::types::FieldMeta>> {
        Ok(HashMap::new())
    }
}

// ── SchemaInferable ────────────────────────────────────────────────

/// A connector that supports sample-based schema inference.
///
/// The connector provides raw sample records, and the inference engine
/// determines the Arrow schema from the data.
#[async_trait]
pub trait SchemaInferable: Send + Sync {
    /// Reads sample records from the source for inference.
    ///
    /// The `max_records` parameter is a hint; implementations may return fewer.
    ///
    /// # Errors
    ///
    /// Returns a schema error if samples cannot be read.
    async fn sample_records(&self, max_records: usize) -> SchemaResult<Vec<RawRecord>>;

    /// Infers a schema from sample records.
    ///
    /// Default implementation delegates to the global
    /// [`FormatInferenceRegistry`](super::inference::FormatInferenceRegistry).
    ///
    /// # Errors
    ///
    /// Returns a schema error if inference fails.
    async fn infer_from_samples(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        super::inference::default_infer_from_samples(samples, config)
    }
}

// ── Inference types ────────────────────────────────────────────────

/// Configuration for schema inference.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Data format to use for inference.
    pub format: String,

    /// How to handle number type inference.
    pub number_inference: NumberInference,

    /// How to handle array type inference.
    pub array_inference: ArrayInference,

    /// Maximum number of samples to use.
    pub max_samples: usize,

    /// Minimum confidence threshold (0.0–1.0) for accepting an inferred type.
    pub min_confidence: f64,

    /// Type hints for specific fields.
    pub type_hints: HashMap<String, DataType>,

    /// Whether to treat empty strings as nulls.
    pub empty_as_null: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            format: "json".to_string(),
            number_inference: NumberInference::PreferLarger,
            array_inference: ArrayInference::Utf8,
            max_samples: 1000,
            min_confidence: 0.8,
            type_hints: HashMap::new(),
            empty_as_null: false,
        }
    }
}

impl InferenceConfig {
    /// Creates a new inference config for the given format.
    #[must_use]
    pub fn new(format: impl Into<String>) -> Self {
        Self {
            format: format.into(),
            ..Self::default()
        }
    }

    /// Sets the minimum confidence threshold.
    #[must_use]
    pub fn with_min_confidence(mut self, confidence: f64) -> Self {
        self.min_confidence = confidence;
        self
    }

    /// Sets the maximum number of samples.
    #[must_use]
    pub fn with_max_samples(mut self, n: usize) -> Self {
        self.max_samples = n;
        self
    }

    /// Adds a type hint for a specific field.
    #[must_use]
    pub fn with_type_hint(mut self, field: impl Into<String>, data_type: DataType) -> Self {
        self.type_hints.insert(field.into(), data_type);
        self
    }

    /// Enables treating empty strings as nulls.
    #[must_use]
    pub fn with_empty_as_null(mut self) -> Self {
        self.empty_as_null = true;
        self
    }
}

/// How to infer numeric types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberInference {
    /// Prefer the smallest type that fits (i32 before i64).
    PreferSmallest,
    /// Prefer larger types (always i64, always f64).
    PreferLarger,
}

/// How to infer array/object types in JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayInference {
    /// Store arrays/objects as JSON-encoded Utf8 strings.
    Utf8,
    /// Attempt to infer Arrow List / Struct types.
    NativeArrow,
}

/// The result of schema inference.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// The inferred Arrow schema.
    pub schema: SchemaRef,

    /// Overall confidence score (0.0–1.0).
    pub confidence: f64,

    /// Number of samples that were analyzed.
    pub sample_count: usize,

    /// Per-field inference details.
    pub field_details: Vec<FieldInferenceDetail>,

    /// Warnings generated during inference.
    pub warnings: Vec<InferenceWarning>,
}

/// Per-field detail from inference.
#[derive(Debug, Clone)]
pub struct FieldInferenceDetail {
    /// The field name.
    pub field_name: String,

    /// The inferred Arrow data type.
    pub inferred_type: DataType,

    /// Confidence for this specific field (0.0–1.0).
    pub confidence: f64,

    /// Number of non-null samples seen for this field.
    pub non_null_count: usize,

    /// Total number of samples that included this field.
    pub total_count: usize,

    /// Whether a type hint was applied.
    pub hint_applied: bool,
}

/// A warning generated during inference.
#[derive(Debug, Clone)]
pub struct InferenceWarning {
    /// The field this warning relates to, if any.
    pub field: Option<String>,

    /// Warning message.
    pub message: String,

    /// Severity level.
    pub severity: WarningSeverity,
}

/// Severity level for inference warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningSeverity {
    /// Informational — inference succeeded but with caveats.
    Info,
    /// Warning — inference may be inaccurate.
    Warning,
    /// Error — inference for this field failed; a fallback type was used.
    Error,
}

// ── SchemaRegistryAware ────────────────────────────────────────────

/// A connector that integrates with a schema registry.
///
/// Supports fetching, registering, and checking compatibility of schemas
/// against an external registry (e.g., Confluent Schema Registry).
#[async_trait]
pub trait SchemaRegistryAware: Send + Sync {
    /// Fetches the latest schema for a subject.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::RegistryError`](super::error::SchemaError::RegistryError)
    /// if the registry is unreachable or the subject is not found.
    async fn fetch_schema(&self, subject: &str) -> SchemaResult<RegisteredSchema>;

    /// Fetches a schema by its numeric ID.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the ID is not found.
    async fn fetch_schema_by_id(&self, schema_id: i32) -> SchemaResult<RegisteredSchema>;

    /// Checks whether `proposed` is compatible with the existing schema
    /// for `subject` under the configured compatibility mode.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the compatibility check itself fails.
    async fn check_compatibility(&self, subject: &str, proposed: &SchemaRef) -> SchemaResult<bool>;

    /// Registers a new schema version for a subject.
    ///
    /// # Errors
    ///
    /// Returns a schema error if registration fails.
    async fn register_schema(
        &self,
        subject: &str,
        schema: &SchemaRef,
    ) -> SchemaResult<RegisteredSchema>;

    /// Builds a [`FormatDecoder`] configured for the registry schema.
    ///
    /// This is a sync method because the decoder construction itself
    /// is not async — the registry lookup should happen beforehand.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the decoder cannot be constructed
    /// (e.g., unsupported schema type).
    fn build_registry_decoder(
        &self,
        schema: &RegisteredSchema,
    ) -> SchemaResult<Box<dyn FormatDecoder>>;
}

/// Configuration for connecting to a schema registry.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Registry URL.
    pub url: String,

    /// Schema type/format used by the registry.
    pub schema_type: RegistrySchemaType,

    /// Compatibility mode.
    pub compatibility: CompatibilityMode,

    /// Optional credentials.
    pub credentials: Option<RegistryCredentials>,
}

/// Schema type stored in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrySchemaType {
    /// Apache Avro.
    Avro,
    /// JSON Schema.
    JsonSchema,
    /// Protocol Buffers.
    Protobuf,
}

/// Compatibility mode for schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityMode {
    /// No compatibility checks.
    None,
    /// New schema can read old data.
    Backward,
    /// Old schema can read new data.
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

/// Credentials for schema registry authentication.
#[derive(Debug, Clone)]
pub struct RegistryCredentials {
    /// Username or API key.
    pub username: String,
    /// Password or API secret.
    pub password: String,
}

/// A schema registered in a schema registry.
#[derive(Debug, Clone)]
pub struct RegisteredSchema {
    /// Numeric schema ID assigned by the registry.
    pub id: i32,

    /// Schema version within its subject.
    pub version: i32,

    /// The subject this schema belongs to.
    pub subject: String,

    /// The Arrow schema.
    pub schema: SchemaRef,

    /// The schema type in the registry.
    pub schema_type: RegistrySchemaType,
}

// ── SchemaEvolvable ────────────────────────────────────────────────

/// A connector that supports schema evolution.
///
/// Provides diffing, evaluation, and application of schema changes.
pub trait SchemaEvolvable: Send + Sync {
    /// Computes the differences between two schemas.
    fn diff_schemas(&self, old: &SchemaRef, new: &SchemaRef) -> Vec<SchemaChange>;

    /// Evaluates whether a set of schema changes is acceptable.
    fn evaluate_evolution(&self, changes: &[SchemaChange]) -> EvolutionVerdict;

    /// Applies schema changes, returning a column projection that maps
    /// old data to the new schema.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the evolution cannot be applied.
    fn apply_evolution(
        &self,
        old: &SchemaRef,
        changes: &[SchemaChange],
    ) -> SchemaResult<ColumnProjection>;

    /// Returns the current schema version, if tracked.
    fn schema_version(&self) -> Option<u32> {
        None
    }
}

/// A single schema change detected by [`SchemaEvolvable::diff_schemas`].
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChange {
    /// A new column was added.
    ColumnAdded {
        /// Column name.
        name: String,
        /// The new column's data type.
        data_type: DataType,
        /// Whether the column is nullable.
        nullable: bool,
    },

    /// An existing column was removed.
    ColumnRemoved {
        /// Column name.
        name: String,
    },

    /// A column's data type changed.
    TypeChanged {
        /// Column name.
        name: String,
        /// Previous data type.
        old_type: DataType,
        /// New data type.
        new_type: DataType,
    },

    /// A column's nullability changed.
    NullabilityChanged {
        /// Column name.
        name: String,
        /// Previous nullable flag.
        was_nullable: bool,
        /// New nullable flag.
        now_nullable: bool,
    },

    /// A column was renamed.
    ColumnRenamed {
        /// Previous name.
        old_name: String,
        /// New name.
        new_name: String,
    },
}

/// The result of evaluating a set of schema changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionVerdict {
    /// All changes are compatible — evolution can proceed.
    Compatible,

    /// Changes require data migration but are feasible.
    RequiresMigration,

    /// Changes are incompatible — evolution is rejected.
    Incompatible(String),
}

/// Describes how to project columns from the old schema to the new schema.
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    /// For each column in the new schema, the index in the old schema
    /// (or `None` if the column is newly added and should be filled
    /// with the default/null).
    pub mappings: Vec<Option<usize>>,

    /// The resulting schema after projection.
    pub target_schema: SchemaRef,
}

// ── Connector config schema (self-describing connectors) ───────────

/// Self-describing connector configuration schema.
///
/// Allows connectors to declare the configuration options they accept,
/// enabling UI generation and validation.
#[derive(Debug, Clone)]
pub struct ConnectorConfigSchema {
    /// The connector type name.
    pub connector_type: String,

    /// Configuration options.
    pub options: Vec<ConfigOption>,
}

/// A single configuration option in a connector's config schema.
#[derive(Debug, Clone)]
pub struct ConfigOption {
    /// The option key.
    pub key: String,

    /// Human-readable description.
    pub description: String,

    /// Whether this option is required.
    pub required: bool,

    /// The expected value type.
    pub value_type: ConfigValueType,

    /// Default value, if any.
    pub default: Option<String>,
}

/// The expected type of a configuration value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigValueType {
    /// A string value.
    String,
    /// An integer value.
    Integer,
    /// A floating-point value.
    Float,
    /// A boolean value.
    Boolean,
    /// A duration (e.g., `"30s"`, `"5m"`).
    Duration,
    /// A URL.
    Url,
    /// A file path.
    Path,
}

// ── Object-safety assertions ───────────────────────────────────────

// Compile-time checks that all six traits are object-safe.
const _: () = {
    fn _assert_format_decoder_object_safe(_: &dyn FormatDecoder) {}
    fn _assert_format_encoder_object_safe(_: &dyn FormatEncoder) {}
    fn _assert_schema_provider_object_safe(_: &dyn SchemaProvider) {}
    fn _assert_schema_inferable_object_safe(_: &dyn SchemaInferable) {}
    fn _assert_schema_registry_aware_object_safe(_: &dyn SchemaRegistryAware) {}
    fn _assert_schema_evolvable_object_safe(_: &dyn SchemaEvolvable) {}
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_schema::{Field, Schema};

    #[test]
    fn test_inference_config_defaults() {
        let cfg = InferenceConfig::default();
        assert_eq!(cfg.format, "json");
        assert_eq!(cfg.max_samples, 1000);
        assert!((cfg.min_confidence - 0.8).abs() < f64::EPSILON);
        assert!(cfg.type_hints.is_empty());
    }

    #[test]
    fn test_inference_config_builder() {
        let cfg = InferenceConfig::new("csv")
            .with_min_confidence(0.9)
            .with_max_samples(500)
            .with_type_hint("id", DataType::Int32)
            .with_empty_as_null();

        assert_eq!(cfg.format, "csv");
        assert!((cfg.min_confidence - 0.9).abs() < f64::EPSILON);
        assert_eq!(cfg.max_samples, 500);
        assert_eq!(cfg.type_hints.get("id"), Some(&DataType::Int32));
        assert!(cfg.empty_as_null);
    }

    #[test]
    fn test_inferred_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let inferred = InferredSchema {
            schema: schema.clone(),
            confidence: 0.95,
            sample_count: 100,
            field_details: vec![
                FieldInferenceDetail {
                    field_name: "id".into(),
                    inferred_type: DataType::Int64,
                    confidence: 1.0,
                    non_null_count: 100,
                    total_count: 100,
                    hint_applied: false,
                },
                FieldInferenceDetail {
                    field_name: "name".into(),
                    inferred_type: DataType::Utf8,
                    confidence: 0.9,
                    non_null_count: 90,
                    total_count: 100,
                    hint_applied: false,
                },
            ],
            warnings: vec![],
        };

        assert_eq!(inferred.schema.fields().len(), 2);
        assert!((inferred.confidence - 0.95).abs() < f64::EPSILON);
        assert_eq!(inferred.field_details.len(), 2);
    }

    #[test]
    fn test_schema_change_variants() {
        let changes = vec![
            SchemaChange::ColumnAdded {
                name: "email".into(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            SchemaChange::ColumnRemoved {
                name: "legacy".into(),
            },
            SchemaChange::TypeChanged {
                name: "age".into(),
                old_type: DataType::Int32,
                new_type: DataType::Int64,
            },
            SchemaChange::NullabilityChanged {
                name: "name".into(),
                was_nullable: false,
                now_nullable: true,
            },
            SchemaChange::ColumnRenamed {
                old_name: "fname".into(),
                new_name: "first_name".into(),
            },
        ];
        assert_eq!(changes.len(), 5);
    }

    #[test]
    fn test_evolution_verdict() {
        assert_eq!(EvolutionVerdict::Compatible, EvolutionVerdict::Compatible);
        assert_ne!(
            EvolutionVerdict::Compatible,
            EvolutionVerdict::RequiresMigration
        );
    }

    #[test]
    fn test_column_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
        ]));

        let proj = ColumnProjection {
            mappings: vec![Some(0), None, Some(1)],
            target_schema: schema,
        };

        assert_eq!(proj.mappings.len(), 3);
        assert_eq!(proj.mappings[0], Some(0));
        assert_eq!(proj.mappings[1], None); // new column
        assert_eq!(proj.mappings[2], Some(1));
    }

    #[test]
    fn test_warning_severity() {
        let w = InferenceWarning {
            field: Some("price".into()),
            message: "mixed int/float".into(),
            severity: WarningSeverity::Warning,
        };
        assert_eq!(w.severity, WarningSeverity::Warning);
        assert_eq!(w.field.as_deref(), Some("price"));
    }

    #[test]
    fn test_registry_config() {
        let cfg = RegistryConfig {
            url: "http://localhost:8081".into(),
            schema_type: RegistrySchemaType::Avro,
            compatibility: CompatibilityMode::Backward,
            credentials: Some(RegistryCredentials {
                username: "user".into(),
                password: "pass".into(),
            }),
        };
        assert_eq!(cfg.schema_type, RegistrySchemaType::Avro);
        assert_eq!(cfg.compatibility, CompatibilityMode::Backward);
        assert!(cfg.credentials.is_some());
    }

    #[test]
    fn test_connector_config_schema() {
        let schema = ConnectorConfigSchema {
            connector_type: "kafka".into(),
            options: vec![
                ConfigOption {
                    key: "bootstrap.servers".into(),
                    description: "Kafka broker addresses".into(),
                    required: true,
                    value_type: ConfigValueType::String,
                    default: None,
                },
                ConfigOption {
                    key: "batch.size".into(),
                    description: "Batch size".into(),
                    required: false,
                    value_type: ConfigValueType::Integer,
                    default: Some("1000".into()),
                },
            ],
        };
        assert_eq!(schema.options.len(), 2);
        assert!(schema.options[0].required);
        assert!(!schema.options[1].required);
    }
}
