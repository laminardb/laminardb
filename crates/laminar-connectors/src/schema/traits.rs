//! Capability traits for the connector schema framework.
#![allow(clippy::disallowed_types)] // cold path: schema management

use std::collections::HashMap;

use super::error::SchemaResult;
use super::types::RawRecord;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};

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

pub use laminar_core::error_codes::WarningSeverity;

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

/// A single schema change detected by schema diffing.
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

// ── Object-safety assertions ───────────────────────────────────────

// Compile-time checks that the codec traits are object-safe.
const _: () = {
    fn _assert_format_decoder_object_safe(_: &dyn FormatDecoder) {}
    fn _assert_format_encoder_object_safe(_: &dyn FormatEncoder) {}
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
        let changes = [
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
}
