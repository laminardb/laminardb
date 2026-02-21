//! Format inference registry and built-in format inferencers.
//!
//! Provides:
//! - [`FormatInference`] trait for format-specific schema inference
//! - [`FormatInferenceRegistry`] for registering and looking up inferencers
//! - Built-in implementations for JSON, CSV, and raw formats
//! - [`default_infer_from_samples`] free function used by the default
//!   [`SchemaInferable`](super::traits::SchemaInferable) implementation

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use arrow_schema::{DataType, Field, Schema};

use super::error::{SchemaError, SchemaResult};
use super::traits::{
    FieldInferenceDetail, InferenceConfig, InferenceWarning, InferredSchema, WarningSeverity,
};
use super::types::RawRecord;

/// Trait for format-specific schema inference.
///
/// Each implementation handles a single data format (JSON, CSV, etc.)
/// and can infer an Arrow schema from sample records.
pub trait FormatInference: Send + Sync {
    /// Returns the format name this inferencer handles (e.g., `"json"`).
    fn format_name(&self) -> &'static str;

    /// Infers a schema from sample raw records.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InferenceFailed`] if the samples cannot be
    /// parsed or the schema cannot be determined.
    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema>;
}

/// Registry of format inferencers.
///
/// Thread-safe registry that maps format names to [`FormatInference`]
/// implementations. A global instance is available via
/// [`FORMAT_INFERENCE_REGISTRY`].
pub struct FormatInferenceRegistry {
    inferencers: RwLock<HashMap<String, Arc<dyn FormatInference>>>,
}

impl FormatInferenceRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inferencers: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a format inferencer.
    ///
    /// If an inferencer for the same format already exists, it is replaced.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn register(&self, inferencer: Arc<dyn FormatInference>) {
        let name = inferencer.format_name().to_string();
        self.inferencers
            .write()
            .expect("inference registry poisoned")
            .insert(name, inferencer);
    }

    /// Gets the inferencer for a format, if registered.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, format: &str) -> Option<Arc<dyn FormatInference>> {
        self.inferencers
            .read()
            .expect("inference registry poisoned")
            .get(format)
            .cloned()
    }

    /// Returns the names of all registered formats.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn registered_formats(&self) -> Vec<String> {
        self.inferencers
            .read()
            .expect("inference registry poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

impl Default for FormatInferenceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global format inference registry, pre-populated with JSON, CSV, and raw.
pub static FORMAT_INFERENCE_REGISTRY: LazyLock<FormatInferenceRegistry> = LazyLock::new(|| {
    let registry = FormatInferenceRegistry::new();
    registry.register(Arc::new(JsonFormatInference));
    registry.register(Arc::new(CsvFormatInference));
    registry.register(Arc::new(RawFormatInference));
    registry
});

/// Default inference implementation that delegates to the global registry.
///
/// Used by the default [`SchemaInferable::infer_from_samples`](super::traits::SchemaInferable::infer_from_samples).
///
/// # Errors
///
/// Returns [`SchemaError::InferenceFailed`] if no inferencer is registered
/// for the requested format, or if inference itself fails.
pub fn default_infer_from_samples(
    samples: &[RawRecord],
    config: &InferenceConfig,
) -> SchemaResult<InferredSchema> {
    let inferencer = FORMAT_INFERENCE_REGISTRY
        .get(&config.format)
        .ok_or_else(|| {
            SchemaError::InferenceFailed(format!(
                "no inferencer registered for format '{}'",
                config.format
            ))
        })?;
    inferencer.infer(samples, config)
}

// ── JSON inference ─────────────────────────────────────────────────

/// JSON format schema inferencer.
///
/// Parses each record's value as a JSON object and merges field types
/// across all samples. Ported from `sdk::schema::infer_schema_from_json`
/// with added confidence scoring.
pub struct JsonFormatInference;

#[allow(clippy::cast_precision_loss)]
impl FormatInference for JsonFormatInference {
    fn format_name(&self) -> &'static str {
        "json"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        if samples.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "cannot infer schema from zero samples".into(),
            ));
        }

        let limit = samples.len().min(config.max_samples);
        let samples = &samples[..limit];

        let mut field_types: HashMap<String, Vec<InferredType>> = HashMap::new();
        let mut field_order: Vec<String> = Vec::new();
        let mut warnings: Vec<InferenceWarning> = Vec::new();

        for (i, record) in samples.iter().enumerate() {
            let value: serde_json::Value = serde_json::from_slice(&record.value).map_err(|e| {
                SchemaError::InferenceFailed(format!("JSON parse error in sample {i}: {e}"))
            })?;

            let obj = value.as_object().ok_or_else(|| {
                SchemaError::InferenceFailed(format!(
                    "sample {i}: expected JSON object, got {}",
                    json_type_name(&value)
                ))
            })?;

            for (key, val) in obj {
                if !field_types.contains_key(key) {
                    field_order.push(key.clone());
                    field_types.insert(key.clone(), Vec::with_capacity(samples.len()));
                }
                let inferred = infer_type_from_json(val, config.empty_as_null);
                field_types.get_mut(key).unwrap().push(inferred);
            }
        }

        let total = samples.len();
        let mut fields: Vec<Field> = Vec::new();
        let mut details: Vec<FieldInferenceDetail> = Vec::new();

        for name in &field_order {
            let types = &field_types[name];

            // Check for a type hint.
            let (data_type, hint_applied) = if let Some(hint) = config.type_hints.get(name) {
                (hint.clone(), true)
            } else {
                (merge_types(types), false)
            };

            let non_null_count = types
                .iter()
                .filter(|t| !matches!(t, InferredType::Null))
                .count();
            let nullable =
                types.iter().any(|t| matches!(t, InferredType::Null)) || types.len() < total;

            let field_confidence = if types.is_empty() {
                0.0
            } else {
                let consistent_count = types
                    .iter()
                    .filter(|t| !matches!(t, InferredType::Null))
                    .filter(|t| inferred_to_arrow(t) == data_type)
                    .count();
                if non_null_count == 0 {
                    0.5 // all nulls — low confidence
                } else {
                    consistent_count as f64 / non_null_count as f64
                }
            };

            if field_confidence < config.min_confidence && !hint_applied {
                warnings.push(InferenceWarning {
                    field: Some(name.clone()),
                    message: format!(
                        "low confidence {field_confidence:.2} for field '{name}', \
                         falling back to Utf8"
                    ),
                    severity: WarningSeverity::Warning,
                });
            }

            fields.push(Field::new(name, data_type.clone(), nullable));
            details.push(FieldInferenceDetail {
                field_name: name.clone(),
                inferred_type: data_type,
                confidence: field_confidence,
                non_null_count,
                total_count: types.len(),
                hint_applied,
            });
        }

        if fields.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "no fields could be inferred from JSON samples".into(),
            ));
        }

        let overall_confidence = if details.is_empty() {
            0.0
        } else {
            details.iter().map(|d| d.confidence).sum::<f64>() / details.len() as f64
        };

        Ok(InferredSchema {
            schema: Arc::new(Schema::new(fields)),
            confidence: overall_confidence,
            sample_count: total,
            field_details: details,
            warnings,
        })
    }
}

// ── CSV inference ──────────────────────────────────────────────────

/// CSV format schema inferencer.
///
/// Treats the first sample's value as a header row and subsequent samples
/// as data rows. Ported from `sdk::schema::infer_schema_from_csv` with
/// added confidence scoring.
pub struct CsvFormatInference;

#[allow(clippy::cast_precision_loss)]
impl FormatInference for CsvFormatInference {
    fn format_name(&self) -> &'static str {
        "csv"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        if samples.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "cannot infer schema from zero samples".into(),
            ));
        }

        let limit = samples.len().min(config.max_samples);
        let samples = &samples[..limit];

        // First sample contains headers.
        let header_line = std::str::from_utf8(&samples[0].value).map_err(|e| {
            SchemaError::InferenceFailed(format!("invalid UTF-8 in CSV header: {e}"))
        })?;

        let headers: Vec<String> = header_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mut field_types: HashMap<String, Vec<InferredType>> = HashMap::new();
        for header in &headers {
            field_types.insert(header.clone(), Vec::with_capacity(samples.len()));
        }

        let mut warnings: Vec<InferenceWarning> = Vec::new();

        for record in samples.iter().skip(1) {
            let line = std::str::from_utf8(&record.value).map_err(|e| {
                SchemaError::InferenceFailed(format!("invalid UTF-8 in CSV row: {e}"))
            })?;

            let values: Vec<&str> = line.split(',').map(str::trim).collect();

            if values.len() != headers.len() {
                warnings.push(InferenceWarning {
                    field: None,
                    message: format!(
                        "column count mismatch: expected {}, got {}",
                        headers.len(),
                        values.len()
                    ),
                    severity: WarningSeverity::Warning,
                });
            }

            for (i, value) in values.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    let inferred = infer_type_from_string(value, config.empty_as_null);
                    if let Some(types) = field_types.get_mut(header) {
                        types.push(inferred);
                    }
                }
            }
        }

        let data_rows = samples.len().saturating_sub(1);
        let mut fields: Vec<Field> = Vec::new();
        let mut details: Vec<FieldInferenceDetail> = Vec::new();

        for name in &headers {
            let types = &field_types[name];

            let (data_type, hint_applied) = if let Some(hint) = config.type_hints.get(name) {
                (hint.clone(), true)
            } else if types.is_empty() {
                (DataType::Utf8, false) // no data rows — default to string
            } else {
                (merge_types(types), false)
            };

            let non_null_count = types
                .iter()
                .filter(|t| !matches!(t, InferredType::Null))
                .count();
            let nullable = types.iter().any(|t| matches!(t, InferredType::Null));

            let field_confidence = if types.is_empty() {
                0.5 // headers only
            } else {
                let consistent = types
                    .iter()
                    .filter(|t| !matches!(t, InferredType::Null))
                    .filter(|t| inferred_to_arrow(t) == data_type)
                    .count();
                if non_null_count == 0 {
                    0.5
                } else {
                    consistent as f64 / non_null_count as f64
                }
            };

            fields.push(Field::new(name, data_type.clone(), nullable));
            details.push(FieldInferenceDetail {
                field_name: name.clone(),
                inferred_type: data_type,
                confidence: field_confidence,
                non_null_count,
                total_count: types.len(),
                hint_applied,
            });
        }

        let overall_confidence = if details.is_empty() {
            0.0
        } else {
            details.iter().map(|d| d.confidence).sum::<f64>() / details.len() as f64
        };

        Ok(InferredSchema {
            schema: Arc::new(Schema::new(fields)),
            confidence: overall_confidence,
            sample_count: data_rows,
            field_details: details,
            warnings,
        })
    }
}

// ── Raw inference ──────────────────────────────────────────────────

/// Raw format inferencer.
///
/// Always returns a single `Binary` column named `"value"`.
/// No actual inference is needed — the schema is fixed.
pub struct RawFormatInference;

impl FormatInference for RawFormatInference {
    fn format_name(&self) -> &'static str {
        "raw"
    }

    fn infer(
        &self,
        samples: &[RawRecord],
        _config: &InferenceConfig,
    ) -> SchemaResult<InferredSchema> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Binary,
            false,
        )]));

        Ok(InferredSchema {
            schema,
            confidence: 1.0,
            sample_count: samples.len(),
            field_details: vec![FieldInferenceDetail {
                field_name: "value".into(),
                inferred_type: DataType::Binary,
                confidence: 1.0,
                non_null_count: samples.len(),
                total_count: samples.len(),
                hint_applied: false,
            }],
            warnings: vec![],
        })
    }
}

// ── Internal helpers ───────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum InferredType {
    Null,
    Bool,
    Int,
    Float,
    String,
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn infer_type_from_json(value: &serde_json::Value, empty_as_null: bool) -> InferredType {
    match value {
        serde_json::Value::Null => InferredType::Null,
        serde_json::Value::Bool(_) => InferredType::Bool,
        serde_json::Value::Number(n) => {
            if n.is_f64() && !n.is_i64() && !n.is_u64() {
                InferredType::Float
            } else {
                InferredType::Int
            }
        }
        serde_json::Value::String(s) => {
            if empty_as_null && s.is_empty() {
                InferredType::Null
            } else {
                InferredType::String
            }
        }
        // Arrays and objects are serialized as JSON strings.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => InferredType::String,
    }
}

fn infer_type_from_string(value: &str, empty_as_null: bool) -> InferredType {
    if value.is_empty() {
        return if empty_as_null {
            InferredType::Null
        } else {
            InferredType::String
        };
    }

    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return InferredType::Bool;
    }

    if value.parse::<i64>().is_ok() {
        return InferredType::Int;
    }

    if value.parse::<f64>().is_ok() {
        return InferredType::Float;
    }

    InferredType::String
}

fn merge_types(types: &[InferredType]) -> DataType {
    let non_null: Vec<_> = types
        .iter()
        .filter(|t| !matches!(t, InferredType::Null))
        .collect();

    if non_null.is_empty() {
        return DataType::Utf8;
    }

    let first = non_null[0];
    if non_null.iter().all(|t| *t == first) {
        return inferred_to_arrow(first);
    }

    // Int + Float → Float.
    let has_float = non_null.iter().any(|t| matches!(t, InferredType::Float));
    let has_int = non_null.iter().any(|t| matches!(t, InferredType::Int));
    let has_other = non_null
        .iter()
        .any(|t| !matches!(t, InferredType::Int | InferredType::Float));

    if has_float && has_int && !has_other {
        return DataType::Float64;
    }

    // Mixed types → Utf8.
    DataType::Utf8
}

fn inferred_to_arrow(t: &InferredType) -> DataType {
    match t {
        InferredType::Bool => DataType::Boolean,
        InferredType::Int => DataType::Int64,
        InferredType::Float => DataType::Float64,
        InferredType::Null | InferredType::String => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_record(json: &str) -> RawRecord {
        RawRecord::new(json.as_bytes().to_vec())
    }

    fn csv_record(line: &str) -> RawRecord {
        RawRecord::new(line.as_bytes().to_vec())
    }

    // ── Registry tests ─────────────────────────────────────────

    #[test]
    fn test_registry_has_builtins() {
        let formats = FORMAT_INFERENCE_REGISTRY.registered_formats();
        assert!(formats.contains(&"json".to_string()));
        assert!(formats.contains(&"csv".to_string()));
        assert!(formats.contains(&"raw".to_string()));
    }

    #[test]
    fn test_registry_get_json() {
        let inf = FORMAT_INFERENCE_REGISTRY.get("json");
        assert!(inf.is_some());
        assert_eq!(inf.unwrap().format_name(), "json");
    }

    #[test]
    fn test_registry_get_unknown() {
        assert!(FORMAT_INFERENCE_REGISTRY.get("protobuf").is_none());
    }

    #[test]
    fn test_default_infer_unknown_format() {
        let cfg = InferenceConfig::new("xml");
        let result = default_infer_from_samples(&[], &cfg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("xml"));
    }

    // ── JSON inference tests ───────────────────────────────────

    #[test]
    fn test_json_infer_basic() {
        let samples = vec![
            json_record(r#"{"id": 1, "name": "Alice"}"#),
            json_record(r#"{"id": 2, "name": "Bob"}"#),
        ];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(result.schema.fields().len(), 2);
        assert_eq!(
            result.schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            result.schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(result.sample_count, 2);
        assert!(result.confidence > 0.9);
    }

    #[test]
    fn test_json_infer_types() {
        let samples = vec![json_record(
            r#"{"int": 42, "float": 3.14, "bool": true, "str": "hello"}"#,
        )];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(
            result.schema.field_with_name("int").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            result.schema.field_with_name("float").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            result.schema.field_with_name("bool").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            result.schema.field_with_name("str").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_json_infer_nullable() {
        let samples = vec![
            json_record(r#"{"value": 1}"#),
            json_record(r#"{"value": null}"#),
        ];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();
        assert!(result
            .schema
            .field_with_name("value")
            .unwrap()
            .is_nullable());
    }

    #[test]
    fn test_json_infer_mixed_int_float() {
        let samples = vec![
            json_record(r#"{"value": 1}"#),
            json_record(r#"{"value": 2.5}"#),
        ];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();
        assert_eq!(
            result.schema.field_with_name("value").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn test_json_infer_type_hint() {
        let samples = vec![json_record(r#"{"id": 42}"#)];
        let cfg = InferenceConfig::new("json").with_type_hint("id", DataType::Int32);
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();
        assert_eq!(
            result.schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int32
        );
        assert!(result.field_details[0].hint_applied);
    }

    #[test]
    fn test_json_infer_empty_error() {
        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&[], &cfg);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_infer_empty_as_null() {
        let samples = vec![
            json_record(r#"{"value": ""}"#),
            json_record(r#"{"value": "text"}"#),
        ];
        let cfg = InferenceConfig::new("json").with_empty_as_null();
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();
        assert!(result
            .schema
            .field_with_name("value")
            .unwrap()
            .is_nullable());
    }

    #[test]
    fn test_json_infer_confidence_details() {
        let samples = vec![
            json_record(r#"{"a": 1, "b": "x"}"#),
            json_record(r#"{"a": 2, "b": "y"}"#),
            json_record(r#"{"a": 3, "b": "z"}"#),
        ];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(result.field_details.len(), 2);
        for detail in &result.field_details {
            assert!((detail.confidence - 1.0).abs() < f64::EPSILON);
            assert_eq!(detail.non_null_count, 3);
            assert_eq!(detail.total_count, 3);
        }
    }

    #[test]
    fn test_json_infer_missing_field_nullable() {
        let samples = vec![
            json_record(r#"{"a": 1, "b": 2}"#),
            json_record(r#"{"a": 3}"#), // "b" missing
        ];

        let cfg = InferenceConfig::new("json");
        let result = JsonFormatInference.infer(&samples, &cfg).unwrap();
        // "b" is only seen once out of 2 samples → nullable
        assert!(result.schema.field_with_name("b").unwrap().is_nullable());
    }

    // ── CSV inference tests ────────────────────────────────────

    #[test]
    fn test_csv_infer_basic() {
        let samples = vec![
            csv_record("id,name,age"),
            csv_record("1,Alice,30"),
            csv_record("2,Bob,25"),
        ];

        let cfg = InferenceConfig::new("csv");
        let result = CsvFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(result.schema.fields().len(), 3);
        assert_eq!(
            result.schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            result.schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            result.schema.field_with_name("age").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(result.sample_count, 2); // 2 data rows
    }

    #[test]
    fn test_csv_infer_types() {
        let samples = vec![
            csv_record("int_col,float_col,bool_col,str_col"),
            csv_record("42,3.14,true,hello"),
            csv_record("100,2.71,false,world"),
        ];

        let cfg = InferenceConfig::new("csv");
        let result = CsvFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(
            result
                .schema
                .field_with_name("int_col")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        assert_eq!(
            result
                .schema
                .field_with_name("float_col")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            result
                .schema
                .field_with_name("bool_col")
                .unwrap()
                .data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            result
                .schema
                .field_with_name("str_col")
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_csv_infer_headers_only() {
        let samples = vec![csv_record("a,b,c")];

        let cfg = InferenceConfig::new("csv");
        let result = CsvFormatInference.infer(&samples, &cfg).unwrap();
        assert_eq!(result.schema.fields().len(), 3);
        // All default to Utf8 with no data.
        for field in result.schema.fields() {
            assert_eq!(field.data_type(), &DataType::Utf8);
        }
    }

    #[test]
    fn test_csv_infer_empty_error() {
        let cfg = InferenceConfig::new("csv");
        assert!(CsvFormatInference.infer(&[], &cfg).is_err());
    }

    // ── Raw inference tests ────────────────────────────────────

    #[test]
    fn test_raw_infer() {
        let samples = vec![
            RawRecord::new(b"hello".to_vec()),
            RawRecord::new(b"world".to_vec()),
        ];

        let cfg = InferenceConfig::new("raw");
        let result = RawFormatInference.infer(&samples, &cfg).unwrap();

        assert_eq!(result.schema.fields().len(), 1);
        assert_eq!(
            result.schema.field_with_name("value").unwrap().data_type(),
            &DataType::Binary
        );
        assert!((result.confidence - 1.0).abs() < f64::EPSILON);
        assert_eq!(result.sample_count, 2);
    }

    // ── default_infer_from_samples tests ───────────────────────

    #[test]
    fn test_default_infer_json() {
        let samples = vec![json_record(r#"{"x": 1}"#)];
        let cfg = InferenceConfig::new("json");
        let result = default_infer_from_samples(&samples, &cfg).unwrap();
        assert_eq!(result.schema.fields().len(), 1);
    }

    #[test]
    fn test_default_infer_csv() {
        let samples = vec![csv_record("col1"), csv_record("42")];
        let cfg = InferenceConfig::new("csv");
        let result = default_infer_from_samples(&samples, &cfg).unwrap();
        assert_eq!(result.schema.fields().len(), 1);
    }

    // ── Helper unit tests ──────────────────────────────────────

    #[test]
    fn test_merge_types_all_same() {
        let types = vec![InferredType::Int, InferredType::Int, InferredType::Int];
        assert_eq!(merge_types(&types), DataType::Int64);
    }

    #[test]
    fn test_merge_types_int_float() {
        let types = vec![InferredType::Int, InferredType::Float];
        assert_eq!(merge_types(&types), DataType::Float64);
    }

    #[test]
    fn test_merge_types_mixed() {
        let types = vec![InferredType::Int, InferredType::String];
        assert_eq!(merge_types(&types), DataType::Utf8);
    }

    #[test]
    fn test_merge_types_all_null() {
        let types = vec![InferredType::Null, InferredType::Null];
        assert_eq!(merge_types(&types), DataType::Utf8);
    }

    #[test]
    fn test_infer_type_from_string_cases() {
        assert_eq!(infer_type_from_string("42", false), InferredType::Int);
        assert_eq!(infer_type_from_string("3.14", false), InferredType::Float);
        assert_eq!(infer_type_from_string("true", false), InferredType::Bool);
        assert_eq!(infer_type_from_string("hello", false), InferredType::String);
        assert_eq!(infer_type_from_string("", false), InferredType::String);
        assert_eq!(infer_type_from_string("", true), InferredType::Null);
    }
}
