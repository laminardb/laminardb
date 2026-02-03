//! Schema discovery and inference.
//!
//! Provides utilities for inferring Arrow schemas from sample data.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::ConnectorError;
use crate::serde::Format;

/// Hints for schema discovery.
#[derive(Debug, Clone, Default)]
pub struct SchemaDiscoveryHints {
    /// Expected field names (in order, if known).
    pub field_names: Option<Vec<String>>,
    /// Type hints for specific fields.
    pub type_hints: HashMap<String, DataType>,
    /// Fields that should be nullable.
    pub nullable_fields: Vec<String>,
    /// Whether to prefer larger types (e.g., i64 over i32).
    pub prefer_larger_types: bool,
    /// Whether to treat empty strings as nulls.
    pub empty_as_null: bool,
    /// Maximum string length before considering as large string.
    pub large_string_threshold: Option<usize>,
}

impl SchemaDiscoveryHints {
    /// Creates new hints with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a type hint for a field.
    #[must_use]
    pub fn with_type_hint(mut self, field: impl Into<String>, data_type: DataType) -> Self {
        self.type_hints.insert(field.into(), data_type);
        self
    }

    /// Adds a nullable field hint.
    #[must_use]
    pub fn with_nullable(mut self, field: impl Into<String>) -> Self {
        self.nullable_fields.push(field.into());
        self
    }

    /// Sets expected field names.
    #[must_use]
    pub fn with_field_names(mut self, names: Vec<String>) -> Self {
        self.field_names = Some(names);
        self
    }

    /// Enables preferring larger types.
    #[must_use]
    pub fn prefer_larger_types(mut self) -> Self {
        self.prefer_larger_types = true;
        self
    }

    /// Enables treating empty strings as nulls.
    #[must_use]
    pub fn empty_as_null(mut self) -> Self {
        self.empty_as_null = true;
        self
    }
}

/// Infers an Arrow schema from sample data.
///
/// Examines the provided samples to determine field types. The format
/// parameter indicates how to parse the raw bytes.
///
/// # Arguments
///
/// * `samples` - Sample records as raw bytes
/// * `format` - Data format (JSON, CSV, etc.)
/// * `hints` - Optional hints to guide inference
///
/// # Errors
///
/// Returns `ConnectorError` if samples cannot be parsed or schema cannot
/// be inferred.
///
/// # Example
///
/// ```rust,ignore
/// let samples = vec![
///     br#"{"id": 1, "name": "Alice"}"#.as_slice(),
///     br#"{"id": 2, "name": "Bob"}"#.as_slice(),
/// ];
/// let schema = infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::default())?;
/// ```
pub fn infer_schema_from_samples(
    samples: &[&[u8]],
    format: Format,
    hints: &SchemaDiscoveryHints,
) -> Result<SchemaRef, ConnectorError> {
    if samples.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "cannot infer schema from empty samples".to_string(),
        ));
    }

    match format {
        Format::Json | Format::Debezium => infer_schema_from_json(samples, hints),
        Format::Csv => infer_schema_from_csv(samples, hints),
        Format::Raw => Err(ConnectorError::ConfigurationError(
            "cannot infer schema from raw format".to_string(),
        )),
        Format::Avro => Err(ConnectorError::ConfigurationError(
            "Avro schema discovery not supported - Avro schemas are embedded in the data"
                .to_string(),
        )),
    }
}

fn infer_schema_from_json(
    samples: &[&[u8]],
    hints: &SchemaDiscoveryHints,
) -> Result<SchemaRef, ConnectorError> {
    use serde_json::Value;

    let mut field_types: HashMap<String, Vec<InferredType>> = HashMap::new();
    let mut field_order: Vec<String> = Vec::new();

    for sample in samples {
        let value: Value = serde_json::from_slice(sample).map_err(|e| {
            ConnectorError::ConfigurationError(format!("failed to parse JSON sample: {e}"))
        })?;

        let obj = value.as_object().ok_or_else(|| {
            ConnectorError::ConfigurationError("JSON sample must be an object".to_string())
        })?;

        for (key, val) in obj {
            if !field_types.contains_key(key) {
                field_order.push(key.clone());
                field_types.insert(key.clone(), Vec::new());
            }

            let inferred = infer_type_from_json_value(val, hints);
            field_types.get_mut(key).unwrap().push(inferred);
        }
    }

    // Use hint field order if provided, otherwise discovered order
    let field_names = hints.field_names.clone().unwrap_or(field_order);

    let fields: Vec<Field> = field_names
        .iter()
        .filter_map(|name| {
            let types = field_types.get(name)?;

            // Check for type hint
            let data_type = if let Some(hint) = hints.type_hints.get(name) {
                hint.clone()
            } else {
                merge_inferred_types(types, hints)
            };

            let nullable = hints.nullable_fields.contains(name)
                || types.iter().any(|t| matches!(t, InferredType::Null));

            Some(Field::new(name, data_type, nullable))
        })
        .collect();

    if fields.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "no fields could be inferred from samples".to_string(),
        ));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn infer_schema_from_csv(
    samples: &[&[u8]],
    hints: &SchemaDiscoveryHints,
) -> Result<SchemaRef, ConnectorError> {
    // For CSV, first line is typically headers
    let first_sample = samples
        .first()
        .ok_or_else(|| ConnectorError::ConfigurationError("no samples provided".to_string()))?;

    let first_line = std::str::from_utf8(first_sample)
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid UTF-8 in CSV: {e}")))?;

    // Parse headers (or use hints)
    let headers: Vec<String> = hints.field_names.clone().unwrap_or_else(|| {
        first_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    });

    // Analyze remaining samples to infer types
    let mut field_types: HashMap<String, Vec<InferredType>> = HashMap::new();
    for header in &headers {
        field_types.insert(header.clone(), Vec::new());
    }

    for sample in samples.iter().skip(1) {
        let line = std::str::from_utf8(sample).map_err(|e| {
            ConnectorError::ConfigurationError(format!("invalid UTF-8 in CSV: {e}"))
        })?;

        let values: Vec<&str> = line.split(',').map(str::trim).collect();

        for (i, value) in values.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                let inferred = infer_type_from_string(value, hints);
                if let Some(types) = field_types.get_mut(header) {
                    types.push(inferred);
                }
            }
        }
    }

    let fields: Vec<Field> = headers
        .iter()
        .map(|name| {
            let types = field_types.get(name).cloned().unwrap_or_default();

            let data_type = if let Some(hint) = hints.type_hints.get(name) {
                hint.clone()
            } else if types.is_empty() {
                DataType::Utf8 // Default to string if no data samples
            } else {
                merge_inferred_types(&types, hints)
            };

            let nullable = hints.nullable_fields.contains(name)
                || types.iter().any(|t| matches!(t, InferredType::Null));

            Field::new(name, data_type, nullable)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

#[derive(Debug, Clone, PartialEq)]
enum InferredType {
    Null,
    Bool,
    Int,
    Float,
    String,
    Object,
    Array,
}

fn infer_type_from_json_value(
    value: &serde_json::Value,
    hints: &SchemaDiscoveryHints,
) -> InferredType {
    match value {
        serde_json::Value::Null => InferredType::Null,
        serde_json::Value::Bool(_) => InferredType::Bool,
        serde_json::Value::Number(n) => {
            if n.is_f64() {
                InferredType::Float
            } else {
                InferredType::Int
            }
        }
        serde_json::Value::String(s) => {
            if hints.empty_as_null && s.is_empty() {
                InferredType::Null
            } else {
                InferredType::String
            }
        }
        serde_json::Value::Array(_) => InferredType::Array,
        serde_json::Value::Object(_) => InferredType::Object,
    }
}

fn infer_type_from_string(value: &str, hints: &SchemaDiscoveryHints) -> InferredType {
    if value.is_empty() {
        return if hints.empty_as_null {
            InferredType::Null
        } else {
            InferredType::String
        };
    }

    // Try parsing as bool
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return InferredType::Bool;
    }

    // Try parsing as integer
    if value.parse::<i64>().is_ok() {
        return InferredType::Int;
    }

    // Try parsing as float
    if value.parse::<f64>().is_ok() {
        return InferredType::Float;
    }

    InferredType::String
}

fn merge_inferred_types(types: &[InferredType], hints: &SchemaDiscoveryHints) -> DataType {
    // Remove nulls for type determination
    let non_null: Vec<_> = types
        .iter()
        .filter(|t| !matches!(t, InferredType::Null))
        .collect();

    if non_null.is_empty() {
        // All nulls - default to string
        return DataType::Utf8;
    }

    // Check for consistency
    let first = non_null.first().unwrap();
    let all_same = non_null.iter().all(|t| *t == *first);

    if all_same {
        return inferred_to_arrow(first, hints);
    }

    // Mixed types - need to find common type
    let has_float = non_null.iter().any(|t| matches!(t, InferredType::Float));
    let has_int = non_null.iter().any(|t| matches!(t, InferredType::Int));

    if has_float && has_int && !non_null.iter().any(|t| matches!(t, InferredType::String)) {
        // Int + Float = Float
        return DataType::Float64;
    }

    // Default to string for mixed types
    DataType::Utf8
}

fn inferred_to_arrow(inferred: &InferredType, _hints: &SchemaDiscoveryHints) -> DataType {
    match inferred {
        InferredType::Bool => DataType::Boolean,
        InferredType::Int => DataType::Int64,
        InferredType::Float => DataType::Float64,
        // Null, String, Object, Array all map to Utf8
        InferredType::Null | InferredType::String | InferredType::Object | InferredType::Array => {
            DataType::Utf8
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hints_builder() {
        let hints = SchemaDiscoveryHints::new()
            .with_type_hint("id", DataType::Int32)
            .with_nullable("optional_field")
            .prefer_larger_types();

        assert_eq!(hints.type_hints.get("id"), Some(&DataType::Int32));
        assert!(hints
            .nullable_fields
            .contains(&"optional_field".to_string()));
        assert!(hints.prefer_larger_types);
    }

    #[test]
    fn test_infer_json_simple() {
        let samples: Vec<&[u8]> = vec![
            br#"{"id": 1, "name": "Alice"}"#,
            br#"{"id": 2, "name": "Bob"}"#,
        ];

        let schema =
            infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::new())
                .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[test]
    fn test_infer_json_types() {
        let samples: Vec<&[u8]> =
            vec![br#"{"int": 42, "float": 3.14, "bool": true, "str": "hello"}"#];

        let schema =
            infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::new())
                .unwrap();

        assert_eq!(
            schema.field_with_name("int").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("float").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("bool").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            schema.field_with_name("str").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_infer_json_nullable() {
        let samples: Vec<&[u8]> = vec![br#"{"value": 1}"#, br#"{"value": null}"#];

        let schema =
            infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::new())
                .unwrap();

        assert!(schema.field_with_name("value").unwrap().is_nullable());
    }

    #[test]
    fn test_infer_json_type_hint() {
        let samples: Vec<&[u8]> = vec![br#"{"id": 42}"#];

        let hints = SchemaDiscoveryHints::new().with_type_hint("id", DataType::Int32);
        let schema = infer_schema_from_samples(&samples, Format::Json, &hints).unwrap();

        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn test_infer_csv_basic() {
        let samples: Vec<&[u8]> = vec![b"id,name,age", b"1,Alice,30", b"2,Bob,25"];

        let schema =
            infer_schema_from_samples(&samples, Format::Csv, &SchemaDiscoveryHints::new()).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("age").is_ok());
    }

    #[test]
    fn test_infer_csv_types() {
        let samples: Vec<&[u8]> = vec![
            b"int_col,float_col,bool_col,str_col",
            b"42,3.14,true,hello",
            b"100,2.71,false,world",
        ];

        let schema =
            infer_schema_from_samples(&samples, Format::Csv, &SchemaDiscoveryHints::new()).unwrap();

        assert_eq!(
            schema.field_with_name("int_col").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("float_col").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("bool_col").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            schema.field_with_name("str_col").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_infer_empty_samples_error() {
        let samples: Vec<&[u8]> = vec![];
        let result =
            infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_infer_raw_format_error() {
        let samples: Vec<&[u8]> = vec![b"raw bytes"];
        let result = infer_schema_from_samples(&samples, Format::Raw, &SchemaDiscoveryHints::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_infer_mixed_int_float() {
        let samples: Vec<&[u8]> = vec![br#"{"value": 1}"#, br#"{"value": 2.5}"#];

        let schema =
            infer_schema_from_samples(&samples, Format::Json, &SchemaDiscoveryHints::new())
                .unwrap();

        // Should promote to float
        assert_eq!(
            schema.field_with_name("value").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn test_infer_with_field_names_hint() {
        let samples: Vec<&[u8]> = vec![br#"{"a": 1, "b": 2, "c": 3}"#];

        let hints =
            SchemaDiscoveryHints::new().with_field_names(vec!["c".to_string(), "a".to_string()]);

        let schema = infer_schema_from_samples(&samples, Format::Json, &hints).unwrap();

        // Should only include hinted fields in order
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name(), "c");
        assert_eq!(schema.fields()[1].name(), "a");
    }

    #[test]
    fn test_empty_as_null_hint() {
        let samples: Vec<&[u8]> = vec![br#"{"value": ""}"#, br#"{"value": "text"}"#];

        let hints = SchemaDiscoveryHints::new().empty_as_null();
        let schema = infer_schema_from_samples(&samples, Format::Json, &hints).unwrap();

        // Field should be nullable because empty string treated as null
        assert!(schema.field_with_name("value").unwrap().is_nullable());
    }
}
