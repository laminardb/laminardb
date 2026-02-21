//! Avro format support (F-SCHEMA-006).
//!
//! Provides:
//! - [`AvroFormatDecoder`] — decodes raw and Confluent wire-format Avro
//!   into Arrow `RecordBatch` (implements [`FormatDecoder`])
//! - [`AvroFormatEncoder`] — encodes Arrow `RecordBatch` into Confluent
//!   wire-format Avro (implements [`FormatEncoder`])
//! - [`avro_to_arrow_type`] — maps Avro JSON type definitions to Arrow
//!   `DataType`
//!
//! All types require the `kafka` feature flag.
//!
//! [`FormatDecoder`]: crate::schema::traits::FormatDecoder
//! [`FormatEncoder`]: crate::schema::traits::FormatEncoder

pub mod decoder;
pub mod encoder;

pub use decoder::{AvroDecoderMode, AvroFormatDecoder};
pub use encoder::AvroFormatEncoder;

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use crate::schema::error::{SchemaError, SchemaResult};

/// Converts an Avro JSON schema string to an Arrow [`SchemaRef`].
///
/// Supports Avro record schemas with primitive, logical, and complex
/// field types.
///
/// # Errors
///
/// Returns [`SchemaError::Incompatible`] if the schema JSON is invalid
/// or contains unsupported types.
pub fn avro_to_arrow_schema(avro_schema_json: &str) -> SchemaResult<SchemaRef> {
    let avro: serde_json::Value = serde_json::from_str(avro_schema_json)
        .map_err(|e| SchemaError::Incompatible(format!("invalid Avro schema JSON: {e}")))?;

    let fields_val = avro
        .get("fields")
        .ok_or_else(|| SchemaError::Incompatible("Avro schema missing 'fields'".into()))?;

    let fields_arr = fields_val
        .as_array()
        .ok_or_else(|| SchemaError::Incompatible("'fields' is not an array".into()))?;

    let mut arrow_fields = Vec::with_capacity(fields_arr.len());
    for field in fields_arr {
        let name = field
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| SchemaError::Incompatible("field missing 'name'".into()))?;

        let avro_type = field
            .get("type")
            .ok_or_else(|| SchemaError::Incompatible(format!("field '{name}' missing 'type'")))?;

        let (data_type, nullable) = avro_to_arrow_type(avro_type)?;
        arrow_fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Maps a single Avro type definition to an Arrow `DataType` + nullable flag.
///
/// # Supported Types
///
/// | Avro | Arrow |
/// |------|-------|
/// | null | Null |
/// | boolean | Boolean |
/// | int | Int32 |
/// | long | Int64 |
/// | float | Float32 |
/// | double | Float64 |
/// | bytes | Binary |
/// | string | Utf8 |
/// | record | Struct |
/// | enum | Dictionary(Int32, Utf8) |
/// | array | List |
/// | map | Map(Utf8, V) |
/// | union `["null", T]` | nullable T |
/// | fixed(N) | FixedSizeBinary(N) |
/// | date (logical) | Date32 |
/// | time-millis | Time32(Millisecond) |
/// | time-micros | Time64(Microsecond) |
/// | timestamp-millis | Timestamp(Millisecond, UTC) |
/// | timestamp-micros | Timestamp(Microsecond, UTC) |
/// | decimal | Decimal128(p, s) |
/// | uuid | Utf8 |
///
/// # Errors
///
/// Returns [`SchemaError::Incompatible`] for unrecognized types.
pub fn avro_to_arrow_type(avro_type: &serde_json::Value) -> SchemaResult<(DataType, bool)> {
    match avro_type {
        serde_json::Value::String(s) => Ok((avro_primitive(s)?, false)),
        serde_json::Value::Array(union) => parse_union(union),
        serde_json::Value::Object(obj) => parse_complex(obj),
        _ => Err(SchemaError::Incompatible(format!(
            "unexpected Avro type: {avro_type}"
        ))),
    }
}

/// Maps Avro primitive type strings to Arrow `DataType`.
fn avro_primitive(type_name: &str) -> SchemaResult<DataType> {
    match type_name {
        "null" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "int" => Ok(DataType::Int32),
        "long" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "bytes" => Ok(DataType::Binary),
        "string" => Ok(DataType::Utf8),
        _ => Err(SchemaError::Incompatible(format!(
            "unsupported Avro primitive: {type_name}"
        ))),
    }
}

/// Parses Avro union types.
///
/// `["null", T]` → nullable T. Multi-type unions fall back to Utf8.
fn parse_union(variants: &[serde_json::Value]) -> SchemaResult<(DataType, bool)> {
    let non_null: Vec<_> = variants
        .iter()
        .filter(|v| v.as_str() != Some("null"))
        .collect();
    let nullable = variants.iter().any(|v| v.as_str() == Some("null"));

    if non_null.len() == 1 {
        let (dt, _) = avro_to_arrow_type(non_null[0])?;
        Ok((dt, nullable))
    } else if non_null.is_empty() {
        Ok((DataType::Null, true))
    } else {
        // Multi-type union — fall back to string.
        Ok((DataType::Utf8, nullable))
    }
}

/// Parses Avro complex/logical types from a JSON object definition.
#[allow(clippy::too_many_lines)]
fn parse_complex(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> SchemaResult<(DataType, bool)> {
    // Check logical type first.
    if let Some(logical) = obj.get("logicalType").and_then(|v| v.as_str()) {
        return match logical {
            "date" => Ok((DataType::Date32, false)),
            "time-millis" => Ok((DataType::Time32(arrow_schema::TimeUnit::Millisecond), false)),
            "time-micros" => Ok((DataType::Time64(arrow_schema::TimeUnit::Microsecond), false)),
            "timestamp-millis" => Ok((
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into())),
                false,
            )),
            "timestamp-micros" => Ok((
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )),
            "decimal" => {
                #[allow(clippy::cast_possible_truncation)]
                let precision = obj
                    .get("precision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(38) as u8;
                #[allow(clippy::cast_possible_truncation)]
                let scale = obj
                    .get("scale")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i8;
                Ok((DataType::Decimal128(precision, scale), false))
            }
            // uuid, unknown, and all other logical types fall back to Utf8.
            _ => Ok((DataType::Utf8, false)),
        };
    }

    let type_str = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match type_str {
        "array" => {
            let items = obj
                .get("items")
                .ok_or_else(|| SchemaError::Incompatible("Avro array missing 'items'".into()))?;
            let (item_type, _) = avro_to_arrow_type(items)?;
            Ok((
                DataType::List(Arc::new(Field::new("item", item_type, true))),
                false,
            ))
        }
        "map" => {
            let values = obj
                .get("values")
                .ok_or_else(|| SchemaError::Incompatible("Avro map missing 'values'".into()))?;
            let (value_type, _) = avro_to_arrow_type(values)?;
            Ok((
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", value_type, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ))
        }
        "record" => {
            let fields_val = obj.get("fields").ok_or_else(|| {
                SchemaError::Incompatible("nested record missing 'fields'".into())
            })?;
            let fields_arr = fields_val.as_array().ok_or_else(|| {
                SchemaError::Incompatible("nested record 'fields' is not an array".into())
            })?;
            let mut arrow_fields = Vec::with_capacity(fields_arr.len());
            for f in fields_arr {
                let name = f.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                    SchemaError::Incompatible("nested record field missing 'name'".into())
                })?;
                let ft = f.get("type").ok_or_else(|| {
                    SchemaError::Incompatible(format!("nested field '{name}' missing 'type'"))
                })?;
                let (dt, nullable) = avro_to_arrow_type(ft)?;
                arrow_fields.push(Field::new(name, dt, nullable));
            }
            Ok((DataType::Struct(Fields::from(arrow_fields)), false))
        }
        "enum" => {
            // Avro enum → Dictionary(Int32, Utf8)
            Ok((
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ))
        }
        "fixed" => {
            let size = obj
                .get("size")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| SchemaError::Incompatible("Avro fixed missing 'size'".into()))?;
            #[allow(clippy::cast_possible_truncation)]
            Ok((DataType::FixedSizeBinary(size as i32), false))
        }
        _ => Err(SchemaError::Incompatible(format!(
            "unsupported Avro type: {type_str}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avro_primitives() {
        assert_eq!(avro_primitive("null").unwrap(), DataType::Null);
        assert_eq!(avro_primitive("boolean").unwrap(), DataType::Boolean);
        assert_eq!(avro_primitive("int").unwrap(), DataType::Int32);
        assert_eq!(avro_primitive("long").unwrap(), DataType::Int64);
        assert_eq!(avro_primitive("float").unwrap(), DataType::Float32);
        assert_eq!(avro_primitive("double").unwrap(), DataType::Float64);
        assert_eq!(avro_primitive("bytes").unwrap(), DataType::Binary);
        assert_eq!(avro_primitive("string").unwrap(), DataType::Utf8);
        assert!(avro_primitive("unknown").is_err());
    }

    #[test]
    fn test_avro_nullable_union() {
        let union_type = serde_json::json!(["null", "string"]);
        let (dt, nullable) = avro_to_arrow_type(&union_type).unwrap();
        assert_eq!(dt, DataType::Utf8);
        assert!(nullable);
    }

    #[test]
    fn test_avro_non_nullable_union() {
        let union_type = serde_json::json!(["int"]);
        let (dt, nullable) = avro_to_arrow_type(&union_type).unwrap();
        assert_eq!(dt, DataType::Int32);
        assert!(!nullable);
    }

    #[test]
    fn test_avro_multi_union_fallback() {
        let union_type = serde_json::json!(["null", "string", "int"]);
        let (dt, nullable) = avro_to_arrow_type(&union_type).unwrap();
        assert_eq!(dt, DataType::Utf8);
        assert!(nullable);
    }

    #[test]
    fn test_avro_array_type() {
        let arr_type = serde_json::json!({"type": "array", "items": "long"});
        let (dt, nullable) = avro_to_arrow_type(&arr_type).unwrap();
        assert!(matches!(dt, DataType::List(_)));
        assert!(!nullable);
    }

    #[test]
    fn test_avro_map_type() {
        let map_type = serde_json::json!({"type": "map", "values": "string"});
        let (dt, _) = avro_to_arrow_type(&map_type).unwrap();
        assert!(matches!(dt, DataType::Map(_, _)));
    }

    #[test]
    fn test_avro_record_type() {
        let rec = serde_json::json!({
            "type": "record",
            "name": "inner",
            "fields": [
                {"name": "x", "type": "int"},
                {"name": "y", "type": "string"}
            ]
        });
        let (dt, _) = avro_to_arrow_type(&rec).unwrap();
        assert!(matches!(dt, DataType::Struct(_)));
    }

    #[test]
    fn test_avro_enum_type() {
        let enum_type = serde_json::json!({
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "GREEN", "BLUE"]
        });
        let (dt, _) = avro_to_arrow_type(&enum_type).unwrap();
        assert!(matches!(dt, DataType::Dictionary(_, _)));
    }

    #[test]
    fn test_avro_fixed_type() {
        let fixed = serde_json::json!({"type": "fixed", "name": "hash", "size": 16});
        let (dt, _) = avro_to_arrow_type(&fixed).unwrap();
        assert_eq!(dt, DataType::FixedSizeBinary(16));
    }

    #[test]
    fn test_logical_date() {
        let date = serde_json::json!({"type": "int", "logicalType": "date"});
        let (dt, _) = avro_to_arrow_type(&date).unwrap();
        assert_eq!(dt, DataType::Date32);
    }

    #[test]
    fn test_logical_timestamp_millis() {
        let ts = serde_json::json!({"type": "long", "logicalType": "timestamp-millis"});
        let (dt, _) = avro_to_arrow_type(&ts).unwrap();
        assert!(matches!(
            dt,
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _)
        ));
    }

    #[test]
    fn test_logical_timestamp_micros() {
        let ts = serde_json::json!({"type": "long", "logicalType": "timestamp-micros"});
        let (dt, _) = avro_to_arrow_type(&ts).unwrap();
        assert!(matches!(
            dt,
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _)
        ));
    }

    #[test]
    fn test_logical_decimal() {
        let dec = serde_json::json!({
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 10,
            "scale": 2
        });
        let (dt, _) = avro_to_arrow_type(&dec).unwrap();
        assert_eq!(dt, DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_logical_uuid() {
        let uuid = serde_json::json!({"type": "string", "logicalType": "uuid"});
        let (dt, _) = avro_to_arrow_type(&uuid).unwrap();
        assert_eq!(dt, DataType::Utf8);
    }

    #[test]
    fn test_logical_time_millis() {
        let t = serde_json::json!({"type": "int", "logicalType": "time-millis"});
        let (dt, _) = avro_to_arrow_type(&t).unwrap();
        assert!(matches!(
            dt,
            DataType::Time32(arrow_schema::TimeUnit::Millisecond)
        ));
    }

    #[test]
    fn test_logical_time_micros() {
        let t = serde_json::json!({"type": "long", "logicalType": "time-micros"});
        let (dt, _) = avro_to_arrow_type(&t).unwrap();
        assert!(matches!(
            dt,
            DataType::Time64(arrow_schema::TimeUnit::Microsecond)
        ));
    }

    #[test]
    fn test_full_schema() {
        let schema = r#"{
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "customer", "type": ["null", "string"]},
                {"name": "amount", "type": "double"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }"#;
        let arrow = avro_to_arrow_schema(schema).unwrap();
        assert_eq!(arrow.fields().len(), 5);
        assert_eq!(arrow.field(0).name(), "id");
        assert_eq!(*arrow.field(0).data_type(), DataType::Int64);
        assert!(!arrow.field(0).is_nullable());
        assert_eq!(arrow.field(1).name(), "customer");
        assert!(arrow.field(1).is_nullable());
        assert_eq!(*arrow.field(2).data_type(), DataType::Float64);
    }

    #[test]
    fn test_invalid_schema_json() {
        assert!(avro_to_arrow_schema("not json").is_err());
    }

    #[test]
    fn test_schema_missing_fields() {
        assert!(avro_to_arrow_schema(r#"{"type": "record"}"#).is_err());
    }
}
