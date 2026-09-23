use arrow_schema::{DataType, Schema, TimeUnit};
use serde::Serialize;

use crate::error::DbError;

#[derive(Serialize)]
pub(crate) struct CanonicalField {
    name: String,
    nullable: bool,
    data_type: CanonicalType,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CanonicalType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Decimal128 { precision: u8, scale: i8 },
    TimestampMicrosecondUtc,
}

/// Canonical schema fields for the initial process protocol. Field order, names, nullability,
/// decimal parameters and timestamp precision are binding; arbitrary Arrow metadata is rejected.
pub(crate) fn canonical_fields(schema: &Schema) -> Result<Vec<CanonicalField>, DbError> {
    if !schema.metadata().is_empty() || schema.fields().len() > 256 {
        return Err(DbError::Unsupported(
            "process schemas cannot contain metadata or more than 256 fields".into(),
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    let mut fields = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if !field.metadata().is_empty() || !names.insert(field.name()) {
            return Err(DbError::Unsupported(
                "process fields must have unique names and no extension metadata".into(),
            ));
        }
        let data_type = match field.data_type() {
            DataType::Boolean => CanonicalType::Boolean,
            DataType::Int8 => CanonicalType::Int8,
            DataType::Int16 => CanonicalType::Int16,
            DataType::Int32 => CanonicalType::Int32,
            DataType::Int64 => CanonicalType::Int64,
            DataType::Float32 => CanonicalType::Float32,
            DataType::Float64 => CanonicalType::Float64,
            DataType::Utf8 => CanonicalType::Utf8,
            DataType::Binary => CanonicalType::Binary,
            DataType::Decimal128(precision, scale) => CanonicalType::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                CanonicalType::TimestampMicrosecondUtc
            }
            _ => {
                return Err(DbError::Unsupported(format!(
                    "unsupported process field type for '{}'",
                    field.name()
                )));
            }
        };
        fields.push(CanonicalField {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            data_type,
        });
    }
    Ok(fields)
}
