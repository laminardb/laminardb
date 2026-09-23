use std::sync::Arc;

use arrow_array::types::{validate_decimal_precision_and_scale, Decimal128Type};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::DbError;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalField {
    name: String,
    nullable: bool,
    data_type: CanonicalType,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

pub(crate) fn schema_from_canonical_fields(
    fields: Vec<CanonicalField>,
) -> Result<SchemaRef, DbError> {
    let fields = fields
        .into_iter()
        .map(|field| {
            let data_type = match field.data_type {
                CanonicalType::Boolean => DataType::Boolean,
                CanonicalType::Int8 => DataType::Int8,
                CanonicalType::Int16 => DataType::Int16,
                CanonicalType::Int32 => DataType::Int32,
                CanonicalType::Int64 => DataType::Int64,
                CanonicalType::Float32 => DataType::Float32,
                CanonicalType::Float64 => DataType::Float64,
                CanonicalType::Utf8 => DataType::Utf8,
                CanonicalType::Binary => DataType::Binary,
                CanonicalType::Decimal128 { precision, scale } => {
                    DataType::Decimal128(precision, scale)
                }
                CanonicalType::TimestampMicrosecondUtc => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
            };
            Field::new(field.name, data_type, field.nullable)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    canonical_fields(&schema)?;
    Ok(schema)
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
        if field.name().is_empty() || !field.metadata().is_empty() || !names.insert(field.name()) {
            return Err(DbError::Unsupported(
                "process fields must have nonempty unique names and no extension metadata".into(),
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
            DataType::Decimal128(precision, scale) => {
                validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale)
                    .map_err(|error| {
                        DbError::Unsupported(format!(
                            "invalid process decimal field '{}': {error}",
                            field.name()
                        ))
                    })?;
                CanonicalType::Decimal128 {
                    precision: *precision,
                    scale: *scale,
                }
            }
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
