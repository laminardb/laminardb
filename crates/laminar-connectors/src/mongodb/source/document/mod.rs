//! Keyed document replication: strict BSON-to-Arrow projection into puts and key-only tombstones.
//!
//! Every typed column accepts exactly one BSON type family, so two distinct `MongoDB` values can
//! never become the same Arrow value. Unsupported or mismatched values fail the batch instead of
//! being coerced, rounded, or defaulted.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{RawBsonRef, RawDocument};

use super::super::change_event::canonical_document_extjson;
use super::ConnectorError;
use crate::connector::SourceMutation;

const RESERVED_COLUMNS: &[&str] = &[
    "_op",
    "__op",
    "_ts_ms",
    "__ts_ms",
    "__weight",
    "__source_mutation",
    "__source_partition",
    "__source_order_key",
    "__source_sub_offset",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    String,
    ObjectIdHex,
    Int32,
    Int64,
    Float64,
    Boolean,
    Decimal { precision: u8, scale: i8 },
    Timestamp(TimeUnit),
    Binary,
    Document,
}

#[derive(Debug, Clone)]
struct ColumnPlan {
    name: String,
    kind: ColumnKind,
}

/// One keyed mutation in emitted order.
pub(super) enum DocumentRow<'a> {
    Put {
        key: &'a RawDocument,
        document: &'a RawDocument,
    },
    Tombstone {
        key: &'a RawDocument,
    },
}

/// Validated projection of declared columns onto top-level document fields.
#[derive(Debug, Clone)]
pub(super) struct DocumentProjection {
    schema: SchemaRef,
    columns: Vec<ColumnPlan>,
    key_columns: Vec<usize>,
    key_names: BTreeSet<String>,
}

fn projection_error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "MongoDB CDC output.mode=document: {}",
        message.into()
    ))
}

fn column_kind(data_type: &DataType, objectid: bool, document: bool) -> Result<ColumnKind, String> {
    let kind = match data_type {
        DataType::Utf8 if document => ColumnKind::Document,
        DataType::Utf8 if objectid => ColumnKind::ObjectIdHex,
        DataType::Utf8 => ColumnKind::String,
        DataType::Int32 => ColumnKind::Int32,
        DataType::Int64 => ColumnKind::Int64,
        DataType::Float64 => ColumnKind::Float64,
        DataType::Boolean => ColumnKind::Boolean,
        DataType::Decimal128(precision, scale) => ColumnKind::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Timestamp(unit, None) => ColumnKind::Timestamp(*unit),
        DataType::Binary => ColumnKind::Binary,
        other => {
            return Err(format!(
                "type {other:?} is not supported; use VARCHAR, INT, BIGINT, DOUBLE, BOOLEAN, \
                 DECIMAL, TIMESTAMP, BYTEA, or document.json.column for nested values"
            ));
        }
    };
    if (objectid || document) && data_type != &DataType::Utf8 {
        return Err("objectid.columns and document.json.column must be VARCHAR".into());
    }
    Ok(kind)
}

impl DocumentProjection {
    /// Validate a declared projection before any document is read.
    ///
    /// # Errors
    /// Rejects reserved or path-like names, unsupported types, a primary key without `_id`,
    /// nullable keys, and non-key columns that cannot hold a key-only tombstone.
    pub(super) fn try_new(
        schema: &SchemaRef,
        primary_key: &[String],
        objectid_columns: &[String],
        document_json_column: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        if primary_key.is_empty() {
            return Err(projection_error(
                "declare PRIMARY KEY with the collection's document key fields (normally _id)",
            ));
        }
        if !primary_key.iter().any(|column| column == "_id") {
            return Err(projection_error("PRIMARY KEY must include _id"));
        }
        for column in objectid_columns
            .iter()
            .map(String::as_str)
            .chain(document_json_column)
        {
            if schema.field_with_name(column).is_err() {
                return Err(projection_error(format!(
                    "projection option names undeclared column '{column}'"
                )));
            }
        }
        let mut columns = Vec::with_capacity(schema.fields().len());
        let mut key_columns = Vec::with_capacity(primary_key.len());
        for (index, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if RESERVED_COLUMNS
                .iter()
                .any(|reserved| name.eq_ignore_ascii_case(reserved))
            {
                return Err(projection_error(format!(
                    "column '{name}' uses an engine-reserved changelog name"
                )));
            }
            if name.is_empty() || name.starts_with('$') || name.contains('.') {
                return Err(projection_error(format!(
                    "column '{name}' must name one top-level document field"
                )));
            }
            let is_key = primary_key.iter().any(|column| column == name);
            let document = document_json_column == Some(name.as_str());
            let objectid = objectid_columns.iter().any(|column| column == name);
            let kind = column_kind(field.data_type(), objectid, document)
                .map_err(|reason| projection_error(format!("column '{name}': {reason}")))?;
            if is_key {
                if field.is_nullable() {
                    return Err(projection_error(format!(
                        "PRIMARY KEY column '{name}' must be NOT NULL"
                    )));
                }
                if document || kind == ColumnKind::Float64 {
                    return Err(projection_error(format!(
                        "PRIMARY KEY column '{name}' must hold an exact scalar key value"
                    )));
                }
                key_columns.push(index);
            } else if !field.is_nullable() {
                return Err(projection_error(format!(
                    "non-key column '{name}' must be nullable: a delete carries only the \
                     document key and its other values are absent"
                )));
            }
            columns.push(ColumnPlan {
                name: name.clone(),
                kind,
            });
        }
        if key_columns.len() != primary_key.len() {
            return Err(projection_error(
                "every PRIMARY KEY column must be a declared column",
            ));
        }
        Ok(Self {
            schema: Arc::clone(schema),
            columns,
            key_columns,
            key_names: primary_key.iter().cloned().collect(),
        })
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Build one batch and its row-aligned mutations in emitted order.
    pub(super) fn build(
        &self,
        rows: &[DocumentRow<'_>],
    ) -> Result<(RecordBatch, Vec<SourceMutation>), ConnectorError> {
        let mut builders: Vec<ColumnValues> = self
            .columns
            .iter()
            .map(|column| ColumnValues::new(column.kind, rows.len()))
            .collect();
        let mut mutations = Vec::with_capacity(rows.len());
        for row in rows {
            let (key, document) = match row {
                DocumentRow::Put { key, document } => (*key, Some(*document)),
                DocumentRow::Tombstone { key } => (*key, None),
            };
            self.check_key_shape(key)?;
            for (index, (column, builder)) in self.columns.iter().zip(&mut builders).enumerate() {
                let value = if self.key_columns.contains(&index) {
                    let value = key_value(key, &column.name)?;
                    if let Some(document) = document {
                        check_key_unchanged(key, document, &column.name, value)?;
                    }
                    Some(value)
                } else {
                    match document {
                        Some(document) if column.kind == ColumnKind::Document => {
                            builder.push_document(document)?;
                            continue;
                        }
                        Some(document) => document_value(document, &column.name)?,
                        None => None,
                    }
                };
                builder.push(column, value, key)?;
            }
            mutations.push(if document.is_some() {
                SourceMutation::Put
            } else {
                SourceMutation::Tombstone
            });
        }
        let arrays = self
            .columns
            .iter()
            .zip(builders)
            .map(|(column, builder)| builder.finish(column.kind))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(|error| {
            ConnectorError::Internal(format!("MongoDB document batch: {error}"))
        })?;
        Ok((batch, mutations))
    }

    fn check_key_shape(&self, key: &RawDocument) -> Result<(), ConnectorError> {
        let mut fields = BTreeSet::new();
        for element in key {
            let (name, _) = element.map_err(|error| {
                ConnectorError::SchemaMismatch(format!("malformed MongoDB document key: {error}"))
            })?;
            fields.insert(name.to_string());
        }
        if fields == self.key_names {
            return Ok(());
        }
        Err(ConnectorError::SchemaMismatch(format!(
            "MongoDB document key fields {fields:?} do not match the declared PRIMARY KEY {:?}; \
             a sharded collection's key includes its shard key fields",
            self.key_names
        )))
    }
}

fn key_value<'a>(key: &'a RawDocument, name: &str) -> Result<RawBsonRef<'a>, ConnectorError> {
    match key.get(name) {
        Ok(Some(RawBsonRef::Null) | None) | Err(_) => Err(ConnectorError::SchemaMismatch(format!(
            "MongoDB document key field '{name}' is absent or null"
        ))),
        Ok(Some(value)) => Ok(value),
    }
}

fn check_key_unchanged(
    key: &RawDocument,
    document: &RawDocument,
    name: &str,
    expected: RawBsonRef<'_>,
) -> Result<(), ConnectorError> {
    let actual = document.get(name).ok().flatten();
    if actual.map(RawBsonRef::to_raw_bson) == Some(expected.to_raw_bson()) {
        return Ok(());
    }
    Err(ConnectorError::SchemaMismatch(format!(
        "MongoDB post-image key field '{name}' differs from document key {}; an in-place key \
         change cannot be represented as a keyed replacement",
        describe_key(key)
    )))
}

fn document_value<'a>(
    document: &'a RawDocument,
    name: &str,
) -> Result<Option<RawBsonRef<'a>>, ConnectorError> {
    match document.get(name) {
        Ok(Some(RawBsonRef::Null) | None) => Ok(None),
        Ok(Some(value)) => Ok(Some(value)),
        Err(error) => Err(ConnectorError::SchemaMismatch(format!(
            "malformed MongoDB document field '{name}': {error}"
        ))),
    }
}

fn describe_key(key: &RawDocument) -> String {
    canonical_document_extjson(key).unwrap_or_else(|_| "<malformed key>".into())
}

enum ColumnValues {
    Text(StringBuilder),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    Decimal(Vec<Option<i128>>),
    Binary(BinaryBuilder),
}

impl ColumnValues {
    fn new(kind: ColumnKind, rows: usize) -> Self {
        match kind {
            ColumnKind::String | ColumnKind::ObjectIdHex | ColumnKind::Document => {
                Self::Text(StringBuilder::with_capacity(rows, rows * 24))
            }
            ColumnKind::Int32 => Self::Int32(Vec::with_capacity(rows)),
            ColumnKind::Int64 | ColumnKind::Timestamp(_) => Self::Int64(Vec::with_capacity(rows)),
            ColumnKind::Float64 => Self::Float64(Vec::with_capacity(rows)),
            ColumnKind::Boolean => Self::Boolean(Vec::with_capacity(rows)),
            ColumnKind::Decimal { .. } => Self::Decimal(Vec::with_capacity(rows)),
            ColumnKind::Binary => Self::Binary(BinaryBuilder::with_capacity(rows, rows * 16)),
        }
    }

    fn push_document(&mut self, document: &RawDocument) -> Result<(), ConnectorError> {
        let Self::Text(builder) = self else {
            return Err(ConnectorError::Internal(
                "document column has a non-text builder".into(),
            ));
        };
        builder.append_value(canonical_document_extjson(document)?);
        Ok(())
    }

    fn push(
        &mut self,
        column: &ColumnPlan,
        value: Option<RawBsonRef<'_>>,
        key: &RawDocument,
    ) -> Result<(), ConnectorError> {
        let mismatch = |value: RawBsonRef<'_>, reason: &str| {
            ConnectorError::SchemaMismatch(format!(
                "MongoDB document {} field '{}' holds BSON {:?}, which cannot be stored exactly \
                 as {:?}{reason}",
                describe_key(key),
                column.name,
                value.element_type(),
                column.kind
            ))
        };
        match (self, value) {
            (Self::Text(builder), None) => builder.append_null(),
            (Self::Int32(values), None) => values.push(None),
            (Self::Int64(values), None) => values.push(None),
            (Self::Float64(values), None) => values.push(None),
            (Self::Boolean(values), None) => values.push(None),
            (Self::Decimal(values), None) => values.push(None),
            (Self::Binary(builder), None) => builder.append_null(),
            (Self::Text(builder), Some(value)) => match (column.kind, value) {
                (ColumnKind::String, RawBsonRef::String(text)) => builder.append_value(text),
                (ColumnKind::ObjectIdHex, RawBsonRef::ObjectId(id)) => {
                    builder.append_value(id.to_hex());
                }
                (ColumnKind::String, RawBsonRef::ObjectId(_)) => {
                    return Err(mismatch(value, "; list the column in objectid.columns"));
                }
                (ColumnKind::String | ColumnKind::ObjectIdHex, value) => {
                    return Err(mismatch(
                        value,
                        "; use document.json.column for nested or mixed values",
                    ));
                }
                (_, value) => return Err(mismatch(value, "")),
            },
            (Self::Int32(values), Some(value)) => match value {
                RawBsonRef::Int32(number) => values.push(Some(number)),
                value => return Err(mismatch(value, "")),
            },
            (Self::Int64(values), Some(value)) => match (column.kind, value) {
                (ColumnKind::Int64, RawBsonRef::Int32(number)) => {
                    values.push(Some(i64::from(number)));
                }
                (ColumnKind::Int64, RawBsonRef::Int64(number)) => values.push(Some(number)),
                (ColumnKind::Timestamp(unit), RawBsonRef::DateTime(at)) => {
                    let converted = timestamp_value(at.timestamp_millis(), unit)
                        .ok_or_else(|| mismatch(value, " without losing precision"))?;
                    values.push(Some(converted));
                }
                (_, value) => return Err(mismatch(value, "")),
            },
            (Self::Float64(values), Some(value)) => match value {
                RawBsonRef::Double(number) => values.push(Some(number)),
                RawBsonRef::Int32(number) => values.push(Some(f64::from(number))),
                value => return Err(mismatch(value, "")),
            },
            (Self::Boolean(values), Some(value)) => match value {
                RawBsonRef::Boolean(flag) => values.push(Some(flag)),
                value => return Err(mismatch(value, "")),
            },
            (Self::Decimal(values), Some(value)) => {
                let ColumnKind::Decimal { precision, scale } = column.kind else {
                    return Err(ConnectorError::Internal("decimal builder kind".into()));
                };
                let unscaled = match value {
                    RawBsonRef::Decimal128(decimal) => {
                        decimal_unscaled(&decimal.to_string(), precision, scale)
                    }
                    RawBsonRef::Int32(number) => {
                        integer_unscaled(i128::from(number), precision, scale)
                    }
                    RawBsonRef::Int64(number) => {
                        integer_unscaled(i128::from(number), precision, scale)
                    }
                    value => return Err(mismatch(value, "")),
                };
                values.push(Some(unscaled.ok_or_else(|| {
                    mismatch(value, " without rounding or exceeding its precision")
                })?));
            }
            (Self::Binary(builder), Some(value)) => match value {
                RawBsonRef::Binary(binary) if binary.subtype == BinarySubtype::Generic => {
                    builder.append_value(binary.bytes);
                }
                value => return Err(mismatch(value, "; only generic binary subtype 0 is exact")),
            },
        }
        Ok(())
    }

    fn finish(self, kind: ColumnKind) -> Result<ArrayRef, ConnectorError> {
        let array: ArrayRef = match (self, kind) {
            (Self::Text(mut builder), _) => Arc::new(builder.finish()),
            (Self::Int32(values), _) => Arc::new(Int32Array::from(values)),
            (Self::Int64(values), ColumnKind::Timestamp(TimeUnit::Second)) => {
                Arc::new(TimestampSecondArray::from(values))
            }
            (Self::Int64(values), ColumnKind::Timestamp(TimeUnit::Millisecond)) => {
                Arc::new(TimestampMillisecondArray::from(values))
            }
            (Self::Int64(values), ColumnKind::Timestamp(TimeUnit::Microsecond)) => {
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            (Self::Int64(values), ColumnKind::Timestamp(TimeUnit::Nanosecond)) => {
                Arc::new(TimestampNanosecondArray::from(values))
            }
            (Self::Int64(values), _) => Arc::new(Int64Array::from(values)),
            (Self::Float64(values), _) => Arc::new(Float64Array::from(values)),
            (Self::Boolean(values), _) => Arc::new(BooleanArray::from(values)),
            (Self::Decimal(values), ColumnKind::Decimal { precision, scale }) => Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|error| {
                        ConnectorError::Internal(format!("decimal column: {error}"))
                    })?,
            ),
            (Self::Decimal(_), _) => {
                return Err(ConnectorError::Internal("decimal builder kind".into()));
            }
            (Self::Binary(mut builder), _) => Arc::new(builder.finish()),
        };
        Ok(array)
    }
}

fn timestamp_value(millis: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => (millis % 1000 == 0).then_some(millis / 1000),
        TimeUnit::Millisecond => Some(millis),
        TimeUnit::Microsecond => millis.checked_mul(1_000),
        TimeUnit::Nanosecond => millis.checked_mul(1_000_000),
    }
}

fn pow10(exponent: u32) -> Option<i128> {
    10_i128.checked_pow(exponent)
}

fn within_precision(unscaled: i128, precision: u8) -> Option<i128> {
    let limit = pow10(u32::from(precision))?;
    (unscaled.unsigned_abs() < limit.unsigned_abs()).then_some(unscaled)
}

fn integer_unscaled(value: i128, precision: u8, scale: i8) -> Option<i128> {
    let unscaled = if scale >= 0 {
        value.checked_mul(pow10(u32::from(scale.unsigned_abs()))?)?
    } else {
        let divisor = pow10(u32::from(scale.unsigned_abs()))?;
        if value % divisor != 0 {
            return None;
        }
        value / divisor
    };
    within_precision(unscaled, precision)
}

/// Exact unscaled value of a `Decimal128` string at `scale`, or `None` when that would round,
/// overflow `precision`, or the value is not finite.
fn decimal_unscaled(text: &str, precision: u8, scale: i8) -> Option<i128> {
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.find(['E', 'e']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i64>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut digits: i128 = 0;
    for byte in whole.bytes().chain(fraction.bytes()) {
        if !byte.is_ascii_digit() {
            return None;
        }
        digits = digits
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
    }
    let shift = exponent
        .checked_sub(i64::try_from(fraction.len()).ok()?)?
        .checked_add(i64::from(scale))?;
    let unscaled = if shift >= 0 {
        digits.checked_mul(pow10(u32::try_from(shift).ok()?)?)?
    } else {
        let divisor = pow10(u32::try_from(shift.unsigned_abs()).ok()?)?;
        if digits % divisor != 0 {
            return None;
        }
        digits / divisor
    };
    within_precision(if negative { -unscaled } else { unscaled }, precision)
}

#[cfg(test)]
mod tests;
