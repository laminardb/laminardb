//! Text-format `PostgreSQL` values to typed Arrow columns of the declared source schema.
//!
//! The snapshot reads and `pgoutput` both deliver each value in its type's text output form
//! under the same session settings, so one parser per [`ValueKind`] produces identical Arrow
//! values for a row however it was read.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::types::Decimal128Type;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

use super::types::ValueKind;
use crate::error::ConnectorError;

/// Session settings that make every supported type's text output canonical and lossless.
pub(crate) const SESSION_OPTIONS: &str =
    "-c DateStyle=ISO,YMD -c TimeZone=UTC -c extra_float_digits=3 -c bytea_output=hex";

/// Builder growth may double capacity, so retained bytes are charged at twice the payload.
const BUILDER_GROWTH_FACTOR: usize = 2;
const OFFSET_BYTES: usize = 4;

/// One declared output column and the relation tuple position it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundColumn {
    pub(crate) name: String,
    pub(crate) tuple_index: usize,
    pub(crate) kind: ValueKind,
    pub(crate) nullable: bool,
    pub(crate) is_key: bool,
}

/// Immutable mapping from the `PostgreSQL` relation to the declared Arrow schema.
#[derive(Debug, Clone)]
pub(crate) struct RowLayout {
    /// Declared visible columns, in declared order.
    pub(crate) columns: Vec<BoundColumn>,
    /// Number of columns in a relation tuple.
    pub(crate) tuple_width: usize,
    /// Declared schema, with a trailing `__weight` in changelog mode.
    pub(crate) schema: SchemaRef,
    /// Whether rows carry a Z-set weight.
    pub(crate) weighted: bool,
}

impl RowLayout {
    /// Retained-byte estimate for one row whose text values have `text_bytes` total length.
    pub(crate) fn planned_row_bytes(&self, text_bytes: usize) -> Result<usize, ConnectorError> {
        let fixed = self
            .columns
            .len()
            .checked_mul(16 + OFFSET_BYTES)
            .and_then(|bytes| bytes.checked_add(text_bytes))
            .and_then(|bytes| bytes.checked_add(usize::from(self.weighted) * 8 + 1))
            .and_then(|bytes| bytes.checked_mul(BUILDER_GROWTH_FACTOR));
        fixed.ok_or_else(|| ConnectorError::ReadError("PostgreSQL CDC row size overflow".into()))
    }
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal128(Decimal128Builder, u8, i8),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    Time64(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder, Option<Arc<str>>, bool),
}

impl ColumnBuilder {
    fn new(kind: ValueKind, declared: &DataType) -> Self {
        match kind {
            ValueKind::Bool => Self::Bool(BooleanBuilder::new()),
            ValueKind::Int16 => Self::Int16(Int16Builder::new()),
            ValueKind::Int32 => Self::Int32(Int32Builder::new()),
            ValueKind::Int64 => Self::Int64(Int64Builder::new()),
            ValueKind::Float32 => Self::Float32(Float32Builder::new()),
            ValueKind::Float64 => Self::Float64(Float64Builder::new()),
            ValueKind::Decimal128 { precision, scale } => {
                Self::Decimal128(Decimal128Builder::new(), precision, scale)
            }
            ValueKind::Utf8 => Self::Utf8(StringBuilder::new()),
            ValueKind::Binary => Self::Binary(BinaryBuilder::new()),
            ValueKind::Date32 => Self::Date32(Date32Builder::new()),
            ValueKind::Time64Micros => Self::Time64(Time64MicrosecondBuilder::new()),
            ValueKind::TimestampMicros | ValueKind::TimestampTzMicros => {
                let zone = match declared {
                    DataType::Timestamp(_, zone) => zone.clone(),
                    _ => None,
                };
                Self::Timestamp(
                    TimestampMicrosecondBuilder::new(),
                    zone,
                    kind == ValueKind::TimestampTzMicros,
                )
            }
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(builder) => builder.append_null(),
            Self::Int16(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Float32(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Decimal128(builder, ..) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Time64(builder) => builder.append_null(),
            Self::Timestamp(builder, ..) => builder.append_null(),
        }
    }

    fn append_text(&mut self, text: &[u8]) -> Result<(), String> {
        match self {
            Self::Binary(builder) => builder.append_value(decode_bytea_hex(text)?),
            Self::Bool(builder) => builder.append_value(match text {
                b"t" => true,
                b"f" => false,
                _ => return Err("invalid boolean text".into()),
            }),
            other => {
                let text = std::str::from_utf8(text).map_err(|_| "invalid UTF-8".to_string())?;
                other.append_str(text)?;
            }
        }
        Ok(())
    }

    fn append_str(&mut self, text: &str) -> Result<(), String> {
        match self {
            Self::Int16(builder) => builder.append_value(parse_number(text)?),
            Self::Int32(builder) => builder.append_value(parse_number(text)?),
            Self::Int64(builder) => builder.append_value(parse_number(text)?),
            Self::Float32(builder) => builder.append_value(parse_number(text)?),
            Self::Float64(builder) => builder.append_value(parse_number(text)?),
            Self::Decimal128(builder, precision, scale) => builder.append_value(
                arrow_cast::parse::parse_decimal::<Decimal128Type>(text, *precision, *scale)
                    .map_err(|error| error.to_string())?,
            ),
            Self::Utf8(builder) => builder.append_value(text),
            Self::Date32(builder) => builder.append_value(parse_date(text)?),
            Self::Time64(builder) => builder.append_value(parse_time(text)?),
            Self::Timestamp(builder, _, with_zone) => builder.append_value(if *with_zone {
                parse_timestamptz(text)?
            } else {
                parse_timestamp(text)?
            }),
            Self::Bool(_) | Self::Binary(_) => unreachable!("byte-level kinds are parsed above"),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef, ConnectorError> {
        Ok(match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Int16(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder, precision, scale) => Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| ConnectorError::Internal(error.to_string()))?,
            ),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::Time64(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder, zone, _) => {
                Arc::new(builder.finish().with_timezone_opt(zone.clone()))
            }
        })
    }
}

/// Typed rows accumulated for one output batch.
pub(crate) struct RowBuilder {
    columns: Vec<ColumnBuilder>,
    weights: Option<Int64Builder>,
    rows: usize,
    retained_bytes: usize,
}

impl RowBuilder {
    pub(crate) fn new(layout: &RowLayout) -> Self {
        Self {
            columns: layout
                .columns
                .iter()
                .zip(layout.schema.fields())
                .map(|(column, field)| ColumnBuilder::new(column.kind, field.data_type()))
                .collect(),
            weights: layout.weighted.then(Int64Builder::new),
            rows: 0,
            retained_bytes: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows
    }

    /// Conservative retained bytes of the rows appended so far.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Append one row. `value(declared_index, column)` yields the text value, or `None` for
    /// SQL `NULL`, or the error that makes the value unavailable.
    /// Non-key columns of a key-only row are `NULL`; `planned_bytes` is the row's
    /// [`RowLayout::planned_row_bytes`] charge.
    ///
    /// # Errors
    /// Returns an error for an unparseable value or a `NULL` in a non-nullable column. The
    /// builder is then partially appended and must be discarded.
    pub(crate) fn append<'a>(
        &mut self,
        layout: &RowLayout,
        mut value: impl FnMut(usize, &BoundColumn) -> Result<Option<&'a [u8]>, ConnectorError>,
        key_only: bool,
        weight: Option<i64>,
        planned_bytes: usize,
    ) -> Result<(), ConnectorError> {
        for (index, (column, builder)) in layout.columns.iter().zip(&mut self.columns).enumerate() {
            let omitted = key_only && !column.is_key;
            let text = if omitted { None } else { value(index, column)? };
            match text {
                Some(text) => builder.append_text(text).map_err(|reason| {
                    ConnectorError::ReadError(format!(
                        "PostgreSQL CDC column '{}' value cannot be decoded: {reason}",
                        column.name
                    ))
                })?,
                None if column.nullable || omitted => builder.append_null(),
                None => {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "PostgreSQL CDC column '{}' is NULL but declared NOT NULL",
                        column.name
                    )));
                }
            }
        }
        match (&mut self.weights, weight) {
            (Some(weights), Some(weight)) => weights.append_value(weight),
            (None, None) => {}
            _ => {
                return Err(ConnectorError::Internal(
                    "PostgreSQL CDC row weight does not match the output mode".into(),
                ));
            }
        }
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| ConnectorError::Internal("PostgreSQL CDC row count overflow".into()))?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(planned_bytes)
            .ok_or_else(|| ConnectorError::Internal("PostgreSQL CDC row size overflow".into()))?;
        Ok(())
    }

    /// Finish the accumulated rows as one batch of the declared schema and reset the builder.
    pub(crate) fn finish(&mut self, layout: &RowLayout) -> Result<RecordBatch, ConnectorError> {
        let mut arrays = self
            .columns
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(weights) = &mut self.weights {
            arrays.push(Arc::new(weights.finish()));
        }
        self.rows = 0;
        self.retained_bytes = 0;
        RecordBatch::try_new(Arc::clone(&layout.schema), arrays).map_err(|error| {
            ConnectorError::SchemaMismatch(format!(
                "PostgreSQL CDC rows do not match the declared schema: {error}"
            ))
        })
    }
}

fn parse_number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
    text.parse::<T>()
        .map_err(|_| format!("invalid numeric text '{text}'"))
}

fn parse_date(text: &str) -> Result<i32, String> {
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| {
        format!("unsupported date text '{text}' (BC and infinite dates are rejected)")
    })?;
    i32::try_from(
        date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch"))
            .num_days(),
    )
    .map_err(|_| format!("date '{text}' is outside Date32"))
}

fn parse_time(text: &str) -> Result<i64, String> {
    let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .map_err(|_| format!("unsupported time text '{text}'"))?;
    Ok(time
        .signed_duration_since(NaiveTime::MIN)
        .num_microseconds()
        .expect("a time of day fits in microseconds"))
}

fn parse_timestamp(text: &str) -> Result<i64, String> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .map(|timestamp| timestamp.and_utc().timestamp_micros())
        .map_err(|_| {
            format!("unsupported timestamp text '{text}' (BC and infinite values are rejected)")
        })
}

fn parse_timestamptz(text: &str) -> Result<i64, String> {
    DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z")
        .map(|timestamp| timestamp.timestamp_micros())
        .map_err(|_| {
            format!("unsupported timestamptz text '{text}' (BC and infinite values are rejected)")
        })
}

fn decode_bytea_hex(text: &[u8]) -> Result<Vec<u8>, String> {
    let hex = text
        .strip_prefix(b"\\x")
        .ok_or_else(|| "bytea text is not in hex format".to_string())?;
    let (pairs, odd) = hex.as_chunks::<2>();
    if !odd.is_empty() {
        return Err("bytea hex text has an odd length".into());
    }
    pairs
        .iter()
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                b'A'..=b'F' => Ok(byte - b'A' + 10),
                _ => Err("bytea hex text contains a non-hex digit".to_string()),
            };
            Ok(digit(pair[0])? << 4 | digit(pair[1])?)
        })
        .collect()
}

#[cfg(test)]
mod tests;
