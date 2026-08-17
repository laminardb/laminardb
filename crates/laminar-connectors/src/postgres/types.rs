//! Validated Arrow-to-PostgreSQL type contract for the sink.

use arrow_schema::{DataType, TimeUnit};

use crate::error::ConnectorError;

/// SQL spellings used by COPY table DDL and typed UNNEST parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PostgresType {
    sql: &'static str,
    ddl: &'static str,
}

impl PostgresType {
    #[must_use]
    pub(super) const fn sql(self) -> &'static str {
        self.sql
    }

    #[must_use]
    pub(super) const fn ddl(self) -> &'static str {
        self.ddl
    }
}

/// Returns the single supported type mapping used by admission, DDL, COPY, and UNNEST.
///
/// The surface is intentionally the intersection of the COPY encoder and the Rust `PostgreSQL`
/// parameter encoder. Types are added only when both write paths have the same lossless contract.
pub(super) fn postgres_type(data_type: &DataType) -> Result<PostgresType, ConnectorError> {
    let mapping = match data_type {
        DataType::Boolean => PostgresType {
            sql: "bool",
            ddl: "BOOLEAN",
        },
        DataType::Int8 | DataType::UInt8 | DataType::Int16 => PostgresType {
            sql: "int2",
            ddl: "SMALLINT",
        },
        DataType::UInt16 | DataType::Int32 => PostgresType {
            sql: "int4",
            ddl: "INTEGER",
        },
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => PostgresType {
            sql: "int8",
            ddl: "BIGINT",
        },
        DataType::Float32 => PostgresType {
            sql: "float4",
            ddl: "REAL",
        },
        DataType::Float64 => PostgresType {
            sql: "float8",
            ddl: "DOUBLE PRECISION",
        },
        DataType::Utf8 | DataType::LargeUtf8 => PostgresType {
            sql: "text",
            ddl: "TEXT",
        },
        DataType::Binary | DataType::LargeBinary => PostgresType {
            sql: "bytea",
            ddl: "BYTEA",
        },
        DataType::Date32 => PostgresType {
            sql: "date",
            ddl: "DATE",
        },
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            None,
        ) => PostgresType {
            sql: "timestamp",
            ddl: "TIMESTAMP",
        },
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            Some(_),
        ) => PostgresType {
            sql: "timestamptz",
            ddl: "TIMESTAMPTZ",
        },
        unsupported => {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL sink does not support Arrow type {unsupported:?}; supported types are \
                 Boolean, signed integers, UInt8/16/32, range-checked UInt64, Float32/64, \
                 Utf8/LargeUtf8, Binary/LargeBinary, Date32, and second/millisecond/microsecond \
                 Timestamp"
            )));
        }
    };
    Ok(mapping)
}

/// `PostgreSQL` type used in an UNNEST cast.
pub(super) fn arrow_type_to_pg_sql(data_type: &DataType) -> Result<&'static str, ConnectorError> {
    postgres_type(data_type).map(PostgresType::sql)
}

/// `PostgreSQL` type used in generated CREATE TABLE DDL.
pub(super) fn arrow_to_pg_ddl_type(data_type: &DataType) -> Result<&'static str, ConnectorError> {
    postgres_type(data_type).map(PostgresType::ddl)
}

/// Typed `PostgreSQL` array parameter used by an UNNEST statement.
pub(super) fn arrow_type_to_pg_array_cast(
    data_type: &DataType,
    parameter: usize,
) -> Result<String, ConnectorError> {
    Ok(format!(
        "${parameter}::{}[]",
        arrow_type_to_pg_sql(data_type)?
    ))
}

#[cfg(feature = "postgres-sink")]
fn checked_u64(value: u64, row: usize) -> Result<i64, ConnectorError> {
    i64::try_from(value).map_err(|_| {
        ConnectorError::SchemaMismatch(format!(
            "PostgreSQL BIGINT cannot represent UInt64 value {value} at row {row}"
        ))
    })
}

#[cfg(feature = "postgres-sink")]
fn checked_date32(value: i32, row: usize) -> Result<chrono::NaiveDate, ConnectorError> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid Unix epoch");
    epoch
        .checked_add_signed(chrono::Duration::days(i64::from(value)))
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "PostgreSQL sink cannot represent Date32 value {value} at row {row}"
            ))
        })
}

/// Validates range-sensitive values before a batch is admitted to the sink buffer.
#[cfg(feature = "postgres-sink")]
pub(super) fn validate_postgres_array_values(
    array: &dyn arrow_array::Array,
) -> Result<(), ConnectorError> {
    use arrow_array::{Array as _, Date32Array, UInt64Array};

    postgres_type(array.data_type())?;
    match array.data_type() {
        DataType::UInt64 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to UInt64Array failed".into()))?;
            for row in 0..values.len() {
                if !values.is_null(row) {
                    checked_u64(values.value(row), row)?;
                }
            }
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to Date32Array failed".into()))?;
            for row in 0..values.len() {
                if !values.is_null(row) {
                    checked_date32(values.value(row), row)?;
                }
            }
        }
        DataType::Timestamp(unit, _) => validate_timestamp_values(array, *unit)?,
        _ => {}
    }
    Ok(())
}

/// Produces the batch schema expected by COPY BINARY.
///
/// `pgpq` encodes Arrow `UInt64` as `PostgreSQL` NUMERIC. The sink deliberately exposes `UInt64` as a
/// range-checked BIGINT so COPY and UNNEST have identical table types; values are therefore widened
/// to an Int64 Arrow column after validation and before the COPY encoder is constructed.
#[cfg(feature = "postgres-sink")]
pub(super) fn postgres_copy_batch(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch, ConnectorError> {
    use std::sync::Arc;

    use arrow_array::{Array as _, ArrayRef, Int64Array, UInt64Array};
    use arrow_schema::Schema;

    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        postgres_type(column.data_type())?;
        if field.data_type() == &DataType::UInt64 {
            let values = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to UInt64Array failed".into()))?;
            let converted = (0..values.len())
                .map(|row| {
                    if values.is_null(row) {
                        Ok(None)
                    } else {
                        checked_u64(values.value(row), row).map(Some)
                    }
                })
                .collect::<Result<Vec<Option<i64>>, ConnectorError>>()?;
            fields.push(Arc::new(
                field.as_ref().clone().with_data_type(DataType::Int64),
            ));
            columns.push(Arc::new(Int64Array::from(converted)));
            changed = true;
        } else {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }

    if changed {
        arrow_array::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| {
            ConnectorError::Internal(format!("build PostgreSQL COPY batch: {error}"))
        })
    } else {
        Ok(batch.clone())
    }
}

#[cfg(feature = "postgres-sink")]
fn validate_timestamp_values(
    array: &dyn arrow_array::Array,
    unit: TimeUnit,
) -> Result<(), ConnectorError> {
    use arrow_array::{
        Array as _, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
    };

    macro_rules! validate {
        ($array_type:ty) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    ConnectorError::Internal(format!(
                        "timestamp array does not match declared unit {unit:?}"
                    ))
                })?;
            for row in 0..values.len() {
                if !values.is_null(row) && to_naive_datetime(values.value(row), unit).is_none() {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "PostgreSQL sink cannot represent {unit:?} timestamp value {} at row {row}",
                        values.value(row)
                    )));
                }
            }
        }};
    }

    match unit {
        TimeUnit::Second => validate!(TimestampSecondArray),
        TimeUnit::Millisecond => validate!(TimestampMillisecondArray),
        TimeUnit::Microsecond => validate!(TimestampMicrosecondArray),
        TimeUnit::Nanosecond => unreachable!("nanosecond timestamps fail type admission"),
    }
    Ok(())
}

/// Converts an Arrow column to a `PostgreSQL` array parameter for UNNEST.
#[cfg(feature = "postgres-sink")]
pub(super) fn arrow_column_to_pg_array(
    array: &dyn arrow_array::Array,
) -> Result<Box<dyn postgres_types::ToSql + Sync + Send>, ConnectorError> {
    use arrow_array::{
        Array as _, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    };

    postgres_type(array.data_type())?;

    macro_rules! widen {
        ($array_type:ty, $target:ty) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    ConnectorError::Internal(format!(
                        "downcast to {} failed",
                        stringify!($array_type)
                    ))
                })?;
            let converted: Vec<Option<$target>> = (0..values.len())
                .map(|row| (!values.is_null(row)).then(|| <$target>::from(values.value(row))))
                .collect();
            Ok(Box::new(converted))
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ConnectorError::Internal("downcast to BooleanArray failed".into())
                })?;
            Ok(Box::new(
                (0..values.len())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row)))
                    .collect::<Vec<Option<bool>>>(),
            ))
        }
        DataType::Int8 => widen!(Int8Array, i16),
        DataType::UInt8 => widen!(UInt8Array, i16),
        DataType::Int16 => widen!(Int16Array, i16),
        DataType::UInt16 => widen!(UInt16Array, i32),
        DataType::Int32 => widen!(Int32Array, i32),
        DataType::UInt32 => widen!(UInt32Array, i64),
        DataType::Int64 => widen!(Int64Array, i64),
        DataType::UInt64 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to UInt64Array failed".into()))?;
            let converted = (0..values.len())
                .map(|row| {
                    if values.is_null(row) {
                        Ok(None)
                    } else {
                        checked_u64(values.value(row), row).map(Some)
                    }
                })
                .collect::<Result<Vec<Option<i64>>, ConnectorError>>()?;
            Ok(Box::new(converted))
        }
        DataType::Float32 => widen!(Float32Array, f32),
        DataType::Float64 => widen!(Float64Array, f64),
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ConnectorError::Internal("downcast to StringArray failed".into()))?;
            Ok(Box::new(
                (0..values.len())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row).to_owned()))
                    .collect::<Vec<Option<String>>>(),
            ))
        }
        DataType::LargeUtf8 => {
            let values = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ConnectorError::Internal("downcast to LargeStringArray failed".into())
                })?;
            Ok(Box::new(
                (0..values.len())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row).to_owned()))
                    .collect::<Vec<Option<String>>>(),
            ))
        }
        DataType::Binary => {
            let values = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ConnectorError::Internal("downcast to BinaryArray failed".into()))?;
            Ok(Box::new(
                (0..values.len())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row).to_vec()))
                    .collect::<Vec<Option<Vec<u8>>>>(),
            ))
        }
        DataType::LargeBinary => {
            let values = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    ConnectorError::Internal("downcast to LargeBinaryArray failed".into())
                })?;
            Ok(Box::new(
                (0..values.len())
                    .map(|row| (!values.is_null(row)).then(|| values.value(row).to_vec()))
                    .collect::<Vec<Option<Vec<u8>>>>(),
            ))
        }
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to Date32Array failed".into()))?;
            let converted = (0..values.len())
                .map(|row| {
                    if values.is_null(row) {
                        Ok(None)
                    } else {
                        checked_date32(values.value(row), row).map(Some)
                    }
                })
                .collect::<Result<Vec<Option<chrono::NaiveDate>>, ConnectorError>>()?;
            Ok(Box::new(converted))
        }
        DataType::Timestamp(unit, timezone) => {
            macro_rules! timestamps {
                ($array_type:ty) => {{
                    let values = array
                        .as_any()
                        .downcast_ref::<$array_type>()
                        .ok_or_else(|| {
                            ConnectorError::Internal(format!(
                                "timestamp array does not match declared unit {unit:?}"
                            ))
                        })?;
                    (0..values.len())
                        .map(|row| {
                            if values.is_null(row) {
                                Ok(None)
                            } else {
                                to_naive_datetime(values.value(row), *unit)
                                    .map(Some)
                                    .ok_or_else(|| {
                                        ConnectorError::SchemaMismatch(format!(
                                            "PostgreSQL sink cannot represent {unit:?} timestamp \
                                             value {} at row {row}",
                                            values.value(row)
                                        ))
                                    })
                            }
                        })
                        .collect::<Result<Vec<Option<chrono::NaiveDateTime>>, ConnectorError>>()?
                }};
            }

            let values = match unit {
                TimeUnit::Second => timestamps!(TimestampSecondArray),
                TimeUnit::Millisecond => timestamps!(TimestampMillisecondArray),
                TimeUnit::Microsecond => timestamps!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => unreachable!("nanosecond timestamps fail type admission"),
            };
            if timezone.is_some() {
                Ok(Box::new(
                    values
                        .into_iter()
                        .map(|value| value.map(|timestamp| timestamp.and_utc()))
                        .collect::<Vec<Option<chrono::DateTime<chrono::Utc>>>>(),
                ))
            } else {
                Ok(Box::new(values))
            }
        }
        unsupported => Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL sink does not support Arrow type {unsupported:?}"
        ))),
    }
}

/// Converts a Unix timestamp using Euclidean division so negative fractional values retain their
/// correct instant (for example, -1 ms is 1969-12-31 23:59:59.999, not one second late).
#[cfg(feature = "postgres-sink")]
fn to_naive_datetime(value: i64, unit: TimeUnit) -> Option<chrono::NaiveDateTime> {
    let (units_per_second, nanos_per_unit) = match unit {
        TimeUnit::Second => (1_i64, 1_000_000_000_u32),
        TimeUnit::Millisecond => (1_000, 1_000_000),
        TimeUnit::Microsecond => (1_000_000, 1_000),
        TimeUnit::Nanosecond => (1_000_000_000, 1),
    };
    let seconds = value.div_euclid(units_per_second);
    let fraction = u32::try_from(value.rem_euclid(units_per_second)).ok()?;
    let nanos = fraction.checked_mul(nanos_per_unit)?;
    chrono::DateTime::from_timestamp(seconds, nanos).map(|timestamp| timestamp.naive_utc())
}

#[cfg(test)]
mod tests;
